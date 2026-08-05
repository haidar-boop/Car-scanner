// ==UserScript==
// @name         FB Marketplace Car Watcher (Edmonton)
// @namespace    car-scanner
// @version      0.3.0
// @description  Rotates a pinned tab through Edmonton car/truck/SUV searches, posts scraped listings to the droplet scorer
// @match        https://www.facebook.com/marketplace/*
// @grant        GM_setValue
// @grant        GM_getValue
// @grant        GM_registerMenuCommand
// @grant        GM_xmlhttpRequest
// @connect      api.telegram.org
// @connect      YOUR_DROPLET_HOST
// @run-at       document-idle
// @noframes
// ==/UserScript==

(function () {
  "use strict";

  // --- CONFIG ---------------------------------------------------------------

  // Every URL must carry sortBy=creation_time_descend — the scraper only sees
  // what's rendered near the top of page one, so any other sort silently
  // breaks the system. FB keyword search has no body-type param, so trucks
  // and SUVs are covered by their own keyword queries.
  const SEARCH_URLS = [
    { label: "cars_2k_15k",
      url: "https://www.facebook.com/marketplace/edmonton/search?query=car&minPrice=2000&maxPrice=15000&sortBy=creation_time_descend&exact=false" },
    { label: "cars_15k_35k",
      url: "https://www.facebook.com/marketplace/edmonton/search?query=car&minPrice=15000&maxPrice=35000&sortBy=creation_time_descend&exact=false" },
    { label: "trucks_5k_30k",
      url: "https://www.facebook.com/marketplace/edmonton/search?query=truck&minPrice=5000&maxPrice=30000&sortBy=creation_time_descend&exact=false" },
    { label: "suvs_5k_30k",
      url: "https://www.facebook.com/marketplace/edmonton/search?query=suv&minPrice=5000&maxPrice=30000&sortBy=creation_time_descend&exact=false" },
  ];

  const RELOAD_MIN_MS = 5 * 60 * 1000;   // reload randomized between 5 ...
  const RELOAD_MAX_MS = 15 * 60 * 1000;  // ... and 15 minutes
  const SCRAPE_INTERVAL_MS = 20 * 1000;  // FB lazy-renders; re-scan the DOM
  const SEEN_CAP = 2000;                 // FIFO cap on remembered listing IDs
  const WARN_COOLDOWN_MS = 6 * 3600 * 1000;
  const SEND_SPACING_MS = 1100;          // Telegram allows ~1 msg/s per chat

  // Scraped listings are POSTed to the droplet, which runs the rejection
  // and scoring pipeline and sends any deal alerts itself. Edit the host
  // here AND in the @connect line above, then set the token via the
  // Tampermonkey menu ("Set droplet ingest token").
  const INGEST_URL = "http://YOUR_DROPLET_HOST:8477/ingest";
  const INGEST_FLUSH_MS = 60 * 1000;
  const INGEST_BUFFER_CAP = 500;         // oldest dropped past this
  const INGEST_BATCH_MAX = 200;          // server-side items-per-request cap
  const INGEST_TIMEOUT_MS = 20 * 1000;
  // FB lazy-renders, so the first ticks legitimately see nothing. Five
  // consecutive empty ticks (~100s) on a sorted search page means the
  // anchors moved — i.e. the scraper is blind, which otherwise looks
  // exactly like "no new listings in Edmonton".
  const EMPTY_TICKS_ALARM = 5;

  // --------------------------------------------------------------------------

  const ITEM_ID_RE = /\/marketplace\/item\/(\d+)/;
  // Comma-grouped capture is load-bearing: textContent glues FB's price and
  // title nodes with no separator ("CA$9,5002014 Ford..."), and a naive
  // [\d,]+ would swallow the title's year into the price.
  const PRICE_RE = /(?:CA\s?\$|C\$|\$)\s?(\d{1,3}(?:,\d{3})*)/;

  // The watcher only ever acts on its own configured searches. Any other
  // marketplace page — homepage, item pages, the user's own browsing tabs —
  // is full of recommendation anchors that are neither new nor price-filtered,
  // so scraping there would be pure false-alert noise, and rotating there
  // would hijack tabs the user is actually reading.
  function onSearchPath() {
    return location.pathname.includes("/marketplace/") &&
           location.pathname.includes("/search");
  }

  function activeSearch() {
    if (!onSearchPath()) return null;
    const here = new URLSearchParams(location.search);
    for (const s of SEARCH_URLS) {
      const want = new URL(s.url);
      if (!location.pathname.startsWith(want.pathname)) continue;
      let match = true;
      for (const [k, v] of want.searchParams) {
        if (k === "sortBy") continue; // the sort guard owns this one
        if (here.get(k) !== v) { match = false; break; }
      }
      if (match) return s;
    }
    return null;
  }

  function promptForCreds() {
    const token = (prompt("Car watcher: Telegram bot token (blank = disable alerts)") || "").trim();
    const chat = token ? (prompt("Car watcher: Telegram chat ID") || "").trim() : "";
    // A deliberate blank stores a sentinel so we never re-prompt on every send.
    GM_setValue("tg_token", token || "DISABLED");
    GM_setValue("tg_chat", chat);
  }

  function getTelegramCreds() {
    // Browsers have no env vars; creds live in Tampermonkey storage so the
    // committed file stays secret-free. Prompted once; re-set via the
    // Tampermonkey menu command.
    let token = GM_getValue("tg_token", "");
    if (!token) {
      promptForCreds();
      token = GM_getValue("tg_token", "");
    }
    const chat = GM_getValue("tg_chat", "");
    return token && token !== "DISABLED" && chat ? { token, chat } : null;
  }

  // Sends are queued and spaced out; a burst of new listings must not trip
  // Telegram's per-chat rate limit and silently drop alerts.
  const sendQueue = [];
  let sendTimer = null;

  function telegramSend(text) {
    sendQueue.push(text);
    if (!sendTimer) drainSendQueue();
  }

  function drainSendQueue() {
    const text = sendQueue.shift();
    if (text === undefined) { sendTimer = null; return; }
    sendTimer = setTimeout(drainSendQueue, SEND_SPACING_MS);
    const creds = getTelegramCreds();
    if (!creds) {
      console.warn("[car-watcher] telegram disabled; would send:", text);
      return;
    }
    GM_xmlhttpRequest({
      method: "POST",
      url: "https://api.telegram.org/bot" + creds.token + "/sendMessage",
      headers: { "Content-Type": "application/json" },
      data: JSON.stringify({
        chat_id: creds.chat,
        text: text,
        disable_web_page_preview: true,
      }),
      onload: (resp) => {
        if (resp.status !== 200) {
          console.warn("[car-watcher] telegram HTTP", resp.status, resp.responseText);
        }
      },
      onerror: (e) => console.warn("[car-watcher] telegram send failed", e),
    });
  }

  // --- droplet ingest bridge ------------------------------------------------

  let ingestInFlight = false;

  function bufferListing(item) {
    const buf = GM_getValue("ingest_buffer", []);
    buf.push(item);
    if (buf.length > INGEST_BUFFER_CAP) {
      // Cap overflow means the droplet has been unreachable for a long time
      // and unsent listings are now being lost — that's an outage, say so.
      console.warn("[car-watcher] ingest buffer full — dropping oldest unsent listings");
      const lastWarn = GM_getValue("buffer_warned_at", 0);
      if (Date.now() - lastWarn > WARN_COOLDOWN_MS) {
        GM_setValue("buffer_warned_at", Date.now());
        telegramSend("WARNING [facebook] ingest buffer overflowing — droplet " +
                     "unreachable? Oldest scraped listings are being dropped.");
      }
    }
    GM_setValue("ingest_buffer", buf.slice(-INGEST_BUFFER_CAP));
    flushIngest();
  }

  function flushIngest() {
    if (ingestInFlight) return;
    // Split literal so a global find-replace of the placeholder (the natural
    // way to configure the host) can't rewrite this guard into matching the
    // user's real host and silently disabling ingest forever.
    if (INGEST_URL.includes("YOUR_" + "DROPLET_HOST")) return; // not configured yet
    const token = GM_getValue("ingest_token", "");
    if (!token) return;
    const buf = GM_getValue("ingest_buffer", []);
    if (buf.length === 0) return;
    const n = Math.min(buf.length, INGEST_BATCH_MAX);
    ingestInFlight = true;
    const done = (ok, why) => {
      ingestInFlight = false;
      if (ok) {
        // splice against a re-read so items scraped mid-flight survive
        const cur = GM_getValue("ingest_buffer", []);
        GM_setValue("ingest_buffer", cur.slice(n));
        if (cur.length > n) flushIngest();
      } else {
        console.warn("[car-watcher] ingest", why, "- buffered", buf.length);
      }
    };
    GM_xmlhttpRequest({
      method: "POST",
      url: INGEST_URL,
      timeout: INGEST_TIMEOUT_MS,
      headers: {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + token,
      },
      data: JSON.stringify(buf.slice(0, n)),
      onload: (resp) => done(resp.status === 200, "HTTP " + resp.status),
      onerror: () => done(false, "network error"),
      ontimeout: () => done(false, "timeout"),
    });
  }

  // --------------------------------------------------------------------------

  function loadSeen() {
    return new Set(GM_getValue("seen_ids", []));
  }

  function saveSeen(ids) {
    // Array doubles as insertion-ordered FIFO; trim oldest past the cap.
    GM_setValue("seen_ids", ids.slice(-SEEN_CAP));
  }

  function setBanner(show, text) {
    const existing = document.getElementById("car-watcher-warning");
    if (!show) {
      if (existing) existing.remove();
      return;
    }
    const banner = existing || document.createElement("div");
    banner.id = "car-watcher-warning";
    banner.textContent = text;
    banner.style.cssText =
      "position:fixed;top:0;left:0;right:0;z-index:99999;background:#c0392b;" +
      "color:#fff;font:14px sans-serif;padding:6px;text-align:center;";
    if (!existing) document.body.appendChild(banner);
  }

  // A tab that stops matching a configured search has stopped watching, and
  // silence is indistinguishable from "no new listings" — so say so loudly
  // rather than no-op'ing. Only fires when we *expected* to be watching:
  // ordinary browsing on some other Marketplace search stays quiet.
  let lostWarned = false;
  let wasActive = false;
  let emptyTicks = 0;
  let emptyWarned = false;

  function warnScraperBlind() {
    if (emptyWarned) return;
    emptyWarned = true;
    console.warn("[car-watcher] no listing anchors found — FB markup changed?");
    setBanner(true, "car-watcher: no listings found on this page — scraper may be blind");
    const lastWarn = GM_getValue("empty_warned_at", 0);
    if (Date.now() - lastWarn > WARN_COOLDOWN_MS) {
      GM_setValue("empty_warned_at", Date.now());
      telegramSend("🚨 BROKEN [facebook] the search page rendered but no listing " +
                   "anchors were found over ~100s. Facebook likely changed its " +
                   "markup — the userscript selector needs updating.");
    }
  }

  function warnLostSearch(reason) {
    if (lostWarned) return;
    lostWarned = true;
    console.warn("[car-watcher]", reason);
    setBanner(true, "car-watcher: " + reason + " — alerts suspended");
    const lastWarn = GM_getValue("lost_warned_at", 0);
    if (Date.now() - lastWarn > WARN_COOLDOWN_MS) {
      GM_setValue("lost_warned_at", Date.now());
      telegramSend("WARNING [facebook] " + reason + "; alerts suspended for this " +
                   "tab. Facebook may have changed its search URL format — check " +
                   "SEARCH_URLS in the userscript.");
    }
  }

  function checkSortGuard() {
    // FB sometimes strips query params on client-side redirects. Without the
    // date-desc sort, everything scraped could be stale "recommended" rows —
    // warn loudly and don't alert.
    const params = new URLSearchParams(location.search);
    if (params.get("sortBy") === "creation_time_descend") {
      setBanner(false);
      return true;
    }
    console.warn("[car-watcher] sortBy=creation_time_descend missing from URL");
    setBanner(true, "car-watcher: this page is NOT sorted by newest — alerts suspended");
    const lastWarn = GM_getValue("sort_warned_at", 0);
    if (Date.now() - lastWarn > WARN_COOLDOWN_MS) {
      GM_setValue("sort_warned_at", Date.now());
      telegramSend("WARNING [facebook] search tab lost its newest-first sort; alerts suspended until it recovers");
    }
    return false;
  }

  function scrapeListings() {
    // Anchors with /marketplace/item/ hrefs are the most churn-resistant part
    // of FB's DOM — everything else (class names, wrappers) rotates weekly.
    const out = [];
    const seen = new Set();
    for (const a of document.querySelectorAll('a[href*="/marketplace/item/"]')) {
      const m = a.href.match(ITEM_ID_RE);
      if (!m || seen.has(m[1])) continue;
      seen.add(m[1]);
      // innerText keeps FB's node boundaries as newlines; the price sits on
      // its own line. Strip that line from the title text so the droplet's
      // year/km parsers see "2014 Ford F-150...", not "CA$9,5002014 Ford...".
      const raw = (a.innerText || a.textContent || "").trim();
      const lines = raw.split("\n").map((s) => s.trim()).filter(Boolean);
      let price = null;
      const titleParts = [];
      for (const line of (lines.length > 1 ? lines : [raw])) {
        const pm = line.match(PRICE_RE);
        if (pm && price === null) {
          price = pm[1];
          if (lines.length > 1) continue; // drop the price line from the title
        }
        titleParts.push(line);
      }
      let text = titleParts.join(" ");
      if (lines.length <= 1 && price !== null) {
        text = raw.replace(PRICE_RE, " "); // glued fallback: excise the price
      }
      text = text.replace(/\s+/g, " ").trim();
      out.push({
        id: m[1],
        url: "https://www.facebook.com/marketplace/item/" + m[1],
        price: price,
        text: text.slice(0, 200),
      });
    }
    return out;
  }

  function tick() {
    const search = activeSearch();
    if (!search) {
      // Drifting off a search we were actively watching means FB rewrote the
      // URL under us (SPA navigation strips params without a reload).
      if (wasActive && onSearchPath()) {
        warnLostSearch("search tab lost its configured query parameters");
      }
      return; // navigated to an item/home page — that's normal, stay quiet
    }
    wasActive = true;
    const sortOk = checkSortGuard();
    if (!sortOk) return; // don't notify AND don't mark seen: listings that
                         // appear while the sort is broken must still alert
                         // once it recovers, not be silently swallowed
    const listings = scrapeListings();
    if (listings.length === 0) {
      // Don't mistake a permanently broken selector for a slow render.
      if (++emptyTicks >= EMPTY_TICKS_ALARM) warnScraperBlind();
      return;
    }
    if (emptyTicks >= EMPTY_TICKS_ALARM) {
      setBanner(false);
      console.log("[car-watcher] listings visible again");
    }
    emptyTicks = 0;
    emptyWarned = false;

    const seenSet = loadSeen();
    const seenArr = GM_getValue("seen_ids", []);
    // Seed each search separately: the first clean scan of a given search
    // marks its current inventory seen without notifying, so neither install
    // nor the first rotation into a new search floods Telegram.
    const seeded = GM_getValue("seeded_labels", {});
    const seeding = !seeded[search.label];
    let dirty = false;

    for (const l of listings) {
      if (seenSet.has(l.id)) continue;
      seenSet.add(l.id);
      seenArr.push(l.id);
      dirty = true;
      // The droplet decides what's alert-worthy; seed-pass items are flagged
      // so it stores them as comp data without ever alerting.
      bufferListing({
        id: l.id, url: l.url, price_text: l.price, text: l.text,
        label: search.label, seen_at: new Date().toISOString(), seed: seeding,
      });
    }
    if (dirty) saveSeen(seenArr);
    if (seeding) {
      seeded[search.label] = true;
      GM_setValue("seeded_labels", seeded);
      console.log("[car-watcher] seeded", search.label, "with", listings.length, "listings");
    }
  }

  function scheduleRotateReload() {
    const delay = RELOAD_MIN_MS + Math.random() * (RELOAD_MAX_MS - RELOAD_MIN_MS);
    console.log("[car-watcher] next rotation in", Math.round(delay / 60000), "min");
    setTimeout(() => {
      const next = (GM_getValue("rot_idx", 0) + 1) % SEARCH_URLS.length;
      GM_setValue("rot_idx", next);
      // Remember where we meant to land, so the next page load can tell a
      // failed rotation apart from the user browsing somewhere else.
      GM_setValue("rot_expect", SEARCH_URLS[next].label);
      location.href = SEARCH_URLS[next].url;
    }, delay);
  }

  function promptForIngestToken() {
    const token = (prompt("Car watcher: droplet ingest token (INGEST_TOKEN)") || "").trim();
    GM_setValue("ingest_token", token);
  }

  function main() {
    if (typeof GM_registerMenuCommand === "function") {
      GM_registerMenuCommand("Set Telegram credentials", promptForCreds);
      GM_registerMenuCommand("Set droplet ingest token", promptForIngestToken);
    }
    // Only a tab sitting on one of the configured searches becomes the
    // watcher; ordinary browsing tabs are left alone (no rotation hijack, no
    // concurrent writers racing on the seen-ID store). To start watching, pin
    // a tab and open the first SEARCH_URLS entry in it.
    const search = activeSearch();
    if (!search) {
      // A rotation we initiated must land on a configured search. If it
      // didn't, FB changed its URL format and the watcher is now blind.
      const expected = GM_getValue("rot_expect", "");
      if (expected && onSearchPath()) {
        GM_setValue("rot_expect", "");
        warnLostSearch("rotation into '" + expected + "' landed on an unrecognized search URL");
      }
      return;
    }
    GM_setValue("rot_expect", "");
    console.log("[car-watcher] active on", search.label);
    tick();
    setInterval(tick, SCRAPE_INTERVAL_MS);
    setInterval(flushIngest, INGEST_FLUSH_MS);
    scheduleRotateReload();
  }

  main();
})();
