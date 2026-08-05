// ==UserScript==
// @name         FB Marketplace Car Watcher (Edmonton)
// @namespace    car-scanner
// @version      0.2.0
// @description  Rotates a pinned tab through Edmonton car/truck/SUV searches, Telegram-notifies new listings
// @match        https://www.facebook.com/marketplace/*
// @grant        GM_setValue
// @grant        GM_getValue
// @grant        GM_registerMenuCommand
// @grant        GM_xmlhttpRequest
// @connect      api.telegram.org
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

  // --------------------------------------------------------------------------

  const ITEM_ID_RE = /\/marketplace\/item\/(\d+)/;
  const PRICE_RE = /(?:CA\s?\$|C\$|\$)\s?([\d,]+)/;

  // The watcher only ever acts on its own configured searches. Any other
  // marketplace page — homepage, item pages, the user's own browsing tabs —
  // is full of recommendation anchors that are neither new nor price-filtered,
  // so scraping there would be pure false-alert noise, and rotating there
  // would hijack tabs the user is actually reading.
  function activeSearch() {
    if (!location.pathname.includes("/marketplace/") ||
        !location.pathname.includes("/search")) return null;
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

  function loadSeen() {
    return new Set(GM_getValue("seen_ids", []));
  }

  function saveSeen(ids) {
    // Array doubles as insertion-ordered FIFO; trim oldest past the cap.
    GM_setValue("seen_ids", ids.slice(-SEEN_CAP));
  }

  function setBanner(show) {
    const existing = document.getElementById("car-watcher-sort-warning");
    if (!show) {
      if (existing) existing.remove();
      return;
    }
    if (existing) return;
    const banner = document.createElement("div");
    banner.id = "car-watcher-sort-warning";
    banner.textContent =
      "car-watcher: this page is NOT sorted by newest — alerts suspended";
    banner.style.cssText =
      "position:fixed;top:0;left:0;right:0;z-index:99999;background:#c0392b;" +
      "color:#fff;font:14px sans-serif;padding:6px;text-align:center;";
    document.body.appendChild(banner);
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
    setBanner(true);
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
      const text = (a.textContent || "").trim();
      const priceMatch = text.match(PRICE_RE);
      out.push({
        id: m[1],
        url: "https://www.facebook.com/marketplace/item/" + m[1],
        price: priceMatch ? priceMatch[1] : null,
        text: text.slice(0, 200),
      });
    }
    return out;
  }

  function tick() {
    const search = activeSearch();
    if (!search) return; // SPA-navigated off our searches — do nothing at all
    const sortOk = checkSortGuard();
    if (!sortOk) return; // don't notify AND don't mark seen: listings that
                         // appear while the sort is broken must still alert
                         // once it recovers, not be silently swallowed
    const listings = scrapeListings();
    if (listings.length === 0) return; // page still rendering

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
      if (!seeding) {
        const price = l.price ? "$" + l.price : "price n/a";
        telegramSend("NEW [facebook/" + search.label + "] " + price + " — " +
                     l.text + "\n" + l.url);
      }
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
      location.href = SEARCH_URLS[next].url;
    }, delay);
  }

  function main() {
    if (typeof GM_registerMenuCommand === "function") {
      GM_registerMenuCommand("Set Telegram credentials", promptForCreds);
    }
    // Only a tab sitting on one of the configured searches becomes the
    // watcher; ordinary browsing tabs are left alone (no rotation hijack, no
    // concurrent writers racing on the seen-ID store). To start watching, pin
    // a tab and open the first SEARCH_URLS entry in it.
    if (!activeSearch()) return;
    console.log("[car-watcher] active on", activeSearch().label);
    tick();
    setInterval(tick, SCRAPE_INTERVAL_MS);
    scheduleRotateReload();
  }

  main();
})();
