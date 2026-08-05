// ==UserScript==
// @name         FB Marketplace Car Watcher (Edmonton)
// @namespace    car-scanner
// @version      0.1.0
// @description  Rotates a pinned tab through Edmonton car/truck/SUV searches, Telegram-notifies new listings
// @match        https://www.facebook.com/marketplace/*
// @grant        GM_setValue
// @grant        GM_getValue
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

  // --------------------------------------------------------------------------

  const ITEM_ID_RE = /\/marketplace\/item\/(\d+)/;
  const PRICE_RE = /(?:CA\s?\$|C\$|\$)\s?([\d,]+)/;

  function getTelegramCreds() {
    // Browsers have no env vars; creds live in Tampermonkey storage so the
    // committed file stays secret-free. Prompted once on first run.
    let token = GM_getValue("tg_token", "");
    let chat = GM_getValue("tg_chat", "");
    if (!token || !chat) {
      token = (prompt("Car watcher: Telegram bot token (blank = disable alerts)") || "").trim();
      chat = token ? (prompt("Car watcher: Telegram chat ID") || "").trim() : "";
      GM_setValue("tg_token", token);
      GM_setValue("tg_chat", chat);
    }
    return token && chat ? { token, chat } : null;
  }

  function telegramSend(text) {
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

  function currentSearch() {
    const idx = GM_getValue("rot_idx", 0) % SEARCH_URLS.length;
    return SEARCH_URLS[idx];
  }

  function checkSortGuard() {
    // FB sometimes strips query params on client-side redirects. If this tab
    // is on a search page without date-desc sort, everything we scrape could
    // be stale "recommended" results — warn loudly and don't notify.
    if (!location.pathname.includes("/marketplace/") || !location.pathname.includes("/search")) {
      return true; // not a search page (e.g. user browsing an item) — no opinion
    }
    const params = new URLSearchParams(location.search);
    if (params.get("sortBy") === "creation_time_descend") return true;

    console.warn("[car-watcher] sortBy=creation_time_descend missing from URL");
    if (!document.getElementById("car-watcher-sort-warning")) {
      const banner = document.createElement("div");
      banner.id = "car-watcher-sort-warning";
      banner.textContent =
        "car-watcher: this page is NOT sorted by newest — alerts suspended";
      banner.style.cssText =
        "position:fixed;top:0;left:0;right:0;z-index:99999;background:#c0392b;" +
        "color:#fff;font:14px sans-serif;padding:6px;text-align:center;";
      document.body.appendChild(banner);
    }
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
    const sortOk = checkSortGuard();
    const listings = scrapeListings();
    if (listings.length === 0) return; // page still rendering; nothing to judge yet

    const seenSet = loadSeen();
    const seenArr = GM_getValue("seen_ids", []);
    const firstRun = seenArr.length === 0;
    const label = currentSearch().label;
    let dirty = false;

    for (const l of listings) {
      if (seenSet.has(l.id)) continue;
      seenSet.add(l.id);
      seenArr.push(l.id);
      dirty = true;
      // Seed mode: the very first scan marks current inventory as seen
      // without notifying, so installing the script doesn't flood Telegram.
      if (!firstRun && sortOk) {
        const price = l.price ? "$" + l.price : "price n/a";
        telegramSend("NEW [facebook/" + label + "] " + price + " — " +
                     l.text + "\n" + l.url);
      }
    }
    if (dirty) saveSeen(seenArr);
    if (firstRun) console.log("[car-watcher] seeded", seenArr.length, "listings");
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
    if (!location.pathname.startsWith("/marketplace")) return;
    console.log("[car-watcher] active on", currentSearch().label);
    tick();
    setInterval(tick, SCRAPE_INTERVAL_MS);
    scheduleRotateReload();
  }

  main();
})();
