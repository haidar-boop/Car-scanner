// ==UserScript==
// @name         FB Marketplace Car Watcher (Edmonton)
// @namespace    car-scanner
// @version      0.4.0
// @description  Rotates a pinned tab through Edmonton car/truck/SUV searches, posts scraped listings to the droplet scorer
// @match        https://www.facebook.com/marketplace/*
// @grant        GM_setValue
// @grant        GM_getValue
// @grant        GM_registerMenuCommand
// @grant        GM_xmlhttpRequest
// @connect      api.telegram.org
// @connect      www.facebook.com
// @connect      YOUR_DROPLET_HOST
// @run-at       document-idle
// @noframes
// ==/UserScript==
// @connect www.facebook.com is required even though the page IS facebook:
// GM_xmlhttpRequest runs in the extension background context and every
// target host must be whitelisted. Cookies ride along by default, which is
// what lets item-page enrichment fetches see the logged-in page.

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
  const LOST_SEARCH_RETRY_MS = 10 * 60 * 1000;  // retry a failed rotation landing

  // Scraped listings are POSTed to the droplet, which runs the rejection
  // and scoring pipeline and sends any deal alerts itself. Edit the host
  // here AND in the @connect line above, then set the token via the
  // Tampermonkey menu ("Set droplet ingest token").
  const INGEST_URL = "http://YOUR_DROPLET_HOST:8477/ingest";
  const INGEST_FLUSH_MS = 60 * 1000;
  const INGEST_BUFFER_CAP = 500;         // oldest dropped past this
  // 60, not the server's 200-item cap: enriched items run ~2-3 KB each, and
  // a batch must stay far below the droplet's 1 MB body limit — an oversized
  // batch would 413 and be retried forever, a permanent silent outage.
  const INGEST_BATCH_MAX = 60;
  const INGEST_TIMEOUT_MS = 20 * 1000;
  const INGEST_HEARTBEAT_MS = 15 * 60 * 1000;  // empty post so silence == dead tab

  // --- item-page enrichment -------------------------------------------------
  // After spotting a NEW listing, fetch its item page with the logged-in
  // session and extract description/odometer, so the droplet's blocklist can
  // see "rebuilt title" and the listing becomes scoreable. This touches the
  // user's real FB account: every cap below is deliberately conservative,
  // and a checkpoint/login response disables the feature for hours.
  const ENRICH_ENABLED = true;           // kill switch: false = exact v0.3 behavior
  const ENRICH_TIMEOUT_MS = 8000;
  const ENRICH_PER_TICK_MAX = 4;         // new listings queued per tick
  const ENRICH_GAP_MIN_MS = 4000;        // jittered spacing between fetches
  const ENRICH_GAP_MAX_MS = 9000;
  const ENRICH_HOURLY_CAP = 30;          // dispatched fetches per rolling hour
  const ENRICH_QUEUE_CAP = 50;           // overflow ships bare immediately
  const ENRICH_QUEUE_MAX_AGE_MS = 3 * 60 * 1000;  // startup sweep bound
  const ENRICH_DISABLE_MS = 6 * 3600 * 1000;      // after a block/login wall
  const ENRICH_DESC_CAP = 1500;
  // Seed listings (install-time inventory) are the initial FB comp pool, so
  // they are worth enriching — but only as an idle drip, never a burst: the
  // burst is the bot-like signature, not the fetches themselves.
  const ENRICH_SEED = true;
  const ENRICH_SEED_GAP_MS = 75 * 1000;
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

  // --- item-page enrichment -------------------------------------------------
  // EVERY JSON key below is an ASSUMPTION from community knowledge of FB's
  // GraphQL payloads — the droplet sandbox cannot see past the login wall.
  // Each shipped item carries enrich_keys telemetry naming exactly which
  // extractors matched, so after one real look at a live item page
  // (view-source, search "redacted_description") the key names can be fixed
  // here without guessing. "failed/no_keys" dominating the telemetry is the
  // signature of FB having renamed things.

  function jsonStrRe(key, between) {
    // Captures a JSON string literal INCLUDING quotes; JSON.parse of the
    // capture then decodes \uXXXX, \", \n correctly.
    return new RegExp('"' + key + '"\\s*:\\s*' + (between || "") +
                      '("(?:[^"\\\\]|\\\\.)*")');
  }

  function extractItemFields(html) {
    const fields = {};
    const keys = [];
    const grab = (name, re, apply) => {
      try {
        const m = html.match(re);
        if (!m) return;
        apply(m);
        keys.push(name);
      } catch (e) {
        keys.push(name + "!parse");
      }
    };
    grab("redacted_description",
         /"redacted_description"\s*:\s*\{[^{}]*?"text"\s*:\s*("(?:[^"\\]|\\.)*")/,
         (m) => { fields.description = JSON.parse(m[1]).slice(0, ENRICH_DESC_CAP); });
    grab("vehicle_odometer_data",
         /"vehicle_odometer_data"\s*:\s*(\{[^{}]*\})/,
         (m) => {
           const o = JSON.parse(m[1]);
           if (o && o.value !== undefined) {
             fields.odo = { unit: String(o.unit || ""), value: o.value };
           } else { throw new Error("shape"); }
         });
    grab("marketplace_listing_title", jsonStrRe("marketplace_listing_title"),
         (m) => { fields.fb_title = JSON.parse(m[1]).slice(0, 300); });
    grab("listing_price",
         /"listing_price"\s*:\s*\{[^{}]*?"(?:amount|formatted_amount)"\s*:\s*"?([\d.,]+)"?/,
         (m) => { fields.price_text_item = m[1]; });
    const extra = {};
    grab("vehicle_transmission_type", jsonStrRe("vehicle_transmission_type"),
         (m) => { extra.transmission = JSON.parse(m[1]).slice(0, 40); });
    grab("vehicle_seller_type", jsonStrRe("vehicle_seller_type"),
         (m) => { extra.seller_type = JSON.parse(m[1]).slice(0, 40); });
    grab("vehicle_is_paid_off", /"vehicle_is_paid_off"\s*:\s*(true|false)/,
         (m) => { extra.paid_off = m[1] === "true"; });
    if (Object.keys(extra).length) fields.enrich_extra = extra;
    return { fields: fields, keys: keys.slice(0, 10) };
  }

  // FB writes this apostrophe four different ways depending on where the
  // string comes from: a curly U+2019 in rendered UI text, HTML entities in
  // server markup, and a ' escape inside embedded JSON (which is how
  // most page strings ship). Matching only the ASCII form meant sold
  // listings fell through to "no_keys" — the signature reserved for "FB
  // renamed their JSON keys" — so ordinary listing churn faked that alarm.
  const APOS = "(?:['’ʼ]|&#0?39;|&#x27;|&apos;|\\\\u0027)?";
  const UNAVAILABLE_RE = new RegExp(
    "content isn" + APOS + "t available|isn" + APOS + "t available right now", "i");

  function isUnavailablePage(status, html) {
    // A sold/deleted listing, NOT a block — these vanish within minutes in a
    // hot market and must never trip the 6h disable.
    if (status === 404) return true;
    return UNAVAILABLE_RE.test(html || "");
  }

  function detectBlocked(status, finalUrl, html) {
    if (status === 429) return "rate-limit";
    if (status === 401 || status === 403) return "blocked";
    const u = String(finalUrl || "");
    if (/\/(login|checkpoint|recover)\b/.test(u)) {
      return u.includes("checkpoint") ? "checkpoint" : "login";
    }
    const h = (html || "").slice(0, 50000);
    if (/name="pass"/.test(h) && (/name="email"/.test(h) || /login_form/.test(h))) {
      return "login";
    }
    // Deliberately NO bare "security check" text match: page prose (a
    // description saying a car "passed the security check") must never buy
    // a 6h disable. The URL and form-action markers carry the detection; a
    // missed soft-block only shows up as failed extractions, which is safe.
    if (/action="[^"]*\/checkpoint\//.test(h)) {
      return "checkpoint";
    }
    return null;
  }

  let enrichBusy = false;
  let enrichTimer = null;
  let enrichQueuedThisTick = 0;   // reset in tick()

  // A true rolling hour, not a clock-hour bucket. A fixed bucket reset at the
  // top of each hour, so 30 dispatches at :59 plus 30 at :00 put 60 item-page
  // fetches inside two minutes — double the rate the cap exists to enforce,
  // and burst rate is exactly the bot signature the caps are here to avoid.
  function enrichRecentDispatches() {
    const arr = GM_getValue("enrich_dispatch_log", []);
    if (!Array.isArray(arr)) return [];
    const cutoff = Date.now() - 3600000;
    return arr.filter((t) => typeof t === "number" && t > cutoff);
  }

  function enrichHourBudgetLeft() {
    return ENRICH_HOURLY_CAP - enrichRecentDispatches().length;
  }

  function enrichCountDispatch() {
    const arr = enrichRecentDispatches();
    arr.push(Date.now());
    // Bounded so a stuck clock or a burst can't grow the stored log forever.
    GM_setValue("enrich_dispatch_log", arr.slice(-ENRICH_HOURLY_CAP * 4));
  }

  function maybeEnrich(payload) {
    if (!ENRICH_ENABLED) {
      bufferListing(payload);  // wire format byte-identical to v0.3
      return;
    }
    const queue = GM_getValue("enrich_queue", []);
    const disabled = GM_getValue("enrich_disabled_until", 0) > Date.now();
    if (disabled || enrichHourBudgetLeft() <= 0
        || enrichQueuedThisTick >= ENRICH_PER_TICK_MAX
        || queue.length >= ENRICH_QUEUE_CAP) {
      bufferListing(Object.assign({}, payload, {
        enrich_status: "skipped",
        enrich_err: disabled ? "disabled" : "capped",
      }));
      return;
    }
    enrichQueuedThisTick += 1;
    queue.push({ payload: payload, tries: 0, queued_at: Date.now(),
                 seed: !!payload.seed });
    GM_setValue("enrich_queue", queue);
    kickEnrichWorker();
  }

  function kickEnrichWorker() {
    if (enrichBusy || enrichTimer) return;
    processEnrichQueue();
  }

  function shipBare(entry, err) {
    bufferListing(Object.assign({}, entry.payload, {
      enrich_status: err === "skipped" ? "skipped" : "failed",
      enrich_err: err,
    }));
  }

  function removeFromQueue(entry) {
    const queue = GM_getValue("enrich_queue", []);
    const i = queue.findIndex((e) => e.payload && e.payload.id === entry.payload.id);
    if (i >= 0) queue.splice(i, 1);
    GM_setValue("enrich_queue", queue);
  }

  function processEnrichQueue() {
    const queue = GM_getValue("enrich_queue", []);
    if (queue.length === 0) return;
    if (GM_getValue("enrich_disabled_until", 0) > Date.now()) {
      // Listings must never wait out the disable window to reach the droplet.
      for (const e of queue) shipBare(e, "disabled");
      GM_setValue("enrich_queue", []);
      return;
    }
    let entry = queue.find((e) => !e.seed);
    if (!entry && ENRICH_SEED) {
      // Seed comps drip only when idle: one per gap, and only while the
      // hourly counter keeps half its headroom for fresh listings.
      const okPace = Date.now() - GM_getValue("enrich_seed_last_at", 0)
                     >= ENRICH_SEED_GAP_MS;
      if (okPace && enrichHourBudgetLeft() > ENRICH_HOURLY_CAP / 2) {
        entry = queue.find((e) => e.seed);
        if (entry) GM_setValue("enrich_seed_last_at", Date.now());
      }
    }
    if (!entry) {
      // Only seed entries and not their turn yet. The chain must re-arm
      // itself: fetch completions are the usual driver, and a queue of
      // pure seeds would otherwise stall until the next new listing or
      // rotation reload — killing the drip on quiet searches.
      if (queue.some((e) => e.seed) && !enrichTimer) {
        enrichTimer = true;
        setTimeout(() => { enrichTimer = null; processEnrichQueue(); },
                   ENRICH_SEED_GAP_MS + 1000);
      }
      return;
    }
    if (!entry.payload || !entry.payload.url) {
      removeFromQueue(entry);  // malformed queue entry: never fetch undefined
      if (entry.payload) shipBare(entry, "stale");
      return processEnrichQueue();
    }
    if (enrichHourBudgetLeft() <= 0) {
      if (!entry.seed) {
        removeFromQueue(entry);
        shipBare(entry, "skipped");
        return processEnrichQueue();  // next entry may be a waitable seed
      }
      if (!enrichTimer) {  // seeds wait for the hour to roll — re-arm
        enrichTimer = true;
        setTimeout(() => { enrichTimer = null; processEnrichQueue(); },
                   ENRICH_SEED_GAP_MS + 1000);
      }
      return;
    }
    // tries increments and persists BEFORE dispatch: if the rotation reload
    // kills the in-flight fetch, the startup sweep sees tries>=1 and ships
    // the listing bare — an enrichment can delay a listing, never lose it.
    entry.tries += 1;
    GM_setValue("enrich_queue", queue);
    enrichCountDispatch();
    enrichBusy = true;
    GM_xmlhttpRequest({
      method: "GET",
      url: entry.payload.url,
      timeout: ENRICH_TIMEOUT_MS,
      onload: (resp) => finishEnrich(entry, resp),
      onerror: () => finishEnrich(entry, null, "network"),
      ontimeout: () => finishEnrich(entry, null, "timeout"),
    });
  }

  function finishEnrich(entry, resp, errKind) {
    removeFromQueue(entry);
    if (!resp) {
      shipBare(entry, errKind || "network");
    } else {
      // Block detection MUST run first: FB's generic "content isn't
      // available" copy is also its permission-denied text, so a login or
      // checkpoint wall carrying that string used to be downgraded to a
      // benign sold-listing and the 6h disable never armed — leaving the
      // script hammering a login wall, the exact account-flag scenario the
      // disable exists to prevent. A real 404/sold page trips none of
      // detectBlocked's status, URL, or login-form markers, so it still
      // falls through to "unavailable" below.
      const blocked = detectBlocked(resp.status, resp.finalUrl, resp.responseText);
      if (blocked) {
        GM_setValue("enrich_disabled_until", Date.now() + ENRICH_DISABLE_MS);
        const lastWarn = GM_getValue("enrich_block_warned_at", 0);
        if (Date.now() - lastWarn > WARN_COOLDOWN_MS) {
          GM_setValue("enrich_block_warned_at", Date.now());
          telegramSend("WARNING [facebook] item-page fetch hit a " + blocked +
                       " wall — enrichment disabled 6h. This can be an " +
                       "account-flag signal; ease off if it repeats.");
        }
        console.warn("[car-watcher] enrichment blocked:", blocked);
        shipBare(entry, "blocked:" + blocked);
        const rest = GM_getValue("enrich_queue", []);
        for (const e of rest) shipBare(e, "disabled");
        GM_setValue("enrich_queue", []);
      } else if (isUnavailablePage(resp.status, resp.responseText)) {
        shipBare(entry, "unavailable");  // sold/deleted — expected, not a block
      } else if (resp.status !== 200) {
        shipBare(entry, "http_" + resp.status);
      } else {
        const out = extractItemFields(resp.responseText || "");
        const enriched = Object.assign({}, entry.payload, out.fields, {
          enrich_keys: out.keys,
          enrich_status: (out.fields.description && out.fields.odo) ? "ok"
            : (out.keys.length ? "partial" : "failed"),
        });
        if (!out.keys.length) enriched.enrich_err = "no_keys";
        bufferListing(enriched);
      }
    }
    enrichBusy = false;
    const delay = ENRICH_GAP_MIN_MS
      + Math.random() * (ENRICH_GAP_MAX_MS - ENRICH_GAP_MIN_MS);
    // Flag set BEFORE scheduling (we never cancel, so no id needed): the
    // assign-after-setTimeout pattern leaves a stale truthy id if a timer
    // ever fires synchronously, wedging the worker.
    enrichTimer = true;
    setTimeout(() => {
      enrichTimer = null;
      processEnrichQueue();
    }, delay);
  }

  function sweepEnrichQueue() {
    // Startup recovery: anything a previous page-life dispatched (tries>=1)
    // or left waiting too long ships bare now — hard latency bound ~3 min.
    const queue = GM_getValue("enrich_queue", []);
    if (queue.length === 0) return;
    const keep = [];
    for (const e of queue) {
      if (e.tries >= 1) shipBare(e, "reload");
      else if (Date.now() - (e.queued_at || 0) > ENRICH_QUEUE_MAX_AGE_MS) {
        shipBare(e, "stale");
      } else keep.push(e);
    }
    GM_setValue("enrich_queue", keep);
    if (keep.length) kickEnrichWorker();
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
    // An empty batch is a heartbeat: without it the droplet can't tell a
    // quiet Marketplace from a closed tab, and would cry breakage on a slow
    // afternoon. Sent at most once per INGEST_HEARTBEAT_MS.
    if (buf.length === 0) {
      if (Date.now() - GM_getValue("last_ingest_at", 0) < INGEST_HEARTBEAT_MS) return;
    }
    const n = Math.min(buf.length, INGEST_BATCH_MAX);
    ingestInFlight = true;
    const done = (ok, why) => {
      ingestInFlight = false;
      if (ok) {
        GM_setValue("last_ingest_at", Date.now());
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

  // Banners are keyed by which alarm raised them. A single shared element
  // meant whoever cleared it last won: checkSortGuard's setBanner(false) on
  // every healthy tick wiped the "scraper may be blind" banner one tick
  // after it appeared, and the emptyWarned latch stopped it ever coming
  // back — so the tab looked fine while the scraper was blind. Each alarm
  // now owns its own reason and can only clear its own.
  const bannerReasons = new Map();

  function renderBanner() {
    const existing = document.getElementById("car-watcher-warning");
    if (!bannerReasons.size) {
      if (existing) existing.remove();
      return;
    }
    const banner = existing || document.createElement("div");
    banner.id = "car-watcher-warning";
    banner.textContent = Array.from(bannerReasons.values()).join(" · ");
    banner.style.cssText =
      "position:fixed;top:0;left:0;right:0;z-index:99999;background:#c0392b;" +
      "color:#fff;font:14px sans-serif;padding:6px;text-align:center;";
    if (!existing) document.body.appendChild(banner);
  }

  function setBanner(show, text, key) {
    const k = key || "generic";
    if (show) bannerReasons.set(k, text);
    else bannerReasons.delete(k);
    renderBanner();
  }

  // A tab that stops matching a configured search has stopped watching, and
  // silence is indistinguishable from "no new listings" — so say so loudly
  // rather than no-op'ing. Only fires when we *expected* to be watching:
  // ordinary browsing on some other Marketplace search stays quiet.
  let lostWarned = false;
  let wasActive = false;
  let emptyTicks = 0;
  let emptyWarned = false;

  // Seeding must not latch on the first non-empty tick: FB lazy-renders the
  // grid, so tick 1 routinely sees a partial page. Latching there marked the
  // rest of that same pre-existing inventory as brand new on tick 2 — the
  // install-time Telegram flood the seed pass exists to prevent. Hold the
  // flag until the visible count stops growing (or the cap is hit).
  const SEED_STABLE_TICKS = 2;
  const SEED_MAX_TICKS = 15;      // ~5 min at a 20 s tick; then latch anyway
  let seedPrevCount = -1;
  let seedStableTicks = 0;
  let seedTicks = 0;
  let seedLabel = null;           // which search the counters above describe

  function warnScraperBlind() {
    if (emptyWarned) return;
    emptyWarned = true;
    console.warn("[car-watcher] no listing anchors found — FB markup changed?");
    setBanner(true, "car-watcher: no listings found on this page — scraper may be blind", "blind");
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
    setBanner(true, "car-watcher: " + reason + " — alerts suspended", "lost");
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
      setBanner(false, null, "sort");
      return true;
    }
    console.warn("[car-watcher] sortBy=creation_time_descend missing from URL");
    setBanner(true, "car-watcher: this page is NOT sorted by newest — alerts suspended", "sort");
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
    // Back on a configured search after an SPA drift: retract the banner and
    // re-arm the warning. Without this the red "alerts suspended" banner
    // outlived the condition it described, since nothing else clears "lost".
    if (lostWarned) {
      lostWarned = false;
      setBanner(false, null, "lost");
      console.log("[car-watcher] back on a configured search — alerting resumed");
    }
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
      setBanner(false, null, "blind");
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

    enrichQueuedThisTick = 0;
    for (const l of listings) {
      if (seenSet.has(l.id)) continue;
      seenSet.add(l.id);
      seenArr.push(l.id);
      dirty = true;
      // The droplet decides what's alert-worthy; seed-pass items are flagged
      // so it stores them as comp data without ever alerting. New listings
      // detour through item-page enrichment (bounded seconds, never lost).
      maybeEnrich({
        id: l.id, url: l.url, price_text: l.price, text: l.text,
        label: search.label, seen_at: new Date().toISOString(), seed: seeding,
      });
    }
    if (dirty) saveSeen(seenArr);
    if (seeding) {
      // An SPA navigation between two configured searches keeps these
      // page-local counters alive, so the previous label's tick count and
      // high-water mark would latch the NEW label on its very first,
      // partial tick — the exact behavior this fix exists to remove.
      if (seedLabel !== search.label) {
        seedLabel = search.label;
        seedPrevCount = -1;
        seedStableTicks = 0;
        seedTicks = 0;
      }
      seedTicks += 1;
      // Any CHANGE resets the stability counter, not just growth. Comparing
      // against a high-water mark treated a shrink as stability, but a
      // shrink means the grid is still churning (FB re-renders and unmounts
      // rows as results stream in) — the least safe moment to latch.
      // SEED_MAX_TICKS still bounds a grid that oscillates forever.
      if (listings.length !== seedPrevCount) {
        seedPrevCount = listings.length;   // grid still settling
        seedStableTicks = 0;
      } else {
        seedStableTicks += 1;
      }
      if (seedStableTicks >= SEED_STABLE_TICKS || seedTicks >= SEED_MAX_TICKS) {
        seeded[search.label] = true;
        GM_setValue("seeded_labels", seeded);
        console.log("[car-watcher] seeded", search.label, "with",
                    listings.length, "listings after", seedTicks, "ticks");
      }
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
      // didn't — FB redirected to the marketplace home, a login wall, a
      // checkpoint, or just changed its URL format — the watcher is now
      // blind and must recover on its own. onSearchPath() must NOT gate
      // this: a login/checkpoint/home redirect isn't a search path at all,
      // and that's exactly the landing that must not go silent — without
      // this branch firing, no further rotation is ever scheduled and the
      // tab sits dead until a human notices.
      const expected = GM_getValue("rot_expect", "");
      if (expected) {
        GM_setValue("rot_expect", "");
        warnLostSearch("rotation into '" + expected + "' landed on " +
                        (onSearchPath() ? "an unrecognized search URL"
                                        : "a non-search page (login/checkpoint/redirect?)"));
        // Retry once after a delay instead of parking here forever: a
        // transient FB redirect self-heals; a genuine login/checkpoint wall
        // just keeps re-warning (rate-limited by WARN_COOLDOWN_MS inside
        // warnLostSearch) until a human logs back in.
        const idx = GM_getValue("rot_idx", 0);
        setTimeout(() => {
          GM_setValue("rot_expect", SEARCH_URLS[idx].label);
          location.href = SEARCH_URLS[idx].url;
        }, LOST_SEARCH_RETRY_MS);
      }
      return;
    }
    GM_setValue("rot_expect", "");
    console.log("[car-watcher] active on", search.label);
    sweepEnrichQueue();
    tick();
    setInterval(tick, SCRAPE_INTERVAL_MS);
    setInterval(flushIngest, INGEST_FLUSH_MS);
    scheduleRotateReload();
  }

  main();
})();
