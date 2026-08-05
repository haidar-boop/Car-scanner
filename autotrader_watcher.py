#!/usr/bin/env python3
"""AutoTrader.ca watcher — polls Edmonton searches and Telegram-notifies new listings.

Step 1 of the Edmonton car deal scanner: locked search targets, baseline
fetch -> parse -> store -> notify loop. Listings accumulate in SQLite from
day one so the Step 2 scorer has comp data to fit against.

AutoTrader.ca runs on an AutoScout24-based stack. Search results pages embed
a <script id="__NEXT_DATA__"> JSON blob whose props.pageProps.listings holds
structured listing objects — we parse that, never the HTML. The server echoes
the query params it actually applied in pageProps.pagePath, which lets
verify_search_state() catch the next silent URL-format migration (the old
/cars/ab/edmonton/?srt=... format already 301s and drops the sort param).
"""

import json
import os
import random
import re
import sqlite3
import sys
import time
import urllib.parse
from datetime import datetime, timezone

import requests

# --- CONFIG -----------------------------------------------------------------

# All URLs must carry sort=age&desc=1 ("Posted Date: New to Old") — both
# parsers only read page one, so any other sort silently breaks the system.
# body codes: Hatchback=1 Convertible=2 Coupe=3 Wagon=5 Sedan=6 Others=7
# Minivan=12 SUV=14 Pick-up=15. Invalid codes return 0 results, not an error.
SEARCH_URLS = [
    {
        "label": "cars_2k_15k",
        "url": "https://www.autotrader.ca/cars/reg_ab/cit_edmonton/ot_used"
               "?pricefrom=2000&priceto=15000&sort=age&desc=1&zipr=100",
    },
    {
        "label": "cars_15k_35k",
        "url": "https://www.autotrader.ca/cars/reg_ab/cit_edmonton/ot_used"
               "?pricefrom=15000&priceto=35000&sort=age&desc=1&zipr=100",
    },
    {
        "label": "trucks_suvs_5k_30k",
        "url": "https://www.autotrader.ca/cars/reg_ab/cit_edmonton/ot_used"
               "?pricefrom=5000&priceto=30000&body=14,15&sort=age&desc=1&zipr=100",
    },
]

POLL_INTERVAL_BASE_S = 300          # 5 min base ...
POLL_JITTER_S = 240                 # ... + uniform(0, 240) -> 5-9 min per cycle
BETWEEN_SEARCHES_S = (2.0, 5.0)     # random pause between the 3 fetches
FETCH_TIMEOUT_S = 25
FETCH_RETRIES = 3
FETCH_BACKOFF_S = 5.0               # doubled per retry
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

DB_PATH = os.environ.get(
    "CAR_SCANNER_DB",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "listings.db"),
)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

VERIFY_WARN_COOLDOWN_S = 6 * 3600   # max one search-health warning per label per 6h

# ----------------------------------------------------------------------------

NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S
)
TAG_RE = re.compile(r"<[^>]+>")


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg):
    print("[%s] %s" % (utc_now_iso(), msg), file=sys.stderr, flush=True)


def fetch_page(url):
    """GET a search page. Returns HTML text or None; never raises."""
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "en-CA,en;q=0.9"}
    backoff = FETCH_BACKOFF_S
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=FETCH_TIMEOUT_S)
            if resp.status_code == 200:
                return resp.text
            log("fetch attempt %d: HTTP %d for %s" % (attempt, resp.status_code, url))
        except requests.RequestException as e:
            log("fetch attempt %d failed: %s" % (attempt, e))
        if attempt < FETCH_RETRIES:
            time.sleep(backoff)
            backoff *= 2
    return None


def extract_next_data(html):
    """Pull props.pageProps out of the embedded __NEXT_DATA__ JSON, or None."""
    m = NEXT_DATA_RE.search(html)
    raw = m.group(1) if m else None
    if raw is None:
        try:
            from bs4 import BeautifulSoup
            tag = BeautifulSoup(html, "html.parser").find("script", id="__NEXT_DATA__")
            raw = tag.string if tag else None
        except Exception as e:
            log("bs4 fallback failed: %s" % e)
    if not raw:
        return None
    try:
        return json.loads(raw)["props"]["pageProps"]
    except (ValueError, KeyError, TypeError) as e:
        log("__NEXT_DATA__ parse failed: %s" % e)
        return None


def verify_search_state(page_props, expected_url, parsed_count):
    """Check the server applied our search exactly. Returns list of problems.

    pageProps.pagePath echoes the applied query (with percent-encoded values,
    so compare parsed dicts, never substrings). A dropped sort=age here is
    exactly how the last silent URL-format migration manifested.
    """
    problems = []
    expected_q = urllib.parse.parse_qs(urllib.parse.urlsplit(expected_url).query)
    page_path = page_props.get("pagePath") or ""
    actual_q = urllib.parse.parse_qs(urllib.parse.urlsplit(page_path).query)
    for key, want in expected_q.items():
        got = actual_q.get(key)
        if got != want:
            problems.append("param %r applied as %r, wanted %r" % (key, got, want))

    n_results = page_props.get("numberOfResults")
    if not n_results:
        problems.append("numberOfResults=%r" % n_results)
    listings = page_props.get("listings") or []
    if not listings:
        problems.append("no listings on page one")
    if parsed_count is not None and listings:
        if parsed_count < len(listings) / 2:
            problems.append(
                "only %d/%d listings parsed — page shape drifted?"
                % (parsed_count, len(listings))
            )

    lo = int(expected_q.get("pricefrom", ["0"])[0])
    hi = int(expected_q.get("priceto", ["10000000"])[0])
    out_of_band = 0
    for l in listings:
        price = (l.get("price") or {}).get("priceRaw")
        if isinstance(price, int) and not (lo <= price <= hi):
            out_of_band += 1
    if out_of_band > 2:  # a couple of boosted rows can stray; more means broken filter
        problems.append("%d listings priced outside $%d-$%d" % (out_of_band, lo, hi))
    return problems


def parse_km(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return int(s)
    digits = re.sub(r"[^\d]", "", str(s))
    return int(digits) if digits else None


def strip_html(s):
    if not isinstance(s, str):
        return None
    return TAG_RE.sub(" ", s).strip() or None


def parse_listings(page_props, search_label):
    """Turn pageProps.listings into rows keyed to the DB columns.

    Every access is defensive: one malformed listing is skipped and logged,
    never allowed to kill the batch.
    """
    rows = []
    for l in page_props.get("listings") or []:
        try:
            listing_id = l.get("id")
            price = (l.get("price") or {}).get("priceRaw")
            if not listing_id or not isinstance(price, int):
                continue
            vehicle = l.get("vehicle") or {}
            seller = l.get("seller") or {}
            location = l.get("location") or {}
            year = vehicle.get("modelYear")
            make = vehicle.get("make")
            model = vehicle.get("model")
            rows.append({
                "id": str(listing_id),
                "source": "autotrader",
                "url": l.get("url"),
                "title": " ".join(str(x) for x in (year, make, model) if x),
                "price": price,
                "year": year,
                "make": make,
                "model": model,
                "km": parse_km(vehicle.get("mileageInKm")),
                "seller_type": seller.get("type"),
                "city": location.get("city"),
                "distance_km": location.get("distanceToSearchLocationInKm"),
                "description": strip_html(l.get("description")),
                "is_damaged": 1 if vehicle.get("isCurrentlyDamaged") else 0,
                "result_type": l.get("searchResultType"),
                "price_label": (l.get("tracking") or {}).get("priceLabel"),
                "search_label": search_label,
                "raw_json": json.dumps(l),
            })
        except Exception as e:
            log("skipping malformed listing: %s" % e)
    return rows


DDL = """
CREATE TABLE IF NOT EXISTS listings (
    id            TEXT NOT NULL,
    source        TEXT NOT NULL,
    url           TEXT,
    title         TEXT,
    price         INTEGER,
    year          INTEGER,
    make          TEXT,
    model         TEXT,
    km            INTEGER,
    seller_type   TEXT,
    city          TEXT,
    distance_km   INTEGER,
    description   TEXT,
    is_damaged    INTEGER,
    result_type   TEXT,
    price_label   TEXT,
    search_label  TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at  TEXT NOT NULL,
    raw_json      TEXT,
    PRIMARY KEY (source, id)
);
CREATE INDEX IF NOT EXISTS idx_listings_comp ON listings(make, model, year);
CREATE INDEX IF NOT EXISTS idx_listings_seen ON listings(first_seen_at);
CREATE TABLE IF NOT EXISTS price_history (
    source  TEXT NOT NULL,
    id      TEXT NOT NULL,
    price   INTEGER,
    seen_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def init_db(path):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(DDL)
    conn.commit()
    return conn


def store_listings(conn, rows):
    """Upsert rows; returns the subset that are brand new (never seen before).

    Every observed price lands in price_history (including the first), so the
    original asking price survives later in-place updates of listings.price.
    """
    now = utc_now_iso()
    new_rows = []
    with conn:
        for r in rows:
            cur = conn.execute(
                "SELECT price FROM listings WHERE source=? AND id=?",
                (r["source"], r["id"]),
            )
            existing = cur.fetchone()
            if existing is None:
                conn.execute(
                    """INSERT INTO listings
                       (id, source, url, title, price, year, make, model, km,
                        seller_type, city, distance_km, description, is_damaged,
                        result_type, price_label, search_label,
                        first_seen_at, last_seen_at, raw_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (r["id"], r["source"], r["url"], r["title"], r["price"],
                     r["year"], r["make"], r["model"], r["km"], r["seller_type"],
                     r["city"], r["distance_km"], r["description"], r["is_damaged"],
                     r["result_type"], r["price_label"], r["search_label"],
                     now, now, r["raw_json"]),
                )
                conn.execute(
                    "INSERT INTO price_history (source, id, price, seen_at)"
                    " VALUES (?,?,?,?)",
                    (r["source"], r["id"], r["price"], now),
                )
                new_rows.append(r)
            else:
                if existing[0] != r["price"]:
                    conn.execute(
                        "INSERT INTO price_history (source, id, price, seen_at)"
                        " VALUES (?,?,?,?)",
                        (r["source"], r["id"], r["price"], now),
                    )
                conn.execute(
                    "UPDATE listings SET last_seen_at=?, price=?, raw_json=?"
                    " WHERE source=? AND id=?",
                    (now, r["price"], r["raw_json"], r["source"], r["id"]),
                )
    return new_rows


def meta_get(conn, key):
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def meta_set(conn, key, value):
    with conn:
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?,?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


def format_new_listing(r):
    # Placeholder format; Step 3 rebuilds this around deal scores.
    price = "$%s" % format(r["price"], ",")
    km = "%s km" % format(r["km"], ",") if r["km"] is not None else "km n/a"
    return "NEW [autotrader/%s] %s — %s — %s — %s, %s\n%s" % (
        r["search_label"], r["title"] or "(no title)", price, km,
        r["seller_type"] or "seller n/a", r["city"] or "city n/a", r["url"] or "",
    )


def telegram_send(text):
    """Send a message. Returns True on success; never raises, never kills the loop."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("telegram disabled (env vars not set); would send: %.120s" % text)
        return False
    try:
        resp = requests.post(
            "https://api.telegram.org/bot%s/sendMessage" % TELEGRAM_BOT_TOKEN,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            log("telegram HTTP %d: %.200s" % (resp.status_code, resp.text))
            return False
        return True
    except requests.RequestException as e:
        log("telegram send failed: %s" % e)
        return False


def warn_search_broken(conn, label, problems):
    """Rate-limited (per label, 6h) Telegram warning that a search looks broken."""
    key = "verify_warned_at:%s" % label
    last = meta_get(conn, key)
    if last:
        try:
            last_dt = datetime.strptime(last, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            if (datetime.now(timezone.utc) - last_dt).total_seconds() < VERIFY_WARN_COOLDOWN_S:
                return
        except ValueError:
            pass
    sent = telegram_send(
        "WARNING [autotrader/%s] search looks broken; notifications suppressed "
        "this cycle:\n- %s" % (label, "\n- ".join(problems))
    )
    if sent:  # a failed send must not consume the cooldown — retry next cycle
        meta_set(conn, key, utc_now_iso())


def poll_once(conn, notifications_enabled):
    """One pass over all searches. Returns {label: parsed_count} for the pass.

    Each search body is exception-isolated so a transient DB error on one
    search can't skip the others.
    """
    parsed_counts = {}
    for i, search in enumerate(SEARCH_URLS):
        if i > 0:
            time.sleep(random.uniform(*BETWEEN_SEARCHES_S))
        label, url = search["label"], search["url"]
        try:
            html = fetch_page(url)
            if html is None:
                log("[%s] fetch failed after retries" % label)
                continue
            page_props = extract_next_data(html)
            if page_props is None:
                log("[%s] no __NEXT_DATA__ found — page structure changed?" % label)
                warn_search_broken(conn, label, ["__NEXT_DATA__ missing from page"])
                continue
            rows = parse_listings(page_props, label)
            problems = verify_search_state(page_props, url, parsed_count=len(rows))
            new_rows = store_listings(conn, rows)  # comp data is comp data — always store
            if problems:
                log("[%s] search-state problems: %s" % (label, "; ".join(problems)))
                warn_search_broken(conn, label, problems)
                continue  # precision over recall: no per-listing pings off a sick search
            parsed_counts[label] = len(rows)
            log("[%s] parsed %d listings (%d new) of %s total"
                % (label, len(rows), len(new_rows), page_props.get("numberOfResults")))
            if notifications_enabled:
                for r in new_rows:
                    telegram_send(format_new_listing(r))
                    time.sleep(1.0)  # stay under Telegram rate limits
        except Exception as e:
            log("[%s] search failed: %s" % (label, e))
    return parsed_counts


def main():
    log("starting autotrader watcher; db=%s" % DB_PATH)
    conn = init_db(DB_PATH)
    # Seed mode: notifications stay off until one full pass has parsed every
    # search cleanly, recorded via an explicit meta flag. Inferring seed state
    # from row count would flood Telegram after a partial seed + restart, or
    # after a first boot where the network wasn't up yet.
    while meta_get(conn, "seeded") != "1":
        try:
            counts = poll_once(conn, notifications_enabled=False)
            if all(counts.get(s["label"]) for s in SEARCH_URLS):
                meta_set(conn, "seeded", "1")
                log("seeded %d listings — notifications enabled"
                    % conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0])
                break
            log("seed pass incomplete (%s) — retrying next cycle" % (counts,))
        except Exception as e:
            log("seed pass failed: %s" % e)
        time.sleep(POLL_INTERVAL_BASE_S + random.uniform(0, POLL_JITTER_S))
    while True:
        sleep_s = POLL_INTERVAL_BASE_S + random.uniform(0, POLL_JITTER_S)
        log("sleeping %.0fs" % sleep_s)
        time.sleep(sleep_s)
        try:
            poll_once(conn, notifications_enabled=True)
        except Exception as e:
            log("poll cycle failed: %s" % e)


if __name__ == "__main__":
    main()
