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

import argparse
import hmac
import json
import math
import os
import random
import re
import sqlite3
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

import scoring

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("America/Edmonton")
except Exception:  # stripped-down images without tzdata
    TZ = timezone(timedelta(hours=-7))

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

# FB->droplet ingest bridge. Empty token = server disabled (never runs open).
INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "")
INGEST_BIND = os.environ.get("INGEST_BIND", "0.0.0.0:8477")
INGEST_MAX_BODY = 256 * 1024
INGEST_MAX_ITEMS = 200

LIFESPAN_CHECK_INTERVAL_S = 6 * 3600   # re-check each scored listing this often
LIFESPAN_CHECK_CAP = 20                # max direct re-fetches per poll cycle
LIFESPAN_MAX_DAYS = 7                  # stop checking once a listing is this old
REFIT_HOUR_LOCAL = 3                   # nightly model refit (America/Edmonton)
DIGEST_HOUR_LOCAL = 20                 # shadow digest at 8 PM local
WEEKLY_REPORT_DOW = 6                  # Sunday
WEEKLY_REPORT_HOUR_LOCAL = 18

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


def open_db(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")  # ingest thread + CLI write too
    return conn


def init_db(path):
    conn = open_db(path)
    conn.executescript(DDL)
    conn.commit()
    return conn


STEP2_COLUMNS = {
    "family": "TEXT",                    # 'FORD:F150' cross-source family key
    "fingerprint": "TEXT",               # family|year|km/5000|price/250
    "reject_reason": "TEXT",             # NULL = passed the rejection layer
    "z": "REAL",
    "pct_below": "REAL",
    "scored_at": "TEXT",
    "disappeared_at": "TEXT",
    "last_checked_at": "TEXT",
    "gone_suspected_at": "TEXT",         # first strike of the 2-check gone test
    "km_converted_from_miles": "INTEGER",
}

STEP2_DDL = """
CREATE INDEX IF NOT EXISTS idx_listings_family ON listings(family);
CREATE INDEX IF NOT EXISTS idx_listings_fp     ON listings(fingerprint);

CREATE TABLE IF NOT EXISTS models (
    model_key  TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    b0 REAL NOT NULL, b1 REAL NOT NULL, b2 REAL NOT NULL,
    mad        REAL NOT NULL,
    comp_count INTEGER NOT NULL,
    km_min INTEGER, km_max INTEGER,
    age_min REAL,  age_max REAL,
    method     TEXT NOT NULL,
    fitted_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,
    listing_id      TEXT NOT NULL,
    fired_at        TEXT NOT NULL,
    z               REAL NOT NULL,
    pct_below       REAL NOT NULL,
    price           INTEGER NOT NULL,
    predicted_price INTEGER NOT NULL,
    model_key       TEXT NOT NULL,
    b0 REAL, b1 REAL, b2 REAL, mad REAL, comp_count INTEGER,
    is_dealer       INTEGER NOT NULL DEFAULT 0,
    low_confidence  INTEGER NOT NULL DEFAULT 0,
    shadow          INTEGER NOT NULL DEFAULT 0,
    label           TEXT CHECK (label IN ('good','bad','scam','already_gone')),
    labeled_at      TEXT,
    UNIQUE (source, listing_id)
);
CREATE INDEX IF NOT EXISTS idx_alerts_fired ON alerts(fired_at);
"""


def migrate_db(conn):
    """Idempotent Step 2 migration: new columns, new tables, backfill.

    The backfill runs the full rejection layer over pre-Step-2 rows so old
    salvage/damaged listings are tagged out of the comp pool retroactively.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(listings)")}
    with conn:
        for name, decl in STEP2_COLUMNS.items():
            if name not in cols:
                conn.execute("ALTER TABLE listings ADD COLUMN %s %s" % (name, decl))
    conn.executescript(STEP2_DDL)
    conn.commit()

    todo = conn.execute(
        "SELECT * FROM listings WHERE family IS NULL AND reject_reason IS NULL"
    ).fetchall()
    if todo:
        now_year = datetime.now(timezone.utc).year
        with conn:
            for r in todo:
                family, fp, reason = scoring.assess(dict(r), now_year)
                conn.execute(
                    "UPDATE listings SET family=?, fingerprint=?, reject_reason=?"
                    " WHERE source=? AND id=?",
                    (family, fp, reason, r["source"], r["id"]),
                )
        log("migration: backfilled %d rows" % len(todo))


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
                        first_seen_at, last_seen_at, raw_json,
                        km_converted_from_miles)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (r["id"], r["source"], r["url"], r["title"], r["price"],
                     r["year"], r["make"], r["model"], r["km"], r["seller_type"],
                     r["city"], r["distance_km"], r["description"], r["is_damaged"],
                     r["result_type"], r["price_label"], r["search_label"],
                     now, now, r["raw_json"], r.get("km_converted_from_miles")),
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
                    # A changed price must pass the rejection layer again —
                    # an edit to $111 would otherwise sit in the comp pool as
                    # a clean row forever. The stale score is cleared too.
                    family, fp, reason = scoring.assess(
                        r, datetime.now(timezone.utc).year)
                    conn.execute(
                        "UPDATE listings SET last_seen_at=?, price=?, raw_json=?,"
                        " family=?, fingerprint=?, reject_reason=?,"
                        " z=NULL, pct_below=NULL, scored_at=NULL"
                        " WHERE source=? AND id=?",
                        (now, r["price"], r["raw_json"], family, fp, reason,
                         r["source"], r["id"]),
                    )
                else:
                    conn.execute(
                        "UPDATE listings SET last_seen_at=?, raw_json=?"
                        " WHERE source=? AND id=?",
                        (now, r["raw_json"], r["source"], r["id"]),
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


def format_alert(row, score, predicted_price, shadow):
    # Minimal deal format; Step 3 rebuilds this properly.
    flags = ""
    if score["low_confidence"]:
        flags += " [LOW CONF]"
    if row.get("seller_type") == "Dealer":
        flags += " [DEALER]"
    if shadow:
        flags += " [SHADOW]"
    km = "%s km" % format(row["km"], ",") if row.get("km") is not None else "km n/a"
    return (
        "DEAL [%s/%s] z=%.1f | %d%% below predicted $%s (%d comps)%s\n"
        "%s — $%s — %s — %s, %s\n%s" % (
            row["source"], row["search_label"], score["z"], round(score["pct_below"]),
            format(predicted_price, ","), score["model"]["comp_count"], flags,
            row.get("title") or "(no title)", format(row["price"], ","), km,
            row.get("seller_type") or "seller n/a", row.get("city") or "city n/a",
            row.get("url") or "",
        )
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


# --- scoring pipeline -------------------------------------------------------

KNOWN = {"makes": set(scoring.STATIC_MAKES), "models_by_make": {}}


def get_model_for(conn, family, search_label):
    """Family model if it has enough comps, else segment model (low conf)."""
    row = conn.execute(
        "SELECT * FROM models WHERE model_key=?", ("family:%s" % family,)
    ).fetchone()
    if row and row["comp_count"] >= scoring.FAMILY_MODEL_MIN_COMPS:
        return dict(row), False
    segment = scoring.SEGMENT_MAP.get(search_label)
    if segment:
        row = conn.execute(
            "SELECT * FROM models WHERE model_key=?", ("segment:%s" % segment,)
        ).fetchone()
        if row:
            return dict(row), True
    return None


def score_listing(conn, row):
    picked = get_model_for(conn, row["family"], row.get("search_label"))
    if picked is None:
        return None
    model_row, low_confidence = picked
    age = scoring.age_of(row["year"])
    z, pct_below = scoring.robust_z(row["price"], model_row, age, row["km"])
    suppress = scoring.gate(model_row, age, row["km"], utc_now_iso(), model_row["kind"])
    return {"z": z, "pct_below": pct_below, "model": model_row,
            "low_confidence": low_confidence, "suppress": suppress}


def maybe_alert(conn, row, score):
    src, lid = row["source"], row["id"]
    if score["suppress"]:
        if score["z"] <= scoring.Z_ALERT.get(src, -2.0):
            log("[suppress] %s/%s z=%.2f gated: %s"
                % (src, lid, score["z"], ",".join(score["suppress"])))
        return
    threshold = (scoring.Z_ALERT_DEALER if row.get("seller_type") == "Dealer"
                 else scoring.Z_ALERT.get(src, -2.0))
    if score["z"] > threshold:
        return  # not a deal — the common case, not worth a log line
    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=scoring.FINGERPRINT_TTL_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    if row.get("fingerprint") and conn.execute(
        "SELECT 1 FROM listings WHERE fingerprint=? AND NOT (source=? AND id=?)"
        " AND first_seen_at >= ? LIMIT 1",
        (row["fingerprint"], src, lid, cutoff),
    ).fetchone():
        log("[suppress] %s/%s z=%.2f: repost/cross-post fingerprint match"
            % (src, lid, score["z"]))
        return
    m = score["model"]
    age = scoring.age_of(row["year"])
    predicted = int(round(math.exp(scoring.predict_log_price(m, age, row["km"]))))
    now = utc_now_iso()
    shadow_until = meta_get(conn, "shadow_until") or ""
    shadow = 1 if now < shadow_until else 0
    try:
        with conn:
            cur = conn.execute(
                """INSERT INTO alerts (source, listing_id, fired_at, z, pct_below,
                       price, predicted_price, model_key, b0, b1, b2, mad,
                       comp_count, is_dealer, low_confidence, shadow)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (src, lid, now, score["z"], score["pct_below"], row["price"],
                 predicted, m["model_key"], m["b0"], m["b1"], m["b2"], m["mad"],
                 m["comp_count"], 1 if row.get("seller_type") == "Dealer" else 0,
                 1 if score["low_confidence"] else 0, shadow),
            )
    except sqlite3.IntegrityError:
        log("[suppress] %s/%s: already alerted" % (src, lid))
        return
    log("[alert%s] #%d %s/%s z=%.2f %d%% below $%d predicted"
        % (" shadow" if shadow else "", cur.lastrowid, src, lid,
           score["z"], round(score["pct_below"]), predicted))
    if not shadow:
        telegram_send(format_alert(row, score, predicted, shadow))
        time.sleep(1.0)


def process_new_rows(conn, rows, allow_alerts):
    """Rejection layer -> score -> (maybe) alert, for freshly stored rows."""
    now_year = datetime.now(timezone.utc).year
    for r in rows:
        try:
            family, fp, reason = scoring.assess(r, now_year)
            with conn:
                conn.execute(
                    "UPDATE listings SET family=?, fingerprint=?, reject_reason=?"
                    " WHERE source=? AND id=?",
                    (family, fp, reason, r["source"], r["id"]),
                )
            if reason:
                log("[reject] %s/%s %s: %s" % (r["source"], r["id"], reason,
                                               (r.get("title") or "")[:60]))
                continue
            r = dict(r, family=family, fingerprint=fp)
            score = score_listing(conn, r)
            if score is None:
                continue  # no model yet for this family/segment; stored for comps
            with conn:
                conn.execute(
                    "UPDATE listings SET z=?, pct_below=?, scored_at=?"
                    " WHERE source=? AND id=?",
                    (score["z"], score["pct_below"], utc_now_iso(),
                     r["source"], r["id"]),
                )
            if meta_get(conn, "shadow_until") is None:
                until = (datetime.now(timezone.utc)
                         + timedelta(days=scoring.SHADOW_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
                meta_set(conn, "shadow_until", until)
                log("shadow mode active until %s — alerts collect silently" % until)
            if allow_alerts:
                maybe_alert(conn, r, score)
        except Exception as e:
            log("[score] %s/%s failed: %s" % (r.get("source"), r.get("id"), e))


def seed_key(label):
    return "seeded:%s" % label


def migrate_seed_flags(conn):
    """Convert the old single global seed flag into per-search flags.

    Only searches that already have stored listings count as seeded — a
    search added to SEARCH_URLS later must seed itself rather than inherit
    someone else's flag and flood Telegram with its whole first page.
    """
    if meta_get(conn, "seeded") != "1":
        return
    for search in SEARCH_URLS:
        label = search["label"]
        row = conn.execute(
            "SELECT 1 FROM listings WHERE search_label=? LIMIT 1", (label,)
        ).fetchone()
        if row and meta_get(conn, seed_key(label)) != "1":
            meta_set(conn, seed_key(label), "1")
    meta_set(conn, "seeded", "migrated")


def poll_once(conn):
    """One pass over all searches. Returns {label: parsed_count} for the pass.

    Seeding is per search: a search's first clean pass stores its current
    inventory silently, then flips its own flag. So one persistently broken
    search can never mute the healthy ones, and a search added to
    SEARCH_URLS later seeds itself instead of flooding Telegram.

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
                # Still assess+score the rows (they'd otherwise sit unassessed
                # until the next restart's backfill) — just never alert off a
                # search that failed verification.
                process_new_rows(conn, new_rows, allow_alerts=False)
                continue
            parsed_counts[label] = len(rows)
            log("[%s] parsed %d listings (%d new) of %s total"
                % (label, len(rows), len(new_rows), page_props.get("numberOfResults")))
            seeding = meta_get(conn, seed_key(label)) != "1"
            if seeding:
                meta_set(conn, seed_key(label), "1")
                log("[%s] seeded %d listings — alerts enabled next cycle"
                    % (label, len(rows)))
            # Seed-pass listings are old inventory: assessed and stored as
            # comps, but never alerted on.
            process_new_rows(conn, new_rows, allow_alerts=not seeding)
        except Exception as e:
            log("[%s] search failed: %s" % (label, e))
    return parsed_counts


# --- scheduled tasks --------------------------------------------------------

def maybe_refit(conn):
    now_local = datetime.now(TZ)
    today = now_local.strftime("%Y-%m-%d")
    due = (meta_get(conn, "last_refit_date") != today
           and now_local.hour >= REFIT_HOUR_LOCAL)
    if not due:
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(days=scoring.COMP_MAX_AGE_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
        grown = conn.execute(
            """SELECT 1 FROM (
                 SELECT l.family, COUNT(*) AS c,
                        COALESCE((SELECT m.comp_count FROM models m
                                  WHERE m.model_key = 'family:' || l.family), 0) AS mc
                 FROM listings l
                 WHERE l.family IS NOT NULL AND l.reject_reason IS NULL
                   AND COALESCE(l.is_damaged, 0) = 0 AND l.first_seen_at >= ?
                 GROUP BY l.family
               ) WHERE c - mc >= ? LIMIT 1""",
            (cutoff, scoring.REFIT_NEW_COMP_THRESHOLD),
        ).fetchone()
        due = grown is not None
    if not due:
        return
    n = scoring.refit_models(conn, utc_now_iso())
    global KNOWN
    KNOWN = scoring.known_vehicles(conn)
    meta_set(conn, "last_refit_date", today)
    log("refit: %d models written" % n)


def send_shadow_digest(conn, manual=False):
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = conn.execute(
        """SELECT a.*, l.title, l.km, l.url FROM alerts a
           LEFT JOIN listings l ON l.source = a.source AND l.id = a.listing_id
           WHERE a.shadow = 1 AND a.fired_at >= ? ORDER BY a.z ASC""",
        (since,),
    ).fetchall()
    lines = ["SHADOW DIGEST — %d would-have-fired alert(s) in the last 24h" % len(rows)]
    for a in rows:
        flags = ("[LOW CONF]" if a["low_confidence"] else "") + \
                ("[DEALER]" if a["is_dealer"] else "")
        lines.append("#%d z=%.1f %d%% below (pred $%s) %s — $%s %s\n%s" % (
            a["alert_id"], a["z"], round(a["pct_below"]),
            format(a["predicted_price"], ","), flags,
            format(a["price"], ","), a["title"] or "", a["url"] or ""))
    if not rows:
        lines.append("(pipeline alive; nothing crossed the threshold)")
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) > 3500:
            telegram_send(chunk)
            chunk = ""
        chunk += line + "\n\n"
    if chunk.strip():
        telegram_send(chunk.rstrip())
    if manual:
        log("digest sent (%d shadow alerts)" % len(rows))


def maybe_shadow_digest(conn):
    now_local = datetime.now(TZ)
    today = now_local.strftime("%Y-%m-%d")
    if now_local.hour < DIGEST_HOUR_LOCAL or meta_get(conn, "last_digest_date") == today:
        return
    shadow_until = meta_get(conn, "shadow_until")
    if not shadow_until or utc_now_iso() >= shadow_until:
        return  # shadow mode over; Step 3's full daily digest takes it from here
    send_shadow_digest(conn)
    meta_set(conn, "last_digest_date", today)


def send_weekly_report(conn):
    since = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    alerts = conn.execute(
        "SELECT * FROM alerts WHERE fired_at >= ?", (since,)).fetchall()
    real = sum(1 for a in alerts if not a["shadow"])
    labels = {}
    for a in alerts:
        if a["label"]:
            labels[a["label"]] = labels.get(a["label"], 0) + 1
    judged = labels.get("good", 0) + labels.get("bad", 0) + labels.get("scam", 0)
    lines = ["WEEKLY REPORT — %d alerts in 7d (%d real, %d shadow)"
             % (len(alerts), real, len(alerts) - real)]
    if labels:
        lines.append("labels: " + ", ".join("%s=%d" % kv for kv in sorted(labels.items())))
    if judged:
        lines.append("precision: %d/%d = %.0f%% of judged alerts were good"
                     % (labels.get("good", 0), judged, 100 * labels.get("good", 0) / judged))
    unlabeled = sum(1 for a in alerts if not a["label"])
    if unlabeled:
        lines.append("%d unlabeled — label with: python3 autotrader_watcher.py"
                     " --label <id> <good|bad|scam|already_gone>" % unlabeled)
    bad_texts = [r[0] for r in conn.execute(
        """SELECT COALESCE(l.title,'') || ' ' || COALESCE(l.description,'')
           FROM alerts a JOIN listings l ON l.source=a.source AND l.id=a.listing_id
           WHERE a.label IN ('bad','scam')""")]
    good_texts = [r[0] for r in conn.execute(
        """SELECT COALESCE(l.title,'') || ' ' || COALESCE(l.description,'')
           FROM alerts a JOIN listings l ON l.source=a.source AND l.id=a.listing_id
           WHERE a.label = 'good'""")]
    good_texts += [r[0] for r in conn.execute(
        """SELECT COALESCE(title,'') || ' ' || COALESCE(description,'') FROM listings
           WHERE reject_reason IS NULL ORDER BY RANDOM() LIMIT 200""")]
    if bad_texts:
        mined = scoring.mine_blocklist_candidates(
            bad_texts, good_texts, extra_exclude=KNOWN["makes"])
        if mined:
            lines.append("consider adding to blocklist: " + ", ".join(
                "%r (bad=%d/clean=%d)" % (g, b, c) for g, b, c in mined))
    gone = conn.execute(
        """SELECT COUNT(*), SUM(CASE WHEN julianday(disappeared_at)
                 - julianday(first_seen_at) <= 2 THEN 1 ELSE 0 END)
           FROM listings WHERE disappeared_at IS NOT NULL AND first_seen_at >= ?""",
        (since,)).fetchone()
    if gone[0]:
        lines.append("lifespan: %d tracked listings disappeared, %d within 48h"
                     % (gone[0], gone[1] or 0))
    telegram_send("\n\n".join(lines))
    log("weekly report sent (%d alerts, %d labeled)" % (len(alerts), judged))


def maybe_weekly_report(conn):
    now_local = datetime.now(TZ)
    today = now_local.strftime("%Y-%m-%d")
    if (now_local.weekday() == WEEKLY_REPORT_DOW
            and now_local.hour >= WEEKLY_REPORT_HOUR_LOCAL
            and meta_get(conn, "last_weekly_date") != today):
        send_weekly_report(conn)
        meta_set(conn, "last_weekly_date", today)


def classify_listing_check(resp, listing_id):
    """'alive' | 'gone' | 'unknown' for one lifespan probe.

    A dead AutoTrader listing 200-redirects off its /offers/ URL to a generic
    search page (verified live), so losing the id from the final URL is the
    primary gone signal; 404/410 and removal phrases are backup. Anything
    else (403/429/5xx bot challenges, outages) is 'unknown' — a transient
    hiccup must never look like a sale."""
    if resp.status_code in (404, 410):
        return "gone"
    if resp.status_code != 200:
        return "unknown"
    if listing_id not in resp.url:
        return "gone"
    if re.search(r"no longer available|listing (has )?expired",
                 resp.text[:200000], re.I):
        return "gone"
    return "alive"


def recheck_listings(conn):
    """Bounded lifespan tracking via direct URL re-checks.

    Falling off page one means nothing (newer listings push it off), so
    disappearance must be observed on the listing URL itself. A listing that
    vanishes within ~48h very likely sold fast — the closest thing to free
    supervision on whether the scorer finds real deals.
    """
    now = datetime.now(timezone.utc)
    rows = conn.execute(
        """SELECT l.source, l.id, l.url, l.gone_suspected_at,
                  EXISTS(SELECT 1 FROM alerts a WHERE a.source = l.source
                         AND a.listing_id = l.id) AS alerted
           FROM listings l
           WHERE l.source = 'autotrader' AND l.url IS NOT NULL
             AND l.disappeared_at IS NULL AND l.scored_at IS NOT NULL
             AND l.first_seen_at >= ?
             AND (l.last_checked_at IS NULL OR l.last_checked_at <= ?)
           ORDER BY alerted DESC, COALESCE(l.last_checked_at, '') ASC
           LIMIT ?""",
        ((now - timedelta(days=LIFESPAN_MAX_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ"),
         (now - timedelta(seconds=LIFESPAN_CHECK_INTERVAL_S)).strftime("%Y-%m-%dT%H:%M:%SZ"),
         LIFESPAN_CHECK_CAP),
    ).fetchall()
    for r in rows:
        try:
            resp = requests.get(
                r["url"], timeout=FETCH_TIMEOUT_S, allow_redirects=True,
                headers={"User-Agent": USER_AGENT, "Accept-Language": "en-CA,en;q=0.9"})
            verdict = classify_listing_check(resp, r["id"])
        except requests.RequestException:
            verdict = "unknown"  # network trouble is not evidence of removal
        checked_at = utc_now_iso()
        # disappeared_at is irreversible and feeds the sold-fast signal, so
        # "gone" must be observed on two checks >= 6h apart before it sticks;
        # "unknown" leaves the suspicion state untouched.
        with conn:
            if verdict == "gone" and r["gone_suspected_at"]:
                conn.execute(
                    "UPDATE listings SET last_checked_at=?, disappeared_at=?"
                    " WHERE source=? AND id=?",
                    (checked_at, checked_at, r["source"], r["id"]))
                log("[lifespan] %s/%s disappeared (confirmed on 2nd check)"
                    % (r["source"], r["id"]))
            elif verdict == "gone":
                conn.execute(
                    "UPDATE listings SET last_checked_at=?, gone_suspected_at=?"
                    " WHERE source=? AND id=?",
                    (checked_at, checked_at, r["source"], r["id"]))
            elif verdict == "alive":
                conn.execute(
                    "UPDATE listings SET last_checked_at=?, gone_suspected_at=NULL"
                    " WHERE source=? AND id=?",
                    (checked_at, r["source"], r["id"]))
            else:
                conn.execute(
                    "UPDATE listings SET last_checked_at=? WHERE source=? AND id=?",
                    (checked_at, r["source"], r["id"]))
        time.sleep(random.uniform(1.5, 3.5))


def run_scheduled_tasks(conn):
    for task in (maybe_refit, maybe_shadow_digest, maybe_weekly_report,
                 recheck_listings):
        try:
            task(conn)
        except Exception as e:
            log("%s failed: %s" % (task.__name__, e))


# --- ingest server (FB->droplet bridge) -------------------------------------

class IngestHandler(BaseHTTPRequestHandler):
    server_version = "CarScanner"
    timeout = 20  # socket read timeout; a slow-drip client can't hold a thread

    def log_message(self, fmt, *args):
        pass  # no client-controlled format strings in our logs

    def _reply(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        try:
            if self.path != "/ingest":
                return self._reply(404, {"ok": False})
            auth = self.headers.get("Authorization", "")
            if not (INGEST_TOKEN
                    and hmac.compare_digest(auth, "Bearer " + INGEST_TOKEN)):
                return self._reply(401, {"ok": False})
            length = self.headers.get("Content-Length")
            if length is None:
                return self._reply(411, {"ok": False})
            try:
                length = int(length)
            except ValueError:
                return self._reply(400, {"ok": False})
            if length < 0:
                return self._reply(400, {"ok": False})  # read(-1) = until EOF
            if length > INGEST_MAX_BODY:
                return self._reply(413, {"ok": False})
            try:
                items = json.loads(self.rfile.read(length))
            except ValueError:
                return self._reply(400, {"ok": False})
            if not isinstance(items, list) or len(items) > INGEST_MAX_ITEMS:
                return self._reply(400, {"ok": False})
            conn = open_db(DB_PATH)
            try:
                stored, skipped = handle_ingest_items(conn, items)
            finally:
                conn.close()
            self._reply(200, {"ok": True, "stored": stored, "skipped": skipped})
        except Exception as e:
            log("ingest request failed: %s" % e.__class__.__name__)
            try:
                self._reply(500, {"ok": False})
            except Exception:
                pass


def handle_ingest_items(conn, items):
    """FB items go through the exact same store/assess/score/alert pipeline."""
    stored = skipped = 0
    now = utc_now_iso()
    for item in items:
        try:
            if not isinstance(item, dict):
                skipped += 1
                continue
            row = scoring.parse_fb_listing(item, KNOWN, now)
            if row is None:
                skipped += 1
                continue
            new_rows = store_listings(conn, [row])
            stored += 1
            if new_rows:
                process_new_rows(conn, new_rows,
                                 allow_alerts=not bool(item.get("seed")))
        except Exception as e:
            skipped += 1
            log("ingest item failed: %s" % e.__class__.__name__)
    return stored, skipped


def start_ingest_server():
    if not INGEST_TOKEN:
        log("ingest server disabled (INGEST_TOKEN not set)")
        return None
    host, _, port = INGEST_BIND.rpartition(":")
    try:
        server = ThreadingHTTPServer((host or "0.0.0.0", int(port)), IngestHandler)
        server.daemon_threads = True
    except Exception as e:
        log("ingest server failed to start on %s: %s" % (INGEST_BIND, e))
        return None
    thread = threading.Thread(target=server.serve_forever, daemon=True,
                              name="ingest")
    thread.start()
    log("ingest server listening on %s" % INGEST_BIND)
    return server


# --- entry points -----------------------------------------------------------

def run_watcher():
    log("starting autotrader watcher; db=%s" % DB_PATH)
    conn = init_db(DB_PATH)
    migrate_db(conn)
    migrate_seed_flags(conn)
    global KNOWN
    KNOWN = scoring.known_vehicles(conn)
    try:
        n = scoring.refit_models(conn, utc_now_iso())
        meta_set(conn, "last_refit_date", datetime.now(TZ).strftime("%Y-%m-%d"))
        log("startup refit: %d models written" % n)
    except Exception as e:
        log("startup refit failed: %s" % e)
    start_ingest_server()
    while True:
        try:
            poll_once(conn)
        except Exception as e:
            log("poll cycle failed: %s" % e)
        try:
            run_scheduled_tasks(conn)
        except Exception as e:
            log("scheduled tasks failed: %s" % e)
        sleep_s = POLL_INTERVAL_BASE_S + random.uniform(0, POLL_JITTER_S)
        log("sleeping %.0fs" % sleep_s)
        time.sleep(sleep_s)


def label_alert(alert_id, label):
    valid = ("good", "bad", "scam", "already_gone")
    if label not in valid:
        print("label must be one of: %s" % "|".join(valid))
        return 2
    conn = init_db(DB_PATH)
    migrate_db(conn)
    with conn:
        cur = conn.execute(
            "UPDATE alerts SET label=?, labeled_at=? WHERE alert_id=?",
            (label, utc_now_iso(), int(alert_id)))
    if cur.rowcount == 0:
        print("no alert with id %s" % alert_id)
        return 1
    row = conn.execute(
        """SELECT a.z, a.price, l.title, l.url FROM alerts a
           LEFT JOIN listings l ON l.source=a.source AND l.id=a.listing_id
           WHERE a.alert_id=?""", (int(alert_id),)).fetchone()
    print("labeled #%s %s: %s — $%s (z=%.2f)\n%s" % (
        alert_id, label, row["title"] or "?",
        format(row["price"], ","), row["z"], row["url"] or ""))
    return 0


def main():
    p = argparse.ArgumentParser(
        description="AutoTrader watcher + deal scorer (Edmonton car scanner)")
    p.add_argument("--label", nargs=2, metavar=("ALERT_ID", "LABEL"),
                   help="label an alert: good|bad|scam|already_gone")
    p.add_argument("--digest", action="store_true",
                   help="send the shadow digest now and exit")
    p.add_argument("--report", action="store_true",
                   help="send the weekly report now and exit")
    args = p.parse_args()
    if args.label:
        raise SystemExit(label_alert(args.label[0], args.label[1]))
    if args.digest or args.report:
        conn = init_db(DB_PATH)
        migrate_db(conn)
        if args.digest:
            send_shadow_digest(conn, manual=True)
        if args.report:
            send_weekly_report(conn)
        return
    run_watcher()


if __name__ == "__main__":
    main()
