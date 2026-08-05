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
import logging
import math
import os
import random
import re
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler

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

LOG_PATH = os.environ.get(
    "CAR_SCANNER_LOG",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "car-scanner.log"))
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUPS = 5

# Silent-failure alarms. A parser that returns nothing looks exactly like a
# quiet market, so breakage has to announce itself.
ZERO_STREAK_ALARM = 2                  # consecutive empty scans before warning
HEALTH_WARN_COOLDOWN_S = 6 * 3600
VOLUME_DROP_RATIO = 0.4                # today < 40% of average = >60% drop
VOLUME_MIN_HISTORY_DAYS = 4            # don't cry breakage during ramp-up
VOLUME_WARN_COOLDOWN_S = 12 * 3600
FB_SILENCE_ALARM_S = 6 * 3600          # ingest quiet this long = browser died

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


class _JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc)
                          .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "level": record.levelname,
            "event": getattr(record, "event", "log"),
            "msg": record.getMessage(),
        }
        payload.update(getattr(record, "fields", None) or {})
        try:
            # Compact separators so the documented greps ('"event":"reject"')
            # match, and the file stays smaller.
            return json.dumps(payload, default=str, separators=(",", ":"))
        except Exception:
            return json.dumps({"ts": payload["ts"], "level": "ERROR",
                               "event": "log_format_failed"}, separators=(",", ":"))


_logger = None


def get_logger():
    """stderr stays human-readable for journalctl; the rotating file gets
    one JSON object per line so breakage can be grepped after the fact."""
    global _logger
    if _logger is not None:
        return _logger
    logger = logging.getLogger("car_scanner")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        stream = logging.StreamHandler(sys.stderr)
        stream.setFormatter(logging.Formatter("[%(asctime)s] %(message)s",
                                              datefmt="%Y-%m-%dT%H:%M:%SZ"))
        logger.addHandler(stream)
        try:
            os.makedirs(os.path.dirname(LOG_PATH) or ".", exist_ok=True)
            rotating = RotatingFileHandler(
                LOG_PATH, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS)
            rotating.setFormatter(_JsonFormatter())
            logger.addHandler(rotating)
        except Exception as e:  # unwritable path must not stop the watcher
            print("[warn] file logging disabled (%s): %s" % (LOG_PATH, e),
                  file=sys.stderr, flush=True)
    _logger = logger
    return logger


def log(msg, event=None, **fields):
    get_logger().info(msg, extra={"event": event or "log", "fields": fields})


# Cloudflare / bot-wall signatures. A blocked droplet IP looks exactly like
# "Edmonton has no cars" unless it is called out by name.
CHALLENGE_MARKERS = (
    "just a moment", "cf-browser-verification", "cf_chl_", "__cf_chl",
    "checking your browser", "attention required! | cloudflare",
    "enable javascript and cookies to continue", "access denied",
    "please verify you are a human", "px-captcha", "perimeterx",
)


def detect_bot_challenge(resp):
    """Returns a reason string when a response looks like a bot wall."""
    if resp.status_code in (401, 403, 429):
        return "http_%d" % resp.status_code
    if resp.status_code == 503:
        return "http_503_maybe_challenge"
    server = (resp.headers.get("Server") or "").lower()
    body = (resp.text or "")[:20000].lower()
    for marker in CHALLENGE_MARKERS:
        if marker in body:
            return "challenge_page:%s" % marker[:32]
    if "cloudflare" in server and resp.status_code != 200:
        return "cloudflare_%d" % resp.status_code
    return None


def fetch_page(url):
    """GET a search page. Returns (html, problem); never raises.

    problem is None on success, else a short reason — bot challenges are
    distinguished from ordinary failures so the operator learns the droplet
    IP is blocked rather than assuming the city ran out of cars.
    """
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "en-CA,en;q=0.9"}
    backoff = FETCH_BACKOFF_S
    problem = "unreachable"
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=FETCH_TIMEOUT_S)
            challenge = detect_bot_challenge(resp)
            if resp.status_code == 200 and not challenge:
                return resp.text, None
            problem = challenge or "http_%d" % resp.status_code
            log("fetch attempt %d: %s for %s" % (attempt, problem, url),
                event="fetch_failed", attempt=attempt, problem=problem, url=url)
        except requests.RequestException as e:
            problem = "network:%s" % e.__class__.__name__
            log("fetch attempt %d failed: %s" % (attempt, e),
                event="fetch_failed", attempt=attempt, problem=problem)
        if attempt < FETCH_RETRIES:
            time.sleep(backoff)
            backoff *= 2
    return None, problem


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

-- One row per search fetched (and per ingest batch), so the daily digest can
-- report scan volume even for listings already seen. Step 4's partial-breakage
-- alarm reads trailing averages from here.
CREATE TABLE IF NOT EXISTS scans (
    scanned_at   TEXT NOT NULL,
    source       TEXT NOT NULL,
    search_label TEXT NOT NULL,
    parsed_count INTEGER NOT NULL,
    new_count    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_scans_at ON scans(scanned_at);
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


def minutes_since(iso_str):
    if not iso_str:
        return None
    try:
        seen = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None
    return max(0, int((datetime.now(timezone.utc) - seen).total_seconds() // 60))


def format_age(minutes):
    """'seen' age, not true posting age — neither site exposes when a listing
    was actually posted, so this measures how long WE have known about it."""
    if minutes is None:
        return "age n/a"
    if minutes < 60:
        return "seen %dm ago" % minutes
    if minutes < 60 * 24:
        return "seen %.0fh ago" % (minutes / 60.0)
    return "seen %.0fd ago" % (minutes / 1440.0)


def vehicle_line(row):
    ymm = " ".join(str(p) for p in (row.get("year"), row.get("make"), row.get("model")) if p)
    return ymm or (row.get("title") or "(unidentified)")


def format_alert(row, score, predicted_price, shadow, first_seen_at=None):
    """Decision-relevant numbers first — the opening line is what shows in a
    phone notification, so it carries discount, z, price and the vehicle."""
    km = "%s km" % format(row["km"], ",") if row.get("km") is not None else "km n/a"
    head = "%s%d%% below · z=%.1f · $%s · %s" % (
        "[SHADOW] " if shadow else "⚡ ",
        round(score["pct_below"]), score["z"],
        format(row["price"], ","), vehicle_line(row),
    )
    lines = [
        head,
        "predicted $%s from %d comps (%s)" % (
            format(predicted_price, ","), score["model"]["comp_count"],
            score["model"]["model_key"].split(":", 1)[-1]),
        "%s · %s · %s · %s" % (
            km, row.get("seller_type") or "seller n/a",
            row.get("city") or "city n/a", format_age(minutes_since(first_seen_at))),
    ]
    marks = []
    if score["low_confidence"]:
        marks.append("⚠️ LOW CONFIDENCE — segment model, too few comps for this family")
    if row.get("seller_type") == "Dealer":
        marks.append("🏪 DEALER — priced by a pro, check for a catch")
    if marks:
        lines.append(" · ".join(marks))
    lines.append(row.get("url") or "")   # URL last so it stays tappable
    return "\n".join(l for l in lines if l)


# --test routes every incidental send here instead of the network, so the
# run can promise exactly one real message.
_SUPPRESSED_SENDS = None


def telegram_send(text):
    """Send a message. Returns True on success.

    Catches everything: a Telegram outage, a DNS failure, or a malformed
    payload must never propagate into the poll loop.
    """
    if _SUPPRESSED_SENDS is not None:
        _SUPPRESSED_SENDS.append(text)
        return True
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
    except Exception as e:
        log("telegram send failed: %s" % e)
        return False


def record_scan(conn, source, search_label, parsed_count, new_count):
    """Scan volume for the digest — listings alone can't show it, since a
    cycle that re-sees 20 known listings inserts no rows."""
    try:
        with conn:
            conn.execute(
                "INSERT INTO scans (scanned_at, source, search_label,"
                " parsed_count, new_count) VALUES (?,?,?,?,?)",
                (utc_now_iso(), source, search_label, parsed_count, new_count))
    except Exception as e:
        log("record_scan failed: %s" % e)


def cooldown_passed(conn, key, seconds):
    """True when key hasn't fired within `seconds`. Does not stamp — the
    caller stamps only after a successful send, so a Telegram outage can't
    silently consume an alarm."""
    last = meta_get(conn, key)
    if not last:
        return True
    try:
        last_dt = datetime.strptime(last, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    return (datetime.now(timezone.utc) - last_dt).total_seconds() >= seconds


def record_health(conn, source, label, ok, reason=None):
    """Track consecutive empty/failed scans; warn once the streak proves it.

    One empty scan is a blip (a hiccup, a slow page). Two in a row on a
    market this size is the parser being broken — which is the failure that
    otherwise reads as 'no deals in Edmonton for three weeks'.
    """
    key = "zero_streak:%s:%s" % (source, label)
    if ok:
        if meta_get(conn, key) not in (None, "0"):
            log("[%s/%s] recovered" % (source, label), event="scan_recovered",
                source=source, search_label=label)
        meta_set(conn, key, "0")
        return
    streak = int(meta_get(conn, key) or 0) + 1
    meta_set(conn, key, str(streak))
    log("[%s/%s] empty scan #%d (%s)" % (source, label, streak, reason),
        event="scan_empty", source=source, search_label=label,
        streak=streak, reason=reason)
    if streak < ZERO_STREAK_ALARM:
        return
    warn_key = "health_warned_at:%s:%s" % (source, label)
    if not cooldown_passed(conn, warn_key, HEALTH_WARN_COOLDOWN_S):
        return
    if reason and (reason.startswith("challenge") or reason.startswith("http_4")
                   or reason.startswith("cloudflare") or reason == "http_503_maybe_challenge"):
        detail = ("Looks like a bot wall (%s) — the droplet IP may be blocked. "
                  "Try curling the search URL from the droplet." % reason)
    else:
        detail = ("Reason: %s. If the site changed its markup the parser needs "
                  "updating — a silent zero looks identical to an empty market."
                  % reason)
    if telegram_send("🚨 BROKEN [%s/%s] %d consecutive scans returned no listings.\n%s"
                     % (source, label, streak, detail)):
        meta_set(conn, warn_key, utc_now_iso())


def warn_search_broken(conn, label, problems):
    """Rate-limited (per label, 6h) Telegram warning that a search looks broken."""
    key = "verify_warned_at:%s" % label
    if not cooldown_passed(conn, key, VERIFY_WARN_COOLDOWN_S):
        return
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
                % (src, lid, score["z"], ",".join(score["suppress"])),
                event="suppress", source=src, listing_id=lid,
                z=round(score["z"], 2), reason=",".join(score["suppress"]))
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
            % (src, lid, score["z"]),
            event="suppress", source=src, listing_id=lid,
            z=round(score["z"], 2), reason="repost_fingerprint")
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
           score["z"], round(score["pct_below"]), predicted),
        event="alert", alert_id=cur.lastrowid, source=src, listing_id=lid,
        z=round(score["z"], 2), pct_below=round(score["pct_below"], 1),
        price=row["price"], predicted=predicted, shadow=bool(shadow),
        model_key=m["model_key"], comps=m["comp_count"],
        low_confidence=bool(score["low_confidence"]))
    if not shadow:
        seen_row = conn.execute(
            "SELECT first_seen_at FROM listings WHERE source=? AND id=?",
            (src, lid)).fetchone()
        telegram_send(format_alert(row, score, predicted, shadow,
                                   seen_row["first_seen_at"] if seen_row else None))
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
                                               (r.get("title") or "")[:60]),
                    event="reject", source=r["source"], listing_id=r["id"],
                    reason=reason, price=r.get("price"))
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
            html, fetch_problem = fetch_page(url)
            if html is None:
                log("[%s] fetch failed after retries (%s)" % (label, fetch_problem))
                record_health(conn, "autotrader", label, ok=False, reason=fetch_problem)
                continue
            page_props = extract_next_data(html)
            if page_props is None:
                log("[%s] no __NEXT_DATA__ found — page structure changed?" % label)
                record_health(conn, "autotrader", label, ok=False,
                              reason="__NEXT_DATA__ missing")
                continue
            rows = parse_listings(page_props, label)
            problems = verify_search_state(page_props, url, parsed_count=len(rows))
            new_rows = store_listings(conn, rows)  # comp data is comp data — always store
            record_scan(conn, "autotrader", label, len(rows), len(new_rows))
            # An empty page is streak-tracked (could be a blip); applied-param
            # drift is config-level breakage and warns on the first sighting.
            record_health(conn, "autotrader", label, ok=bool(rows),
                          reason=None if rows else "zero listings parsed")
            # Every verification problem suppresses this search's alerts, so
            # every one must be announced — otherwise a search stops alerting
            # indefinitely while its scan counts still look healthy. The two
            # empty-page symptoms are the exception: the streak counter above
            # owns them (warn on the 2nd consecutive miss, not the 1st blip).
            # Everything else coexists with parsed rows, so the streak can
            # never catch it and it must warn on sight.
            notify = [p for p in problems
                      if not p.startswith("no listings")
                      and not p.startswith("numberOfResults")]
            if notify:
                warn_search_broken(conn, label, notify)
            if problems:
                log("[%s] search-state problems: %s" % (label, "; ".join(problems)),
                    event="search_problems", search_label=label, problems=problems)
                # Still assess+score the rows (they'd otherwise sit unassessed
                # until the next restart's backfill) — just never alert off a
                # search that failed verification.
                process_new_rows(conn, new_rows, allow_alerts=False)
                continue
            parsed_counts[label] = len(rows)
            log("[%s] parsed %d listings (%d new) of %s total"
                % (label, len(rows), len(new_rows), page_props.get("numberOfResults")),
                event="scan_ok", source="autotrader", search_label=label,
                parsed=len(rows), new=len(new_rows),
                site_total=page_props.get("numberOfResults"))
            seeding = meta_get(conn, seed_key(label)) != "1"
            if seeding:
                meta_set(conn, seed_key(label), "1")
                log("[%s] seeded %d listings — alerts enabled next cycle"
                    % (label, len(rows)))
            # Seed-pass listings are old inventory: assessed and stored as
            # comps, but never alerted on.
            process_new_rows(conn, new_rows, allow_alerts=not seeding)
        except Exception as e:
            # A search that raises every cycle would otherwise never reach
            # record_health below and stay invisible to every alarm.
            log("[%s] search failed: %s" % (label, e),
                event="search_exception", search_label=label,
                error=e.__class__.__name__)
            try:
                record_health(conn, "autotrader", label, ok=False,
                              reason="exception:%s" % e.__class__.__name__)
            except Exception:
                pass
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
    log("refit: %d models written" % n, event="refit", models=n)


def local_day_start_utc():
    """UTC timestamp of 00:00 today in Edmonton — the digest's day boundary."""
    midnight = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def send_telegram_chunks(lines, limit=3500):
    """Returns True only if every chunk was accepted — the caller must not
    mark the day done when the message never arrived."""
    ok = True
    chunk = ""
    for line in lines:
        if chunk and len(chunk) + len(line) > limit:
            ok = telegram_send(chunk.rstrip()) and ok
            chunk = ""
        chunk += line + "\n"
    if chunk.strip():
        ok = telegram_send(chunk.rstrip()) and ok
    return ok


def send_daily_digest(conn, manual=False):
    """8 PM summary: scan volume, alerts, rejections, the day's best scores.

    During shadow mode this is the only thing that reaches the phone, and it
    goes out even on empty days — silence would be indistinguishable from a
    dead pipeline."""
    # Window from the previous digest, NOT local midnight: the digest fires at
    # 20:00, so a midnight anchor would drop 20:00-to-midnight into a hole no
    # digest ever covers — prime private-seller posting hours.
    since = meta_get(conn, "last_digest_at")
    floor = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    if not since or since < floor:
        since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    scans = conn.execute(
        """SELECT source, search_label, SUM(parsed_count) AS parsed
           FROM scans WHERE scanned_at >= ? GROUP BY source, search_label
           ORDER BY source, search_label""", (since,)).fetchall()
    by_source = {}
    for s in scans:
        by_source[s["source"]] = by_source.get(s["source"], 0) + (s["parsed"] or 0)

    alerts = conn.execute(
        "SELECT shadow, COUNT(*) AS n FROM alerts WHERE fired_at >= ? GROUP BY shadow",
        (since,)).fetchall()
    sent = sum(a["n"] for a in alerts if not a["shadow"])
    shadowed = sum(a["n"] for a in alerts if a["shadow"])

    rejects = conn.execute(
        """SELECT reject_reason, COUNT(*) AS n FROM listings
           WHERE first_seen_at >= ? AND reject_reason IS NOT NULL
           GROUP BY reject_reason""", (since,)).fetchall()
    by_reason = {}
    for r in rejects:  # collapse 'blocklist:rebuilt' -> 'blocklist'
        key = r["reject_reason"].split(":", 1)[0]
        by_reason[key] = by_reason.get(key, 0) + r["n"]

    best = conn.execute(
        """SELECT l.*,
                  EXISTS(SELECT 1 FROM alerts a WHERE a.source=l.source
                         AND a.listing_id=l.id AND a.shadow=0) AS sent_alert,
                  EXISTS(SELECT 1 FROM alerts a WHERE a.source=l.source
                         AND a.listing_id=l.id) AS any_alert
           FROM listings l WHERE l.scored_at >= ? AND l.z IS NOT NULL
           ORDER BY l.z ASC LIMIT 5""", (since,)).fetchall()

    lines = ["📊 DIGEST — %s (since %s)" % (
        datetime.now(TZ).strftime("%a %d %b"),
        datetime.strptime(since, "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=timezone.utc).astimezone(TZ).strftime("%a %H:%M"))]
    lines.append("Scanned: " + (" · ".join(
        "%s %d" % (src, n) for src, n in sorted(by_source.items())) or "nothing"))
    if scans:
        # source-tagged: both sources use a 'cars_2k_15k' label, and an
        # untagged breakdown makes a dead search look like a live one
        tag = {"autotrader": "at", "facebook": "fb"}
        lines.append("  " + " · ".join(
            "%s:%s %d" % (tag.get(s["source"], s["source"]), s["search_label"],
                          s["parsed"] or 0) for s in scans))
    lines.append("Alerts: %d sent · %d shadow" % (sent, shadowed))
    lines.append("Rejected %d: %s" % (
        sum(by_reason.values()),
        " · ".join("%s %d" % kv for kv in sorted(
            by_reason.items(), key=lambda kv: -kv[1])) or "none"))
    if best:
        lines.append("")
        lines.append("Best scores today:")
        for i, b in enumerate(best, 1):
            # "alerted" must mean it reached the phone; a shadow row did not.
            mark = ("  ✅ alerted" if b["sent_alert"]
                    else "  🌒 would have fired" if b["any_alert"] else "")
            lines.append(" %d. z=%.1f · %d%% below · $%s · %s%s" % (
                i, b["z"], round(b["pct_below"] or 0), format(b["price"], ","),
                vehicle_line(dict(b)), mark))

    shadow_until = meta_get(conn, "shadow_until")
    in_shadow = shadow_until and utc_now_iso() < shadow_until
    if in_shadow:
        would = conn.execute(
            """SELECT a.*, l.url FROM alerts a
               LEFT JOIN listings l ON l.source=a.source AND l.id=a.listing_id
               WHERE a.shadow=1 AND a.fired_at >= ? ORDER BY a.z ASC""",
            (since,)).fetchall()
        lines.append("")
        lines.append("🌒 SHADOW MODE until %s — nothing has buzzed your phone."
                     % shadow_until[:10])
        lines.append("Would have fired (%d):" % len(would))
        for a in would:
            marks = ("  ⚠️ low conf" if a["low_confidence"] else "") + \
                    ("  🏪 dealer" if a["is_dealer"] else "")
            lines.append("#%d z=%.1f · %d%% below · $%s vs $%s%s\n%s" % (
                a["alert_id"], a["z"], round(a["pct_below"]),
                format(a["price"], ","), format(a["predicted_price"], ","),
                marks, a["url"] or ""))
        if not would:
            lines.append("(none crossed the threshold today)")
    ok = send_telegram_chunks(lines)
    if ok:
        meta_set(conn, "last_digest_at", utc_now_iso())
    if manual:
        log("digest %s (%d sent, %d shadow alerts in window)"
            % ("sent" if ok else "FAILED to send", sent, shadowed))
    return ok


def maybe_daily_digest(conn):
    now_local = datetime.now(TZ)
    today = now_local.strftime("%Y-%m-%d")
    if now_local.hour < DIGEST_HOUR_LOCAL or meta_get(conn, "last_digest_date") == today:
        return
    # Only mark the day done once it actually arrived: during shadow mode this
    # is the sole thing reaching the phone, and a swallowed digest is
    # indistinguishable from the dead pipeline it exists to rule out.
    if send_daily_digest(conn):
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


def check_scan_volume(conn):
    """Partial breakage: volume quietly collapses while scans still 'work'.

    Compares the last 24h against the daily average of the 7 days before it,
    per source — one dead search or a dead FB tab halves a source's volume
    without ever returning a zero scan.
    """
    now = datetime.now(timezone.utc)
    day_ago = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    week_ago = (now - timedelta(days=8)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = conn.execute(
        """SELECT source,
                  SUM(CASE WHEN scanned_at >= ? THEN parsed_count ELSE 0 END) AS today,
                  SUM(CASE WHEN scanned_at <  ? THEN parsed_count ELSE 0 END) AS prior,
                  COUNT(DISTINCT CASE WHEN scanned_at < ? THEN date(scanned_at) END) AS days
           FROM scans WHERE scanned_at >= ? GROUP BY source""",
        (day_ago, day_ago, day_ago, week_ago)).fetchall()
    for r in rows:
        if (r["days"] or 0) < VOLUME_MIN_HISTORY_DAYS:
            continue  # not enough history to call anything abnormal
        avg = (r["prior"] or 0) / float(r["days"])
        if avg <= 0:
            continue
        today = r["today"] or 0
        if today >= VOLUME_DROP_RATIO * avg:
            continue
        key = "volume_warned_at:%s" % r["source"]
        if not cooldown_passed(conn, key, VOLUME_WARN_COOLDOWN_S):
            continue
        drop = 100 * (1 - today / avg)
        log("volume drop on %s: %d vs %.0f/day" % (r["source"], today, avg),
            event="volume_drop", source=r["source"], today=today, avg=avg)
        if telegram_send(
            "⚠️ VOLUME DROP [%s] %d listings scanned in 24h vs %.0f/day average "
            "(%.0f%% below).\nPartial breakage — one search or the FB tab may be "
            "dead while the rest still works." % (r["source"], today, avg, drop)
        ):
            meta_set(conn, key, utc_now_iso())


def check_fb_silence(conn):
    """The FB bridge dies silently: closed tab, logged-out browser, bad token."""
    if not INGEST_TOKEN:
        return  # bridge not configured; nothing to be silent about
    last = conn.execute(
        "SELECT MAX(scanned_at) FROM scans WHERE source='facebook'").fetchone()[0]
    if not last:
        return  # never received anything yet — setup pending, not breakage
    # `last` includes heartbeats, so this measures browser liveness rather
    # than "time since a new car was posted" (a quiet market is not breakage).
    try:
        last_dt = datetime.strptime(last, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return
    quiet_s = (datetime.now(timezone.utc) - last_dt).total_seconds()
    if quiet_s < FB_SILENCE_ALARM_S:
        return
    if not cooldown_passed(conn, "fb_silence_warned_at", FB_SILENCE_ALARM_S):
        return
    log("facebook ingest silent for %.1fh" % (quiet_s / 3600.0),
        event="fb_silent", quiet_hours=round(quiet_s / 3600.0, 1))
    if telegram_send(
        "⚠️ FACEBOOK SILENT — no listings received from the browser in %.1fh.\n"
        "Check the pinned Marketplace tab is open and logged in, and that the "
        "ingest token still matches." % (quiet_s / 3600.0)
    ):
        meta_set(conn, "fb_silence_warned_at", utc_now_iso())


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
                    % (r["source"], r["id"]),
                    event="lifespan_gone", source=r["source"], listing_id=r["id"])
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


def refresh_known(conn):
    """Re-read the make/model dictionary the FB title parser matches against.

    It grows with every AutoTrader family stored, and FB rows whose model
    can't be matched are dropped as incomplete — so a dictionary refreshed
    only at the nightly refit would keep FB parsing degraded all day.
    """
    global KNOWN
    KNOWN = scoring.known_vehicles(conn)


def run_scheduled_tasks(conn):
    for task in (refresh_known, check_scan_volume, check_fb_silence, maybe_refit,
                 maybe_daily_digest, maybe_weekly_report, recheck_listings):
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
    by_label = {}
    for item in items:
        if isinstance(item, dict):
            lbl = str(item.get("label") or "facebook")[:40]
            by_label[lbl] = by_label.get(lbl, 0) + 1
    if not items:
        # Heartbeat: the userscript posts an empty batch when it has nothing
        # new, so silence means a dead bridge rather than a quiet market.
        record_scan(conn, "facebook", "heartbeat", 0, 0)
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
    for lbl, n in by_label.items():
        record_scan(conn, "facebook", lbl, n, 0)
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


def _sandbox_db(real_path):
    """Snapshot the live DB so --test can score against real comps and models
    without writing a single row back. Uses the backup API, which is
    WAL-safe against a running watcher."""
    tmp_path = os.path.join(
        tempfile.gettempdir(), "car-scanner-test-%d.db" % os.getpid())
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(tmp_path + suffix)
        except OSError:
            pass
    dest = open_db(tmp_path)
    if os.path.exists(real_path):
        src = sqlite3.connect("file:%s?mode=ro" % real_path, uri=True)
        try:
            src.backup(dest)
        finally:
            src.close()
    dest.executescript(DDL)
    dest.commit()
    return dest, tmp_path


def run_test():
    """One live fetch end to end, against a throwaway copy of the database.

    Prints listings parsed per search, the rejection breakdown, and the top
    scored listings; sends exactly one Telegram message. The real database
    is never written to.
    """
    global _SUPPRESSED_SENDS
    conn, tmp_path = _sandbox_db(DB_PATH)
    _SUPPRESSED_SENDS = []
    started = datetime.now(timezone.utc)
    try:
        migrate_db(conn)
        migrate_seed_flags(conn)
        global KNOWN
        KNOWN = scoring.known_vehicles(conn)
        base_listings = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        print("=" * 72)
        print("CAR SCANNER SELF-TEST — live fetch, sandboxed database")
        print("real db : %s (%d listings, untouched)" % (DB_PATH, base_listings))
        print("sandbox : %s" % tmp_path)
        print("=" * 72)

        # Alerts must record but never send; the one real message comes later.
        meta_set(conn, "shadow_until",
                 (started + timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ"))
        for s in SEARCH_URLS:
            meta_set(conn, seed_key(s["label"]), "1")   # exercise the alert path
        alerts_before = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]

        print("\n--- 1. LIVE FETCH ------------------------------------------------")
        rows_by_label = {}
        for i, search in enumerate(SEARCH_URLS):
            if i:
                time.sleep(random.uniform(*BETWEEN_SEARCHES_S))
            label = search["label"]
            t0 = time.time()
            html, problem = fetch_page(search["url"])
            if html is None:
                print("  %-20s FETCH FAILED (%s)" % (label, problem))
                rows_by_label[label] = []
                continue
            page_props = extract_next_data(html)
            if page_props is None:
                print("  %-20s NO __NEXT_DATA__ — page structure changed" % label)
                rows_by_label[label] = []
                continue
            rows = parse_listings(page_props, label)
            problems = verify_search_state(page_props, search["url"], len(rows))
            rows_by_label[label] = rows
            print("  %-20s %2d parsed of %s site-wide  %5.1fs  %s" % (
                label, len(rows), format(page_props.get("numberOfResults") or 0, ","),
                time.time() - t0,
                "OK" if not problems else "PROBLEMS: " + "; ".join(problems)))

        all_rows = [r for rows in rows_by_label.values() for r in rows]
        new_rows = store_listings(conn, all_rows)
        ids = {(r["source"], r["id"]) for r in all_rows}
        print("  %d listings fetched (%d unique — searches overlap), %d not "
              "already in the database" % (len(all_rows), len(ids), len(new_rows)))

        print("\n--- 2. REJECTION BREAKDOWN ---------------------------------------")
        process_new_rows(conn, new_rows, allow_alerts=True)
        reasons, clean = {}, 0
        for src, lid in ids:
            row = conn.execute(
                "SELECT reject_reason FROM listings WHERE source=? AND id=?",
                (src, lid)).fetchone()
            reason = row["reject_reason"] if row else "not stored"
            if reason:
                reasons[reason.split(":", 1)[0]] = reasons.get(reason.split(":", 1)[0], 0) + 1
            else:
                clean += 1
        if reasons:
            for reason, n in sorted(reasons.items(), key=lambda kv: -kv[1]):
                print("  %-24s %3d  (%.0f%%)" % (reason, n, 100.0 * n / len(ids)))
        else:
            print("  (nothing rejected)")
        print("  %-24s %3d  (%.0f%%)  eligible for scoring"
              % ("clean", clean, 100.0 * clean / max(1, len(ids))))

        print("\n--- 3. MODELS ----------------------------------------------------")
        n_models = scoring.refit_models(conn, utc_now_iso())
        fams = conn.execute(
            "SELECT COUNT(*) FROM models WHERE kind='family'").fetchone()[0]
        segs = conn.execute(
            "SELECT COUNT(*) FROM models WHERE kind='segment'").fetchone()[0]
        print("  refit %d models: %d family, %d segment" % (n_models, fams, segs))
        for m in conn.execute(
            "SELECT * FROM models ORDER BY kind, comp_count DESC LIMIT 6"
        ):
            print("    %-28s n=%-4d mad=%.3f  %s" % (
                m["model_key"], m["comp_count"], m["mad"], m["method"]))

        print("\n--- 4. TOP SCORED LISTINGS ---------------------------------------")
        # rescore everything fetched now that models exist
        for src, lid in ids:
            row = conn.execute("SELECT * FROM listings WHERE source=? AND id=?",
                               (src, lid)).fetchone()
            if not row or row["reject_reason"]:
                continue
            score = score_listing(conn, dict(row))
            if score is None:
                continue
            with conn:
                conn.execute("UPDATE listings SET z=?, pct_below=?, scored_at=?"
                             " WHERE source=? AND id=?",
                             (score["z"], score["pct_below"], utc_now_iso(), src, lid))
        scored = conn.execute(
            """SELECT * FROM listings WHERE z IS NOT NULL AND scored_at >= ?
               ORDER BY z ASC LIMIT 10""",
            (started.strftime("%Y-%m-%dT%H:%M:%SZ"),)).fetchall()
        would_alert = 0
        if scored:
            print("   %-8s %-7s %-6s %-9s %-34s %s"
                  % ("z", "below", "comps", "price", "vehicle", "verdict"))
            for row in scored:
                d = dict(row)
                score = score_listing(conn, d)
                if score is None:
                    continue
                threshold = (scoring.Z_ALERT_DEALER if d.get("seller_type") == "Dealer"
                             else scoring.Z_ALERT.get(d["source"], -2.0))
                # Mirror maybe_alert exactly, including the two suppressions
                # that come after the gates — otherwise --test overstates what
                # would fire for listings already alerted or reposted.
                cutoff = (datetime.now(timezone.utc) - timedelta(
                    days=scoring.FINGERPRINT_TTL_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
                already = conn.execute(
                    "SELECT 1 FROM alerts WHERE source=? AND listing_id=?",
                    (d["source"], d["id"])).fetchone()
                repost = d.get("fingerprint") and conn.execute(
                    "SELECT 1 FROM listings WHERE fingerprint=? AND NOT (source=?"
                    " AND id=?) AND first_seen_at >= ? LIMIT 1",
                    (d["fingerprint"], d["source"], d["id"], cutoff)).fetchone()
                if score["suppress"]:
                    verdict = "gated: " + ",".join(score["suppress"])
                elif score["z"] > threshold:
                    verdict = "above threshold"
                elif already:
                    verdict = "already alerted"
                elif repost:
                    verdict = "repost/cross-post"
                else:
                    verdict = "would ALERT"
                    would_alert += 1
                print("   %-8.2f %-7s %-6d %-9s %-34s %s" % (
                    d["z"], "%d%%" % round(d["pct_below"]),
                    score["model"]["comp_count"], "$" + format(d["price"], ","),
                    vehicle_line(d)[:34], verdict))
        else:
            print("  nothing scored — no family or segment model covers these"
                  " listings yet (expected on a young database)")
        recorded = conn.execute(
            "SELECT COUNT(*) FROM alerts").fetchone()[0] - alerts_before
        print("  %d of the top %d would alert now; %d alert row(s) were recorded "
              "during the fetch itself (only brand-new listings can alert)"
              % (would_alert, len(scored), recorded))
        print("\n--- 5. COMP READINESS --------------------------------------------")
        readiness = comp_readiness(conn)
        for line in readiness:
            print("  " + line)

        print("\n--- 6. TELEGRAM --------------------------------------------------")
        suppressed = list(_SUPPRESSED_SENDS)
        _SUPPRESSED_SENDS = None
        summary = (
            "🧪 SELF-TEST %s\n"
            "Fetched %d listings across %d searches (%d new).\n"
            "Rejected %d (%s); %d clean.\n"
            "Models: %d family, %d segment. Would-fire alerts: %d.\n"
            "%d incidental warnings were suppressed during the test.\n"
            "The real database was not written to."
            % (started.strftime("%Y-%m-%d %H:%M UTC"), len(all_rows),
               len(SEARCH_URLS), len(new_rows), sum(reasons.values()),
               ", ".join("%s %d" % kv for kv in sorted(reasons.items())) or "none",
               clean, fams, segs, would_alert, len(suppressed)))
        ok = telegram_send(summary)
        print("  sent exactly 1 message: %s" % ("yes" if ok else
              "NO — Telegram not configured (message printed above/below)"))
        if not ok:
            print("  " + summary.replace("\n", "\n  "))
        if suppressed:
            print("  suppressed during test: %s"
                  % "; ".join(s.split("\n")[0][:60] for s in suppressed))
        print("\n" + "=" * 72)
        print("Real database untouched: %s" % DB_PATH)
        print("=" * 72)
    finally:
        _SUPPRESSED_SENDS = None
        try:
            conn.close()
        except Exception:
            pass
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(tmp_path + suffix)
            except OSError:
                pass


def comp_readiness(conn):
    """How close each family is to being trustworthy, and how fast it's growing."""
    out = []
    fam_counts = conn.execute(
        """SELECT family, COUNT(*) AS n FROM listings
           WHERE family IS NOT NULL AND reject_reason IS NULL
             AND COALESCE(is_damaged,0)=0 AND year IS NOT NULL
             AND km IS NOT NULL AND price IS NOT NULL
           GROUP BY family ORDER BY n DESC""").fetchall()
    if not fam_counts:
        return ["no clean comps stored yet"]
    ge8 = sum(1 for f in fam_counts if f["n"] >= scoring.MIN_COMPS_GATE)
    ge30 = sum(1 for f in fam_counts if f["n"] >= scoring.FAMILY_MODEL_MIN_COMPS)
    out.append("%d families with clean comps; %d have >=%d (gate), %d have >=%d "
               "(own model)" % (len(fam_counts), ge8, scoring.MIN_COMPS_GATE,
                                ge30, scoring.FAMILY_MODEL_MIN_COMPS))
    out.append("largest: " + ", ".join("%s %d" % (f["family"].split(":", 1)[-1], f["n"])
                                       for f in fam_counts[:6]))
    # Arrival rate must exclude the seeding burst: the first cycle stores a
    # whole page at once, and extrapolating that gives absurd projections.
    first = conn.execute(
        "SELECT MIN(first_seen_at) FROM listings").fetchone()[0]
    if not first:
        return out
    try:
        seed_end = (datetime.strptime(first, "%Y-%m-%dT%H:%M:%SZ")
                    + timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        return out
    post = conn.execute(
        """SELECT COUNT(*) AS n, MAX(first_seen_at) AS last FROM listings
           WHERE reject_reason IS NULL AND first_seen_at > ?""", (seed_end,)).fetchone()
    try:
        hours = (datetime.strptime(post["last"], "%Y-%m-%dT%H:%M:%SZ")
                 - datetime.strptime(seed_end, "%Y-%m-%dT%H:%M:%SZ")).total_seconds() / 3600.0
    except (TypeError, ValueError):
        hours = 0.0
    if hours < 2.0 or not post["n"]:
        out.append("not enough post-seed history to measure the arrival rate yet "
                   "(need a few hours of running; the first cycle's burst would "
                   "skew any estimate)")
        return out
    per_day = post["n"] / hours * 24
    out.append("clean comps arriving at ~%.0f/day (measured over %.1fh after the "
               "seed cycle)" % (per_day, hours))
    biggest = fam_counts[0]
    need = max(0, scoring.FAMILY_MODEL_MIN_COMPS - biggest["n"])
    total_clean = sum(f["n"] for f in fam_counts)
    share = biggest["n"] / float(total_clean) if total_clean else 0
    if need and per_day * share > 0:
        out.append("%s needs ~%d more comps: ~%.0f days at its share of that rate"
                   % (biggest["family"].split(":", 1)[-1], need,
                      need / (per_day * share)))
    return out


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
                   help="send the daily digest now and exit")
    p.add_argument("--report", action="store_true",
                   help="send the weekly report now and exit")
    p.add_argument("--test", action="store_true",
                   help="one live fetch end to end against a sandboxed copy of "
                        "the database; sends exactly one Telegram message")
    args = p.parse_args()
    if args.test:
        run_test()
        return
    if args.label:
        raise SystemExit(label_alert(args.label[0], args.label[1]))
    if args.digest or args.report:
        conn = init_db(DB_PATH)
        migrate_db(conn)
        if args.digest:
            send_daily_digest(conn, manual=True)
        if args.report:
            send_weekly_report(conn)
        return
    run_watcher()


if __name__ == "__main__":
    main()
