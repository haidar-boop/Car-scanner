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
# maybe_alert()'s own send can be in flight (up to the 15s HTTP timeout) when
# notified_at is still NULL — retry_failed_alerts() must not race it. A row
# only becomes retry-eligible once it's older than this, well past worst case.
ALERT_RETRY_GRACE_S = 60
ALERT_RETRY_SPACING_S = 1.0         # match maybe_alert's ~1 msg/s pacing
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

# AI verification layer — the FINAL gate before a non-shadow alert is sent,
# strictly after the price-score gate, blocklist and rejection layer have all
# passed. It runs on the 1-5 listings/day that clear those, never on the ~300
# that don't; that is what keeps it ~$1-5/month. Fail-open by design: no key,
# no SDK, an API error or a timeout all degrade to today's behavior (send the
# alert unchanged). Requires the optional `anthropic` package — a deliberate
# addition beyond the original requests/bs4/numpy-only constraint.
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
AI_VERIFY_ENABLED = os.environ.get("AI_VERIFY_ENABLED", "1") != "0"
AI_MODEL = "claude-opus-5"
AI_TIMEOUT_S = 60.0                 # bounded delay; on timeout the alert sends unchanged
AI_MAX_TOKENS = 4000                # covers adaptive thinking + the small verdict JSON
AI_MAX_PHOTOS = 5

VERIFY_WARN_COOLDOWN_S = 6 * 3600   # max one search-health warning per label per 6h

# FB->droplet ingest bridge. Empty token = server disabled (never runs open).
INGEST_TOKEN = os.environ.get("INGEST_TOKEN", "")
INGEST_BIND = os.environ.get("INGEST_BIND", "0.0.0.0:8477")
# 1 MB, not 256 KB: enriched items carry ~1.5 KB descriptions, and a batch
# that exceeded this cap would 413 and be retried by the userscript forever
# — a permanent silent ingest outage. The userscript batches at 60 items
# (~150 KB worst case) so there is deliberate headroom on both sides.
INGEST_MAX_BODY = 1024 * 1024
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
DIGEST_MAX_LOOKBACK_DAYS = 7           # cap after an outage; never a forward snap
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


# The search JSON's images[] carry a size suffix after the extension
# ("....png/250x188.w") — a 250px thumbnail, useless for damage detection,
# and the resizer 400s for off-site fetches anyway. The bare URL (suffix
# stripped) serves the full-resolution original. Verified live 2026-08.
_PHOTO_SIZE_SUFFIX_RE = re.compile(r"(\.(?:jpe?g|png|webp))/.*$", re.I)


def parse_photos(images):
    """AutoTrader images[] -> JSON array of <=5 full-res URLs, or None."""
    photos = []
    for u in (images or []):
        if isinstance(u, str) and u.startswith("https://"):
            photos.append(_PHOTO_SIZE_SUFFIX_RE.sub(r"\1", u)[:400])
            if len(photos) >= AI_MAX_PHOTOS:
                break
    return json.dumps(photos) if photos else None


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
            mv = vehicle.get("modelVersionInput")
            title = " ".join(str(x) for x in (year, make, model) if x)
            description = strip_html(l.get("description"))
            # title=None on purpose: the synthesized "year make model" can
            # only ever match model-name tokens (a Lexus LS is not an LS
            # trim) — pure noise as a trim source.
            trim_tier, drivetrain = scoring.extract_features(
                make, mv, None, description)
            rows.append({
                "id": str(listing_id),
                "source": "autotrader",
                "url": l.get("url"),
                "title": title,
                "price": price,
                "year": year,
                "make": make,
                "model": model,
                "km": parse_km(vehicle.get("mileageInKm")),
                "seller_type": seller.get("type"),
                "city": location.get("city"),
                "distance_km": location.get("distanceToSearchLocationInKm"),
                "description": description,
                "is_damaged": 1 if vehicle.get("isCurrentlyDamaged") else 0,
                "result_type": l.get("searchResultType"),
                "price_label": (l.get("tracking") or {}).get("priceLabel"),
                "search_label": search_label,
                "raw_json": json.dumps(l),
                "trim_tier": trim_tier,
                "drivetrain": drivetrain,
                "model_version": (str(mv)[:200] if mv else None),
                "photos": parse_photos(l.get("images")),
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

# Trim/drivetrain awareness (see scoring.py lexicon).
STEP6_LISTING_COLUMNS = {
    "trim_tier": "INTEGER",      # 0 base / 1 mid / 2 premium / NULL unknown
    "drivetrain": "TEXT",        # 'awd' | 'fwd' | NULL
    "model_version": "TEXT",     # raw vehicle.modelVersionInput, for audit
}
STEP6_MODEL_COLUMNS = {
    "offsets_json": "TEXT",      # {'trim': {tier: {off,n}}, 'drivetrain': ...}
    "mad_adj": "REAL",           # MAD after offsets explained their variance
}
STEP6_ALERT_COLUMNS = {
    "adjustments": "TEXT",       # JSON snapshot of offsets applied at firing
}
# FB item-page enrichment. NULL = pre-enrichment row (no backfill possible).
STEP7_LISTING_COLUMNS = {
    "enrich_status": "TEXT",     # 'ok'|'partial'|'failed'|'skipped'|NULL
}
# NULL (non-shadow) = fired but never successfully delivered — a Telegram
# outage at fire time must not lose the alert, so the row is committed
# regardless and retry_failed_alerts() sweeps anything still NULL. Shadow
# alerts are never sent, so they stay NULL forever by design.
STEP8_ALERT_COLUMNS = {
    "notified_at": "TEXT",
}
# AI verification layer. photos = JSON array of up to 5 listing photo URLs
# (AutoTrader: from the embedded search JSON; FB: from item-page enrichment,
# signed URLs that expire in hours — fine, the AI check runs minutes after
# discovery, never on a backfill). ai_verdict/ai_summary record the verdict
# on the alert row so a 'reject' is auditable, excluded from the retry sweep,
# and a 'caution' summary survives into retried messages.
STEP9_LISTING_COLUMNS = {
    "photos": "TEXT",
}
STEP9_ALERT_COLUMNS = {
    "ai_verdict": "TEXT",        # 'clear'|'caution'|'reject'|NULL (not checked)
    "ai_summary": "TEXT",
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
    for table, decls in (("listings", STEP6_LISTING_COLUMNS),
                         ("models", STEP6_MODEL_COLUMNS),
                         ("alerts", STEP6_ALERT_COLUMNS),
                         ("listings", STEP7_LISTING_COLUMNS),
                         ("alerts", STEP8_ALERT_COLUMNS),
                         ("listings", STEP9_LISTING_COLUMNS),
                         ("alerts", STEP9_ALERT_COLUMNS)):
        have = {r[1] for r in conn.execute("PRAGMA table_info(%s)" % table)}
        with conn:
            for name, decl in decls.items():
                if name not in have:
                    conn.execute("ALTER TABLE %s ADD COLUMN %s %s"
                                 % (table, name, decl))

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

    # Versioned trim/drivetrain extraction: runs once per lexicon version and
    # OVERWRITES existing values — that is what makes editing the lexicon and
    # bumping TRIM_LEXICON_VERSION re-classify history.
    if meta_get(conn, "trim_lexicon_v") != str(scoring.TRIM_LEXICON_VERSION):
        n = failed = 0
        rows = conn.execute(
            "SELECT source, id, make, model, title, description, raw_json"
            " FROM listings").fetchall()
        with conn:
            for r in rows:
                # One malformed historical row must never crash-loop the
                # watcher at startup — skip it, keep migrating.
                try:
                    mv, title = None, r["title"]
                    if r["raw_json"]:
                        try:
                            raw = json.loads(r["raw_json"])
                        except (ValueError, TypeError):
                            raw = {}
                        if r["source"] == "autotrader":
                            mv = (raw.get("vehicle") or {}).get("modelVersionInput")
                            title = None  # synthesized "year make model": noise
                        else:
                            # Re-extract from what live extraction actually
                            # saw: the enriched fb_title when one was
                            # accepted (same year cross-check as
                            # parse_fb_listing), else the full scraped text
                            # from raw_json — never just the 200-char title
                            # column. A version bump must not degrade rows.
                            text = raw.get("text") or r["title"]
                            fb_title = (raw.get("enrich") or {}).get("fb_title")
                            title = text
                            if fb_title:
                                yt = scoring.parse_year_from_text(text or "")
                                yf = scoring.parse_year_from_text(fb_title)
                                if yt is None or yf is None or yt == yf:
                                    title = fb_title
                    exclude = ()
                    if r["source"] == "facebook" and r["model"]:
                        exclude = (r["model"],)
                    tier, dt = scoring.extract_features(
                        r["make"], mv, title, r["description"],
                        exclude_tokens=exclude)
                    conn.execute(
                        "UPDATE listings SET trim_tier=?, drivetrain=?,"
                        " model_version=? WHERE source=? AND id=?",
                        (tier, dt, (str(mv)[:200] if mv else None),
                         r["source"], r["id"]))
                    n += 1
                except Exception:
                    failed += 1
        meta_set(conn, "trim_lexicon_v", str(scoring.TRIM_LEXICON_VERSION))
        log("migration: extracted trim/drivetrain for %d rows (lexicon v%d%s)"
            % (n, scoring.TRIM_LEXICON_VERSION,
               ", %d skipped" % failed if failed else ""))


def store_listings(conn, rows):
    """Upsert rows; returns (new_rows, reassess_rows).

    new_rows are never-seen-before listings. reassess_rows are previously
    stored listings whose price just changed or which just gained
    enrichment — callers must run these through process_new_rows too (with
    the same allow_alerts they'd give new_rows from this pass), or a price
    cut can never score or alert: the UPDATE below wipes z/pct_below/
    scored_at, but nothing else ever recomputes them. Each reassess row is
    the fully merged dict (whatever the DB already knew filled in), so
    downstream scoring sees the same values just written to the row.

    Every observed price lands in price_history (including the first), so the
    original asking price survives later in-place updates of listings.price.
    """
    now = utc_now_iso()
    new_rows = []
    reassess_rows = []
    with conn:
        for r in rows:
            cur = conn.execute(
                "SELECT price, description, km, enrich_status, trim_tier,"
                " drivetrain, year, make, model, title, photos FROM listings"
                " WHERE source=? AND id=?",
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
                        km_converted_from_miles, trim_tier, drivetrain,
                        model_version, enrich_status, photos)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (r["id"], r["source"], r["url"], r["title"], r["price"],
                     r["year"], r["make"], r["model"], r["km"], r["seller_type"],
                     r["city"], r["distance_km"], r["description"], r["is_damaged"],
                     r["result_type"], r["price_label"], r["search_label"],
                     now, now, r["raw_json"], r.get("km_converted_from_miles"),
                     r.get("trim_tier"), r.get("drivetrain"),
                     r.get("model_version"), r.get("enrich_status"),
                     r.get("photos")),
                )
                conn.execute(
                    "INSERT INTO price_history (source, id, price, seen_at)"
                    " VALUES (?,?,?,?)",
                    (r["source"], r["id"], r["price"], now),
                )
                new_rows.append(r)
            else:
                price_changed = existing["price"] != r["price"]
                incoming_enriched = r.get("enrich_status") in ("ok", "partial")
                already_enriched = existing["enrich_status"] in ("ok", "partial")
                if price_changed:
                    conn.execute(
                        "INSERT INTO price_history (source, id, price, seen_at)"
                        " VALUES (?,?,?,?)",
                        (r["source"], r["id"], r["price"], now),
                    )
                if price_changed or (incoming_enriched and not already_enriched):
                    # Re-assess on a price change (an edit to $111 must not
                    # sit in the comp pool as clean forever) AND on enrichment
                    # arriving for a row first seen bare — otherwise a
                    # rebuilt-title description that shows up on a re-detected
                    # listing at an unchanged price would never reach the
                    # blocklist at all.
                    # The merge runs both ways: incoming values win, stored
                    # values fill the gaps — so a bare repost can't launder
                    # away an enrichment-earned verdict, and late enrichment
                    # can't leave year/make/model as NULL zombies that pass
                    # assess but are invisible to COMP_SQL.
                    merged = dict(r)
                    for col in ("description", "km", "trim_tier", "drivetrain",
                                "year", "make", "model", "title", "photos"):
                        if merged.get(col) is None:
                            merged[col] = existing[col]
                    keep_status = existing["enrich_status"]
                    if merged.get("enrich_status") in ("ok", "partial") \
                            or keep_status not in ("ok", "partial"):
                        keep_status = merged.get("enrich_status")
                    family, fp, reason = scoring.assess(
                        merged, datetime.now(timezone.utc).year)
                    conn.execute(
                        "UPDATE listings SET last_seen_at=?, price=?, raw_json=?,"
                        " family=?, fingerprint=?, reject_reason=?,"
                        " trim_tier=?, drivetrain=?, model_version=?,"
                        " description=?, km=?, enrich_status=?, photos=?,"
                        " year=?, make=?, model=?, title=?,"
                        " z=NULL, pct_below=NULL, scored_at=NULL"
                        " WHERE source=? AND id=?",
                        (now, r["price"], r["raw_json"], family, fp, reason,
                         merged.get("trim_tier"), merged.get("drivetrain"),
                         merged.get("model_version"), merged.get("description"),
                         merged.get("km"), keep_status, merged.get("photos"),
                         merged.get("year"), merged.get("make"),
                         merged.get("model"), merged.get("title"),
                         r["source"], r["id"]),
                    )
                    reassess_rows.append(merged)
                elif already_enriched and not incoming_enriched:
                    # Bare repost of an enriched row at the same price: keep
                    # the enriched raw_json (it carries the fb_title the
                    # lexicon backfill re-extracts from).
                    conn.execute(
                        "UPDATE listings SET last_seen_at=? WHERE source=? AND id=?",
                        (now, r["source"], r["id"]),
                    )
                else:
                    conn.execute(
                        "UPDATE listings SET last_seen_at=?, raw_json=?"
                        " WHERE source=? AND id=?",
                        (now, r["raw_json"], r["source"], r["id"]),
                    )
    return new_rows, reassess_rows


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
    adj_labels = {"awd": "4x4/AWD", "fwd": "2WD"}
    adj_bits = "".join(
        " · %s %+.0f%%" % (
            scoring.TRIM_TIER_NAMES.get(level, level) if feature == "trim"
            else adj_labels.get(level, level),
            (math.exp(delta) - 1) * 100)
        for feature, level, delta in score.get("adjustments") or [])
    lines = [
        head,
        "predicted $%s from %d comps (%s%s)" % (
            format(predicted_price, ","), score["model"]["comp_count"],
            score["model"]["model_key"].split(":", 1)[-1], adj_bits),
        "%s · %s · %s · %s" % (
            km, row.get("seller_type") or "seller n/a",
            row.get("city") or "city n/a", format_age(minutes_since(first_seen_at))),
    ]
    marks = []
    if score["low_confidence"]:
        marks.append("⚠️ LOW CONFIDENCE — segment model, too few comps for this family")
    if row.get("seller_type") == "Dealer":
        marks.append("🏪 DEALER — priced by a pro, check for a catch")
    if score.get("unpriced_features"):
        marks.append("⚠️ %s listing, curve not adjusted (few same-spec comps)"
                     " — discount may read high"
                     % "/".join(score["unpriced_features"]))
    if marks:
        lines.append(" · ".join(marks))
    lines.append(row.get("url") or "")   # URL last so it stays tappable
    return "\n".join(l for l in lines if l)


def _insert_before_url(msg, note):
    """Add a line to an alert message ABOVE its trailing URL — format_alert
    deliberately puts the URL last so it stays tappable on a phone, and an
    appended AI note must not break that."""
    head, _, tail = msg.rpartition("\n")
    if head and tail.startswith("http"):
        return head + "\n" + note + "\n" + tail
    return msg + "\n" + note


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
        d = dict(row)
        d["offsets"] = scoring.unpack_offsets(d.get("offsets_json"))
        return d, False
    segment = scoring.SEGMENT_MAP.get(search_label)
    if segment:
        row = conn.execute(
            "SELECT * FROM models WHERE model_key=?", ("segment:%s" % segment,)
        ).fetchone()
        if row:
            d = dict(row)
            d["offsets"] = scoring.unpack_offsets(d.get("offsets_json"))
            return d, True
    return None


def score_listing(conn, row):
    picked = get_model_for(conn, row["family"], row.get("search_label"))
    if picked is None:
        return None
    model_row, low_confidence = picked
    age = scoring.age_of(row["year"])
    tier, drivetrain = row.get("trim_tier"), row.get("drivetrain")
    z, pct_below = scoring.robust_z(row["price"], model_row, age, row["km"],
                                    tier, drivetrain)
    adjustments = scoring.applicable_offsets(model_row, tier, drivetrain)
    predicted = int(round(math.exp(
        scoring.predict_log_price(model_row, age, row["km"])
        + sum(a[2] for a in adjustments))))
    # Known-but-unadjusted features matter only in the false-positive
    # direction: a base-spec / 2WD listing scored against the blended curve
    # reads cheaper than it is. (Premium/awd unadjusted reads expensive —
    # conservative — so no flag.)
    offsets = model_row.get("offsets") or {}
    unpriced = []
    if tier == 0 and 0 not in (offsets.get("trim") or {}):
        unpriced.append("base trim")
    if drivetrain == "fwd" and "fwd" not in (offsets.get("drivetrain") or {}):
        unpriced.append("2WD")
    suppress = scoring.gate(model_row, age, row["km"], utc_now_iso(), model_row["kind"])
    return {"z": z, "pct_below": pct_below, "model": model_row,
            "low_confidence": low_confidence, "suppress": suppress,
            "adjustments": adjustments, "predicted_price": predicted,
            "unpriced_features": unpriced}


# --- AI verification (final gate, strictly after all other filters) ---------

AI_SYSTEM_PROMPT = """\
You are the final human-facing check in a used-car deal-alert system for
Edmonton, Alberta. Every listing you see has already passed a statistical
price filter: it is priced significantly below what comparable vehicles
(same make/model/year/mileage/trim) are asking. Your job is NOT to judge
whether the price is good — that's already been decided. Your job is to
judge whether there's a visible reason the price is low that a human
buyer would want to know about before driving out to see it.

You will be given the listing's title, full description text, asking
price, statistical discount (z-score and percent below predicted), and
up to 5 photos from the listing.

Look for, using BOTH the text and the photos:
1. VISIBLE DAMAGE — dents, rust, mismatched paint, cracked glass, bent
   panels, missing trim, warning lights on the dash in an interior shot,
   flood/water lines, anything a buyer should be warned about before
   going to see the car in person.
2. UNDISCLOSED MECHANICAL ISSUES mentioned in the text but easy to miss —
   "needs a transmission," "runs but," "sold as-is for parts," "check
   engine light," "head gasket," etc., especially phrased in ways a
   simple keyword filter would miss (typos, French, slang, indirect
   phrasing).
3. SALVAGE / REBUILT / INSURANCE SIGNALS not caught by an exact-keyword
   list — "clean bill from insurance after," "back on the road after,"
   "no accident *reported*," odd phrasing that hints at a branded title
   without using the word.
4. SCAM PATTERNS — a stock/dealer/brochure photo paired with a private
   seller price; photos that don't match the described trim, color, or
   interior; seller pushing off-platform contact, wire transfer,
   shipping, "car is out of country," urgency pressure, or refusing a
   test drive; price that is dramatically below EVERY comparable, not
   just modestly below.
5. PHOTO/LISTING MISMATCH — interior condition inconsistent with the
   claimed mileage, a visibly different vehicle in one of the photos,
   watermarks from another marketplace or dealer site.

You are the LAST gate before this alert reaches a human's phone. Default
to letting it through. A missed warning costs the buyer ten minutes of
looking at a bad car in person. A wrongly suppressed alert costs them a
genuinely good deal they'll never know they missed — that is the more
expensive mistake. Only escalate to REJECT when you are confident this
is fraudulent, not merely imperfect.

Respond with a structured verdict — no prose outside the fields:

- damage_visible: true/false
- damage_notes: one short phrase, or null
- scam_risk: "none" | "low" | "medium" | "high"
- scam_notes: one short phrase, or null
- undisclosed_issues: one short phrase quoting/paraphrasing the listing, or null
- verdict: "clear" | "caution" | "reject"
    clear   = nothing notable, send the alert as-is
    caution = send the alert, but attach a one-line warning
    reject  = do not alert; this is very likely fraudulent or the
              vehicle is materially misrepresented (reserve for high
              scam_risk or unmistakable damage the title/price implies
              should be pristine)
- summary: ONE short line (under 100 characters) suitable for appending
  directly to a Telegram message, e.g. "clean, no red flags" or
  "⚠️ rear quarter panel damage visible in photo 3" or
  "⚠️ stock photos, private-seller price — verify in person"
"""

AI_USER_TEMPLATE = """\
Listing under review — already passed the price-score filter.

Vehicle: %(vehicle)s
Asking price: $%(price)s
Statistical read: %(pct_below)d%% below predicted ($%(predicted)s), z=%(z).2f
Seller type: %(seller_type)s
Source: %(source)s (%(url)s)

Title as posted:
%(title)s

Full description as posted:
%(description)s

Evaluate per your instructions and return the structured verdict."""

AI_SCHEMA = {
    "type": "object",
    "properties": {
        "damage_visible": {"type": "boolean"},
        "damage_notes": {"type": ["string", "null"]},
        "scam_risk": {"type": "string", "enum": ["none", "low", "medium", "high"]},
        "scam_notes": {"type": ["string", "null"]},
        "undisclosed_issues": {"type": ["string", "null"]},
        "verdict": {"type": "string", "enum": ["clear", "caution", "reject"]},
        "summary": {"type": "string"},
    },
    "required": ["damage_visible", "damage_notes", "scam_risk", "scam_notes",
                 "undisclosed_issues", "verdict", "summary"],
    "additionalProperties": False,
}

_AI_UNAVAILABLE_LOGGED = False


def ai_verify(row, score, predicted):
    """One Claude call on an alert-bound listing. Returns the verdict dict or
    None — and None ALWAYS means "send the alert unchanged" (fail-open).

    Runs strictly AFTER the price gate, blocklist and rejection layer: only
    maybe_alert's non-shadow path calls this, so it sees the 1-5 listings a
    day that earned an alert, never the ~300 that didn't. Every failure mode
    (kill switch, no key, SDK missing, API error, timeout, malformed reply)
    degrades to today's behavior rather than suppressing or delaying a real
    alert beyond the bounded timeout.
    """
    global _AI_UNAVAILABLE_LOGGED
    if not AI_VERIFY_ENABLED or not ANTHROPIC_API_KEY:
        return None
    if _SUPPRESSED_SENDS is not None:
        return None  # --test must never spend real API money
    try:
        import anthropic
    except ImportError:
        if not _AI_UNAVAILABLE_LOGGED:
            _AI_UNAVAILABLE_LOGGED = True
            log("ai verify unavailable: anthropic SDK not installed"
                " (pip install anthropic) — alerts send unchanged")
        return None
    try:
        photos = row.get("photos")
        if isinstance(photos, str):
            try:
                photos = json.loads(photos)
            except ValueError:
                photos = None
        if not isinstance(photos, list):
            photos = []
        photos = [u for u in photos
                  if isinstance(u, str) and u.startswith("https://")][:AI_MAX_PHOTOS]
        content = [{"type": "image", "source": {"type": "url", "url": u}}
                   for u in photos]
        content.append({"type": "text", "text": AI_USER_TEMPLATE % {
            "vehicle": vehicle_line(row),
            "price": format(row["price"], ","),
            "pct_below": round(score["pct_below"]),
            "predicted": format(predicted, ","),
            "z": score["z"],
            "seller_type": row.get("seller_type") or "n/a",
            "source": row.get("source") or "n/a",
            "url": row.get("url") or "n/a",
            "title": (row.get("title") or "(none)")[:300],
            "description": (row.get("description") or "(no description)")[:2000],
        }})
        client = anthropic.Anthropic(
            api_key=ANTHROPIC_API_KEY, timeout=AI_TIMEOUT_S, max_retries=0)
        resp = client.messages.create(
            model=AI_MODEL,
            max_tokens=AI_MAX_TOKENS,
            thinking={"type": "adaptive"},
            system=AI_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": content}],
            output_config={"format": {"type": "json_schema", "schema": AI_SCHEMA}},
        )
        text = next(b.text for b in resp.content if b.type == "text")
        out = json.loads(text)
        if out.get("verdict") not in ("clear", "caution", "reject"):
            return None
        out["summary"] = str(out.get("summary") or "")[:160]
        return out
    except Exception as e:
        # Bounded-delay fail-open: timeouts, 4xx/5xx, network, bad JSON.
        log("ai verify failed (%s) — sending alert unchanged"
            % e.__class__.__name__)
        return None


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
    # The displayed prediction must be the one the z was computed against —
    # score_listing already folded the trim/drivetrain offsets in.
    predicted = score["predicted_price"]
    now = utc_now_iso()
    shadow_until = meta_get(conn, "shadow_until") or ""
    shadow = 1 if now < shadow_until else 0
    try:
        with conn:
            cur = conn.execute(
                """INSERT INTO alerts (source, listing_id, fired_at, z, pct_below,
                       price, predicted_price, model_key, b0, b1, b2, mad,
                       comp_count, is_dealer, low_confidence, shadow, adjustments)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (src, lid, now, score["z"], score["pct_below"], row["price"],
                 predicted, m["model_key"], m["b0"], m["b1"], m["b2"], m["mad"],
                 m["comp_count"], 1 if row.get("seller_type") == "Dealer" else 0,
                 1 if score["low_confidence"] else 0, shadow,
                 json.dumps(score["adjustments"])),
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
        low_confidence=bool(score["low_confidence"]),
        adjustments=len(score["adjustments"]))
    if not shadow:
        # AI verification — the FINAL gate, after every other filter has
        # already passed. ai_verify is fail-open: None = send unchanged.
        ai = ai_verify(row, score, predicted)
        if ai:
            with conn:
                conn.execute(
                    "UPDATE alerts SET ai_verdict=?, ai_summary=?"
                    " WHERE alert_id=?",
                    (ai["verdict"], ai.get("summary") or None, cur.lastrowid))
        if ai and ai["verdict"] == "reject":
            # Suppressed, not vanished: the alert row above keeps the full
            # score plus the verdict for later audit (--label still works on
            # it), and retry_failed_alerts explicitly skips rejects so this
            # never resurfaces as a "pending" send.
            log("[ai_reject] #%d %s/%s: %s"
                % (cur.lastrowid, src, lid, ai.get("summary") or "no summary"),
                event="ai_reject", alert_id=cur.lastrowid, source=src,
                listing_id=lid, summary=ai.get("summary") or "",
                scam_risk=ai.get("scam_risk"),
                damage_visible=bool(ai.get("damage_visible")))
            return
        seen_row = conn.execute(
            "SELECT first_seen_at FROM listings WHERE source=? AND id=?",
            (src, lid)).fetchone()
        msg = format_alert(row, score, predicted, shadow,
                           seen_row["first_seen_at"] if seen_row else None)
        if ai and ai["verdict"] == "caution" and ai.get("summary"):
            msg = _insert_before_url(msg, "🤖 " + ai["summary"])
        delivered = telegram_send(msg)
        # notified_at stays NULL on failure — the alert row is already
        # committed above (it must survive a Telegram outage), and
        # retry_failed_alerts() sweeps anything still NULL every cycle.
        if delivered:
            with conn:
                conn.execute("UPDATE alerts SET notified_at=? WHERE alert_id=?",
                             (utc_now_iso(), cur.lastrowid))
        else:
            log("[alert] #%d %s/%s: telegram send failed, will retry"
                % (cur.lastrowid, src, lid))
        time.sleep(1.0)


def retry_failed_alerts(conn):
    """Sweep alerts that fired but were never successfully delivered.

    maybe_alert commits an alert row before it knows whether Telegram is
    reachable, so a network blip must not lose the message: the row stays
    notified_at=NULL until a send succeeds, and this runs every cycle to
    retry it. Reconstructed from the stored alert plus a fresh read of the
    listing (km/url/city/etc aren't duplicated into the alerts table).
    score.unpriced_features isn't persisted, so a retried message can be
    missing that one advisory line — every decision-relevant number
    (price, z, discount, comps) is intact.

    Excludes anything younger than ALERT_RETRY_GRACE_S: maybe_alert's own
    send can still be in flight (blocking on the HTTP timeout) with
    notified_at still NULL — sweeping it here too would double-send. And
    each retry sleeps ALERT_RETRY_SPACING_S like maybe_alert does, so a
    backlog of several pending alerts doesn't fire in a burst that trips
    Telegram's per-chat rate limit.
    """
    cutoff = (datetime.now(timezone.utc)
              - timedelta(seconds=ALERT_RETRY_GRACE_S)).strftime("%Y-%m-%dT%H:%M:%SZ")
    # ai_verdict='reject' rows are DELIBERATE suppressions, not failed sends
    # — sweeping them would undo the AI gate on the very next cycle.
    rows = conn.execute(
        """SELECT a.alert_id, a.source, a.listing_id, a.z, a.pct_below,
                  a.price, a.predicted_price, a.model_key, a.comp_count,
                  a.low_confidence, a.adjustments, a.ai_verdict, a.ai_summary,
                  l.km, l.url, l.city, l.seller_type, l.title,
                  l.year, l.make, l.model, l.first_seen_at
           FROM alerts a JOIN listings l
             ON l.source = a.source AND l.id = a.listing_id
           WHERE a.shadow = 0 AND a.notified_at IS NULL AND a.fired_at <= ?
             AND (a.ai_verdict IS NULL OR a.ai_verdict != 'reject')""",
        (cutoff,)
    ).fetchall()
    for r in rows:
        row = {"km": r["km"], "url": r["url"], "city": r["city"],
               "seller_type": r["seller_type"], "title": r["title"],
               "year": r["year"], "make": r["make"], "model": r["model"],
               "price": r["price"]}
        score = {"pct_below": r["pct_below"], "z": r["z"],
                 "model": {"model_key": r["model_key"],
                           "comp_count": r["comp_count"]},
                 "low_confidence": bool(r["low_confidence"]),
                 "adjustments": json.loads(r["adjustments"]) if r["adjustments"] else []}
        msg = format_alert(row, score, r["predicted_price"], False,
                           r["first_seen_at"])
        if r["ai_verdict"] == "caution" and r["ai_summary"]:
            msg = _insert_before_url(msg, "🤖 " + r["ai_summary"])
        delivered = telegram_send(msg)
        if delivered:
            with conn:
                conn.execute("UPDATE alerts SET notified_at=? WHERE alert_id=?",
                             (utc_now_iso(), r["alert_id"]))
            log("[alert] #%d %s/%s: retry delivered"
                % (r["alert_id"], r["source"], r["listing_id"]))
        else:
            log("[alert] #%d %s/%s: retry still failing"
                % (r["alert_id"], r["source"], r["listing_id"]))
        time.sleep(ALERT_RETRY_SPACING_S)


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
            new_rows, reassess_rows = store_listings(conn, rows)  # comp data is comp data — always store
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
                process_new_rows(conn, reassess_rows, allow_alerts=False)
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
            process_new_rows(conn, reassess_rows, allow_alerts=not seeding)
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
    # The lookback is capped so one long outage can't build an enormous
    # digest — but it must never snap FORWARD to a fresh 24h window, which
    # is what the old 2-day floor did: a single missed evening orphaned
    # [last_digest_at, now-24h] into a gap no digest ever covered, silently.
    # Poll jitter alone pushed the gap past 48h routinely. When the cap
    # bites, the digest says so instead of dropping the period in silence.
    since = meta_get(conn, "last_digest_at")
    truncated_from = None
    cap = (datetime.now(timezone.utc)
           - timedelta(days=DIGEST_MAX_LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    if not since:
        since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif since < cap:
        truncated_from, since = since, cap
    scans = conn.execute(
        """SELECT source, search_label, SUM(parsed_count) AS parsed
           FROM scans WHERE scanned_at >= ? GROUP BY source, search_label
           ORDER BY source, search_label""", (since,)).fetchall()
    by_source = {}
    for s in scans:
        by_source[s["source"]] = by_source.get(s["source"], 0) + (s["parsed"] or 0)

    alerts = conn.execute(
        "SELECT shadow, notified_at, ai_verdict FROM alerts WHERE fired_at >= ?",
        (since,)).fetchall()
    # notified_at distinguishes an actually-delivered alert from one that
    # fired but is still waiting on retry_failed_alerts() — a Telegram
    # outage must show up here, not read as "sent" when it wasn't. An
    # AI-rejected alert is a deliberate suppression, not a pending send.
    sent = sum(1 for a in alerts if not a["shadow"] and a["notified_at"])
    ai_rejected = sum(1 for a in alerts
                      if not a["shadow"] and a["ai_verdict"] == "reject")
    pending = sum(1 for a in alerts if not a["shadow"] and not a["notified_at"]
                  and a["ai_verdict"] != "reject")
    shadowed = sum(1 for a in alerts if a["shadow"])

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

    def _local(iso_str, fmt="%a %H:%M"):
        """UTC-stored timestamp -> local wall clock. Every time shown to the
        user is local; mixing in a raw UTC string made the truncation notice
        contradict the header directly above it by the 6-7h offset."""
        try:
            return (datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
                    .replace(tzinfo=timezone.utc).astimezone(TZ).strftime(fmt))
        except (ValueError, TypeError):
            return iso_str or "?"

    lines = ["📊 DIGEST — %s (since %s)" % (
        datetime.now(TZ).strftime("%a %d %b"), _local(since))]
    if truncated_from:
        lines.append("⚠️ window capped at %d days — %s to %s went unreported "
                     "(digest was down that long)"
                     % (DIGEST_MAX_LOOKBACK_DAYS,
                        _local(truncated_from, "%a %d %b %H:%M"),
                        _local(since, "%a %d %b %H:%M")))
    lines.append("Scanned: " + (" · ".join(
        "%s %d" % (src, n) for src, n in sorted(by_source.items())) or "nothing"))
    if scans:
        # source-tagged: both sources use a 'cars_2k_15k' label, and an
        # untagged breakdown makes a dead search look like a live one
        tag = {"autotrader": "at", "facebook": "fb"}
        lines.append("  " + " · ".join(
            "%s:%s %d" % (tag.get(s["source"], s["source"]), s["search_label"],
                          s["parsed"] or 0) for s in scans))
    lines.append("Alerts: %d sent · %d shadow%s%s" % (
        sent, shadowed,
        " · %d pending retry" % pending if pending else "",
        " · %d ai-rejected" % ai_rejected if ai_rejected else ""))
    lines.append("Rejected %d: %s" % (
        sum(by_reason.values()),
        " · ".join("%s %d" % kv for kv in sorted(
            by_reason.items(), key=lambda kv: -kv[1])) or "none"))

    fb = conn.execute(
        """SELECT COUNT(*) AS n,
                  SUM(enrich_status='ok') AS ok,
                  SUM(enrich_status='partial') AS part,
                  SUM(enrich_status='failed') AS fail,
                  SUM(enrich_status='skipped') AS skip,
                  SUM(km IS NOT NULL) AS has_km,
                  SUM(description IS NOT NULL) AS has_desc
           FROM listings WHERE source='facebook' AND first_seen_at >= ?""",
        (since,)).fetchone()
    if fb["n"]:
        # A collapsing ok%% here is the "FB changed their item-page JSON"
        # alarm — the extractor keys need updating.
        pct = lambda x: round(100 * (x or 0) / fb["n"])
        lines.append("FB enrich (n=%d): ok %d%% · partial %d%% · failed %d%% ·"
                     " skipped %d%% — km %d%% · desc %d%%" % (
                         fb["n"], pct(fb["ok"]), pct(fb["part"]), pct(fb["fail"]),
                         pct(fb["skip"]), pct(fb["has_km"]), pct(fb["has_desc"])))
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
    ok = telegram_send("\n\n".join(lines))
    log("weekly report %s (%d alerts, %d labeled)"
        % ("sent" if ok else "FAILED to send", len(alerts), judged))
    return ok


def maybe_weekly_report(conn):
    now_local = datetime.now(TZ)
    today = now_local.strftime("%Y-%m-%d")
    if (now_local.weekday() == WEEKLY_REPORT_DOW
            and now_local.hour >= WEEKLY_REPORT_HOUR_LOCAL
            and meta_get(conn, "last_weekly_date") != today):
        # Only mark the week done once it actually arrived — stamping on a
        # failed send skipped the report until the NEXT Sunday, by which
        # time its 7-day window has rolled past most of the lost content.
        if send_weekly_report(conn):
            meta_set(conn, "last_weekly_date", today)


def check_scan_volume(conn):
    """Partial breakage: volume quietly collapses while scans still 'work'.

    Compares the last 24h against the daily average of the 7 days before it,
    per source — one dead search or a dead FB tab halves a source's volume
    without ever returning a zero scan.
    """
    now = datetime.now(timezone.utc)
    day_ago_dt = now - timedelta(hours=24)
    day_ago = day_ago_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    week_ago = (now - timedelta(days=8)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = conn.execute(
        """SELECT source,
                  SUM(CASE WHEN scanned_at >= ? THEN parsed_count ELSE 0 END) AS today,
                  SUM(CASE WHEN scanned_at <  ? THEN parsed_count ELSE 0 END) AS prior,
                  MIN(CASE WHEN scanned_at < ? THEN scanned_at END) AS prior_earliest
           FROM scans WHERE scanned_at >= ? GROUP BY source""",
        (day_ago, day_ago, day_ago, week_ago)).fetchall()
    for r in rows:
        # History length as actual ELAPSED TIME of the prior window's data,
        # not a count of distinct UTC calendar dates it touched — that
        # count could run to 8 for a 7-day-wide window whenever the window
        # falls mid-day (routine, since it's anchored to "now"), understating
        # the average and needing a smaller drop than documented to trip.
        if not r["prior_earliest"]:
            continue
        try:
            earliest_dt = (datetime.strptime(r["prior_earliest"], "%Y-%m-%dT%H:%M:%SZ")
                           .replace(tzinfo=timezone.utc))
        except (ValueError, TypeError):
            continue
        span_days = (day_ago_dt - earliest_dt).total_seconds() / 86400.0
        if span_days < VOLUME_MIN_HISTORY_DAYS:
            continue  # not enough history to call anything abnormal
        avg = (r["prior"] or 0) / min(span_days, 7.0)
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
                 maybe_daily_digest, maybe_weekly_report, recheck_listings,
                 retry_failed_alerts):
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
            new_rows, reassess_rows = store_listings(conn, [row])
            stored += 1
            to_process = new_rows + reassess_rows
            if to_process:
                process_new_rows(conn, to_process,
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
        new_rows, reassess_rows = store_listings(conn, all_rows)
        ids = {(r["source"], r["id"]) for r in all_rows}
        print("  %d listings fetched (%d unique — searches overlap), %d not "
              "already in the database" % (len(all_rows), len(ids), len(new_rows)))

        print("\n--- 2. REJECTION BREAKDOWN ---------------------------------------")
        process_new_rows(conn, new_rows, allow_alerts=True)
        process_new_rows(conn, reassess_rows, allow_alerts=True)
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
        with_off = conn.execute(
            "SELECT * FROM models WHERE kind='family'"
            " AND COALESCE(offsets_json,'{}') NOT IN ('','{}')"
            " ORDER BY comp_count DESC").fetchall()
        print("  offsets: %d of %d family models carry trim/drivetrain offsets"
              % (len(with_off), fams))
        if with_off:
            m = with_off[0]
            offs = scoring.unpack_offsets(m["offsets_json"])
            bits = []
            for tier, v in sorted((offs.get("trim") or {}).items()):
                bits.append("trim%d %+.0f%% (n=%d)"
                            % (tier, (math.exp(v["off"]) - 1) * 100, v["n"]))
            for dt, v in sorted((offs.get("drivetrain") or {}).items()):
                bits.append("%s %+.0f%% (n=%d)"
                            % (dt, (math.exp(v["off"]) - 1) * 100, v["n"]))
            print("    %s: %s, mad %.3f->%.3f" % (
                m["model_key"], " · ".join(bits), m["mad"],
                m["mad_adj"] if m["mad_adj"] else m["mad"]))
        cov = conn.execute(
            """SELECT COUNT(*) AS n,
                      SUM(trim_tier IS NOT NULL) AS t,
                      SUM(drivetrain IS NOT NULL) AS d
               FROM listings WHERE reject_reason IS NULL
                 AND source='autotrader'""").fetchone()
        if cov["n"]:
            print("  extraction coverage (clean AT rows): trim %d%%, drivetrain %d%%"
                  % (100 * (cov["t"] or 0) // cov["n"],
                     100 * (cov["d"] or 0) // cov["n"]))

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

        fb_week = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
        fb = conn.execute(
            """SELECT COUNT(*) AS n,
                      SUM(enrich_status='ok') AS ok,
                      SUM(enrich_status='partial') AS part,
                      SUM(enrich_status='failed') AS fail,
                      SUM(enrich_status='skipped') AS skip,
                      SUM(km IS NOT NULL) AS has_km,
                      SUM(description IS NOT NULL) AS has_desc
               FROM listings WHERE source='facebook' AND first_seen_at >= ?""",
            (fb_week,)).fetchone()
        if fb["n"]:
            print("\n--- 5b. FB ENRICHMENT (7d) ---------------------------------------")
            print("  %d rows: ok %s · partial %s · failed %s · skipped %s"
                  " · km %s · desc %s" % (
                      fb["n"], fb["ok"] or 0, fb["part"] or 0, fb["fail"] or 0,
                      fb["skip"] or 0, fb["has_km"] or 0, fb["has_desc"] or 0))
            # The blind-tuning readout: which extractor keys are matching on
            # real pages, and what the failures say. no_keys dominating =
            # FB renamed things; fix the extractor table in the userscript.
            from collections import Counter
            key_counts, err_counts = Counter(), Counter()
            for (rj,) in conn.execute(
                """SELECT raw_json FROM listings WHERE source='facebook'
                   AND first_seen_at >= ? ORDER BY first_seen_at DESC LIMIT 200""",
                    (fb_week,)):
                try:
                    enrich = (json.loads(rj) or {}).get("enrich") or {}
                except (ValueError, TypeError):
                    continue
                key_counts.update(enrich.get("keys") or [])
                if enrich.get("err"):
                    err_counts[enrich["err"]] += 1
            if key_counts:
                print("  keys matched: " + " · ".join(
                    "%s %d" % kv for kv in key_counts.most_common(8)))
            if err_counts:
                print("  errors: " + " · ".join(
                    "%s %d" % kv for kv in err_counts.most_common(6)))

        print("\n--- 5c. AI VERIFICATION ------------------------------------------")
        try:
            import anthropic  # noqa: F401
            sdk = "installed"
        except ImportError:
            sdk = "NOT installed (pip install anthropic)"
        print("  enabled: %s · SDK: %s · key: %s"
              % ("yes" if AI_VERIFY_ENABLED else "no (AI_VERIFY_ENABLED=0)",
                 sdk, "set" if ANTHROPIC_API_KEY else "NOT set"))
        print("  --test never calls the API (no cost, and the 'exactly one"
              " message' guarantee holds); in live runs the check gates only"
              " non-shadow alerts, fail-open.")
        ai_counts = conn.execute(
            """SELECT ai_verdict, COUNT(*) FROM alerts
               WHERE ai_verdict IS NOT NULL GROUP BY ai_verdict""").fetchall()
        if ai_counts:
            print("  verdicts to date: " + " · ".join(
                "%s %d" % (r[0], r[1]) for r in ai_counts))

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
