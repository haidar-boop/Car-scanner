"""Deal scoring for the Edmonton car scanner — the four layers.

Layer order is load-bearing: reject -> fit robustly -> z-score robustly ->
gate confidence. Cars that are cheap *for a reason* (salvage, parts, scams)
must be stripped before anything is measured, or they dominate the top of
the ranking and poison the fitted curves.

CAVEAT, stated plainly: everything here models ASKING prices, not
transaction prices. The fitted curve predicts what sellers *list* a car at,
not what the car is worth — a listing 20% below the curve may just have a
realistic seller. The lifespan signal (disappeared_at, collected by the
watcher) is the partial correction.

Pure functions + numpy; the only I/O is through a sqlite3 connection the
caller passes in. `python3 scoring.py` runs a selftest.
"""

import json
import math
import re
import unicodedata
import zlib
from datetime import datetime, timedelta, timezone

import numpy as np

# --- REJECTION LAYER --------------------------------------------------------

# Terms that make a car legitimately cheap. Editable — add terms as the
# weekly report surfaces them. Matching is case-insensitive, word-bounded
# ("projector" does not hit "project"), tolerant of missing apostrophes
# ("doesnt start") and hyphen/space variants ("write off"/"write-off").
BLOCKLIST_TERMS = [
    "salvage", "rebuilt", "rebuild", "branded", "brand title", "written off",
    "write-off", "insurance total", "total loss", "flood", "hail",
    "parts only", "for parts", "parts car", "part out", "mechanic special",
    "project", "project car", "no start", "doesn't start", "does not start",
    "won't start", "not running", "non-runner", "needs engine", "needs motor",
    "needs trans", "needs transmission", "blown", "seized", "knocking",
    "no rad", "overheats", "head gasket", "as-is", "as is", "lien",
    "no registration", "out of province",
]

PRICE_MIN = 500
PRICE_MAX = 200_000          # also catches phone-number "prices" (7+ digits)

# Explicit, auditable set — deliberately NOT "any repeated digit", because
# $9,999 and $8,888 are common real asking prices in this market.
PLACEHOLDER_PRICE_STRINGS = (
    {"1" * k for k in range(1, 7)}
    | {"123", "1234", "12345", "123456", "321", "4321", "54321", "654321"}
)

KM_MAX = 600_000
KM_MIN_IF_OLD = 1_000        # km below this on an old car is a misparse
OLD_YEARS = 3
MILES_TO_KM = 1.60934

FINGERPRINT_KM_BUCKET = 5_000
FINGERPRINT_PRICE_BUCKET = 250
FINGERPRINT_TTL_DAYS = 90

# --- MODEL FIT --------------------------------------------------------------

N_TRIPLES = 2000             # random point-triples per Theil-Sen fit
MIN_VALID_TRIPLES = 100      # fewer non-degenerate triples -> Huber fallback
HUBER_C = 1.345
HUBER_ITERS = 10
FAMILY_MODEL_MIN_COMPS = 30  # below this a family scores via its segment model
COMP_MAX_AGE_DAYS = 365
REFIT_NEW_COMP_THRESHOLD = 200

# --- SCORING / GATES --------------------------------------------------------

# --- TRIM / DRIVETRAIN OFFSETS ----------------------------------------------

TRIM_LEXICON_VERSION = 1     # bump after editing the lexicon below: migrate_db
                             # re-extracts every stored row on the next start
MIN_OFFSET_COMPS = 5         # comps sharing a level before an offset exists
OFFSET_SHRINK_K = 5.0        # offset = median_residual * n / (n + K)
OFFSET_CLAMP = 0.30          # log-space cap (~±35%); a contaminated thin
                             # group must not swing predictions absurdly
TRIM_TIER_NAMES = {0: "base trim", 1: "mid trim", 2: "premium trim"}

MAD_SCALE = 1.4826           # makes MAD comparable to a normal sigma
Z_ALERT = {"autotrader": -2.0, "facebook": -2.0}
Z_ALERT_DEALER = -2.5        # dealer pricing is market-calibrated; demand more
MIN_COMPS_GATE = 8
MAD_MIN = 0.02               # log-space; below = degenerate comp pool
MAD_MAX = 0.50               # above = family too heterogeneous to price
MAD_MAX_SEGMENT = 0.80       # segment models are heterogeneous by nature
MODEL_MAX_AGE_DAYS = 30
SHADOW_DAYS = 14

# --- CROSS-SOURCE NORMALIZATION ---------------------------------------------

STATIC_MAKES = [
    "ACURA", "ALFA ROMEO", "AUDI", "BMW", "BUICK", "CADILLAC", "CHEVROLET",
    "CHRYSLER", "DODGE", "FIAT", "FORD", "GENESIS", "GMC", "HONDA", "HYUNDAI",
    "INFINITI", "JAGUAR", "JEEP", "KIA", "LAND ROVER", "LEXUS", "LINCOLN",
    "MAZDA", "MERCEDES-BENZ", "MINI", "MITSUBISHI", "NISSAN", "PONTIAC",
    "PORSCHE", "RAM", "SATURN", "SCION", "SUBARU", "SUZUKI", "TESLA",
    "TOYOTA", "VOLKSWAGEN", "VOLVO",
]

MAKE_ALIASES = {
    "CHEVY": "CHEVROLET",
    "VW": "VOLKSWAGEN",
    "MERCEDES": "MERCEDES-BENZ",
    "MERCEDESBENZ": "MERCEDES-BENZ",
    "BENZ": "MERCEDES-BENZ",
    "LANDROVER": "LAND ROVER",
    "ALFAROMEO": "ALFA ROMEO",
}

# AutoTrader modelGroup quirks that would split a family across sources.
FAMILY_MERGES = {
    "NISSAN:QASHQAI": "NISSAN:ROGUE",
}

# --- trim / drivetrain lexicon ----------------------------------------------
# Tiers are BUCKETS, not enforced ordinals: offsets are learned per
# (family, tier), so what matters is that a token lands in the same bucket
# consistently within a family — an arguably-off-by-one label still gets the
# right learned offset. When you edit anything below, bump
# TRIM_LEXICON_VERSION so the migration re-classifies stored rows.

# Drivetrain is a binary "all driven wheels" vs "two driven wheels" flag;
# RWD folds into 'fwd' deliberately (within BMW:3SERIES the two-wheel group
# IS the RWD cars, and offsets are per-family, so only consistency matters).
AWD_TOKENS = ["4X4", "4WD", "AWD", "4MOTION", "QUATTRO", "4MATIC", "AWC",
              "SH AWD", "ALL WHEEL DRIVE"]
FWD_TOKENS = ["FWD", "2WD", "RWD", "4X2", "FRONT WHEEL DRIVE",
              "REAR WHEEL DRIVE"]
# Prefix-match tokens: BMW writes "xDrive28i" as one word, so a word-bounded
# XDRIVE never matches.
AWD_PREFIXES = ["XDRIVE"]
FWD_PREFIXES = ["SDRIVE"]

TRIM_TIERS_GLOBAL = {
    2: ["PLATINUM", "KING RANCH", "LARIAT", "LARAMIE", "LONGHORN", "DENALI",
        "HIGH COUNTRY", "LTZ", "SLT", "AT4", "REBEL", "RAPTOR", "TRX",
        "LIMITED", "ULTIMATE", "LUXURY", "CALLIGRAPHY", "AVENIR", "SUMMIT",
        "OVERLAND", "TRAILHAWK", "RUBICON", "SAHARA", "TOURING", "EX L",
        "XLE", "XSE", "TRD PRO", "PRO 4X", "TITANIUM", "F SPORT", "TYPE R",
        "TYPE S", "PREMIER", "HIGHLINE", "EXECLINE", "SIGNATURE",
        "GRAND TOURING", "OUTER BANKS", "BADLANDS", "KING CAB PRO"],
    1: ["XLT", "LT", "SV", "SE", "SEL", "SLE", "EX", "LE", "SPORT",
        "BIG HORN", "BIGHORN", "TRAIL BOSS", "PREFERRED", "COMFORTLINE",
        "ELEVATION", "TRD", "GT LINE", "ALTITUDE", "LATITUDE", "SR5",
        "VALUE PACKAGE"],
    0: ["XL", "LS", "LX", "DX", "CE", "BASE", "WORK TRUCK", "TRADESMAN",
        "ESSENTIAL", "TRENDLINE", "CONVENIENCE", "SXT", "STX", "LAREDO"],
}
# Same-token conflicts across makes get scoped entries; scoped beats global.
TRIM_TIERS_BY_MAKE = {
    "JEEP":       {"SPORT": 0},               # Wrangler/GC Sport is the base trim
    "RAM":        {"ST": 0},
    "DODGE":      {"ST": 0},
    "FORD":       {"ST": 2, "GT": 2},         # Explorer ST / Mustang GT
    "MAZDA":      {"GX": 0, "GS": 1, "GT": 2},  # Canadian Mazda ladder
    "TOYOTA":     {"SR": 0},                  # Tacoma/Tundra SR is base
    "NISSAN":     {"S": 0, "SL": 2},          # bare "S" only safe when scoped
    "SUBARU":     {"TOURING": 1},             # 2nd-from-base in Canada
    "CHEVROLET":  {"WT": 0},
    "GMC":        {"WT": 0},
    "KIA":        {"SX": 2},
}
# Phrases blanked out before trim scanning — feature prose that contains trim
# tokens ("limited slip diff" is not a Limited; "premier propriétaire" is
# French for first owner, not a Buick Premier).
TRIM_IGNORE = ["LIMITED SLIP", "LIMITED WARRANTY", "LIMITED TIME",
               "SPORT MODE", "SPORT PACKAGE", "SPORT PKG",
               "SPORT APPEARANCE", "SPORT WHEELS", "LS SWAP",
               "EX FLEET", "EX LEASE", "EX RENTAL", "EX DEMO",
               "PREMIER PROPRIETAIRE", "PREMIERE PROPRIETAIRE",
               "LE PLUS", "SPORT UTILITY"]
# Deliberately excluded: bare "S" (only Nissan-scoped — hits everywhere),
# bare "L", "PREMIUM" ("premium audio" is universal), "CUSTOM" ("custom
# exhaust" in FB text). Add them scoped if a family needs them.

# search_label -> segment model key (FB labels fold into AutoTrader segments).
SEGMENT_MAP = {
    "cars_2k_15k": "cars_2k_15k",
    "cars_15k_35k": "cars_15k_35k",
    "trucks_suvs_5k_30k": "trucks_suvs_5k_30k",
    "trucks_5k_30k": "trucks_suvs_5k_30k",
    "suvs_5k_30k": "trucks_suvs_5k_30k",
}

# ----------------------------------------------------------------------------


def _compile_blocklist(terms):
    parts = []
    for term in terms:
        esc = re.escape(term.lower())
        esc = esc.replace(r"\-", r"[\s\-]?").replace(r"\ ", r"[\s\-]+")
        esc = esc.replace("'", "['’]?")
        parts.append(r"\b%s\b" % esc)
    return re.compile("|".join(parts), re.IGNORECASE)


BLOCKLIST_RE = _compile_blocklist(BLOCKLIST_TERMS)

ISO = "%Y-%m-%dT%H:%M:%SZ"


def _iso_year(iso_str):
    try:
        return int(iso_str[:4])
    except (TypeError, ValueError):
        return None


# --- normalization ----------------------------------------------------------

def normalize_make(s):
    if not s or not isinstance(s, str):
        return None
    key = re.sub(r"[^A-Z0-9 \-]", "", s.upper().strip())
    key = re.sub(r"\s+", " ", key)
    key = MAKE_ALIASES.get(key.replace(" ", "").replace("-", ""), key)
    return key or None


def normalize_family(make, model):
    """Cross-source family key 'MAKE:MODELKEY'; F 150/F-150/f150 collapse."""
    mk = normalize_make(make)
    if not mk or not model or not isinstance(model, str):
        return None
    modelkey = re.sub(r"[^A-Z0-9]", "", model.upper())
    if not modelkey:
        return None
    fam = "%s:%s" % (mk, modelkey)
    return FAMILY_MERGES.get(fam, fam)


def family_from_raw(raw_json, make, model):
    """Prefer the site's own modelGroup normalization when raw_json has it."""
    group = None
    if raw_json:
        try:
            group = (json.loads(raw_json).get("vehicle") or {}).get("modelGroup")
        except (ValueError, AttributeError):
            pass
    return normalize_family(make, group or model)


# --- trim / drivetrain extraction -------------------------------------------

def _translit(s):
    """Fold accents to base letters ("sécurité"→"securite") — stripping them
    to spaces would leave stray single-letter tokens that misfire as trims."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


def _norm_trim_text(s):
    """Uppercase, transliterate, collapse every non-alphanumeric run to one
    space, pad ends — "King-Ranch"→" KING RANCH "."""
    if not s or not isinstance(s, str):
        return ""
    return " %s " % re.sub(r"[^A-Z0-9]+", " ", _translit(s).upper()).strip()


def parse_drivetrain(*texts):
    """'awd' | 'fwd' | None from any of the given texts (Nones skipped).

    Word-bounded token scan; XDRIVE/SDRIVE match as prefixes because BMW
    writes "xDrive28i" as one word. Conflicting evidence -> None: never
    guess, an unadjusted prediction is safer than a wrong one.
    """
    norm = _norm_trim_text(" ".join(str(t) for t in texts if t))
    if not norm.strip():
        return None
    awd = any(" %s " % t in norm for t in AWD_TOKENS) or \
        any(re.search(r"\b%s" % p, norm) for p in AWD_PREFIXES)
    fwd = any(" %s " % t in norm for t in FWD_TOKENS) or \
        any(re.search(r"\b%s" % p, norm) for p in FWD_PREFIXES)
    if awd and fwd:
        return None
    return "awd" if awd else "fwd" if fwd else None


def parse_trim_tier(make, *texts, exclude_tokens=()):
    """0 | 1 | 2 | None. make = canonical make ('FORD') or None.

    Precision rules, each earned by a real failure mode:
    - Accents transliterate ("sécurité"→SECURITE), so French text never
      fragments into stray one-letter tokens.
    - TRIM_IGNORE word-sequences are blanked first ("limited slip",
      "ex-fleet", "premier propriétaire").
    - Tokens of <=2 letters (LE/CE/SE/XL/LT/EX/...) only match where the
      ORIGINAL text has them fully uppercase — sellers write trims as "XL"
      but French articles as "le"/"Le", which is what keeps bilingual
      Alberta listings from being tiered by their prose.
    - All candidates are collected, long (>=3 char or multiword) matches
      beat short ones, and if only short matches remain and they disagree
      on tier, the answer is None — ambiguity never resolves by list order.
    - exclude_tokens suppresses the listing's own model name (a Lexus LS
      is not an LS trim).
    """
    raw = " ".join(str(t) for t in texts if t)
    if not raw.strip():
        return None
    cased = re.sub(r"[^A-Za-z0-9]+", " ", _translit(raw)).split()
    upper = [t.upper() for t in cased]
    blanked = [False] * len(upper)
    for phrase in TRIM_IGNORE:
        words = phrase.split()
        for i in range(len(upper) - len(words) + 1):
            if upper[i:i + len(words)] == words:
                for j in range(i, i + len(words)):
                    blanked[j] = True

    scoped = TRIM_TIERS_BY_MAKE.get(make or "", {})
    candidates = [(tok, tier) for tok, tier in scoped.items()]
    shadowed = set(scoped)
    for tier, toks in TRIM_TIERS_GLOBAL.items():
        candidates.extend((tok, tier) for tok in toks if tok not in shadowed)
    excluded = {str(t).upper() for t in exclude_tokens}

    long_hits, short_hits = [], []
    for tok, tier in candidates:
        if tok in excluded:
            continue
        words = tok.split()
        for i in range(len(upper) - len(words) + 1):
            if upper[i:i + len(words)] != words or any(blanked[i:i + len(words)]):
                continue
            if len(tok) <= 2 and not cased[i].isupper():
                continue  # "le camion" is not an LE
            (long_hits if (len(words) > 1 or len(tok) >= 3)
             else short_hits).append((tok, tier))
            break
    if long_hits:
        long_hits.sort(key=lambda t: len(t[0]), reverse=True)
        return long_hits[0][1]
    tiers = {tier for _, tier in short_hits}
    if len(tiers) == 1:
        return tiers.pop()
    return None  # nothing found, or short tokens disagree — never guess


def extract_features(make_raw, model_version, title, description,
                     exclude_tokens=()):
    """(trim_tier, drivetrain) — the single entry point shared by the
    AutoTrader parser, the FB parser, and the migration backfill.

    Trim scans modelVersionInput, else the title (FB has nothing else) —
    NEVER the description: dealer prose is full of "limited warranty" and
    "sport mode". Callers whose title is a synthesized "year make model"
    (AutoTrader) must pass title=None: such a title can only ever match
    model-name tokens, which is pure noise. Drivetrain scans everything;
    "4x4"/"quattro" are rarely metaphorical.
    """
    make = normalize_make(make_raw)
    mv = str(model_version) if model_version is not None else None
    tier = parse_trim_tier(make, mv, exclude_tokens=exclude_tokens) if mv \
        else parse_trim_tier(make, title, exclude_tokens=exclude_tokens)
    drivetrain = parse_drivetrain(mv, title, description)
    return tier, drivetrain


# --- rejection layer --------------------------------------------------------

def price_sanity(price):
    if price is None or not isinstance(price, int) or price <= 0:
        return "price_unparseable"
    if str(price) in PLACEHOLDER_PRICE_STRINGS:
        return "price_placeholder:%d" % price
    if price < PRICE_MIN:
        return "price_too_low:%d" % price
    if price > PRICE_MAX:
        return "price_insane:%d" % price
    return None


def blocklist_hit(title, description):
    text = " ".join(t for t in (title, description) if t)
    m = BLOCKLIST_RE.search(text)
    return "blocklist:%s" % m.group(0).lower() if m else None


def odometer_sanity(km, year, now_year):
    if km > KM_MAX:
        return "km_too_high:%d" % km
    if km < KM_MIN_IF_OLD and year is not None and now_year - year > OLD_YEARS:
        return "km_implausibly_low:%d" % km
    return None


def fingerprint(family, year, km, price):
    if None in (family, year, km, price):
        return None
    return "%s|%d|%d|%d" % (
        family, year,
        round(km / FINGERPRINT_KM_BUCKET),
        round(price / FINGERPRINT_PRICE_BUCKET),
    )


def assess(row, now_year):
    """Full rejection layer, in order. Returns (family, fingerprint, reason).

    family/fingerprint are computed even for rejected rows when possible so
    reposts of rejected listings are still recognizable; reason is the FIRST
    failure. reason None = clean: eligible for scoring and as a comp.
    """
    price = row.get("price")
    year = row.get("year")
    km = row.get("km")
    family = family_from_raw(row.get("raw_json"), row.get("make"), row.get("model"))
    fp = fingerprint(family, year, km, price)

    reason = price_sanity(price)
    if reason:
        return family, fp, reason
    if year is None or km is None or not row.get("make") or not row.get("model"):
        return family, fp, "incomplete"
    if family is None:
        return family, fp, "no_family"
    reason = blocklist_hit(row.get("title"), row.get("description"))
    if reason:
        return family, fp, reason
    if row.get("is_damaged"):
        return family, fp, "damaged_flag"
    reason = odometer_sanity(km, year, now_year)
    if reason:
        return family, fp, reason
    return family, fp, None


# --- FB title parsing -------------------------------------------------------

YEAR_RE = re.compile(r"\b(19[89]\d|20[0-2]\d)\b")
MILES_RE = re.compile(r"\b([\d][\d,\.]*)\s*(?:miles|mi)\b", re.I)
# Left \b is load-bearing: without it, a digit token before the mileage
# ("4x4 185,000 km", "V8 120,000 km") merges into the capture and the row
# is falsely rejected km_too_high.
KM_FULL_RE = re.compile(r"\b(\d{1,3}(?:[,\s]\d{3})+|\d{4,6})\s*(?:km|kms)\b", re.I)
K_KM_RE = re.compile(r"\b(\d{2,3})\s*k\s*(?:km|kms)\b", re.I)
K_BARE_RE = re.compile(r"\b(\d{2,3})k\b", re.I)
ODO_CONTEXT_RE = re.compile(r"\b(km|kms|odometer|mileage|kilometers|kilometres)\b", re.I)
PRICE_CONTEXT_RE = re.compile(r"\b(obo|firm|asking|neg(?:otiable)?|cash|trade)\b|\$", re.I)


def parse_year_from_text(text):
    m = YEAR_RE.search(text or "")
    return int(m.group(1)) if m else None


def parse_km_text(text):
    """Returns (km, original_miles_or_None). None km = ambiguous/absent."""
    if not text:
        return None, None
    m = MILES_RE.search(text)
    if m:
        try:
            miles = int(float(m.group(1).replace(",", "")))
            return int(miles * MILES_TO_KM), miles
        except ValueError:
            pass
    m = KM_FULL_RE.search(text)
    if m:
        return int(re.sub(r"[^\d]", "", m.group(1))), None
    m = K_KM_RE.search(text)
    if m:
        return int(m.group(1)) * 1000, None
    # Bare "185k" is ambiguous — it's an asking price at least as often as an
    # odometer ("15k obo"). Price-ish words adjacent to the number always win
    # (a global "low kms!" elsewhere must not turn the price into mileage);
    # only with no price context nearby AND odometer context in the text do
    # we read it as km. Otherwise km stays unknown -> store-only.
    m = K_BARE_RE.search(text)
    if m:
        nearby = text[max(0, m.start() - 10):m.start()] + " " + text[m.end():m.end() + 12]
        if PRICE_CONTEXT_RE.search(nearby):
            return None, None
        if ODO_CONTEXT_RE.search(text):
            return int(m.group(1)) * 1000, None
    return None, None


def _find_make_span(text, known_makes):
    """Returns (canonical_make, text_after_the_matched_token). The matched
    token may be an alias ('chevy'), so the caller must use the returned
    after-text — searching for the canonical name would find nothing."""
    if not text:
        return None, ""
    norm = " %s " % re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", text.upper()))
    candidates = [(a, c) for a, c in MAKE_ALIASES.items()]
    candidates += [(m, m) for m in known_makes]
    for token, canon in sorted(candidates, key=lambda t: len(t[0]), reverse=True):
        needle = " %s " % token.replace("-", " ")
        i = norm.find(needle)
        if i >= 0:
            return canon, norm[i + len(needle):]
    return None, ""


def find_make(text, known_makes):
    return _find_make_span(text, known_makes)[0]


def find_model(after_text, models_for_make):
    """Longest-prefix match of the text following the make token."""
    if not after_text or not models_for_make:
        return None
    norm = re.sub(r"[^A-Z0-9]", "", after_text.upper())
    for modelkey in sorted(models_for_make, key=len, reverse=True):
        if norm.startswith(modelkey):
            return modelkey
    return None


def known_vehicles(conn):
    """{'makes': set, 'models_by_make': {make: set(modelkeys)}} from the DB."""
    makes = set(STATIC_MAKES)
    models = {}
    try:
        for (family,) in conn.execute(
            "SELECT DISTINCT family FROM listings WHERE family IS NOT NULL"
        ):
            make, _, modelkey = family.partition(":")
            if make and modelkey:
                makes.add(make)
                models.setdefault(make, set()).add(modelkey)
    except Exception:
        pass  # pre-migration DB; static makes still work
    return {"makes": makes, "models_by_make": models}


def parse_fb_listing(item, known, now_iso):
    """Ingest item {id,url,price_text,text,label,seen_at,seed} -> store row.

    Fields that can't be parsed stay None; assess() downstream turns that
    into reject_reason='incomplete' (store-only, never scored, never a comp)
    — expected for most FB anchors, and precision-preserving.
    """
    listing_id = str(item.get("id") or "").strip()
    if not listing_id or not listing_id.isdigit():
        return None
    text = str(item.get("text") or "")[:500]
    price = None
    digits = re.sub(r"[^\d]", "", str(item.get("price_text") or ""))
    if digits:
        try:
            price = int(digits[:9])
        except ValueError:
            price = None
    km, miles = parse_km_text(text)
    if km is not None and price is not None and abs(km - price) <= max(500, price * 0.02):
        km, miles = None, None  # almost certainly the price echoed as mileage
    make, after = _find_make_span(text, known["makes"])
    modelkey = find_model(after, known["models_by_make"].get(make)) if make else None
    # The listing's own model name must not be read as a trim ("Lexus LS").
    trim_tier, drivetrain = extract_features(
        make, None, text, None,
        exclude_tokens=(modelkey,) if modelkey else ())
    return {
        "id": listing_id,
        "source": "facebook",
        "url": "https://www.facebook.com/marketplace/item/%s" % listing_id,
        "title": text[:200],
        "price": price,
        "year": parse_year_from_text(text),
        "make": make,
        "model": modelkey,
        "km": km,
        "seller_type": None,   # FB anchors carry no seller info
        "city": None,
        "distance_km": None,
        "description": None,
        "is_damaged": 0,
        "result_type": None,
        "price_label": None,
        "search_label": str(item.get("label") or "facebook")[:40],
        "km_converted_from_miles": miles,
        "trim_tier": trim_tier,
        "drivetrain": drivetrain,
        "model_version": None,
        "raw_json": json.dumps({
            "text": text, "price_text": str(item.get("price_text") or "")[:40],
            "seen_at": str(item.get("seen_at") or now_iso)[:32],
        }),
    }


# --- robust fit -------------------------------------------------------------

def fit_robust(ages, log_kms, log_prices, seed):
    """Generalized Theil-Sen for log(price) = b0 + b1*age + b2*log(km+1).

    The comp pool is contaminated with exactly the outliers being hunted, so
    OLS is unusable. Coordinate-wise median over exact solutions of random
    point-triples has high breakdown, no dependencies beyond numpy, and is
    deterministic per family. Fallbacks: Huber-IRLS when too few triples are
    non-degenerate; intercept-only median when the design is rank-deficient
    (e.g. a family whose comps are all one model year).
    """
    ages = np.asarray(ages, dtype=float)
    log_kms = np.asarray(log_kms, dtype=float)
    y = np.asarray(log_prices, dtype=float)
    n = y.shape[0]
    if n < 3:
        return None
    X = np.column_stack([np.ones(n), ages, log_kms])

    if np.linalg.matrix_rank(X) < 3:
        beta = np.array([float(np.median(y)), 0.0, 0.0])
        method = "median"
    else:
        rng = np.random.default_rng(seed)
        idx = rng.integers(0, n, size=(N_TRIPLES, 3))
        distinct = (idx[:, 0] != idx[:, 1]) & (idx[:, 1] != idx[:, 2]) & (idx[:, 0] != idx[:, 2])
        idx = idx[distinct]
        A = X[idx]                       # (T, 3, 3)
        b = y[idx]                       # (T, 3)
        good = np.abs(np.linalg.det(A)) > 1e-8
        if good.sum() >= MIN_VALID_TRIPLES:
            coef = np.linalg.solve(A[good], b[good][..., None])[..., 0]
            coef = coef[np.isfinite(coef).all(axis=1)]
            beta = np.median(coef, axis=0)
            method = "theil-sen"
        else:
            beta = np.linalg.lstsq(X, y, rcond=None)[0]
            for _ in range(HUBER_ITERS):
                r = y - X @ beta
                s = max(MAD_SCALE * float(np.median(np.abs(r - np.median(r)))), 1e-6)
                w = np.minimum(1.0, HUBER_C * s / np.maximum(np.abs(r), 1e-9))
                sw = np.sqrt(w)[:, None]
                beta = np.linalg.lstsq(X * sw, y * np.sqrt(w), rcond=None)[0]
            method = "huber-irls"

    # Recenter so the median residual is exactly 0 — the z-score below
    # divides the residual by 1.4826*MAD and assumes an unbiased center.
    resid = y - X @ beta
    beta = beta.copy()
    beta[0] += float(np.median(resid))
    resid = y - X @ beta
    mad = float(np.median(np.abs(resid)))
    return {"b0": float(beta[0]), "b1": float(beta[1]), "b2": float(beta[2]),
            "mad": mad, "method": method, "n": n}


def predict_log_price(model_row, age, km):
    return model_row["b0"] + model_row["b1"] * age + model_row["b2"] * math.log(km + 1)


def compute_offsets(residuals, labels):
    """{label: {'off': float, 'n': int}} — shrunk, clamped median residual
    per label. Labels may contain None (those comps are skipped). Groups
    below MIN_OFFSET_COMPS get no offset at all: silence over guessing."""
    residuals = np.asarray(residuals, dtype=float)
    out = {}
    for label in {l for l in labels if l is not None}:
        mask = np.array([l == label for l in labels])
        n = int(mask.sum())
        if n < MIN_OFFSET_COMPS:
            continue
        med = float(np.median(residuals[mask]))
        off = med * n / (n + OFFSET_SHRINK_K)
        off = max(-OFFSET_CLAMP, min(OFFSET_CLAMP, off))
        out[label] = {"off": off, "n": n}
    return out


def unpack_offsets(offsets_json):
    """JSON text/None -> {'trim': {int_tier: {...}}, 'drivetrain': {...}}.
    Tolerant of NULL/''/'{}'; JSON stringifies int keys, so restore them."""
    if not offsets_json:
        return {}
    try:
        raw = json.loads(offsets_json)
    except (ValueError, TypeError):
        return {}
    out = {}
    for feature, levels in (raw or {}).items():
        if not isinstance(levels, dict):
            continue
        fixed = {}
        for k, v in levels.items():
            key = int(k) if feature == "trim" and str(k).lstrip("-").isdigit() else k
            fixed[key] = v
        if fixed:
            out[feature] = fixed
    return out


def applicable_offsets(model_row, trim_tier, drivetrain):
    """[('trim', 2, +0.087), ('drivetrain', 'awd', +0.055)] — empty when the
    listing's features are unknown or the model has no matching level.
    Unknown never becomes a penalty."""
    offsets = model_row.get("offsets") or {}
    out = []
    if trim_tier is not None and trim_tier in (offsets.get("trim") or {}):
        out.append(("trim", trim_tier, offsets["trim"][trim_tier]["off"]))
    if drivetrain and drivetrain in (offsets.get("drivetrain") or {}):
        out.append(("drivetrain", drivetrain,
                    offsets["drivetrain"][drivetrain]["off"]))
    return out


def robust_z(price, model_row, age, km, trim_tier=None, drivetrain=None):
    """Returns (z, pct_below_predicted). Negative z = below the curve.

    When the listing's trim/drivetrain is known and the model carries a
    matching offset, the prediction is adjusted and z divides by mad_adj
    (the spread after offsets explained their variance). Otherwise the raw
    mad is used — an unknown-trim listing scores exactly as it would on a
    model with no offsets at all, which is what keeps this change strictly
    no-worse for the rows we know least about.
    """
    adjustments = applicable_offsets(model_row, trim_tier, drivetrain)
    delta = sum(a[2] for a in adjustments)
    residual = math.log(price) - (predict_log_price(model_row, age, km) + delta)
    mad = model_row["mad"]
    # mad_adj is measured after ALL offset features were adjusted, so it is
    # the right denominator only when this listing got all of them — a
    # trim-unknown listing still carries full between-trim variance in its
    # residual and must divide by the raw mad. And the floor: offsets can
    # legitimately explain most of a family's spread (dealer fleets cluster
    # tightly within a trim), but a divisor below MAD_MIN would mint huge z
    # from a few percent of noise — the same degeneracy the raw-mad gate
    # exists to block.
    offset_features = {f for f, levels in (model_row.get("offsets") or {}).items()
                       if levels}
    applied_features = {a[0] for a in adjustments}
    if adjustments and model_row.get("mad_adj") and applied_features >= offset_features:
        mad = max(model_row["mad_adj"], MAD_MIN)
    z = residual / (MAD_SCALE * max(mad, 1e-9))
    pct_below = (1.0 - math.exp(residual)) * 100.0
    return z, pct_below


def age_of(year, ref_iso=None):
    ref_year = _iso_year(ref_iso) or datetime.now(timezone.utc).year
    return float(max(0, ref_year - year))


def gate(model_row, age, km, now_iso, kind):
    """Confidence gates. Non-empty return = suppress the alert entirely."""
    problems = []
    if model_row["comp_count"] < MIN_COMPS_GATE:
        problems.append("comps<%d" % MIN_COMPS_GATE)
    mad_cap = MAD_MAX_SEGMENT if kind == "segment" else MAD_MAX
    if model_row["mad"] < MAD_MIN:
        problems.append("mad_degenerate")
    elif model_row["mad"] > mad_cap:
        problems.append("mad_absurd")
    if model_row.get("km_min") is not None and not (
        model_row["km_min"] <= km <= model_row["km_max"]
    ):
        problems.append("extrapolation_km")
    if model_row.get("age_min") is not None and not (
        model_row["age_min"] <= age <= model_row["age_max"]
    ):
        problems.append("extrapolation_age")
    fitted_at = model_row.get("fitted_at")
    if fitted_at and fitted_at < (
        datetime.strptime(now_iso, ISO) - timedelta(days=MODEL_MAX_AGE_DAYS)
    ).strftime(ISO):
        problems.append("model_stale")
    return problems


# family IS NOT NULL is load-bearing beyond the family query: a row the
# rejection layer never ran on has reject_reason NULL *and* family NULL, and
# must not slip into segment fits as a "clean" comp.
COMP_SQL = """
SELECT year, km, price, first_seen_at, trim_tier, drivetrain FROM listings
WHERE {where} AND reject_reason IS NULL AND family IS NOT NULL
  AND COALESCE(is_damaged, 0) = 0
  AND year IS NOT NULL AND km IS NOT NULL AND price IS NOT NULL
  AND km > 0 AND price > 0 AND first_seen_at >= ?
"""


def _fit_and_pack(comps, seed, with_offsets=True):
    ages = np.array([age_of(c[0], c[3]) for c in comps])
    kms = np.array([float(c[1]) for c in comps])
    prices = np.array([float(c[2]) for c in comps])
    fit = fit_robust(ages, np.log(kms + 1), np.log(prices), seed)
    if fit is None:
        return None
    fit.update({
        "km_min": int(kms.min()), "km_max": int(kms.max()),
        "age_min": float(ages.min()), "age_max": float(ages.max()),
        "offsets": {}, "mad_adj": None,
    })
    if not with_offsets:
        return fit
    # Post-fit residual offsets, sequentially residualized: trim first (the
    # larger effect), then drivetrain on the trim-adjusted residuals — so
    # applying both at score time never double-counts their shared variance
    # (Platinums are mostly 4x4).
    tiers = [c[4] for c in comps]
    drivetrains = [c[5] for c in comps]
    resid = np.log(prices) - (fit["b0"] + fit["b1"] * ages
                              + fit["b2"] * np.log(kms + 1))
    trim_offs = compute_offsets(resid, tiers)
    applied = np.array([trim_offs[t]["off"] if t in trim_offs else 0.0
                        for t in tiers])
    resid2 = resid - applied
    dt_offs = compute_offsets(resid2, drivetrains)
    applied2 = np.array([dt_offs[d]["off"] if d in dt_offs else 0.0
                         for d in drivetrains])
    resid3 = resid2 - applied2
    offsets = {}
    if trim_offs:
        offsets["trim"] = trim_offs
    if dt_offs:
        offsets["drivetrain"] = dt_offs
    if offsets:
        fit["offsets"] = offsets
        fit["mad_adj"] = float(np.median(np.abs(resid3)))
    return fit


def refit_models(conn, now_iso):
    """Refit every eligible family + every segment. Returns models written."""
    cutoff = (
        datetime.strptime(now_iso, ISO) - timedelta(days=COMP_MAX_AGE_DAYS)
    ).strftime(ISO)
    written = 0

    families = [r[0] for r in conn.execute(
        "SELECT family FROM listings WHERE family IS NOT NULL"
        " AND reject_reason IS NULL AND COALESCE(is_damaged,0)=0"
        " AND year IS NOT NULL AND km IS NOT NULL AND price IS NOT NULL"
        " AND first_seen_at >= ? GROUP BY family HAVING COUNT(*) >= ?",
        (cutoff, FAMILY_MODEL_MIN_COMPS))]
    for family in families:
        comps = conn.execute(
            COMP_SQL.format(where="family = ?"), (family, cutoff)).fetchall()
        fit = _fit_and_pack(comps, zlib.crc32(family.encode()) & 0xFFFFFFFF)
        if fit:
            _upsert_model(conn, "family:%s" % family, "family", fit, now_iso)
            written += 1

    for segment in sorted(set(SEGMENT_MAP.values())):
        labels = [k for k, v in SEGMENT_MAP.items() if v == segment]
        ph = ",".join("?" * len(labels))
        comps = conn.execute(
            COMP_SQL.format(where="search_label IN (%s)" % ph),
            (*labels, cutoff)).fetchall()
        if len(comps) < MIN_COMPS_GATE:
            continue
        # No offsets on segment models: a tier's median residual there
        # confounds trim with family mix (base trims of expensive families
        # vs premium trims of cheap ones).
        fit = _fit_and_pack(comps, zlib.crc32(segment.encode()) & 0xFFFFFFFF,
                            with_offsets=False)
        if fit:
            _upsert_model(conn, "segment:%s" % segment, "segment", fit, now_iso)
            written += 1
    return written


def _upsert_model(conn, model_key, kind, fit, now_iso):
    with conn:
        conn.execute(
            """INSERT INTO models (model_key, kind, b0, b1, b2, mad, comp_count,
                                   km_min, km_max, age_min, age_max, method,
                                   fitted_at, offsets_json, mad_adj)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(model_key) DO UPDATE SET
                 kind=excluded.kind, b0=excluded.b0, b1=excluded.b1,
                 b2=excluded.b2, mad=excluded.mad, comp_count=excluded.comp_count,
                 km_min=excluded.km_min, km_max=excluded.km_max,
                 age_min=excluded.age_min, age_max=excluded.age_max,
                 method=excluded.method, fitted_at=excluded.fitted_at,
                 offsets_json=excluded.offsets_json, mad_adj=excluded.mad_adj""",
            (model_key, kind, fit["b0"], fit["b1"], fit["b2"], fit["mad"],
             fit["n"], fit["km_min"], fit["km_max"], fit["age_min"],
             fit["age_max"], fit["method"], now_iso,
             json.dumps(fit.get("offsets") or {}), fit.get("mad_adj")),
        )


# --- feedback loop helpers --------------------------------------------------

_MINE_STOPWORDS = {
    "the", "and", "for", "with", "this", "that", "has", "have", "was", "are",
    "you", "your", "not", "new", "car", "truck", "suv", "sale", "price",
    "call", "text", "please", "very", "great", "good", "clean", "will",
    "can", "come", "see", "just", "all", "one", "out", "any", "get", "its",
    "in", "on", "to", "of", "is", "it", "at", "or", "be", "by", "my", "we",
    "us", "an", "as", "if", "so", "up", "km",
}


def mine_blocklist_candidates(bad_texts, good_texts, extra_exclude=()):
    """Terms frequent in bad/scam-labeled listings but rare in clean ones."""
    exclude = {t.lower() for t in BLOCKLIST_TERMS}
    exclude |= {m.lower() for m in STATIC_MAKES}
    exclude |= {str(t).lower() for t in extra_exclude}

    def grams(text):
        toks = re.findall(r"\b[a-z']{2,}\b", (text or "").lower())
        toks = [t for t in toks if t not in _MINE_STOPWORDS]
        # unigrams need 3+ chars; 2-char words only matter inside bigrams
        return {t for t in toks if len(t) >= 3} | {
            " ".join(p) for p in zip(toks, toks[1:])
        }

    bad_df, good_df = {}, {}
    for t in bad_texts:
        for g in grams(t):
            bad_df[g] = bad_df.get(g, 0) + 1
    for t in good_texts:
        for g in grams(t):
            good_df[g] = good_df.get(g, 0) + 1

    out = []
    for g, bdf in bad_df.items():
        if bdf < 2 or g in exclude or any(g in e or e in g for e in exclude):
            continue
        ratio = bdf / (good_df.get(g, 0) + 1)
        if ratio >= 3:
            out.append((g, bdf, good_df.get(g, 0)))
    out.sort(key=lambda x: (-x[1], x[0]))
    return out[:5]


# --- selftest ---------------------------------------------------------------

def _selftest():
    failures = []

    def check(name, cond):
        if not cond:
            failures.append(name)

    # rejection layer literals
    check("price $1111 placeholder", price_sanity(1111) is not None)
    check("price $12345 placeholder", price_sanity(12345) is not None)
    check("price $9999 accepted", price_sanity(9999) is None)
    check("price $400 too low", price_sanity(400) is not None)
    check("price 7805551234 insane", price_sanity(7805551234) is not None)
    check("price None unparseable", price_sanity(None) is not None)

    check("salvage hits", blocklist_hit("2015 Civic SALVAGE title", None))
    check("doesnt start hits", blocklist_hit("doesnt start, needs battery", None))
    check("write off hits", blocklist_hit("was a write off", None))
    check("as-is hits", blocklist_hit("selling as is where is", None))
    check("projector clean", blocklist_hit("projector headlights installed", None) is None)
    check("gas issue clean", blocklist_hit("minor gas issue fixed", None) is None)
    check("description scanned", blocklist_hit("2015 Civic", "rebuilt status"))

    check("km 700k rejected", odometer_sanity(700_000, 2015, 2026) is not None)
    check("km 500 on old rejected", odometer_sanity(500, 2015, 2026) is not None)
    check("km 500 on new ok", odometer_sanity(500, 2025, 2026) is None)

    check("family collapse", normalize_family("Ford", "F 150")
          == normalize_family("FORD", "f-150"))
    check("qashqai merge", normalize_family("Nissan", "Qashqai")
          == normalize_family("Nissan", "Rogue"))
    check("fingerprint", fingerprint("FORD:F150", 2015, 151_749, 11_900)
          == "FORD:F150|2015|30|48")

    # FB parsing
    check("year parse", parse_year_from_text("2014 Ford F-150 XLT") == 2014)
    check("km 185,000km", parse_km_text("185,000km runs great")[0] == 185_000)
    check("km 185k km", parse_km_text("185k km")[0] == 185_000)
    check("bare 185k ambiguous", parse_km_text("f150 185k firm")[0] is None)
    km, miles = parse_km_text("only 120,000 miles")
    check("miles converted", km == 193_120 and miles == 120_000)
    # digit tokens before the mileage must not merge into it
    check("4x4 before km", parse_km_text("2014 Ford F-150 4x4 185,000 km")[0] == 185_000)
    check("V8 before km", parse_km_text("F-150 V8 120,000 km")[0] == 120_000)
    check("year before km", parse_km_text("Honda Civic 2014 185,000 km")[0] == 185_000)
    # a bare-k asking price must not become the odometer just because
    # "kms" appears somewhere else in the title
    check("15k obo not km", parse_km_text("2014 F-150 XLT 15k obo, low kms!")[0] is None)
    check("asking 25k not km", parse_km_text("asking 25k firm. kms are low")[0] is None)
    check("185k with odo ctx", parse_km_text("185k on it, low kms, runs mint")[0] == 185_000)
    known = {"makes": set(STATIC_MAKES),
             "models_by_make": {"FORD": {"F150", "FOCUS"},
                                "CHEVROLET": {"SILVERADO", "CRUZE"}}}
    check("find make", find_make("2014 ford f-150 xlt", known["makes"]) == "FORD")
    mk, after = _find_make_span("2014 ford f-150 xlt low kms", known["makes"])
    check("find model", mk == "FORD"
          and find_model(after, known["models_by_make"]["FORD"]) == "F150")
    # aliased makes must still resolve the model from the after-text
    mk, after = _find_make_span("2014 chevy silverado 1500 lt", known["makes"])
    check("alias make", mk == "CHEVROLET")
    check("alias model", find_model(after, known["models_by_make"]["CHEVROLET"]) == "SILVERADO")
    row = parse_fb_listing(
        {"id": "123456789", "price_text": "CA$9,500",
         "text": "2014 Ford F-150 XLT 185,000 km", "label": "trucks_5k_30k"},
        known, "2026-08-05T00:00:00Z")
    check("fb parse full", row["price"] == 9500 and row["year"] == 2014
          and row["make"] == "FORD" and row["model"] == "F150"
          and row["km"] == 185_000)
    # km that echoes the price is discarded (store-only), not trusted
    row = parse_fb_listing(
        {"id": "123456780", "price_text": "$15,000",
         "text": "2012 Ford F-150 15k low kms great truck", "label": "trucks_5k_30k"},
        known, "2026-08-05T00:00:00Z")
    check("price-echo km dropped", row["km"] is None)

    # trim / drivetrain extraction
    check("4x4 -> awd", parse_drivetrain("4x4 SV") == "awd")
    check("FWD -> fwd", parse_drivetrain("FWD 4dr V6 Auto LX") == "fwd")
    check("xDrive28i -> awd", parse_drivetrain("X3 xDrive28i") == "awd")
    check("quattro -> awd", parse_drivetrain("Quattro Technik") == "awd")
    check("conflict -> None", parse_drivetrain("FWD model, also has 4x4 badge") is None)
    check("4Runner not 4x4", parse_drivetrain("2014 Toyota 4Runner") is None)
    check("projector -> None", parse_drivetrain("projector headlights") is None)

    check("XLT tier 1", parse_trim_tier("FORD", "4x4 XLT") == 1)
    check("Platinum tier 2", parse_trim_tier("FORD", "Platinum") == 2)
    check("no trim -> None", parse_trim_tier("FORD", "projector headlights") is None)
    check("SV tier 1", parse_trim_tier("NISSAN", "4x4 SV") == 1)
    check("Nissan bare S", parse_trim_tier("NISSAN", "AWD S 4dr") == 0)
    check("Ford bare S -> None", parse_trim_tier("FORD", "AWD S 4dr") is None)
    check("SEL beats SE", parse_trim_tier("FORD", "SEL AWD") == 1)
    check("LTZ beats LT", parse_trim_tier("CHEVROLET", "LTZ Z71") == 2)
    check("TRD PRO beats TRD", parse_trim_tier("TOYOTA", "TRD PRO 4x4") == 2)
    check("ignore LIMITED SLIP", parse_trim_tier("FORD", "XL limited slip diff") == 0)
    check("Subaru Touring mid", parse_trim_tier("SUBARU", "Touring") == 1)
    check("Honda Touring premium", parse_trim_tier("HONDA", "Touring") == 2)
    check("Jeep Sport base", parse_trim_tier("JEEP", "Sport") == 0)
    check("Honda Sport mid", parse_trim_tier("HONDA", "Sport") == 1)
    check("dealer garbage", parse_trim_tier("CHEVROLET", "LT  - $199.79 /Wk") == 1)
    # bilingual Alberta prose must not tier a listing (short tokens need to
    # be uppercase in the original text)
    check("french le not LE", parse_trim_tier("FORD", "F-150 XL le camion est propre") == 0)
    check("french ce not CE", parse_trim_tier("TOYOTA", "ce char est propre 120k") is None)
    check("french se not SE", parse_trim_tier("TOYOTA", "Corolla se vend vite") is None)
    check("premier proprietaire", parse_trim_tier("CHEVROLET",
          "Cruze premier propriétaire") is None)
    check("accents no stray S", parse_trim_tier("NISSAN",
          "Rogue avec inspection de sécurité") is None)
    check("ex-fleet not EX", parse_trim_tier("FORD", "2018 F-150 XL ex-fleet truck") == 0)
    check("EX-LEASE caps blanked", parse_trim_tier("FORD", "F-150 XL EX-LEASE") == 0)
    check("real Honda EX caps", parse_trim_tier("HONDA", "Civic EX 4dr") == 1)
    check("lowercase xl rejected", parse_trim_tier("FORD", "f-150 xl clean") is None)
    check("short conflict -> None", parse_trim_tier("FORD", "XL or LT trades?") is None)
    check("model-name shield", parse_trim_tier("LEXUS", "2013 Lexus LS 460",
                                               exclude_tokens=("LS",)) is None)
    tier, dt = extract_features("Ford", "4x4 XLT", "2015 Ford F-150", "clean truck")
    check("extract_features", tier == 1 and dt == "awd")
    check("non-string mv survives", extract_features("Ford", 1500, None, None)
          == (None, None))
    # trim never scans the description ("limited warranty" prose)
    tier, _ = extract_features("Ford", "XL", "2015 Ford F-150",
                               "limited warranty included")
    check("description not trim-scanned", tier == 0)
    row = parse_fb_listing(
        {"id": "123456781", "price_text": "CA$9,500",
         "text": "2014 Ford F-150 XLT 4x4 185,000 km", "label": "trucks_5k_30k"},
        known, "2026-08-05T00:00:00Z")
    check("fb trim+drive", row["trim_tier"] == 1 and row["drivetrain"] == "awd")

    # offset math
    offs = compute_offsets(np.array([0.10] * 5 + [0.0] * 20),
                           [2] * 5 + [None] * 20)
    check("shrinkage n=5", abs(offs[2]["off"] - 0.05) < 1e-9)
    offs = compute_offsets(np.array([0.10] * 45), [2] * 45)
    check("shrinkage n=45", abs(offs[2]["off"] - 0.09) < 1e-9)
    check("n=4 absent", compute_offsets(np.array([0.1] * 4), [2] * 4) == {})
    offs = compute_offsets(np.array([1.0] * 100), [0] * 100)
    check("clamp", offs[0]["off"] == OFFSET_CLAMP)
    check("unpack tolerant", unpack_offsets(None) == {} and unpack_offsets("") == {}
          and unpack_offsets("{}") == {} and unpack_offsets("not json") == {})
    packed = json.dumps({"trim": {"2": {"off": 0.08, "n": 9}},
                         "drivetrain": {"awd": {"off": 0.05, "n": 12}}})
    up = unpack_offsets(packed)
    check("unpack int keys", 2 in up["trim"] and up["drivetrain"]["awd"]["n"] == 12)

    # sequential residualization: all tier-2 comps are awd, tier-0 are 2wd ->
    # after trim adjustment the drivetrain offsets must be ~0 (no double count)
    rng2 = np.random.default_rng(11)
    n2 = 60
    ages2 = rng2.uniform(2, 12, n2)
    kms2 = rng2.uniform(50_000, 250_000, n2)
    tiers2 = [2] * 20 + [0] * 20 + [None] * 20
    dts2 = ["awd"] * 20 + ["fwd"] * 20 + [None] * 20
    base_log = 10.4 - 0.08 * ages2 - 0.32 * np.log(kms2 + 1)
    tier_fx = np.array([0.22 if t == 2 else -0.16 if t == 0 else 0.0 for t in tiers2])
    prices2 = np.exp(base_log + tier_fx + rng2.normal(0, 0.06, n2))
    comps2 = [(int(2026 - a), int(k), int(p), "2026-08-05T00:00:00Z", t, d)
              for a, k, p, t, d in zip(ages2, kms2, prices2, tiers2, dts2)]
    packed_fit = _fit_and_pack(comps2, seed=3)
    check("trim offsets learned", 2 in packed_fit["offsets"]["trim"]
          and 0 in packed_fit["offsets"]["trim"])
    check("tier2 positive", packed_fit["offsets"]["trim"][2]["off"] > 0.05)
    check("tier0 negative", packed_fit["offsets"]["trim"][0]["off"] < -0.05)
    dt_offs = packed_fit["offsets"].get("drivetrain", {})
    residual_dt = max((abs(v["off"]) for v in dt_offs.values()), default=0.0)
    check("no drivetrain double-count", residual_dt < 0.04)
    check("mad_adj < mad", packed_fit["mad_adj"] < packed_fit["mad"])

    # end-to-end: offsets change verdicts the right way, unknowns unchanged
    m2 = dict(packed_fit, comp_count=n2, fitted_at="2026-08-05T00:00:00Z",
              model_key="family:TEST:CAR", kind="family")
    dt0 = "fwd" if "fwd" in m2["offsets"].get("drivetrain", {}) else None
    fair0 = math.exp(predict_log_price(m2, 8.0, 150_000)
                     + m2["offsets"]["trim"][0]["off"]
                     + (m2["offsets"]["drivetrain"][dt0]["off"] if dt0 else 0))
    deal_price = int(fair0 * 0.70)
    z0, _ = robust_z(deal_price, m2, 8.0, 150_000, trim_tier=0, drivetrain=dt0)
    z2, _ = robust_z(deal_price, m2, 8.0, 150_000, trim_tier=2, drivetrain="awd")
    check("tier0 deal alerts", z0 < -2.0)
    check("same price on tier2 much deeper", z2 < z0 - 1.0)
    m2_plain = dict(m2, offsets={}, mad_adj=None)
    zu, pu = robust_z(deal_price, m2, 8.0, 150_000)          # unknown features
    zp, pp = robust_z(deal_price, m2_plain, 8.0, 150_000)    # no-offset model
    check("unknown tier strictly unchanged", zu == zp and pu == pp)
    # only ONE of two offset features known -> residual keeps the other
    # feature's variance, so the divisor must stay the RAW mad
    if dt0 and m2["mad_adj"] < m2["mad"]:
        z_partial, _ = robust_z(deal_price, m2, 8.0, 150_000, trim_tier=0)
        expected = (math.log(deal_price)
                    - (predict_log_price(m2, 8.0, 150_000)
                       + m2["offsets"]["trim"][0]["off"])) / (MAD_SCALE * m2["mad"])
        check("partial features use raw mad", abs(z_partial - expected) < 1e-9)
    # a degenerate mad_adj is floored at MAD_MIN, never divides raw noise
    m_degen = dict(m2, mad_adj=0.001)
    z_floor, _ = robust_z(deal_price, m_degen, 8.0, 150_000,
                          trim_tier=0, drivetrain=dt0)
    delta_all = (m2["offsets"]["trim"][0]["off"]
                 + (m2["offsets"]["drivetrain"][dt0]["off"] if dt0 else 0))
    expect_floor = (math.log(deal_price)
                    - (predict_log_price(m2, 8.0, 150_000) + delta_all)) \
        / (MAD_SCALE * MAD_MIN)
    check("mad_adj floored at MAD_MIN", abs(z_floor - expect_floor) < 1e-9)

    # robust fit on contaminated synthetic comps
    rng = np.random.default_rng(42)
    n = 200
    true_b = (10.2, -0.09, -0.35)
    ages = rng.uniform(1, 15, n)
    kms = rng.uniform(20_000, 300_000, n)
    log_kms = np.log(kms + 1)
    y = true_b[0] + true_b[1] * ages + true_b[2] * log_kms + rng.normal(0, 0.12, n)
    junk = rng.choice(n, size=30, replace=False)          # 15% contamination
    y[junk] = np.log(rng.uniform(800, 3000, 30))           # scam/salvage prices
    fit = fit_robust(ages, log_kms, y, seed=7)
    check("fit method", fit["method"] == "theil-sen")
    for i, (est, true) in enumerate(zip((fit["b0"], fit["b1"], fit["b2"]), true_b)):
        check("coef b%d within 30%%" % i, abs(est - true) <= 0.30 * abs(true))
    model_row = dict(fit, comp_count=n, km_min=20_000, km_max=300_000,
                     age_min=1.0, age_max=15.0, fitted_at="2026-08-05T00:00:00Z")
    fair = math.exp(predict_log_price(model_row, 8.0, 150_000))
    z_deal, pct = robust_z(int(fair * 0.60), model_row, 8.0, 150_000)
    check("planted deal z<-2", z_deal < -2.0)
    check("planted deal pct ~40", 30 < pct < 50)
    z_fair, _ = robust_z(int(fair), model_row, 8.0, 150_000)
    check("fair price |z|<1", abs(z_fair) < 1.0)

    # gates
    now = "2026-08-05T00:00:00Z"
    check("gate clean", gate(model_row, 8.0, 150_000, now, "family") == [])
    check("gate extrapolation", "extrapolation_km" in
          gate(model_row, 8.0, 400_000, now, "family"))
    stale = dict(model_row, fitted_at="2026-06-01T00:00:00Z")
    check("gate stale", "model_stale" in gate(stale, 8.0, 150_000, now, "family"))
    few = dict(model_row, comp_count=5)
    check("gate few comps", "comps<8" in gate(few, 8.0, 150_000, now, "family"))
    degen = dict(model_row, mad=0.001)
    check("gate degenerate", "mad_degenerate" in gate(degen, 8.0, 150_000, now, "family"))

    # mining
    cands = mine_blocklist_candidates(
        ["no title in hand", "no title, bill of sale only", "title missing no title"],
        ["clean title, runs great", "one owner"], ())
    check("mining finds 'no title'", any("title" in g for (g, _, _) in cands))

    if failures:
        print("SELFTEST FAILURES:")
        for f in failures:
            print("  -", f)
        raise SystemExit(1)
    print("scoring selftest: all checks passed")


if __name__ == "__main__":
    _selftest()
