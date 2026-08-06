# Edmonton Car Deal Scanner

Two scrapers, one scorer, one goal: be first to message the seller on genuinely
underpriced cars around Edmonton. Precision over recall at every decision point.

- `autotrader_watcher.py` — polls three AutoTrader.ca searches from a droplet,
  stores every listing in SQLite (`listings.db`), scores them, and Telegram-alerts
  real deals. Also hosts the ingest endpoint that receives Facebook listings.
- `scoring.py` — the four-layer scoring engine (see below).
- `fb_marketplace_watcher.user.js` — Tampermonkey userscript; one pinned tab
  rotates through four Marketplace searches (cars ×2 price bands, trucks, SUVs)
  on a randomized 5–15 min reload and posts what it scrapes to the droplet.

**Status: complete** (all five steps). The system scans, scores in shadow
mode for its first two weeks, alerts, digests, and shouts when it breaks.

**The asking-price caveat, stated plainly:** everything scraped is an **asking**
price, not a transaction price. The model predicts what a car will be *listed*
at, not what it is worth — a car asking 20% below the curve may just have a
realistic seller. The listing-lifespan signal (`disappeared_at`) is the partial
correction: listings that vanish within ~48 h very likely sold fast.

## The four scoring layers

1. **Rejection** — before anything is measured: price sanity (<$500, placeholder
   patterns like $1111/$12345, unparseable), a condition blocklist (salvage,
   rebuilt, flood, no start, as-is, lien, … — editable at the top of
   `scoring.py`), data completeness (no year/make/model/km → stored but never
   scored), odometer sanity, and 90-day repost/cross-post fingerprints
   (family+year+km/5000+price/250). Rejected and damaged rows are also excluded
   from comp fitting — legitimately cheap cars must not drag the curve down.
   Every rejection is logged with its reason.
2. **Price model** — per model family (normalized make+modelGroup):
   `log(price) = b0 + b1·age + b2·log(km+1)`, fit with generalized Theil-Sen
   (median over exact solutions of random point-triples — robust to the very
   outliers being hunted), Huber-IRLS fallback. Refit nightly at 03:00 or after
   200 new comps per family; scoring is a cached lookup, never a fit. Families
   under 30 comps fall back to a segment model (their search's price/body band)
   and alerts get marked **[LOW CONF]**.
3. **Robust z-score** — `z = (log(price) − predicted) / (1.4826 × MAD)` of the
   family's residuals. Alerts fire at `z ≤ −2.0` (per source), dealers at
   `z ≤ −2.5` (their pricing is already market-calibrated). Alerts report both
   z and percent-below-predicted.

   **Trim & drivetrain offsets.** A base XL 2WD and a Platinum 4x4 sit on
   very different price ladders, so after each family fit the comps'
   residuals are grouped by extracted trim tier (base/mid/premium, from a
   keyword lexicon at the top of `scoring.py`) and drivetrain (awd vs 2wd),
   and each group with ≥5 comps gets a shrunk, clamped median offset —
   sequentially (trim first, then drivetrain on trim-adjusted residuals, so
   correlated features never double-count). Scoring adds the offsets that
   apply and divides by the offset-adjusted MAD. **A listing whose trim or
   drivetrain can't be parsed scores exactly as if the offsets didn't exist**
   — raw prediction, raw MAD. That dual-MAD rule is load-bearing: dividing an
   unadjusted residual by the smaller adjusted MAD would inflate |z| and mint
   false positives on precisely the rows we know least about. Don't
   "simplify" it away. Alerts show what was applied
   (`… (F150 · base trim -10% · 2WD -2%)`), and a base-spec listing scored
   without a matching offset is flagged — its discount may read high.

   To edit the lexicon (add a trim token, fix a tier): change the lists at
   the top of `scoring.py`, bump `TRIM_LEXICON_VERSION`, restart — the
   migration re-extracts every stored row, and the next refit relearns the
   offsets. Tiers are buckets, not strict ordinals: what matters is that a
   token lands in the same bucket consistently within a family.
4. **Confidence gates** — no alert at all when the model can't be trusted:
   fewer than 8 comps, degenerate or absurd residual spread, the listing's km
   or age outside the fitted comps' range (no extrapolating depreciation
   curves), or a fit older than 30 days.

## What lands on your phone

An alert leads with the numbers that decide whether to drive across town,
because the first line is all a notification preview shows:

```
⚡ 31% below · z=-2.9 · $22,741 · 2015 Ford F-150
predicted $32,958 from 45 comps (FORD:F150)
145,000 km · Private · Edmonton · seen 3m ago
https://www.autotrader.ca/offers/...
```

Low-confidence and dealer alerts say so on their own line (`⚠️ LOW CONFIDENCE
— segment model, too few comps for this family`, `🏪 DEALER — priced by a pro,
check for a catch`). The URL is always last so it stays tappable.

"seen 3m ago" is how long *we* have known about the listing, not its true
posting age — neither site exposes when a listing was actually posted. It
still answers the question that matters: whether you are first.

At 8 PM Edmonton time a digest summarizes the day — listings scanned per
source and per search, alerts fired, rejections by reason, and the five best
scores (marking which ones alerted). It goes out even on empty days, since
silence is indistinguishable from a dead pipeline. Telegram or network
failures are logged and swallowed; nothing can take down the poll loop.

## Silent-failure alarms

The dangerous failure isn't a crash — it's a parser that returns zero
listings while the service looks healthy, so you conclude Edmonton has no
deals for three weeks. Every path below announces itself:

| Failure | Detection | Warning |
|---|---|---|
| Parser returns nothing | 2 consecutive empty scans, per search | `🚨 BROKEN [source/search]` naming the search and reason |
| Droplet IP blocked | Cloudflare/bot-wall signatures in the response (status **and** body markers) | Same alarm, worded as a bot wall with a `curl` next step |
| FB markup changed | 5 consecutive ticks (~100 s) with zero listing anchors | `🚨 BROKEN [facebook]` + red in-page banner |
| FB bridge died (tab closed, logged out, bad token) | no ingest received for 6 h | `⚠️ FACEBOOK SILENT` |
| **Partial** breakage | last 24 h volume below 40% of the trailing 7-day daily average, per source | `⚠️ VOLUME DROP` |
| Search params silently dropped | applied-params echo compared against the request | `WARNING … search looks broken` |

One empty scan is a blip and stays quiet; two in a row is breakage. All
alarms are rate-limited (6 h, 12 h for volume) and the cooldown is stamped
only after Telegram actually accepts the message — an outage delays an
alarm, it never swallows it. The volume check needs 4 days of history
before it will fire, so ramping up doesn't look like breakage.

Poll timing stays randomized: 5–9 min per cycle on the droplet (plus 2–5 s
between searches), 5–15 min per reload in the browser.

## Logs

Human-readable lines go to stderr (`journalctl -u car-scanner -f`). The
same events go to a rotating file as one JSON object per line —
`car-scanner.log`, 10 MB × 5 backups, path overridable with
`CAR_SCANNER_LOG`. Tagged events: `scan_ok`, `scan_empty`,
`scan_recovered`, `fetch_failed`, `reject`, `alert`, `suppress`,
`volume_drop`, `fb_silent`, `lifespan_gone`, `refit`.

```sh
# what got rejected today, by reason
grep '"event":"reject"' car-scanner.log | jq -r .reason | sort | uniq -c
# every alert with its z-score
grep '"event":"alert"' car-scanner.log | jq -r '[.ts,.z,.pct_below,.msg]|@tsv'
```

## Verifying it works: `--test`

```sh
# on the droplet — source the env file, or the run has no Telegram
# credentials (systemd normally supplies them) and prints the message
# instead of sending it
set -a && . /etc/car-scanner.env && set +a
python3 /opt/car-scanner/autotrader_watcher.py --test
```

One live fetch, end to end, against a **throwaway copy** of the database
(SQLite backup API, WAL-safe against the running service) — it prints
listings parsed per search, the rejection breakdown, the models it fitted,
and a table of the top scored listings with z, percent below and comp count,
then sends **exactly one** Telegram message and deletes the copy. Incidental
warnings that would normally fire are collected and counted instead of sent,
so the one-message promise holds even when a search is broken. Real output:

```
--- 1. LIVE FETCH ------------------------------------------------
  cars_2k_15k          20 parsed of 1,748 site-wide    1.2s  OK
  cars_15k_35k         20 parsed of 4,916 site-wide    1.0s  OK
  trucks_suvs_5k_30k   20 parsed of 3,502 site-wide    1.7s  OK
  60 listings fetched (45 unique — searches overlap), 6 not already in the database

--- 2. REJECTION BREAKDOWN ---------------------------------------
  incomplete                 2  (4%)
  clean                     43  (96%)  eligible for scoring

--- 4. TOP SCORED LISTINGS ---------------------------------------
   z        below   comps  price     vehicle                     verdict
   -2.78    30%     21     $6,680    2014 Dodge Journey          would ALERT
   -1.51    26%     21     $16,999   2020 Nissan Kicks           above threshold
   -1.49    17%     21     $9,997    2019 Dodge Journey          above threshold
   ...
```

Run it after any change to the search URLs, after a site redesign, or any
time the alerts go quiet and you want to know whether that's the market or
the scraper.

## How much data before you can trust the scorer

Measured on this market, not guessed: two page-one snapshots 1.21 h apart
shared 28 of 46 listings (so nothing had rolled off unseen), giving
**~297 new listings/day** across the three searches and a page-one turnover
of ~4.8 h. At a 7-minute poll that's ~1.4 new listings per cycle against 20
slots per search — the poller is comfortably fast enough, with ~40× headroom.

Thresholds the code enforces: **8 comps** minimum to score at all,
**30 comps** for a family to get its own curve (below that it borrows the
segment model and the alert is marked low-confidence).

What that means in days, from the observed family mix (~56 families in the
sample; shares are long-tailed):

| Family | Share of arrivals | Reaches 8 comps | Reaches 30 comps |
|---|---|---|---|
| RAV4 | ~5.7% | ~0.5 day | ~2 days |
| Escape / Rogue | ~4.5% | ~0.6 day | ~2 days |
| Tucson / Tiguan / Sentra | ~3.4% | ~0.8 day | ~3 days |
| median family | ~1.8% | ~1.5 days | ~6 days |

- **After ~3 days** the top ~8 families have their own curves, covering
  roughly a third of arriving listings.
- **After ~1 week** ~20 families are covered — about 60% of arrivals — and
  the daily digest becomes genuinely informative.
- **After ~2 weeks** (the shadow-mode window, not a coincidence) most
  regularly-listed families have 30+ comps.

My honest recommendation: **treat z-scores as advisory until a family has
~50 comps.** Thirty is enough to fit a stable curve, but the MAD — the
denominator of every z — is still noisy there, so a z of −2.2 on a
30-comp family is a weaker claim than the same number on a 100-comp one.
The comp count is printed in every alert for exactly this reason. Rare
families (one listing a week) may never reach 30; they will keep scoring
through the segment model, flagged low-confidence, which is the correct
outcome rather than a false precision.

Caveat on the numbers above: they come from ~88 real listings pooled over a
few hours, so the long tail is under-sampled and rare families will be
slower than the table suggests. Re-run `--test` after a week to see real
counts — it prints exactly this breakdown from your own database.

## Tuning

**The z threshold** lives in `scoring.py`:

```python
Z_ALERT = {"autotrader": -2.0, "facebook": -2.0}
Z_ALERT_DEALER = -2.5
```

More negative = fewer, better alerts. Change it based on the labels you
collect, not on a hunch: after a couple of weeks of `--label`, the Sunday
report gives you a precision rate. If precision is high and you feel you're
missing deals, loosen to −1.8; if you're triaging junk, tighten to −2.3.
Move it 0.2 at a time and wait a week — a threshold change alters which
listings alert, and you need a fresh batch of labels to judge it. The
dealer threshold should stay stricter than the private one; dealer pricing
is already market-calibrated, so an apparent dealer bargain more often has
a catch.

Other knobs worth knowing, all in `scoring.py`: `BLOCKLIST_TERMS` (add
anything the weekly report suggests), `MIN_COMPS_GATE` (8),
`FAMILY_MODEL_MIN_COMPS` (30), `MAD_MIN` / `MAD_MAX` (degenerate and absurd
spread bounds), `FINGERPRINT_TTL_DAYS` (90-day repost suppression).

## Adding a fourth search

AutoTrader — append to `SEARCH_URLS` in `autotrader_watcher.py`:

```python
{
    "label": "luxury_35k_60k",
    "url": "https://www.autotrader.ca/cars/reg_ab/cit_edmonton/ot_used"
           "?pricefrom=35000&priceto=60000&sort=age&desc=1&zipr=100",
},
```

**Then add the label to `SEGMENT_MAP` in `scoring.py`** — this is easy to
miss and the search will never score without it:

```python
SEGMENT_MAP = {
    ...
    "luxury_35k_60k": "luxury_35k_60k",   # its own segment, or point it at
                                          # an existing one to share comps
}
```

Segment models are fitted only over `set(SEGMENT_MAP.values())`, and a
listing whose `search_label` isn't in the map gets no fallback model — so
until its family reaches 30 comps on its own, nothing from that search can
score at all. `--test` will show this as `nothing scored`.

Other rules that matter: `sort=age&desc=1` is mandatory (only page one is
read, so any other sort silently hides new listings); keep `zipr=100`; use a
unique `label`, since it also keys the seed flag and the health streak.
Body-type codes for `body=`: Hatchback 1, Convertible 2, Coupe 3,
Wagon 5, Sedan 6, Others 7, Minivan 12, SUV 14, Pick-up 15 (comma-separate
for several; an invalid code silently returns zero results, which
`--test` will show you). Restart the service — the new search seeds
silently on its first clean pass, so it won't flood you, and it starts
contributing comps immediately.

Facebook — add to `SEARCH_URLS` in the userscript. Keep
`sortBy=creation_time_descend`. More entries means each is visited less
often (one tab rotates through all of them every 5–15 min), so past four or
five searches, freshness suffers.

## Troubleshooting

**Facebook changed its DOM.** Symptom: `🚨 BROKEN [facebook]` plus a red
banner in the tab, or the FB digest line stuck at zero. The scraper
deliberately depends on one thing only — anchors whose `href` contains
`/marketplace/item/` — because everything else in FB's markup (class names,
wrappers, aria labels) churns constantly. To fix: open the pinned search,
DevTools → Console, and run

```js
document.querySelectorAll('a[href*="/marketplace/item/"]').length
```

Zero means the anchor pattern moved; find a listing card, copy its link
element, and update the selector in `scrapeListings()`. Non-zero while the
script still warns means the *price/title* extraction broke instead — check
`a.innerText` on one card: the script expects the price on its own line and
strips that line from the title, falling back to excising the price by
regex when everything is glued into one string. FB rows whose year or km
can't be parsed are stored unscored on purpose, so a partial break degrades
gracefully rather than producing wrong comps.

**AutoTrader stopped parsing.** `🚨 BROKEN [autotrader/...]`. Run
`--test`: if it says `NO __NEXT_DATA__`, the site moved off its embedded
JSON and `parse_listings` needs rewriting; if it says `PROBLEMS: param
'sort' applied as ...`, the URL format changed again (this already happened
once — the old `/cars/ab/edmonton/?srt=` format now 301s and drops the
sort). If the alarm mentions a bot wall, curl the search URL from the
droplet: a challenge page means the IP is blocked, and the fix is a
different IP or a slower cadence, not a parser change.

**Alerts went quiet.** Check the digest first — if scan counts look normal
and rejections are unremarkable, the market is genuinely quiet. If
`VOLUME DROP` fired, one source is degraded while the other masks it.

## Shadow mode and the feedback loop

For the first **14 days** after scoring starts, nothing buzzes your phone:
alerts are recorded silently and the 8 PM digest gains a "would have fired"
section listing each one with its z, discount and link. Judge the precision
over a week of real days, tune, then let it go live.

Label alerts from your phone (via SSH) after checking them:

```sh
python3 autotrader_watcher.py --label 12 good     # or bad | scam | already_gone
```

Sunday 6 PM a weekly report lands in Telegram: alerts fired, labeled precision,
and blocklist-term suggestions mined from bad/scam-labeled listings. Manual
triggers: `--digest`, `--report` (both send immediately and exit).

## AI verification (optional)

The last gate before a Telegram alert fires — strictly **after** the price
gate, blocklist and rejection layer have all passed. A single Claude call
(model `claude-opus-5`) reads the listing's title, full description and up
to 5 photos, and returns one of three verdicts:

- **clear** — alert sends unchanged.
- **caution** — alert sends with a one-line 🤖 note (e.g. "rear quarter
  panel damage visible in photo 3") inserted above the URL.
- **reject** — alert is suppressed. Reserved for high-confidence fraud or
  unmistakable damage; the alert row is still written with the verdict, so
  it shows in the digest as `N ai-rejected`, stays labelable via `--label`,
  and is never resurrected by the delivery-retry sweep.

Why it exists: the blocklist is exact keyword matching — "bent frame" in
prose, a salvage-yard background, or a stock dealer photo on a private
listing all sail past it. The AI reads like a person. It is biased toward
letting alerts through: a missed warning costs ten minutes of your time; a
wrongly suppressed real deal costs the deal.

**Cost.** It runs only on alert-bound listings (the 1-5/day that clear
every other gate), never the ~300/day scanned. Roughly **$1-5/month**.

**Setup.** `pip install anthropic` (a deliberate optional extra beyond the
core requests/bs4/numpy stack), then set `ANTHROPIC_API_KEY` in
`/etc/car-scanner.env` (key from console.anthropic.com, pay-as-you-go).
`AI_VERIFY_ENABLED=0` is the kill switch.

**Fail-open guarantee.** No key, SDK not installed, API error, or timeout
(60 s cap) — the alert sends exactly as it does today, unchanged and
undelayed beyond that cap. The feature can only ever *annotate or
suppress* an alert the scorer already fired; it never touches scoring,
comps, or the rejection layer, and `--test` never calls the API.

Photo sources: AutoTrader photos come free with the search JSON we already
store (full-resolution originals). Facebook photos ride the item-page
enrichment fetch — the `listing_photos` extractor key is an assumption
until the first real run, like every key in that table; check `--test`
section 5b's `keys matched` line. FB photo URLs are signed and expire in
hours, which is fine: the check runs minutes after discovery, never on a
backfill.

## Install (droplet)

```sh
apt update && apt install -y python3-requests python3-bs4 python3-numpy
# (Debian 12 / Ubuntu 23.04+ block bare pip3 installs — PEP 668. If you'd
#  rather use pip, make a venv and point the unit's ExecStart at its python.)
# Optional, for the AI verification gate (see section above):
#   apt install -y python3-pip && pip3 install --break-system-packages anthropic
#   (or use a venv; the scanner runs fine without it — alerts just skip the AI check)
mkdir -p /opt/car-scanner && cp autotrader_watcher.py scoring.py /opt/car-scanner/
cp car-scanner.env.example /etc/car-scanner.env  # fill in real values
chmod 600 /etc/car-scanner.env
cp car-scanner.service /etc/systemd/system/
useradd -r carscanner && chown carscanner /opt/car-scanner
systemctl daemon-reload && systemctl enable --now car-scanner
journalctl -u car-scanner -f
```

## FB → droplet bridge

FB listings get the same scoring and cross-source dedupe as AutoTrader; the
droplet decides what alerts. Setup:

1. Generate a token: `openssl rand -hex 24` → set `INGEST_TOKEN` in
   `/etc/car-scanner.env`, restart the service. (Empty token = ingest disabled;
   the server never runs unauthenticated.)
2. Open the port: `ufw allow 8477/tcp` — or better, restrict to your home IP:
   `ufw allow from YOUR.HOME.IP to any port 8477 proto tcp`.
3. In the userscript, replace `YOUR_DROPLET_HOST` with your droplet's IP or
   hostname (a global find-replace is safe), reinstall it, then set the token
   via Tampermonkey menu → "Set droplet ingest token".

Traffic is plain HTTP; the bearer token is the only protection. The payload is
public listing data, so the worst case of a sniffed token is fake listings —
treat the token as disposable, or put an nginx/caddy TLS proxy in front later.

FB anchor rows lacking a parseable year or km are stored unscored
(`reject_reason='incomplete'`) as future reference data — a guessed comp is
worse than no alert. FB listing lifespans are not tracked (the droplet can't
fetch FB pages behind the login wall).

### Item-page enrichment

When the userscript spots a **new** listing, it quietly fetches that
listing's own page *with your logged-in session* and extracts the
description and odometer before shipping it to the droplet. That gives the
blocklist eyes on "rebuilt title" for the scam-heaviest source, and turns
most FB rows from `incomplete` into scoreable listings with real km.

**This touches your real Facebook account.** The caps are deliberately
conservative and the safety posture is back off, never push:

| Cap | Value |
|---|---|
| New listings enriched per tick | 4 |
| Gap between item-page fetches | 4–9 s jittered |
| Fetches per rolling hour | 30 |
| Fetch timeout | 8 s |
| Seed (install-time) listings | idle-priority drip, 1 per 75 s, only while half the hourly budget is free |
| On login/checkpoint/429 response | enrichment disabled 6 h + Telegram warning |

A checkpoint warning on Telegram is an account-flag signal: **reduce the
caps or set `ENRICH_ENABLED = false`**, don't raise them. Sold/deleted
listings ("content isn't available") are expected in a hot market and never
trigger the disable. With enrichment off or failing, every listing still
ships bare within ~3 minutes worst case (normally 10–20 s) — enrichment can
delay a listing, never lose one. `@connect www.facebook.com` in the header
exists because GM_xmlhttpRequest requires host whitelisting even for the
page's own domain.

**The extraction keys are assumptions until your first real run.** The
sandbox this was built in cannot see past FB's login wall, so the extractor
table targets the community-known GraphQL keys (`redacted_description`,
`vehicle_odometer_data`, `marketplace_listing_title`, …). Verify with one
real look: open any listing, view page source, and search for
`redacted_description` — if FB renamed it, update the regex table in
`extractItemFields()`. The telemetry tells you without guessing:

- daily digest line — `FB enrich (n=124): ok 78% · … — km 81% · desc 85%`;
  a collapsing ok% means FB changed their JSON;
- `--test` section 5b — per-key match counts and top error kinds from real
  rows (`no_keys` dominating = key names wrong);
- raw data: `SELECT json_extract(raw_json,'$.enrich') FROM listings WHERE
  source='facebook' ORDER BY first_seen_at DESC LIMIT 20;`

Description text feeds the blocklist and drivetrain detection only — never
trim (seller prose says "limited warranty"), never identity ("will trade
for a Honda Civic"), never km ("timing belt done at 120,000 km"; the
structured odometer field carries the real reading). The enriched
`vehicle_seller_type` is stored raw but not yet used for dealer detection —
its value vocabulary is unknown until real pages are observed.

## Database

One SQLite file (`listings.db`, WAL mode). Migrations are idempotent and run
at startup, so upgrading is just `git pull` + restart.

**`listings`** — every listing ever seen, PK `(source, id)`:

| Column | Meaning |
|---|---|
| `source`, `id`, `url` | `autotrader` \| `facebook`; site's own listing id |
| `title`, `year`, `make`, `model`, `km`, `price` | parsed vehicle facts |
| `seller_type`, `city`, `distance_km` | `Dealer`/`Private` (AutoTrader only) |
| `description`, `is_damaged`, `result_type`, `price_label` | `result_type` marks boosted (`Nfm`) rows; `price_label` is the site's own opinion, never used for scoring |
| `family` | normalized `MAKE:MODELKEY` — the cross-source comp key |
| `fingerprint` | `family\|year\|km/5000\|price/250` — repost/cross-post key |
| `reject_reason` | `NULL` = passed every rejection layer; only these become comps |
| `z`, `pct_below`, `scored_at` | latest score; cleared when the price changes |
| `first_seen_at`, `last_seen_at` | UTC ISO8601 |
| `disappeared_at`, `gone_suspected_at`, `last_checked_at` | lifespan tracking; `disappeared_at` needs two confirmations |
| `km_converted_from_miles` | original miles value when converted |
| `raw_json` | the full scraped object, so a future column can be backfilled |

**`models`** — one cached fit per family and per segment: `model_key`
(`family:FORD:F150` / `segment:cars_2k_15k`), `kind`, `b0`/`b1`/`b2`, `mad`,
`comp_count`, `km_min`/`km_max`/`age_min`/`age_max` (the extrapolation gate's
bounds), `method` (`theil-sen`/`huber-irls`/`median`), `fitted_at`.

**`alerts`** — one row per alert, real or shadow, `UNIQUE (source, listing_id)`:
`z`, `pct_below`, `price`, `predicted_price`, `model_key`, the coefficients
**frozen at firing time** (`b0`,`b1`,`b2`,`mad`,`comp_count`), `is_dealer`,
`low_confidence`, `shadow`, `fired_at`, and your `label`/`labeled_at`.
Freezing the coefficients is what makes the weekly precision report
meaningful — you can tell whether a bad alert came from a bad model or a
bad listing.

**`price_history`** — every observed price including the first, so the
original asking price survives later edits.

**`scans`** — one row per search fetched and per ingest batch
(`scanned_at`, `source`, `search_label`, `parsed_count`, `new_count`).
`listings` cannot answer "how much did we scan today", because a cycle that
re-sees twenty known listings inserts nothing; this feeds the digest and the
volume-drop alarm.

**`meta`** — key/value: per-search seed flags, `shadow_until`, health
streaks, last-run dates and alarm cooldowns.

Telegram credentials come from environment variables only
(`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`); with them unset the watcher runs
and stores listings but only logs what it would have sent.

## Install (browser)

Install the userscript in Tampermonkey (after the bridge edits above), then
pin a tab and open the **first URL from `SEARCH_URLS`** in it — the watcher
only activates on its own configured searches, so ordinary Marketplace
browsing in other tabs is left untouched (no alerts from recommendation
feeds, no surprise redirects). Enter the Telegram token/chat ID when prompted
(used only for the script's own health warnings; deal alerts come from the
droplet). Each search seeds silently on its first clean scan, and scraped
listings are buffered in Tampermonkey storage and retried until the droplet
accepts them, so nothing is lost across reloads.

**One-time manual check (can't be automated):** open each of the four search
URLs from the top of the userscript while logged in, and confirm results are
sorted newest-first and that `sortBy=creation_time_descend` survives in the
address bar. The script shows a red banner and suspends alerts if a page loses
that sort parameter.

## Search URL notes

AutoTrader.ca now runs on an AutoScout24-based stack. Old-format URLs
(`/cars/ab/edmonton/?srt=…`) still redirect but **silently lose their sort
parameter** — always use the new format with `sort=age&desc=1` ("Posted Date:
New to Old"). Body-type codes for the `body=` param: SUV=14, Pick-up Truck=15.
The watcher verifies on every poll that the server echoed the requested params
back (`pageProps.pagePath`) and suppresses notifications — with a Telegram
warning — when a search looks broken.
