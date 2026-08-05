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

**Status: Step 4 of 5** (scoring in shadow mode, phone-ready alerts, daily
digest, silent-failure alarms). `--test` and the full troubleshooting guide
arrive in Step 5.

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

## Install (droplet)

```sh
apt update && apt install -y python3-requests python3-bs4 python3-numpy
# (Debian 12 / Ubuntu 23.04+ block bare pip3 installs — PEP 668. If you'd
#  rather use pip, make a venv and point the unit's ExecStart at its python.)
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

Most FB anchor rows lack a parseable year or km — those are stored unscored
(`reject_reason='incomplete'`) as future reference data. That is by design:
a guessed comp is worse than no alert. FB listing lifespans are not tracked
(the droplet can't fetch FB pages behind the login wall).

## Database

`listings` (every listing ever seen, with `family`, `fingerprint`,
`reject_reason`, `z`, `pct_below`, `disappeared_at`), `price_history` (every
observed price), `models` (cached fit coefficients + comp ranges + fitted_at),
`alerts` (every real or shadow alert with the coefficients frozen at firing
time, plus your labels), `scans` (one row per search fetched or ingest batch —
scan volume the `listings` table can't show, since a cycle that re-sees 20
known listings inserts nothing), `meta` (seed flags, shadow_until, schedules).

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
