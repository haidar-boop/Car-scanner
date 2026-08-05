# Edmonton Car Deal Scanner

Two scrapers, one goal: be first to message the seller on genuinely underpriced
cars around Edmonton. Precision over recall at every decision point.

- `autotrader_watcher.py` — polls three AutoTrader.ca searches from a droplet,
  stores every listing in SQLite (`listings.db`), Telegram-notifies new ones.
- `fb_marketplace_watcher.user.js` — Tampermonkey userscript; one pinned tab
  rotates through four Marketplace searches (cars ×2 price bands, trucks, SUVs)
  on a randomized 5–15 min reload.

**Status: Step 1 of 5** (search targets locked, baseline watch loop). Deal
scoring, alert formatting, failure hardening, and the full README arrive in
Steps 2–5. Note: everything scraped is an **asking** price, not a transaction
price.

## Install (droplet)

```sh
apt install python3-pip && pip3 install requests beautifulsoup4
mkdir -p /opt/car-scanner && cp autotrader_watcher.py /opt/car-scanner/
cp car-scanner.env.example /etc/car-scanner.env  # fill in real values
chmod 600 /etc/car-scanner.env
cp car-scanner.service /etc/systemd/system/
useradd -r carscanner && chown carscanner /opt/car-scanner
systemctl daemon-reload && systemctl enable --now car-scanner
journalctl -u car-scanner -f
```

Telegram credentials come from environment variables only
(`TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`); with them unset the watcher runs
and stores listings but only logs what it would have sent.

## Install (browser)

Install the userscript in Tampermonkey, pin a tab on any
`facebook.com/marketplace` page, and enter the Telegram token/chat ID when
prompted once (stored in Tampermonkey storage, never in the file).

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
