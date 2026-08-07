# Project pickup — resume here (paused 2026-08-07, resuming ~late August)

The build is DONE and fully committed. This file is the map back in.

## State when paused

| Piece | Status |
|---|---|
| Code (watcher, scoring, userscript, AI gate, proxy support) | ✅ complete, tested, all on this branch |
| Telegram bot | ✅ created: **@EDMYEGbot**, chat ID `7698957215`. Token is in BotFather (`/mybots` → API Token) — it was never written down anywhere else. |
| Droplet | Was created and verified (Telegram self-test delivered), but AutoTrader 403-blocks datacenter IPs, so it can't scan without the proxy below. Fine to destroy while away and recreate on return — nothing irreplaceable lives on it. |
| Residential proxy | ❌ not purchased yet — this is the FIRST thing to do on resume. Decision made: **IPRoyal, Residential, pay-as-you-go, 2 GB** (~$7). The scanner uses ~3–5 GB/month. |
| Facebook side (Tampermonkey on Chromebook) | ❌ not installed yet — independent of everything else, do any time. |
| AI verification layer | ✅ built, dormant until `ANTHROPIC_API_KEY` is set (waiting on API credits). |

## Resume checklist, in order

1. **Buy the proxy**: iproyal.com → Residential Proxies → pay-as-you-go → 2 GB.
   From the dashboard take: hostname (usually `geo.iproyal.com`), port (usually
   `12321`), username, password → they form
   `http://USERNAME:PASSWORD@geo.iproyal.com:12321`
2. **(Re)create the droplet** if it was destroyed: DigitalOcean, Ubuntu 24.04,
   cheapest $6 Basic, password auth. Then follow **README → Install (droplet)**
   verbatim. Env values needed: bot token (BotFather), chat ID above,
   `INGEST_TOKEN` (`openssl rand -hex 24` — generate fresh),
   `AUTOTRADER_PROXY_URL` (step 1).
3. **Verify**: `python3 autotrader_watcher.py --test` (with env sourced) —
   expect `20 parsed` per search (not `http_403`) and one Telegram message.
   Then `systemctl daemon-reload && systemctl enable --now car-scanner`.
4. **Chromebook**: README → Install (browser). Remember to replace
   `YOUR_DROPLET_HOST` (2 places) with the droplet IP, and enter the bot
   token + chat ID + ingest token via the Tampermonkey menu prompts.
5. **First 14 days = shadow mode**: digests at 8 PM, no alerts yet — label the
   "would have fired" entries (`--label N good|bad|scam|already_gone`).
6. **When API credits exist**: `pip3 install --break-system-packages anthropic`,
   set `ANTHROPIC_API_KEY=` in `/etc/car-scanner.env`, restart the service.
   (README → "AI verification" section.)

## Things a future session should know

- Every step above is documented in detail in README.md — this file is just
  the sequencing and the state.
- The FB item-page enrichment keys AND the `listing_photos` photo key are
  assumptions until the first real run. After ~1 hour live, check the digest's
  `FB enrich` line / `--test` section 5b: `ok`-dominant = fine;
  `no_keys`/`failed`-dominant = the extractor key names need fixing against a
  real item page (view-source one listing).
- AutoTrader photo URLs: strip the size suffix for full-res (already handled
  in `parse_photos`). The WAF block signature and proxy rationale are in
  README → Troubleshooting → "Every AutoTrader fetch 403s".
