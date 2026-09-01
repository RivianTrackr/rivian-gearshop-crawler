# RivianCrawlr by RivianTrackr

Monitors [gearshop.rivian.com](https://gearshop.rivian.com) and [rivian.com/support](https://rivian.com/support) for changes and sends alerts via email and Discord. Includes a dark-themed admin panel for managing crawlers, notifications, and browsing historical data.

## Features

- **Gear Shop Crawler** — Tracks product inventory, prices, and availability across the Rivian Gear Shop
- **Support Article Crawler** — Tracks Rivian Support articles for content, title, and URL changes
- **Offers Crawler** — Tracks rivian.com/offers for new, removed, and changed promotions
- **Admin Panel** — Dark-themed web UI for managing crawlers, viewing data, and configuring notifications
- **Multi-channel Notifications** — Email (Brevo SMTP) and Discord webhook alerts with per-event toggles
- **Content Filters** — Configurable patterns to ignore noisy article sections (e.g. "Related articles") from triggering false-positive diffs

## How It Works

### Gear Shop Crawler

1. **Crawl** — Uses Playwright (headless Chromium) to infinite-scroll the collection page and discover all products
2. **Fetch** — Pulls `/products/{handle}.json` for each product to get variants, prices, and availability
3. **Availability fallback** — If the JSON says unavailable, tries variant API → product JS → HTML JSON-LD → button text parsing
4. **Store** — Saves snapshots to a SQLite database for historical tracking
5. **Diff** — Compares current crawl to previous snapshots to detect new products, removals, and price/availability changes
6. **Notify** — Sends email and/or Discord alerts with a detailed change report
7. **Export** — Writes a `gearshop.json` snapshot for frontend consumption

### Support Article Crawler

1. **Discover** — Navigates the support hub and category pages to find all article URLs
2. **Extract** — Visits each article page to capture title and full body text
3. **Hash** — Computes SHA-256 of normalized body text (after applying content filters) for stable comparison
4. **Diff** — Detects new articles, removals, title changes, body changes, and URL changes
5. **Notify** — Sends email and/or Discord alerts with inline unified diffs

### Safeguards

- **Anomaly guard** — Skips a run if item count drops >50% vs last good run (prevents false alerts from crawl failures)
- **Removal confirmation** — Requires 3 consecutive misses before reporting a product/article as removed
- **Dedup memory** — Tracks already-reported removals to prevent repeated alerts
- **Content filters** — Strips noisy sections (like "Related articles") before hashing, so they don't trigger change notifications
- **Rate limiting** — Caps HTML fallback checks at 200/run, throttles JSON and article fetches
- **Retry on 429/5xx** — Shopify rate-limits `/products/<handle>.json` mid-run; every fetch retries with exponential backoff honouring `Retry-After`, under a per-run time budget. Without it a single 429 skipped that product for the whole run, so its price/availability change that hour was never recorded. 404 is never retried — removal detection treats it as a real answer
- **SQLite busy timeout** — 30-second retry window prevents "database is locked" errors from concurrent access
- **Indexed hot paths** — the per-variant snapshot lookup that runs once per variant per crawl is index-backed; unindexed it made a run cost O(variants x snapshots)
- **Bounded marker history** — `crawl_markers` is pruned to the newest `MARKER_RETENTION_RUNS` runs instead of growing forever
- **Error alerts** — Sends notifications on browser launch failures, anomalies, and unhandled exceptions

## Admin Panel

A dark-themed web UI built with FastAPI and Pico CSS for managing both crawlers.

- **Dashboard** — System metrics (memory, CPU, disk, uptime) and crawler status at a glance
- **Script Detail** — Service status, actions (run/stop/restart), logs, and crawl stats
- **Notifications** — Per-crawler email and Discord configuration with test buttons
- **Content Filters** — Add, toggle, and delete patterns that strip noisy sections from article diff comparison
- **Data Viewer** — Browse products/articles, view variant price history charts, and inspect snapshot diffs
- **Crawl History** — Paginated run history with status, duration, and change counts
- **Configuration** — Edit per-crawler and global environment variables
- **Deploy** — Install/update systemd units from the admin UI

## Setup

### Prerequisites

- Python 3.9+
- Debian/Ubuntu (for systemd automation)

### Configuration

Copy `.env.example` to `.env` and fill in your values:

```env
BREVO_API_KEY=your-brevo-api-key
EMAIL_FROM="Your Name <you@example.com>"
EMAIL_TO="you@example.com"
SITE_ROOT=https://gearshop.rivian.com
COLLECTION_URL=https://gearshop.rivian.com/collections/all
DB_PATH=/opt/rivian-gearshop-crawler/gearshop.db
PRODUCT_DELAY=0.2
```

Optional settings:

| Variable | Default | Description |
|---|---|---|
| `MAX_SCROLL_SECONDS` | `120` | Max time scrolling the collection page |
| `AVAIL_HTML_MAX` | `200` | Cap on HTML availability fallback checks per run |
| `HEARTBEAT_UTC_HOUR` | `-1` (disabled) | UTC hour to send a daily "no changes" heartbeat |
| `CRAWLER_DEBUG` | `0` | Set to `1` for verbose debug logging |
| `JSON_OUT_PATH` | `/opt/rivian-gearshop-crawler/gearshop.json` | Output path for JSON export |
| `SUPPORT_URL` | `https://rivian.com/support` | Support hub URL to crawl |
| `SUPPORT_DB_PATH` | `/opt/rivian-gearshop-crawler/support.db` | Support article database path |
| `SUPPORT_ARTICLE_DELAY` | `1.0` | Seconds between article fetches |
| `SUPPORT_MAX_ARTICLES` | `500` | Maximum articles to process per run |
| `DISCORD_WEBHOOK_URL` | _(empty)_ | Discord webhook for notifications |
| `ADMIN_SECRET_KEY` | _(auto-generated)_ | Session signing key for admin panel |
| `ADMIN_COOKIE_SECURE` | `1` | Mark the session cookie `Secure`. Set to `0` only when reaching the panel over plain HTTP (e.g. an SSH tunnel to `127.0.0.1:8111`), otherwise the browser withholds the cookie and every request bounces to `/login` |
| `FETCH_MAX_ATTEMPTS` | `4` | Attempts per Shopify fetch before giving up (429/5xx only) |
| `FETCH_BACKOFF_BASE` | `1.0` | Base seconds for exponential backoff between retries |
| `FETCH_RETRY_AFTER_CAP` | `30` | Longest server-supplied `Retry-After` that will be honoured |
| `FETCH_RETRY_BUDGET_SECONDS` | `300` | Total seconds a run may spend sleeping on retries, so a sustained rate-limit cannot push the run past the unit's `TimeoutStartSec` |
| `MARKER_RETENTION_RUNS` | `720` | Crawl-marker runs to keep (~30 days hourly). Floor of 3, which removal detection needs |
| `SQLITE_BUSY_TIMEOUT_MS` | `30000` | Busy timeout applied to every SQLite connection |
| `SQLITE_CACHE_SIZE_KIB` | `16384` | Page cache per connection |
| `SQLITE_WAL_AUTOCHECKPOINT` | `1000` | WAL pages before an automatic checkpoint |

### Quick Install (Debian)

```bash
git clone https://github.com/RivianTrackr/rivian-gearshop-crawler.git
cd rivian-gearshop-crawler
cp .env.example .env   # edit with your values
sudo bash setup.sh
```

This installs dependencies, sets up a Python venv with Playwright, and enables systemd timers for all three crawlers.

### Crawl schedule

Timers fire on fixed wall-clock slots rather than "60 minutes after the last
run finished", so they never drift into each other and launch three headless
Chromium instances at once:

| Crawler | Slot |
|---|---|
| Gear Shop | every hour at `:05` |
| Support Articles | every hour at `:25` |
| Offers | every hour at `:45` |

### Manual Run

```bash
cd /opt/rivian-gearshop-crawler
./venv/bin/python3 crawler.py          # gear shop
./venv/bin/python3 support_crawler.py  # support articles
./venv/bin/python3 offers_crawler.py   # offers
```

### Useful Commands

```bash
# Gear Shop
systemctl status rivian-gearshop-crawler.timer
systemctl start rivian-gearshop-crawler.service
journalctl -u rivian-gearshop-crawler -f

# Support Articles
systemctl status rivian-support-crawler.timer
systemctl start rivian-support-crawler.service
journalctl -u rivian-support-crawler -f

# Offers
systemctl status rivian-offers-crawler.timer
systemctl start rivian-offers-crawler.service
journalctl -u rivian-offers-crawler -f

# Admin Panel
systemctl status gearshop-admin.service
journalctl -u gearshop-admin -f

# List all timers
systemctl list-timers --all
```

## Hardening

Two items below need a change on the server itself and are **not** applied by
`setup.sh`. Both are listed in rough order of how much they matter.

### 1. Move Cloudflare off Flexible SSL

`nginx-riviancrawlr.conf` listens on plain HTTP because Cloudflare is set to
**Flexible** SSL: browser→Cloudflare is encrypted, but Cloudflare→origin is
not. Every admin session cookie and every password POST crosses the public
internet in cleartext on that second hop, so anyone on the path between
Cloudflare and the VPS can read them.

To fix:

1. Cloudflare dashboard → SSL/TLS → **Origin Server** → *Create Certificate*.
   Save the cert and key to `/etc/ssl/riviancrawlr/`.
2. Add a TLS server block to the nginx config:

   ```nginx
   server {
       listen 443 ssl;
       listen [::]:443 ssl;
       server_name riviancrawlr.com www.riviancrawlr.com;

       ssl_certificate     /etc/ssl/riviancrawlr/origin.pem;
       ssl_certificate_key /etc/ssl/riviancrawlr/origin.key;
       ssl_protocols       TLSv1.2 TLSv1.3;

       # ... the same add_header / location blocks as the port-80 server ...
   }
   ```
3. Redirect port 80 to 443, then switch Cloudflare SSL mode to
   **Full (strict)**.
4. Once HTTPS is confirmed end to end, add HSTS:
   `add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;`

Also worth doing: uncomment the `allow`/`deny all` Cloudflare-only block in
the nginx config so the origin IP cannot be hit directly, bypassing
Cloudflare's WAF and rate limiting entirely.

### 2. Run the crawlers as a non-root user

All four units still run `User=root`. The crawlers drive headless Chromium
over untrusted remote pages, which is the single process on the box most
worth isolating. The units carry sandboxing directives (`NoNewPrivileges`,
`PrivateTmp`, `ProtectSystem=full`, `ProtectHome=read-only`, and friends), but
those only limit what root can reach — they are not a substitute for dropping
the privilege.

To move off root:

```bash
sudo useradd --system --home /opt/rivian-gearshop-crawler --shell /usr/sbin/nologin riviancrawlr
sudo chown -R riviancrawlr:riviancrawlr /opt/rivian-gearshop-crawler

# Playwright's Chromium currently lives under /root/.cache/ms-playwright,
# which the new user cannot read. Reinstall it inside the install dir:
sudo -u riviancrawlr PLAYWRIGHT_BROWSERS_PATH=/opt/rivian-gearshop-crawler/ms-playwright \
    /opt/rivian-gearshop-crawler/venv/bin/python3 -m playwright install chromium
```

Then in each crawler unit set `User=riviancrawlr`, add
`Environment=PLAYWRIGHT_BROWSERS_PATH=/opt/rivian-gearshop-crawler/ms-playwright`,
and tighten `ProtectHome=read-only` to `ProtectHome=true` (nothing under
`/root` is needed once the browser moves).

The **admin UI unit is the exception** — it installs unit files into
`/etc/systemd/system` and shells out to `systemctl` for the Deploy page, so it
needs root or a narrowly scoped sudoers rule and a polkit policy. That is why
`gearshop-admin.service` is deliberately left unsandboxed. If you want it off
root too, the shape is: run it as `riviancrawlr`, and grant exactly the
`systemctl start/stop/restart/enable` verbs it needs via
`/etc/sudoers.d/riviancrawlr`.

### Dependencies

The admin UI's pins carry a security floor, noted inline in
`requirements.txt`: `fastapi>=0.141` is what pulls `starlette>=1.3.1`, and
`python-multipart>=0.0.31`. Earlier pins resolved to `starlette` 0.38.x, which
is affected by CVE-2026-48710 and CVE-2026-54282 — `request.url` disagreeing
with the path the router actually matched. That one is not theoretical here:
`AuthMiddleware` grants access on a path prefix, so it now reads the path
straight from the ASGI scope instead.

Re-check before upgrading:

```bash
pip install pip-audit
pip-audit -r requirements.txt
```

Note that Starlette 1.0 removed the `TemplateResponse(name, context)`
signature and the `@app.on_event` hook. Both fail only when a route renders,
not at import, so `tests/test_admin_app.py` exercises every template page.

### Session invalidation caveat

Admin sessions are stateless signed tokens (`itsdangerous`), so **logging out
only deletes the browser's copy** — a token captured beforehand stays valid for
its full 7-day `SESSION_MAX_AGE`. Rotating `ADMIN_SECRET_KEY` in `.env` and
restarting the admin service invalidates every outstanding session at once,
which is the lever to pull if you suspect a token leaked.

## Project Structure

```
├── crawler.py                        # Gear Shop crawler, DB, and notification logic
├── support_crawler.py                # Support Article crawler and diff engine
├── availability.py                   # HTML/JSON-LD availability inference
├── dbtune.py                         # Shared SQLite pragmas, ANALYZE/optimize helpers
├── migrations.py                     # Gear Shop DB schema migrations
├── support_migrations.py             # Support DB schema migrations
├── notify.py                         # Retry queue and error alert dispatch
├── requirements.txt                  # Python dependencies
├── setup.sh                          # One-command Debian deployment script
├── .env                              # Configuration (not committed)
├── rivian-gearshop-crawler.service   # systemd service: gear shop
├── rivian-gearshop-crawler.timer     # systemd timer: gear shop (60 min)
├── rivian-support-crawler.service    # systemd service: support articles
├── rivian-support-crawler.timer      # systemd timer: support articles
├── rivian-offers-crawler.service     # systemd service: offers
├── rivian-offers-crawler.timer       # systemd timer: offers
├── gearshop-admin.service            # systemd service: admin panel
├── nginx-riviancrawlr.conf           # nginx reverse proxy config
├── admin/                            # Admin panel (FastAPI + Pico CSS)
│   ├── app.py                        # FastAPI app, auth middleware
│   ├── auth.py                       # Session & password management
│   ├── config.py                     # App configuration constants
│   ├── db.py                         # Admin DB + crawler DB connections
│   ├── systemd.py                    # systemd control helpers
│   ├── routes/                       # Route handlers
│   │   ├── dashboard.py              # System metrics & crawler overview
│   │   ├── scripts.py                # Script detail, start/stop/restart
│   │   ├── notifications.py          # Email & Discord notification config
│   │   ├── content_filters.py        # Content filter management
│   │   ├── config_editor.py          # Per-script .env editor
│   │   ├── data_viewer.py            # Product/article data browser
│   │   ├── deploy.py                 # systemd unit installer
│   │   └── settings.py               # Password & global config
│   ├── templates/                    # Jinja2 HTML templates
│   └── static/                       # CSS and JS assets
└── tests/                            # pytest test suite
```
