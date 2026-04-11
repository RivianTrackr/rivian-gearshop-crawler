# RivianCrawlr by RivianTrackr

Monitors [gearshop.rivian.com](https://gearshop.rivian.com) and [rivian.com/support](https://rivian.com/support) for changes and sends alerts via email and Discord. Includes a dark-themed admin panel for managing crawlers, notifications, and browsing historical data.

## Features

- **Gear Shop Crawler** — Tracks product inventory, prices, and availability across the Rivian Gear Shop
- **Support Article Crawler** — Tracks Rivian Support articles for content, title, and URL changes
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
- **SQLite busy timeout** — 10-second retry window prevents "database is locked" errors from concurrent access
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

### Quick Install (Debian)

```bash
git clone https://github.com/RivianTrackr/rivian-gearshop-crawler.git
cd rivian-gearshop-crawler
cp .env.example .env   # edit with your values
sudo bash setup.sh
```

This installs dependencies, sets up a Python venv with Playwright, and enables systemd timers for both crawlers.

### Manual Run

```bash
cd /opt/rivian-gearshop-crawler
./venv/bin/python3 crawler.py          # gear shop
./venv/bin/python3 support_crawler.py  # support articles
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

# Admin Panel
systemctl status gearshop-admin.service
journalctl -u gearshop-admin -f

# List all timers
systemctl list-timers --all
```

## Project Structure

```
├── crawler.py                        # Gear Shop crawler, DB, and notification logic
├── support_crawler.py                # Support Article crawler and diff engine
├── availability.py                   # HTML/JSON-LD availability inference
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
