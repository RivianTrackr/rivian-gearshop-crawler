# RivianCrawlr by RivianTrackr — Gear Shop Crawler

Monitors [gearshop.rivian.com](https://gearshop.rivian.com) for inventory changes and sends email alerts when products are added, removed, or change price/availability.

## How It Works

1. **Crawl** — Uses Playwright (headless Chromium) to infinite-scroll the collection page and discover all products
2. **Fetch** — Pulls `/products/{handle}.json` for each product to get variants, prices, and availability
3. **Availability fallback** — If the JSON says unavailable, tries variant API → product JS → HTML JSON-LD → button text parsing
4. **Store** — Saves snapshots to a SQLite database for historical tracking
5. **Diff** — Compares current crawl to previous snapshots to detect new products, removals, and price/availability changes
6. **Notify** — Sends an HTML email via [Brevo](https://brevo.com) with a detailed change report
7. **Export** — Writes a `gearshop.json` snapshot for frontend consumption

### Safeguards

- **Anomaly guard** — Skips a run if product count drops >50% vs last good run (prevents false alerts from crawl failures)
- **Removal confirmation** — Requires 3 consecutive misses + a 404 check before reporting a product as removed
- **Rate limiting** — Caps HTML fallback checks at 200/run, throttles JSON fetches

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
| `HEARTBEAT_UTC_HOUR` | `-1` (disabled) | UTC hour to send a daily "no changes" heartbeat email |
| `CRAWLER_DEBUG` | `0` | Set to `1` for verbose debug logging |
| `JSON_OUT_PATH` | `/opt/rivian-gearshop-crawler/gearshop.json` | Output path for the JSON export |

### Quick Install (Debian)

```bash
git clone https://github.com/RivianTrackr/rivian-gearshop-crawler.git
cd rivian-gearshop-crawler
cp .env.example .env   # edit with your values
sudo bash setup.sh
```

This installs dependencies, sets up a Python venv with Playwright, and enables a **systemd timer that runs every 60 minutes**.

### Manual Run

```bash
cd /opt/rivian-gearshop-crawler
./venv/bin/python3 crawler.py
```

### Useful Commands

```bash
systemctl status rivian-gearshop-crawler.timer    # check timer
systemctl start rivian-gearshop-crawler.service    # trigger a manual run
journalctl -u rivian-gearshop-crawler -f           # follow logs
systemctl list-timers --all                        # list all timers
```

## Project Structure

```
├── crawler.py                        # Main crawler, DB, and email logic
├── availability.py                   # HTML/JSON-LD availability inference
├── requirements.txt                  # Python dependencies
├── .env                              # Configuration (not committed)
├── setup.sh                          # One-command Debian deployment script
├── rivian-gearshop-crawler.service   # systemd service unit
└── rivian-gearshop-crawler.timer     # systemd timer unit (60 min)
```
