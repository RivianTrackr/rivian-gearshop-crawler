#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import logging
import sqlite3
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

from html import escape as html_escape

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from pathlib import Path

# Availability fallbacks (external helper you already have)
from availability import infer_availability_from_html, get_avail_html_checks, reset_avail_state

# ---------------------- Config & Logging ----------------------

load_dotenv()

logger = logging.getLogger("crawler")
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG if os.getenv("CRAWLER_DEBUG", "0") == "1" else logging.INFO,
)

def log(msg: str):
    logger.debug(msg)

SITE_ROOT       = (os.getenv("SITE_ROOT", "https://gearshop.rivian.com")).rstrip("/")
COLLECTION_URL  = os.getenv("COLLECTION_URL", f"{SITE_ROOT}/collections/all")
DB_PATH         = os.getenv("DB_PATH", "/opt/rivian-gearshop-crawler/gearshop.db")
BREVO_API_KEY   = os.getenv("BREVO_API_KEY", "")
EMAIL_FROM      = os.getenv("EMAIL_FROM", "RivianTrackr Alerts <alerts@example.com>")
EMAIL_TO        = [e.strip() for e in os.getenv("EMAIL_TO", "you@example.com").split(",") if e.strip()]

# Discord
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

# Tuning
MAX_SCROLL_SECONDS = int(os.getenv("MAX_SCROLL_SECONDS", "120"))   # stop scroll after N sec
PRODUCT_DELAY      = float(os.getenv("PRODUCT_DELAY", "0.2"))      # pause between product JSON hits
AVAIL_HTML_MAX     = int(os.getenv("AVAIL_HTML_MAX", "200"))       # 0 = unlimited HTML fallbacks
HEARTBEAT_UTC_HOUR = int(os.getenv("HEARTBEAT_UTC_HOUR", "-1"))    # -1 = disabled
SNAPSHOT_RETENTION = 30  # keep latest N snapshots per variant

HEADERS = {
    "User-Agent": "RivianGearshopCrawler/1.0 (+https://riviantrackr.com)"
}

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def today_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def write_json(rows, out_path="/opt/rivian-gearshop-crawler/gearshop.json"):
    """Write a normalized snapshot for the website table."""
    Path(os.path.dirname(out_path)).mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_utc": int(time.time()),
        "count": len(rows),
        "items": rows,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def export_current_inventory_json(conn, out_path="/opt/rivian-gearshop-crawler/gearshop.json", site_root=None):
    """
    Build a flat list of the most recent snapshot per variant,
    join products and variants, then write JSON for the front end.
    """
    if site_root is None:
        site_root = (os.getenv("SITE_ROOT", "https://gearshop.rivian.com")).rstrip("/")

    sql = """
    WITH latest AS (
      SELECT variant_id, MAX(crawled_at) AS max_crawled
      FROM snapshots
      GROUP BY variant_id
    )
    SELECT
      p.product_id,
      p.handle,
      p.title          AS product_title,
      p.vendor,
      p.product_type,
      COALESCE(p.url, (? || '/products/' || p.handle)) AS product_url,
      v.variant_id,
      v.title          AS variant_title,
      v.sku,
      s.price_cents,
      s.compare_at_cents,
      s.available,
      l.max_crawled    AS last_seen
    FROM latest l
    JOIN snapshots s ON s.variant_id = l.variant_id AND s.crawled_at = l.max_crawled
    JOIN variants  v ON v.variant_id = s.variant_id
    JOIN products  p ON p.product_id = s.product_id
    ORDER BY p.title, v.title
    """
    cur = conn.execute(sql, (site_root,))
    rows_db = cur.fetchall()

    rows = []
    for r in rows_db:
        product_title   = r["product_title"] or ""
        variant_title   = (r["variant_title"] or "").strip()
        nice_title = product_title if variant_title in ("", "Default Title") else f"{product_title} ({variant_title})"

        price_cents     = r["price_cents"]
        price           = None if price_cents is None else round(price_cents / 100, 2)

        availability    = "In stock" if r["available"] == 1 else "Sold out"

        try:
            dt = datetime.fromisoformat(r["last_seen"].replace("Z", "+00:00"))
            last_seen_utc = int(dt.timestamp())
        except Exception:
            last_seen_utc = None

        rows.append({
            "title":        nice_title,
            "sku":          r["sku"] or "",
            "variant_id":   r["variant_id"],
            "price":        price,
            "availability": availability,
            "url":          r["product_url"],
            "image":        None,
            "category":     r["product_type"] or "",
            "last_seen_utc": last_seen_utc,
        })

    write_json(rows, out_path=out_path)
    logger.info("Wrote JSON with %d items to %s", len(rows), out_path)

# ---------------------- SQLite Schema ----------------------

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS products (
  product_id INTEGER PRIMARY KEY,
  handle TEXT NOT NULL,
  title TEXT,
  vendor TEXT,
  product_type TEXT,
  url TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS variants (
  variant_id INTEGER PRIMARY KEY,
  product_id INTEGER NOT NULL,
  title TEXT,
  sku TEXT,
  FOREIGN KEY(product_id) REFERENCES products(product_id)
);

CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
  crawled_at TEXT NOT NULL,
  product_id INTEGER NOT NULL,
  variant_id INTEGER NOT NULL,
  price_cents INTEGER,
  compare_at_cents INTEGER,
  available INTEGER NOT NULL,
  FOREIGN KEY(product_id) REFERENCES products(product_id),
  FOREIGN KEY(variant_id) REFERENCES variants(variant_id)
);

CREATE TABLE IF NOT EXISTS crawl_markers (
  crawled_at TEXT NOT NULL,
  product_id INTEGER NOT NULL,
  PRIMARY KEY (crawled_at, product_id)
);

-- Tracks last product-count per run; used for anomaly guard
CREATE TABLE IF NOT EXISTS crawl_stats (
  run_at TEXT PRIMARY KEY,
  product_count INTEGER NOT NULL
);

-- Daily heartbeat tracking
CREATE TABLE IF NOT EXISTS heartbeats (
  day_utc TEXT PRIMARY KEY  -- format YYYY-MM-DD
);

-- De-duplication of removal notices
CREATE TABLE IF NOT EXISTS removed_once (
  product_id INTEGER PRIMARY KEY,
  first_reported_at TEXT NOT NULL
);
"""

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db() as conn:
        conn.executescript(SCHEMA)

def latest_snapshot_for_variant(conn, variant_id):
    cur = conn.execute(
        "SELECT * FROM snapshots WHERE variant_id=? ORDER BY snapshot_id DESC LIMIT 1",
        (variant_id,)
    )
    return cur.fetchone()

def has_any_snapshot(conn):
    cur = conn.execute("SELECT 1 FROM snapshots LIMIT 1")
    return cur.fetchone() is not None

def last_product_count(conn):
    row = conn.execute(
        "SELECT product_count FROM crawl_stats ORDER BY run_at DESC LIMIT 1"
    ).fetchone()
    return row["product_count"] if row else None

def heartbeat_sent_today(conn):
    cur = conn.execute("SELECT 1 FROM heartbeats WHERE day_utc=?", (today_utc_str(),))
    return cur.fetchone() is not None

def mark_heartbeat_sent(conn):
    conn.execute("INSERT OR IGNORE INTO heartbeats (day_utc) VALUES (?)", (today_utc_str(),))
    conn.commit()

def should_send_heartbeat(conn):
    if HEARTBEAT_UTC_HOUR < 0 or HEARTBEAT_UTC_HOUR > 23:
        return False
    now_utc = datetime.now(timezone.utc)
    if now_utc.hour != HEARTBEAT_UTC_HOUR:
        return False
    return not heartbeat_sent_today(conn)

# ---------------------- Scrape Helpers ----------------------

def infinite_scroll_collect_product_links(page, collection_url):
    log(f"Opening collection: {collection_url}")
    page.goto(collection_url, wait_until="networkidle")
    last_height = 0
    stable_rounds = 0
    start = time.time()

    while True:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            stable_rounds += 1
            if stable_rounds >= 3:
                log("Scrolling stabilized.")
                break
        else:
            stable_rounds = 0
            last_height = new_height
        if time.time() - start > MAX_SCROLL_SECONDS:
            log(f"Stopped scrolling after {MAX_SCROLL_SECONDS}s to avoid hanging.")
            break

    anchors = page.query_selector_all("a[href*='/products/']")
    links = set()
    for a in anchors:
        href = a.get_attribute("href") or ""
        if not href:
            continue
        if href.startswith("/"):
            href = urljoin(SITE_ROOT, href)
        if "/products/" in href:
            href = href.split("#")[0].split("?")[0]
            links.add(href)
    log(f"Collected {len(links)} product URLs.")
    return sorted(links)

def handle_from_product_url(url):
    path = urlparse(url).path
    if "/products/" not in path:
        return None
    return path.split("/products/", 1)[1].strip("/")

def _requests_session_with_retries(retries=3, backoff_factor=0.5, status_forcelist=(500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

_retry_session = _requests_session_with_retries()

def fetch_product_json(handle):
    url = f"{SITE_ROOT}/products/{handle}.json"
    r = _retry_session.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "product" not in data:
        raise ValueError(f"Missing 'product' key in JSON response for {handle}")
    return data["product"]

def cents(value):
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        try:
            return int(round(float(value) * 100))
        except (ValueError, TypeError):
            return None

# ---------------------- Email ----------------------

def render_money(cents_val):
    if cents_val is None: return "—"
    return "${:,.2f}".format(cents_val / 100)

def send_email(subject, html):
    if not BREVO_API_KEY:
        logger.warning("BREVO_API_KEY missing; printing email instead.")
        logger.info("Subject: %s", subject)
        logger.info(html)
        return

    # Extract sender email & name
    m = re.search(r"<([^>]+)>", EMAIL_FROM)
    if m:
        sender_email = m.group(1)
        sender_name = EMAIL_FROM.replace(m.group(0), "").strip()
    else:
        sender_email = EMAIL_FROM
        sender_name = "Alerts"

    payload = {
        "sender": {"email": sender_email, "name": sender_name},
        "to": [{"email": addr} for addr in EMAIL_TO],
        "subject": subject,
        "htmlContent": html
    }
    headers = {"accept": "application/json", "api-key": BREVO_API_KEY, "content-type": "application/json"}
    url = "https://api.brevo.com/v3/smtp/email"
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
    if resp.status_code >= 300:
        logger.error("Brevo send failed: %d %s", resp.status_code, resp.text)

def build_email_fixed(is_initial, diffs, new_products, removed_products, initial_rows=None):
    title = "RivianTrackr: Initial Catalog" if is_initial else "RivianTrackr: Changes Detected"
    parts = [f"<h2>{title}</h2>"]
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    parts.append(f"<p><small>Generated {ts} UTC</small></p>")

    if is_initial:
        parts.append("<p>This is the first run. Full catalog listing below.</p>")
    else:
        if not diffs and not new_products and not removed_products:
            parts.append("<p>No changes detected.</p>")
        else:
            if new_products:
                parts.append("<h3>New products</h3><ul>")
                for p in new_products:
                    parts.append(f"<li><a href='{html_escape(p['url'])}'>{html_escape(p['title'])}</a> — {html_escape(p.get('vendor') or '')} ({html_escape(p['handle'])})</li>")
                parts.append("</ul>")
            if removed_products:
                parts.append("<h3>Removed products</h3><ul>")
                for p in removed_products:
                    parts.append(f"<li>{html_escape(p['title'])} ({html_escape(p['handle'])})</li>")
                parts.append("</ul>")

    parts.append("<h3>Full catalog</h3>" if is_initial else "<h3>Variant changes</h3>")

    if is_initial or diffs:
        parts.append("""
        <table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse;font-family:system-ui,Segoe UI,Arial;font-size:13px;">
          <thead>
            <tr>
              <th>Product</th><th>Variant</th><th>SKU</th><th>Available</th><th>Price</th><th>Compare At</th><th>Change</th>
            </tr>
          </thead>
          <tbody>
        """)

    if not is_initial:
        for row in diffs:
            link = html_escape(row.get("variant_url") or row["url"])
            parts.append(f"""
              <tr>
                <td><a href="{link}">{html_escape(row['product_title'])}</a></td>
                <td>{html_escape(row['variant_title'] or '')}</td>
                <td>{html_escape(row['sku'] or '')}</td>
                <td>{'Yes' if row['new_available'] else 'No'}</td>
                <td>{render_money(row['new_price'])}</td>
                <td>{render_money(row['new_compare_at'])}</td>
                <td>{html_escape(row['change_desc'])}</td>
              </tr>
            """)

    if is_initial and initial_rows:
        parts.extend(initial_rows)

    if is_initial or diffs:
        parts.append("</tbody></table>")

    parts.append("<p style='color:#666'>Note: Some items may not be purchasable online; they will still appear here with availability = No or a disabled purchase state from the product JSON.</p>")
    return "".join(parts)

# ---------------------- Discord ----------------------

def send_discord(subject, diffs=None, new_products=None, removed_products=None, is_heartbeat=False, heartbeat_info=None):
    """Send a notification to Discord via webhook. Formats data as embeds."""
    if not DISCORD_WEBHOOK_URL:
        return

    embeds = []

    if is_heartbeat and heartbeat_info:
        embeds.append({
            "title": subject,
            "description": (
                f"No catalog changes detected.\n"
                f"**Run time:** {heartbeat_info.get('run_time', 'N/A')}\n"
                f"**Products seen:** {heartbeat_info.get('product_count', 0)}\n"
                f"**HTML checks:** {heartbeat_info.get('html_checks', 0)}"
            ),
            "color": 0x3B82F6,  # blue
        })
    else:
        # New products
        if new_products:
            lines = []
            for p in new_products[:10]:
                url = p.get("url", "")
                title = p.get("title", p.get("handle", "Unknown"))
                vendor = p.get("vendor") or ""
                line = f"[{title}]({url})" if url else title
                if vendor:
                    line += f" — {vendor}"
                lines.append(line)
            if len(new_products) > 10:
                lines.append(f"*...and {len(new_products) - 10} more*")
            embeds.append({
                "title": f"New Products ({len(new_products)})",
                "description": "\n".join(lines),
                "color": 0x34C759,  # green
            })

        # Removed products
        if removed_products:
            lines = []
            for p in removed_products[:10]:
                lines.append(f"{p.get('title', p.get('handle', 'Unknown'))}")
            if len(removed_products) > 10:
                lines.append(f"*...and {len(removed_products) - 10} more*")
            embeds.append({
                "title": f"Removed Products ({len(removed_products)})",
                "description": "\n".join(lines),
                "color": 0xFF3B30,  # red
            })

        # Variant changes
        if diffs:
            lines = []
            for row in diffs[:15]:
                product = row.get("product_title", "")
                variant = row.get("variant_title") or ""
                change = row.get("change_desc", "")
                name = f"{product} ({variant})" if variant and variant != "Default Title" else product
                url = row.get("variant_url") or row.get("url", "")
                line = f"[{name}]({url}): {change}" if url else f"{name}: {change}"
                lines.append(line)
            if len(diffs) > 15:
                lines.append(f"*...and {len(diffs) - 15} more changes*")
            embeds.append({
                "title": f"Variant Changes ({len(diffs)})",
                "description": "\n".join(lines),
                "color": 0xFBA919,  # gold
            })

    if not embeds:
        return

    # Discord limits: max 10 embeds, 6000 total chars
    payload = {
        "username": "RivianTrackr",
        "embeds": embeds[:10],
    }

    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=15)
        if resp.status_code >= 300:
            logger.error("Discord webhook failed: %d %s", resp.status_code, resp.text[:200])
        else:
            logger.info("Discord notification sent (%d embeds)", len(embeds))
    except Exception as e:
        logger.error("Discord webhook error: %s", e)


# ---------------------- Availability Helpers (extra) ----------------------

def check_variant_api_available(variant_id, timeout=15):
    """Try Shopify's /variants/<id>.json. Returns True/False/None."""
    try:
        url = f"{SITE_ROOT}/variants/{variant_id}.json"
        log(f"Variant API check: {url}")
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        log(f"    variant api status={r.status_code}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json() or {}
        v = data.get("variant") or {}
        if "available" in v:
            return bool(v["available"])
        return None
    except Exception as e:
        log(f"Variant API error for {variant_id}: {e}")
        return None

def check_product_js_variant_available(handle, variant_id, timeout=15):
    """Try Shopify's /products/<handle>.js (often includes variant.available)."""
    try:
        url = f"{SITE_ROOT}/products/{handle}.js"
        log(f"Product JS check: {url}")
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        data = r.json() or {}
        for v in data.get("variants", []):
            try:
                if int(v.get("id")) == int(variant_id):
                    if "available" in v:
                        return bool(v["available"])
                    break
            except Exception:
                continue
        return None
    except Exception as e:
        log(f"Product JS error for {handle}/{variant_id}: {e}")
        return None

# ---------------------- Removal Confirmation Helper ----------------------

def confirm_product_removed(handle, timeout=15):
    """
    Return True if /products/<handle>.json is 404 (truly removed),
    False if 200 (exists; maybe hidden/sold out), None on other errors.
    """
    url = f"{SITE_ROOT}/products/{handle}.json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code == 404:
            log(f"Removal check: {handle} -> 404 (confirmed removed)")
            return True
        if r.ok:
            log(f"Removal check: {handle} -> 200 (exists; not removed)")
            return False
        log(f"Removal check: {handle} -> {r.status_code} (indeterminate)")
        return None
    except Exception as e:
        log(f"Removal check error for {handle}: {e}")
        return None

# ---------------------- Main Crawl ----------------------

def main():
    init_db()
    reset_avail_state()
    crawled_at = now_utc_iso()

    # --- Collect product links with lazy-load + one retry if suspiciously low
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                ],
                timeout=60000,  # 60s launch timeout
            )
        except Exception as e:
            logger.error("Failed to launch browser: %s", e)
            return

        context = browser.new_context(user_agent=HEADERS.get("User-Agent"))
        page = context.new_page()
        links = infinite_scroll_collect_product_links(page, COLLECTION_URL)

        if len(links) < 50:
            log("Few links collected on first attempt; retrying scroll once...")
            page = context.new_page()
            links = infinite_scroll_collect_product_links(page, COLLECTION_URL)

        browser.close()

    # Normalize to handles → canonical URLs
    handle_to_url = {}
    for link in links:
        h = handle_from_product_url(link)
        if h:
            handle_to_url[h] = f"{SITE_ROOT}/products/{h}"

    log(f"Processing {len(handle_to_url)} products...")

    # Use a single DB connection for the entire run
    conn = db()

    # --- Anomaly guard: if suddenly far fewer products than last good run, skip safely
    prev_count = last_product_count(conn)
    current_count = len(handle_to_url)
    if prev_count and prev_count >= 100 and current_count < max(20, int(prev_count * 0.5)):
        log(f"Anomaly: only {current_count} products vs last {prev_count}. Skipping this run to avoid false removals.")
        conn.execute("INSERT OR REPLACE INTO crawl_stats (run_at, product_count) VALUES (?,?)", (now_utc_iso(), current_count))
        conn.commit()
        export_current_inventory_json(
            conn,
            out_path=os.getenv("JSON_OUT_PATH", "/opt/rivian-gearshop-crawler/gearshop.json"),
            site_root=SITE_ROOT
        )
        conn.close()
        return

    seen_product_ids = set()
    diffs = []
    initial_rows_for_email = []
    new_products_report_block = []
    removed_products_report_block = []

    try:
        conn.execute("BEGIN")
        is_initial = not has_any_snapshot(conn)
        cur = conn.cursor()

        # Crawl products
        for handle, url in handle_to_url.items():
            log(f"Product: {handle} → JSON")
            time.sleep(PRODUCT_DELAY)
            try:
                pj = fetch_product_json(handle)
            except Exception as e:
                logger.warning("Failed JSON for %s: %s", handle, e)
                continue

            product_id = pj.get("id")
            title = pj.get("title")
            vendor = pj.get("vendor")
            product_type = pj.get("product_type")
            created_at = pj.get("created_at")
            updated_at = pj.get("updated_at")

            seen_product_ids.add(product_id)

            # Upsert product
            cur.execute("""
              INSERT INTO products (product_id, handle, title, vendor, product_type, url, created_at, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT(product_id) DO UPDATE SET
                handle=excluded.handle,
                title=excluded.title,
                vendor=excluded.vendor,
                product_type=excluded.product_type,
                url=excluded.url,
                updated_at=excluded.updated_at
            """, (product_id, handle, title, vendor, product_type, handle_to_url[handle], created_at, updated_at))

            # Mark presence this crawl
            cur.execute("INSERT OR IGNORE INTO crawl_markers (crawled_at, product_id) VALUES (?,?)", (crawled_at, product_id))

            # Variants
            for v in pj.get("variants", []):
                log(f"  Variant {v.get('id')} | title={v.get('title')} | sku={v.get('sku')}")
                vid = v.get("id")
                vtitle = v.get("title")
                sku = v.get("sku")
                variant_url = f"{handle_to_url[handle]}?variant={vid}"

                cur.execute("""
                  INSERT INTO variants (variant_id, product_id, title, sku)
                  VALUES (?, ?, ?, ?)
                  ON CONFLICT(variant_id) DO UPDATE SET
                    product_id=excluded.product_id,
                    title=excluded.title,
                    sku=excluded.sku
                """, (vid, product_id, vtitle, sku))

                price = cents(v.get("price"))
                compare_at = cents(v.get("compare_at_price"))

                # Availability: product JSON → Variant API → Product JS → HTML (capped)
                raw_avail = v.get("available")
                available = 1 if raw_avail else 0
                log(f"    avail: json={raw_avail!r} -> {available}")

                if not available:
                    api_avail = check_variant_api_available(vid)
                    log(f"    avail: api={api_avail!r}")
                    if api_avail is True:
                        available = 1
                    elif api_avail is False:
                        available = 0
                    else:
                        js_avail = check_product_js_variant_available(handle, vid)
                        log(f"    avail: js={js_avail!r}")
                        if js_avail is True:
                            available = 1
                        elif js_avail is False:
                            available = 0
                        else:
                            if (AVAIL_HTML_MAX == 0 or get_avail_html_checks() < AVAIL_HTML_MAX):
                                inferred = infer_availability_from_html(handle, vid, SITE_ROOT, HEADERS, log)
                                log(f"    avail: html={inferred!r}")
                                if inferred is True:
                                    available = 1
                                elif inferred is False:
                                    available = 0
                            else:
                                log(f"    avail: html=SKIPPED (cap {get_avail_html_checks()}/{AVAIL_HTML_MAX})")

                prev = latest_snapshot_for_variant(conn, vid)
                cur.execute("""
                  INSERT INTO snapshots (crawled_at, product_id, variant_id, price_cents, compare_at_cents, available)
                  VALUES (?, ?, ?, ?, ?, ?)
                """, (crawled_at, product_id, vid, price, compare_at, available))

                if is_initial:
                    initial_rows_for_email.append(f"""
                      <tr>
                        <td><a href="{html_escape(variant_url)}">{html_escape(title or '')}</a></td>
                        <td>{html_escape(vtitle or '')}</td>
                        <td>{html_escape(sku or '')}</td>
                        <td>{'Yes' if available else 'No'}</td>
                        <td>{render_money(price)}</td>
                        <td>{render_money(compare_at)}</td>
                        <td>Initial</td>
                      </tr>
                    """)
                else:
                    if prev:
                        changes = []
                        if (prev["price_cents"] or 0) != (price or 0):
                            changes.append(f"Price {render_money(prev['price_cents'])} → {render_money(price)}")
                        if (prev["compare_at_cents"] or 0) != (compare_at or 0):
                            changes.append(f"CompareAt {render_money(prev['compare_at_cents'])} → {render_money(compare_at)}")
                        if (prev["available"] or 0) != (available or 0):
                            changes.append(f"Availability {'Yes' if prev['available'] else 'No'} → {'Yes' if available else 'No'}")
                        if changes:
                            diffs.append({
                                "url": handle_to_url[handle],
                                "variant_url": variant_url,
                                "product_title": title,
                                "variant_title": vtitle,
                                "sku": sku,
                                "new_price": price,
                                "new_compare_at": compare_at,
                                "new_available": available,
                                "change_desc": "; ".join(changes)
                            })
                    else:
                        diffs.append({
                            "url": handle_to_url[handle],
                            "variant_url": variant_url,
                            "product_title": title,
                            "variant_title": vtitle,
                            "sku": sku,
                            "new_price": price,
                            "new_compare_at": compare_at,
                            "new_available": available,
                            "change_desc": "New variant"
                        })

        # Clear dedupe memory for any product seen this run (so future removals can be re-reported)
        if seen_product_ids:
            placeholders = ",".join("?" for _ in seen_product_ids)
            conn.execute(
                f"DELETE FROM removed_once WHERE product_id IN ({placeholders})",
                list(seen_product_ids)
            )

        conn.commit()

        # New products: first time seen at/after this crawl time
        cur2 = conn.execute("""
          SELECT p.product_id, p.title, p.handle, p.vendor, p.url
          FROM products p
          WHERE NOT EXISTS (
            SELECT 1 FROM crawl_markers cm_prev
            WHERE cm_prev.product_id = p.product_id
              AND cm_prev.crawled_at < ?
          )
        """, (crawled_at,))
        for row in cur2:
            new_products_report_block.append(dict(row))

        # --- Removed products: require three consecutive misses ---
        rows = conn.execute("""
          SELECT DISTINCT crawled_at
          FROM crawl_markers
          WHERE crawled_at < ?
          ORDER BY crawled_at DESC
          LIMIT 2
        """, (crawled_at,)).fetchall()
        prev_times = [r["crawled_at"] for r in rows]
        removed_candidates = []

        if len(prev_times) == 2:
            prev1, prev2 = prev_times[0], prev_times[1]
            removed_candidates = [
                dict(row) for row in conn.execute("""
                    SELECT p.product_id, p.title, p.handle
                    FROM products p
                    WHERE EXISTS (
                            SELECT 1 FROM crawl_markers cm_seen
                            WHERE cm_seen.product_id = p.product_id
                        )
                      AND NOT EXISTS (
                            SELECT 1 FROM crawl_markers cm_now
                            WHERE cm_now.product_id = p.product_id AND cm_now.crawled_at = ?
                        )
                      AND NOT EXISTS (
                            SELECT 1 FROM crawl_markers cm_prev1
                            WHERE cm_prev1.product_id = p.product_id AND cm_prev1.crawled_at = ?
                        )
                      AND NOT EXISTS (
                            SELECT 1 FROM crawl_markers cm_prev2
                            WHERE cm_prev2.product_id = p.product_id AND cm_prev2.crawled_at = ?
                        )
                """, (crawled_at, prev1, prev2)).fetchall()
            ]

        # De-dup already reported removals
        already = {row["product_id"] for row in conn.execute("SELECT product_id FROM removed_once").fetchall()}
        removed_candidates = [r for r in removed_candidates if r["product_id"] not in already]

        # Confirm each candidate via /products/<handle>.json 404
        confirmed_removed = []
        for r in removed_candidates:
            verdict = confirm_product_removed(r["handle"])
            if verdict is True:
                confirmed_removed.append(r)
            elif verdict is False:
                # product exists (likely hidden/sold out) — do not report as removed
                pass
            else:
                # network/5xx indeterminate — be conservative, skip reporting this run
                log(f"Removal check indeterminate for {r['handle']} — not reporting this run.")

        removed_products_report_block = confirmed_removed

        # --- Prune old snapshots: keep only the latest N per variant ---
        conn.execute(f"""
            DELETE FROM snapshots
            WHERE snapshot_id NOT IN (
                SELECT snapshot_id FROM (
                    SELECT snapshot_id,
                           ROW_NUMBER() OVER (PARTITION BY variant_id ORDER BY snapshot_id DESC) AS rn
                    FROM snapshots
                ) sub
                WHERE sub.rn <= {SNAPSHOT_RETENTION}
            )
        """)
        pruned = conn.execute("SELECT changes()").fetchone()[0]
        if pruned:
            log(f"Pruned {pruned} old snapshot rows.")

        conn.commit()
        cur.close()

    except Exception:
        conn.rollback()
        conn.close()
        raise

    # Summary log
    log(
        f"Diffs: {len(diffs)} | New: {len(new_products_report_block)} | "
        f"Removed: {len(removed_products_report_block)} | "
        f"HTML availability checks: {get_avail_html_checks()}"
    )

    # Email / Heartbeat
    changes_exist = bool(diffs or new_products_report_block or removed_products_report_block)

    if is_initial or changes_exist:
        html = build_email_fixed(
            is_initial=is_initial,
            diffs=diffs,
            new_products=new_products_report_block,
            removed_products=removed_products_report_block,
            initial_rows=initial_rows_for_email
        )
        subject = "RivianTrackr: Initial Catalog" if is_initial else "RivianTrackr: Changes Detected"
        send_email(subject, html)
        send_discord(
            subject,
            diffs=diffs,
            new_products=new_products_report_block,
            removed_products=removed_products_report_block,
        )

        # Remember newly reported removals to avoid re-emailing every run
        if removed_products_report_block:
            conn.executemany(
                "INSERT OR IGNORE INTO removed_once (product_id, first_reported_at) VALUES (?, ?)",
                [(r["product_id"], crawled_at) for r in removed_products_report_block]
            )
            conn.commit()
    else:
        if should_send_heartbeat(conn):
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            html = f"""
                <h2>RivianTrackr: Daily Heartbeat</h2>
                <p>No catalog changes detected in the last checks.</p>
                <ul>
                  <li><b>Run time:</b> {ts}</li>
                  <li><b>Products seen this run:</b> {len(handle_to_url)}</li>
                  <li><b>HTML availability checks:</b> {get_avail_html_checks()}</li>
                </ul>
                <p style='color:#666'>Heartbeat is sent once per day at hour {HEARTBEAT_UTC_HOUR:02d}:00 UTC when there are no changes.</p>
            """
            send_email("RivianTrackr: Daily Heartbeat (No Changes)", html)
            send_discord(
                "RivianTrackr: Daily Heartbeat",
                is_heartbeat=True,
                heartbeat_info={
                    "run_time": ts,
                    "product_count": len(handle_to_url),
                    "html_checks": get_avail_html_checks(),
                },
            )
            mark_heartbeat_sent(conn)
        else:
            log("No changes detected — not sending email (heartbeat either already sent today or outside heartbeat hour).")

    # Record product-count for this successful run (used by anomaly guard)
    conn.execute(
        "INSERT OR REPLACE INTO crawl_stats (run_at, product_count) VALUES (?, ?)",
        (crawled_at, len(handle_to_url))
    )
    conn.commit()

    # Export current inventory to JSON for frontend consumption
    export_current_inventory_json(
        conn,
        out_path=os.getenv("JSON_OUT_PATH", "/opt/rivian-gearshop-crawler/gearshop.json"),
        site_root=SITE_ROOT
    )

    conn.close()

if __name__ == "__main__":
    main()