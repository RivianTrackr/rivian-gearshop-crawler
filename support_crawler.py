#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rivian Support Article Crawler — tracks article content changes and sends alerts.

Discovers all support articles at rivian.com/support, extracts full article text,
detects changes (new, removed, title/body/URL changes), and sends email/Discord
notifications with detailed diffs.
"""

import os
import re
import sys
import json
import time
import hashlib
import logging
import resource
import sqlite3
import difflib
from datetime import datetime, timezone
from html import escape as html_escape
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from notify import retry_queue, send_error_alert
from support_migrations import run_migrations

# ---------------------- Config & Logging ----------------------

load_dotenv()

logger = logging.getLogger("support_crawler")
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG if os.getenv("CRAWLER_DEBUG", "0") == "1" else logging.INFO,
)


def log(msg: str):
    logger.debug(msg)


SUPPORT_URL = os.getenv("SUPPORT_URL", "https://rivian.com/support")
SUPPORT_DB_PATH = os.getenv("SUPPORT_DB_PATH", "/opt/rivian-gearshop-crawler/support.db")

# Notification settings — loaded from admin DB if available, else fall back to env vars
ADMIN_DB_PATH = os.getenv("ADMIN_DB_PATH", os.path.join(os.getcwd(), "admin.db"))
SCRIPT_NAME = os.getenv("SUPPORT_SCRIPT_NAME", "rivian-support-crawler")

BREVO_API_KEY = ""
EMAIL_FROM = ""
EMAIL_TO = []
DISCORD_WEBHOOK_URL = ""

DISCORD_CONFIG = {
    "webhook_url": "",
    "thread_id": "",
    "username": "RivianCrawlr",
    "avatar_url": "",
    "embed_color": "#FBA919",
    "notify_new_articles": True,
    "notify_removed_articles": True,
    "notify_article_changes": True,
    "notify_heartbeat": True,
    "mention_role_id": "",
    "mention_user_id": "",
    "mention_on_new": True,
    "mention_on_removed": False,
    "mention_on_changes": False,
}


def _load_notification_settings():
    """Load notification settings from admin DB, falling back to env vars."""
    global BREVO_API_KEY, EMAIL_FROM, EMAIL_TO, DISCORD_WEBHOOK_URL, DISCORD_CONFIG

    if os.path.exists(ADMIN_DB_PATH):
        try:
            conn = sqlite3.connect(f"file:{ADMIN_DB_PATH}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row

            script = conn.execute(
                "SELECT id FROM managed_scripts WHERE name = ?", (SCRIPT_NAME,)
            ).fetchone()

            if script:
                sid = script["id"]
                rows = conn.execute(
                    "SELECT channel, enabled, config FROM script_notifications WHERE script_id = ?",
                    (sid,),
                ).fetchall()
                for row in rows:
                    cfg = json.loads(row["config"])
                    if row["channel"] == "email" and row["enabled"]:
                        BREVO_API_KEY = cfg.get("brevo_api_key", "")
                        EMAIL_FROM = cfg.get("email_from", "")
                        EMAIL_TO = [e.strip() for e in cfg.get("email_to", "").split(",") if e.strip()]
                        logger.info("Loaded email notification settings from admin DB")
                    elif row["channel"] == "discord" and row["enabled"]:
                        DISCORD_WEBHOOK_URL = cfg.get("webhook_url", "")
                        for key in DISCORD_CONFIG:
                            if key in cfg:
                                DISCORD_CONFIG[key] = cfg[key]
                        logger.info("Loaded Discord notification settings from admin DB")
            conn.close()
        except Exception as e:
            logger.warning("Could not load notification settings from admin DB: %s", e)

    if not BREVO_API_KEY:
        BREVO_API_KEY = os.getenv("BREVO_API_KEY", "")
    if not EMAIL_FROM:
        EMAIL_FROM = os.getenv("EMAIL_FROM", "RivianCrawlr Alerts <alerts@example.com>")
    if not EMAIL_TO:
        EMAIL_TO = [e.strip() for e in os.getenv("EMAIL_TO", "you@example.com").split(",") if e.strip()]
    if not DISCORD_WEBHOOK_URL:
        DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")


_load_notification_settings()

# Tuning
ARTICLE_DELAY = float(os.getenv("SUPPORT_ARTICLE_DELAY", "1.0"))
MAX_ARTICLES = int(os.getenv("SUPPORT_MAX_ARTICLES", "500"))
HEARTBEAT_UTC_HOUR = int(os.getenv("HEARTBEAT_UTC_HOUR", "-1"))
SNAPSHOT_RETENTION = 30

HEADERS = {
    "User-Agent": "RivianSupportCrawler/1.0 (+https://riviantrackr.com)"
}

MAX_DIFF_LINES_EMAIL = 100


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------------------- SQLite ----------------------

def db():
    conn = sqlite3.connect(SUPPORT_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        run_migrations(conn)
    finally:
        conn.close()


def last_article_count(conn):
    row = conn.execute(
        "SELECT article_count FROM support_crawl_stats ORDER BY run_at DESC LIMIT 1"
    ).fetchone()
    return row["article_count"] if row else None


def has_any_article(conn):
    cur = conn.execute("SELECT 1 FROM support_articles LIMIT 1")
    return cur.fetchone() is not None


def heartbeat_sent_today(conn):
    cur = conn.execute("SELECT 1 FROM support_heartbeats WHERE day_utc=?", (today_utc_str(),))
    return cur.fetchone() is not None


def mark_heartbeat_sent(conn):
    conn.execute("INSERT OR IGNORE INTO support_heartbeats (day_utc) VALUES (?)", (today_utc_str(),))
    conn.commit()


def should_send_heartbeat(conn):
    if HEARTBEAT_UTC_HOUR < 0 or HEARTBEAT_UTC_HOUR > 23:
        return False
    now_utc = datetime.now(timezone.utc)
    if now_utc.hour != HEARTBEAT_UTC_HOUR:
        return False
    return not heartbeat_sent_today(conn)


# ---------------------- Content Filters ----------------------

_content_filters: list[dict] = []


def load_content_filters(conn):
    """Load enabled content filters from the database."""
    global _content_filters
    try:
        rows = conn.execute(
            "SELECT id, pattern, filter_type FROM content_filters WHERE enabled = 1"
        ).fetchall()
        _content_filters = [dict(r) for r in rows]
        if _content_filters:
            logger.info("Loaded %d content filter(s)", len(_content_filters))
    except Exception as e:
        logger.warning("Could not load content filters: %s", e)
        _content_filters = []


def apply_content_filters(text: str) -> str:
    """Strip sections matching content filters from body text.

    For 'section_strip' filters, removes everything from the line containing
    the pattern through the end of the text.  This handles "Related articles"
    blocks that appear at the bottom of pages.
    """
    for f in _content_filters:
        pattern = f["pattern"]
        if f["filter_type"] == "section_strip":
            lines = text.splitlines()
            cut_index = None
            for i, line in enumerate(lines):
                if pattern.lower() in line.lower().strip():
                    cut_index = i
                    break
            if cut_index is not None:
                text = "\n".join(lines[:cut_index]).rstrip()
    return text


# ---------------------- Content Helpers ----------------------

def normalize_text(text: str) -> str:
    """Normalize whitespace for stable comparison."""
    return re.sub(r'\s+', ' ', text.strip())


def compute_content_hash(text: str) -> str:
    """SHA-256 of normalized text, after stripping filtered sections."""
    return hashlib.sha256(normalize_text(apply_content_filters(text)).encode("utf-8")).hexdigest()


def slug_from_url(url: str) -> str:
    """Extract article slug from URL like /support/article/my-article."""
    if "/support/article/" in url:
        return url.split("/support/article/", 1)[1].strip("/").split("?")[0].split("#")[0]
    return url.rstrip("/").rsplit("/", 1)[-1]


def category_from_referrer(referrer_url: str) -> str:
    """Extract category from the category page URL that linked to this article."""
    if not referrer_url:
        return ""
    path = referrer_url.rstrip("/")
    if "/support/" in path:
        return path.rsplit("/support/", 1)[-1].split("?")[0].split("#")[0]
    return ""


# ---------------------- Playwright Discovery & Extraction ----------------------

def discover_article_urls(page) -> list[dict]:
    """
    Visit /support, discover category links, then visit each category
    to collect all article URLs. Returns deduplicated list of article info dicts.
    """
    log(f"Discovering articles from {SUPPORT_URL}")

    # Retry the initial page load — this is critical and can fail on cold starts
    last_err = None
    for attempt in range(3):
        try:
            page.goto(SUPPORT_URL, wait_until="commit", timeout=90000)
            last_err = None
            break
        except Exception as e:
            last_err = e
            logger.warning("Attempt %d to load %s failed: %s", attempt + 1, SUPPORT_URL, e)
            if attempt < 2:
                page.wait_for_timeout(3000)

    if last_err:
        raise last_err

    # Wait for JS to render — the support page is likely an SPA
    try:
        page.wait_for_selector('a[href*="/support/"]', timeout=30000)
    except Exception:
        logger.warning("No support links appeared after 30s — page may not have rendered")

    # Scroll to trigger any lazy-loaded content
    for _ in range(3):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1500)

    # Collect all internal links from the support landing page
    all_links = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => e.href)"
    )

    # Find category pages: /support/<category> (but not /support/article/*)
    category_urls = set()
    for href in all_links:
        if "/support/" in href and "/support/article/" not in href:
            # Normalize
            clean = href.split("?")[0].split("#")[0].rstrip("/")
            if clean != SUPPORT_URL.rstrip("/") and "/support/" in clean:
                category_urls.add(clean)

    log(f"Found {len(category_urls)} category pages")

    # Collect article URLs from each category page + the main support page
    articles = {}  # slug -> {url, slug, category}

    def _collect_articles_from_page(current_page, category=""):
        """Extract article links from the current page."""
        links = current_page.eval_on_selector_all(
            'a[href*="/support/article/"]',
            "els => els.map(e => e.href)"
        )
        for href in links:
            clean = href.split("?")[0].split("#")[0].rstrip("/")
            slug = slug_from_url(clean)
            if slug and slug not in articles:
                articles[slug] = {
                    "url": clean,
                    "slug": slug,
                    "category": category,
                }

    # Collect from main support page
    _collect_articles_from_page(page, "")

    # Visit each category page
    for cat_url in sorted(category_urls):
        cat_name = category_from_referrer(cat_url)
        log(f"  Category: {cat_name} ({cat_url})")
        try:
            page.goto(cat_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(1000)
            _collect_articles_from_page(page, cat_name)
        except Exception as e:
            logger.warning("Failed to load category %s: %s", cat_url, e)

    result = list(articles.values())[:MAX_ARTICLES]
    log(f"Discovered {len(result)} unique articles")

    if not result:
        logger.warning(
            "Zero articles discovered — page may not have rendered. "
            "Found %d raw links, %d category URLs.",
            len(all_links), len(category_urls),
        )

    return result


def extract_article_content(page, url: str) -> dict | None:
    """
    Navigate to an article page and extract title + body text.
    Returns dict with title, body_text, or None on failure.
    """
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(1000)

        # Wait for heading to render — try h1, fall back to h2
        title = ""
        try:
            page.wait_for_selector("h1", timeout=10000)
            title_el = page.query_selector("h1")
            title = title_el.inner_text().strip() if title_el else ""
        except Exception:
            # Some pages (e.g. recall-information) may not have an h1
            pass

        # Fall back to h2 if no h1 found
        if not title:
            title_el = page.query_selector("h2")
            title = title_el.inner_text().strip() if title_el else ""

        # Last resort: use the page <title> tag
        if not title:
            title = page.title().strip()
            # Strip common suffixes like " | Rivian"
            if " | " in title:
                title = title.rsplit(" | ", 1)[0].strip()

        # Extract article body - try common content selectors
        body_text = ""
        for selector in ["article", "main", '[role="main"]', ".article-content", ".support-article"]:
            el = page.query_selector(selector)
            if el:
                body_text = el.inner_text().strip()
                if len(body_text) > 50:  # meaningful content threshold
                    break

        # Fallback: get all text below the h1
        if len(body_text) < 50:
            body_text = page.evaluate("""() => {
                const h1 = document.querySelector('h1');
                if (!h1) return document.body.innerText;
                let text = [];
                let el = h1.parentElement;
                while (el && el !== document.body) {
                    el = el.parentElement;
                }
                // Get everything in the main content area
                const main = document.querySelector('main') || document.body;
                return main.innerText;
            }""")

        if not title:
            logger.warning("No title found for %s", url)
            return None

        return {
            "title": title,
            "body_text": body_text or "",
        }
    except Exception as e:
        logger.warning("Failed to extract content from %s: %s", url, e)
        return None


# ---------------------- Diff Generation ----------------------

def generate_text_diff(old_text: str, new_text: str) -> str:
    """Generate a unified diff between old and new text."""
    old_lines = old_text.splitlines()
    new_lines = new_text.splitlines()
    diff = list(difflib.unified_diff(old_lines, new_lines, lineterm="", n=2))
    return "\n".join(diff)


def generate_html_diff(old_text: str, new_text: str) -> str:
    """Generate HTML-formatted diff for email notifications."""
    old_lines = old_text.splitlines()
    new_lines = new_text.splitlines()
    diff = list(difflib.unified_diff(old_lines, new_lines, lineterm="", n=2))

    if not diff:
        return "<em>No visible text differences</em>"

    html_parts = []
    line_count = 0
    for line in diff:
        if line.startswith("---") or line.startswith("+++"):
            continue
        if line_count >= MAX_DIFF_LINES_EMAIL:
            remaining = len(diff) - line_count
            html_parts.append(
                f'<div style="color:#6b7280;padding:2px 6px;font-style:italic;">'
                f'... and {remaining} more lines changed</div>'
            )
            break
        if line.startswith("+"):
            html_parts.append(
                f'<div style="background:#d4edda;padding:2px 6px;font-family:monospace;font-size:12px;">'
                f'+ {html_escape(line[1:])}</div>'
            )
        elif line.startswith("-"):
            html_parts.append(
                f'<div style="background:#f8d7da;padding:2px 6px;font-family:monospace;font-size:12px;">'
                f'- {html_escape(line[1:])}</div>'
            )
        elif line.startswith("@@"):
            html_parts.append(
                f'<div style="color:#6b7280;padding:4px 6px 2px;font-family:monospace;font-size:11px;">'
                f'{html_escape(line)}</div>'
            )
        line_count += 1

    return "".join(html_parts) or "<em>No visible text differences</em>"


# ---------------------- Email ----------------------

def send_email(subject, html):
    if not BREVO_API_KEY:
        logger.warning("BREVO_API_KEY missing; printing email instead.")
        logger.info("Subject: %s", subject)
        logger.info(html)
        return

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
        "htmlContent": html,
    }
    headers = {"accept": "application/json", "api-key": BREVO_API_KEY, "content-type": "application/json"}
    try:
        resp = requests.post(
            "https://api.brevo.com/v3/smtp/email",
            headers=headers,
            data=json.dumps(payload),
            timeout=30,
        )
        if resp.status_code >= 300:
            logger.error("Brevo send failed: %d %s", resp.status_code, resp.text)
            retry_queue.enqueue("email", send_email, args=(subject, html))
    except Exception as e:
        logger.error("Brevo send exception: %s", e)
        retry_queue.enqueue("email", send_email, args=(subject, html))


def build_changes_email(changes: dict, is_initial: bool = False, article_count: int = 0) -> str:
    """Build HTML email body for detected changes."""
    title = "RivianCrawlr Support: Initial Scan" if is_initial else "RivianCrawlr Support: Changes Detected"
    parts = [
        '<div style="font-family:system-ui,-apple-system,sans-serif;max-width:700px;">',
        f"<h2>{title}</h2>",
    ]
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    parts.append(f"<p><small>Generated {ts} UTC</small></p>")

    if is_initial:
        parts.append(f"<p>Initial scan complete. Found {article_count} support articles.</p>")
        if changes.get("new"):
            parts.append("<h3>Articles Found</h3><ul>")
            for a in changes["new"]:
                url = html_escape(a["url"])
                atitle = html_escape(a["title"])
                cat = html_escape(a.get("category", ""))
                cat_label = f" &mdash; <em>{cat}</em>" if cat else ""
                parts.append(f'<li><a href="{url}">{atitle}</a>{cat_label}</li>')
            parts.append("</ul>")
        parts.append("</div>")
        return "".join(parts)

    # New articles
    if changes.get("new"):
        parts.append(f'<h3 style="color:#34c759;">New Articles ({len(changes["new"])})</h3><ul>')
        for a in changes["new"]:
            url = html_escape(a["url"])
            atitle = html_escape(a["title"])
            cat = html_escape(a.get("category", ""))
            cat_label = f" &mdash; <em>{cat}</em>" if cat else ""
            parts.append(f'<li><a href="{url}">{atitle}</a>{cat_label}</li>')
        parts.append("</ul>")

    # Removed articles
    if changes.get("removed"):
        parts.append(f'<h3 style="color:#ff3b30;">Removed Articles ({len(changes["removed"])})</h3><ul>')
        for a in changes["removed"]:
            atitle = html_escape(a["title"])
            slug = html_escape(a["slug"])
            parts.append(f"<li>{atitle} (<code>{slug}</code>)</li>")
        parts.append("</ul>")

    # Title changes
    if changes.get("title_changed"):
        parts.append(f'<h3>Title Changes ({len(changes["title_changed"])})</h3>')
        parts.append(
            '<table border="1" cellspacing="0" cellpadding="6" '
            'style="border-collapse:collapse;font-family:system-ui;font-size:13px;">'
            "<thead><tr><th>Article</th><th>Old Title</th><th>New Title</th></tr></thead><tbody>"
        )
        for c in changes["title_changed"]:
            url = html_escape(c["url"])
            slug = html_escape(c["slug"])
            old_t = html_escape(c["old_title"])
            new_t = html_escape(c["new_title"])
            parts.append(
                f'<tr><td><a href="{url}">{slug}</a></td>'
                f"<td>{old_t}</td><td>{new_t}</td></tr>"
            )
        parts.append("</tbody></table>")

    # URL changes
    if changes.get("url_changed"):
        parts.append(f'<h3>URL Changes ({len(changes["url_changed"])})</h3>')
        parts.append(
            '<table border="1" cellspacing="0" cellpadding="6" '
            'style="border-collapse:collapse;font-family:system-ui;font-size:13px;">'
            "<thead><tr><th>Article</th><th>Old URL</th><th>New URL</th></tr></thead><tbody>"
        )
        for c in changes["url_changed"]:
            atitle = html_escape(c["title"])
            old_u = html_escape(c["old_url"])
            new_u = html_escape(c["new_url"])
            parts.append(
                f"<tr><td>{atitle}</td>"
                f'<td><a href="{old_u}">{old_u}</a></td>'
                f'<td><a href="{new_u}">{new_u}</a></td></tr>'
            )
        parts.append("</tbody></table>")

    # Body changes with diffs
    if changes.get("body_changed"):
        parts.append(f'<h3>Content Changes ({len(changes["body_changed"])})</h3>')
        for c in changes["body_changed"]:
            url = html_escape(c["url"])
            atitle = html_escape(c["title"])
            diff_html = c["diff_html"]
            parts.append(f'<h4><a href="{url}">{atitle}</a></h4>')
            parts.append(
                f'<div style="border:1px solid #ddd;padding:8px;border-radius:4px;'
                f'max-height:400px;overflow-y:auto;margin-bottom:16px;">{diff_html}</div>'
            )

    parts.append(
        '<p style="color:#666;font-size:12px;">This is an automated notification from '
        "RivianCrawlr Support Article Monitor.</p>"
    )
    parts.append("</div>")
    return "".join(parts)


# ---------------------- Discord ----------------------

def _discord_hex_color(hex_str: str) -> int:
    try:
        return int(hex_str.strip().lstrip("#"), 16)
    except (ValueError, TypeError):
        return 0xFBA919


def _discord_mention(event: str) -> str:
    cfg = DISCORD_CONFIG
    if not cfg.get(f"mention_on_{event}", False):
        return ""
    parts = []
    if cfg.get("mention_role_id"):
        parts.append(f"<@&{cfg['mention_role_id']}>")
    if cfg.get("mention_user_id"):
        parts.append(f"<@{cfg['mention_user_id']}>")
    return " ".join(parts)


def send_discord(subject, changes=None, is_heartbeat=False, heartbeat_info=None):
    """Send notification to Discord via webhook."""
    if not DISCORD_WEBHOOK_URL:
        return

    cfg = DISCORD_CONFIG
    accent_color = _discord_hex_color(cfg.get("embed_color", "#FBA919"))
    embeds = []
    mentions = []
    now_iso = datetime.now(timezone.utc).isoformat()
    embed_footer = {"text": "RivianCrawlr by RivianTrackr"}

    if is_heartbeat and heartbeat_info:
        if not cfg.get("notify_heartbeat", True):
            return
        embeds.append({
            "title": subject,
            "description": "No support article changes detected.",
            "fields": [
                {"name": "Run Time", "value": str(heartbeat_info.get("run_time", "N/A")), "inline": True},
                {"name": "Articles Seen", "value": str(heartbeat_info.get("article_count", 0)), "inline": True},
            ],
            "color": 0x3B82F6,
            "footer": embed_footer,
            "timestamp": now_iso,
        })
    elif changes:
        # New articles
        if changes.get("new") and cfg.get("notify_new_articles", True):
            lines = []
            for a in changes["new"][:10]:
                url = a.get("url", "")
                atitle = a.get("title", a.get("slug", "Unknown"))
                line = f"\u2022 [{atitle}]({url})" if url else f"\u2022 {atitle}"
                lines.append(line)
            if len(changes["new"]) > 10:
                lines.append(f"*...and {len(changes['new']) - 10} more*")
            embeds.append({
                "title": f"New Support Articles ({len(changes['new'])})",
                "description": "\n".join(lines),
                "color": 0x34C759,
                "footer": embed_footer,
                "timestamp": now_iso,
            })
            mention = _discord_mention("new")
            if mention:
                mentions.append(mention)

        # Removed articles
        if changes.get("removed") and cfg.get("notify_removed_articles", True):
            lines = []
            for a in changes["removed"][:10]:
                lines.append(f"\u2022 {a.get('title', a.get('slug', 'Unknown'))}")
            if len(changes["removed"]) > 10:
                lines.append(f"*...and {len(changes['removed']) - 10} more*")
            embeds.append({
                "title": f"Removed Support Articles ({len(changes['removed'])})",
                "description": "\n".join(lines),
                "color": 0xFF3B30,
                "footer": embed_footer,
                "timestamp": now_iso,
            })
            mention = _discord_mention("removed")
            if mention:
                mentions.append(mention)

        # Content/title/URL changes
        change_items = (
            changes.get("body_changed", [])
            + changes.get("title_changed", [])
            + changes.get("url_changed", [])
        )
        if change_items and cfg.get("notify_article_changes", True):
            lines = []
            for c in change_items[:15]:
                atitle = c.get("title", c.get("slug", ""))
                url = c.get("url", "")
                change_type = c.get("change_type", "updated")
                line = f"\u2022 [{atitle}]({url}): **{change_type}**" if url else f"\u2022 {atitle}: **{change_type}**"
                lines.append(line)
            if len(change_items) > 15:
                lines.append(f"*...and {len(change_items) - 15} more changes*")
            embeds.append({
                "title": f"Article Changes ({len(change_items)})",
                "description": "\n".join(lines),
                "color": accent_color,
                "footer": embed_footer,
                "timestamp": now_iso,
            })
            mention = _discord_mention("changes")
            if mention:
                mentions.append(mention)

    if not embeds:
        return

    payload = {
        "username": cfg.get("username") or "RivianCrawlr",
        "embeds": embeds[:10],
    }
    if cfg.get("avatar_url"):
        payload["avatar_url"] = cfg["avatar_url"]
    if mentions:
        payload["content"] = " ".join(dict.fromkeys(mentions))

    url = DISCORD_WEBHOOK_URL
    if cfg.get("thread_id"):
        url += f"?thread_id={cfg['thread_id']}"

    try:
        resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code >= 300:
            logger.error("Discord webhook failed: %d %s", resp.status_code, resp.text[:200])
            retry_queue.enqueue(
                "discord", send_discord,
                args=(subject,),
                kwargs=dict(changes=changes, is_heartbeat=is_heartbeat, heartbeat_info=heartbeat_info),
            )
        else:
            logger.info("Discord notification sent (%d embeds)", len(embeds))
    except Exception as e:
        logger.error("Discord webhook error: %s", e)
        retry_queue.enqueue(
            "discord", send_discord,
            args=(subject,),
            kwargs=dict(changes=changes, is_heartbeat=is_heartbeat, heartbeat_info=heartbeat_info),
        )


# ---------------------- Removal Confirmation ----------------------

def confirm_article_removed(url: str, timeout=15) -> bool | None:
    """
    Return True if the article URL returns 404 (confirmed removed),
    False if 200 (still exists), None on other errors.
    """
    try:
        r = requests.head(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code == 404:
            log(f"Removal check: {url} -> 404 (confirmed removed)")
            return True
        if r.status_code < 400:
            log(f"Removal check: {url} -> {r.status_code} (exists)")
            return False
        log(f"Removal check: {url} -> {r.status_code} (indeterminate)")
        return None
    except Exception as e:
        log(f"Removal check error for {url}: {e}")
        return None


# ---------------------- Main Crawl ----------------------

def _error_alert_ctx():
    """Build context kwargs for send_error_alert from current config."""
    return dict(
        discord_webhook_url=DISCORD_WEBHOOK_URL,
        discord_config=DISCORD_CONFIG,
        brevo_api_key=BREVO_API_KEY,
        email_from=EMAIL_FROM,
        email_to=EMAIL_TO,
    )


MAX_RUN_SECONDS = int(os.getenv("SUPPORT_MAX_RUN_SECONDS", "1500"))  # 25 min default


def _check_timeout(run_start: float, phase: str):
    """Raise if we've exceeded the max run time."""
    elapsed = time.time() - run_start
    if elapsed > MAX_RUN_SECONDS:
        raise TimeoutError(
            f"Crawl exceeded {MAX_RUN_SECONDS}s limit during {phase} "
            f"(elapsed: {elapsed:.0f}s)"
        )


def main():
    init_db()
    conn = db()
    try:
        load_content_filters(conn)
    finally:
        conn.close()
    crawled_at = now_utc_iso()
    run_start = time.time()

    # --- Discover articles with Playwright ---
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                ],
                timeout=60000,
            )
        except Exception as e:
            logger.error("Failed to launch browser: %s", e)
            send_error_alert(
                "Browser Launch Failed",
                "Playwright/Chromium failed to start. The support crawl was aborted.",
                details=str(e),
                **_error_alert_ctx(),
            )
            _record_crawl_run(crawled_at, run_start, status="error", error_message=str(e))
            return

        context = browser.new_context(
            user_agent=HEADERS.get("User-Agent"),
            viewport={"width": 1600, "height": 1200},
        )
        page = context.new_page()

        # Phase 1: Discover all article URLs
        article_infos = discover_article_urls(page)

        if not article_infos:
            logger.error("No articles discovered — aborting")
            send_error_alert(
                "No Articles Found",
                "The support crawler found zero articles. This likely indicates a site change or crawl failure.",
                **_error_alert_ctx(),
            )
            browser.close()
            _record_crawl_run(crawled_at, run_start, status="error",
                              error_message="No articles discovered")
            return

        # Anomaly guard
        conn = db()
        prev_count = last_article_count(conn)
        current_count = len(article_infos)

        if prev_count and prev_count >= 20 and current_count < max(5, int(prev_count * 0.5)):
            msg = (
                f"Anomaly: only {current_count} articles vs last {prev_count}. "
                "Skipping this run to avoid false removals."
            )
            logger.warning(msg)
            send_error_alert(
                "Anomaly Guard Triggered",
                msg,
                details=f"Previous count: {prev_count}\nCurrent count: {current_count}",
                **_error_alert_ctx(),
            )
            conn.execute(
                "INSERT OR REPLACE INTO support_crawl_stats (run_at, article_count) VALUES (?,?)",
                (crawled_at, current_count),
            )
            conn.commit()
            _record_crawl_run(crawled_at, run_start, status="skipped",
                              article_count=current_count, error_message=msg)
            conn.close()
            browser.close()
            return

        # Phase 2: Visit each article and extract content
        is_initial = not has_any_article(conn)
        articles_data = []

        for i, info in enumerate(article_infos):
            _check_timeout(run_start, f"article extraction ({i+1}/{len(article_infos)})")
            log(f"[{i+1}/{len(article_infos)}] Extracting: {info['slug']}")
            time.sleep(ARTICLE_DELAY)

            content = extract_article_content(page, info["url"])
            if content:
                articles_data.append({
                    **info,
                    "title": content["title"],
                    "body_text": content["body_text"],
                    "body_hash": compute_content_hash(content["body_text"]),
                })
            else:
                logger.warning("Skipping article %s — extraction failed", info["slug"])

        browser.close()

    log(f"Extracted content from {len(articles_data)} articles")

    # Phase 3: Compare against database and detect changes
    changes = {
        "new": [],
        "removed": [],
        "title_changed": [],
        "body_changed": [],
        "url_changed": [],
    }

    seen_article_ids = set()

    try:
        conn.execute("BEGIN")
        cur = conn.cursor()

        for article in articles_data:
            slug = article["slug"]
            existing = conn.execute(
                "SELECT * FROM support_articles WHERE slug = ?", (slug,)
            ).fetchone()

            if existing:
                article_id = existing["id"]
                seen_article_ids.add(article_id)

                # Check for changes
                old_title = existing["title"]
                old_hash = existing["body_hash"]
                old_url = existing["url"]

                title_changed = old_title != article["title"]
                body_changed = old_hash != article["body_hash"]
                url_changed = old_url != article["url"]

                if title_changed and not is_initial:
                    changes["title_changed"].append({
                        "slug": slug,
                        "url": article["url"],
                        "old_title": old_title,
                        "new_title": article["title"],
                        "title": article["title"],
                        "change_type": "title changed",
                    })

                if body_changed and not is_initial:
                    # Get the previous body text from the latest snapshot
                    prev_snap = conn.execute(
                        "SELECT body_text FROM article_snapshots WHERE article_id = ? ORDER BY id DESC LIMIT 1",
                        (article_id,),
                    ).fetchone()
                    old_body = prev_snap["body_text"] if prev_snap else existing["body_text"]

                    # Apply content filters so diffs exclude noisy sections
                    filtered_old = apply_content_filters(old_body)
                    filtered_new = apply_content_filters(article["body_text"])

                    changes["body_changed"].append({
                        "slug": slug,
                        "url": article["url"],
                        "title": article["title"],
                        "old_body": filtered_old,
                        "new_body": filtered_new,
                        "diff_html": generate_html_diff(filtered_old, filtered_new),
                        "change_type": "content updated",
                    })

                if url_changed and not is_initial:
                    changes["url_changed"].append({
                        "slug": slug,
                        "title": article["title"],
                        "old_url": old_url,
                        "new_url": article["url"],
                        "url": article["url"],
                        "change_type": "URL changed",
                    })

                # Update the article record
                if title_changed or body_changed or url_changed:
                    cur.execute(
                        """UPDATE support_articles
                           SET title=?, body_text=?, body_hash=?, url=?, category=?,
                               last_seen_at=?, updated_at=?, removed=0
                           WHERE id=?""",
                        (article["title"], article["body_text"], article["body_hash"],
                         article["url"], article.get("category", ""),
                         crawled_at, crawled_at, article_id),
                    )
                else:
                    cur.execute(
                        "UPDATE support_articles SET last_seen_at=?, removed=0 WHERE id=?",
                        (crawled_at, article_id),
                    )

                # Always save a snapshot when content changed
                if title_changed or body_changed or url_changed or is_initial:
                    cur.execute(
                        """INSERT INTO article_snapshots
                           (article_id, crawled_at, title, body_text, body_hash, url)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (article_id, crawled_at, article["title"],
                         article["body_text"], article["body_hash"], article["url"]),
                    )

            else:
                # New article
                cur.execute(
                    """INSERT INTO support_articles
                       (slug, url, title, body_text, body_hash, category,
                        first_seen_at, last_seen_at, updated_at, removed)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                    (slug, article["url"], article["title"],
                     article["body_text"], article["body_hash"],
                     article.get("category", ""),
                     crawled_at, crawled_at, crawled_at),
                )
                article_id = cur.lastrowid
                seen_article_ids.add(article_id)

                # Save initial snapshot
                cur.execute(
                    """INSERT INTO article_snapshots
                       (article_id, crawled_at, title, body_text, body_hash, url)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (article_id, crawled_at, article["title"],
                     article["body_text"], article["body_hash"], article["url"]),
                )

                changes["new"].append({
                    "slug": slug,
                    "url": article["url"],
                    "title": article["title"],
                    "category": article.get("category", ""),
                })

            # Mark presence this crawl
            cur.execute(
                "INSERT OR IGNORE INTO support_crawl_markers (crawled_at, article_id) VALUES (?,?)",
                (crawled_at, article_id),
            )

        # Clear dedupe memory for articles seen this run
        if seen_article_ids:
            placeholders = ",".join("?" for _ in seen_article_ids)
            conn.execute(
                f"DELETE FROM support_removed_once WHERE article_id IN ({placeholders})",
                list(seen_article_ids),
            )

        conn.commit()

        # --- Removed articles: require three consecutive misses ---
        rows = conn.execute("""
            SELECT DISTINCT crawled_at
            FROM support_crawl_markers
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
                    SELECT a.id, a.title, a.slug, a.url
                    FROM support_articles a
                    WHERE a.removed = 0
                      AND EXISTS (
                            SELECT 1 FROM support_crawl_markers cm
                            WHERE cm.article_id = a.id
                        )
                      AND NOT EXISTS (
                            SELECT 1 FROM support_crawl_markers cm
                            WHERE cm.article_id = a.id AND cm.crawled_at = ?
                        )
                      AND NOT EXISTS (
                            SELECT 1 FROM support_crawl_markers cm
                            WHERE cm.article_id = a.id AND cm.crawled_at = ?
                        )
                      AND NOT EXISTS (
                            SELECT 1 FROM support_crawl_markers cm
                            WHERE cm.article_id = a.id AND cm.crawled_at = ?
                        )
                """, (crawled_at, prev1, prev2)).fetchall()
            ]

        # De-dup already reported removals
        already = {
            row["article_id"]
            for row in conn.execute("SELECT article_id FROM support_removed_once").fetchall()
        }
        removed_candidates = [r for r in removed_candidates if r["id"] not in already]

        # Confirm each candidate via HTTP 404
        confirmed_removed = []
        for r in removed_candidates:
            verdict = confirm_article_removed(r["url"])
            if verdict is True:
                confirmed_removed.append(r)
                conn.execute("UPDATE support_articles SET removed=1 WHERE id=?", (r["id"],))
            elif verdict is False:
                log(f"Removal check: {r['slug']} still exists — not reporting")
            else:
                log(f"Removal check indeterminate for {r['slug']} — skipping")

        changes["removed"] = confirmed_removed

        # Prune old snapshots: keep only the latest N per article
        conn.execute(f"""
            DELETE FROM article_snapshots
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY id DESC) AS rn
                    FROM article_snapshots
                ) sub
                WHERE sub.rn <= {SNAPSHOT_RETENTION}
            )
        """)
        pruned = conn.execute("SELECT changes()").fetchone()[0]
        if pruned:
            log(f"Pruned {pruned} old snapshot rows.")

        conn.commit()
        cur.close()

    except Exception as exc:
        conn.rollback()
        conn.close()
        send_error_alert(
            "Support Crawl Failed",
            "An unhandled error occurred during the support article crawl.",
            details=f"{type(exc).__name__}: {exc}",
            **_error_alert_ctx(),
        )
        _record_crawl_run(crawled_at, run_start, status="error", error_message=str(exc))
        raise

    # Summary
    n_new = len(changes["new"])
    n_removed = len(changes["removed"])
    n_title = len(changes["title_changed"])
    n_body = len(changes["body_changed"])
    n_url = len(changes["url_changed"])
    log(
        f"New: {n_new} | Removed: {n_removed} | "
        f"Title changes: {n_title} | Body changes: {n_body} | URL changes: {n_url}"
    )

    changes_exist = bool(n_new or n_removed or n_title or n_body or n_url)

    if is_initial or changes_exist:
        html = build_changes_email(changes, is_initial=is_initial, article_count=len(articles_data))
        subject = "RivianCrawlr Support: Initial Scan" if is_initial else "RivianCrawlr Support: Changes Detected"
        send_email(subject, html)
        send_discord(subject, changes=changes)

        # Remember reported removals
        if confirmed_removed:
            conn.executemany(
                "INSERT OR IGNORE INTO support_removed_once (article_id, first_reported_at) VALUES (?, ?)",
                [(r["id"], crawled_at) for r in confirmed_removed],
            )
            conn.commit()
    else:
        if should_send_heartbeat(conn):
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            html = f"""
                <h2>RivianCrawlr Support: Daily Heartbeat</h2>
                <p>No support article changes detected.</p>
                <ul>
                  <li><b>Run time:</b> {ts}</li>
                  <li><b>Articles seen this run:</b> {len(articles_data)}</li>
                </ul>
                <p style='color:#666'>Heartbeat sent once per day at hour
                {HEARTBEAT_UTC_HOUR:02d}:00 UTC when there are no changes.</p>
            """
            send_email("RivianCrawlr Support: Daily Heartbeat (No Changes)", html)
            send_discord(
                "RivianCrawlr Support: Daily Heartbeat",
                is_heartbeat=True,
                heartbeat_info={
                    "run_time": ts,
                    "article_count": len(articles_data),
                },
            )
            mark_heartbeat_sent(conn)
        else:
            log("No changes detected — not sending email.")

    # Record stats
    conn.execute(
        "INSERT OR REPLACE INTO support_crawl_stats (run_at, article_count) VALUES (?, ?)",
        (crawled_at, len(articles_data)),
    )
    conn.commit()

    # Record successful crawl run
    _record_crawl_run(
        crawled_at, run_start, status="success",
        article_count=len(articles_data),
        new_articles=n_new, removed_articles=n_removed,
        title_changes=n_title, body_changes=n_body, url_changes=n_url,
    )

    # Flush retry queue
    if retry_queue.pending_count > 0:
        logger.info("Flushing retry queue (%d pending)...", retry_queue.pending_count)
        permanently_failed = retry_queue.flush()
        if permanently_failed:
            labels = ", ".join(item["label"] for item in permanently_failed)
            send_error_alert(
                "Notification Delivery Failed",
                f"{len(permanently_failed)} notification(s) failed after all retries: {labels}",
                **_error_alert_ctx(),
            )

    conn.close()


def _get_peak_memory_mb() -> float:
    """Get peak RSS memory usage of this process in MB."""
    try:
        # ru_maxrss is in KB on Linux
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    except Exception:
        return 0.0


def _record_crawl_run(crawled_at, run_start, status="success", article_count=None,
                       new_articles=None, removed_articles=None,
                       title_changes=None, body_changes=None, url_changes=None,
                       error_message=None):
    """Record a crawl run in the support_crawl_runs table."""
    duration = round(time.time() - run_start, 2)
    peak_mb = _get_peak_memory_mb()
    finished_at = now_utc_iso()
    logger.info("Crawl finished: status=%s duration=%.1fs peak_memory=%.1fMB articles=%s",
                status, duration, peak_mb, article_count)
    try:
        conn = db()
        table_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='support_crawl_runs'"
        ).fetchone()
        if table_check:
            conn.execute(
                """INSERT INTO support_crawl_runs
                   (started_at, finished_at, status, article_count, new_articles,
                    removed_articles, title_changes, body_changes, url_changes,
                    error_message, duration_seconds)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (crawled_at, finished_at, status, article_count, new_articles,
                 removed_articles, title_changes, body_changes, url_changes,
                 error_message, duration),
            )
            conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed to record crawl run: %s", e)


if __name__ == "__main__":
    try:
        main()
    except TimeoutError as e:
        logger.error("Crawl aborted: %s", e)
        try:
            send_error_alert(
                "Crawl Timeout",
                str(e),
                **_error_alert_ctx(),
            )
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        logger.error("Crawl failed with unexpected error: %s", e, exc_info=True)
        try:
            send_error_alert(
                "Unexpected Error",
                str(e),
                **_error_alert_ctx(),
            )
        except Exception:
            pass
        sys.exit(1)
