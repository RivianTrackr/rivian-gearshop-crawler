#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rivian Offers Crawler — tracks promotional offer changes and sends alerts.

Discovers all offers on rivian.com/offers, extracts title and body text for
each offer card, detects changes (new, removed, title/body/CTA changes), and
sends email/Discord notifications with detailed diffs.
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

import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from notify import retry_queue, send_error_alert
from offers_migrations import run_migrations

# ---------------------- Config & Logging ----------------------

load_dotenv()

logger = logging.getLogger("offers_crawler")
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG if os.getenv("CRAWLER_DEBUG", "0") == "1" else logging.INFO,
)


def log(msg: str):
    logger.debug(msg)


OFFERS_URL = os.getenv("OFFERS_URL", "https://rivian.com/offers")
OFFERS_DB_PATH = os.getenv("OFFERS_DB_PATH", "/opt/rivian-gearshop-crawler/offers.db")

ADMIN_DB_PATH = os.getenv("ADMIN_DB_PATH", os.path.join(os.getcwd(), "admin.db"))
SCRIPT_NAME = os.getenv("OFFERS_SCRIPT_NAME", "rivian-offers-crawler")

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

OFFER_DELAY = float(os.getenv("OFFERS_DELAY", "0.5"))
MAX_OFFERS = int(os.getenv("OFFERS_MAX", "200"))
HEARTBEAT_UTC_HOUR = int(os.getenv("HEARTBEAT_UTC_HOUR", "-1"))
SNAPSHOT_RETENTION = 30
MAX_RUN_SECONDS = int(os.getenv("OFFERS_MAX_RUN_SECONDS", "600"))

HEADERS = {"User-Agent": "RivianOffersCrawler/1.0 (+https://riviantrackr.com)"}

MAX_DIFF_LINES_EMAIL = 100


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ---------------------- SQLite ----------------------

def db():
    conn = sqlite3.connect(OFFERS_DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        run_migrations(conn)
    finally:
        conn.close()


def last_offer_count(conn):
    row = conn.execute(
        "SELECT offer_count FROM offers_crawl_stats ORDER BY run_at DESC LIMIT 1"
    ).fetchone()
    return row["offer_count"] if row else None


def has_any_offer(conn):
    return conn.execute("SELECT 1 FROM offers LIMIT 1").fetchone() is not None


def heartbeat_sent_today(conn):
    return conn.execute(
        "SELECT 1 FROM offers_heartbeats WHERE day_utc=?", (today_utc_str(),)
    ).fetchone() is not None


def mark_heartbeat_sent(conn):
    conn.execute("INSERT OR IGNORE INTO offers_heartbeats (day_utc) VALUES (?)", (today_utc_str(),))
    conn.commit()


def should_send_heartbeat(conn):
    if HEARTBEAT_UTC_HOUR < 0 or HEARTBEAT_UTC_HOUR > 23:
        return False
    if datetime.now(timezone.utc).hour != HEARTBEAT_UTC_HOUR:
        return False
    return not heartbeat_sent_today(conn)


# ---------------------- Content Filters ----------------------

_content_filters: list[dict] = []


def load_content_filters(conn):
    global _content_filters
    try:
        rows = conn.execute(
            "SELECT id, pattern, filter_type FROM offers_content_filters WHERE enabled = 1"
        ).fetchall()
        _content_filters = [dict(r) for r in rows]
        if _content_filters:
            logger.info("Loaded %d offers content filter(s)", len(_content_filters))
    except Exception as e:
        logger.warning("Could not load content filters: %s", e)
        _content_filters = []


def apply_content_filters(text: str) -> str:
    for f in _content_filters:
        pattern = f["pattern"]
        if f["filter_type"] == "section_strip":
            lines = text.splitlines()
            cut = None
            for i, line in enumerate(lines):
                if pattern.lower() in line.lower().strip():
                    cut = i
                    break
            if cut is not None:
                text = "\n".join(lines[:cut]).rstrip()
    return text


# ---------------------- Content Helpers ----------------------

def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip())


def compute_content_hash(text: str) -> str:
    return hashlib.sha256(
        normalize_text(apply_content_filters(text)).encode("utf-8")
    ).hexdigest()


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slug_from_title(title: str) -> str:
    """Produce a stable slug from an offer title."""
    s = _SLUG_RE.sub("-", (title or "").lower()).strip("-")
    return s[:100] or hashlib.sha1((title or "").encode("utf-8")).hexdigest()[:16]


# ---------------------- Playwright Discovery & Extraction ----------------------

def discover_offers(page) -> list[dict]:
    """
    Visit /offers and extract each offer card.
    Returns a list of dicts: {slug, url, title, body_text, cta_url, expiration}.

    Strategy: the Rivian marketing site is an SPA. We wait for any heading to
    render, scroll to trigger lazy content, then walk DOM headings (h2/h3/h4)
    and treat each as an offer candidate. For each heading we collect the
    closest ancestor that also contains paragraph text and/or a CTA link.
    """
    log(f"Discovering offers from {OFFERS_URL}")

    last_err = None
    for attempt in range(3):
        try:
            page.goto(OFFERS_URL, wait_until="commit", timeout=90000)
            last_err = None
            break
        except Exception as e:
            last_err = e
            logger.warning("Attempt %d to load %s failed: %s", attempt + 1, OFFERS_URL, e)
            if attempt < 2:
                page.wait_for_timeout(3000)
    if last_err:
        raise last_err

    try:
        page.wait_for_selector("h1, h2, h3", timeout=30000)
    except Exception:
        logger.warning("No headings appeared after 30s — page may not have rendered")

    # Trigger any lazy content
    for _ in range(4):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1200)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(500)

    page_title = (page.title() or "").strip().lower()

    # Extract offer cards via in-page JS. We look for any heading h2–h4 and
    # ascend to an ancestor "card" (block element containing both the heading
    # and at least one paragraph or link). This is defensive against markup
    # changes — we don't rely on framework-specific class names.
    raw = page.evaluate(
        r"""() => {
            const results = [];
            const seenContainers = new Set();

            const headings = Array.from(document.querySelectorAll('h2, h3, h4'));
            for (const h of headings) {
                const titleText = (h.innerText || h.textContent || '').trim();
                if (!titleText || titleText.length < 3 || titleText.length > 200) continue;

                // Walk up to find a container that wraps meaningful content.
                let container = h.parentElement;
                let depth = 0;
                while (container && depth < 6) {
                    const hasPara = container.querySelector('p');
                    const hasLink = container.querySelector('a[href]');
                    const rect = container.getBoundingClientRect();
                    if ((hasPara || hasLink) && rect.height > 60) break;
                    container = container.parentElement;
                    depth += 1;
                }
                if (!container) continue;
                if (seenContainers.has(container)) continue;
                seenContainers.add(container);

                const bodyText = (container.innerText || '').trim();
                if (bodyText.length < 20) continue;

                // Primary CTA: first in-container link that isn't the heading itself.
                let ctaUrl = '';
                const links = Array.from(container.querySelectorAll('a[href]'));
                for (const a of links) {
                    const href = a.href || '';
                    if (!href || href.startsWith('javascript:')) continue;
                    ctaUrl = href;
                    break;
                }

                results.push({
                    title: titleText,
                    body_text: bodyText,
                    cta_url: ctaUrl,
                });
            }
            return results;
        }"""
    )

    # Dedupe by slug, respect MAX_OFFERS, drop obvious chrome items.
    seen_slugs = set()
    offers: list[dict] = []
    for item in raw:
        title = (item.get("title") or "").strip()
        body = (item.get("body_text") or "").strip()
        if not title:
            continue

        # Skip site chrome / nav headings that happen to match our selectors.
        low = title.lower()
        if low in {"offers", "rivian", "menu", "navigation", "search"}:
            continue
        if page_title and low == page_title:
            continue

        slug = slug_from_title(title)
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        offers.append({
            "slug": slug,
            "url": OFFERS_URL + "#" + slug,
            "title": title,
            "body_text": body,
            "cta_url": item.get("cta_url") or "",
            "expiration": _extract_expiration(body),
        })

        if len(offers) >= MAX_OFFERS:
            break

    log(f"Discovered {len(offers)} offers")
    if not offers:
        logger.warning("Zero offers discovered — page may not have rendered or selectors drifted")
    return offers


_EXPIRATION_PATTERNS = [
    re.compile(r"(?:expires?|ends?|valid\s+through|through)\s*[:\-]?\s*([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", re.I),
    re.compile(r"(?:expires?|ends?|valid\s+through|through)\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})", re.I),
]


def _extract_expiration(text: str) -> str:
    for pat in _EXPIRATION_PATTERNS:
        m = pat.search(text or "")
        if m:
            return m.group(1).strip()
    return ""


# ---------------------- Diff Generation ----------------------

def generate_text_diff(old_text: str, new_text: str) -> str:
    old_lines = old_text.splitlines()
    new_lines = new_text.splitlines()
    return "\n".join(difflib.unified_diff(old_lines, new_lines, lineterm="", n=2))


def generate_html_diff(old_text: str, new_text: str) -> str:
    diff = list(difflib.unified_diff(old_text.splitlines(), new_text.splitlines(), lineterm="", n=2))
    if not diff:
        return "<em>No visible text differences</em>"
    parts = []
    count = 0
    for line in diff:
        if line.startswith("---") or line.startswith("+++"):
            continue
        if count >= MAX_DIFF_LINES_EMAIL:
            remaining = len(diff) - count
            parts.append(
                f'<div style="color:#6b7280;padding:2px 6px;font-style:italic;">'
                f'... and {remaining} more lines changed</div>'
            )
            break
        if line.startswith("+"):
            parts.append(
                f'<div style="background:#d4edda;padding:2px 6px;font-family:monospace;font-size:12px;">'
                f'+ {html_escape(line[1:])}</div>'
            )
        elif line.startswith("-"):
            parts.append(
                f'<div style="background:#f8d7da;padding:2px 6px;font-family:monospace;font-size:12px;">'
                f'- {html_escape(line[1:])}</div>'
            )
        elif line.startswith("@@"):
            parts.append(
                f'<div style="color:#6b7280;padding:4px 6px 2px;font-family:monospace;font-size:11px;">'
                f'{html_escape(line)}</div>'
            )
        count += 1
    return "".join(parts) or "<em>No visible text differences</em>"


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


def _offer_link(o: dict) -> str:
    """Return the best link for an offer: its CTA if present, else an anchor
    into the offers page keyed by slug."""
    cta = (o.get("cta_url") or "").strip()
    if cta:
        return cta
    slug = o.get("slug") or ""
    return f"{OFFERS_URL}#{slug}" if slug else OFFERS_URL


def build_changes_email(changes: dict, is_initial: bool = False, offer_count: int = 0) -> str:
    title = "RivianCrawlr Offers: Initial Scan" if is_initial else "RivianCrawlr Offers: Changes Detected"
    parts = [
        '<div style="font-family:system-ui,-apple-system,sans-serif;max-width:700px;">',
        f"<h2>{title}</h2>",
    ]
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    parts.append(f"<p><small>Generated {ts} UTC</small></p>")

    if is_initial:
        parts.append(f"<p>Initial scan complete. Found {offer_count} offers.</p>")
        if changes.get("new"):
            parts.append("<h3>Offers Found</h3><ul>")
            for o in changes["new"]:
                otitle = html_escape(o["title"])
                link = html_escape(_offer_link(o))
                exp = html_escape(o.get("expiration", ""))
                exp_label = f" &mdash; <em>expires {exp}</em>" if exp else ""
                parts.append(f'<li><a href="{link}">{otitle}</a>{exp_label}</li>')
            parts.append("</ul>")
        parts.append("</div>")
        return "".join(parts)

    if changes.get("new"):
        parts.append(f'<h3 style="color:#34c759;">New Offers ({len(changes["new"])})</h3><ul>')
        for o in changes["new"]:
            otitle = html_escape(o["title"])
            link = html_escape(_offer_link(o))
            exp = html_escape(o.get("expiration", ""))
            exp_label = f" &mdash; <em>expires {exp}</em>" if exp else ""
            parts.append(f'<li><a href="{link}">{otitle}</a>{exp_label}</li>')
        parts.append("</ul>")

    if changes.get("removed"):
        parts.append(f'<h3 style="color:#ff3b30;">Removed Offers ({len(changes["removed"])})</h3><ul>')
        for o in changes["removed"]:
            otitle = html_escape(o["title"])
            slug = html_escape(o["slug"])
            parts.append(f"<li>{otitle} (<code>{slug}</code>)</li>")
        parts.append("</ul>")

    if changes.get("title_changed"):
        parts.append(f'<h3>Title Changes ({len(changes["title_changed"])})</h3>')
        parts.append(
            '<table border="1" cellspacing="0" cellpadding="6" '
            'style="border-collapse:collapse;font-family:system-ui;font-size:13px;">'
            "<thead><tr><th>Offer</th><th>Old Title</th><th>New Title</th></tr></thead><tbody>"
        )
        for c in changes["title_changed"]:
            slug = html_escape(c["slug"])
            link = html_escape(_offer_link(c))
            old_t = html_escape(c["old_title"])
            new_t = html_escape(c["new_title"])
            parts.append(
                f'<tr><td><a href="{link}">{slug}</a></td>'
                f"<td>{old_t}</td><td>{new_t}</td></tr>"
            )
        parts.append("</tbody></table>")

    if changes.get("url_changed"):
        parts.append(f'<h3>CTA URL Changes ({len(changes["url_changed"])})</h3>')
        parts.append(
            '<table border="1" cellspacing="0" cellpadding="6" '
            'style="border-collapse:collapse;font-family:system-ui;font-size:13px;">'
            "<thead><tr><th>Offer</th><th>Old CTA</th><th>New CTA</th></tr></thead><tbody>"
        )
        for c in changes["url_changed"]:
            otitle = html_escape(c["title"])
            old_u = html_escape(c["old_url"])
            new_u = html_escape(c["new_url"])
            parts.append(
                f'<tr><td>{otitle}</td>'
                f'<td><a href="{old_u}">{old_u}</a></td>'
                f'<td><a href="{new_u}">{new_u}</a></td></tr>'
            )
        parts.append("</tbody></table>")

    if changes.get("body_changed"):
        parts.append(f'<h3>Content Changes ({len(changes["body_changed"])})</h3>')
        for c in changes["body_changed"]:
            otitle = html_escape(c["title"])
            link = html_escape(_offer_link(c))
            parts.append(f'<h4><a href="{link}">{otitle}</a></h4>')
            parts.append(
                f'<div style="border:1px solid #ddd;padding:8px;border-radius:4px;'
                f'max-height:400px;overflow-y:auto;margin-bottom:16px;">{c["diff_html"]}</div>'
            )

    parts.append(
        '<p style="color:#666;font-size:12px;">This is an automated notification from '
        "RivianCrawlr Offers Monitor.</p></div>"
    )
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
            "description": "No offer changes detected.",
            "fields": [
                {"name": "Run Time", "value": str(heartbeat_info.get("run_time", "N/A")), "inline": True},
                {"name": "Offers Seen", "value": str(heartbeat_info.get("offer_count", 0)), "inline": True},
            ],
            "color": 0x3B82F6,
            "footer": embed_footer,
            "timestamp": now_iso,
        })
    elif changes:
        if changes.get("new") and cfg.get("notify_new_articles", True):
            lines = []
            for o in changes["new"][:10]:
                otitle = o.get("title", o.get("slug", "Unknown"))
                cta = o.get("cta_url") or OFFERS_URL
                lines.append(f"\u2022 [{otitle}]({cta})")
            if len(changes["new"]) > 10:
                lines.append(f"*...and {len(changes['new']) - 10} more*")
            embeds.append({
                "title": f"New Offers ({len(changes['new'])})",
                "description": "\n".join(lines),
                "color": 0x34C759,
                "footer": embed_footer,
                "timestamp": now_iso,
            })
            mention = _discord_mention("new")
            if mention:
                mentions.append(mention)

        if changes.get("removed") and cfg.get("notify_removed_articles", True):
            lines = [f"\u2022 {o.get('title', o.get('slug', 'Unknown'))}" for o in changes["removed"][:10]]
            if len(changes["removed"]) > 10:
                lines.append(f"*...and {len(changes['removed']) - 10} more*")
            embeds.append({
                "title": f"Removed Offers ({len(changes['removed'])})",
                "description": "\n".join(lines),
                "color": 0xFF3B30,
                "footer": embed_footer,
                "timestamp": now_iso,
            })
            mention = _discord_mention("removed")
            if mention:
                mentions.append(mention)

        change_items = (
            changes.get("body_changed", [])
            + changes.get("title_changed", [])
            + changes.get("url_changed", [])
        )
        if change_items and cfg.get("notify_article_changes", True):
            lines = []
            for c in change_items[:15]:
                otitle = c.get("title", c.get("slug", ""))
                change_type = c.get("change_type", "updated")
                cta = c.get("cta_url") or OFFERS_URL
                lines.append(f"\u2022 [{otitle}]({cta}): **{change_type}**")
            if len(change_items) > 15:
                lines.append(f"*...and {len(change_items) - 15} more changes*")
            embeds.append({
                "title": f"Offer Changes ({len(change_items)})",
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

    payload = {"username": cfg.get("username") or "RivianCrawlr", "embeds": embeds[:10]}
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


# ---------------------- Main ----------------------

def _error_alert_ctx():
    return dict(
        discord_webhook_url=DISCORD_WEBHOOK_URL,
        discord_config=DISCORD_CONFIG,
        brevo_api_key=BREVO_API_KEY,
        email_from=EMAIL_FROM,
        email_to=EMAIL_TO,
    )


def _check_timeout(run_start: float, phase: str):
    elapsed = time.time() - run_start
    if elapsed > MAX_RUN_SECONDS:
        raise TimeoutError(
            f"Offers crawl exceeded {MAX_RUN_SECONDS}s limit during {phase} (elapsed: {elapsed:.0f}s)"
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

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-dev-shm-usage", "--no-sandbox", "--disable-setuid-sandbox"],
                timeout=60000,
            )
        except Exception as e:
            logger.error("Failed to launch browser: %s", e)
            send_error_alert(
                "Browser Launch Failed",
                "Playwright/Chromium failed to start. The offers crawl was aborted.",
                details=str(e),
                **_error_alert_ctx(),
            )
            _record_crawl_run(crawled_at, run_start, status="error", error_message=str(e))
            return

        context = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1600, "height": 1200},
        )
        page = context.new_page()

        try:
            offers_data = discover_offers(page)
        except Exception as e:
            logger.error("Discovery failed: %s", e)
            send_error_alert(
                "Offers Discovery Failed",
                "The offers crawler failed during page discovery.",
                details=f"{type(e).__name__}: {e}",
                **_error_alert_ctx(),
            )
            browser.close()
            _record_crawl_run(crawled_at, run_start, status="error", error_message=str(e))
            return

        browser.close()

    _check_timeout(run_start, "post-discovery")

    # Add computed hash per offer
    for o in offers_data:
        o["body_hash"] = compute_content_hash(o["body_text"])

    if not offers_data:
        logger.error("No offers discovered — aborting")
        send_error_alert(
            "No Offers Found",
            "The offers crawler found zero offers. This likely indicates a site change or crawl failure.",
            **_error_alert_ctx(),
        )
        _record_crawl_run(crawled_at, run_start, status="error", error_message="No offers discovered")
        return

    conn = db()
    is_initial = not has_any_offer(conn)

    # Anomaly guard
    prev_count = last_offer_count(conn)
    current_count = len(offers_data)
    if prev_count and prev_count >= 4 and current_count < max(2, int(prev_count * 0.5)):
        msg = (
            f"Anomaly: only {current_count} offers vs last {prev_count}. "
            "Skipping this run to avoid false removals."
        )
        logger.warning(msg)
        send_error_alert(
            "Offers Anomaly Guard Triggered",
            msg,
            details=f"Previous count: {prev_count}\nCurrent count: {current_count}",
            **_error_alert_ctx(),
        )
        conn.execute(
            "INSERT OR REPLACE INTO offers_crawl_stats (run_at, offer_count) VALUES (?,?)",
            (crawled_at, current_count),
        )
        conn.commit()
        _record_crawl_run(crawled_at, run_start, status="skipped",
                          offer_count=current_count, error_message=msg)
        conn.close()
        return

    changes = {"new": [], "removed": [], "title_changed": [], "body_changed": [], "url_changed": []}
    seen_ids = set()
    confirmed_removed: list[dict] = []

    try:
        conn.execute("BEGIN")
        cur = conn.cursor()

        for offer in offers_data:
            slug = offer["slug"]
            existing = conn.execute("SELECT * FROM offers WHERE slug = ?", (slug,)).fetchone()

            if existing:
                oid = existing["id"]
                seen_ids.add(oid)

                title_changed = existing["title"] != offer["title"]
                body_changed = existing["body_hash"] != offer["body_hash"]
                cta_changed = (existing["cta_url"] or "") != (offer["cta_url"] or "")

                if title_changed and not is_initial:
                    changes["title_changed"].append({
                        "slug": slug,
                        "old_title": existing["title"],
                        "new_title": offer["title"],
                        "title": offer["title"],
                        "cta_url": offer["cta_url"],
                        "change_type": "title changed",
                    })

                if body_changed and not is_initial:
                    prev_snap = conn.execute(
                        "SELECT body_text FROM offer_snapshots WHERE offer_id = ? ORDER BY id DESC LIMIT 1",
                        (oid,),
                    ).fetchone()
                    old_body = prev_snap["body_text"] if prev_snap else existing["body_text"]
                    filtered_old = apply_content_filters(old_body)
                    filtered_new = apply_content_filters(offer["body_text"])
                    changes["body_changed"].append({
                        "slug": slug,
                        "title": offer["title"],
                        "cta_url": offer["cta_url"],
                        "old_body": filtered_old,
                        "new_body": filtered_new,
                        "diff_html": generate_html_diff(filtered_old, filtered_new),
                        "change_type": "content updated",
                    })

                if cta_changed and not is_initial:
                    changes["url_changed"].append({
                        "slug": slug,
                        "title": offer["title"],
                        "old_url": existing["cta_url"] or "",
                        "new_url": offer["cta_url"] or "",
                        "cta_url": offer["cta_url"],
                        "change_type": "CTA changed",
                    })

                if title_changed or body_changed or cta_changed:
                    cur.execute(
                        """UPDATE offers
                           SET title=?, body_text=?, body_hash=?, cta_url=?, expiration=?,
                               last_seen_at=?, updated_at=?, removed=0
                           WHERE id=?""",
                        (offer["title"], offer["body_text"], offer["body_hash"],
                         offer["cta_url"], offer["expiration"],
                         crawled_at, crawled_at, oid),
                    )
                else:
                    cur.execute(
                        "UPDATE offers SET last_seen_at=?, removed=0 WHERE id=?",
                        (crawled_at, oid),
                    )

                if title_changed or body_changed or cta_changed or is_initial:
                    cur.execute(
                        """INSERT INTO offer_snapshots
                           (offer_id, crawled_at, title, body_text, body_hash, url, cta_url, expiration)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (oid, crawled_at, offer["title"], offer["body_text"],
                         offer["body_hash"], offer["url"], offer["cta_url"], offer["expiration"]),
                    )

            else:
                cur.execute(
                    """INSERT INTO offers
                       (slug, url, title, body_text, body_hash, cta_url, expiration,
                        first_seen_at, last_seen_at, updated_at, removed)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                    (slug, offer["url"], offer["title"], offer["body_text"],
                     offer["body_hash"], offer["cta_url"], offer["expiration"],
                     crawled_at, crawled_at, crawled_at),
                )
                oid = cur.lastrowid
                seen_ids.add(oid)

                cur.execute(
                    """INSERT INTO offer_snapshots
                       (offer_id, crawled_at, title, body_text, body_hash, url, cta_url, expiration)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (oid, crawled_at, offer["title"], offer["body_text"],
                     offer["body_hash"], offer["url"], offer["cta_url"], offer["expiration"]),
                )

                changes["new"].append({
                    "slug": slug,
                    "title": offer["title"],
                    "cta_url": offer["cta_url"],
                    "expiration": offer["expiration"],
                })

            cur.execute(
                "INSERT OR IGNORE INTO offers_crawl_markers (crawled_at, offer_id) VALUES (?,?)",
                (crawled_at, oid),
            )

        if seen_ids:
            placeholders = ",".join("?" for _ in seen_ids)
            conn.execute(
                f"DELETE FROM offers_removed_once WHERE offer_id IN ({placeholders})",
                list(seen_ids),
            )

        conn.commit()

        # Removed offers: require three consecutive misses. Unlike support
        # articles, offers share a single URL so we can't 404-check individually.
        rows = conn.execute("""
            SELECT DISTINCT crawled_at FROM offers_crawl_markers
            WHERE crawled_at < ? ORDER BY crawled_at DESC LIMIT 2
        """, (crawled_at,)).fetchall()
        prev_times = [r["crawled_at"] for r in rows]
        removed_candidates: list[dict] = []

        if len(prev_times) == 2:
            prev1, prev2 = prev_times[0], prev_times[1]
            removed_candidates = [
                dict(row) for row in conn.execute("""
                    SELECT o.id, o.title, o.slug
                    FROM offers o
                    WHERE o.removed = 0
                      AND EXISTS (SELECT 1 FROM offers_crawl_markers cm WHERE cm.offer_id = o.id)
                      AND NOT EXISTS (
                            SELECT 1 FROM offers_crawl_markers cm
                            WHERE cm.offer_id = o.id AND cm.crawled_at = ?)
                      AND NOT EXISTS (
                            SELECT 1 FROM offers_crawl_markers cm
                            WHERE cm.offer_id = o.id AND cm.crawled_at = ?)
                      AND NOT EXISTS (
                            SELECT 1 FROM offers_crawl_markers cm
                            WHERE cm.offer_id = o.id AND cm.crawled_at = ?)
                """, (crawled_at, prev1, prev2)).fetchall()
            ]

        already = {
            row["offer_id"]
            for row in conn.execute("SELECT offer_id FROM offers_removed_once").fetchall()
        }
        removed_candidates = [r for r in removed_candidates if r["id"] not in already]

        for r in removed_candidates:
            confirmed_removed.append(r)
            conn.execute("UPDATE offers SET removed=1 WHERE id=?", (r["id"],))

        changes["removed"] = confirmed_removed

        # Prune old snapshots
        conn.execute(f"""
            DELETE FROM offer_snapshots
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY id DESC) AS rn
                    FROM offer_snapshots
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
            "Offers Crawl Failed",
            "An unhandled error occurred during the offers crawl.",
            details=f"{type(exc).__name__}: {exc}",
            **_error_alert_ctx(),
        )
        _record_crawl_run(crawled_at, run_start, status="error", error_message=str(exc))
        raise

    n_new = len(changes["new"])
    n_removed = len(changes["removed"])
    n_title = len(changes["title_changed"])
    n_body = len(changes["body_changed"])
    n_url = len(changes["url_changed"])
    log(f"New: {n_new} | Removed: {n_removed} | Title: {n_title} | Body: {n_body} | CTA: {n_url}")

    changes_exist = bool(n_new or n_removed or n_title or n_body or n_url)

    if is_initial or changes_exist:
        html = build_changes_email(changes, is_initial=is_initial, offer_count=len(offers_data))
        subject = "RivianCrawlr Offers: Initial Scan" if is_initial else "RivianCrawlr Offers: Changes Detected"
        send_email(subject, html)
        send_discord(subject, changes=changes)

        if confirmed_removed:
            conn.executemany(
                "INSERT OR IGNORE INTO offers_removed_once (offer_id, first_reported_at) VALUES (?, ?)",
                [(r["id"], crawled_at) for r in confirmed_removed],
            )
            conn.commit()
    else:
        if should_send_heartbeat(conn):
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            html = f"""
                <h2>RivianCrawlr Offers: Daily Heartbeat</h2>
                <p>No offer changes detected.</p>
                <ul>
                  <li><b>Run time:</b> {ts}</li>
                  <li><b>Offers seen this run:</b> {len(offers_data)}</li>
                </ul>
                <p style='color:#666'>Heartbeat sent once per day at hour
                {HEARTBEAT_UTC_HOUR:02d}:00 UTC when there are no changes.</p>
            """
            send_email("RivianCrawlr Offers: Daily Heartbeat (No Changes)", html)
            send_discord(
                "RivianCrawlr Offers: Daily Heartbeat",
                is_heartbeat=True,
                heartbeat_info={"run_time": ts, "offer_count": len(offers_data)},
            )
            mark_heartbeat_sent(conn)
        else:
            log("No changes detected — not sending email.")

    conn.execute(
        "INSERT OR REPLACE INTO offers_crawl_stats (run_at, offer_count) VALUES (?, ?)",
        (crawled_at, len(offers_data)),
    )
    conn.commit()

    _record_crawl_run(
        crawled_at, run_start, status="success",
        offer_count=len(offers_data),
        new_offers=n_new, removed_offers=n_removed,
        title_changes=n_title, body_changes=n_body, url_changes=n_url,
    )

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
    try:
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    except Exception:
        return 0.0


def _record_crawl_run(crawled_at, run_start, status="success", offer_count=None,
                      new_offers=None, removed_offers=None,
                      title_changes=None, body_changes=None, url_changes=None,
                      error_message=None):
    duration = round(time.time() - run_start, 2)
    peak_mb = _get_peak_memory_mb()
    finished_at = now_utc_iso()
    logger.info("Offers crawl finished: status=%s duration=%.1fs peak_memory=%.1fMB offers=%s",
                status, duration, peak_mb, offer_count)
    try:
        conn = db()
        table_check = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='offers_crawl_runs'"
        ).fetchone()
        if table_check:
            conn.execute(
                """INSERT INTO offers_crawl_runs
                   (started_at, finished_at, status, offer_count, new_offers,
                    removed_offers, title_changes, body_changes, url_changes,
                    error_message, duration_seconds)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (crawled_at, finished_at, status, offer_count, new_offers,
                 removed_offers, title_changes, body_changes, url_changes,
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
            send_error_alert("Offers Crawl Timeout", str(e), **_error_alert_ctx())
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        logger.error("Crawl failed with unexpected error: %s", e, exc_info=True)
        try:
            send_error_alert("Offers Crawl Unexpected Error", str(e), **_error_alert_ctx())
        except Exception:
            pass
        sys.exit(1)
