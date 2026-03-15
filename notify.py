"""
Notification helpers with retry queue and error alerting.

Provides:
- RetryQueue: exponential-backoff retry for transient notification failures
- send_error_alert: dedicated channel for crawler error/failure notifications
"""

import json
import logging
import os
import time
import threading
from datetime import datetime, timezone
from typing import Callable, Optional

import re

import requests

logger = logging.getLogger("crawler.notify")


# --------------- Retry Queue ---------------

class RetryQueue:
    """
    Queue failed notification calls and retry with exponential backoff.

    Usage:
        rq = RetryQueue(max_retries=3, delays=[60, 300, 900])
        rq.enqueue("email", send_email, args=("subject", "<html>..."))
        rq.flush()   # call at end of crawl to process pending retries
    """

    def __init__(self, max_retries: int = 3, delays: Optional[list] = None):
        self.max_retries = max_retries
        self.delays = delays or [60, 300, 900]  # 1m, 5m, 15m
        self._queue: list[dict] = []
        self._lock = threading.Lock()

    def enqueue(self, label: str, func: Callable, args: tuple = (), kwargs: dict = None):
        """Add a failed notification to the retry queue."""
        with self._lock:
            self._queue.append({
                "label": label,
                "func": func,
                "args": args,
                "kwargs": kwargs or {},
                "attempt": 0,
            })
            logger.info("Queued %s notification for retry (%d pending)", label, len(self._queue))

    def flush(self) -> list[dict]:
        """
        Process all queued notifications with exponential backoff.
        Returns list of permanently failed items (exceeded max_retries).
        """
        permanently_failed = []

        with self._lock:
            items = list(self._queue)
            self._queue.clear()

        for item in items:
            success = False
            while item["attempt"] < self.max_retries:
                delay = self.delays[min(item["attempt"], len(self.delays) - 1)]
                item["attempt"] += 1

                logger.info(
                    "Retry %d/%d for %s (waiting %ds)...",
                    item["attempt"], self.max_retries, item["label"], delay
                )
                time.sleep(delay)

                try:
                    item["func"](*item["args"], **item["kwargs"])
                    logger.info("Retry succeeded for %s on attempt %d", item["label"], item["attempt"])
                    success = True
                    break
                except Exception as e:
                    logger.warning(
                        "Retry %d/%d failed for %s: %s",
                        item["attempt"], self.max_retries, item["label"], e
                    )

            if not success:
                logger.error("Permanently failed: %s after %d retries", item["label"], self.max_retries)
                permanently_failed.append(item)

        return permanently_failed

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._queue)


# Global retry queue instance
retry_queue = RetryQueue(max_retries=3, delays=[60, 300, 900])


# --------------- Error Alert Channel ---------------

def send_error_alert(
    error_type: str,
    message: str,
    details: str = "",
    discord_webhook_url: str = "",
    discord_config: dict = None,
    brevo_api_key: str = "",
    email_from: str = "",
    email_to: list = None,
):
    """
    Send an error notification via Discord and/or email.

    Called when:
    - Browser launch fails
    - Anomaly guard triggers (product count dropped)
    - Database write errors
    - Notification delivery failures (after all retries exhausted)
    - Unhandled exceptions in the main crawl

    Uses same channels as regular notifications but with distinct formatting
    so errors are visually distinguishable.
    """

    now_iso = datetime.now(timezone.utc).isoformat()
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # --- Discord ---
    webhook_url = discord_webhook_url or os.getenv("ERROR_DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        # Fall back to main webhook from config
        if discord_config and discord_config.get("webhook_url"):
            webhook_url = discord_config["webhook_url"]

    if webhook_url:
        cfg = discord_config or {}
        embed = {
            "title": f"Crawler Error: {error_type}",
            "description": message,
            "color": 0xDC2626,  # red
            "fields": [],
            "footer": {"text": "RivianTrackr \u2022 Error Alert"},
            "timestamp": now_iso,
        }
        if details:
            # Truncate to Discord's 1024 char limit for field values
            embed["fields"].append({
                "name": "Details",
                "value": f"```\n{details[:1000]}\n```",
                "inline": False,
            })
        embed["fields"].append({
            "name": "Timestamp",
            "value": ts,
            "inline": True,
        })

        payload = {
            "username": cfg.get("username", "RivianTrackr"),
            "embeds": [embed],
        }
        if cfg.get("avatar_url"):
            payload["avatar_url"] = cfg["avatar_url"]

        # Add mentions for errors (use same role/user as configured)
        mentions = []
        if cfg.get("mention_role_id"):
            mentions.append(f"<@&{cfg['mention_role_id']}>")
        if cfg.get("mention_user_id"):
            mentions.append(f"<@{cfg['mention_user_id']}>")
        if mentions:
            payload["content"] = " ".join(mentions)

        url = webhook_url
        if cfg.get("thread_id"):
            url += f"?thread_id={cfg['thread_id']}"

        try:
            resp = requests.post(url, json=payload, timeout=15)
            if resp.status_code < 300:
                logger.info("Error alert sent to Discord")
            else:
                logger.error("Error alert Discord failed: %d %s", resp.status_code, resp.text[:200])
        except Exception as e:
            logger.error("Error alert Discord exception: %s", e)

    # --- Email ---
    api_key = brevo_api_key or os.getenv("ERROR_BREVO_API_KEY", "")
    from_addr = email_from
    to_addrs = email_to or []

    if api_key and to_addrs:
        m = re.search(r"<([^>]+)>", from_addr)
        if m:
            sender_email = m.group(1)
            sender_name = from_addr.replace(m.group(0), "").strip()
        else:
            sender_email = from_addr
            sender_name = "RivianTrackr Alerts"

        html = f"""
        <div style="font-family: system-ui, -apple-system, sans-serif; max-width: 600px;">
            <div style="background: #DC2626; color: white; padding: 16px; border-radius: 8px 8px 0 0;">
                <h2 style="margin: 0;">Crawler Error: {error_type}</h2>
            </div>
            <div style="border: 1px solid #e5e7eb; border-top: none; padding: 16px; border-radius: 0 0 8px 8px;">
                <p>{message}</p>
                {"<pre style='background: #f3f4f6; padding: 12px; border-radius: 4px; overflow-x: auto;'>" + details[:2000] + "</pre>" if details else ""}
                <p style="color: #6b7280; font-size: 13px;">Timestamp: {ts}</p>
            </div>
        </div>
        """

        email_payload = {
            "sender": {"email": sender_email, "name": sender_name},
            "to": [{"email": addr} for addr in to_addrs],
            "subject": f"[ERROR] RivianTrackr: {error_type}",
            "htmlContent": html,
        }
        headers = {
            "accept": "application/json",
            "api-key": api_key,
            "content-type": "application/json",
        }

        try:
            resp = requests.post(
                "https://api.brevo.com/v3/smtp/email",
                headers=headers,
                data=json.dumps(email_payload),
                timeout=30,
            )
            if resp.status_code < 300:
                logger.info("Error alert sent via email")
            else:
                logger.error("Error alert email failed: %d %s", resp.status_code, resp.text[:200])
        except Exception as e:
            logger.error("Error alert email exception: %s", e)
