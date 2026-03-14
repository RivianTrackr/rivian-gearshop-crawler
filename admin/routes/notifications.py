import os
import json
import logging
from datetime import datetime, timezone

import requests as http_requests
from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from admin.db import get_admin_db
from admin.auth import verify_csrf
from admin.routes.helpers import get_script as _get_script

logger = logging.getLogger("admin.notifications")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)


def _get_notification(script_id: int, channel: str) -> dict | None:
    conn = get_admin_db()
    try:
        row = conn.execute(
            "SELECT * FROM script_notifications WHERE script_id = ? AND channel = ?",
            (script_id, channel),
        ).fetchone()
        if row:
            return {
                "id": row["id"],
                "script_id": row["script_id"],
                "channel": row["channel"],
                "enabled": bool(row["enabled"]),
                "config": json.loads(row["config"]),
            }
        return None
    finally:
        conn.close()


def _upsert_notification(script_id: int, channel: str, enabled: bool, config: dict):
    conn = get_admin_db()
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute(
            """INSERT INTO script_notifications (script_id, channel, enabled, config, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(script_id, channel)
               DO UPDATE SET enabled = excluded.enabled, config = excluded.config, updated_at = excluded.updated_at""",
            (script_id, channel, int(enabled), json.dumps(config), now, now),
        )
        conn.commit()
    finally:
        conn.close()


@router.get("/scripts/{script_id}/notifications", response_class=HTMLResponse)
def notifications_page(request: Request, script_id: int):
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)

    email_notif = _get_notification(script_id, "email")
    discord_notif = _get_notification(script_id, "discord")

    return templates.TemplateResponse("script_notifications.html", {
        "request": request,
        "script": script,
        "email": email_notif or {"enabled": False, "config": {}},
        "discord": discord_notif or {"enabled": False, "config": {}},
        "csrf_token": request.state.csrf_token,
        "flash_message": None,
    })


@router.post("/scripts/{script_id}/notifications/email", response_class=HTMLResponse)
def update_email_settings(request: Request, script_id: int,
                          email_enabled: str = Form(""),
                          brevo_api_key: str = Form(""),
                          email_from: str = Form(""),
                          email_to: str = Form(""),
                          csrf: str = Depends(verify_csrf)):
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)

    enabled = email_enabled == "on"
    config = {
        "brevo_api_key": brevo_api_key.strip(),
        "email_from": email_from.strip(),
        "email_to": email_to.strip(),
    }

    if enabled and not config["brevo_api_key"]:
        return _notif_response(request, script_id, flash="Brevo API key is required when email is enabled.", flash_type="error")

    if enabled and not config["email_to"]:
        return _notif_response(request, script_id, flash="At least one recipient is required.", flash_type="error")

    _upsert_notification(script_id, "email", enabled, config)
    return _notif_response(request, script_id, flash="Email notification settings saved.", flash_type="success")


@router.post("/scripts/{script_id}/notifications/discord", response_class=HTMLResponse)
def update_discord_settings(request: Request, script_id: int,
                            discord_enabled: str = Form(""),
                            discord_webhook_url: str = Form(""),
                            csrf: str = Depends(verify_csrf)):
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)

    enabled = discord_enabled == "on"
    webhook = discord_webhook_url.strip()

    if enabled and not webhook:
        return _notif_response(request, script_id, flash="Discord webhook URL is required when enabled.", flash_type="error")

    if webhook and not webhook.startswith("https://discord.com/api/webhooks/"):
        return _notif_response(
            request, script_id,
            flash="Invalid Discord webhook URL. Must start with https://discord.com/api/webhooks/",
            flash_type="error",
        )

    config = {"webhook_url": webhook}
    _upsert_notification(script_id, "discord", enabled, config)
    return _notif_response(request, script_id, flash="Discord notification settings saved.", flash_type="success")


@router.post("/scripts/{script_id}/notifications/test-email", response_class=HTMLResponse)
def test_email(request: Request, script_id: int, csrf: str = Depends(verify_csrf)):
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)

    notif = _get_notification(script_id, "email")
    if not notif or not notif["enabled"]:
        return _notif_response(request, script_id, flash="Email notifications are not enabled for this script.", flash_type="error")

    cfg = notif["config"]
    brevo_key = cfg.get("brevo_api_key", "")
    email_from = cfg.get("email_from") or "RivianTrackr Alerts <alerts@riviantrackr.com>"
    email_to_str = cfg.get("email_to", "")

    if not brevo_key:
        return _notif_response(request, script_id, flash="Brevo API key is not configured.", flash_type="error")

    import re
    recipients = [e.strip() for e in email_to_str.split(",") if e.strip()]
    if not recipients:
        return _notif_response(request, script_id, flash="No email recipients configured.", flash_type="error")

    m = re.search(r"<([^>]+)>", email_from)
    if m:
        sender_email = m.group(1)
        sender_name = email_from.replace(m.group(0), "").strip()
    else:
        sender_email = email_from
        sender_name = "RivianTrackr"

    html = """
    <h2>RivianTrackr: Test Email</h2>
    <p>This is a test email from your RivianTrackr admin panel.</p>
    <p>Script: <strong>{}</strong></p>
    <p>If you're reading this, your email configuration is working correctly.</p>
    """.format(script["display_name"])

    payload = {
        "sender": {"email": sender_email, "name": sender_name},
        "to": [{"email": addr} for addr in recipients],
        "subject": f"RivianTrackr: Test Email — {script['display_name']}",
        "htmlContent": html,
    }
    headers = {"accept": "application/json", "api-key": brevo_key, "content-type": "application/json"}

    try:
        resp = http_requests.post("https://api.brevo.com/v3/smtp/email", headers=headers,
                                  data=json.dumps(payload), timeout=15)
        if resp.status_code < 300:
            return _notif_response(request, script_id, flash=f"Test email sent to {', '.join(recipients)}.", flash_type="success")
        else:
            return _notif_response(request, script_id, flash=f"Email send failed (HTTP {resp.status_code}): {resp.text[:200]}", flash_type="error")
    except Exception as e:
        return _notif_response(request, script_id, flash=f"Email send error: {e}", flash_type="error")


@router.post("/scripts/{script_id}/notifications/test-discord", response_class=HTMLResponse)
def test_discord(request: Request, script_id: int, csrf: str = Depends(verify_csrf)):
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)

    notif = _get_notification(script_id, "discord")
    if not notif or not notif["enabled"]:
        return _notif_response(request, script_id, flash="Discord notifications are not enabled for this script.", flash_type="error")

    webhook_url = notif["config"].get("webhook_url", "")
    if not webhook_url:
        return _notif_response(request, script_id, flash="Discord webhook URL is not configured.", flash_type="error")

    payload = {
        "username": "RivianTrackr",
        "embeds": [{
            "title": f"RivianTrackr: Test Notification",
            "description": (
                f"This is a test notification from your RivianTrackr admin panel.\n"
                f"**Script:** {script['display_name']}\n"
                "If you're reading this, your Discord webhook is working correctly."
            ),
            "color": 0xFBA919,
        }],
    }

    try:
        resp = http_requests.post(webhook_url, json=payload, timeout=15)
        if resp.status_code < 300:
            return _notif_response(request, script_id, flash="Test notification sent to Discord.", flash_type="success")
        else:
            return _notif_response(request, script_id, flash=f"Discord send failed (HTTP {resp.status_code}): {resp.text[:200]}", flash_type="error")
    except Exception as e:
        return _notif_response(request, script_id, flash=f"Discord send error: {e}", flash_type="error")


def _notif_response(request: Request, script_id: int, flash: str = None, flash_type: str = "info"):
    script = _get_script(script_id)
    email_notif = _get_notification(script_id, "email")
    discord_notif = _get_notification(script_id, "discord")

    return templates.TemplateResponse("script_notifications.html", {
        "request": request,
        "script": script,
        "email": email_notif or {"enabled": False, "config": {}},
        "discord": discord_notif or {"enabled": False, "config": {}},
        "csrf_token": request.state.csrf_token,
        "flash_message": flash,
        "flash_type": flash_type,
    })
