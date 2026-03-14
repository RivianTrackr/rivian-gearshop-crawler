import os
import re
import json
import logging

import requests as http_requests
from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from admin.db import get_admin_db
from admin.auth import verify_password, hash_password

logger = logging.getLogger("admin.settings")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)

# Basic email pattern: local@domain.tld
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _validate_email_list(email_str: str) -> str | None:
    """Validate a comma-separated list of emails. Returns error message or None."""
    emails = [e.strip() for e in email_str.split(",") if e.strip()]
    if not emails:
        return "At least one email address is required."
    for addr in emails:
        if not _EMAIL_RE.match(addr):
            return f"Invalid email address: {addr}"
    return None


def _read_env_value(env_path: str, key: str) -> str:
    """Read a single value from a .env file."""
    if not env_path or not os.path.exists(env_path):
        return ""
    with open(env_path, "r") as f:
        for line in f:
            line_s = line.strip()
            if line_s.startswith(f"{key}="):
                val = line_s.split("=", 1)[1].strip().strip('"').strip("'")
                return val
    return ""


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    # Load email recipients from the first managed script's .env
    conn = get_admin_db()
    try:
        script = conn.execute("SELECT * FROM managed_scripts LIMIT 1").fetchone()
    finally:
        conn.close()

    email_to = ""
    if script and script["env_file_path"]:
        email_to = _read_env_value(script["env_file_path"], "EMAIL_TO")

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "email_to": email_to,
        "csrf_token": request.state.csrf_token,
        "flash_message": None,
    })


@router.post("/settings/password", response_class=HTMLResponse)
def change_password(request: Request,
                    current_password: str = Form(...),
                    new_password: str = Form(...),
                    confirm_password: str = Form(...)):
    user_id = request.state.session["uid"]

    if new_password != confirm_password:
        return _settings_response(request, flash="Passwords do not match.", flash_type="error")

    if len(new_password) < 8:
        return _settings_response(request, flash="Password must be at least 8 characters.", flash_type="error")

    conn = get_admin_db()
    try:
        user = conn.execute("SELECT password_hash FROM users WHERE id = ?", (user_id,)).fetchone()

        if not user or not verify_password(current_password, user["password_hash"]):
            return _settings_response(request, flash="Current password is incorrect.", flash_type="error")

        new_hash = hash_password(new_password)
        conn.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_hash, user_id))
        conn.commit()
    finally:
        conn.close()

    return _settings_response(request, flash="Password changed successfully.", flash_type="success")


@router.post("/settings/emails", response_class=HTMLResponse)
def update_emails(request: Request, email_to: str = Form(...)):
    # Validate email addresses
    error = _validate_email_list(email_to)
    if error:
        return _settings_response(request, email_to=email_to, flash=error, flash_type="error")

    conn = get_admin_db()
    try:
        script = conn.execute("SELECT * FROM managed_scripts LIMIT 1").fetchone()
    finally:
        conn.close()

    if not script or not script["env_file_path"]:
        return _settings_response(request, flash="No script configured.", flash_type="error")

    env_path = script["env_file_path"]

    # Read, update EMAIL_TO, write back
    lines = []
    found = False
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if line.strip().startswith("EMAIL_TO="):
                    val = email_to.strip()
                    if " " in val or "," in val:
                        val = f'"{val}"'
                    lines.append(f"EMAIL_TO={val}\n")
                    found = True
                else:
                    lines.append(line)

    if not found:
        val = email_to.strip()
        if " " in val or "," in val:
            val = f'"{val}"'
        lines.append(f"EMAIL_TO={val}\n")

    with open(env_path, "w") as f:
        f.writelines(lines)

    return _settings_response(request, email_to=email_to,
                              flash="Email recipients updated.", flash_type="success")


@router.post("/settings/test-email", response_class=HTMLResponse)
def send_test_email(request: Request):
    """Send a test email using the configured Brevo API key and recipients."""
    conn = get_admin_db()
    try:
        script = conn.execute("SELECT * FROM managed_scripts LIMIT 1").fetchone()
    finally:
        conn.close()

    if not script or not script["env_file_path"]:
        return _settings_response(request, flash="No script configured.", flash_type="error")

    env_path = script["env_file_path"]
    brevo_key = _read_env_value(env_path, "BREVO_API_KEY")
    email_from = _read_env_value(env_path, "EMAIL_FROM") or "RivianTrackr Alerts <alerts@example.com>"
    email_to_str = _read_env_value(env_path, "EMAIL_TO")

    if not brevo_key:
        return _settings_response(request, flash="BREVO_API_KEY is not configured.", flash_type="error")

    if not email_to_str:
        return _settings_response(request, flash="No email recipients configured.", flash_type="error")

    recipients = [e.strip() for e in email_to_str.split(",") if e.strip()]

    # Parse sender
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
    <p>If you're reading this, your email configuration is working correctly.</p>
    <p style="color:#666">Sent from the Settings page.</p>
    """

    payload = {
        "sender": {"email": sender_email, "name": sender_name},
        "to": [{"email": addr} for addr in recipients],
        "subject": "RivianTrackr: Test Email",
        "htmlContent": html,
    }
    headers = {
        "accept": "application/json",
        "api-key": brevo_key,
        "content-type": "application/json",
    }

    try:
        resp = http_requests.post(
            "https://api.brevo.com/v3/smtp/email",
            headers=headers,
            data=json.dumps(payload),
            timeout=15,
        )
        if resp.status_code < 300:
            logger.info("Test email sent successfully to %s", recipients)
            return _settings_response(
                request,
                flash=f"Test email sent to {', '.join(recipients)}.",
                flash_type="success",
            )
        else:
            logger.error("Test email failed: %d %s", resp.status_code, resp.text)
            return _settings_response(
                request,
                flash=f"Email send failed (HTTP {resp.status_code}): {resp.text[:200]}",
                flash_type="error",
            )
    except Exception as e:
        logger.error("Test email exception: %s", e)
        return _settings_response(
            request,
            flash=f"Email send error: {e}",
            flash_type="error",
        )


def _settings_response(request: Request, flash: str = None, flash_type: str = "info",
                       email_to: str = None):
    if email_to is None:
        conn = get_admin_db()
        try:
            script = conn.execute("SELECT * FROM managed_scripts LIMIT 1").fetchone()
        finally:
            conn.close()
        email_to = ""
        if script and script["env_file_path"]:
            email_to = _read_env_value(script["env_file_path"], "EMAIL_TO")

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "email_to": email_to,
        "csrf_token": request.state.csrf_token,
        "flash_message": flash,
        "flash_type": flash_type,
    })
