import os
import re

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from admin.db import get_admin_db
from admin.auth import verify_password, hash_password

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


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    # Load email recipients from the first managed script's .env
    conn = get_admin_db()
    try:
        script = conn.execute("SELECT * FROM managed_scripts LIMIT 1").fetchone()
    finally:
        conn.close()

    email_to = ""
    if script and script["env_file_path"] and os.path.exists(script["env_file_path"]):
        with open(script["env_file_path"], "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("EMAIL_TO="):
                    email_to = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break

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


def _settings_response(request: Request, flash: str = None, flash_type: str = "info",
                       email_to: str = None):
    if email_to is None:
        conn = get_admin_db()
        try:
            script = conn.execute("SELECT * FROM managed_scripts LIMIT 1").fetchone()
        finally:
            conn.close()
        email_to = ""
        if script and script["env_file_path"] and os.path.exists(script["env_file_path"]):
            with open(script["env_file_path"], "r") as f:
                for line in f:
                    line_s = line.strip()
                    if line_s.startswith("EMAIL_TO="):
                        email_to = line_s.split("=", 1)[1].strip().strip('"').strip("'")
                        break

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "email_to": email_to,
        "csrf_token": request.state.csrf_token,
        "flash_message": flash,
        "flash_type": flash_type,
    })
