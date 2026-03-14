import os
import logging

from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from admin.db import get_admin_db
from admin.auth import verify_password, hash_password, verify_csrf

logger = logging.getLogger("admin.settings")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)


@router.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request):
    conn = get_admin_db()
    try:
        scripts = conn.execute("SELECT id, display_name FROM managed_scripts ORDER BY id").fetchall()
    finally:
        conn.close()

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "scripts": scripts,
        "csrf_token": request.state.csrf_token,
        "flash_message": None,
    })


@router.post("/settings/password", response_class=HTMLResponse)
def change_password(request: Request,
                    current_password: str = Form(...),
                    new_password: str = Form(...),
                    confirm_password: str = Form(...),
                    csrf: str = Depends(verify_csrf)):
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


def _settings_response(request: Request, flash: str = None, flash_type: str = "info"):
    conn = get_admin_db()
    try:
        scripts = conn.execute("SELECT id, display_name FROM managed_scripts ORDER BY id").fetchall()
    finally:
        conn.close()

    return templates.TemplateResponse("settings.html", {
        "request": request,
        "scripts": scripts,
        "csrf_token": request.state.csrf_token,
        "flash_message": flash,
        "flash_type": flash_type,
    })
