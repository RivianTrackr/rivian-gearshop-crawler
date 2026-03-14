import time
import logging
from collections import defaultdict

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import os

from admin.auth import (
    verify_password, create_session_token, COOKIE_NAME,
)
from admin.db import get_admin_db

logger = logging.getLogger("admin.auth")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)

# Rate limiting: track failed login attempts per IP
_login_attempts: dict[str, list[float]] = defaultdict(list)
_MAX_ATTEMPTS = 5       # max failures per window
_WINDOW_SECONDS = 300   # 5-minute window


def _is_rate_limited(ip: str) -> bool:
    """Check if an IP has exceeded the login attempt limit."""
    now = time.monotonic()
    attempts = _login_attempts[ip]
    # Prune old attempts outside the window
    _login_attempts[ip] = [t for t in attempts if now - t < _WINDOW_SECONDS]
    return len(_login_attempts[ip]) >= _MAX_ATTEMPTS


def _record_failed_attempt(ip: str):
    _login_attempts[ip].append(time.monotonic())


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    # If already logged in, redirect to dashboard
    from admin.auth import validate_session_token
    token = request.cookies.get(COOKIE_NAME)
    if token and validate_session_token(token):
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login", response_class=HTMLResponse)
def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    client_ip = request.client.host if request.client else "unknown"

    if _is_rate_limited(client_ip):
        logger.warning("Rate limited login attempt from %s", client_ip)
        return templates.TemplateResponse(
            "login.html", {"request": request, "error": "Too many login attempts. Please try again later."}
        )

    conn = get_admin_db()
    try:
        row = conn.execute("SELECT id, password_hash FROM users WHERE username = ?", (username,)).fetchone()
    finally:
        conn.close()

    if not row or not verify_password(password, row["password_hash"]):
        _record_failed_attempt(client_ip)
        logger.warning("Failed login attempt for user '%s' from %s", username, client_ip)
        return templates.TemplateResponse(
            "login.html", {"request": request, "error": "Invalid username or password."}
        )

    token = create_session_token(row["id"])
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(
        COOKIE_NAME, token,
        httponly=True, samesite="lax", max_age=60 * 60 * 24 * 7,
    )
    return response


@router.post("/logout")
def logout():
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(COOKIE_NAME)
    return response
