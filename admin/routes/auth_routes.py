import time
import logging
from collections import defaultdict

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import os

from admin.auth import (
    verify_password, create_session_token, hash_password, COOKIE_NAME,
)
from admin.config import SESSION_MAX_AGE, COOKIE_SECURE, COOKIE_SAMESITE
from admin.db import get_admin_db

# Compared against when the submitted username does not exist, purely to burn
# the same bcrypt time a real comparison would.
_DUMMY_HASH = hash_password("not-a-real-password")

logger = logging.getLogger("admin.auth")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)

# Rate limiting: track failed login attempts per IP.
#
# NOTE: this is only per-IP if uvicorn is started with `--proxy-headers
# --forwarded-allow-ips=127.0.0.1` (see gearshop-admin.service). Without it
# every request appears to come from nginx at 127.0.0.1, so all attempts share
# one bucket and five failures from anyone lock out everybody.
_login_attempts: dict[str, list[float]] = defaultdict(list)
_MAX_ATTEMPTS = 5       # max failures per window
_WINDOW_SECONDS = 300   # 5-minute window
_MAX_TRACKED_IPS = 10_000  # ceiling so the table cannot grow without bound


def _prune(now: float) -> None:
    """Drop buckets with no attempts left inside the window.

    Without this, every distinct source IP that ever fails a login leaves a
    permanent entry behind — an unbounded dict in a long-running process.
    """
    for ip in [ip for ip, ts in _login_attempts.items()
               if not ts or now - ts[-1] >= _WINDOW_SECONDS]:
        del _login_attempts[ip]


def _is_rate_limited(ip: str) -> bool:
    """Check if an IP has exceeded the login attempt limit."""
    now = time.monotonic()
    attempts = [t for t in _login_attempts.get(ip, ()) if now - t < _WINDOW_SECONDS]
    if attempts:
        _login_attempts[ip] = attempts
    else:
        _login_attempts.pop(ip, None)
    return len(attempts) >= _MAX_ATTEMPTS


def _record_failed_attempt(ip: str):
    now = time.monotonic()
    _prune(now)
    if ip not in _login_attempts and len(_login_attempts) >= _MAX_TRACKED_IPS:
        # Table is full of live buckets — an attacker spraying from many
        # addresses. Drop the least recently active rather than growing.
        oldest = min(_login_attempts, key=lambda k: _login_attempts[k][-1])
        del _login_attempts[oldest]
    _login_attempts[ip].append(now)


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

    # Always run a bcrypt verification, even when the username does not exist,
    # so response time does not reveal which usernames are real.
    if row:
        password_ok = verify_password(password, row["password_hash"])
    else:
        verify_password(password, _DUMMY_HASH)
        password_ok = False

    if not password_ok:
        _record_failed_attempt(client_ip)
        logger.warning("Failed login attempt for user '%s' from %s", username, client_ip)
        return templates.TemplateResponse(
            "login.html", {"request": request, "error": "Invalid username or password."}
        )

    token = create_session_token(row["id"])
    response = RedirectResponse("/", status_code=303)
    response.set_cookie(
        COOKIE_NAME, token,
        httponly=True, samesite=COOKIE_SAMESITE, secure=COOKIE_SECURE,
        max_age=SESSION_MAX_AGE,
    )
    return response


@router.post("/logout")
def logout():
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(COOKIE_NAME)
    return response
