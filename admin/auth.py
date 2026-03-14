import os
import hmac
import hashlib

import bcrypt as _bcrypt
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from fastapi import Request, HTTPException, Form
from fastapi.responses import RedirectResponse

from admin.config import SECRET_KEY, SESSION_MAX_AGE

_serializer = URLSafeTimedSerializer(SECRET_KEY)

COOKIE_NAME = "session"


def hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode()


def verify_password(password: str, password_hash: str) -> bool:
    return _bcrypt.checkpw(password.encode(), password_hash.encode())


def create_session_token(user_id: int) -> str:
    return _serializer.dumps({"uid": user_id})


def validate_session_token(token: str) -> dict | None:
    try:
        return _serializer.loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


def get_csrf_token(user_id: int) -> str:
    return hmac.HMAC(
        SECRET_KEY.encode(), str(user_id).encode(), hashlib.sha256
    ).hexdigest()[:32]


def require_auth(request: Request) -> dict:
    """FastAPI dependency that returns session data or redirects to login."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    data = validate_session_token(token)
    if data is None:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    return data


def require_auth_dependency(request: Request) -> dict:
    """Same as require_auth but raises redirect for template routes."""
    token = request.cookies.get(COOKIE_NAME)
    if not token:
        return None
    return validate_session_token(token)


async def verify_csrf(request: Request) -> str:
    """FastAPI dependency that validates the CSRF token from form data.
    Must be included as a Depends() in every POST route handler."""
    form = await request.form()
    csrf = form.get("_csrf", "")
    expected = getattr(request.state, "csrf_token", None)
    if not expected or csrf != expected:
        raise HTTPException(status_code=403, detail="CSRF validation failed")
    return csrf
