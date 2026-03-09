from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import os

from admin.auth import (
    verify_password, create_session_token, COOKIE_NAME,
)
from admin.db import get_admin_db

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)


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
    conn = get_admin_db()
    row = conn.execute("SELECT id, password_hash FROM users WHERE username = ?", (username,)).fetchone()
    conn.close()

    if not row or not verify_password(password, row["password_hash"]):
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
