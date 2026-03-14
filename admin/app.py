import os

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware

from admin.db import init_admin_db
from admin.auth import validate_session_token, get_csrf_token, create_session_token, COOKIE_NAME
from admin.config import SESSION_MAX_AGE
from admin.routes import auth_routes, dashboard, scripts, config_editor, data_viewer, settings

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = FastAPI(title="RivianTrackr Admin", docs_url=None, redoc_url=None)

app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@app.on_event("startup")
def startup():
    init_admin_db()


class AuthCSRFMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Skip auth for login page and static files
        if path.startswith("/static/") or path in ("/login", "/favicon.ico"):
            return await call_next(request)

        # Logout: validate session then clear cookie
        if path == "/logout" and request.method == "POST":
            token = request.cookies.get(COOKIE_NAME)
            if token and validate_session_token(token):
                response = RedirectResponse("/login", status_code=303)
                response.delete_cookie(COOKIE_NAME)
                return response
            return RedirectResponse("/login", status_code=303)

        # Check authentication
        token = request.cookies.get(COOKIE_NAME)
        if not token:
            return RedirectResponse("/login", status_code=303)

        session = validate_session_token(token)
        if session is None:
            response = RedirectResponse("/login", status_code=303)
            response.delete_cookie(COOKIE_NAME)
            return response

        # Store session and CSRF token in request state
        request.state.session = session
        request.state.csrf_token = get_csrf_token(token)

        # CSRF check on POST requests
        # Read the raw body and cache it so downstream handlers can re-read it
        if request.method == "POST":
            body = await request.body()
            # Parse form from the cached body
            form = await request.form()
            submitted_csrf = form.get("_csrf", "")
            if submitted_csrf != request.state.csrf_token:
                return RedirectResponse(path, status_code=303)

        response = await call_next(request)

        # Sliding window: refresh the session token on each request
        new_token = create_session_token(session["uid"])
        response.set_cookie(
            COOKIE_NAME, new_token,
            httponly=True, samesite="lax", max_age=SESSION_MAX_AGE,
        )

        return response


app.add_middleware(AuthCSRFMiddleware)

# Register routers
app.include_router(auth_routes.router)
app.include_router(dashboard.router)
app.include_router(scripts.router)
app.include_router(config_editor.router)
app.include_router(data_viewer.router)
app.include_router(settings.router)
