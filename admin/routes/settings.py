import os
import shutil
import tempfile
import logging

from fastapi import APIRouter, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from admin.db import get_admin_db
from admin.auth import verify_password, hash_password, verify_csrf
from admin.config import HIDDEN_CONFIG_KEYS, SENSITIVE_KEYS, KNOWN_ENV_KEYS, GLOBAL_ENV_KEYS

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


# ---------------------- Global Config ----------------------

ENV_PATH = os.path.join(os.getenv("DEPLOY_DIR", "/opt/rivian-gearshop-crawler"), ".env")


def _parse_global_env() -> list[dict]:
    """Parse .env and return only global keys."""
    entries = []
    if not os.path.exists(ENV_PATH):
        return entries
    with open(ENV_PATH, "r") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") and "=" not in stripped:
                continue
            is_commented = stripped.startswith("#")
            if is_commented:
                stripped = stripped.lstrip("# ")
            if "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key in HIDDEN_CONFIG_KEYS or key not in GLOBAL_ENV_KEYS:
                continue
            entries.append({
                "key": key,
                "value": value,
                "is_sensitive": key in SENSITIVE_KEYS,
            })
    return entries


@router.get("/settings/global-config", response_class=HTMLResponse)
def global_config_page(request: Request):
    entries = _parse_global_env()
    return templates.TemplateResponse("global_config.html", {
        "request": request,
        "entries": entries,
        "env_path": ENV_PATH,
        "csrf_token": request.state.csrf_token,
        "flash_message": None,
    })


@router.post("/settings/global-config", response_class=HTMLResponse)
async def global_config_save(request: Request):
    form = await request.form()
    csrf_token = form.get("_csrf", "")
    expected = getattr(request.state, "csrf_token", None)
    if not expected or csrf_token != expected:
        raise HTTPException(status_code=403, detail="CSRF validation failed")

    keys = form.getlist("key")
    values = form.getlist("value")
    new_pairs = dict(zip(keys, values))

    # Validate keys
    invalid = [k for k in keys if k not in GLOBAL_ENV_KEYS]
    if invalid:
        entries = _parse_global_env()
        return templates.TemplateResponse("global_config.html", {
            "request": request,
            "entries": entries,
            "env_path": ENV_PATH,
            "csrf_token": request.state.csrf_token,
            "flash_message": f"Unknown key(s): {', '.join(invalid)}",
            "flash_type": "error",
        })

    # Read original file, update matching keys, preserve everything else
    lines = []
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH, "r") as f:
            lines = f.readlines()

    written_keys = set()
    output_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped or (stripped.startswith("#") and "=" not in stripped):
            output_lines.append(line)
            continue
        is_commented = stripped.startswith("#")
        check = stripped.lstrip("# ") if is_commented else stripped
        if "=" in check:
            key = check.split("=", 1)[0].strip()
            if key in new_pairs:
                val = new_pairs[key]
                if " " in val or '"' in val:
                    val = f'"{val}"'
                output_lines.append(f"{key}={val}\n")
                written_keys.add(key)
                continue
        output_lines.append(line)

    # Add new keys not in original
    for key in keys:
        if key not in written_keys and key in GLOBAL_ENV_KEYS:
            val = new_pairs[key]
            if " " in val or '"' in val:
                val = f'"{val}"'
            output_lines.append(f"{key}={val}\n")

    # Atomic write
    if os.path.exists(ENV_PATH):
        shutil.copy2(ENV_PATH, ENV_PATH + ".bak")
    dir_name = os.path.dirname(ENV_PATH)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, prefix=".env.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.writelines(output_lines)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, ENV_PATH)
    except Exception:
        os.unlink(tmp_path)
        raise

    entries = _parse_global_env()
    return templates.TemplateResponse("global_config.html", {
        "request": request,
        "entries": entries,
        "env_path": ENV_PATH,
        "csrf_token": request.state.csrf_token,
        "flash_message": "Global configuration saved. Restart crawlers for changes to take effect.",
        "flash_type": "success",
    })
