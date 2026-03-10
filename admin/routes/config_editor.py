import os
import shutil

from fastapi import APIRouter, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from admin.db import get_admin_db
from admin.config import HIDDEN_CONFIG_KEYS, SENSITIVE_KEYS

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)


def _parse_env_file(path: str) -> list[dict]:
    """Parse a .env file into a list of {key, value, comment, is_commented} dicts."""
    entries = []
    if not os.path.exists(path):
        return entries

    with open(path, "r") as f:
        for line in f:
            line = line.rstrip("\n")
            stripped = line.strip()

            # Blank line
            if not stripped:
                entries.append({"type": "blank"})
                continue

            # Pure comment (not a commented-out key)
            if stripped.startswith("#") and "=" not in stripped:
                entries.append({"type": "comment", "text": line})
                continue

            # Commented-out key=value
            is_commented = stripped.startswith("#")
            if is_commented:
                stripped = stripped.lstrip("# ")

            if "=" in stripped:
                key, value = stripped.split("=", 1)
                key = key.strip()

                # Strip quotes from value
                value = value.strip()
                if (value.startswith('"') and value.endswith('"')) or \
                   (value.startswith("'") and value.endswith("'")):
                    value = value[1:-1]

                if key in HIDDEN_CONFIG_KEYS:
                    continue

                entries.append({
                    "type": "env",
                    "key": key,
                    "value": value,
                    "is_commented": is_commented,
                    "is_sensitive": key in SENSITIVE_KEYS,
                })
            else:
                entries.append({"type": "comment", "text": line})

    return entries


def _write_env_file(path: str, keys: list[str], values: list[str]):
    """Write key-value pairs back to .env, preserving hidden keys from original."""
    # Read original to preserve hidden keys and structure
    original_lines = []
    if os.path.exists(path):
        with open(path, "r") as f:
            original_lines = f.readlines()

    # Build new content
    new_pairs = dict(zip(keys, values))
    written_keys = set()
    output_lines = []

    for line in original_lines:
        stripped = line.strip()

        # Preserve blanks and pure comments
        if not stripped or (stripped.startswith("#") and "=" not in stripped):
            output_lines.append(line)
            continue

        is_commented = stripped.startswith("#")
        check = stripped.lstrip("# ") if is_commented else stripped

        if "=" in check:
            key = check.split("=", 1)[0].strip()
            if key in HIDDEN_CONFIG_KEYS:
                output_lines.append(line)
                written_keys.add(key)
                continue
            if key in new_pairs:
                val = new_pairs[key]
                # Quote values with spaces
                if " " in val or '"' in val:
                    val = f'"{val}"'
                output_lines.append(f"{key}={val}\n")
                written_keys.add(key)
                continue

        output_lines.append(line)

    # Add any new keys not in original
    for key in keys:
        if key not in written_keys and key not in HIDDEN_CONFIG_KEYS:
            val = new_pairs[key]
            if " " in val or '"' in val:
                val = f'"{val}"'
            output_lines.append(f"{key}={val}\n")

    with open(path, "w") as f:
        f.writelines(output_lines)


def _get_script(script_id: int):
    conn = get_admin_db()
    row = conn.execute("SELECT * FROM managed_scripts WHERE id = ?", (script_id,)).fetchone()
    conn.close()
    return row


@router.get("/scripts/{script_id}/config", response_class=HTMLResponse)
def config_page(request: Request, script_id: int):
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)

    env_path = script["env_file_path"]
    entries = _parse_env_file(env_path) if env_path else []

    return templates.TemplateResponse("config_editor.html", {
        "request": request,
        "script": script,
        "entries": entries,
        "csrf_token": request.state.csrf_token,
        "flash_message": None,
    })


@router.post("/scripts/{script_id}/config", response_class=HTMLResponse)
async def config_save(request: Request, script_id: int):
    script = _get_script(script_id)
    if not script or not script["env_file_path"]:
        return RedirectResponse("/", status_code=303)

    env_path = script["env_file_path"]
    form = await request.form()

    # Collect all key-value pairs from form
    keys = form.getlist("key")
    values = form.getlist("value")

    # Backup before writing
    if os.path.exists(env_path):
        shutil.copy2(env_path, env_path + ".bak")

    _write_env_file(env_path, keys, values)

    entries = _parse_env_file(env_path)

    return templates.TemplateResponse("config_editor.html", {
        "request": request,
        "script": script,
        "entries": entries,
        "csrf_token": request.state.csrf_token,
        "flash_message": "Configuration saved. Restart the crawler for changes to take effect.",
        "flash_type": "success",
    })
