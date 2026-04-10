import os
import subprocess
import logging

from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from admin.auth import verify_csrf
from admin.db import get_admin_db
from admin.systemd import (
    install_unit_files, daemon_reload, is_unit_installed,
    enable_service, restart_admin_service, install_admin_service,
)

logger = logging.getLogger("admin.deploy")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)

# The working directory where the git repo lives
DEPLOY_DIR = os.getenv("DEPLOY_DIR", "/opt/rivian-gearshop-crawler")
SUBPROCESS_TIMEOUT = 30


def _git_status() -> dict:
    """Get current git branch, commit, and remote status."""
    info = {"branch": "unknown", "commit": "", "commit_msg": "", "behind": 0, "error": None}
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
            cwd=DEPLOY_DIR,
        )
        info["branch"] = r.stdout.strip() if r.returncode == 0 else "unknown"

        r = subprocess.run(
            ["git", "log", "-1", "--format=%h %s"],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
            cwd=DEPLOY_DIR,
        )
        if r.returncode == 0:
            parts = r.stdout.strip().split(" ", 1)
            info["commit"] = parts[0]
            info["commit_msg"] = parts[1] if len(parts) > 1 else ""

        # Fetch to check how far behind
        subprocess.run(
            ["git", "fetch", "--quiet"],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
            cwd=DEPLOY_DIR,
        )
        r = subprocess.run(
            ["git", "rev-list", "--count", f"HEAD..origin/{info['branch']}"],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
            cwd=DEPLOY_DIR,
        )
        if r.returncode == 0:
            info["behind"] = int(r.stdout.strip())
    except Exception as e:
        info["error"] = str(e)
    return info


def _git_pull() -> tuple[bool, str]:
    """Pull latest changes from origin."""
    try:
        r = subprocess.run(
            ["git", "pull", "--ff-only"],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
            cwd=DEPLOY_DIR,
        )
        output = r.stdout.strip()
        if r.returncode != 0:
            return False, r.stderr.strip() or output
        return True, output
    except Exception as e:
        return False, str(e)


def _get_unit_status() -> list[dict]:
    """Get installation status for all managed scripts' systemd units."""
    conn = get_admin_db()
    try:
        rows = conn.execute("SELECT * FROM managed_scripts ORDER BY display_name").fetchall()
    finally:
        conn.close()

    units = []
    for row in rows:
        service_installed = is_unit_installed(row["service_unit"]) if row["service_unit"] else False
        timer_installed = is_unit_installed(row["timer_unit"]) if row["timer_unit"] else False
        units.append({
            "id": row["id"],
            "name": row["name"],
            "display_name": row["display_name"],
            "service_unit": row["service_unit"],
            "timer_unit": row["timer_unit"],
            "working_directory": row["working_directory"],
            "service_installed": service_installed,
            "timer_installed": timer_installed,
        })
    return units


def _admin_installed() -> bool:
    return is_unit_installed("gearshop-admin.service")


@router.get("/deploy", response_class=HTMLResponse)
def deploy_page(request: Request):
    git_info = _git_status()
    units = _get_unit_status()

    return templates.TemplateResponse("deploy.html", {
        "request": request,
        "git": git_info,
        "units": units,
        "admin_installed": _admin_installed(),
        "deploy_dir": DEPLOY_DIR,
        "csrf_token": request.state.csrf_token,
        "flash_message": None,
    })


def _deploy_response(request: Request, flash: str, flash_type: str = "info"):
    git_info = _git_status()
    units = _get_unit_status()
    return templates.TemplateResponse("deploy.html", {
        "request": request,
        "git": git_info,
        "units": units,
        "admin_installed": _admin_installed(),
        "deploy_dir": DEPLOY_DIR,
        "csrf_token": request.state.csrf_token,
        "flash_message": flash,
        "flash_type": flash_type,
    })


@router.post("/deploy/pull", response_class=HTMLResponse)
def deploy_pull(request: Request, csrf: str = Depends(verify_csrf)):
    ok, output = _git_pull()
    if ok:
        logger.info("Git pull succeeded: %s", output)
        return _deploy_response(request, f"Pull successful: {output}", "success")
    else:
        logger.error("Git pull failed: %s", output)
        return _deploy_response(request, f"Pull failed: {output}", "error")


@router.post("/deploy/install-units/{script_id}", response_class=HTMLResponse)
def install_units(request: Request, script_id: int, csrf: str = Depends(verify_csrf)):
    conn = get_admin_db()
    try:
        row = conn.execute("SELECT * FROM managed_scripts WHERE id = ?", (script_id,)).fetchone()
    finally:
        conn.close()

    if not row:
        return _deploy_response(request, "Script not found.", "error")

    ok, err = install_unit_files(row["working_directory"], row["service_unit"], row["timer_unit"])
    if ok:
        logger.info("Installed systemd units for %s", row["name"])
        return _deploy_response(request, f"Installed systemd units for {row['display_name']}.", "success")
    else:
        logger.error("Failed to install units for %s: %s", row["name"], err)
        return _deploy_response(request, f"Failed: {err}", "error")


@router.post("/deploy/enable-timer/{script_id}", response_class=HTMLResponse)
def enable_timer(request: Request, script_id: int, csrf: str = Depends(verify_csrf)):
    conn = get_admin_db()
    try:
        row = conn.execute("SELECT * FROM managed_scripts WHERE id = ?", (script_id,)).fetchone()
    finally:
        conn.close()

    if not row or not row["timer_unit"]:
        return _deploy_response(request, "Script or timer not found.", "error")

    ok, err = enable_service(row["timer_unit"])
    if ok:
        return _deploy_response(request, f"Timer {row['timer_unit']} enabled and started.", "success")
    else:
        return _deploy_response(request, f"Failed to enable timer: {err}", "error")


@router.post("/deploy/install-admin", response_class=HTMLResponse)
def install_admin(request: Request, csrf: str = Depends(verify_csrf)):
    ok, err = install_admin_service(DEPLOY_DIR)
    if ok:
        return _deploy_response(request, "Admin service installed and enabled.", "success")
    else:
        return _deploy_response(request, f"Failed to install admin service: {err}", "error")


@router.post("/deploy/restart-admin", response_class=HTMLResponse)
def restart_admin(request: Request, csrf: str = Depends(verify_csrf)):
    if not _admin_installed():
        return _deploy_response(
            request,
            "Admin service is not installed yet. Click 'Install Admin Service' first.",
            "error",
        )
    ok, err = restart_admin_service()
    if ok:
        return _deploy_response(
            request,
            "Admin UI is restarting. The page will briefly go down — refresh in a few seconds.",
            "success",
        )
    else:
        return _deploy_response(request, f"Failed to restart admin: {err}", "error")
