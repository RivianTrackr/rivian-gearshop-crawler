from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import os
import shutil

from dataclasses import asdict, is_dataclass

from admin.db import get_admin_db, get_crawler_db
from admin.systemd import get_service_status, get_timer_active

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)


def _get_system_stats() -> dict:
    """Get CPU, memory, and disk usage."""
    stats = {}

    # Memory
    try:
        with open("/proc/meminfo") as f:
            meminfo = {}
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    meminfo[parts[0].rstrip(":")] = int(parts[1])
            total = meminfo.get("MemTotal", 0)
            available = meminfo.get("MemAvailable", 0)
            used = total - available
            stats["mem_total_mb"] = round(total / 1024)
            stats["mem_used_mb"] = round(used / 1024)
            stats["mem_pct"] = round(used / total * 100) if total else 0
    except Exception:
        stats["mem_total_mb"] = 0
        stats["mem_used_mb"] = 0
        stats["mem_pct"] = 0

    # CPU load (1-min average)
    try:
        with open("/proc/loadavg") as f:
            parts = f.read().split()
            stats["load_1m"] = parts[0]
            stats["load_5m"] = parts[1]
            stats["load_15m"] = parts[2]
    except Exception:
        stats["load_1m"] = "?"
        stats["load_5m"] = "?"
        stats["load_15m"] = "?"

    # Disk
    try:
        usage = shutil.disk_usage("/opt")
        stats["disk_total_gb"] = round(usage.total / (1024 ** 3), 1)
        stats["disk_used_gb"] = round(usage.used / (1024 ** 3), 1)
        stats["disk_pct"] = round(usage.used / usage.total * 100) if usage.total else 0
    except Exception:
        stats["disk_total_gb"] = 0
        stats["disk_used_gb"] = 0
        stats["disk_pct"] = 0

    # Uptime
    try:
        with open("/proc/uptime") as f:
            secs = int(float(f.read().split()[0]))
            days, rem = divmod(secs, 86400)
            hours, rem = divmod(rem, 3600)
            mins = rem // 60
            stats["uptime"] = f"{days}d {hours}h {mins}m"
    except Exception:
        stats["uptime"] = "?"

    return stats


def _build_dashboard_data() -> dict:
    """Compute everything the dashboard displays. Shared by the HTML and JSON routes."""
    conn = get_admin_db()
    try:
        rows = conn.execute("SELECT * FROM managed_scripts ORDER BY display_name").fetchall()
    finally:
        conn.close()

    scripts = []
    for row in rows:
        status = get_service_status(row["service_unit"], row["timer_unit"])
        timer_active = get_timer_active(row["timer_unit"]) if row["timer_unit"] else False
        if "support" in row["name"]:
            script_type = "support"
        elif "offers" in row["name"]:
            script_type = "offers"
        else:
            script_type = "gearshop"

        last_count = None
        if row["db_path"] and os.path.exists(row["db_path"]):
            try:
                cdb = get_crawler_db(row["db_path"])
                try:
                    if script_type == "support":
                        r = cdb.execute("SELECT article_count FROM support_crawl_stats ORDER BY run_at DESC LIMIT 1").fetchone()
                    elif script_type == "offers":
                        r = cdb.execute("SELECT offer_count FROM offers_crawl_stats ORDER BY run_at DESC LIMIT 1").fetchone()
                    else:
                        r = cdb.execute("SELECT product_count FROM crawl_stats ORDER BY run_at DESC LIMIT 1").fetchone()
                    if r:
                        last_count = r[0]
                finally:
                    cdb.close()
            except Exception:
                pass

        scripts.append({
            "id": row["id"],
            "name": row["name"],
            "display_name": row["display_name"],
            "description": row["description"],
            "status": asdict(status) if is_dataclass(status) else (status or {}),
            "timer_active": timer_active,
            "script_type": script_type,
            "last_count": last_count,
        })

    return {"scripts": scripts, "system": _get_system_stats()}


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    data = _build_dashboard_data()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "scripts": data["scripts"],
        "system": data["system"],
        "csrf_token": request.state.csrf_token,
    })


@router.get("/api/dashboard-stats")
def dashboard_stats_json():
    return JSONResponse(_build_dashboard_data())
