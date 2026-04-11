from fastapi import APIRouter, Request, Query, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import os

from admin.auth import verify_csrf

from admin.db import get_crawler_db
from admin.systemd import (
    get_service_status, get_timer_active,
    start_service, stop_service, get_journal_logs,
    enable_service, disable_service, is_unit_installed,
)
from admin.routes.helpers import get_script as _get_script

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)

CRAWL_STATS_LIMIT = 50


@router.get("/scripts/{script_id}", response_class=HTMLResponse)
def script_detail(request: Request, script_id: int, lines: int = Query(100, ge=10, le=1000)):
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)

    status = get_service_status(script["service_unit"], script["timer_unit"])
    timer_active = get_timer_active(script["timer_unit"]) if script["timer_unit"] else False
    logs = get_journal_logs(script["service_unit"], lines=lines)

    # Get crawl stats if DB exists (gearshop uses crawl_stats, support uses support_crawl_stats)
    crawl_stats = []
    db_type = "support" if "support" in script["name"] else "gearshop"
    if script["db_path"] and os.path.exists(script["db_path"]):
        cdb = get_crawler_db(script["db_path"])
        try:
            if db_type == "support":
                crawl_stats = cdb.execute(
                    "SELECT run_at, article_count AS item_count FROM support_crawl_stats ORDER BY run_at DESC LIMIT ?",
                    (CRAWL_STATS_LIMIT,)
                ).fetchall()
            else:
                crawl_stats = cdb.execute(
                    "SELECT run_at, product_count AS item_count FROM crawl_stats ORDER BY run_at DESC LIMIT ?",
                    (CRAWL_STATS_LIMIT,)
                ).fetchall()
        except Exception:
            pass
        finally:
            cdb.close()

    units_installed = is_unit_installed(script["service_unit"])

    return templates.TemplateResponse("script_detail.html", {
        "request": request,
        "script": script,
        "status": status,
        "timer_active": timer_active,
        "units_installed": units_installed,
        "logs": logs,
        "log_lines": lines,
        "crawl_stats": crawl_stats,
        "db_type": db_type,
        "csrf_token": request.state.csrf_token,
    })


@router.post("/scripts/{script_id}/start")
def script_start(request: Request, script_id: int, csrf: str = Depends(verify_csrf)):
    script = _get_script(script_id)
    if script:
        start_service(script["service_unit"])
    return RedirectResponse(f"/scripts/{script_id}", status_code=303)


@router.post("/scripts/{script_id}/stop")
def script_stop(request: Request, script_id: int, csrf: str = Depends(verify_csrf)):
    script = _get_script(script_id)
    if script and script["timer_unit"]:
        stop_service(script["timer_unit"])
    return RedirectResponse(f"/scripts/{script_id}", status_code=303)


@router.post("/scripts/{script_id}/restart")
def script_restart(request: Request, script_id: int, csrf: str = Depends(verify_csrf)):
    script = _get_script(script_id)
    if script:
        if script["timer_unit"]:
            stop_service(script["timer_unit"])
        start_service(script["service_unit"])
        if script["timer_unit"]:
            start_service(script["timer_unit"])
    return RedirectResponse(f"/scripts/{script_id}", status_code=303)


@router.post("/scripts/{script_id}/enable-timer")
def script_enable_timer(request: Request, script_id: int, csrf: str = Depends(verify_csrf)):
    script = _get_script(script_id)
    if script and script["timer_unit"]:
        enable_service(script["timer_unit"])
    return RedirectResponse(f"/scripts/{script_id}", status_code=303)


@router.post("/scripts/{script_id}/disable-timer")
def script_disable_timer(request: Request, script_id: int, csrf: str = Depends(verify_csrf)):
    script = _get_script(script_id)
    if script and script["timer_unit"]:
        disable_service(script["timer_unit"])
    return RedirectResponse(f"/scripts/{script_id}", status_code=303)


@router.get("/scripts/{script_id}/logs", response_class=HTMLResponse)
def script_logs(request: Request, script_id: int,
                lines: int = Query(200, ge=10, le=2000),
                since: str = Query(None)):
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)

    logs = get_journal_logs(script["service_unit"], lines=lines, since=since)

    return templates.TemplateResponse("script_logs.html", {
        "request": request,
        "script": script,
        "logs": logs,
        "log_lines": lines,
        "since": since,
        "csrf_token": request.state.csrf_token,
    })
