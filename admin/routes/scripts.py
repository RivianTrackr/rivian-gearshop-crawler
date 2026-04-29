from fastapi import APIRouter, Request, Query, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import os

from admin.auth import verify_csrf

from admin.db import get_crawler_db
from admin.dbops import (
    check_lock_status, find_lock_holders, force_unlock,
    get_db_files_info, wal_checkpoint,
)
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


def _render_detail(
    request: Request,
    script_id: int,
    lines: int = 100,
    flash: str | None = None,
    flash_type: str = "info",
) -> HTMLResponse:
    """Render the script detail page. Shared by the GET handler and the
    DB-health POST handlers so they can show a flash message on the same view."""
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)

    status = get_service_status(script["service_unit"], script["timer_unit"])
    timer_active = get_timer_active(script["timer_unit"]) if script["timer_unit"] else False
    logs = get_journal_logs(script["service_unit"], lines=lines)

    # Get crawl stats if DB exists (gearshop uses crawl_stats, support uses support_crawl_stats, offers uses offers_crawl_stats)
    crawl_stats = []
    if "support" in script["name"]:
        db_type = "support"
    elif "offers" in script["name"]:
        db_type = "offers"
    else:
        db_type = "gearshop"
    if script["db_path"] and os.path.exists(script["db_path"]):
        cdb = get_crawler_db(script["db_path"])
        try:
            if db_type == "support":
                crawl_stats = cdb.execute(
                    "SELECT run_at, article_count AS item_count FROM support_crawl_stats ORDER BY run_at DESC LIMIT ?",
                    (CRAWL_STATS_LIMIT,)
                ).fetchall()
            elif db_type == "offers":
                crawl_stats = cdb.execute(
                    "SELECT run_at, offer_count AS item_count FROM offers_crawl_stats ORDER BY run_at DESC LIMIT ?",
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

    # DB Health: file sizes, lock probe, lsof holders. All best-effort —
    # any failure is shown in the panel, never raised.
    db_files = get_db_files_info(script["db_path"]) if script["db_path"] else None
    lock_status = check_lock_status(script["db_path"]) if script["db_path"] else None
    lock_holders = find_lock_holders(script["db_path"]) if script["db_path"] else None

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
        "db_files": db_files,
        "lock_status": lock_status,
        "lock_holders": lock_holders,
        "flash_message": flash,
        "flash_type": flash_type,
        "csrf_token": request.state.csrf_token,
    })


@router.get("/scripts/{script_id}", response_class=HTMLResponse)
def script_detail(request: Request, script_id: int, lines: int = Query(100, ge=10, le=1000)):
    return _render_detail(request, script_id, lines=lines)


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


@router.post("/scripts/{script_id}/db-checkpoint", response_class=HTMLResponse)
def script_db_checkpoint(request: Request, script_id: int, csrf: str = Depends(verify_csrf)):
    """Run PRAGMA wal_checkpoint(TRUNCATE) on the crawler's DB. Safe anytime."""
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)
    if not script["db_path"]:
        return _render_detail(
            request, script_id,
            flash="No DB path configured for this script.",
            flash_type="error",
        )

    ok, info = wal_checkpoint(script["db_path"], mode="TRUNCATE")
    if ok:
        msg = (
            f"Checkpoint complete: {info['checkpointed_pages']} pages flushed, "
            f"{info['log_pages']} remaining in WAL"
        )
        if info["busy"]:
            msg += " (another connection was busy — partial checkpoint)"
            flash_type = "warning"
        else:
            flash_type = "success"
    else:
        msg = f"Checkpoint failed: {info.get('error', 'unknown error')}"
        flash_type = "error"
    return _render_detail(request, script_id, flash=msg, flash_type=flash_type)


@router.post("/scripts/{script_id}/db-force-unlock", response_class=HTMLResponse)
def script_db_force_unlock(
    request: Request,
    script_id: int,
    csrf: str = Depends(verify_csrf),
):
    """Stop service + timer, wait for exit, then run a TRUNCATE checkpoint.

    Heavy-handed recovery for when a checkpoint won't take because something
    is still holding the WAL. Caller must re-enable the timer manually after.
    """
    script = _get_script(script_id)
    if not script:
        return RedirectResponse("/", status_code=303)
    if not script["db_path"] or not script["service_unit"]:
        return _render_detail(
            request, script_id,
            flash="DB path or service unit not configured for this script.",
            flash_type="error",
        )

    ok, summary = force_unlock(
        db_path=script["db_path"],
        service_unit=script["service_unit"],
        timer_unit=script["timer_unit"],
    )
    flash = (
        f"Force unlock {'succeeded' if ok else 'completed with warnings'}: {summary}. "
        "Re-enable the timer below if you want scheduled runs to resume."
    )
    return _render_detail(
        request, script_id,
        flash=flash,
        flash_type="success" if ok else "warning",
    )


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
