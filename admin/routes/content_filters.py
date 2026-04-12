import os
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from admin.db import get_crawler_db, get_crawler_db_rw
from admin.auth import verify_csrf
from admin.routes.helpers import get_script as _get_script

logger = logging.getLogger("admin.content_filters")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)

FILTER_TYPES = {
    "section_strip": "Section strip — removes the matching line and everything below it",
    "title_exclude": "Title exclude — drops any offer whose title matches (offers crawler only)",
}


def _resolve_filter_target(script) -> tuple[str, str]:
    """Return (db_path, table_name) for the script's content filters."""
    name = script["name"] if script else ""
    if "offers" in name:
        return script["db_path"], "offers_content_filters"
    return script["db_path"], "content_filters"


def _get_support_script():
    """Return the support crawler script row."""
    from admin.db import get_admin_db
    conn = get_admin_db()
    try:
        return conn.execute(
            "SELECT * FROM managed_scripts WHERE name = ?", ("rivian-support-crawler",)
        ).fetchone()
    finally:
        conn.close()


def _has_filters_table(db_path: str, table: str = "content_filters") -> bool:
    """Check if the filters table exists in the crawler DB."""
    try:
        conn = get_crawler_db(db_path)
        try:
            conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
            return True
        except Exception:
            return False
        finally:
            conn.close()
    except Exception:
        return False


@router.get("/scripts/{script_id}/content-filters", response_class=HTMLResponse)
def content_filters_page(request: Request, script_id: int):
    script = _get_script(script_id)
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")

    db_path, table = _resolve_filter_target(script)
    filters = []
    table_exists = _has_filters_table(db_path, table)

    if table_exists:
        try:
            conn = get_crawler_db(db_path)
            try:
                filters = conn.execute(
                    f"SELECT * FROM {table} ORDER BY id"
                ).fetchall()
                filters = [dict(r) for r in filters]
            finally:
                conn.close()
        except Exception as e:
            logger.warning("Could not load content filters: %s", e)

    return templates.TemplateResponse("content_filters.html", {
        "request": request,
        "script": script,
        "filters": filters,
        "filter_types": FILTER_TYPES,
        "table_exists": table_exists,
        "csrf_token": request.state.csrf_token,
        "flash_message": None,
    })


@router.post("/scripts/{script_id}/content-filters/add", response_class=HTMLResponse)
def add_content_filter(
    request: Request,
    script_id: int,
    pattern: str = Form(...),
    filter_type: str = Form("section_strip"),
    description: str = Form(""),
    csrf: str = Depends(verify_csrf),
):
    script = _get_script(script_id)
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")

    pattern = pattern.strip()
    if not pattern:
        return _filters_response(request, script, flash="Pattern cannot be empty.", flash_type="error")

    if filter_type not in FILTER_TYPES:
        return _filters_response(request, script, flash="Invalid filter type.", flash_type="error")

    db_path, table = _resolve_filter_target(script)
    try:
        conn = get_crawler_db_rw(db_path)
        try:
            # Check for duplicate pattern
            existing = conn.execute(
                f"SELECT 1 FROM {table} WHERE pattern = ?", (pattern,)
            ).fetchone()
            if existing:
                return _filters_response(
                    request, script,
                    flash=f"A filter with pattern \"{pattern}\" already exists.",
                    flash_type="error",
                )

            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                f"INSERT INTO {table} (pattern, filter_type, enabled, description, created_at) "
                "VALUES (?, ?, 1, ?, ?)",
                (pattern, filter_type, description.strip(), now),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.error("Failed to add content filter: %s", e)
        return _filters_response(request, script, flash=f"Error: {e}", flash_type="error")

    return _filters_response(
        request, script,
        flash=f"Filter \"{pattern}\" added. Changes take effect on next crawl run.",
        flash_type="success",
    )


@router.post("/scripts/{script_id}/content-filters/{filter_id}/toggle", response_class=HTMLResponse)
def toggle_content_filter(
    request: Request,
    script_id: int,
    filter_id: int,
    csrf: str = Depends(verify_csrf),
):
    script = _get_script(script_id)
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")

    db_path, table = _resolve_filter_target(script)
    try:
        conn = get_crawler_db_rw(db_path)
        try:
            row = conn.execute(f"SELECT enabled FROM {table} WHERE id = ?", (filter_id,)).fetchone()
            if not row:
                return _filters_response(request, script, flash="Filter not found.", flash_type="error")

            new_state = 0 if row["enabled"] else 1
            conn.execute(f"UPDATE {table} SET enabled = ? WHERE id = ?", (new_state, filter_id))
            conn.commit()
            label = "enabled" if new_state else "disabled"
        finally:
            conn.close()
    except Exception as e:
        logger.error("Failed to toggle content filter: %s", e)
        return _filters_response(request, script, flash=f"Error: {e}", flash_type="error")

    return _filters_response(
        request, script,
        flash=f"Filter {label}. Changes take effect on next crawl run.",
        flash_type="success",
    )


@router.post("/scripts/{script_id}/content-filters/{filter_id}/delete", response_class=HTMLResponse)
def delete_content_filter(
    request: Request,
    script_id: int,
    filter_id: int,
    csrf: str = Depends(verify_csrf),
):
    script = _get_script(script_id)
    if not script:
        raise HTTPException(status_code=404, detail="Script not found")

    db_path, table = _resolve_filter_target(script)
    try:
        conn = get_crawler_db_rw(db_path)
        try:
            row = conn.execute(f"SELECT pattern FROM {table} WHERE id = ?", (filter_id,)).fetchone()
            if not row:
                return _filters_response(request, script, flash="Filter not found.", flash_type="error")
            pattern = row["pattern"]
            conn.execute(f"DELETE FROM {table} WHERE id = ?", (filter_id,))
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.error("Failed to delete content filter: %s", e)
        return _filters_response(request, script, flash=f"Error: {e}", flash_type="error")

    return _filters_response(
        request, script,
        flash=f"Filter \"{pattern}\" deleted. Changes take effect on next crawl run.",
        flash_type="success",
    )


def _filters_response(request: Request, script, flash: str = None, flash_type: str = "info"):
    db_path, table = _resolve_filter_target(script)
    filters = []
    table_exists = _has_filters_table(db_path, table)

    if table_exists:
        try:
            conn = get_crawler_db(db_path)
            try:
                filters = conn.execute(
                    f"SELECT * FROM {table} ORDER BY id"
                ).fetchall()
                filters = [dict(r) for r in filters]
            finally:
                conn.close()
        except Exception:
            pass

    return templates.TemplateResponse("content_filters.html", {
        "request": request,
        "script": script,
        "filters": filters,
        "filter_types": FILTER_TYPES,
        "table_exists": table_exists,
        "csrf_token": request.state.csrf_token,
        "flash_message": flash,
        "flash_type": flash_type,
    })
