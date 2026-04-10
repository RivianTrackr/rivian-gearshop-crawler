from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import os

from admin.db import get_admin_db, get_crawler_db
from admin.systemd import get_service_status, get_timer_active

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    conn = get_admin_db()
    try:
        rows = conn.execute("SELECT * FROM managed_scripts ORDER BY display_name").fetchall()
    finally:
        conn.close()

    scripts = []
    for row in rows:
        status = get_service_status(row["service_unit"], row["timer_unit"])
        timer_active = get_timer_active(row["timer_unit"]) if row["timer_unit"] else False
        is_support = "support" in row["name"]

        # Fetch last crawl stat
        last_count = None
        if row["db_path"] and os.path.exists(row["db_path"]):
            try:
                cdb = get_crawler_db(row["db_path"])
                try:
                    if is_support:
                        r = cdb.execute("SELECT article_count FROM support_crawl_stats ORDER BY run_at DESC LIMIT 1").fetchone()
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
            "status": status,
            "timer_active": timer_active,
            "is_support": is_support,
            "last_count": last_count,
        })

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "scripts": scripts,
        "csrf_token": request.state.csrf_token,
    })
