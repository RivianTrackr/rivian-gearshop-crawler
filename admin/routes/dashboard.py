from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import os

from admin.db import get_admin_db
from admin.systemd import get_service_status, get_timer_active

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    conn = get_admin_db()
    rows = conn.execute("SELECT * FROM managed_scripts ORDER BY display_name").fetchall()
    conn.close()

    scripts = []
    for row in rows:
        status = get_service_status(row["service_unit"], row["timer_unit"])
        timer_active = get_timer_active(row["timer_unit"]) if row["timer_unit"] else False
        scripts.append({
            "id": row["id"],
            "name": row["name"],
            "display_name": row["display_name"],
            "description": row["description"],
            "status": status,
            "timer_active": timer_active,
        })

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "scripts": scripts,
        "csrf_token": request.state.csrf_token,
    })
