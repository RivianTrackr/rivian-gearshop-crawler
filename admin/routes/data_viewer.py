import os
import io
import csv
import json
import math

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from admin.db import get_crawler_db
from admin.routes.helpers import get_script as _get_script

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)

PAGE_SIZE = 50
VARIANT_HISTORY_LIMIT = 100

# SQL for full product+variant+latest snapshot export
_EXPORT_SQL = """
SELECT
  p.product_id, p.handle, p.title AS product_title, p.vendor, p.product_type, p.url,
  v.variant_id, v.title AS variant_title, v.sku,
  s.price_cents, s.compare_at_cents, s.available, s.crawled_at AS last_seen
FROM variants v
JOIN products p ON p.product_id = v.product_id
LEFT JOIN snapshots s ON s.variant_id = v.variant_id
  AND s.crawled_at = (SELECT MAX(s2.crawled_at) FROM snapshots s2 WHERE s2.variant_id = v.variant_id)
ORDER BY p.title, v.title
"""


def _export_rows(db_path: str) -> list[dict]:
    """Fetch all products+variants with latest snapshot as dicts."""
    cdb = get_crawler_db(db_path)
    try:
        rows = cdb.execute(_EXPORT_SQL).fetchall()
        return [
            {
                "product_id": r["product_id"],
                "handle": r["handle"],
                "product_title": r["product_title"],
                "vendor": r["vendor"],
                "product_type": r["product_type"],
                "url": r["url"],
                "variant_id": r["variant_id"],
                "variant_title": r["variant_title"],
                "sku": r["sku"],
                "price": round(r["price_cents"] / 100, 2) if r["price_cents"] is not None else None,
                "compare_at_price": round(r["compare_at_cents"] / 100, 2) if r["compare_at_cents"] is not None else None,
                "available": bool(r["available"]) if r["available"] is not None else None,
                "last_seen": r["last_seen"],
            }
            for r in rows
        ]
    finally:
        cdb.close()


@router.get("/data/{script_id}/export.json")
def export_json(script_id: int):
    script = _get_script(script_id)
    if not script or not script["db_path"] or not os.path.exists(script["db_path"]):
        return RedirectResponse("/", status_code=303)

    rows = _export_rows(script["db_path"])
    content = json.dumps({"count": len(rows), "items": rows}, indent=2, ensure_ascii=False)
    return StreamingResponse(
        io.BytesIO(content.encode("utf-8")),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="gearshop-export.json"'},
    )


@router.get("/data/{script_id}/export.csv")
def export_csv(script_id: int):
    script = _get_script(script_id)
    if not script or not script["db_path"] or not os.path.exists(script["db_path"]):
        return RedirectResponse("/", status_code=303)

    rows = _export_rows(script["db_path"])
    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="gearshop-export.csv"'},
    )


@router.get("/data/{script_id}/products", response_class=HTMLResponse)
def products_list(request: Request, script_id: int,
                  page: int = Query(1, ge=1),
                  q: str = Query("")):
    script = _get_script(script_id)
    if not script or not script["db_path"] or not os.path.exists(script["db_path"]):
        return RedirectResponse("/", status_code=303)

    cdb = get_crawler_db(script["db_path"])
    try:
        # Count total
        if q:
            count_row = cdb.execute(
                "SELECT COUNT(*) as cnt FROM products WHERE title LIKE ? OR handle LIKE ?",
                (f"%{q}%", f"%{q}%"),
            ).fetchone()
        else:
            count_row = cdb.execute("SELECT COUNT(*) as cnt FROM products").fetchone()

        total = count_row["cnt"]
        total_pages = max(1, math.ceil(total / PAGE_SIZE))
        offset = (page - 1) * PAGE_SIZE

        if q:
            products = cdb.execute(
                """SELECT p.*, COUNT(v.variant_id) as variant_count
                   FROM products p
                   LEFT JOIN variants v ON v.product_id = p.product_id
                   WHERE p.title LIKE ? OR p.handle LIKE ?
                   GROUP BY p.product_id
                   ORDER BY p.title
                   LIMIT ? OFFSET ?""",
                (f"%{q}%", f"%{q}%", PAGE_SIZE, offset),
            ).fetchall()
        else:
            products = cdb.execute(
                """SELECT p.*, COUNT(v.variant_id) as variant_count
                   FROM products p
                   LEFT JOIN variants v ON v.product_id = p.product_id
                   GROUP BY p.product_id
                   ORDER BY p.title
                   LIMIT ? OFFSET ?""",
                (PAGE_SIZE, offset),
            ).fetchall()
    finally:
        cdb.close()

    return templates.TemplateResponse("data_products.html", {
        "request": request,
        "script": script,
        "products": products,
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "q": q,
        "csrf_token": request.state.csrf_token,
    })


@router.get("/data/{script_id}/products/{product_id}", response_class=HTMLResponse)
def product_detail(request: Request, script_id: int, product_id: int):
    script = _get_script(script_id)
    if not script or not script["db_path"] or not os.path.exists(script["db_path"]):
        return RedirectResponse("/", status_code=303)

    cdb = get_crawler_db(script["db_path"])
    try:
        product = cdb.execute(
            "SELECT * FROM products WHERE product_id = ?", (product_id,)
        ).fetchone()
        if not product:
            return RedirectResponse(f"/data/{script_id}/products", status_code=303)

        # Get variants with latest snapshot
        variants = cdb.execute(
            """SELECT v.*,
                      s.price_cents, s.compare_at_cents, s.available, s.crawled_at
               FROM variants v
               LEFT JOIN snapshots s ON s.variant_id = v.variant_id
                 AND s.crawled_at = (
                   SELECT MAX(s2.crawled_at) FROM snapshots s2
                   WHERE s2.variant_id = v.variant_id
                 )
               WHERE v.product_id = ?
               ORDER BY v.title""",
            (product_id,),
        ).fetchall()
    finally:
        cdb.close()

    return templates.TemplateResponse("data_viewer.html", {
        "request": request,
        "script": script,
        "product": product,
        "variants": variants,
        "csrf_token": request.state.csrf_token,
    })


@router.get("/data/{script_id}/variants/{variant_id}/history", response_class=HTMLResponse)
def variant_history(request: Request, script_id: int, variant_id: int):
    script = _get_script(script_id)
    if not script or not script["db_path"] or not os.path.exists(script["db_path"]):
        return RedirectResponse("/", status_code=303)

    cdb = get_crawler_db(script["db_path"])
    try:
        snapshots = cdb.execute(
            """SELECT s.*, v.title as variant_title, p.title as product_title
               FROM snapshots s
               JOIN variants v ON v.variant_id = s.variant_id
               JOIN products p ON p.product_id = s.product_id
               WHERE s.variant_id = ?
               ORDER BY s.crawled_at DESC
               LIMIT ?""",
            (variant_id, VARIANT_HISTORY_LIMIT),
        ).fetchall()
    finally:
        cdb.close()

    # Prepare chart data (chronological order for charts)
    chart_data = []
    for s in reversed(snapshots):
        chart_data.append({
            "date": s["crawled_at"],
            "price": round(s["price_cents"] / 100, 2) if s["price_cents"] is not None else None,
            "available": bool(s["available"]) if s["available"] is not None else None,
        })

    return templates.TemplateResponse("data_snapshots.html", {
        "request": request,
        "script": script,
        "variant_id": variant_id,
        "snapshots": snapshots,
        "chart_data_json": json.dumps(chart_data),
        "csrf_token": request.state.csrf_token,
    })


@router.get("/data/{script_id}/crawl-history", response_class=HTMLResponse)
def crawl_history(request: Request, script_id: int, page: int = Query(1, ge=1)):
    script = _get_script(script_id)
    if not script or not script["db_path"] or not os.path.exists(script["db_path"]):
        return RedirectResponse("/", status_code=303)

    cdb = get_crawler_db(script["db_path"])
    try:
        count_row = cdb.execute("SELECT COUNT(*) as cnt FROM crawl_stats").fetchone()
        total = count_row["cnt"]
        total_pages = max(1, math.ceil(total / PAGE_SIZE))
        offset = (page - 1) * PAGE_SIZE

        stats = cdb.execute(
            "SELECT * FROM crawl_stats ORDER BY run_at DESC LIMIT ? OFFSET ?",
            (PAGE_SIZE, offset),
        ).fetchall()
    finally:
        cdb.close()

    return templates.TemplateResponse("crawl_history.html", {
        "request": request,
        "script": script,
        "stats": stats,
        "page": page,
        "total_pages": total_pages,
        "total": total,
        "csrf_token": request.state.csrf_token,
    })
