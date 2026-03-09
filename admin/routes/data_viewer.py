import os
import math

from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from admin.db import get_admin_db, get_crawler_db

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "templates")
)

PAGE_SIZE = 50


def _get_script(script_id: int):
    conn = get_admin_db()
    row = conn.execute("SELECT * FROM managed_scripts WHERE id = ?", (script_id,)).fetchone()
    conn.close()
    return row


@router.get("/data/{script_id}/products", response_class=HTMLResponse)
def products_list(request: Request, script_id: int,
                  page: int = Query(1, ge=1),
                  q: str = Query("")):
    script = _get_script(script_id)
    if not script or not script["db_path"] or not os.path.exists(script["db_path"]):
        return RedirectResponse("/", status_code=303)

    cdb = get_crawler_db(script["db_path"])

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

    product = cdb.execute(
        "SELECT * FROM products WHERE product_id = ?", (product_id,)
    ).fetchone()
    if not product:
        cdb.close()
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

    snapshots = cdb.execute(
        """SELECT s.*, v.title as variant_title, p.title as product_title
           FROM snapshots s
           JOIN variants v ON v.variant_id = s.variant_id
           JOIN products p ON p.product_id = s.product_id
           WHERE s.variant_id = ?
           ORDER BY s.crawled_at DESC
           LIMIT 100""",
        (variant_id,),
    ).fetchall()

    cdb.close()

    return templates.TemplateResponse("data_snapshots.html", {
        "request": request,
        "script": script,
        "variant_id": variant_id,
        "snapshots": snapshots,
        "csrf_token": request.state.csrf_token,
    })


@router.get("/data/{script_id}/crawl-history", response_class=HTMLResponse)
def crawl_history(request: Request, script_id: int, page: int = Query(1, ge=1)):
    script = _get_script(script_id)
    if not script or not script["db_path"] or not os.path.exists(script["db_path"]):
        return RedirectResponse("/", status_code=303)

    cdb = get_crawler_db(script["db_path"])

    count_row = cdb.execute("SELECT COUNT(*) as cnt FROM crawl_stats").fetchone()
    total = count_row["cnt"]
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    offset = (page - 1) * PAGE_SIZE

    stats = cdb.execute(
        "SELECT * FROM crawl_stats ORDER BY run_at DESC LIMIT ? OFFSET ?",
        (PAGE_SIZE, offset),
    ).fetchall()

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
