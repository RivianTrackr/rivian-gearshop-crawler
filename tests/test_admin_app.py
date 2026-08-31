"""Smoke tests for the admin UI against the pinned dependency stack.

These exist because upgrading fastapi/starlette to clear the CVEs in
starlette 0.38.x was a breaking change: Starlette 1.0 removed the
`TemplateResponse(name, context)` signature and the `@app.on_event` hook.
Both failures are invisible at import time and only show up when a route is
actually rendered, so every template-rendering page gets exercised here.
"""

import sqlite3

import pytest

pytest.importorskip("httpx", reason="starlette TestClient requires httpx")
from fastapi.testclient import TestClient  # noqa: E402


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Boot the admin app against throwaway databases."""
    import migrations, support_migrations, offers_migrations

    monkeypatch.setenv("ADMIN_DB_PATH", str(tmp_path / "admin.db"))
    monkeypatch.setenv("ADMIN_SECRET_KEY", "test-secret-key")

    g = sqlite3.connect(tmp_path / "gearshop.db")
    migrations.run_migrations(g)
    g.execute("INSERT INTO products VALUES (1,'jacket','Jacket','Rivian','Apparel','https://x/1','','')")
    g.execute("INSERT INTO variants VALUES (11,1,'Small','SKU-S')")
    g.execute("INSERT INTO snapshots (crawled_at,product_id,variant_id,price_cents,compare_at_cents,available)"
              " VALUES ('2026-08-30T10:00:00Z',1,11,12500,15000,1)")
    g.execute("INSERT INTO crawl_stats VALUES ('2026-08-30T10:00:00Z',1)")
    g.commit(); g.close()

    s = sqlite3.connect(tmp_path / "support.db")
    support_migrations.run_migrations(s)
    s.execute("INSERT INTO support_articles (slug,url,title,body_text,body_hash,category,"
              "first_seen_at,last_seen_at,updated_at,removed) VALUES "
              "('a','https://x/a','Charging','body','h','Cat','2026-08-01','2026-08-30','2026-08-30',0)")
    s.execute("INSERT INTO article_snapshots (article_id,crawled_at,title,body_text,body_hash,url)"
              " VALUES (1,'2026-08-30T10:00:00Z','Charging','body','h','https://x/a')")
    s.execute("INSERT INTO support_crawl_stats VALUES ('2026-08-30T10:00:00Z',1)")
    s.commit(); s.close()

    o = sqlite3.connect(tmp_path / "offers.db")
    offers_migrations.run_migrations(o)
    o.execute("INSERT INTO offers (slug,url,title,body_text,body_hash,cta_url,expiration,"
              "first_seen_at,last_seen_at,updated_at,removed) VALUES "
              "('o','https://x/o','Offer','body','h','https://x/c','2026-12-01',"
              "'2026-08-01','2026-08-30','2026-08-30',0)")
    o.execute("INSERT INTO offer_snapshots (offer_id,crawled_at,title,body_text,body_hash,url,cta_url,expiration)"
              " VALUES (1,'2026-08-30T10:00:00Z','Offer','body','h','https://x/o','https://x/c','2026-12-01')")
    o.execute("INSERT INTO offers_crawl_stats VALUES ('2026-08-30T10:00:00Z',1)")
    o.commit(); o.close()

    import importlib
    import admin.config, admin.db, admin.app, admin.routes.auth_routes as ar
    for mod in (admin.config, admin.db, ar, admin.app):
        importlib.reload(mod)
    from admin.auth import hash_password

    with TestClient(admin.app.app, base_url="https://testserver") as c:
        conn = sqlite3.connect(tmp_path / "admin.db")
        conn.execute("UPDATE users SET password_hash=? WHERE username='admin'",
                     (hash_password("s3cret"),))
        for name, db in (("rivian-gearshop-crawler", "gearshop.db"),
                         ("rivian-support-crawler", "support.db"),
                         ("rivian-offers-crawler", "offers.db")):
            conn.execute("UPDATE managed_scripts SET db_path=? WHERE name=?",
                         (str(tmp_path / db), name))
        conn.commit(); conn.close()
        importlib.import_module("admin.routes.auth_routes")._login_attempts.clear()
        c.post("/login", data={"username": "admin", "password": "s3cret"},
               follow_redirects=False)
        yield c


TEMPLATE_PAGES = [
    "/", "/settings", "/settings/global-config", "/deploy",
    "/scripts/1", "/scripts/1/logs", "/scripts/1/notifications", "/scripts/1/config",
    "/scripts/2/content-filters",
    "/data/1/products", "/data/1/products/1", "/data/1/variants/11/history",
    "/data/1/crawl-history",
    "/data/2/articles", "/data/2/articles/1", "/data/2/support-crawl-history",
    "/data/3/offers", "/data/3/offers/1", "/data/3/offers-crawl-history",
]


@pytest.mark.parametrize("path", TEMPLATE_PAGES)
def test_template_pages_render(client, path):
    """Catches the Starlette 1.0 TemplateResponse signature change."""
    assert client.get(path, follow_redirects=False).status_code == 200


def test_lifespan_bootstraps_the_admin_db(client):
    """`@app.on_event` was removed in Starlette 1.0; the lifespan handler must
    still create the schema and seed the default user."""
    assert client.get("/api/dashboard-stats").status_code == 200


def test_unauthenticated_request_redirects_to_login(tmp_path, monkeypatch):
    monkeypatch.setenv("ADMIN_DB_PATH", str(tmp_path / "admin.db"))
    monkeypatch.setenv("ADMIN_SECRET_KEY", "test-secret-key")
    import importlib, admin.config, admin.db, admin.app
    for mod in (admin.config, admin.db, admin.app):
        importlib.reload(mod)
    with TestClient(admin.app.app, base_url="https://testserver") as c:
        r = c.get("/", follow_redirects=False)
        assert r.status_code == 303
        assert r.headers["location"] == "/login"


def test_session_cookie_is_secure_and_httponly(client):
    cookie = client.cookies.jar._cookies  # noqa: SLF001
    r = client.get("/", follow_redirects=False)
    set_cookie = r.headers.get("set-cookie", "")
    assert "Secure" in set_cookie
    assert "HttpOnly" in set_cookie
    assert cookie is not None


def test_csrf_is_enforced_on_post(client):
    r = client.post("/settings/password", data={
        "_csrf": "bogus", "current_password": "s3cret",
        "new_password": "x", "confirm_password": "x",
    })
    assert r.status_code == 403


def test_auth_middleware_reads_path_from_asgi_scope():
    """The middleware must not authorise on `request.url.path`, which Starlette
    rebuilds from the Host header (CVE-2026-48710 / CVE-2026-54282).

    Checked over the parsed AST rather than the raw text so the explanatory
    comment naming the attribute does not count as a use of it.
    """
    import ast
    import inspect
    import textwrap
    import admin.app

    tree = ast.parse(textwrap.dedent(inspect.getsource(admin.app.AuthMiddleware.dispatch)))
    reads_url_path = any(
        isinstance(n, ast.Attribute) and n.attr == "path"
        and isinstance(n.value, ast.Attribute) and n.value.attr == "url"
        for n in ast.walk(tree)
    )
    reads_scope = any(
        isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
        and n.func.attr == "get"
        and isinstance(n.func.value, ast.Attribute) and n.func.value.attr == "scope"
        for n in ast.walk(tree)
    )
    assert not reads_url_path, "AuthMiddleware must not authorise on request.url.path"
    assert reads_scope, "AuthMiddleware should read the path from the ASGI scope"
