"""Regression tests for new-product detection under marker retention.

The marker retention window introduced a false-positive storm: rows are never
deleted from `products`, so a product removed from the store months ago still
has a row. New-product detection only asked "has no marker older than this
run", which that dead row satisfied once the prune deleted its old markers —
and because a product that is no longer crawled never gains a new marker,
every subsequent run reported it again. 16 long-dead products were emailed as
"new", hourly.
"""

import sqlite3

import pytest

from crawler import prune_crawl_markers
from migrations import run_migrations

# Kept in sync with the query in _process_run().
NEW_PRODUCTS_SQL = """
  SELECT p.handle
  FROM products p
  WHERE EXISTS (
    SELECT 1 FROM crawl_markers cm_now
    WHERE cm_now.product_id = p.product_id AND cm_now.crawled_at = ?
  )
  AND NOT EXISTS (
    SELECT 1 FROM crawl_markers cm_prev
    WHERE cm_prev.product_id = p.product_id AND cm_prev.crawled_at < ?
  )
"""


def _new_products(conn, now):
    return sorted(r[0] for r in conn.execute(NEW_PRODUCTS_SQL, (now, now)))


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    yield conn
    conn.close()


def _add_product(conn, pid, handle):
    conn.execute(
        "INSERT INTO products (product_id, handle, title) VALUES (?,?,?)",
        (pid, handle, handle.title()),
    )


def _mark(conn, run, pid):
    conn.execute("INSERT OR IGNORE INTO crawl_markers (crawled_at, product_id) VALUES (?,?)",
                 (run, pid))


def test_dead_product_is_not_reported_new_after_prune(db):
    """The exact production failure: a product removed from the store long ago,
    whose old markers the retention prune deleted."""
    _add_product(db, 1, "live")
    _add_product(db, 2, "r1t-essentials")     # discontinued
    for r in range(3000):
        _mark(db, f"run-{r:05d}", 1)
        if r < 2000:
            _mark(db, f"run-{r:05d}", 2)
    db.commit()

    prune_crawl_markers(db, retention_runs=720)
    db.commit()

    now = "run-03000"
    _mark(db, now, 1)          # only the live product is crawled
    db.commit()

    assert _new_products(db, now) == []


def test_dead_product_stays_quiet_across_many_runs(db):
    """It repeated every hour, so prove it stays silent run after run."""
    _add_product(db, 1, "live")
    _add_product(db, 2, "dead")
    for r in range(1000):
        _mark(db, f"run-{r:05d}", 1)
        if r < 500:
            _mark(db, f"run-{r:05d}", 2)
    db.commit()
    prune_crawl_markers(db, retention_runs=100)
    db.commit()

    for r in range(1000, 1010):
        now = f"run-{r:05d}"
        _mark(db, now, 1)
        db.commit()
        assert _new_products(db, now) == [], f"dead product resurfaced at {now}"


def test_genuinely_new_product_is_still_reported(db):
    """The fix must not silence real additions."""
    _add_product(db, 1, "existing")
    for r in range(10):
        _mark(db, f"run-{r:05d}", 1)
    db.commit()

    now = "run-00010"
    _add_product(db, 2, "brand-new")
    _mark(db, now, 1)
    _mark(db, now, 2)
    db.commit()

    assert _new_products(db, now) == ["brand-new"]


def test_first_ever_run_reports_everything(db):
    _add_product(db, 1, "a")
    _add_product(db, 2, "b")
    now = "run-00000"
    _mark(db, now, 1)
    _mark(db, now, 2)
    db.commit()
    assert _new_products(db, now) == ["a", "b"]


def test_product_skipped_this_run_is_not_reported_new(db):
    """A 429 makes the crawler skip a product, so it gets no marker this run.
    That must not read as 'new' either."""
    _add_product(db, 1, "ok")
    _add_product(db, 2, "rate-limited")
    for r in range(5):
        _mark(db, f"run-{r:05d}", 1)
        _mark(db, f"run-{r:05d}", 2)
    db.commit()

    now = "run-00005"
    _mark(db, now, 1)          # product 2 skipped by a 429
    db.commit()
    assert _new_products(db, now) == []


def test_returning_product_beyond_retention_reads_as_new(db):
    """Documented behaviour: a product absent longer than the retention window
    and then restocked is reported as new again, because nothing older
    survives to say otherwise."""
    _add_product(db, 1, "seasonal")
    for r in range(50):
        _mark(db, f"run-{r:05d}", 1)
    for r in range(50, 1000):
        _mark(db, f"run-{r:05d}", 999)      # some other product keeps runs flowing
    db.execute("INSERT INTO products (product_id, handle, title) VALUES (999,'filler','Filler')")
    db.commit()
    prune_crawl_markers(db, retention_runs=100)
    db.commit()

    now = "run-01000"
    _mark(db, now, 1)
    db.commit()
    assert _new_products(db, now) == ["seasonal"]


# ---------------------- social summary dedupe ----------------------

def test_summary_dedupe_key_is_stable_and_negative():
    """social_posts is keyed on an INTEGER product_id, so the summary needs a
    synthetic id that cannot collide with a real (positive) Shopify id."""
    from crawler import _summary_dedupe_key

    a = _summary_dedupe_key("2 new items")
    b = _summary_dedupe_key("2 new items")
    c = _summary_dedupe_key("3 new items")
    assert a == b
    assert a != c
    assert a < 0 and c < 0


def test_summary_key_roundtrips_through_social_posts(tmp_path, monkeypatch):
    """The dedupe row must actually be storable and readable back, or the
    summary re-posts every hour exactly as it did in production."""
    import crawler
    from crawler import _summary_dedupe_key

    dbfile = str(tmp_path / "g.db")
    conn = sqlite3.connect(dbfile)
    run_migrations(conn)
    conn.close()
    monkeypatch.setattr(crawler, "DB_PATH", dbfile)

    key = _summary_dedupe_key("📰 Plus 11 new items at the Rivian Gear Shop")
    assert crawler._already_posted_social(key, "summary", "bluesky") is False

    c = sqlite3.connect(dbfile)
    c.execute(
        "INSERT INTO social_posts (product_id, change_type, platform, posted_at, post_ref)"
        " VALUES (?,?,?,?,?)", (key, "summary", "bluesky", "2026-09-01T00:00:00Z", ""),
    )
    c.commit(); c.close()

    assert crawler._already_posted_social(key, "summary", "bluesky") is True
    assert crawler._already_posted_social(key, "summary", "x") is False
