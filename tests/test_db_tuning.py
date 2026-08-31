"""Tests for shared connection tuning, index coverage, and marker pruning."""

import sqlite3

import pytest

import dbtune


# ---------------------- dbtune ----------------------

def test_pragmas_applied_to_read_write_connection(tmp_db):
    conn = sqlite3.connect(tmp_db)
    dbtune.apply_connection_pragmas(conn)
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == dbtune.BUSY_TIMEOUT_MS
    assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    # synchronous NORMAL == 1
    assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1
    conn.close()


def test_foreign_keys_left_off(tmp_db):
    """Enforcing FKs would break crawler.py's stale-handle DELETE."""
    conn = sqlite3.connect(tmp_db)
    dbtune.apply_connection_pragmas(conn)
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 0
    conn.close()


def test_read_only_connection_still_gets_busy_timeout(tmp_db):
    sqlite3.connect(tmp_db).close()
    conn = sqlite3.connect(f"file:{tmp_db}?mode=ro", uri=True)
    dbtune.apply_connection_pragmas(conn, read_only=True)
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == dbtune.BUSY_TIMEOUT_MS
    conn.close()


def test_optimize_and_analyze_never_raise(tmp_db):
    conn = sqlite3.connect(tmp_db)
    conn.execute("CREATE TABLE t (a INTEGER)")
    dbtune.analyze(conn)
    dbtune.optimize(conn)
    conn.close()


def test_pragma_failure_is_swallowed():
    """A pragma that cannot apply must not take down a crawl run."""
    conn = sqlite3.connect(":memory:")
    conn.close()  # now every statement raises ProgrammingError/Error
    dbtune._try(conn, "PRAGMA busy_timeout=1000")  # must not raise


# ---------------------- index coverage ----------------------

def _plan(conn, sql, params=()):
    return " ".join(r[-1] for r in conn.execute("EXPLAIN QUERY PLAN " + sql, params))


def test_variant_snapshot_lookup_uses_index(crawler_db):
    """The per-variant lookup runs once per variant per crawl; a SCAN here is
    what made a run cost O(variants x snapshots)."""
    plan = _plan(
        crawler_db,
        "SELECT * FROM snapshots WHERE variant_id=? ORDER BY snapshot_id DESC LIMIT 5",
        (1,),
    )
    assert "SCAN snapshots" not in plan
    assert "idx_snapshots_variant_snapid" in plan


def test_crawl_marker_product_probe_uses_index(crawler_db):
    plan = _plan(
        crawler_db,
        "SELECT 1 FROM crawl_markers WHERE product_id=? AND crawled_at < ?",
        (1, "z"),
    )
    assert "idx_crawl_markers_product_crawled" in plan


def test_expected_indexes_exist(crawler_db):
    names = {
        r[0] for r in crawler_db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        )
    }
    assert {
        "idx_snapshots_variant_snapid",
        "idx_snapshots_variant_crawled",
        "idx_crawl_markers_product_crawled",
        "idx_variants_product",
        "idx_products_handle",
        "idx_crawl_runs_started",
        "idx_notification_queue_pending",
    } <= names


def test_support_indexes_exist(tmp_db):
    from support_migrations import run_migrations as run_support

    conn = sqlite3.connect(tmp_db)
    run_support(conn)
    names = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        )
    }
    assert {
        "idx_article_snapshots_article_id",
        "idx_support_crawl_markers_article",
        "idx_support_articles_updated",
    } <= names
    conn.close()


def test_offers_indexes_exist(tmp_db):
    from offers_migrations import run_migrations as run_offers

    conn = sqlite3.connect(tmp_db)
    run_offers(conn)
    names = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        )
    }
    assert {
        "idx_offer_snapshots_offer_id",
        "idx_offers_crawl_markers_offer",
        "idx_offers_updated",
    } <= names
    conn.close()


# ---------------------- crawl marker pruning ----------------------

def _seed_markers(conn, runs, products=3):
    for r in range(runs):
        for p in range(1, products + 1):
            conn.execute(
                "INSERT OR IGNORE INTO crawl_markers (crawled_at, product_id) VALUES (?,?)",
                (f"2026-01-01T{r:04d}:00Z", p),
            )
    conn.commit()


def test_prune_keeps_newest_n_runs(crawler_db):
    from crawler import prune_crawl_markers

    _seed_markers(crawler_db, runs=20, products=3)
    deleted = prune_crawl_markers(crawler_db, retention_runs=5)

    assert deleted == 15 * 3
    remaining = {r[0] for r in crawler_db.execute("SELECT DISTINCT crawled_at FROM crawl_markers")}
    assert remaining == {f"2026-01-01T{r:04d}:00Z" for r in range(15, 20)}


def test_prune_is_a_noop_below_the_retention_window(crawler_db):
    from crawler import prune_crawl_markers

    _seed_markers(crawler_db, runs=4, products=2)
    assert prune_crawl_markers(crawler_db, retention_runs=10) == 0
    assert crawler_db.execute("SELECT COUNT(*) FROM crawl_markers").fetchone()[0] == 8


def test_prune_on_empty_table(crawler_db):
    from crawler import prune_crawl_markers

    assert prune_crawl_markers(crawler_db, retention_runs=5) == 0


@pytest.mark.parametrize("requested", [0, 1, 2, -5])
def test_prune_floors_retention_at_three_runs(crawler_db, requested):
    """Removal detection compares the current run against the two before it,
    so the window can never shrink below three."""
    from crawler import prune_crawl_markers

    _seed_markers(crawler_db, runs=10, products=1)
    prune_crawl_markers(crawler_db, retention_runs=requested)
    kept = crawler_db.execute("SELECT COUNT(DISTINCT crawled_at) FROM crawl_markers").fetchone()[0]
    assert kept == 3


def test_pruning_preserves_new_product_detection(crawler_db):
    """A long-present product must not resurface as 'new' after a prune."""
    from crawler import prune_crawl_markers

    crawler_db.execute(
        "INSERT INTO products (product_id, handle, title) VALUES (1, 'h', 'Old Product')"
    )
    _seed_markers(crawler_db, runs=30, products=1)
    prune_crawl_markers(crawler_db, retention_runs=5)

    current = "2026-01-01T0029:00Z"
    new_products = crawler_db.execute(
        """SELECT p.product_id FROM products p
           WHERE NOT EXISTS (SELECT 1 FROM crawl_markers cm
                             WHERE cm.product_id = p.product_id AND cm.crawled_at < ?)""",
        (current,),
    ).fetchall()
    assert new_products == []
