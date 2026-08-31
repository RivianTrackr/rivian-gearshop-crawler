"""
Database schema versioning and migration runner.

Tracks applied migrations in a `schema_versions` table and runs
pending migrations in order. Each migration is a (version, description, sql) tuple.
"""

import logging
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger("crawler.migrations")

# Schema version tracking table (created before any migrations run)
VERSION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_versions (
    version INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
"""

# Ordered list of migrations. Each entry: (version, description, sql)
# IMPORTANT: Only append new migrations — never modify or remove existing ones.
MIGRATIONS = [
    (
        1,
        "Initial schema: products, variants, snapshots, crawl_markers, crawl_stats, heartbeats, removed_once",
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            handle TEXT NOT NULL,
            title TEXT,
            vendor TEXT,
            product_type TEXT,
            url TEXT,
            created_at TEXT,
            updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS variants (
            variant_id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            title TEXT,
            sku TEXT,
            FOREIGN KEY(product_id) REFERENCES products(product_id)
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            crawled_at TEXT NOT NULL,
            product_id INTEGER NOT NULL,
            variant_id INTEGER NOT NULL,
            price_cents INTEGER,
            compare_at_cents INTEGER,
            available INTEGER NOT NULL,
            FOREIGN KEY(product_id) REFERENCES products(product_id),
            FOREIGN KEY(variant_id) REFERENCES variants(variant_id)
        );

        CREATE TABLE IF NOT EXISTS crawl_markers (
            crawled_at TEXT NOT NULL,
            product_id INTEGER NOT NULL,
            PRIMARY KEY (crawled_at, product_id)
        );

        CREATE TABLE IF NOT EXISTS crawl_stats (
            run_at TEXT PRIMARY KEY,
            product_count INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS heartbeats (
            day_utc TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS removed_once (
            product_id INTEGER PRIMARY KEY,
            first_reported_at TEXT NOT NULL
        );
        """,
    ),
    (
        2,
        "Add crawl_runs table for run history and error tracking",
        """
        CREATE TABLE IF NOT EXISTS crawl_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            product_count INTEGER,
            variants_changed INTEGER,
            new_products INTEGER,
            removed_products INTEGER,
            html_checks INTEGER,
            error_message TEXT,
            duration_seconds REAL
        );
        """,
    ),
    (
        3,
        "Add notification_queue table for retry support",
        """
        CREATE TABLE IF NOT EXISTS notification_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            channel TEXT NOT NULL,
            payload TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            next_retry_at TEXT,
            last_error TEXT,
            status TEXT NOT NULL DEFAULT 'pending'
        );
        """,
    ),
    (
        4,
        "Add support article tables for rivian.com/support crawler",
        """
        CREATE TABLE IF NOT EXISTS support_articles (
            article_id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            slug TEXT NOT NULL,
            title TEXT,
            category TEXT,
            content_hash TEXT,
            first_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS support_article_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER NOT NULL,
            crawled_at TEXT NOT NULL,
            title TEXT,
            category TEXT,
            content_text TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            FOREIGN KEY(article_id) REFERENCES support_articles(article_id)
        );

        CREATE TABLE IF NOT EXISTS support_crawl_markers (
            crawled_at TEXT NOT NULL,
            article_id INTEGER NOT NULL,
            PRIMARY KEY (crawled_at, article_id)
        );

        CREATE TABLE IF NOT EXISTS support_crawl_stats (
            run_at TEXT PRIMARY KEY,
            article_count INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS support_crawl_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            article_count INTEGER,
            articles_changed INTEGER,
            new_articles INTEGER,
            removed_articles INTEGER,
            error_message TEXT,
            duration_seconds REAL
        );

        CREATE TABLE IF NOT EXISTS support_heartbeats (
            day_utc TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS support_removed_once (
            article_id INTEGER PRIMARY KEY,
            first_reported_at TEXT NOT NULL
        );
        """,
    ),
    (
        5,
        "Add social_posts table to de-duplicate social media posts per product/change/platform",
        """
        CREATE TABLE IF NOT EXISTS social_posts (
            product_id INTEGER NOT NULL,
            change_type TEXT NOT NULL,   -- 'new' or 'removed'
            platform TEXT NOT NULL,      -- 'bluesky', 'x', 'threads'
            posted_at TEXT NOT NULL,
            post_ref TEXT,               -- platform post id / URI for audit
            PRIMARY KEY (product_id, change_type, platform)
        );
        """,
    ),
    (
        6,
        "Add performance indexes for snapshot, marker, variant and queue lookups",
        """
        -- Hot path: recent_snapshots_for_variant() runs once per variant per
        -- crawl as `WHERE variant_id=? ORDER BY snapshot_id DESC LIMIT n`.
        -- Unindexed, every one of those is a full scan of `snapshots`, making a
        -- run cost O(variants x snapshots). Benchmarked at 1,200 variants /
        -- 36k snapshots: 721ms -> 12ms for the per-variant lookups in a run.
        CREATE INDEX IF NOT EXISTS idx_snapshots_variant_snapid
            ON snapshots(variant_id, snapshot_id DESC);

        -- export_current_inventory_json(): GROUP BY variant_id + MAX(crawled_at),
        -- then joins back on (variant_id, crawled_at). 19ms -> 6ms.
        CREATE INDEX IF NOT EXISTS idx_snapshots_variant_crawled
            ON snapshots(variant_id, crawled_at DESC);

        -- New/removed-product detection probes crawl_markers by product_id,
        -- which the (crawled_at, product_id) primary key cannot serve.
        CREATE INDEX IF NOT EXISTS idx_crawl_markers_product_crawled
            ON crawl_markers(product_id, crawled_at DESC);

        -- Admin data viewer joins variants by product_id; the FK was unindexed.
        CREATE INDEX IF NOT EXISTS idx_variants_product
            ON variants(product_id);

        -- Per-product stale-handle cleanup: DELETE FROM products WHERE handle=?
        CREATE INDEX IF NOT EXISTS idx_products_handle
            ON products(handle);

        -- Crawl-history pagination in the admin UI.
        CREATE INDEX IF NOT EXISTS idx_crawl_runs_started
            ON crawl_runs(started_at DESC);

        -- Retry-queue scans filter on status and due time.
        CREATE INDEX IF NOT EXISTS idx_notification_queue_pending
            ON notification_queue(status, next_retry_at);
        """,
    ),
]


def get_current_version(conn: sqlite3.Connection) -> int:
    """Return the highest applied migration version, or 0 if none."""
    try:
        row = conn.execute(
            "SELECT MAX(version) as v FROM schema_versions"
        ).fetchone()
        return row[0] or 0 if row else 0
    except sqlite3.OperationalError:
        # Table doesn't exist yet
        return 0


def run_migrations(conn: sqlite3.Connection) -> list[int]:
    """
    Run all pending migrations on the given connection.
    Returns list of newly applied version numbers.
    """
    # Ensure version tracking table exists
    conn.executescript(VERSION_TABLE_SQL)

    current = get_current_version(conn)
    applied = []

    for version, description, sql in MIGRATIONS:
        if version <= current:
            continue

        logger.info("Applying migration v%d: %s", version, description)
        try:
            conn.executescript(sql)
            conn.execute(
                "INSERT INTO schema_versions (version, description, applied_at) VALUES (?, ?, ?)",
                (version, description, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            applied.append(version)
            logger.info("Migration v%d applied successfully", version)
        except Exception as e:
            logger.error("Migration v%d failed: %s", version, e)
            conn.rollback()
            raise RuntimeError(f"Migration v{version} failed: {e}") from e

    if not applied:
        logger.info("Database schema is up to date (v%d)", current)
    else:
        logger.info("Applied %d migration(s), now at v%d", len(applied), applied[-1])

    return applied


def check_schema_status(conn: sqlite3.Connection) -> dict:
    """Return schema status info for health checks."""
    current = get_current_version(conn)
    latest = MIGRATIONS[-1][0] if MIGRATIONS else 0
    return {
        "current_version": current,
        "latest_version": latest,
        "up_to_date": current >= latest,
        "pending_migrations": latest - current,
    }
