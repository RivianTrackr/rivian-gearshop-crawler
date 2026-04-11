"""
Database schema versioning and migration runner for the Support Article Crawler.

Tracks applied migrations in a `schema_versions` table and runs
pending migrations in order. Each migration is a (version, description, sql) tuple.
"""

import logging
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger("support_crawler.migrations")

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
        "Initial support schema: articles, snapshots, crawl_markers, crawl_stats, crawl_runs, heartbeats, removed_once",
        """
        CREATE TABLE IF NOT EXISTS support_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            body_text TEXT NOT NULL,
            body_hash TEXT NOT NULL,
            category TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            removed INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS article_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER NOT NULL,
            crawled_at TEXT NOT NULL,
            title TEXT NOT NULL,
            body_text TEXT NOT NULL,
            body_hash TEXT NOT NULL,
            url TEXT NOT NULL,
            FOREIGN KEY(article_id) REFERENCES support_articles(id)
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_article_crawled
            ON article_snapshots(article_id, crawled_at DESC);

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
            new_articles INTEGER,
            removed_articles INTEGER,
            title_changes INTEGER,
            body_changes INTEGER,
            url_changes INTEGER,
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

        CREATE TABLE IF NOT EXISTS support_notification_queue (
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
        5,
        "Add content_filters table for stripping noisy sections before diff comparison",
        """
        CREATE TABLE IF NOT EXISTS content_filters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern TEXT NOT NULL,
            filter_type TEXT NOT NULL DEFAULT 'section_strip',
            enabled INTEGER NOT NULL DEFAULT 1,
            description TEXT,
            created_at TEXT NOT NULL
        );

        INSERT INTO content_filters (pattern, filter_type, enabled, description, created_at)
        VALUES ('Related articles', 'section_strip', 1,
                'Strips the Related articles section and everything below it from article body before comparison',
                datetime('now'));
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
        return 0


def run_migrations(conn: sqlite3.Connection) -> list[int]:
    """
    Run all pending migrations on the given connection.
    Returns list of newly applied version numbers.
    """
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
        logger.info("Support database schema is up to date (v%d)", current)
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
