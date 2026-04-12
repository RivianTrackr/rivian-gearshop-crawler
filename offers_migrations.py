"""
Database schema versioning and migration runner for the Offers Crawler.

Tracks applied migrations in an `offers_schema_versions` table and runs
pending migrations in order. Each migration is a (version, description, sql) tuple.
"""

import logging
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger("offers_crawler.migrations")

VERSION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS offers_schema_versions (
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
        "Initial offers schema: offers, snapshots, crawl_markers, crawl_stats, crawl_runs, heartbeats, removed_once, content_filters",
        """
        CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            body_text TEXT NOT NULL,
            body_hash TEXT NOT NULL,
            cta_url TEXT,
            expiration TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            removed INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS offer_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            offer_id INTEGER NOT NULL,
            crawled_at TEXT NOT NULL,
            title TEXT NOT NULL,
            body_text TEXT NOT NULL,
            body_hash TEXT NOT NULL,
            url TEXT NOT NULL,
            cta_url TEXT,
            expiration TEXT,
            FOREIGN KEY(offer_id) REFERENCES offers(id)
        );

        CREATE INDEX IF NOT EXISTS idx_offer_snapshots_offer_crawled
            ON offer_snapshots(offer_id, crawled_at DESC);

        CREATE TABLE IF NOT EXISTS offers_crawl_markers (
            crawled_at TEXT NOT NULL,
            offer_id INTEGER NOT NULL,
            PRIMARY KEY (crawled_at, offer_id)
        );

        CREATE TABLE IF NOT EXISTS offers_crawl_stats (
            run_at TEXT PRIMARY KEY,
            offer_count INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS offers_crawl_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            offer_count INTEGER,
            new_offers INTEGER,
            removed_offers INTEGER,
            title_changes INTEGER,
            body_changes INTEGER,
            url_changes INTEGER,
            error_message TEXT,
            duration_seconds REAL
        );

        CREATE TABLE IF NOT EXISTS offers_heartbeats (
            day_utc TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS offers_removed_once (
            offer_id INTEGER PRIMARY KEY,
            first_reported_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS offers_content_filters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern TEXT NOT NULL,
            filter_type TEXT NOT NULL DEFAULT 'section_strip',
            enabled INTEGER NOT NULL DEFAULT 1,
            description TEXT,
            created_at TEXT NOT NULL
        );
        """,
    ),
]


def get_current_version(conn: sqlite3.Connection) -> int:
    try:
        row = conn.execute("SELECT MAX(version) as v FROM offers_schema_versions").fetchone()
        return row[0] or 0 if row else 0
    except sqlite3.OperationalError:
        return 0


def run_migrations(conn: sqlite3.Connection) -> list[int]:
    conn.executescript(VERSION_TABLE_SQL)
    current = get_current_version(conn)
    applied = []
    for version, description, sql in MIGRATIONS:
        if version <= current:
            continue
        logger.info("Applying offers migration v%d: %s", version, description)
        try:
            conn.executescript(sql)
            conn.execute(
                "INSERT INTO offers_schema_versions (version, description, applied_at) VALUES (?, ?, ?)",
                (version, description, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            applied.append(version)
            logger.info("Offers migration v%d applied", version)
        except Exception as e:
            logger.error("Offers migration v%d failed: %s", version, e)
            conn.rollback()
            raise RuntimeError(f"Offers migration v{version} failed: {e}") from e
    if not applied:
        logger.info("Offers database schema is up to date (v%d)", current)
    else:
        logger.info("Applied %d offers migration(s), now at v%d", len(applied), applied[-1])
    return applied


def check_schema_status(conn: sqlite3.Connection) -> dict:
    current = get_current_version(conn)
    latest = MIGRATIONS[-1][0] if MIGRATIONS else 0
    return {
        "current_version": current,
        "latest_version": latest,
        "up_to_date": current >= latest,
        "pending_migrations": latest - current,
    }
