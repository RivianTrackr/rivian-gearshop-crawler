"""Tests for migrations.py — schema versioning and migration runner."""

import sqlite3

import pytest

from migrations import (
    run_migrations,
    get_current_version,
    check_schema_status,
    MIGRATIONS,
)


class TestRunMigrations:
    def test_fresh_database(self, tmp_db):
        conn = sqlite3.connect(tmp_db)
        conn.row_factory = sqlite3.Row
        applied = run_migrations(conn)

        assert len(applied) == len(MIGRATIONS)
        assert get_current_version(conn) == MIGRATIONS[-1][0]
        conn.close()

    def test_idempotent(self, tmp_db):
        conn = sqlite3.connect(tmp_db)
        conn.row_factory = sqlite3.Row

        # Run twice
        applied1 = run_migrations(conn)
        applied2 = run_migrations(conn)

        assert len(applied1) == len(MIGRATIONS)
        assert len(applied2) == 0  # No new migrations
        conn.close()

    def test_creates_all_tables(self, crawler_db):
        tables = [
            row[0]
            for row in crawler_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        ]
        assert "products" in tables
        assert "variants" in tables
        assert "snapshots" in tables
        assert "crawl_markers" in tables
        assert "crawl_stats" in tables
        assert "heartbeats" in tables
        assert "removed_once" in tables
        assert "schema_versions" in tables
        assert "crawl_runs" in tables
        assert "notification_queue" in tables

    def test_schema_versions_populated(self, crawler_db):
        rows = crawler_db.execute(
            "SELECT version, description FROM schema_versions ORDER BY version"
        ).fetchall()
        assert len(rows) == len(MIGRATIONS)
        for row, (version, desc, _) in zip(rows, MIGRATIONS):
            assert row["version"] == version
            assert row["description"] == desc


class TestGetCurrentVersion:
    def test_empty_db(self, tmp_db):
        conn = sqlite3.connect(tmp_db)
        assert get_current_version(conn) == 0
        conn.close()

    def test_after_migrations(self, crawler_db):
        assert get_current_version(crawler_db) == MIGRATIONS[-1][0]


class TestCheckSchemaStatus:
    def test_up_to_date(self, crawler_db):
        status = check_schema_status(crawler_db)
        assert status["up_to_date"] is True
        assert status["pending_migrations"] == 0
        assert status["current_version"] == status["latest_version"]

    def test_needs_migration(self, tmp_db):
        conn = sqlite3.connect(tmp_db)
        conn.row_factory = sqlite3.Row
        status = check_schema_status(conn)
        assert status["up_to_date"] is False
        assert status["pending_migrations"] > 0
        conn.close()
