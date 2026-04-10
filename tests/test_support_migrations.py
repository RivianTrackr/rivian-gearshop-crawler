"""Tests for support_migrations.py — schema versioning and migration runner."""

import sqlite3

import pytest

from support_migrations import (
    run_migrations,
    get_current_version,
    check_schema_status,
    MIGRATIONS,
)


@pytest.fixture
def support_db(tmp_path):
    """Create a temporary support database with full schema."""
    db_path = str(tmp_path / "support_test.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    yield conn
    conn.close()


class TestRunMigrations:
    def test_fresh_database(self, tmp_path):
        db_path = str(tmp_path / "fresh.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        applied = run_migrations(conn)

        assert len(applied) == len(MIGRATIONS)
        assert get_current_version(conn) == MIGRATIONS[-1][0]
        conn.close()

    def test_idempotent(self, tmp_path):
        db_path = str(tmp_path / "idem.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        applied1 = run_migrations(conn)
        applied2 = run_migrations(conn)

        assert len(applied1) == len(MIGRATIONS)
        assert len(applied2) == 0
        conn.close()

    def test_creates_all_tables(self, support_db):
        tables = {
            row[0]
            for row in support_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        }
        assert "support_articles" in tables
        assert "article_snapshots" in tables
        assert "support_crawl_markers" in tables
        assert "support_crawl_stats" in tables
        assert "support_crawl_runs" in tables
        assert "support_heartbeats" in tables
        assert "support_removed_once" in tables
        assert "support_notification_queue" in tables
        assert "schema_versions" in tables

    def test_schema_versions_populated(self, support_db):
        rows = support_db.execute(
            "SELECT version, description FROM schema_versions ORDER BY version"
        ).fetchall()
        assert len(rows) == len(MIGRATIONS)
        for row, (version, desc, _) in zip(rows, MIGRATIONS):
            assert row["version"] == version
            assert row["description"] == desc


class TestGetCurrentVersion:
    def test_empty_db(self, tmp_path):
        db_path = str(tmp_path / "empty.db")
        conn = sqlite3.connect(db_path)
        assert get_current_version(conn) == 0
        conn.close()

    def test_after_migrations(self, support_db):
        assert get_current_version(support_db) == MIGRATIONS[-1][0]


class TestCheckSchemaStatus:
    def test_up_to_date(self, support_db):
        status = check_schema_status(support_db)
        assert status["up_to_date"] is True
        assert status["pending_migrations"] == 0
        assert status["current_version"] == status["latest_version"]

    def test_needs_migration(self, tmp_path):
        db_path = str(tmp_path / "needs.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        status = check_schema_status(conn)
        assert status["up_to_date"] is False
        assert status["pending_migrations"] > 0
        conn.close()
