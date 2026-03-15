"""Shared test fixtures for the RivianTrackr test suite."""

import os
import sqlite3
import tempfile

import pytest


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary SQLite database and return its path."""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.close()
    return db_path


@pytest.fixture
def crawler_db(tmp_db):
    """Create a temporary crawler database with full schema."""
    from migrations import run_migrations

    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_product_json():
    """Return a sample Shopify product JSON response."""
    return {
        "id": 123456,
        "title": "Rivian Adventure Jacket",
        "handle": "rivian-adventure-jacket",
        "vendor": "Rivian",
        "product_type": "Apparel",
        "created_at": "2024-01-15T10:00:00-05:00",
        "updated_at": "2024-06-01T12:00:00-05:00",
        "variants": [
            {
                "id": 111,
                "title": "Small",
                "sku": "RAJ-SM",
                "price": "12500",
                "compare_at_price": "15000",
                "available": True,
            },
            {
                "id": 222,
                "title": "Medium",
                "sku": "RAJ-MD",
                "price": "12500",
                "compare_at_price": None,
                "available": False,
            },
        ],
    }


@pytest.fixture
def mock_env(monkeypatch, tmp_db):
    """Set environment variables for testing."""
    monkeypatch.setenv("DB_PATH", tmp_db)
    monkeypatch.setenv("SITE_ROOT", "https://gearshop.rivian.com")
    monkeypatch.setenv("COLLECTION_URL", "https://gearshop.rivian.com/collections/all")
    monkeypatch.setenv("BREVO_API_KEY", "")
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "")
    monkeypatch.setenv("HEARTBEAT_UTC_HOUR", "-1")
    monkeypatch.setenv("CRAWLER_DEBUG", "0")
    return tmp_db
