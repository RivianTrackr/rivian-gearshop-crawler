"""Tests for admin/dbops.py — DB health helpers used by the unlock UI."""
import os
import sqlite3

import pytest

from admin import dbops


@pytest.fixture
def fresh_db(tmp_path):
    """Create a tiny WAL-mode SQLite DB with one row of data."""
    db_path = str(tmp_path / "fixture.db")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
        conn.executemany("INSERT INTO t (val) VALUES (?)", [("a",), ("b",), ("c",)])
        conn.commit()
    finally:
        conn.close()
    return db_path


def test_get_db_files_info_existing(fresh_db):
    info = dbops.get_db_files_info(fresh_db)
    assert info["db_path"] == fresh_db
    assert info["main"] is not None
    assert info["main"]["size_bytes"] > 0
    assert info["main"]["size_human"].endswith(("B", "KB", "MB"))
    # WAL/SHM may or may not be present depending on whether a checkpoint
    # has happened — both states are valid.


def test_get_db_files_info_missing(tmp_path):
    info = dbops.get_db_files_info(str(tmp_path / "does-not-exist.db"))
    assert info["main"] is None
    assert info["wal"] is None
    assert info["shm"] is None


def test_get_db_files_info_empty_path():
    info = dbops.get_db_files_info("")
    assert info["main"] is None


def test_check_lock_status_free(fresh_db):
    status = dbops.check_lock_status(fresh_db)
    assert status["exists"] is True
    assert status["locked"] is False
    assert status["error"] is None


def test_check_lock_status_missing(tmp_path):
    status = dbops.check_lock_status(str(tmp_path / "missing.db"))
    assert status["exists"] is False
    assert status["locked"] is False


def test_check_lock_status_when_locked(fresh_db):
    """Hold an exclusive lock on the DB and verify check_lock_status reports it."""
    holder = sqlite3.connect(fresh_db, timeout=1)
    try:
        holder.execute("BEGIN EXCLUSIVE")
        # check_lock_status uses a 0.5s timeout by default, well under our hold.
        status = dbops.check_lock_status(fresh_db, timeout_seconds=0.2)
        assert status["exists"] is True
        assert status["locked"] is True
        assert status["error"] is not None
    finally:
        holder.rollback()
        holder.close()


def test_wal_checkpoint_truncate(fresh_db):
    # Force a WAL by writing without checkpointing
    c = sqlite3.connect(fresh_db)
    try:
        c.execute("INSERT INTO t (val) VALUES ('d')")
        c.commit()
    finally:
        c.close()

    ok, info = dbops.wal_checkpoint(fresh_db, mode="TRUNCATE")
    assert ok is True
    assert info["busy"] == 0
    assert info["mode"] == "TRUNCATE"
    assert "checkpointed_pages" in info


def test_wal_checkpoint_invalid_mode(fresh_db):
    ok, info = dbops.wal_checkpoint(fresh_db, mode="BOGUS")
    assert ok is False
    assert "Invalid mode" in info["error"]


def test_wal_checkpoint_missing_db(tmp_path):
    ok, info = dbops.wal_checkpoint(str(tmp_path / "missing.db"))
    assert ok is False
    assert "does not exist" in info["error"]


def test_find_lock_holders_missing(tmp_path):
    """Missing DB returns the 'available' shape with no holders."""
    result = dbops.find_lock_holders(str(tmp_path / "missing.db"))
    assert result["holders"] == []
    assert result["available"] is True


def test_find_lock_holders_existing_db_no_writers(fresh_db):
    """A DB with no live connections should report no holders.

    On systems without lsof, available=False and we just skip the assertion.
    """
    result = dbops.find_lock_holders(fresh_db)
    if not result["available"]:
        pytest.skip("lsof not installed on this system")
    # No connections held during this test, so holders should be empty.
    assert result["holders"] == []
    assert result["error"] is None


def test_human_bytes_scale():
    assert dbops._human_bytes(0) == "0 B"
    assert dbops._human_bytes(1023) == "1023 B"
    assert dbops._human_bytes(1024).endswith("KB")
    assert dbops._human_bytes(1024 * 1024).endswith("MB")
    assert dbops._human_bytes(1024 * 1024 * 1024).endswith("GB")
