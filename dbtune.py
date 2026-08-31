"""
Shared SQLite connection tuning.

Every crawler and the admin UI open their own connections to the same handful
of database files, and until now each did so with different settings: busy
timeouts of 10s, 30s and 60s, WAL mode set in some places but not others, and
no cache/temp-store tuning anywhere. This module is the single place those
decisions live.

`apply_connection_pragmas()` is safe to call on any connection, including
read-only ones — the pragmas that cannot apply to a read-only handle are
skipped rather than raising.
"""

import logging
import os
import sqlite3

logger = logging.getLogger("dbtune")

# Busy timeout applied to every connection. Crawl runs hold a write
# transaction while fetching, and the admin UI writes content filters, so a
# generous window is what keeps "database is locked" out of the logs.
BUSY_TIMEOUT_MS = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "30000"))

# Page cache per connection, in KiB (negative = KiB rather than pages).
CACHE_SIZE_KIB = int(os.getenv("SQLITE_CACHE_SIZE_KIB", "16384"))  # 16 MiB

# Checkpoint the WAL once it passes this many pages (~4 MiB at 4 KiB pages).
WAL_AUTOCHECKPOINT_PAGES = int(os.getenv("SQLITE_WAL_AUTOCHECKPOINT", "1000"))


def apply_connection_pragmas(conn: sqlite3.Connection, *, read_only: bool = False) -> None:
    """Apply the shared pragma set to `conn`.

    Args:
        conn: an open SQLite connection.
        read_only: skip pragmas that require write access to the database.
    """
    # busy_timeout is the one that matters most and works on every handle.
    _try(conn, f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")

    # Per-connection, no write needed.
    _try(conn, "PRAGMA temp_store=MEMORY")
    _try(conn, f"PRAGMA cache_size=-{CACHE_SIZE_KIB}")

    if read_only:
        return

    # journal_mode is persisted in the database header, so this is a no-op
    # after the first time — but init paths still rely on it being set here.
    _try(conn, "PRAGMA journal_mode=WAL")

    # NORMAL is the standard companion to WAL: transactions stay durable
    # across an application crash, and only an OS crash or power loss can
    # cost the most recent commit. For crawl snapshots that is an acceptable
    # trade for dropping an fsync per transaction.
    _try(conn, "PRAGMA synchronous=NORMAL")

    _try(conn, f"PRAGMA wal_autocheckpoint={WAL_AUTOCHECKPOINT_PAGES}")

    # NOTE: `PRAGMA foreign_keys=ON` is deliberately NOT set. The schemas do
    # declare FOREIGN KEYs, but enforcing them would break the stale-handle
    # cleanup in crawler.py, which deletes a product row while its variants
    # and snapshots still reference it. Turning enforcement on needs those
    # deletes reworked (ON DELETE CASCADE, or delete children first) first.


def optimize(conn: sqlite3.Connection) -> None:
    """Run `PRAGMA optimize` — cheap, and keeps index statistics current.

    Intended to be called just before closing a long-lived connection (i.e. at
    the end of a crawl run). Without up-to-date statistics the query planner
    can ignore the indexes it has.
    """
    _try(conn, "PRAGMA optimize")


def analyze(conn: sqlite3.Connection) -> None:
    """Run a full ANALYZE. Called once after migrations add new indexes."""
    _try(conn, "ANALYZE")


def _try(conn: sqlite3.Connection, statement: str) -> None:
    """Execute a pragma, logging and swallowing failures.

    A connection that cannot accept a given pragma (read-only handle, older
    SQLite build) must not take down a crawl run over it.
    """
    try:
        conn.execute(statement)
    except sqlite3.Error as e:
        logger.debug("%s failed: %s", statement, e)
