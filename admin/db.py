import sqlite3
import secrets
from datetime import datetime, timezone

from admin.config import ADMIN_DB_PATH
from admin.auth import hash_password

ADMIN_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS managed_scripts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    display_name TEXT NOT NULL,
    service_unit TEXT NOT NULL,
    timer_unit TEXT,
    env_file_path TEXT,
    db_path TEXT,
    working_directory TEXT NOT NULL,
    description TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS script_notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id INTEGER NOT NULL,
    channel TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (script_id) REFERENCES managed_scripts(id),
    UNIQUE(script_id, channel)
);
"""

DEFAULT_SCRIPT = {
    "name": "rivian-gearshop-crawler",
    "display_name": "RivianCrawlr Gear Shop Crawler",
    "service_unit": "rivian-gearshop-crawler.service",
    "timer_unit": "rivian-gearshop-crawler.timer",
    "env_file_path": "/opt/rivian-gearshop-crawler/.env",
    "db_path": "/opt/rivian-gearshop-crawler/gearshop.db",
    "working_directory": "/opt/rivian-gearshop-crawler",
    "description": "Monitors Rivian Gear Shop for inventory changes and sends email alerts.",
}

SUPPORT_SCRIPT = {
    "name": "rivian-support-crawler",
    "display_name": "RivianCrawlr Support Article Crawler",
    "service_unit": "rivian-support-crawler.service",
    "timer_unit": "rivian-support-crawler.timer",
    "env_file_path": "/opt/rivian-gearshop-crawler/.env",
    "db_path": "/opt/rivian-gearshop-crawler/support.db",
    "working_directory": "/opt/rivian-gearshop-crawler",
    "description": "Monitors Rivian Support articles for content changes and sends email alerts.",
}

OFFERS_SCRIPT = {
    "name": "rivian-offers-crawler",
    "display_name": "Rivian Offers Crawler",
    "service_unit": "rivian-offers-crawler.service",
    "timer_unit": "rivian-offers-crawler.timer",
    "env_file_path": "/opt/rivian-gearshop-crawler/.env",
    "db_path": "/opt/rivian-gearshop-crawler/offers.db",
    "working_directory": "/opt/rivian-gearshop-crawler",
    "description": "Monitors rivian.com/offers for new/removed/changed promotional offers",
}


def get_admin_db() -> sqlite3.Connection:
    conn = sqlite3.connect(ADMIN_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_crawler_db(db_path: str) -> sqlite3.Connection:
    """Open a crawler DB in read-only mode."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def get_crawler_db_rw(db_path: str) -> sqlite3.Connection:
    """Open a crawler DB in read-write mode (for managing content filters)."""
    # Match the crawler-side timeout (30s) so admin write ops don't time out
    # fast while a crawl holds the lock — and vice versa.
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_admin_db():
    """Create tables and bootstrap default data if needed."""
    conn = get_admin_db()
    conn.executescript(ADMIN_SCHEMA)

    # Bootstrap admin user if none exist
    row = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()
    if row["cnt"] == 0:
        password = secrets.token_urlsafe(16)
        pw_hash = hash_password(password)
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?)",
            ("admin", pw_hash, now),
        )
        conn.commit()
        print("=" * 50)
        print("  ADMIN UI — First-run bootstrap")
        print(f"  Username: admin")
        print(f"  Password: {password}")
        print("  Change this password immediately after login!")
        print("=" * 50)

    # Seed default managed scripts if they don't exist
    now = datetime.now(timezone.utc).isoformat()
    for script_def in (DEFAULT_SCRIPT, SUPPORT_SCRIPT, OFFERS_SCRIPT):
        existing = conn.execute(
            "SELECT 1 FROM managed_scripts WHERE name = ?", (script_def["name"],)
        ).fetchone()
        if not existing:
            conn.execute(
                """INSERT INTO managed_scripts
                   (name, display_name, service_unit, timer_unit,
                    env_file_path, db_path, working_directory, description, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    script_def["name"],
                    script_def["display_name"],
                    script_def["service_unit"],
                    script_def["timer_unit"],
                    script_def["env_file_path"],
                    script_def["db_path"],
                    script_def["working_directory"],
                    script_def["description"],
                    now,
                ),
            )
    conn.commit()

    # Ensure script_notifications table exists (handles upgrades)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS script_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            script_id INTEGER NOT NULL,
            channel TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            config TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (script_id) REFERENCES managed_scripts(id),
            UNIQUE(script_id, channel)
        )
    """)
    conn.commit()

    conn.close()
