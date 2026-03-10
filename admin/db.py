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
"""

DEFAULT_SCRIPT = {
    "name": "rivian-gearshop-crawler",
    "display_name": "RivianTrackr Gear Shop Crawler",
    "service_unit": "rivian-gearshop-crawler.service",
    "timer_unit": "rivian-gearshop-crawler.timer",
    "env_file_path": "/opt/rivian-gearshop-crawler/.env",
    "db_path": "/opt/rivian-gearshop-crawler/gearshop.db",
    "working_directory": "/opt/rivian-gearshop-crawler",
    "description": "Monitors Rivian Gear Shop for inventory changes and sends email alerts.",
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

    # Seed default managed script if none exist
    row = conn.execute("SELECT COUNT(*) as cnt FROM managed_scripts").fetchone()
    if row["cnt"] == 0:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """INSERT INTO managed_scripts
               (name, display_name, service_unit, timer_unit,
                env_file_path, db_path, working_directory, description, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                DEFAULT_SCRIPT["name"],
                DEFAULT_SCRIPT["display_name"],
                DEFAULT_SCRIPT["service_unit"],
                DEFAULT_SCRIPT["timer_unit"],
                DEFAULT_SCRIPT["env_file_path"],
                DEFAULT_SCRIPT["db_path"],
                DEFAULT_SCRIPT["working_directory"],
                DEFAULT_SCRIPT["description"],
                now,
            ),
        )
        conn.commit()

    conn.close()
