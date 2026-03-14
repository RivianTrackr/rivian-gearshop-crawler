"""Shared helpers used across multiple route modules."""

from admin.db import get_admin_db


def get_script(script_id: int):
    """Fetch a managed script by ID. Returns a Row or None."""
    conn = get_admin_db()
    try:
        row = conn.execute("SELECT * FROM managed_scripts WHERE id = ?", (script_id,)).fetchone()
        return row
    finally:
        conn.close()
