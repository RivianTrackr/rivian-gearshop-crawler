"""SQLite operational helpers for the admin UI.

These power the per-crawler "Database Health" panel: inspecting WAL state,
detecting which processes hold a DB file open, running WAL checkpoints,
and force-unlocking by stopping the owning service first.

Everything here is read-only or operates on the crawler's own DB files —
no admin-DB writes happen from this module.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("admin.dbops")

VALID_CHECKPOINT_MODES = ("PASSIVE", "FULL", "RESTART", "TRUNCATE")
LSOF_TIMEOUT = 5  # seconds
SERVICE_STOP_WAIT_SECONDS = 5.0


def _human_bytes(n: int) -> str:
    """Format a byte count as a human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def get_db_files_info(db_path: str) -> dict:
    """Return size + mtime for the .db, .db-wal, and .db-shm files.

    Each key is None if the corresponding file doesn't exist.
    """
    info = {"db_path": db_path, "main": None, "wal": None, "shm": None}
    if not db_path:
        return info
    for key, suffix in (("main", ""), ("wal", "-wal"), ("shm", "-shm")):
        path = db_path + suffix
        try:
            st = os.stat(path)
            info[key] = {
                "path": path,
                "size_bytes": st.st_size,
                "size_human": _human_bytes(st.st_size),
                "mtime_iso": datetime.fromtimestamp(
                    st.st_mtime, tz=timezone.utc
                ).isoformat(),
            }
        except FileNotFoundError:
            info[key] = None
        except OSError as e:
            logger.warning("stat(%s) failed: %s", path, e)
            info[key] = None
    return info


def find_lock_holders(db_path: str) -> dict:
    """Return processes holding the DB or its WAL/SHM open, via lsof.

    Returns {"available": bool, "holders": [{"pid", "command", "files"}], "error"}.
    available=False means lsof isn't installed — that's expected on some boxes,
    we just can't show holders. The DB-Health panel still works without it.
    """
    result = {"available": True, "holders": [], "error": None}
    if not db_path or not os.path.exists(db_path):
        result["available"] = True
        return result

    by_pid: dict[int, dict] = {}
    saw_lsof = False

    for suffix, label in (("", "main"), ("-wal", "wal"), ("-shm", "shm")):
        path = db_path + suffix
        if not os.path.exists(path):
            continue
        try:
            r = subprocess.run(
                ["lsof", "-Fpc", path],
                capture_output=True, text=True, timeout=LSOF_TIMEOUT,
            )
            saw_lsof = True
        except FileNotFoundError:
            result["available"] = False
            result["error"] = "lsof not installed"
            return result
        except subprocess.TimeoutExpired:
            result["error"] = "lsof timed out"
            return result

        # rc=1 with empty stdout means "no holders" — that's normal.
        if r.returncode not in (0, 1):
            logger.warning("lsof on %s exited %d: %s", path, r.returncode, r.stderr)
            continue

        # lsof -F output: lines start with a single-char field code.
        # p<pid>, c<command>, then more fields per file. We only want pid+command.
        current_pid: Optional[int] = None
        for line in r.stdout.splitlines():
            if not line:
                continue
            tag, _, val = line[0], line[0], line[1:]
            if tag == "p":
                try:
                    current_pid = int(val)
                except ValueError:
                    current_pid = None
            elif tag == "c" and current_pid is not None:
                entry = by_pid.setdefault(
                    current_pid,
                    {"pid": current_pid, "command": val, "files": []},
                )
                if label not in entry["files"]:
                    entry["files"].append(label)

    if saw_lsof:
        result["holders"] = sorted(by_pid.values(), key=lambda h: h["pid"])
    return result


def check_lock_status(db_path: str, timeout_seconds: float = 0.5) -> dict:
    """Probe the DB by attempting a no-op write transaction.

    Returns {"exists", "locked", "error"}. Uses a very short SQLite busy
    timeout so a held lock is reported quickly instead of waiting 30s.
    """
    if not db_path:
        return {"exists": False, "locked": False, "error": None}
    if not os.path.exists(db_path):
        return {"exists": False, "locked": False, "error": None}
    try:
        conn = sqlite3.connect(db_path, timeout=timeout_seconds)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.rollback()
            return {"exists": True, "locked": False, "error": None}
        except sqlite3.OperationalError as e:
            msg = str(e)
            return {
                "exists": True,
                "locked": "locked" in msg.lower() or "busy" in msg.lower(),
                "error": msg,
            }
        finally:
            conn.close()
    except Exception as e:  # pragma: no cover — bubble up for diagnostics
        return {"exists": True, "locked": True, "error": str(e)}


def wal_checkpoint(db_path: str, mode: str = "TRUNCATE") -> tuple[bool, dict]:
    """Run `PRAGMA wal_checkpoint(<mode>)` and return the result row.

    SQLite returns (busy, log_pages, checkpointed_pages):
      - busy=0 means no other connection blocked the checkpoint
      - log_pages = pages still in the WAL
      - checkpointed_pages = pages moved into the main DB

    TRUNCATE additionally truncates the WAL file to 0 bytes when complete.
    Safe to call anytime; a failure here is informational, not destructive.
    """
    if not db_path:
        return False, {"error": "db_path is empty"}
    if not os.path.exists(db_path):
        return False, {"error": f"DB does not exist: {db_path}"}
    if mode not in VALID_CHECKPOINT_MODES:
        return False, {"error": f"Invalid mode: {mode}"}
    try:
        # Use a moderate timeout — if a writer is busy we want to know fast,
        # not block the admin UI for 30s.
        conn = sqlite3.connect(db_path, timeout=5)
        try:
            row = conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
            if row is None:
                return False, {"error": "checkpoint returned no row"}
            return True, {
                "busy": int(row[0]),
                "log_pages": int(row[1]),
                "checkpointed_pages": int(row[2]),
                "mode": mode,
            }
        finally:
            conn.close()
    except sqlite3.OperationalError as e:
        return False, {"error": str(e)}
    except Exception as e:
        logger.exception("wal_checkpoint failed for %s", db_path)
        return False, {"error": str(e)}


def force_unlock(
    db_path: str,
    service_unit: str,
    timer_unit: Optional[str] = None,
) -> tuple[bool, str]:
    """Stop the owning service (and timer) and run a TRUNCATE checkpoint.

    Order of operations:
      1. Stop the timer so it can't fire during recovery.
      2. Stop the service synchronously.
      3. Wait briefly for systemd to reflect inactive state.
      4. Run wal_checkpoint(TRUNCATE) — should now succeed since no holder.
      5. Return a summary; caller is responsible for re-enabling the timer.
    """
    # Imported lazily so this module stays importable in unit tests
    # without triggering the systemd subprocess machinery at import time.
    from admin.systemd import stop_service, get_service_status

    msgs: list[str] = []

    if timer_unit:
        ok, err = stop_service(timer_unit)
        msgs.append(f"timer stop: {'ok' if ok else err.strip() or 'failed'}")

    ok, err = stop_service(service_unit)
    msgs.append(f"service stop: {'ok' if ok else err.strip() or 'failed'}")

    # Poll for the service to leave 'active' state, up to ~5s.
    deadline = time.monotonic() + SERVICE_STOP_WAIT_SECONDS
    final_state = "unknown"
    while time.monotonic() < deadline:
        status = get_service_status(service_unit, timer_unit)
        final_state = status.active_state
        if final_state != "active":
            break
        time.sleep(0.5)
    msgs.append(f"service state: {final_state}")

    ok_cp, info = wal_checkpoint(db_path, mode="TRUNCATE")
    if ok_cp:
        msgs.append(
            f"checkpoint ok (busy={info['busy']}, "
            f"log_pages={info['log_pages']}, "
            f"checkpointed={info['checkpointed_pages']})"
        )
        # busy>0 means another connection blocked us — report but don't lie.
        success = info["busy"] == 0
    else:
        msgs.append(f"checkpoint failed: {info.get('error')}")
        success = False

    return success, "; ".join(msgs)
