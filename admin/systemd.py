import os
import re
import subprocess
import threading
import time
from dataclasses import dataclass

SUBPROCESS_TIMEOUT = 10

# Short-lived cache for status reads. The dashboard polls every 5s per viewer
# and each read fans out to multiple `systemctl show`/`list-timers`/`is-active`
# subprocess calls, so without caching a single viewer can trigger ~12 shellouts
# every 5s. A 2s TTL keeps the UI responsive while collapsing bursts.
_STATUS_CACHE_TTL = 2.0
_status_cache: dict = {}
_cache_lock = threading.Lock()


def _cache_get(key):
    now = time.monotonic()
    with _cache_lock:
        entry = _status_cache.get(key)
        if entry and entry[0] > now:
            return entry[1]
    return None


def _cache_set(key, value):
    with _cache_lock:
        _status_cache[key] = (time.monotonic() + _STATUS_CACHE_TTL, value)


def _cache_invalidate(*units: str):
    """Drop cached entries for the given units (and any entry if none given)."""
    with _cache_lock:
        if not units:
            _status_cache.clear()
            return
        drop = [k for k in _status_cache if any(u and u in k for u in units)]
        for k in drop:
            _status_cache.pop(k, None)

# Matches timestamps like "Tue 2026-03-10 02:25:00 UTC"
_TIMESTAMP_RE = re.compile(
    r"[A-Z][a-z]{2}\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+\w+"
)


def _extract_timestamp(text: str) -> str | None:
    """Extract a systemd-style timestamp from text."""
    m = _TIMESTAMP_RE.search(text)
    return m.group(0) if m else None


@dataclass
class ServiceStatus:
    active_state: str
    sub_state: str
    last_trigger: str | None
    next_trigger: str | None
    result: str | None


def _parse_props(stdout: str) -> dict:
    props = {}
    for line in stdout.strip().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            props[k] = v
    return props


def get_service_status(service_unit: str, timer_unit: str | None = None) -> ServiceStatus:
    key = ("status", service_unit, timer_unit or "")
    cached = _cache_get(key)
    if cached is not None:
        return cached
    value = _get_service_status_uncached(service_unit, timer_unit)
    _cache_set(key, value)
    return value


def _get_service_status_uncached(service_unit: str, timer_unit: str | None = None) -> ServiceStatus:
    result = subprocess.run(
        [
            "systemctl", "show", service_unit,
            "--property=ActiveState,SubState,ExecMainStartTimestamp,Result",
        ],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    props = _parse_props(result.stdout)

    next_trigger = None
    last_trigger = props.get("ExecMainStartTimestamp")

    if timer_unit:
        # Use list-timers which gives clean, human-readable timestamps
        # Format: NEXT  LEFT  LAST  PASSED  UNIT  ACTIVATES
        try:
            lt_result = subprocess.run(
                ["systemctl", "list-timers", timer_unit, "--no-pager"],
                capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
            )
            output = lt_result.stdout.strip()
            # Find the data line (skip header, look for the unit name)
            for line in output.splitlines():
                if timer_unit in line:
                    # The line format is like:
                    # Tue 2026-03-10 02:25:00 UTC  48min left  Tue 2026-03-10 01:14:21 UTC  22min ago  rivian-...timer  rivian-...service
                    # Split on the unit name to get the timestamps portion
                    before_unit = line.split(timer_unit)[0].strip()
                    # Split on "left" to separate NEXT from LAST
                    if " left " in before_unit:
                        next_part, rest = before_unit.split(" left ", 1)
                        # next_part ends with the LEFT duration, NEXT timestamp is before that
                        # e.g. "Tue 2026-03-10 02:25:00 UTC  48min"
                        # Find the timezone marker to split
                        next_trigger = _extract_timestamp(next_part)
                    if " ago " in before_unit:
                        ago_idx = before_unit.rfind(" ago ")
                        last_part = before_unit[:ago_idx]
                        # last_part has NEXT...left...LAST_timestamp PASSED
                        # The LAST timestamp is between "left" and "ago"
                        if " left " in last_part:
                            last_part = last_part.split(" left ", 1)[1].strip()
                        last_trigger = _extract_timestamp(last_part)
                    break
        except Exception:
            pass

    return ServiceStatus(
        active_state=props.get("ActiveState", "unknown"),
        sub_state=props.get("SubState", "unknown"),
        last_trigger=last_trigger if last_trigger and last_trigger not in ("n/a", "0", "") else None,
        next_trigger=next_trigger if next_trigger and next_trigger not in ("n/a", "0", "") else None,
        result=props.get("Result"),
    )


def get_timer_active(timer_unit: str) -> bool:
    key = ("timer_active", timer_unit)
    cached = _cache_get(key)
    if cached is not None:
        return cached
    result = subprocess.run(
        ["systemctl", "is-active", timer_unit],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    value = result.stdout.strip() == "active"
    _cache_set(key, value)
    return value


def start_service(unit: str) -> tuple[bool, str]:
    r = subprocess.run(
        ["systemctl", "start", "--no-block", unit],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    _cache_invalidate(unit)
    return r.returncode == 0, r.stderr


def stop_service(unit: str) -> tuple[bool, str]:
    r = subprocess.run(
        ["systemctl", "stop", unit],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    _cache_invalidate(unit)
    return r.returncode == 0, r.stderr


def enable_service(unit: str) -> tuple[bool, str]:
    """Enable and start a systemd unit."""
    r = subprocess.run(
        ["systemctl", "enable", "--now", unit],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    _cache_invalidate(unit)
    return r.returncode == 0, r.stderr


def disable_service(unit: str) -> tuple[bool, str]:
    """Disable and stop a systemd unit."""
    r = subprocess.run(
        ["systemctl", "disable", "--now", unit],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    _cache_invalidate(unit)
    return r.returncode == 0, r.stderr


def daemon_reload() -> tuple[bool, str]:
    r = subprocess.run(
        ["systemctl", "daemon-reload"],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    _cache_invalidate()
    return r.returncode == 0, r.stderr


def is_unit_installed(unit: str) -> bool:
    """Check if a systemd unit file exists on disk."""
    r = subprocess.run(
        ["systemctl", "cat", unit],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    return r.returncode == 0


def install_unit_files(working_directory: str, service_unit: str, timer_unit: str | None) -> tuple[bool, str]:
    """Copy .service and .timer files from repo to /etc/systemd/system/ and reload."""
    import shutil
    errors = []
    target_dir = "/etc/systemd/system"

    for unit in (service_unit, timer_unit):
        if not unit:
            continue
        src = os.path.join(working_directory, unit)
        dst = os.path.join(target_dir, unit)
        if not os.path.exists(src):
            errors.append(f"{unit} not found in {working_directory}")
            continue
        try:
            shutil.copy2(src, dst)
        except Exception as e:
            errors.append(f"Failed to copy {unit}: {e}")

    if errors:
        return False, "; ".join(errors)

    ok, err = daemon_reload()
    if not ok:
        return False, f"daemon-reload failed: {err}"
    return True, ""


def install_admin_service(working_directory: str) -> tuple[bool, str]:
    """Install the admin service unit file, reload, and enable it."""
    import shutil
    unit = "gearshop-admin.service"
    src = os.path.join(working_directory, unit)
    dst = os.path.join("/etc/systemd/system", unit)

    if not os.path.exists(src):
        return False, f"{unit} not found in {working_directory}"

    try:
        shutil.copy2(src, dst)
    except Exception as e:
        return False, f"Failed to copy {unit}: {e}"

    ok, err = daemon_reload()
    if not ok:
        return False, f"daemon-reload failed: {err}"

    ok, err = enable_service(unit)
    if not ok:
        return False, f"Failed to enable {unit}: {err}"

    return True, ""


def restart_admin_service() -> tuple[bool, str]:
    """Restart the admin UI service after a short delay.

    Uses a detached shell process that sleeps 1 second before restarting,
    giving the HTTP response time to complete before systemd kills uvicorn.
    """
    try:
        subprocess.Popen(
            ["bash", "-c", "sleep 1 && systemctl restart gearshop-admin.service"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return True, ""
    except Exception as e:
        return False, str(e)


def get_journal_logs(unit: str, lines: int = 100, since: str | None = None) -> str:
    cmd = [
        "journalctl", "-u", unit, "--no-pager",
        "-n", str(lines), "--output=short-iso",
    ]
    if since:
        cmd.extend(["--since", since])
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    return r.stdout
