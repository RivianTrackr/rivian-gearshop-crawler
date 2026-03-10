import re
import subprocess
from dataclasses import dataclass

SUBPROCESS_TIMEOUT = 10

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
    result = subprocess.run(
        ["systemctl", "is-active", timer_unit],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    return result.stdout.strip() == "active"


def start_service(unit: str) -> tuple[bool, str]:
    r = subprocess.run(
        ["systemctl", "start", unit],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    return r.returncode == 0, r.stderr


def stop_service(unit: str) -> tuple[bool, str]:
    r = subprocess.run(
        ["systemctl", "stop", unit],
        capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
    )
    return r.returncode == 0, r.stderr


def get_journal_logs(unit: str, lines: int = 100, since: str | None = None) -> str:
    cmd = [
        "journalctl", "-u", unit, "--no-pager",
        "-n", str(lines), "--output=short-iso",
    ]
    if since:
        cmd.extend(["--since", since])
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT)
    return r.stdout
