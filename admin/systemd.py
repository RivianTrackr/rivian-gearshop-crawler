import subprocess
from dataclasses import dataclass

SUBPROCESS_TIMEOUT = 10


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
        # Query all timer-related properties to handle different systemd versions
        timer_result = subprocess.run(
            [
                "systemctl", "show", timer_unit,
                "--property=NextElapseUSecRealtime,NextElapseUSecMonotonic,"
                "LastTriggerUSec,LastTriggerUSecRealtime",
            ],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
        )
        timer_props = _parse_props(timer_result.stdout)

        # Try multiple property names for next trigger
        next_trigger = (
            timer_props.get("NextElapseUSecRealtime")
            or timer_props.get("NextElapseUSecMonotonic")
        )

        # Try multiple property names for last trigger
        lt = (
            timer_props.get("LastTriggerUSecRealtime")
            or timer_props.get("LastTriggerUSec")
        )
        if lt and lt not in ("n/a", "0", ""):
            last_trigger = lt

        # Fallback: use systemctl list-timers to parse next/last run
        if not next_trigger or next_trigger in ("n/a", "0", ""):
            try:
                lt_result = subprocess.run(
                    ["systemctl", "list-timers", timer_unit, "--no-pager", "--plain"],
                    capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
                )
                lines = lt_result.stdout.strip().splitlines()
                # Header line then data line
                if len(lines) >= 2:
                    # Format: NEXT  LEFT  LAST  PASSED  UNIT  ACTIVATES
                    parts = lines[1].split()
                    if len(parts) >= 6:
                        # NEXT is first 3 fields (day date time), LAST is fields after LEFT
                        next_trigger = " ".join(parts[0:3])
                        # Find LAST: skip NEXT(3) + LEFT(2) = index 5 for last
                        last_trigger = " ".join(parts[5:8]) if len(parts) >= 9 else last_trigger
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
