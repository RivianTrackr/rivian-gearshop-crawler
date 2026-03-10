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
        timer_result = subprocess.run(
            [
                "systemctl", "show", timer_unit,
                "--property=NextElapseUSecRealtime,LastTriggerUSec",
            ],
            capture_output=True, text=True, timeout=SUBPROCESS_TIMEOUT,
        )
        timer_props = _parse_props(timer_result.stdout)
        next_trigger = timer_props.get("NextElapseUSecRealtime")
        lt = timer_props.get("LastTriggerUSec")
        if lt and lt != "n/a":
            last_trigger = lt

    return ServiceStatus(
        active_state=props.get("ActiveState", "unknown"),
        sub_state=props.get("SubState", "unknown"),
        last_trigger=last_trigger if last_trigger and last_trigger != "n/a" else None,
        next_trigger=next_trigger if next_trigger and next_trigger != "n/a" else None,
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
