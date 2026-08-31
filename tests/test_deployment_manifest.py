"""Guards that the deployment manifest matches what the code actually imports.

`setup.sh` copies an explicit list of modules into /opt. When a new top-level
module is added and that list is not updated, the install still succeeds and
then the crawler dies at the first timer firing with ModuleNotFoundError —
which is how `social.py` and the whole offers crawler came to be missing.
"""

import ast
import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parent.parent

ENTRYPOINTS = ("crawler.py", "support_crawler.py", "offers_crawler.py")


def _local_module_names() -> set[str]:
    return {p.stem for p in REPO.glob("*.py")}


def _setup_sh_modules() -> set[str]:
    text = (REPO / "setup.sh").read_text()
    block = re.search(r"PROJECT_MODULES=\((.*?)\)", text, re.S)
    assert block, "PROJECT_MODULES array not found in setup.sh"
    return {line.strip() for line in block.group(1).splitlines() if line.strip()}


def _imports_of(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text())
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module.split(".")[0])
    return names


def test_setup_sh_installs_every_locally_imported_module():
    local = _local_module_names()
    installed = _setup_sh_modules()

    missing = set()
    for entry in ENTRYPOINTS:
        path = REPO / entry
        assert path.exists(), f"{entry} missing from repo"
        for name in _imports_of(path) & local:
            if f"{name}.py" not in installed:
                missing.add(f"{name}.py (imported by {entry})")

    assert not missing, (
        "setup.sh PROJECT_MODULES is missing modules that are imported at "
        f"module scope: {sorted(missing)}"
    )


def test_setup_sh_only_lists_files_that_exist():
    for module in _setup_sh_modules():
        assert (REPO / module).exists(), f"setup.sh copies {module}, which does not exist"


def test_every_entrypoint_has_service_and_timer_units():
    for unit in (
        "rivian-gearshop-crawler.service", "rivian-gearshop-crawler.timer",
        "rivian-support-crawler.service", "rivian-support-crawler.timer",
        "rivian-offers-crawler.service", "rivian-offers-crawler.timer",
        "gearshop-admin.service",
    ):
        assert (REPO / unit).exists(), f"{unit} missing"


def test_setup_sh_enables_every_timer():
    text = (REPO / "setup.sh").read_text()
    for timer in (
        "rivian-gearshop-crawler.timer",
        "rivian-support-crawler.timer",
        "rivian-offers-crawler.timer",
    ):
        assert f"systemctl enable {timer}" in text, f"setup.sh never enables {timer}"


def test_crawler_timers_do_not_share_a_slot():
    """Overlapping slots mean concurrent headless Chromium instances."""
    slots = {}
    for timer in REPO.glob("rivian-*.timer"):
        m = re.search(r"^OnCalendar=(.+)$", timer.read_text(), re.M)
        assert m, f"{timer.name} has no OnCalendar"
        slots.setdefault(m.group(1).strip(), []).append(timer.name)
    clashes = {slot: names for slot, names in slots.items() if len(names) > 1}
    assert not clashes, f"timers share a slot: {clashes}"
