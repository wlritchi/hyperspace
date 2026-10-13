# vim: filetype=python
"""Checks on the repository layout."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _pep723_scripts() -> list[Path]:
    found: list[Path] = []
    for directory in ("server", "scripts"):
        for path in sorted((ROOT / directory).glob("*.py")):
            if "# /// script" in path.read_text():
                found.append(path)
    return found


def test_every_deployed_script_has_a_lock_file() -> None:
    # The container image installs from the lock files, so a script with no
    # lock file cannot be built.
    missing = [
        str(p.relative_to(ROOT))
        for p in _pep723_scripts()
        if not p.with_name(p.name + ".lock").exists()
    ]
    assert missing == []
