"""
Compatibility shim — implementation lives in `legacy_archive/scripts/split_forward_tester.py`.

This file is expected to be used as a script (CLI). We delegate via `runpy`.
"""

from __future__ import annotations

from pathlib import Path
import runpy

_LEGACY = (
    Path(__file__).resolve().parents[1]
    / "legacy_archive"
    / "scripts"
    / "split_forward_tester.py"
)


def main() -> None:
    runpy.run_path(str(_LEGACY), run_name="__main__")


if __name__ == "__main__":
    main()

