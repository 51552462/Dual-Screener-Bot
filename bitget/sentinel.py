"""
REMOVED — dashboard/heatmap are separate systemd units (dante-bitget-dashboard / heatmap).

`python -m bitget.sentinel` is disabled to prevent duplicate Streamlit subprocesses.
See bitget/RUNBOOK.md
"""
from __future__ import annotations

import sys

_LEGACY_MSG = (
    "[BLOCKED] bitget.sentinel is removed. Use systemd:\n"
    "  dante-bitget-dashboard  (port 8511)\n"
    "  dante-bitget-heatmap    (port 8512)\n"
    "See bitget/RUNBOOK.md\n"
)


def run_sentinel() -> None:
    sys.stderr.write(_LEGACY_MSG)
    raise SystemExit(2)


if __name__ == "__main__":
    run_sentinel()
