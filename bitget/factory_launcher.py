"""
REMOVED — production SSOT: systemd dante-bitget-factory + dante-bitget-dashboard.

`python -m bitget.factory_launcher` is disabled (was sentinel subprocess launcher).
See bitget/RUNBOOK.md
"""
from __future__ import annotations

import sys

_LEGACY_MSG = (
    "[BLOCKED] bitget.factory_launcher is removed. Use:\n"
    "  systemctl start dante-bitget-factory dante-bitget-dashboard dante-bitget-heatmap\n"
    "  or: bitget/deploy/bitget.sh --daemon  (→ bitget_auto_pilot)\n"
    "See bitget/RUNBOOK.md\n"
)


def launch_factory() -> None:
    sys.stderr.write(_LEGACY_MSG)
    raise SystemExit(2)


if __name__ == "__main__":
    launch_factory()
