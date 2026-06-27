"""
REMOVED — production SSOT: bitget/pipelines/bitget_auto_pilot.py + bitget/deploy/bitget.sh

`python -m bitget.main` is disabled to prevent duplicate pipeline / thread execution.
See bitget/RUNBOOK.md
"""
from __future__ import annotations

import sys

_LEGACY_MSG = (
    "[BLOCKED] bitget.main is removed. Production SSOT:\n"
    "  24/7 daemon : python -m bitget.pipelines.bitget_auto_pilot --daemon\n"
    "              (systemd: dante-bitget-factory)\n"
    "  cron jobs   : bitget/deploy/bitget.sh --scan-all|--daily-audit|...\n"
    "See bitget/RUNBOOK.md\n"
)


def _blocked() -> None:
    sys.stderr.write(_LEGACY_MSG)
    raise SystemExit(2)


if __name__ == "__main__":
    _blocked()
