"""Split auto_forward_tester.py into forward/shared|ledger|deep_dive."""
from __future__ import annotations

import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "auto_forward_tester.py")
FWD = os.path.join(ROOT, "forward")

# line numbers 1-based inclusive
SHARED_END = 2045
LEDGER_START = 2046
LEDGER_END = 2429
DEEP_START = 2430
DEEP_END = 3557  # before scheduler


def slice_lines(path: str, start: int, end: int) -> str:
    lines = open(path, encoding="utf-8").read().splitlines(keepends=True)
    return "".join(lines[start - 1 : end])


def main() -> None:
    os.makedirs(FWD, exist_ok=True)
    shared_body = slice_lines(SRC, 1, SHARED_END)
    ledger_body = slice_lines(SRC, LEDGER_START, LEDGER_END)
    deep_body = slice_lines(SRC, DEEP_START, DEEP_END)

    shared_hdr = '"""Forward equity shared — DB, telegram, config, helpers."""\n'
    ledger_hdr = (
        '"""Forward ledger — track_daily_positions, virtual entries."""\n'
        "from forward.shared import *  # noqa: F403\n\n"
    )
    deep_hdr = (
        '"""Forward reporting — deep dive, comprehensive daily, practitioner."""\n'
        "from forward.shared import *  # noqa: F403\n\n"
    )

    with open(os.path.join(FWD, "shared.py"), "w", encoding="utf-8") as fh:
        fh.write(shared_hdr + shared_body)
    with open(os.path.join(FWD, "ledger.py"), "w", encoding="utf-8") as fh:
        fh.write(ledger_hdr + ledger_body)
    with open(os.path.join(FWD, "deep_dive.py"), "w", encoding="utf-8") as fh:
        fh.write(deep_hdr + deep_body)

    init = '''"""
Forward equity package — ledger + reporting (split from auto_forward_tester).
"""
from forward.shared import *  # noqa: F403
from forward.ledger import track_daily_positions, try_add_virtual_position, init_forward_db
from forward.deep_dive import (
    run_deep_dive_analysis,
    send_comprehensive_daily_report,
    send_group_practitioner_reports,
)

__all__ = [
    "track_daily_positions",
    "try_add_virtual_position",
    "init_forward_db",
    "run_deep_dive_analysis",
    "send_comprehensive_daily_report",
    "send_group_practitioner_reports",
    "send_telegram_msg",
    "load_system_config",
    "save_system_config",
    "DB_PATH",
]
'''
    with open(os.path.join(FWD, "__init__.py"), "w", encoding="utf-8") as fh:
        fh.write(init)

    facade = '''"""
Compatibility facade — implementation lives under forward/.
Cron SSOT: factory.sh only (internal Python scheduler disabled).
"""
from forward.shared import *  # noqa: F403
from forward.ledger import *  # noqa: F403
from forward.deep_dive import *  # noqa: F403
from forward import (  # explicit re-exports
    track_daily_positions,
    try_add_virtual_position,
    init_forward_db,
    run_deep_dive_analysis,
    send_comprehensive_daily_report,
    send_group_practitioner_reports,
)


def run_daily_scheduler():
    """
    DISABLED — equity schedule SSOT is Linux cron via factory.sh only.
    Prevents double-firing with daily-kr / daily-us pipelines.
    """
    print(
        "ℹ️ [포워드 장부] run_daily_scheduler 비활성 — "
        "스케줄은 cron(factory.sh --daily-kr|--daily-us) 전용입니다."
    )
    return
'''
    with open(SRC, "w", encoding="utf-8") as fh:
        fh.write(facade)
    print("split complete:", FWD)


if __name__ == "__main__":
    main()
