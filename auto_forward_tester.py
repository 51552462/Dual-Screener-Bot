"""
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
        "[forward] run_daily_scheduler disabled - use cron: factory.sh --daily-kr|--daily-us"
    )
    return
