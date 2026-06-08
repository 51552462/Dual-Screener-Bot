"""Bitget virtual forward ledger package (stock `forward/` mirror)."""

from bitget.forward.ledger import track_daily_positions, try_add_virtual_position
from bitget.forward.shared import (
    DB_PATH,
    CONFIG_PATH,
    init_forward_db,
    load_system_config,
    save_system_config,
    send_telegram_msg,
)
from bitget.forward.reports import (
    run_deep_dive_analysis,
    send_comprehensive_daily_report,
    send_group_practitioner_reports,
)
from bitget.forward.mutant import generate_mutant_strategies
from bitget.forward.execution_bridge import (
    build_practitioner_reality_leaderboard,
    log_real_execution,
    sync_real_leaderboard_with_virtual,
)

__all__ = [
    "DB_PATH",
    "CONFIG_PATH",
    "init_forward_db",
    "load_system_config",
    "save_system_config",
    "send_telegram_msg",
    "try_add_virtual_position",
    "track_daily_positions",
    "log_real_execution",
    "sync_real_leaderboard_with_virtual",
    "build_practitioner_reality_leaderboard",
    "generate_mutant_strategies",
    "send_group_practitioner_reports",
    "send_comprehensive_daily_report",
    "run_deep_dive_analysis",
    "compute_evolved_alpha_bonus_score",
]

# Optional re-export used by supernova_hunter
from bitget.forward.gates import compute_evolved_alpha_bonus_score  # noqa: E402
