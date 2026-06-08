"""
Backward-compatible facade — import from `bitget.forward` or this module.

Implementation lives in `bitget.forward._core` (Phase 3 split).
"""
from bitget.forward._core import (  # noqa: F401
    build_practitioner_reality_leaderboard,
    compute_evolved_alpha_bonus_score,
    evaluate_evolved_alpha_formula,
    generate_mutant_strategies,
    init_forward_db,
    log_real_execution,
    run_deep_dive_analysis,
    send_comprehensive_daily_report,
    send_group_practitioner_reports,
    send_telegram_msg,
    sync_real_leaderboard_with_virtual,
    track_daily_positions,
    try_add_virtual_position,
)
from bitget.forward.shared import CONFIG_PATH, DB_PATH, load_system_config, save_system_config

__all__ = [
    "DB_PATH",
    "CONFIG_PATH",
    "TELEGRAM_TOKEN",
    "TELEGRAM_CHAT_ID",
    "send_telegram_msg",
    "load_system_config",
    "save_system_config",
    "init_forward_db",
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
    "evaluate_evolved_alpha_formula",
]

# Legacy module-level names
from bitget.forward.shared import TELEGRAM_CHAT_ID, TELEGRAM_TOKEN  # noqa: E402
