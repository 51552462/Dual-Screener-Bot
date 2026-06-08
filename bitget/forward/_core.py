"""Backward-compat aggregate — prefer `bitget.forward.*` submodules."""
from bitget.forward.execution_bridge import (
    build_practitioner_reality_leaderboard,
    log_real_execution,
    sync_real_leaderboard_with_virtual,
)
from bitget.forward.gates import (
    compute_evolved_alpha_bonus_score,
    evaluate_evolved_alpha_formula,
)
from bitget.forward.ledger import track_daily_positions, try_add_virtual_position
from bitget.forward.mutant import generate_mutant_strategies
from bitget.forward.reports import (
    run_deep_dive_analysis,
    send_comprehensive_daily_report,
    send_group_practitioner_reports,
)
from bitget.forward.shared import init_forward_db, send_telegram_msg

__all__ = [
    "init_forward_db",
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
    "evaluate_evolved_alpha_formula",
]
