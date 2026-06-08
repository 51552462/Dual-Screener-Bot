"""
Bitget mode -> Step pipeline mapping (sequential SSOT).

Phase 2: scan / track / daily / reconcile / data_refresh / weekly_evolution
"""
from __future__ import annotations

import os
import sqlite3
from typing import Callable, Dict, List, Sequence

from bitget.infra.runtime import StepSpec


# ---------------------------------------------------------------------------
# Scanner hooks — delegate only; never reimplement signal logic here.
# ---------------------------------------------------------------------------
def _step_supernova_spot() -> None:
    from bitget.pipelines.scanner_hooks import run_supernova_spot

    run_supernova_spot()


def _step_supernova_futures() -> None:
    from bitget.pipelines.scanner_hooks import run_supernova_futures

    run_supernova_futures()


def _step_scan_all() -> None:
    from bitget.pipelines.scanner_hooks import run_master_scan

    run_master_scan()


def _step_scan_spot() -> None:
    from bitget.pipelines.scanner_hooks import run_master_scan

    run_master_scan(market_filter="spot")


def _step_scan_futures() -> None:
    from bitget.pipelines.scanner_hooks import run_master_scan

    run_master_scan(market_filter="futures")


def _step_config_bootstrap() -> None:
    from bitget.infra.config_manager import bootstrap_from_json_if_empty

    bootstrap_from_json_if_empty()


def _step_artifact_guard() -> None:
    from bitget.infra.data_paths import market_data_db_path

    db = market_data_db_path()
    if not os.path.isfile(db):
        raise RuntimeError(f"bitget_artifact_guard: market DB missing ({db})")
    conn = sqlite3.connect(db, timeout=15.0)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1"
        ).fetchone()
        if row is None:
            raise RuntimeError(f"bitget_artifact_guard: no tables in {db}")
    finally:
        conn.close()


_CONFIG_BOOTSTRAP = StepSpec("config_bootstrap", _step_config_bootstrap, critical=False)
_ARTIFACT_GUARD = StepSpec("artifact_guard", _step_artifact_guard, critical=True)


def _with_guard(steps: List[StepSpec]) -> List[StepSpec]:
    return [_CONFIG_BOOTSTRAP, _ARTIFACT_GUARD, *steps]


def _step_data_refresh() -> None:
    from bitget.mtf_data_updater import run_mtf_update

    run_mtf_update()


def _step_track_spot() -> None:
    from bitget.forward_tester import track_daily_positions

    track_daily_positions("spot")


def _step_track_futures() -> None:
    from bitget.forward_tester import track_daily_positions

    track_daily_positions("futures")


def _step_reconcile() -> None:
    from bitget.oms import run_scheduled_reconciliation

    run_scheduled_reconciliation()


def _step_sentiment() -> None:
    from bitget.sentiment_miner import run_sentiment_mining

    run_sentiment_mining()


def _step_deep_dive_spot() -> None:
    from bitget.forward_tester import run_deep_dive_analysis

    run_deep_dive_analysis("spot")


def _step_deep_dive_futures() -> None:
    from bitget.forward_tester import run_deep_dive_analysis

    run_deep_dive_analysis("futures")


def _step_comprehensive_report() -> None:
    from bitget.forward_tester import send_comprehensive_daily_report

    send_comprehensive_daily_report()


def _step_ai_overseer() -> None:
    from bitget.ai_overseer import run_ai_auditor

    run_ai_auditor()


def _step_doomsday_radar() -> None:
    from bitget.macro_doomsday_bot import run_doomsday_radar

    run_doomsday_radar()


def _step_weekly_evolution() -> None:
    from bitget.auto_pilot import run_autonomous_analysis

    run_autonomous_analysis()


def _step_shadow_eval() -> None:
    from bitget.shadow_performance_tracker import run_shadow_performance_evaluation

    run_shadow_performance_evaluation()


def _step_gap_heal() -> None:
    from bitget.data.gap_healer import run_scheduled_gap_heal

    run_scheduled_gap_heal()


def _step_snapshot() -> None:
    from bitget.infra.snapshot_service import run_snapshot_job

    result = run_snapshot_job()
    if not result.get("ok"):
        raise RuntimeError(result.get("error") or "snapshot backup failed")


def _step_record_baseline() -> None:
    from bitget.validation.runner import run_record_baseline

    run_record_baseline()


def _step_validate_parity() -> None:
    from bitget.validation.runner import run_validate_parity

    run_validate_parity()


def _step_load_test() -> None:
    from bitget.validation.runner import run_load_test_job

    run_load_test_job()


def _step_cutover_check() -> None:
    from bitget.validation.runner import run_cutover_check

    run_cutover_check()


def _step_validate_all() -> None:
    from bitget.validation.runner import run_validate_all

    run_validate_all()


def _step_start_parallel() -> None:
    from bitget.validation.runner import run_start_parallel_run

    run_start_parallel_run(note="pipeline start_parallel")


def _pipeline_data_refresh() -> List[StepSpec]:
    return _with_guard(
        [
            StepSpec("gap_heal", _step_gap_heal, critical=False),
            StepSpec("data_refresh", _step_data_refresh, critical=True),
        ]
    )


def _pipeline_scan_spot() -> List[StepSpec]:
    return _with_guard(
        [
            StepSpec("supernova_spot", _step_supernova_spot, critical=False),
            StepSpec("scan_spot", _step_scan_spot, critical=True),
            StepSpec("track_spot", _step_track_spot, critical=False, delay_after_sec=0.5),
        ]
    )


def _pipeline_scan_futures() -> List[StepSpec]:
    return _with_guard(
        [
            StepSpec("supernova_futures", _step_supernova_futures, critical=False),
            StepSpec("scan_futures", _step_scan_futures, critical=True),
            StepSpec("track_futures", _step_track_futures, critical=False, delay_after_sec=0.5),
        ]
    )


def _pipeline_scan_all() -> List[StepSpec]:
    return _with_guard(
        [
            StepSpec("gap_heal", _step_gap_heal, critical=False),
            StepSpec("data_refresh_incremental", _step_data_refresh, critical=False),
            StepSpec("supernova_spot", _step_supernova_spot, critical=False),
            StepSpec("scan_spot", _step_scan_spot, critical=True),
            StepSpec("supernova_futures", _step_supernova_futures, critical=False),
            StepSpec("scan_futures", _step_scan_futures, critical=True),
            StepSpec("track_spot", _step_track_spot, critical=False, delay_after_sec=0.3),
            StepSpec("track_futures", _step_track_futures, critical=False, delay_after_sec=0.3),
            StepSpec("shadow_eval", _step_shadow_eval, critical=False),
        ]
    )


def _pipeline_track_positions() -> List[StepSpec]:
    return _with_guard(
        [
            StepSpec("track_spot", _step_track_spot, critical=True),
            StepSpec("track_futures", _step_track_futures, critical=True),
        ]
    )


def _pipeline_reconcile() -> List[StepSpec]:
    return _with_guard([StepSpec("reconcile", _step_reconcile, critical=True)])


def _pipeline_daily_audit() -> List[StepSpec]:
    return _with_guard(
        [
            StepSpec("sentiment_mining", _step_sentiment, critical=False),
            StepSpec("doomsday_radar", _step_doomsday_radar, critical=False),
            StepSpec("track_spot", _step_track_spot, critical=True),
            StepSpec("track_futures", _step_track_futures, critical=True),
            StepSpec("deep_dive_spot", _step_deep_dive_spot, critical=False, delay_after_sec=0.5),
            StepSpec("deep_dive_futures", _step_deep_dive_futures, critical=False, delay_after_sec=0.5),
            StepSpec("comprehensive_report", _step_comprehensive_report, critical=False),
            StepSpec("ai_overseer", _step_ai_overseer, critical=False),
            StepSpec("reconcile", _step_reconcile, critical=False),
        ]
    )


def _pipeline_weekly_evolution() -> List[StepSpec]:
    return _with_guard(
        [
            StepSpec("weekly_evolution", _step_weekly_evolution, critical=True),
        ]
    )


def _pipeline_health() -> List[StepSpec]:
    from bitget.pipelines.runner import step_infra_health

    return [StepSpec("infra_health", step_infra_health, critical=True)]


PIPELINE_BUILDERS: Dict[str, Callable[[], List[StepSpec]]] = {
    "health": _pipeline_health,
    "data_refresh": _pipeline_data_refresh,
    "scan_spot": _pipeline_scan_spot,
    "scan_futures": _pipeline_scan_futures,
    "scan_all": _pipeline_scan_all,
    "track_positions": _pipeline_track_positions,
    "reconcile": _pipeline_reconcile,
    "daily_audit": _pipeline_daily_audit,
    "weekly_evolution": _pipeline_weekly_evolution,
    "gap_heal": lambda: _with_guard([StepSpec("gap_heal", _step_gap_heal, critical=True)]),
    "snapshot": lambda: _with_guard([StepSpec("snapshot", _step_snapshot, critical=True)]),
    "record_baseline": lambda: _with_guard(
        [StepSpec("record_baseline", _step_record_baseline, critical=True)]
    ),
    "validate": lambda: _with_guard([StepSpec("validate_parity", _step_validate_parity, critical=True)]),
    "load_test": lambda: _with_guard([StepSpec("load_test", _step_load_test, critical=True)]),
    "cutover_check": lambda: _with_guard(
        [StepSpec("cutover_check", _step_cutover_check, critical=False)]
    ),
    "validate_all": lambda: _with_guard(
        [
            StepSpec("validate_parity", _step_validate_parity, critical=True),
            StepSpec("load_test", _step_load_test, critical=True),
            StepSpec("cutover_check", _step_cutover_check, critical=False),
        ]
    ),
    "start_parallel": lambda: _with_guard(
        [StepSpec("start_parallel", _step_start_parallel, critical=True)]
    ),
}


def get_pipeline(mode: str) -> Sequence[StepSpec]:
    key = (mode or "").strip().lower()
    builder = PIPELINE_BUILDERS.get(key)
    if builder is None:
        raise KeyError(f"unknown bitget pipeline mode: {mode!r}")
    return builder()
