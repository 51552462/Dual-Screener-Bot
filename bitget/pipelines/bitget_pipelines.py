"""
Bitget mode -> Step pipeline mapping (sequential SSOT).

scan / track / daily / reconcile / data_refresh / weekly_evolution
Prelude (scan + daily_audit): meta_governor_sync → artifact_guard → …
"""
from __future__ import annotations

from typing import Callable, Dict, List, Sequence

from bitget.bitget_scan_schedule import ALL_SCAN_SLOTS, ScanSlot
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


def _step_meta_governor_sync() -> None:
    """scan/daily 직전 REGIME + MetaGovernor 동기 (degraded 시 자동 복구)."""
    from bitget.governance.meta_sync import (
        ensure_config_regime_aligned,
        is_bitget_meta_degraded,
        load_bitget_meta_resolved,
        rebuild_bitget_meta_state,
    )

    out = rebuild_bitget_meta_state(force=False, refresh_regime=True)
    align = ensure_config_regime_aligned()
    out["config_regime_align"] = align
    print(f"🛰️ [Bitget] meta_governor_sync: {out}")

    failures: list[str] = []
    if out.get("meta") == "failed":
        failures.append(f"meta_error={out.get('meta_error')}")
    if out.get("regime") == "failed":
        failures.append(f"regime_error={out.get('regime_error')}")
    sync = out.get("config_regime_sync")
    if isinstance(sync, dict) and sync.get("error"):
        failures.append(f"config_regime_sync={sync.get('error')}")
    if isinstance(align, dict) and align.get("error"):
        failures.append(f"config_regime_align={align.get('error')}")

    meta = load_bitget_meta_resolved()
    if is_bitget_meta_degraded(meta):
        rk = meta.get("META_REGIME_KEY", "UNKNOWN")
        st = meta.get("META_GOVERNOR_LAST_RUN_STATUS", "NEVER")
        at = meta.get("META_GOVERNOR_LAST_RUN_AT", "—")
        failures.append(f"meta_degraded regime={rk} status={st} last_at={at}")

    if failures:
        detail = "; ".join(failures)
        try:
            from bitget.governance.meta_alerts import send_meta_critical_alert

            send_meta_critical_alert(
                "meta_governor_sync aborted",
                detail,
                prefix="BITGET_PIPELINE",
            )
        except Exception:
            pass
        raise RuntimeError(
            "meta_governor_sync aborted — refusing stale UNKNOWN report: " + detail
        )


def _step_artifact_guard() -> None:
    from bitget.infra.artifact_guard import ensure_bitget_artifacts

    result = ensure_bitget_artifacts()
    err = result.get("error")
    if err == "no_db":
        raise RuntimeError(
            f"bitget_artifact_guard: market DB missing ({result.get('db')})"
        )
    if err == "schema_incomplete":
        raise RuntimeError(
            f"bitget_artifact_guard: required tables missing "
            f"({(result.get('schema') or {}).get('missing')})"
        )
    if result.get("meta") == "failed":
        raise RuntimeError(
            f"bitget_artifact_guard: meta heal failed ({result.get('meta_error')})"
        )


_META_GOVERNOR_SYNC = StepSpec(
    "meta_governor_sync",
    _step_meta_governor_sync,
    critical=True,
    delay_after_sec=0.5,
)
_META_SYNC_SCAN = StepSpec(
    "meta_governor_sync_scan",
    _step_meta_governor_sync,
    critical=True,
    delay_after_sec=0.5,
)
_ARTIFACT_GUARD = StepSpec("artifact_guard", _step_artifact_guard, critical=True)
_CONFIG_BOOTSTRAP = StepSpec("config_bootstrap", _step_config_bootstrap, critical=False)


def _with_guard(steps: List[StepSpec]) -> List[StepSpec]:
    """Non-scan/daily modes: bootstrap + artifact guard only."""
    return [_CONFIG_BOOTSTRAP, _ARTIFACT_GUARD, *steps]


def _with_scan_prelude(steps: List[StepSpec]) -> List[StepSpec]:
    """scan_*: meta sync → guard → bootstrap → scan body (주식 scan-kr/us 패턴)."""
    return [_META_SYNC_SCAN, _ARTIFACT_GUARD, _CONFIG_BOOTSTRAP, *steps]


def _with_daily_audit_prelude(steps: List[StepSpec]) -> List[StepSpec]:
    """daily_audit: meta sync → guard → bootstrap → sentiment → body."""
    return [
        _META_GOVERNOR_SYNC,
        _ARTIFACT_GUARD,
        _CONFIG_BOOTSTRAP,
        StepSpec("sentiment_mining", _step_sentiment, critical=False),
        *steps,
    ]


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


def _step_pil_practitioner_reports() -> None:
    from bitget.forward.reports import send_group_practitioner_reports

    send_group_practitioner_reports()


def _step_comprehensive_report() -> None:
    from bitget.forward_tester import send_comprehensive_daily_report

    send_comprehensive_daily_report()


def _step_ai_overseer() -> None:
    from bitget.ai_overseer import run_ai_auditor

    run_ai_auditor()


def _step_doomsday_radar() -> None:
    from bitget.macro_doomsday_bot import run_doomsday_radar

    run_doomsday_radar()


def _step_report_pipeline_hydrate() -> None:
    from bitget.report_pipeline_hydrate import ensure_bitget_report_pipeline_data

    ensure_bitget_report_pipeline_data()


def _step_canary_export() -> None:
    """코인 선행 레이더 산출 → bitget_canary_state.json 원자적 기록 (data_refresh tail)."""
    from bitget.canary_exporter import run_canary_export

    run_canary_export()


def _step_doomsday_bridge_sync() -> None:
    from bitget.doomsday_bridge import sync_doomsday_to_bitget_config

    sync_doomsday_to_bitget_config()


def _step_reporter_cleanup_zombie() -> None:
    from bitget.forward.shared import reporter_cleanup_zombie_forward_trades

    reporter_cleanup_zombie_forward_trades()


def _step_forward_trade_identity() -> None:
    from bitget.forward.forward_trade_identity import run_identity_repair_all

    run_identity_repair_all()


def _step_weekly_evolution() -> None:
    from bitget.auto_pilot import run_autonomous_analysis

    run_autonomous_analysis()


def _step_weekly_flow_master() -> None:
    """주식 factory weekly_master → weekly_flow_master 패리티."""
    from bitget.auto_pilot import send_weekly_flow_master_report

    send_weekly_flow_master_report()


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


def _step_doomsday_bridge_scan() -> None:
    from bitget.doomsday_bridge import sync_doomsday_to_bitget_config

    sync_doomsday_to_bitget_config()


def _make_engine_scan_step(slot: ScanSlot) -> StepSpec:
    market_raw = "spot" if slot.market == "SPOT" else "futures"

    def _fn() -> None:
        from bitget.pipelines.scanner_hooks import run_engine_scan, run_supernova_futures, run_supernova_spot

        if slot.scanner_key == "supernova":
            if slot.market == "SPOT":
                run_supernova_spot()
            else:
                run_supernova_futures()
        else:
            run_engine_scan(market=market_raw, scanner_key=slot.scanner_key)

    return StepSpec(
        f"scan_{market_raw}_{slot.scanner_key}" + ("" if slot.cycle == 1 else "_r2"),
        _fn,
        critical=True,
        delay_after_sec=2.0 if slot.scanner_key == "supernova" else 0.5,
    )


def _prelude_full() -> List[StepSpec]:
    return [
        _META_SYNC_SCAN,
        _ARTIFACT_GUARD,
        _CONFIG_BOOTSTRAP,
        StepSpec("gap_heal", _step_gap_heal, critical=False),
        StepSpec("data_refresh_incremental", _step_data_refresh, critical=False),
    ]


def _prelude_light() -> List[StepSpec]:
    return [
        _ARTIFACT_GUARD,
        StepSpec("gap_heal", _step_gap_heal, critical=False),
        StepSpec("data_refresh_incremental", _step_data_refresh, critical=False),
    ]


def _prelude_none() -> List[StepSpec]:
    return [_ARTIFACT_GUARD]


def _build_staggered_scan_pipeline(slot: ScanSlot) -> List[StepSpec]:
    if slot.prelude == "full":
        prelude = _prelude_full()
    elif slot.prelude == "light":
        prelude = _prelude_light()
    else:
        prelude = _prelude_none()

    scan_step = _make_engine_scan_step(slot)
    tail: List[StepSpec] = []
    if slot.tail_doomsday:
        tail.append(
            StepSpec("doomsday_bridge_sync", _step_doomsday_bridge_scan, critical=False, delay_after_sec=0.3)
        )
    if slot.tail_shadow:
        tail.append(StepSpec("shadow_eval", _step_shadow_eval, critical=False))
    if slot.tail_track:
        if slot.market == "SPOT":
            tail.append(
                StepSpec("track_spot", _step_track_spot, critical=False, delay_after_sec=0.3)
            )
        else:
            tail.append(
                StepSpec("track_futures", _step_track_futures, critical=False, delay_after_sec=0.3)
            )

    return [*prelude, scan_step, *tail]


def _staggered_scan_pipelines() -> Dict[str, Callable[[], List[StepSpec]]]:
    out: Dict[str, Callable[[], List[StepSpec]]] = {}

    def _builder_for(slot: ScanSlot) -> Callable[[], List[StepSpec]]:
        def _build() -> List[StepSpec]:
            return _build_staggered_scan_pipeline(slot)

        return _build

    for slot in ALL_SCAN_SLOTS:
        out[slot.mode] = _builder_for(slot)
    return out


def _pipeline_data_refresh() -> List[StepSpec]:
    return _with_guard(
        [
            StepSpec("gap_heal", _step_gap_heal, critical=False),
            StepSpec("data_refresh", _step_data_refresh, critical=True),
            # 선행 레이더: OHLCV 갱신 직후 OI/펀딩/BTC·VIX 로 canary JSON 원자적 기록.
            StepSpec("canary_export", _step_canary_export, critical=False, delay_after_sec=0.3),
        ]
    )


def _pipeline_scan_spot() -> List[StepSpec]:
    return _with_scan_prelude(
        [
            StepSpec("supernova_spot", _step_supernova_spot, critical=False),
            StepSpec("scan_spot", _step_scan_spot, critical=True),
            StepSpec("track_spot", _step_track_spot, critical=False, delay_after_sec=0.5),
        ]
    )


def _pipeline_scan_futures() -> List[StepSpec]:
    return _with_scan_prelude(
        [
            StepSpec("supernova_futures", _step_supernova_futures, critical=False),
            StepSpec("scan_futures", _step_scan_futures, critical=True),
            StepSpec("track_futures", _step_track_futures, critical=False, delay_after_sec=0.5),
        ]
    )


def _pipeline_scan_all() -> List[StepSpec]:
    return _with_scan_prelude(
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
    return _with_daily_audit_prelude(
        [
            StepSpec("doomsday_radar", _step_doomsday_radar, critical=False),
            StepSpec("report_pipeline_hydrate", _step_report_pipeline_hydrate, critical=False),
            StepSpec("track_spot", _step_track_spot, critical=True),
            StepSpec("track_futures", _step_track_futures, critical=True),
            StepSpec("deep_dive_spot", _step_deep_dive_spot, critical=False, delay_after_sec=0.5),
            StepSpec("deep_dive_futures", _step_deep_dive_futures, critical=False, delay_after_sec=0.5),
            StepSpec("doomsday_bridge_sync", _step_doomsday_bridge_sync, critical=False, delay_after_sec=0.3),
            StepSpec(
                "reporter_cleanup_zombie_forward_trades",
                _step_reporter_cleanup_zombie,
                critical=False,
            ),
            StepSpec("forward_trade_identity", _step_forward_trade_identity, critical=False),
            StepSpec("pil_practitioner_reports", _step_pil_practitioner_reports, critical=False, delay_after_sec=0.5),
            StepSpec("comprehensive_report", _step_comprehensive_report, critical=False),
            StepSpec("ai_overseer", _step_ai_overseer, critical=False),
            StepSpec("reconcile", _step_reconcile, critical=False),
        ]
    )


def _pipeline_weekly_evolution() -> List[StepSpec]:
    return _with_guard(
        [
            StepSpec("weekly_evolution", _step_weekly_evolution, critical=True, delay_after_sec=1.0),
            StepSpec("weekly_flow_master", _step_weekly_flow_master, critical=True),
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
PIPELINE_BUILDERS.update(_staggered_scan_pipelines())


def get_pipeline(mode: str) -> Sequence[StepSpec]:
    key = (mode or "").strip().lower()
    builder = PIPELINE_BUILDERS.get(key)
    if builder is None:
        raise KeyError(f"unknown bitget pipeline mode: {mode!r}")
    return builder()
