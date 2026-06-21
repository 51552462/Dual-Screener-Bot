"""
Factory mode → Step 파이프라인 매핑 (순차 실행 SSOT).

daily_audit* 동기 파이프라인 (순서 고정):
  meta_governor_sync → factory_artifact_guard → sentiment_mining
  → (KR: track_us → sector_spillover_refresh → us_cross_market_publish → kr_hydrate)
  → track → deep_dive → comprehensive_daily_report → ai_overseer

factory_runtime.run_step 은 각 StepSpec.fn() 을 동기 호출한다 (비동기 spawn 없음).
"""
from __future__ import annotations

from typing import Dict, List, Sequence

from factory_runtime import StepSpec
from factory_scan_schedule import ALL_SCAN_SLOTS, ScanSlot


# --- Step implementations (lazy import) ---


def _step_meta_governor_sync() -> None:
    """리포트·감사 전 REGIME_ANALYSIS + MetaGovernor 동기 (degraded 시 자동 복구)."""
    from meta_governor_consumer import load_meta_state_resolved
    from meta_state_store import (
        ensure_config_regime_aligned,
        is_meta_state_degraded,
        rebuild_meta_state,
    )

    out = rebuild_meta_state(force=False, refresh_regime=True)
    align = ensure_config_regime_aligned()
    out["config_regime_align"] = align
    print(f"🛰️ [Factory] meta_governor_sync: {out}")

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

    meta = load_meta_state_resolved()
    if is_meta_state_degraded(meta):
        rk = meta.get("META_REGIME_KEY", "UNKNOWN")
        st = meta.get("META_GOVERNOR_LAST_RUN_STATUS", "NEVER")
        at = meta.get("META_GOVERNOR_LAST_RUN_AT", "—")
        failures.append(f"meta_degraded regime={rk} status={st} last_at={at}")

    if failures:
        raise RuntimeError(
            "meta_governor_sync aborted — refusing stale UNKNOWN report: "
            + "; ".join(failures)
        )

    try:
        from evolution.fluid_evolution_bridge import post_meta_governor_fluid_sync

        post_meta_governor_fluid_sync()
    except Exception as _fluid_ex:
        print(f"⚠️ [Factory] fluid evolution post-meta sync skip: {_fluid_ex}")


_META_GOVERNOR_SYNC = StepSpec(
    "meta_governor_sync",
    _step_meta_governor_sync,
    critical=True,
    delay_after_sec=0.5,
)


def _step_artifact_guard() -> None:
    from factory_artifact_guard import ensure_factory_artifacts

    result = ensure_factory_artifacts()
    err = result.get("error")
    if err == "no_db":
        raise RuntimeError(
            f"factory_artifact_guard: market DB missing ({result.get('db')})"
        )
    if err == "schema_incomplete":
        raise RuntimeError(
            f"factory_artifact_guard: required tables missing "
            f"({(result.get('schema') or {}).get('main')})"
        )


_ARTIFACT_GUARD = StepSpec(
    "factory_artifact_guard",
    _step_artifact_guard,
    critical=True,
    delay_after_sec=0.0,
)


def _with_artifact_guard(steps: List[StepSpec]) -> List[StepSpec]:
    return [_ARTIFACT_GUARD, *steps]


def _step_sentiment_mining() -> None:
    """동기 실행 — 일일 통합 리포트 직전 당일(KST) news_data.sqlite 갱신."""
    from sentiment_miner import run_sentiment_mining
    from news_data_paths import assert_sentiment_fresh_for_report, today_kst_str

    out = run_sentiment_mining()
    print(f"🧠 [Factory] sentiment_mining 완료 (KST {today_kst_str()}): {out}")
    if not assert_sentiment_fresh_for_report():
        print(
            "⚠️ [Factory] 당일 센티먼트 행 없음 — comprehensive 리포트에 "
            "'데이터 없음' 또는 스냅샷 날짜가 표시됩니다 (GEMINI/헤드라인 실패 가능)."
        )


def _step_sector_spillover_refresh() -> None:
    from sector_spillover_refresh import refresh_sector_spillover_state

    out = refresh_sector_spillover_state(save=True)
    print(f"🔄 [Factory] sector_spillover_refresh: {out}")


def _step_us_cross_market_publish() -> None:
    """US 파이프라인 종료 — DB 기반 스냅샷 publish (scan-us 재실행 없음)."""
    from cross_market_ssot import publish_us_snapshot_after_pipeline

    publish_us_snapshot_after_pipeline()


def _step_kr_cross_market_hydrate() -> None:
    """KR 직전 — SSOT 로드·graceful mode (US 스캐너 미호출)."""
    from cross_market_ssot import hydrate_kr_runtime_from_ssot

    hydrate_kr_runtime_from_ssot()


def _step_cross_market_theme_snapshot() -> None:
    """
    US forward DB → spillover KV → us_kr_theme_bridge → CROSS_MARKET_SSOT + sqlite snapshot.
    KR 스캐너/가상매매가 US 주도 테마를 실제 가중치에 쓰도록 SSOT 재발행.
    """
    from cross_market_ssot import (
        ensure_cross_market_schema,
        hydrate_kr_runtime_from_ssot,
        publish_us_market_snapshot,
    )
    from sector_spillover_refresh import refresh_sector_spillover_state

    ensure_cross_market_schema()
    spill = {"reason": "unknown"}
    try:
        spill = refresh_sector_spillover_state(save=True)
    except Exception as ex:
        print(f"⚠️ [Factory] spillover refresh degraded: {ex}")

    try:
        ssot = publish_us_market_snapshot(source="factory_theme_snapshot", save=True)
    except Exception as ex:
        print(f"⚠️ [Factory] cross_market publish degraded: {ex}")
        # publish 실패여도 hydrate는 진행해 stale 고착을 막는다.
        ssot = hydrate_kr_runtime_from_ssot()
    else:
        hydrate_kr_runtime_from_ssot()
    print(
        f"🌐 [Factory] cross_market_theme_snapshot: spill={spill.get('reason')} "
        f"mode={ssot.get('mode')} kr_std={ssot.get('kr_sector_std')}"
    )


def _step_sync_us_toxic_ml_ssot() -> None:
    """us_toxic_ml_antipatterns.json → system_config (blackhole/ graveyard 산출물 반영)."""
    import json
    import os

    from config_manager import load_system_config, save_system_config

    cfg = load_system_config() or {}
    root = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(root, "us_toxic_ml_antipatterns.json")
    if not os.path.isfile(path):
        print(f"💡 [Factory] us_toxic_ml SSOT skip: missing {path}")
        return
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        cfg["US_TOXIC_ML_ANTIPATTERNS"] = payload
        try:
            from toxic_decay_bandit import sync_decayed_toxic_to_config

            decay_out = sync_decayed_toxic_to_config(cfg)
        except Exception as _dec_ex:
            decay_out = {"error": str(_dec_ex)}
        save_system_config(cfg)
        n_rules = len(payload.get("rules", payload)) if isinstance(payload, dict) else 0
        print(f"🧪 [Factory] US_TOXIC_ML_ANTIPATTERNS synced ({n_rules} rules) decay={decay_out}")
    except (OSError, json.JSONDecodeError) as ex:
        print(f"⚠️ [Factory] us_toxic_ml sync failed: {ex}")


_US_CROSS_MARKET_PUBLISH = StepSpec(
    "us_cross_market_publish",
    _step_us_cross_market_publish,
    critical=False,
    delay_after_sec=0.5,
)

_KR_CROSS_MARKET_HYDRATE = StepSpec(
    "kr_cross_market_hydrate",
    _step_kr_cross_market_hydrate,
    critical=False,
    delay_after_sec=0.0,
)

_CROSS_MARKET_THEME_SNAPSHOT = StepSpec(
    "cross_market_theme_snapshot",
    _step_cross_market_theme_snapshot,
    critical=False,
    delay_after_sec=0.5,
)

_SYNC_US_TOXIC_ML = StepSpec(
    "sync_us_toxic_ml_ssot",
    _step_sync_us_toxic_ml_ssot,
    critical=False,
    delay_after_sec=0.0,
)


def _step_doomsday_bridge_sync() -> None:
    from doomsday_bridge import sync_doomsday_to_system_config

    out = sync_doomsday_to_system_config(
        alert_on_escalation=True,
        run_inverse_cycle=True,
    )
    print(f"🛰️ [Factory] doomsday_bridge: {out}")


_DOOMSDAY_BRIDGE = StepSpec(
    "doomsday_bridge_sync",
    _step_doomsday_bridge_sync,
    critical=False,
    delay_after_sec=0.5,
)


def _step_report_hydrate_kr() -> None:
    from report_pipeline_hydrate import ensure_report_pipeline_data

    ensure_report_pipeline_data(market="KR", refresh_macro=True, refresh_ohlcv=True)


def _step_report_hydrate_us() -> None:
    from report_pipeline_hydrate import ensure_report_pipeline_data

    ensure_report_pipeline_data(market="US", refresh_macro=True, refresh_ohlcv=True)


def _step_report_hydrate_both() -> None:
    from report_pipeline_hydrate import ensure_report_pipeline_data

    ensure_report_pipeline_data(market=None, refresh_macro=True, refresh_ohlcv=True)


_REPORT_HYDRATE_KR = StepSpec(
    "report_pipeline_hydrate_kr",
    _step_report_hydrate_kr,
    critical=True,
    delay_after_sec=1.0,
)

_REPORT_HYDRATE_US = StepSpec(
    "report_pipeline_hydrate_us",
    _step_report_hydrate_us,
    critical=True,
    delay_after_sec=1.0,
)

_REPORT_HYDRATE_BOTH = StepSpec(
    "report_pipeline_hydrate_both",
    _step_report_hydrate_both,
    critical=True,
    delay_after_sec=1.0,
)


def _step_comprehensive_daily_report() -> None:
    from auto_forward_tester import send_comprehensive_daily_report

    send_comprehensive_daily_report(
        refresh_sentiment=False,
        refresh_sector_spillover=False,
        refresh_meta_governor=False,
        cleanup_zombie_trades=False,
    )


_SECTOR_SPILLOVER_REFRESH = StepSpec(
    "sector_spillover_refresh",
    _step_sector_spillover_refresh,
    critical=False,
    delay_after_sec=1.0,
)


_SENTIMENT_MINING = StepSpec(
    "sentiment_mining",
    _step_sentiment_mining,
    critical=False,
    delay_after_sec=2.0,
)

_COMPREHENSIVE_REPORT = StepSpec(
    "comprehensive_daily_report",
    _step_comprehensive_daily_report,
    critical=False,
    delay_after_sec=3.0,
)


def _step_pil_practitioner_reports(*, markets: tuple[str, ...] = ("KR", "US")) -> None:
    """PIL 실무자 리포트 + ZOMBIE → Kelly=0 / RETIRED (MetaGovernor 자동 반영)."""
    from auto_forward_tester import send_group_practitioner_reports

    send_group_practitioner_reports(
        cleanup_zombie_trades=False,
        markets=markets,
    )


def _step_pil_kr() -> None:
    _step_pil_practitioner_reports(markets=("KR",))


def _step_pil_us() -> None:
    _step_pil_practitioner_reports(markets=("US",))


_PIL_PRACTITIONER = StepSpec(
    "pil_practitioner_reports",
    lambda: _step_pil_practitioner_reports(markets=("KR", "US")),
    critical=False,
    delay_after_sec=2.0,
)

_PIL_PRACTITIONER_KR = StepSpec(
    "pil_practitioner_reports_kr",
    _step_pil_kr,
    critical=False,
    delay_after_sec=2.0,
)

_PIL_PRACTITIONER_US = StepSpec(
    "pil_practitioner_reports_us",
    _step_pil_us,
    critical=False,
    delay_after_sec=2.0,
)


def _step_reporter_cleanup_zombie_forward_trades() -> None:
    from forward.shared import _reporter_cleanup_zombie_forward_trades

    try:
        nz = _reporter_cleanup_zombie_forward_trades()
        if nz:
            print(f"🧹 [Factory] reporter cleanup: zombie OPEN 정리 {nz}건")
    except Exception as e:
        # 원래 deep_dive 내부에서 try/except로 “계속” 처리하던 작업 — 여기서도 치명 실패로 전환하지 않음.
        print(f"⚠️ [Factory] reporter cleanup zombie 정리 스킵: {e}")


_REPORTER_CLEANUP_ZOMBIE = StepSpec(
    "reporter_cleanup_zombie_forward_trades",
    _step_reporter_cleanup_zombie_forward_trades,
    critical=False,
    delay_after_sec=0.0,
)


def _step_us_data_incremental_update() -> None:
    """daily_audit_us / scan-us — US OHLCV 증분 갱신 (KR 07:00 bulk 대칭)."""
    from data_updater import run_us_incremental_db_update

    out = run_us_incremental_db_update()
    print(f"🇺🇸 [Factory] us_data_incremental: {out}")


def _step_us_fluid_health_prelude() -> None:
    from evolution.us_fluid_upstream_bridge import run_us_health_fluid_prelude

    run_us_health_fluid_prelude(context="daily")


def _step_us_post_incremental_upstream() -> None:
    from evolution.us_fluid_upstream_bridge import run_post_us_incremental_upstream

    out = run_post_us_incremental_upstream(context="daily")
    print(f"🌊 [Factory] us_post_incremental_upstream: {out}")


def _step_us_health_gate(context: str = "scan") -> None:
    """US 혈관 검사 — 치명 이슈 시 CRITICAL 알림 (repair는 다음 스텝)."""
    from factory_us_health import assess_us_pipeline_health, format_us_health_log_line

    rep = assess_us_pipeline_health()
    print(f"🩺 [Factory] us_health_gate({context}): {format_us_health_log_line(rep)}")
    if rep.get("critical_failures"):
        print(f"⚠️ [Factory] US critical: {rep['critical_failures']}")


def _step_us_health_repair(context: str = "scan") -> None:
    from factory_us_health import ensure_us_pipeline_ready_for_scan, format_us_health_log_line

    out = ensure_us_pipeline_ready_for_scan(context=context, repair=True)
    after = out.get("after") or {}
    print(f"🔧 [Factory] us_health_repair({context}): {format_us_health_log_line(after)}")


_US_DATA_INCREMENTAL = StepSpec(
    "us_data_incremental",
    _step_us_data_incremental_update,
    critical=False,
    delay_after_sec=1.0,
)

_US_HEALTH_GATE = StepSpec(
    "us_health_gate",
    lambda: _step_us_health_gate("scan"),
    critical=False,
    delay_after_sec=0.5,
)

_US_HEALTH_REPAIR = StepSpec(
    "us_health_repair",
    lambda: _step_us_health_repair("scan"),
    critical=False,
    delay_after_sec=0.5,
)


def _with_daily_audit_prelude(steps: List[StepSpec]) -> List[StepSpec]:
    """일일 감사·통합 리포트: meta sync → guard → sentiment → 본 작업 (스필오버는 KR/US 선행 블록)."""
    return [
        _META_GOVERNOR_SYNC,
        _ARTIFACT_GUARD,
        _SENTIMENT_MINING,
        *steps,
    ]


def _with_daily_audit_kr_prelude(steps: List[StepSpec]) -> List[StepSpec]:
    return [
        *_with_daily_audit_prelude([]),
        _REPORT_HYDRATE_KR,
        *steps,
    ]


def _with_daily_audit_us_prelude(steps: List[StepSpec]) -> List[StepSpec]:
    """US 일일 감사: 공통 prelude + US health + OHLCV 증분 + hydrate + track/spillover/publish."""
    return [
        *_with_daily_audit_prelude([]),
        StepSpec("us_health_gate_daily", lambda: _step_us_health_gate("daily"), critical=False, delay_after_sec=0.5),
        StepSpec("us_fluid_health_prelude", _step_us_fluid_health_prelude, critical=False, delay_after_sec=0.5),
        StepSpec("us_health_repair_daily", lambda: _step_us_health_repair("daily"), critical=False, delay_after_sec=0.5),
        _US_DATA_INCREMENTAL,
        StepSpec("us_post_incremental_upstream", _step_us_post_incremental_upstream, critical=False, delay_after_sec=1.0),
        _SYNC_US_TOXIC_ML,
        _REPORT_HYDRATE_US,
        *_with_us_spillover_tail([*steps, _CROSS_MARKET_THEME_SNAPSHOT]),
    ]


def _with_kr_spillover_prerequisite(steps: List[StepSpec]) -> List[StepSpec]:
    """
    KR V28 직전: US 장부 갱신 → 스필오버 KV/SSOT → KR hydrate.
    Option A — daily-kr / combined 의 KR deep_dive 전 필수.
    """
    return [
        StepSpec(
            "track_daily_positions_us_prereq",
            _step_track_us,
            critical=True,
            delay_after_sec=2.0,
        ),
        StepSpec(
            "sector_spillover_refresh_prereq",
            _step_sector_spillover_refresh,
            critical=False,
            delay_after_sec=1.0,
        ),
        _US_CROSS_MARKET_PUBLISH,
        _CROSS_MARKET_THEME_SNAPSHOT,
        _KR_CROSS_MARKET_HYDRATE,
        *steps,
    ]


def _with_us_spillover_tail(steps: List[StepSpec]) -> List[StepSpec]:
    """US track 직후 스필오버 갱신 + cross_market publish."""
    return [
        *steps,
        _SECTOR_SPILLOVER_REFRESH,
        _US_CROSS_MARKET_PUBLISH,
    ]


_META_SYNC_SCAN = StepSpec(
    "meta_governor_sync_scan",
    _step_meta_governor_sync,
    critical=True,
    delay_after_sec=0.5,
)


def _with_scan_us_prelude(steps: List[StepSpec]) -> List[StepSpec]:
    """scan-us: meta → health → repair → 증분 OHLCV → supernova → doomsday (KR scan 대칭)."""
    return [
        _META_SYNC_SCAN,
        _US_HEALTH_GATE,
        _US_HEALTH_REPAIR,
        _US_DATA_INCREMENTAL,
        _SYNC_US_TOXIC_ML,
        *steps,
        _CROSS_MARKET_THEME_SNAPSHOT,
        _DOOMSDAY_BRIDGE,
    ]


def _with_scan_kr_prelude(steps: List[StepSpec]) -> List[StepSpec]:
    """scan-kr: meta → US track/spillover/theme → KR hydrate → supernova → doomsday."""
    return [
        _META_SYNC_SCAN,
        StepSpec(
            "track_daily_positions_us_prereq_kr_scan",
            _step_track_us,
            critical=False,
            delay_after_sec=2.0,
        ),
        _CROSS_MARKET_THEME_SNAPSHOT,
        _KR_CROSS_MARKET_HYDRATE,
        *steps,
        _DOOMSDAY_BRIDGE,
    ]


def _require_market_session_for_scan(market: str) -> None:
    from market_session_gate import require_market_open_for_scan

    require_market_open_for_scan(market)


def _step_supernova_kr() -> None:
    _require_market_session_for_scan("KR")
    from supernova_hunter import execute_supernova_live_scan

    execute_supernova_live_scan("KR")


def _step_supernova_us() -> None:
    _require_market_session_for_scan("US")
    from supernova_hunter import execute_supernova_live_scan

    execute_supernova_live_scan("US")


def _step_kr_bowl_optional() -> None:
    _run_equity_scan_module(
        "legacy_archive.scanners.kr",
        label="KR 밥그릇",
        market="KR",
    )


def _step_kr_dante_scan() -> None:
    _run_equity_scan_module(
        "legacy_archive.scanners.dante_krx_reverse_breakout_screener",
        label="KR 역매공파",
        market="KR",
    )


def _run_equity_scan_module(module_path: str, *, label: str, market: str) -> None:
    """legacy_archive.scanners.* scan_market_1d — 예외·큐 drain SSOT."""
    _require_market_session_for_scan(market)
    import importlib

    mod = importlib.import_module(module_path)
    scan_fn = getattr(mod, "scan_market_1d", None)
    if not callable(scan_fn):
        raise AttributeError(f"{module_path} has no scan_market_1d")
    token = str(getattr(mod, "TELEGRAM_TOKEN_MAIN", "") or "")
    chat_id = str(getattr(mod, "TELEGRAM_CHAT_ID", "") or "")
    send = bool(getattr(mod, "SEND_TELEGRAM", False))
    try:
        scan_fn()
    except Exception as exc:
        try:
            from scanner_funnel import notify_equity_scan_fatal

            notify_equity_scan_fatal(
                market=market,
                label=label,
                exc=exc,
                token_main=token,
                chat_id=chat_id,
                send_enabled=send,
            )
        except Exception:
            pass
        raise
    if send:
        from telegram_message_queue import wait_telegram_queue_drained

        wait_telegram_queue_drained(("MAIN", "PROMO"), timeout_sec=7200.0)


def _step_kr_nulrim_scan() -> None:
    _run_equity_scan_module(
        "legacy_archive.scanners.nulrim",
        label="KR 눌림목",
        market="KR",
    )


def _step_kr_ema5_scan() -> None:
    _run_equity_scan_module(
        "legacy_archive.scanners.ema5",
        label="KR 5일선",
        market="KR",
    )


def _step_kr_master_scan() -> None:
    _run_equity_scan_module(
        "legacy_archive.scanners.master",
        label="KR 마스터",
        market="KR",
    )


def _step_us_nulrim_scan() -> None:
    _run_equity_scan_module(
        "legacy_archive.scanners.nulusa",
        label="US 눌림목",
        market="US",
    )


def _step_us_ema5_scan() -> None:
    _run_equity_scan_module(
        "legacy_archive.scanners.us_5ema",
        label="US 5일선",
        market="US",
    )


def _step_us_bowl_optional() -> None:
    _run_equity_scan_module(
        "legacy_archive.scanners.usa",
        label="US 밥그릇",
        market="US",
    )


def _step_us_dante_scan() -> None:
    _run_equity_scan_module(
        "legacy_archive.scanners.nasdaq_dante_reverse_breakout_screener",
        label="US 역매공파",
        market="US",
    )


_SCANNER_STEP_BUILDERS = {
    "KR": {
        "supernova": lambda: StepSpec(
            "supernova_scan_kr", _step_supernova_kr, critical=True, delay_after_sec=5.0
        ),
        "nulrim": lambda: StepSpec(
            "kr_nulrim_scan", _step_kr_nulrim_scan, critical=False, delay_after_sec=2.0
        ),
        "dante": lambda: StepSpec(
            "kr_dante_scan", _step_kr_dante_scan, critical=False, delay_after_sec=2.0
        ),
        "ema5": lambda: StepSpec(
            "kr_ema5_scan", _step_kr_ema5_scan, critical=False, delay_after_sec=2.0
        ),
        "master": lambda: StepSpec(
            "kr_master_scan", _step_kr_master_scan, critical=False, delay_after_sec=2.0
        ),
        "bowl": lambda: StepSpec(
            "kr_bowl_scan", _step_kr_bowl_optional, critical=False, delay_after_sec=0.0
        ),
    },
    "US": {
        "supernova": lambda: StepSpec(
            "supernova_scan_us", _step_supernova_us, critical=True, delay_after_sec=5.0
        ),
        "nulrim": lambda: StepSpec(
            "us_nulrim_scan", _step_us_nulrim_scan, critical=False, delay_after_sec=2.0
        ),
        "dante": lambda: StepSpec(
            "us_dante_scan", _step_us_dante_scan, critical=False, delay_after_sec=2.0
        ),
        "ema5": lambda: StepSpec(
            "us_ema5_scan", _step_us_ema5_scan, critical=False, delay_after_sec=2.0
        ),
        "bowl": lambda: StepSpec(
            "us_bowl_scan", _step_us_bowl_optional, critical=False, delay_after_sec=0.0
        ),
    },
}


def _kr_prelude_full() -> List[StepSpec]:
    return [
        _META_SYNC_SCAN,
        StepSpec(
            "track_daily_positions_us_prereq_kr_scan",
            _step_track_us,
            critical=False,
            delay_after_sec=2.0,
        ),
        _CROSS_MARKET_THEME_SNAPSHOT,
        _KR_CROSS_MARKET_HYDRATE,
    ]


def _kr_prelude_light() -> List[StepSpec]:
    return [_KR_CROSS_MARKET_HYDRATE]


def _us_prelude_full() -> List[StepSpec]:
    return [
        _META_SYNC_SCAN,
        _US_HEALTH_GATE,
        _US_HEALTH_REPAIR,
        _US_DATA_INCREMENTAL,
        _SYNC_US_TOXIC_ML,
    ]


def _us_prelude_light() -> List[StepSpec]:
    return [
        StepSpec("us_health_gate_light", lambda: _step_us_health_gate("scan"), critical=False),
        _US_DATA_INCREMENTAL,
    ]


def _build_staggered_scan_pipeline(slot: ScanSlot) -> List[StepSpec]:
    builders = _SCANNER_STEP_BUILDERS[slot.market]
    if slot.scanner_key not in builders:
        raise KeyError(f"no scanner step for {slot.market}/{slot.scanner_key}")
    scan_step = builders[slot.scanner_key]()

    prelude: List[StepSpec] = []
    if slot.prelude == "full":
        prelude = _kr_prelude_full() if slot.market == "KR" else _us_prelude_full()
    elif slot.prelude == "light":
        prelude = _kr_prelude_light() if slot.market == "KR" else _us_prelude_light()

    tail: List[StepSpec] = []
    if slot.tail_doomsday:
        tail.append(_DOOMSDAY_BRIDGE)
    if slot.tail_us_publish:
        tail.extend([_US_CROSS_MARKET_PUBLISH, _CROSS_MARKET_THEME_SNAPSHOT])

    return _with_artifact_guard([*prelude, scan_step, *tail])


def _staggered_scan_pipelines() -> Dict[str, List[StepSpec]]:
    return {slot.mode: _build_staggered_scan_pipeline(slot) for slot in ALL_SCAN_SLOTS}


def _step_track_kr() -> None:
    from auto_forward_tester import track_daily_positions

    track_daily_positions("KR")


def _step_track_us() -> None:
    from auto_forward_tester import track_daily_positions

    track_daily_positions("US")


def _step_deep_dive_kr() -> None:
    from auto_forward_tester import run_deep_dive_analysis

    run_deep_dive_analysis("KR")


def _step_deep_dive_us() -> None:
    from auto_forward_tester import run_deep_dive_analysis

    run_deep_dive_analysis("US")


def _step_overseer_optional() -> None:
    from ai_overseer import run_ai_auditor

    run_ai_auditor()


def _step_comprehensive_optional() -> None:
    from auto_forward_tester import send_comprehensive_daily_report

    send_comprehensive_daily_report(refresh_sentiment=True)


def _step_weekly_master() -> None:
    import system_auto_pilot as sap
    from weekly_flow_report import send_weekly_flow_master_report

    send_weekly_flow_master_report(
        db_path=sap.DB_PATH,
        sys_config=sap.load_or_create_config(),
        send_fn=sap.send_telegram_report,
    )


def _pipeline_daily_audit_kr() -> List[StepSpec]:
    return _with_daily_audit_kr_prelude(
        _with_kr_spillover_prerequisite(
            [
                StepSpec("track_daily_positions_kr", _step_track_kr, critical=True, delay_after_sec=3.0),
                StepSpec("deep_dive_kr", _step_deep_dive_kr, critical=True, delay_after_sec=3.0),
                _DOOMSDAY_BRIDGE,
                _REPORTER_CLEANUP_ZOMBIE,
                _PIL_PRACTITIONER_KR,
                _COMPREHENSIVE_REPORT,
                StepSpec("ai_overseer", _step_overseer_optional, critical=False, delay_after_sec=0),
            ]
        )
    )


def _pipeline_daily_audit_us() -> List[StepSpec]:
    return _with_daily_audit_us_prelude(
        [
            StepSpec("track_daily_positions_us", _step_track_us, critical=True, delay_after_sec=3.0),
            StepSpec("deep_dive_us", _step_deep_dive_us, critical=True, delay_after_sec=3.0),
            _DOOMSDAY_BRIDGE,
            _REPORTER_CLEANUP_ZOMBIE,
            _PIL_PRACTITIONER_US,
            _COMPREHENSIVE_REPORT,
            StepSpec("ai_overseer", _step_overseer_optional, critical=False, delay_after_sec=0),
        ]
    )


def _pipeline_daily_audit_combined() -> List[StepSpec]:
    """수동 ./factory.sh --daily 용 — US 선행 후 KR V28, 이후 US deep_dive."""
    return [
        *_with_daily_audit_prelude([]),
        _REPORT_HYDRATE_BOTH,
        *_with_kr_spillover_prerequisite(
            [
                StepSpec("track_daily_positions_kr", _step_track_kr, critical=True, delay_after_sec=3.0),
                StepSpec("deep_dive_kr", _step_deep_dive_kr, critical=True, delay_after_sec=8.0),
                _US_DATA_INCREMENTAL,
                StepSpec(
                    "track_daily_positions_us_combined",
                    _step_track_us,
                    critical=True,
                    delay_after_sec=3.0,
                ),
                StepSpec("sector_spillover_refresh_us_tail", _step_sector_spillover_refresh, critical=False),
                StepSpec("us_cross_market_publish_combined", _step_us_cross_market_publish, critical=False),
                StepSpec("deep_dive_us", _step_deep_dive_us, critical=True, delay_after_sec=3.0),
                _DOOMSDAY_BRIDGE,
                _REPORTER_CLEANUP_ZOMBIE,
                _PIL_PRACTITIONER,
                _COMPREHENSIVE_REPORT,
                StepSpec("ai_overseer", _step_overseer_optional, critical=False),
            ]
        ),
    ]


def build_factory_pipelines() -> Dict[str, List[StepSpec]]:
    daily_kr = _pipeline_daily_audit_kr()
    daily_us = _pipeline_daily_audit_us()
    staggered = _staggered_scan_pipelines()
    pipelines: Dict[str, List[StepSpec]] = {
        "scan_us": _with_artifact_guard(
            _with_scan_us_prelude(
                [
                    StepSpec("supernova_scan_us", _step_supernova_us, critical=True, delay_after_sec=5.0),
                    StepSpec("us_nulrim_scan", _step_us_nulrim_scan, critical=False, delay_after_sec=2.0),
                    StepSpec("us_dante_scan", _step_us_dante_scan, critical=False, delay_after_sec=2.0),
                    StepSpec("us_ema5_scan", _step_us_ema5_scan, critical=False, delay_after_sec=2.0),
                    StepSpec("us_bowl_scan", _step_us_bowl_optional, critical=False),
                    _US_CROSS_MARKET_PUBLISH,
                ]
            )
        ),
        "scan_kr": _with_artifact_guard(
            _with_scan_kr_prelude(
                [
                    StepSpec("supernova_scan_kr", _step_supernova_kr, critical=True, delay_after_sec=5.0),
                    StepSpec("kr_nulrim_scan", _step_kr_nulrim_scan, critical=False, delay_after_sec=2.0),
                    StepSpec("kr_dante_scan", _step_kr_dante_scan, critical=False, delay_after_sec=2.0),
                    StepSpec("kr_ema5_scan", _step_kr_ema5_scan, critical=False, delay_after_sec=2.0),
                    StepSpec("kr_master_scan", _step_kr_master_scan, critical=False, delay_after_sec=2.0),
                    StepSpec("kr_bowl_scan", _step_kr_bowl_optional, critical=False),
                ]
            )
        ),
        "daily_audit_kr": daily_kr,
        "daily_audit_us": daily_us,
        "daily_audit": _pipeline_daily_audit_combined(),
        "weekly_master": _with_artifact_guard(
            [StepSpec("weekly_flow_master", _step_weekly_master, critical=True)]
        ),
    }
    pipelines.update(staggered)
    return pipelines


PIPELINE: Dict[str, List[StepSpec]] = build_factory_pipelines()
FACTORY_MODES = frozenset(PIPELINE.keys())


def get_pipeline(mode: str) -> Sequence[StepSpec]:
    key = str(mode).strip().lower()
    if key not in PIPELINE:
        raise KeyError(f"Unknown factory mode {mode!r}; known: {sorted(PIPELINE)}")
    return PIPELINE[key]
