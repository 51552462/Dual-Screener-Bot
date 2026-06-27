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

    # daily-us prelude 직전 us_data_incremental 완료 — OHLCV 2회차(3시간+) 방지
    ensure_report_pipeline_data(market="US", refresh_macro=True, refresh_ohlcv=False)


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


def _step_comprehensive_daily_report(*, executive_market: str | None = None) -> None:
    from auto_forward_tester import send_comprehensive_daily_report

    send_comprehensive_daily_report(
        refresh_sentiment=False,
        refresh_sector_spillover=False,
        refresh_meta_governor=False,
        cleanup_zombie_trades=False,
        refresh_macro=False,
        refresh_ohlcv=False,
        executive_market=executive_market,
    )


def _step_comprehensive_daily_report_kr() -> None:
    _step_comprehensive_daily_report(executive_market="KR")


def _step_comprehensive_daily_report_us() -> None:
    _step_comprehensive_daily_report(executive_market="US")


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


def _step_forward_trade_identity_repair(market: str) -> None:
    """종목명 공백 진단·백필 — 리포트 전. FORWARD_IDENTITY_AUTO_REPAIR=1 시 UPDATE."""
    from forward.forward_trade_identity import format_repair_log_line, run_identity_repair_for_market

    try:
        diag, backfill = run_identity_repair_for_market(market)
        print(f"🏷️ [Factory] {format_repair_log_line(diag, backfill)}")
        if backfill.sample_updates and not backfill.dry_run:
            for rid, code, old, new in backfill.sample_updates[:3]:
                print(f"   ↳ id={rid} {code}: '{old}' → '{new}'")
    except Exception as ex:
        print(f"⚠️ [Factory] forward_trade_identity({market}) skip: {ex}")


def _step_forward_trade_identity_kr() -> None:
    _step_forward_trade_identity_repair("KR")


def _step_forward_trade_identity_us() -> None:
    _step_forward_trade_identity_repair("US")


_FORWARD_TRADE_IDENTITY_KR = StepSpec(
    "forward_trade_identity_kr",
    _step_forward_trade_identity_kr,
    critical=False,
    delay_after_sec=0.0,
)

_FORWARD_TRADE_IDENTITY_US = StepSpec(
    "forward_trade_identity_us",
    _step_forward_trade_identity_us,
    critical=False,
    delay_after_sec=0.0,
)

_FORWARD_TRADE_IDENTITY_BOTH = StepSpec(
    "forward_trade_identity_both",
    lambda: (_step_forward_trade_identity_kr(), _step_forward_trade_identity_us()),
    critical=False,
    delay_after_sec=0.0,
)


def _step_us_data_incremental_update() -> None:
    """daily_audit_us / scan-us — US OHLCV 증분 갱신 (KR 07:00 bulk 대칭)."""
    from data_updater import run_us_incremental_db_update

    out = run_us_incremental_db_update()
    print(f"🇺🇸 [Factory] us_data_incremental: {out}")


def _step_daily_db_full_refresh() -> None:
    """KR/US 종목별 OHLCV 전체(bulk) 갱신.

    legacy_archive/scanners/main.py 가 항상-기동 루프에서 매일 07:00 에 돌리던
    run_daily_db_update() 를 factory 크론으로 복원한 것. 이 잡이 누락되면서
    종목 OHLCV DB(KR_xxxxxx)가 동결 → 스캐너가 신선 시세를 못 읽어 신규 진입 0 →
    OPEN 장부 고갈 → 청산 0 → 실무자 리포트 워터마크 영구 RED 가 됐다.
    factory 런타임 락을 공유하므로 스캔/감사와 직렬화되어 4GB 서버 동시실행·OOM 을 막는다.
    """
    from data_updater import run_daily_db_update

    run_daily_db_update()
    print("🛢️ [Factory] data_refresh: KR/US OHLCV bulk 갱신 완료")


def _step_smart_money_refresh() -> None:
    """스마트머니 라다 갱신 (legacy --daemon 16:10 잡의 factory 일급 이관).

    KR: 외인·기관 순매수 다이버전스(pykrx/네이버) + 칼만 평단.
    US: 다크풀 프록시(저변동성 거래량 폭증 = 기관 매집 징후).
    factory 런타임 락으로 스캔/감사와 직렬화되어 4GB 서버 충돌·OOM 을 막는다.
    """
    import smart_money_tracker as smt

    smt.run_all_smart_money()


def _step_limit_up_forensics_kr() -> None:
    """KR 상한가 부검 (legacy --daemon 15:40/16:20 잡 이관)."""
    import limit_up_forensics as L

    L.run_limit_up_forensics(markets=("KR",))


def _step_doomsday_radar() -> None:
    """거시 둠스데이 레이더 (legacy --daemon 17:00 잡 이관)."""
    import macro_doomsday_bot as D

    D.main()


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


def _step_kr_fluid_health_prelude() -> None:
    from config_manager import load_system_config, save_system_config
    from fluid_time_anchor import persist_anchor_state, resolve_kr_with_db_fallback

    cfg = load_system_config() or {}
    anchor = resolve_kr_with_db_fallback(cfg)
    persist_anchor_state(cfg, anchor)
    try:
        save_system_config(cfg)
    except Exception:
        pass
    print(
        f"🌊 [KR Fluid] anchor={anchor.mode} session={anchor.session_date} "
        f"({anchor.reason}) lag={anchor.lag_business_days}bd"
    )


def _with_daily_audit_kr_prelude(steps: List[StepSpec]) -> List[StepSpec]:
    return [
        *_with_daily_audit_prelude([]),
        StepSpec("kr_fluid_health_prelude", _step_kr_fluid_health_prelude, critical=False, delay_after_sec=0.5),
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
    KR V28 직전: US 장부 갱신 → cross_market SSOT 1회 발행.
    sector_spillover / us_publish / kr_hydrate 개별 스텝은 theme_snapshot 에 통합(중복 FDR 방지).
    """
    return [
        StepSpec(
            "track_daily_positions_us_prereq",
            _step_track_us,
            critical=True,
            delay_after_sec=2.0,
        ),
        _CROSS_MARKET_THEME_SNAPSHOT,
        *steps,
    ]


def _with_us_spillover_tail(steps: List[StepSpec]) -> List[StepSpec]:
    """US track 직후 — cross_market_theme_snapshot 이 spillover+publish+hydrate 를 일괄 처리."""
    return list(steps)


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


def _log_open_book_snapshot(market: str) -> None:
    from forward.forward_book_integrity import diagnose_open_book_from_db
    from reports.report_timekeeper import ReportTimekeeper

    try:
        tk = ReportTimekeeper.for_market(market)
        st = diagnose_open_book_from_db(market, session_anchor=tk.session_anchor)
        print(
            f"📒 [Factory] open_book/{market}: raw={st.open_raw} valid={st.open_valid} "
            f"ghost={st.open_ghost} today={st.open_today_valid} · {st.integrity_note}"
        )
    except Exception as ex:
        print(f"⚠️ [Factory] open_book({market}) skip: {ex}")


def _step_track_kr() -> None:
    from auto_forward_tester import track_daily_positions

    track_daily_positions("KR")
    _log_open_book_snapshot("KR")


def _step_track_us() -> None:
    from auto_forward_tester import track_daily_positions

    track_daily_positions("US")
    _log_open_book_snapshot("US")


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


def _step_macro_matrix_incremental() -> None:
    """주말 MACRO_EVOLUTION_MATRIX 증분 갱신 (Shadow · MetaGovernor 미연동)."""
    try:
        from macro_matrix_incremental import update_macro_matrix_incremental
        from macro_sentinel_quant import compute_macro_sentinel_snapshot

        snap = compute_macro_sentinel_snapshot()
        _, stats = update_macro_matrix_incremental(regime_by_date=snap.regime_by_date)
        print(f"[macro_matrix_incremental] {stats}")
    except Exception as ex:
        print(f"[macro_matrix_incremental] skip: {ex}")


def _step_weekly_proprietary_regime() -> None:
    """주간 PRI Shadow 산출 (평일 적재 → 토요일 일괄 연산 · Meta 미연동)."""
    try:
        from weekly_proprietary_regime import compute_weekly_proprietary_regime

        out = compute_weekly_proprietary_regime()
        print(f"[weekly_proprietary_regime] blended={out.get('blended')}")
    except Exception as ex:
        print(f"[weekly_proprietary_regime] skip: {ex}")
    # [진화형 메타-러닝] PRI 산출 직후 발산 채점·신뢰 가중치 갱신(주간 cron 경로).
    try:
        from meta_learner import run_meta_learning_cycle

        ml = run_meta_learning_cycle()
        print(f"[meta_learner] {ml}")
    except Exception as ex:
        print(f"[meta_learner] skip: {ex}")
    # [진화형 둠스데이 감쇠] γ(형상) 경사하강 자율 갱신(주간 cron 경로).
    try:
        from doomsday_dampener import evolve_gamma

        gv = evolve_gamma()
        print(f"[doomsday_dampener] {gv}")
    except Exception as ex:
        print(f"[doomsday_dampener] skip: {ex}")
    # [M2] 프리러너 볼록 래칫 κ 곡선 주간 RL 갱신(주간 cron 경로).
    try:
        from exit_ratchet_rl import evolve_ratchet_kappa

        kv = evolve_ratchet_kappa()
        print(f"[ratchet_kappa] {kv.get('rates')} → updated={kv.get('updated')}")
    except Exception as ex:
        print(f"[ratchet_kappa] skip: {ex}")


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
                _FORWARD_TRADE_IDENTITY_KR,
                _PIL_PRACTITIONER_KR,
                StepSpec(
                    "comprehensive_daily_report",
                    _step_comprehensive_daily_report_kr,
                    critical=False,
                    delay_after_sec=3.0,
                ),
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
            _FORWARD_TRADE_IDENTITY_US,
            _PIL_PRACTITIONER_US,
            StepSpec(
                "comprehensive_daily_report",
                _step_comprehensive_daily_report_us,
                critical=False,
                delay_after_sec=3.0,
            ),
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
                _FORWARD_TRADE_IDENTITY_BOTH,
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
        "data_refresh": _with_artifact_guard(
            [
                StepSpec(
                    "daily_db_full_refresh",
                    _step_daily_db_full_refresh,
                    critical=True,
                    delay_after_sec=1.0,
                ),
            ]
        ),
        "smart_money_refresh": [
            StepSpec(
                "smart_money_refresh",
                _step_smart_money_refresh,
                critical=False,
                delay_after_sec=0.0,
            ),
        ],
        "limit_up_forensics": [
            StepSpec(
                "limit_up_forensics_kr",
                _step_limit_up_forensics_kr,
                critical=False,
                delay_after_sec=0.0,
            ),
        ],
        "doomsday_radar": [
            StepSpec(
                "doomsday_radar",
                _step_doomsday_radar,
                critical=False,
                delay_after_sec=0.0,
            ),
        ],
        "weekly_master": _with_artifact_guard(
            [
                StepSpec(
                    "macro_matrix_incremental",
                    _step_macro_matrix_incremental,
                    critical=False,
                    delay_after_sec=1.0,
                ),
                StepSpec(
                    "weekly_proprietary_regime",
                    _step_weekly_proprietary_regime,
                    critical=False,
                    delay_after_sec=1.0,
                ),
                StepSpec("weekly_flow_master", _step_weekly_master, critical=True),
            ]
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
