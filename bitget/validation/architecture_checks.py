"""
Phase 7 — Bitget factory architecture invariants (Phase 1~6 SSOT).

`check_cutover_readiness()` 및 pytest에서 호출. 루트 주식 파일은 검사하지 않음.
"""
from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

_BITGET_ROOT = Path(__file__).resolve().parents[1]

_SATELLITE_SOURCES = (
    "supernova_hunter.py",
    "master_scanner.py",
    "signal_engines.py",
    "executor.py",
    "blackhole_hunter.py",
    "shadow_performance_tracker.py",
    "doomsday_bot.py",
    "underdog_miner.py",
    "toxic_graveyard_analyzer.py",
    "time_machine_backtester.py",
    "synthetic_data_generator.py",
    "pump_forensics.py",
    "data_miner.py",
)

_LEGACY_BLOCKERS: Tuple[Tuple[str, str, type], ...] = (
    ("bitget.main", "_blocked", SystemExit),
    ("bitget.factory_launcher", "launch_factory", SystemExit),
    ("bitget.sentinel", "run_sentinel", SystemExit),
    ("bitget.system_auto_pilot", "system_main_loop", RuntimeError),
)

_DAILY_PRELUDE = (
    "meta_governor_sync",
    "artifact_guard",
    "config_bootstrap",
    "sentiment_mining",
)
_SCAN_PRELUDE = ("meta_governor_sync_scan", "artifact_guard", "config_bootstrap")
_DAILY_BODY_KEYS = (
    "doomsday_radar",
    "report_pipeline_hydrate",
    "deep_dive_spot",
    "deep_dive_futures",
    "doomsday_bridge_sync",
    "reporter_cleanup_zombie_forward_trades",
    "forward_trade_identity",
    "pil_practitioner_reports",
    "comprehensive_report",
    "ai_overseer",
    "reconcile",
)


def _check_prefix(names: List[str], expected: Tuple[str, ...]) -> Tuple[bool, str]:
    if len(names) < len(expected):
        return False, f"expected prefix {expected}, got {names[: len(expected) + 2]}"
    head = tuple(names[: len(expected)])
    if head != expected:
        return False, f"expected prefix {expected}, got {head}"
    return True, "ok"


def check_legacy_entrypoints_blocked() -> Dict[str, Any]:
    """Phase 1 — 레거시 진입점이 SystemExit(2) / RuntimeError 로 차단되는지."""
    details: Dict[str, bool] = {}
    for mod_name, attr, exc_type in _LEGACY_BLOCKERS:
        mod = importlib.import_module(mod_name)
        fn = getattr(mod, attr)
        blocked = False
        try:
            fn()
        except exc_type:
            blocked = True
        except SystemExit as ex:
            blocked = exc_type is SystemExit and int(ex.code or 0) == 2
        details[f"{mod_name}.{attr}"] = blocked
    ok = all(details.values())
    return {"ok": ok, "details": details, "message": "legacy entrypoints blocked" if ok else "legacy leak"}


def check_pipeline_structure() -> Dict[str, Any]:
    """Phase 2~4 — scan/daily prelude·PIL·deep_dive step 순서."""
    from bitget.pipelines.bitget_pipelines import get_pipeline

    daily = [s.name for s in get_pipeline("daily_audit")]
    scan = [s.name for s in get_pipeline("scan_spot")]
    track = [s.name for s in get_pipeline("track_positions")]

    daily_pre_ok, daily_pre_msg = _check_prefix(daily, _DAILY_PRELUDE)
    scan_pre_ok, scan_pre_msg = _check_prefix(scan, _SCAN_PRELUDE)
    daily_body_ok = all(k in daily for k in _DAILY_BODY_KEYS)
    daily_count_ok = len(daily) == 19
    track_ok = track == ["config_bootstrap", "artifact_guard", "track_spot", "track_futures"]

    ok = daily_pre_ok and scan_pre_ok and daily_body_ok and daily_count_ok and track_ok
    return {
        "ok": ok,
        "daily_audit": daily,
        "daily_step_count": len(daily),
        "scan_spot": scan,
        "track_positions": track,
        "daily_prelude": daily_pre_msg,
        "scan_prelude": scan_pre_msg,
        "daily_body_keys": daily_body_ok,
        "daily_count_ok": daily_count_ok,
        "message": "pipeline SSOT structure ok" if ok else "pipeline structure drift",
    }


def check_satellite_config_hub() -> Dict[str, Any]:
    """Phase 5 — 위성 모듈 JSON runtime I/O 잔존 여부 (정적 스캔)."""
    offenders: List[str] = []
    for name in _SATELLITE_SOURCES:
        path = _BITGET_ROOT / name
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if "bitget_system_config.json" in text and "open(" in text:
            offenders.append(name)
    ok = not offenders
    return {
        "ok": ok,
        "offenders": offenders,
        "scanned": len(_SATELLITE_SOURCES),
        "message": "satellite config_hub only" if ok else f"json io in {offenders}",
    }


def check_scanner_engine_pool_ssot() -> Dict[str, Any]:
    """Lock full-scan practitioner pool — no bare `bse` NameError (P0-6)."""
    details: Dict[str, Any] = {}

    _, ms = _file_text("master_scanner.py")
    ms_req = (
        "import bitget.signal_engines as bse",
        "def _build_engine_pool",
        "compute_practitioner_",
        "PRACT_",
    )
    details["master_scanner"] = {
        "ok": bool(ms) and not _require_all(ms, ms_req),
        "missing": _require_all(ms, ms_req) if ms else ["master_scanner.py missing"],
    }

    _, se = _file_text("signal_engines.py")
    se_req = ("def compute_practitioner_01", "def compute_practitioner_30")
    details["signal_engines"] = {
        "ok": bool(se) and not _require_all(se, se_req),
        "missing": _require_all(se, se_req) if se else ["signal_engines.py missing"],
    }

    failed = [k for k, v in details.items() if not v.get("ok")]
    ok = not failed
    return {
        "ok": ok,
        "failed": failed,
        "details": details,
        "message": "scanner engine pool SSOT ok" if ok else f"scanner engine drift: {failed}",
    }


def check_exploration_budget_market_ssot() -> Dict[str, Any]:
    """Lock exploration MAB registry filter to Bitget market-key SSOT."""
    details: Dict[str, Any] = {}

    _, mk = _file_text("infra/market_keys.py")
    mk_req = (
        "def is_bitget_registry_market",
        "BG_FUTURES",
        "FUTURES",
        "def to_deathmatch_key",
    )
    details["market_keys"] = {
        "ok": bool(mk) and not _require_all(mk, mk_req),
        "missing": _require_all(mk, mk_req) if mk else ["infra/market_keys.py missing"],
    }

    _, eb = _file_text("governance/exploration_budget.py")
    eb_req = (
        "is_bitget_registry_market",
        "def _load_registry_role_map",
        "def get_exploration_role_scaler",
    )
    details["exploration_budget"] = {
        "ok": bool(eb) and not _require_all(eb, eb_req),
        "missing": _require_all(eb, eb_req)
        if eb
        else ["governance/exploration_budget.py missing"],
    }

    failed = [k for k, v in details.items() if not v.get("ok")]
    ok = not failed
    return {
        "ok": ok,
        "failed": failed,
        "details": details,
        "message": "exploration budget market SSOT ok"
        if ok
        else f"exploration market drift: {failed}",
    }


def check_config_hard_bounds_ssot() -> Dict[str, Any]:
    """Lock write-time capital-parameter clamps (P1-4) — save/set/OCC."""
    details: Dict[str, Any] = {}

    _, bounds = _file_text("infra/config_bounds.py")
    bounds_req = (
        "CONFIG_NUMERIC_BOUNDS",
        "def apply_config_hard_bounds",
        "def clamp_config_value",
        "DYNAMIC_KELLY_RISK",
        "NAV_DD_HALT_PCT",
        "_enforce_nav_stage_order",
    )
    details["config_bounds"] = {
        "ok": bool(bounds) and not _require_all(bounds, bounds_req),
        "missing": _require_all(bounds, bounds_req)
        if bounds
        else ["infra/config_bounds.py missing"],
    }

    _, mgr = _file_text("infra/config_manager.py")
    mgr_req = (
        "apply_config_hard_bounds",
        "clamp_config_value",
        "config hard-bound clamp",
    )
    details["config_manager"] = {
        "ok": bool(mgr) and not _require_all(mgr, mgr_req),
        "missing": _require_all(mgr, mgr_req) if mgr else ["infra/config_manager.py missing"],
    }

    failed = [k for k, v in details.items() if not v.get("ok")]
    ok = not failed
    return {
        "ok": ok,
        "failed": failed,
        "details": details,
        "message": "config hard bounds SSOT ok" if ok else f"config bounds drift: {failed}",
    }


def check_config_meta_alignment() -> Dict[str, Any]:
    """Phase 3 config/meta — `regime_audit` 위임 (상세 감사 SSOT)."""
    from bitget.validation.regime_audit import run_regime_kelly_audit

    r = run_regime_kelly_audit()
    if not r.get("ok"):
        return {
            "ok": False,
            "message": r.get("message", "regime audit failed"),
            "error": r.get("error"),
        }
    regime = r.get("regime") or {}
    return {
        "ok": r.get("passed", False),
        "CURRENT_REGIME_KEY": regime.get("CURRENT_REGIME_KEY"),
        "META_REGIME_KEY": regime.get("META_REGIME_KEY"),
        "misaligned": regime.get("misaligned"),
        "meta_degraded": regime.get("meta_degraded"),
        "DYNAMIC_KELLY_RISK": (r.get("kelly") or {}).get("DYNAMIC_KELLY_RISK"),
        "resolve_trading_kelly_base": (r.get("kelly") or {}).get("resolve_trading_kelly_base"),
        "message": r.get("message"),
        "audit": r,
    }


def check_scan_schedule_ssot() -> Dict[str, Any]:
    """Phase 5 — staggered scan slots · pipeline · cron template drift."""
    from bitget.bitget_scan_schedule import ALL_SCAN_SLOTS, STAGGERED_SCAN_MODES
    from bitget.infra.runtime import BITGET_MODES
    from bitget.pipelines.bitget_pipelines import PIPELINE_BUILDERS, get_pipeline

    missing_modes = [s.mode for s in ALL_SCAN_SLOTS if s.mode not in BITGET_MODES]
    missing_pipes = [s.mode for s in ALL_SCAN_SLOTS if s.mode not in PIPELINE_BUILDERS]
    prelude_ok = True
    for slot in ALL_SCAN_SLOTS:
        if slot.mode in missing_pipes:
            continue
        names = [s.name for s in get_pipeline(slot.mode)]
        if slot.prelude == "full" and "meta_governor_sync_scan" not in names:
            prelude_ok = False
        if "artifact_guard" not in names:
            prelude_ok = False

    cron_ok = True
    cron_msg = "ok"
    try:
        import subprocess
        import sys

        gen = _BITGET_ROOT / "deploy" / "generate_bitget_crontab.py"
        proc = subprocess.run(
            [sys.executable, str(gen), "--check"],
            cwd=str(_BITGET_ROOT.parent),
            capture_output=True,
            text=True,
        )
        cron_ok = proc.returncode == 0
        if not cron_ok:
            cron_msg = proc.stderr or proc.stdout or "cron drift"
    except Exception as ex:
        cron_ok = False
        cron_msg = str(ex)

    ok = not missing_modes and not missing_pipes and prelude_ok and cron_ok
    return {
        "ok": ok,
        "n_slots": len(ALL_SCAN_SLOTS),
        "n_staggered_modes": len(STAGGERED_SCAN_MODES),
        "missing_modes": missing_modes,
        "missing_pipelines": missing_pipes,
        "prelude_ok": prelude_ok,
        "cron_template": cron_msg if cron_ok else cron_msg,
        "message": "scan schedule SSOT ok" if ok else "scan schedule drift",
    }


def check_bitget_shell_daily_audit_guard() -> Dict[str, Any]:
    """Phase 2 — bitget.sh daily_audit pgrep 중복 가드 (주식 factory.sh 패리티)."""
    path = _BITGET_ROOT / "deploy" / "bitget.sh"
    if not path.is_file():
        return {"ok": False, "message": "bitget.sh missing"}
    text = path.read_text(encoding="utf-8", errors="replace")
    required = (
        "_bitget_live_daily_audit_lines",
        "runner --mode daily_audit",
        "SKIP: another daily_audit job is already running",
        '[[ "$pid" -eq "$$" ]]',
        "exit 0",
    )
    missing = [s for s in required if s not in text]
    ok = not missing
    return {
        "ok": ok,
        "missing": missing,
        "path": str(path.relative_to(_BITGET_ROOT.parent)),
        "message": "daily_audit shell guard ok" if ok else f"missing: {missing}",
    }


def check_weekly_evolution_pipeline() -> Dict[str, Any]:
    """Phase 3 — weekly_evolution: autonomous tuning → weekly flow master report."""
    from bitget.pipelines.bitget_pipelines import get_pipeline

    names = [s.name for s in get_pipeline("weekly_evolution")]
    expected_tail = ("weekly_evolution", "weekly_flow_master")
    tail_ok = len(names) >= 2 and names[-2:] == list(expected_tail)
    steps = {s.name: s for s in get_pipeline("weekly_evolution")}
    critical_ok = all(
        steps.get(n) and steps[n].critical for n in expected_tail
    )
    ok = tail_ok and critical_ok
    return {
        "ok": ok,
        "steps": names,
        "tail_ok": tail_ok,
        "critical_ok": critical_ok,
        "message": "weekly_evolution pipeline ok" if ok else "weekly pipeline drift",
    }


def check_deep_dive_dna_autopsy_ssot() -> Dict[str, Any]:
    """Phase 5 — run_deep_dive_analysis must delegate DNA to dna_autopsy module."""
    path = _BITGET_ROOT / "forward" / "reports.py"
    if not path.is_file():
        return {"ok": False, "message": "forward/reports.py missing"}
    text = path.read_text(encoding="utf-8", errors="replace")
    fn_start = text.find("def run_deep_dive_analysis")
    if fn_start < 0:
        return {"ok": False, "message": "run_deep_dive_analysis missing"}
    fn_body = text[fn_start : fn_start + 12000]
    required = (
        "build_dna_autopsy_slice",
        "format_dna_autopsy_section",
        "BitgetReportContext.build",
        "slice_for_market",
    )
    forbidden = ("def get_dna(", "Universal) DNA 분석")
    missing = [s for s in required if s not in fn_body]
    leaks = [s for s in forbidden if s in fn_body]
    ok = not missing and not leaks
    return {
        "ok": ok,
        "missing": missing,
        "inline_dna_leaks": leaks,
        "message": "deep_dive dna_autopsy SSOT ok" if ok else "deep_dive inline DNA drift",
    }


def check_daemon_sniper_policy() -> Dict[str, Any]:
    """Phase 6 — supernova sniper in daemon must be opt-in (cron staggered SSOT)."""
    path = _BITGET_ROOT / "pipelines" / "bitget_auto_pilot.py"
    if not path.is_file():
        return {"ok": False, "message": "bitget_auto_pilot.py missing"}
    text = path.read_text(encoding="utf-8", errors="replace")
    required = (
        "BITGET_DAEMON_SNIPER",
        "def _daemon_sniper_enabled",
        "if _daemon_sniper_enabled():",
    )
    missing = [s for s in required if s not in text]
    # unconditional sniper start is a regression
    unconditional = 'Thread(target=_supernova_sniper_thread, daemon=True, name="bitget_supernova_sniper").start()'
    ok = not missing and unconditional not in text
    return {
        "ok": ok,
        "missing": missing,
        "unconditional_sniper": unconditional in text,
        "message": "daemon sniper policy ok" if ok else "daemon sniper policy drift",
    }


def check_daemon_public_ws_policy() -> Dict[str, Any]:
    """Public WS → StreamBuffer must be opt-in (4GB RAM / connection budget)."""
    path = _BITGET_ROOT / "pipelines" / "bitget_auto_pilot.py"
    if not path.is_file():
        return {"ok": False, "message": "bitget_auto_pilot.py missing"}
    text = path.read_text(encoding="utf-8", errors="replace")
    required = (
        "BITGET_DAEMON_PUBLIC_WS",
        "def _daemon_public_ws_enabled",
        "if _daemon_public_ws_enabled():",
        "start_public_ws_market_service",
        "heartbeat_public_ws_snapshot",
        'kwargs["public_ws"]',
    )
    missing = [s for s in required if s not in text]
    ok = not missing
    return {
        "ok": ok,
        "missing": missing,
        "message": "daemon public WS policy ok" if ok else "daemon public WS policy drift",
    }


def check_daemon_private_ws_policy() -> Dict[str, Any]:
    """Private WS → PrivateStreamBuffer must be opt-in (credentials required)."""
    path = _BITGET_ROOT / "pipelines" / "bitget_auto_pilot.py"
    if not path.is_file():
        return {"ok": False, "message": "bitget_auto_pilot.py missing"}
    text = path.read_text(encoding="utf-8", errors="replace")
    required = (
        "BITGET_DAEMON_PRIVATE_WS",
        "def _daemon_private_ws_enabled",
        "if _daemon_private_ws_enabled():",
        "start_private_ws_market_service",
        "heartbeat_private_ws_snapshot",
        'kwargs["private_ws"]',
    )
    missing = [s for s in required if s not in text]
    ok = not missing
    return {
        "ok": ok,
        "missing": missing,
        "message": "daemon private WS policy ok" if ok else "daemon private WS policy drift",
    }


def _file_text(rel: str) -> tuple[Path, str]:
    path = _BITGET_ROOT / rel
    if not path.is_file():
        return path, ""
    return path, path.read_text(encoding="utf-8", errors="replace")


def _require_all(text: str, required: Tuple[str, ...]) -> List[str]:
    return [s for s in required if s not in text]


def _forbid_any(text: str, forbidden: Tuple[str, ...]) -> List[str]:
    return [s for s in forbidden if s in text]


def check_oms_book_consumer_ssot() -> Dict[str, Any]:
    """Lock OMS dual-plane consumers — executor/snapshots/slippage/telemetry.

    Prevents silent regression to inline REST on live path and tk contaminating
    private REST-heavy alerts.
    """
    details: Dict[str, Any] = {}

    # --- executor: no inline fetch_ticker / fetch_balance ---
    _, ex = _file_text("executor.py")
    ex_missing = _require_all(
        ex,
        (
            "fetch_ref_price",
            "fetch_usdt_balance",
            "prefer_ws=True",
            "prefer_ws=False",
        ),
    )
    ex_forbidden = _forbid_any(
        ex,
        (
            'op="oms.fetch_ticker"',
            'op="oms.fetch_balance"',
            "ex.fetch_ticker",
            "ex.fetch_balance",
        ),
    )
    details["executor"] = {
        "ok": bool(ex) and not ex_missing and not ex_forbidden,
        "missing": ex_missing,
        "forbidden_present": ex_forbidden,
    }

    # --- account snapshot SSOT ---
    _, acct = _file_text("trading/account_snapshot.py")
    details["account_snapshot"] = {
        "ok": bool(acct)
        and not _require_all(
            acct,
            (
                "def try_private_ws_usdt_total",
                "def fetch_usdt_balance",
                'channel_age_sec("account")',
                "get_account",
                'record_oms_source("fetch_balance"',
            ),
        ),
        "missing": _require_all(
            acct,
            (
                "def try_private_ws_usdt_total",
                "def fetch_usdt_balance",
                'channel_age_sec("account")',
                "get_account",
                'record_oms_source("fetch_balance"',
            ),
        )
        if acct
        else ["trading/account_snapshot.py missing"],
    }

    # --- public ref price SSOT ---
    _, mkt = _file_text("trading/market_price_snapshot.py")
    details["market_price_snapshot"] = {
        "ok": bool(mkt)
        and not _require_all(
            mkt,
            (
                "def try_public_ws_ref_price",
                "def fetch_ref_price",
                "normalize_inst_id",
                "PUBLIC_REF_PRICE_MAX_AGE_SEC",
                'record_oms_source("fetch_ticker"',
                '"public_ws"',
            ),
        ),
        "missing": _require_all(
            mkt,
            (
                "def try_public_ws_ref_price",
                "def fetch_ref_price",
                "normalize_inst_id",
                "PUBLIC_REF_PRICE_MAX_AGE_SEC",
                'record_oms_source("fetch_ticker"',
                '"public_ws"',
            ),
        )
        if mkt
        else ["trading/market_price_snapshot.py missing"],
    }

    # --- margin mode WS (never invent from virgin flat book) ---
    _, lev = _file_text("trading/leverage_manager.py")
    details["leverage_margin_ws"] = {
        "ok": bool(lev)
        and not _require_all(
            lev,
            (
                "def try_private_ws_margin_mode",
                "prefer_ws=True",
                "prefer_ws=False",
                'channel_age_sec("positions")',
                # posMode must not be treated as margin
                "never treat posMode",
            ),
        ),
        "missing": _require_all(
            lev,
            (
                "def try_private_ws_margin_mode",
                "prefer_ws=True",
                "prefer_ws=False",
                'channel_age_sec("positions")',
                "never treat posMode",
            ),
        )
        if lev
        else ["trading/leverage_manager.py missing"],
    }

    # --- slippage instId SSOT (BTC/USDT:USDT → BTCUSDT) ---
    _, slip = _file_text("trading/slippage_guard.py")
    slip_missing = _require_all(
        slip,
        (
            "from bitget.data.ws_stream_producer import normalize_inst_id",
            "normalize_inst_id(symbol)",
        ),
    )
    slip_forbidden = _forbid_any(slip, ("def _normalize_inst_id",))
    details["slippage_inst_id"] = {
        "ok": bool(slip) and not slip_missing and not slip_forbidden,
        "missing": slip_missing if slip else ["trading/slippage_guard.py missing"],
        "forbidden_present": slip_forbidden,
    }

    # --- dual-plane telemetry + alert + smoke ---
    _, stats = _file_text("trading/oms_source_stats.py")
    stats_req = (
        "private_status",
        "public_status",
        "private_rest_share",
        "public_rest_share",
        '"tk_ws"',
        '"bal_ws"',
        '"mm_ws"',
        '"plane": "private"',
        '"plane": "public"',
        "public_ws_enabled",
    )
    details["dual_plane_stats"] = {
        "ok": bool(stats) and not _require_all(stats, stats_req),
        "missing": _require_all(stats, stats_req)
        if stats
        else ["trading/oms_source_stats.py missing"],
    }

    _, smoke = _file_text("validation/ws_oms_smoke.py")
    smoke_req = ("oms_book_private", "oms_book_public", "private_status", "public_status")
    details["dual_plane_smoke"] = {
        "ok": bool(smoke) and not _require_all(smoke, smoke_req),
        "missing": _require_all(smoke, smoke_req)
        if smoke
        else ["validation/ws_oms_smoke.py missing"],
    }

    _, pilot = _file_text("pipelines/bitget_auto_pilot.py")
    pilot_req = (
        "maybe_warn_oms_rest_share",
        "public_ws_enabled=",
        "private_ws_enabled=",
        "oms_source_heartbeat_snapshot",
    )
    details["daemon_oms_warn"] = {
        "ok": bool(pilot) and not _require_all(pilot, pilot_req),
        "missing": _require_all(pilot, pilot_req)
        if pilot
        else ["pipelines/bitget_auto_pilot.py missing"],
    }

    # --- memory_policy knobs present ---
    _, pol = _file_text("infra/memory_policy.py")
    pol_req = (
        "PRIVATE_POS_INDEX_MAX_AGE_SEC",
        "PUBLIC_REF_PRICE_MAX_AGE_SEC",
        "OMS_REST_SHARE_WARN",
        "OMS_REST_SHARE_MIN_SAMPLES",
    )
    details["memory_policy_knobs"] = {
        "ok": bool(pol) and not _require_all(pol, pol_req),
        "missing": _require_all(pol, pol_req) if pol else ["infra/memory_policy.py missing"],
    }

    failed = [k for k, v in details.items() if not v.get("ok")]
    ok = not failed
    return {
        "ok": ok,
        "failed": failed,
        "details": details,
        "message": "OMS book consumer SSOT ok" if ok else f"OMS consumer drift: {failed}",
    }


def check_integrity_backup_ssot() -> Dict[str, Any]:
    """Lock P0 integrity backup cron + stamped-log GC + archive prune.

    CQRS snapshot is not a verified backup — institutional_db_backup must stay
    wired through bitget.sh --db-backup and daily crontab.
    """
    details: Dict[str, Any] = {}

    _, script = _file_text("scripts/institutional_db_backup.py")
    script_req = (
        "def run_backup_job",
        "def prune_old_archives",
        "PRAGMA integrity_check",
        "DB_BACKUP_KEEP_ARCHIVES",
        "cleanup_stamped_shell_logs",
    )
    details["backup_script"] = {
        "ok": bool(script) and not _require_all(script, script_req),
        "missing": _require_all(script, script_req)
        if script
        else ["scripts/institutional_db_backup.py missing"],
    }

    _, disk = _file_text("disk_manager.py")
    disk_req = (
        "def cleanup_stamped_shell_logs",
        "def is_stamped_shell_log",
        "STAMPED_LOG_RETENTION_DAYS",
        "LOG_FILE_NAME",
    )
    details["disk_manager"] = {
        "ok": bool(disk) and not _require_all(disk, disk_req),
        "missing": _require_all(disk, disk_req) if disk else ["disk_manager.py missing"],
    }

    _, pol = _file_text("infra/memory_policy.py")
    pol_req = ("STAMPED_LOG_RETENTION_DAYS", "DB_BACKUP_KEEP_ARCHIVES")
    details["memory_policy"] = {
        "ok": bool(pol) and not _require_all(pol, pol_req),
        "missing": _require_all(pol, pol_req) if pol else ["infra/memory_policy.py missing"],
    }

    _, pipes = _file_text("pipelines/bitget_pipelines.py")
    pipes_req = ("db_backup", "_step_institutional_db_backup", "run_backup_job")
    details["pipeline"] = {
        "ok": bool(pipes) and not _require_all(pipes, pipes_req),
        "missing": _require_all(pipes, pipes_req)
        if pipes
        else ["pipelines/bitget_pipelines.py missing"],
    }

    _, rt = _file_text("infra/runtime.py")
    details["runtime_mode"] = {
        "ok": bool(rt) and '"db_backup"' in (rt or ""),
        "missing": [] if (rt and '"db_backup"' in rt) else ["db_backup not in BITGET_MODES"],
    }

    sh_path = _BITGET_ROOT / "deploy" / "bitget.sh"
    sh = sh_path.read_text(encoding="utf-8", errors="replace") if sh_path.is_file() else ""
    details["bitget_sh"] = {
        "ok": "--db-backup" in sh and 'MODE="db_backup"' in sh,
        "missing": []
        if ("--db-backup" in sh and 'MODE="db_backup"' in sh)
        else ["bitget.sh --db-backup wiring missing"],
    }

    gen_path = _BITGET_ROOT / "deploy" / "generate_bitget_crontab.py"
    gen = gen_path.read_text(encoding="utf-8", errors="replace") if gen_path.is_file() else ""
    details["crontab_gen"] = {
        "ok": "--db-backup" in gen and "5 0 * * *" in gen,
        "missing": []
        if ("--db-backup" in gen and "5 0 * * *" in gen)
        else ["crontab generator missing daily --db-backup"],
    }

    failed = [k for k, v in details.items() if not v.get("ok")]
    ok = not failed
    return {
        "ok": ok,
        "failed": failed,
        "details": details,
        "message": "integrity backup SSOT ok" if ok else f"integrity backup drift: {failed}",
    }


def check_portfolio_nav_risk_ssot() -> Dict[str, Any]:
    """Lock capital-survival gates — circuit + NAV + gross + doomsday + concentration + MAX_LEVERAGE.

    Paper≈live via execution_safety / ledger / oms_core. Never auto-flatten.
    """
    details: Dict[str, Any] = {}

    _, safety = _file_text("trading/execution_safety.py")
    safety_req = (
        "GLOBAL_CIRCUIT_BREAKER",
        "def evaluate_nav_risk_gate",
        "def evaluate_orphan_gate",
        "def evaluate_gross_notional_gate",
        "def evaluate_tail_risk_gate",
        "def evaluate_doomsday_gate",
        "def evaluate_concentration_gate",
        "def oms_defense_block_reason",
        "def max_leverage_cap",
        "ORPHAN_BLOCKED",
        "NAV_BLOCKED",
        "GROSS_BLOCKED",
        "TAIL_RISK_BLOCKED",
        "DOOMSDAY_BLOCKED",
        "CONCENTRATION_BLOCKED",
        "PRICE_SANITY_BLOCKED",
        "CIRCUIT_BLOCKED",
        "nav_size_mult",
        "tail_risk_size_mult",
        "doomsday_size_mult",
        "def evaluate_price_sanity_gate",
        "portfolio_nav_snapshot",
        "no auto-flatten",
    )
    details["execution_safety"] = {
        "ok": bool(safety) and not _require_all(safety, safety_req),
        "missing": _require_all(safety, safety_req)
        if safety
        else ["trading/execution_safety.py missing"],
    }

    _, tail = _file_text("trading/tail_risk_gate.py")
    tail_req = (
        "def accrue_tail_risk_fund",
        "def tail_risk_entry_blocked",
        "release_1to1",
        "Never auto-flatten",
        "never mint",
    )
    details["tail_risk_gate"] = {
        "ok": bool(tail) and not _require_all(tail, tail_req),
        "missing": _require_all(tail, tail_req)
        if tail
        else ["trading/tail_risk_gate.py missing"],
    }

    _, doom = _file_text("trading/doomsday_gate.py")
    doom_req = (
        "def doomsday_long_entry_blocked",
        "def crypto_contagion_score",
        "Never auto-flatten",
        "Soft-pass",
    )
    details["doomsday_gate"] = {
        "ok": bool(doom) and not _require_all(doom, doom_req),
        "missing": _require_all(doom, doom_req)
        if doom
        else ["trading/doomsday_gate.py missing"],
    }

    _, conc = _file_text("trading/concentration_gate.py")
    conc_req = (
        "def concentration_entry_blocked",
        "def corr_vs_btc",
        "soft_pass",
        "Never auto-flatten",
    )
    details["concentration_gate"] = {
        "ok": bool(conc) and not _require_all(conc, conc_req),
        "missing": _require_all(conc, conc_req)
        if conc
        else ["trading/concentration_gate.py missing"],
    }

    _, psan = _file_text("trading/price_sanity_gate.py")
    psan_req = (
        "def price_sanity_entry_blocked",
        "def analyze_price_sanity",
        "Never auto-flatten",
        "Soft-pass",
    )
    details["price_sanity_gate"] = {
        "ok": bool(psan) and not _require_all(psan, psan_req),
        "missing": _require_all(psan, psan_req)
        if psan
        else ["trading/price_sanity_gate.py missing"],
    }

    _, ex = _file_text("executor.py")
    ex_req = (
        "CIRCUIT_BLOCKED",
        "ORPHAN_BLOCKED",
        "NAV_BLOCKED",
        "GROSS_BLOCKED",
        "TAIL_RISK_BLOCKED",
        "DOOMSDAY_BLOCKED",
        "CONCENTRATION_BLOCKED",
        "PRICE_SANITY_BLOCKED",
        "position_side=side_u",
        "nav_size_mult",
        "tail_risk_size_mult",
        "doomsday_size_mult",
        "amount_after_nav_reduce",
        "GLOBAL_CIRCUIT_BREAKER",
    )
    details["executor"] = {
        "ok": bool(ex) and not _require_all(ex, ex_req),
        "missing": _require_all(ex, ex_req) if ex else ["executor.py missing"],
    }

    _, oms = _file_text("trading/oms_core.py")
    oms_req = (
        "oms_defense_block_reason",
        "circuit_blocked",
        "orphan_blocked",
        "nav_blocked",
        "gross_blocked",
        "tail_risk_blocked",
        "doomsday_blocked",
        "concentration_blocked",
        "price_sanity_blocked",
        "position_side",
    )
    details["oms_core"] = {
        "ok": bool(oms) and not _require_all(oms, oms_req),
        "missing": _require_all(oms, oms_req) if oms else ["trading/oms_core.py missing"],
    }

    _, lev = _file_text("trading/leverage_manager.py")
    lev_req = ("max_leverage_cap",)
    details["leverage_manager"] = {
        "ok": bool(lev) and not _require_all(lev, lev_req),
        "missing": _require_all(lev, lev_req)
        if lev
        else ["trading/leverage_manager.py missing"],
    }

    _, nav = _file_text("live_nav_manager.py")
    nav_req = ("def portfolio_nav_snapshot", "mdd_pct")
    details["live_nav_manager"] = {
        "ok": bool(nav) and not _require_all(nav, nav_req),
        "missing": _require_all(nav, nav_req)
        if nav
        else ["live_nav_manager.py missing"],
    }

    _, pol = _file_text("infra/memory_policy.py")
    pol_req = (
        "NAV_DD_REDUCE_PCT",
        "NAV_DD_BLOCK_PCT",
        "NAV_DD_HALT_PCT",
        "NAV_DD_REDUCE_SIZE_MULT",
        "DEFAULT_MAX_LEVERAGE",
        "OMS_ORPHAN_STREAK_PROPOSE_KILL",
        "GROSS_NOTIONAL_MAX_PCT",
        "CORR_BTC_MIN",
        "CORR_CLUSTER_MAX_PCT",
        "DOOMSDAY_BLOCK_LEVEL",
        "TAIL_RISK_ACCRUAL_PCT",
        "TAIL_RISK_MIN_COVERAGE_PCT",
        "TAIL_RISK_EMPTY_BLOCK",
        "BAD_TICK_MAX_GAP_PCT",
        "BAD_TICK_LOOKBACK_BARS",
    )
    details["memory_policy"] = {
        "ok": bool(pol) and not _require_all(pol, pol_req),
        "missing": _require_all(pol, pol_req) if pol else ["infra/memory_policy.py missing"],
    }

    _, recon = _file_text("trading/reconciliation.py")
    recon_req = (
        "def apply_orphan_escalation",
        "OMS_ORPHAN_ACTIVE",
        "OMS_ORPHAN_KILL_SWITCH_PROPOSED",
        "NEVER auto-flatten",
        "LAST_OMS_RECON_ORPHANS",
    )
    details["reconciliation"] = {
        "ok": bool(recon) and not _require_all(recon, recon_req),
        "missing": _require_all(recon, recon_req)
        if recon
        else ["trading/reconciliation.py missing"],
    }

    _, br = _file_text("infra/bounded_reads.py")
    br_req = (
        "def forward_open_gross_notional_sum_sql",
        "def forward_open_concentration_book_sql",
    )
    details["bounded_reads_gross"] = {
        "ok": bool(br) and not _require_all(br, br_req),
        "missing": _require_all(br, br_req)
        if br
        else ["infra/bounded_reads.py missing"],
    }

    _, led = _file_text("forward/ledger.py")
    led_req = (
        "gross_entry_blocked",
        "명목노출 상한",
        "evaluate_nav_risk_gate",
        "NAV 드로다운",
        "nav_size_mult",
        "max_leverage_cap",
        "tail_risk_entry_blocked",
        "테일리스크",
        "doomsday_long_entry_blocked",
        "둠스데이 DEFCON",
        "concentration_entry_blocked",
        "집중도 상한",
        "price_sanity_entry_blocked",
        "배드틱",
    )
    details["paper_ledger_gross"] = {
        "ok": bool(led) and not _require_all(led, led_req),
        "missing": _require_all(led, led_req) if led else ["forward/ledger.py missing"],
    }

    _, meta_c = _file_text("governance/meta_consumer.py")
    meta_req = ("apply_doomsday_dampening",)
    details["meta_kelly_doomsday"] = {
        "ok": bool(meta_c) and not _require_all(meta_c, meta_req),
        "missing": _require_all(meta_c, meta_req)
        if meta_c
        else ["governance/meta_consumer.py missing"],
    }

    _, radar = _file_text("doomsday_bot.py")
    radar_req = ("Global_Contagion_Score", "crypto_contagion_score")
    details["doomsday_radar_score"] = {
        "ok": bool(radar) and not _require_all(radar, radar_req),
        "missing": _require_all(radar, radar_req) if radar else ["doomsday_bot.py missing"],
    }

    failed = [k for k, v in details.items() if not v.get("ok")]
    ok = not failed
    return {
        "ok": ok,
        "failed": failed,
        "details": details,
        "message": "portfolio NAV risk SSOT ok" if ok else f"NAV risk gate drift: {failed}",
    }


def check_meta_alerts_ssot() -> Dict[str, Any]:
    """Phase 7 — Bitget meta critical alerts (factory_meta_alerts 패리티)."""
    path = _BITGET_ROOT / "governance" / "meta_alerts.py"
    sync_path = _BITGET_ROOT / "governance" / "meta_sync.py"
    if not path.is_file():
        return {"ok": False, "message": "governance/meta_alerts.py missing"}
    text = path.read_text(encoding="utf-8", errors="replace")
    sync_text = sync_path.read_text(encoding="utf-8", errors="replace") if sync_path.is_file() else ""
    ok = (
        "def send_meta_critical_alert" in text
        and "bitget.forward.shared" in text
        and "send_meta_critical_alert" in sync_text
        and "MetaGovernor" in sync_text
        and "run_governor_cycle" in sync_text
    )
    return {
        "ok": ok,
        "message": "meta_alerts + governor cycle ok" if ok else "meta governance drift",
    }


def check_governance_infra_removed() -> Dict[str, Any]:
    """Phase 6 — deprecated `governance/infra` shim must not exist; SSOT is `bitget/infra`."""
    shim_dir = _BITGET_ROOT / "governance" / "infra"
    remnants = []
    if shim_dir.is_dir():
        remnants = [
            str(p.relative_to(_BITGET_ROOT))
            for p in shim_dir.rglob("*")
            if p.is_file() and "__pycache__" not in p.parts
        ]
    ok = not remnants
    return {
        "ok": ok,
        "shim_dir_exists": shim_dir.is_dir(),
        "remnants": remnants,
        "ssot": "bitget/infra",
        "message": "governance/infra shim removed" if ok else f"shim remnants: {remnants}",
    }


def check_no_merge_conflict_markers() -> Dict[str, Any]:
    """Phase 7 — git conflict markers must not ship in bitget/."""
    offenders: List[str] = []
    for path in _BITGET_ROOT.rglob("*"):
        if not path.is_file():
            continue
        if "__pycache__" in path.parts:
            continue
        if path.suffix not in (".py", ".md", ".sh", ".in", ".example"):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in text.splitlines():
            if line.startswith("<<<<<<< HEAD") or line.startswith(">>>>>>>"):
                offenders.append(str(path.relative_to(_BITGET_ROOT)))
                break
    ok = not offenders
    return {
        "ok": ok,
        "offenders": offenders[:20],
        "n_offenders": len(offenders),
        "message": "no conflict markers" if ok else f"conflict markers in {len(offenders)} files",
    }


def check_watchdog_restart_matrix_ssot() -> Dict[str, Any]:
    """Lock multi-unit watchdog restart matrix (factory / queue / ws / async).

    Invariants: per-unit budget, queue only with work, never auto-flatten,
    Persistent=true on watchdog timer (parity with snapshot).
    """
    details: Dict[str, Any] = {}

    _, wd = _file_text("watchdog.py")
    wd_req = (
        "def execute_unit_restart",
        "def restart_budget_ok",
        "def unit_restart_cmd",
        "def evaluate_ws_plane_health",
        "UNIT_QUEUE",
        "UNIT_WS",
        "UNIT_ASYNC",
        "BITGET_WATCHDOG_RESTART_QUEUE",
        "BITGET_WATCHDOG_RESTART_ASYNC",
        "no auto-flatten",
        "_monitor_ws_plane",
        "_monitor_async_plane",
        "dante-bitget-async",
    )
    details["watchdog"] = {
        "ok": bool(wd) and not _require_all(wd, wd_req),
        "missing": _require_all(wd, wd_req) if wd else ["watchdog.py missing"],
    }

    sudo_path = _BITGET_ROOT / "deploy" / "ubuntu" / "bitget-watchdog-sudoers.example"
    sudo = sudo_path.read_text(encoding="utf-8", errors="replace") if sudo_path.is_file() else ""
    sudo_req = (
        "dante-bitget-factory",
        "dante-bitget-queue-worker",
        "dante-bitget-ws",
        "dante-bitget-async",
    )
    details["sudoers"] = {
        "ok": bool(sudo) and not _require_all(sudo, sudo_req),
        "missing": _require_all(sudo, sudo_req) if sudo else ["sudoers example missing"],
    }

    timer_path = _BITGET_ROOT / "deploy" / "systemd" / "dante-bitget-watchdog.timer"
    timer = timer_path.read_text(encoding="utf-8", errors="replace") if timer_path.is_file() else ""
    details["watchdog_timer"] = {
        "ok": "Persistent=true" in timer,
        "missing": [] if "Persistent=true" in timer else ["Persistent=true missing on watchdog.timer"],
    }

    _, async_entry = _file_text("async_telegram_daemon.py")
    async_req = (
        "def _patch_bitget_ops_logger",
        "bitget.infra.ops_logger",
        "atd.ops_logger",
    )
    details["async_ops_patch"] = {
        "ok": bool(async_entry) and not _require_all(async_entry, async_req),
        "missing": _require_all(async_entry, async_req)
        if async_entry
        else ["async_telegram_daemon.py missing"],
    }

    failed = [k for k, v in details.items() if not v.get("ok")]
    ok = not failed
    return {
        "ok": ok,
        "failed": failed,
        "details": details,
        "message": "watchdog restart matrix ok" if ok else f"watchdog matrix drift: {failed}",
    }


def check_watchdog_component_env() -> Dict[str, Any]:
    """Watchdog heartbeat component가 bitget.main 이 아닌지."""
    comp = str(os.environ.get("BITGET_WATCHDOG_HEARTBEAT_COMPONENT", "")).strip()
    bad = comp.lower() in ("bitget.main", "main", "bitget.main:main")
    ok = not bad
    recommended = "bitget_auto_pilot"
    return {
        "ok": ok,
        "component": comp or "(unset)",
        "recommended": recommended,
        "message": "watchdog component ok" if ok else f"wrong component={comp}",
    }


def _wrap_regime_audit() -> Dict[str, Any]:
    from bitget.validation.regime_audit import run_regime_kelly_audit

    r = run_regime_kelly_audit()
    return {
        "ok": bool(r.get("passed")),
        "passed": bool(r.get("passed")),
        "message": r.get("message"),
        "audit": r,
    }


def run_architecture_checks() -> Dict[str, Any]:
    """모든 아키텍처 불변식 실행."""
    runners: Tuple[Tuple[str, Callable[[], Dict[str, Any]]], ...] = (
        ("legacy_entrypoints", check_legacy_entrypoints_blocked),
        ("pipeline_structure", check_pipeline_structure),
        ("scan_schedule_ssot", check_scan_schedule_ssot),
        ("governance_infra_removed", check_governance_infra_removed),
        ("bitget_shell_daily_audit_guard", check_bitget_shell_daily_audit_guard),
        ("weekly_evolution_pipeline", check_weekly_evolution_pipeline),
        ("deep_dive_dna_autopsy_ssot", check_deep_dive_dna_autopsy_ssot),
        ("daemon_sniper_policy", check_daemon_sniper_policy),
        ("daemon_public_ws_policy", check_daemon_public_ws_policy),
        ("daemon_private_ws_policy", check_daemon_private_ws_policy),
        ("oms_book_consumer_ssot", check_oms_book_consumer_ssot),
        ("portfolio_nav_risk_ssot", check_portfolio_nav_risk_ssot),
        ("integrity_backup_ssot", check_integrity_backup_ssot),
        ("watchdog_restart_matrix_ssot", check_watchdog_restart_matrix_ssot),
        ("meta_alerts_ssot", check_meta_alerts_ssot),
        ("no_merge_conflict_markers", check_no_merge_conflict_markers),
        ("satellite_config_hub", check_satellite_config_hub),
        ("config_hard_bounds_ssot", check_config_hard_bounds_ssot),
        ("scanner_engine_pool_ssot", check_scanner_engine_pool_ssot),
        ("exploration_budget_market_ssot", check_exploration_budget_market_ssot),
        ("regime_kelly_audit", lambda: _wrap_regime_audit()),
        ("watchdog_component", check_watchdog_component_env),
    )
    checks: Dict[str, Any] = {}
    for name, fn in runners:
        checks[name] = fn()
    passed = all(bool(c.get("ok")) for c in checks.values())
    failed = [k for k, v in checks.items() if not v.get("ok")]
    return {
        "ok": True,
        "passed": passed,
        "checks": checks,
        "failed": failed,
        "message": "architecture checks PASS" if passed else f"failed: {failed}",
    }
