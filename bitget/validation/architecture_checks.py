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
    daily_count_ok = len(daily) == 17
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
        ("meta_alerts_ssot", check_meta_alerts_ssot),
        ("no_merge_conflict_markers", check_no_merge_conflict_markers),
        ("satellite_config_hub", check_satellite_config_hub),
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
