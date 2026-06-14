<<<<<<< HEAD
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
    "deep_dive_spot",
    "deep_dive_futures",
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
    track_ok = track == ["config_bootstrap", "artifact_guard", "track_spot", "track_futures"]

    ok = daily_pre_ok and scan_pre_ok and daily_body_ok and track_ok
    return {
        "ok": ok,
        "daily_audit": daily,
        "scan_spot": scan,
        "track_positions": track,
        "daily_prelude": daily_pre_msg,
        "scan_prelude": scan_pre_msg,
        "daily_body_keys": daily_body_ok,
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
=======
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
    "deep_dive_spot",
    "deep_dive_futures",
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
    track_ok = track == ["config_bootstrap", "artifact_guard", "track_spot", "track_futures"]

    ok = daily_pre_ok and scan_pre_ok and daily_body_ok and track_ok
    return {
        "ok": ok,
        "daily_audit": daily,
        "scan_spot": scan,
        "track_positions": track,
        "daily_prelude": daily_pre_msg,
        "scan_prelude": scan_pre_msg,
        "daily_body_keys": daily_body_ok,
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
>>>>>>> a6f17ca59385c6492c35a2f0368a732550fef092
