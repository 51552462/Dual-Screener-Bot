<<<<<<< HEAD
"""
Bitget Regime / Meta / Kelly 감사 — 주식 config·meta DB와 완전 분리 검증.

로컬 CI(Track A) 및 cutover 전 서버 점검에 사용.
데이터 소스 SSOT:
  - config → bitget.infra.config_manager → bitget_system_config.sqlite
  - meta   → bitget.governance.meta_sync / meta_consumer (Bitget JSON+KV)
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

from bitget.infra.data_paths import meta_governor_state_path, system_config_db_path

_KELLY_HARD_MAX = 0.25


def _basename(path: str) -> str:
    return os.path.basename(str(path or ""))


def run_regime_kelly_audit(
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Regime·Kelly가 Bitget 전용 SQLite/KV에서 읽히고 meta와 정합한지 감사.

    Returns:
        ok: 감사 실행 성공 여부
        passed: 모든 불변식 통과 여부 (CI assert용)
    """
    from bitget.config_hub import load_config
    from bitget.governance.meta_consumer import (
        load_meta_state_resolved,
        resolve_trading_kelly_base,
    )
    from bitget.governance.meta_sync import (
        is_bitget_meta_degraded,
        is_config_regime_misaligned,
        normalize_regime_key,
        resolve_config_regime_key,
    )

    cfg_db = system_config_db_path()
    meta_path = meta_governor_state_path()

    try:
        cfg = sys_config if isinstance(sys_config, dict) else load_config()
        meta_state = meta if isinstance(meta, dict) else load_meta_state_resolved()
    except Exception as exc:
        return {
            "ok": False,
            "passed": False,
            "error": str(exc),
            "message": "regime_kelly_audit: load failed",
        }

    rk_cfg = normalize_regime_key(cfg.get("CURRENT_REGIME_KEY"))
    rk_resolved = normalize_regime_key(resolve_config_regime_key(cfg))
    rk_meta = normalize_regime_key(meta_state.get("META_REGIME_KEY"))
    misaligned = is_config_regime_misaligned(meta_state, sys_config=cfg)
    degraded = is_bitget_meta_degraded(meta_state)

    kelly_cfg = float(cfg.get("DYNAMIC_KELLY_RISK", 0.0) or 0.0)
    kelly_resolved = float(resolve_trading_kelly_base(cfg, meta_state))

    ra = cfg.get("REGIME_ANALYSIS")
    rk_ra = (
        normalize_regime_key(ra.get("regime_key"))
        if isinstance(ra, dict)
        else "UNKNOWN"
    )

    isolation = {
        "config_db_filename": _basename(cfg_db),
        "config_db_is_bitget_sqlite": _basename(cfg_db) == "bitget_system_config.sqlite",
        "config_db_path_contains_bitget": "bitget" in cfg_db.replace("\\", "/").lower(),
        "meta_json_is_bitget": "bitget" in _basename(meta_path).lower()
        or "bitget" in meta_path.replace("\\", "/").lower(),
        "meta_json_path": meta_path,
    }

    regime = {
        "CURRENT_REGIME_KEY": rk_cfg,
        "resolve_config_regime_key": rk_resolved,
        "REGIME_ANALYSIS.regime_key": rk_ra,
        "META_REGIME_KEY": rk_meta,
        "misaligned": misaligned,
        "meta_degraded": degraded,
    }

    kelly = {
        "DYNAMIC_KELLY_RISK": kelly_cfg,
        "resolve_trading_kelly_base": kelly_resolved,
        "within_hard_max": 0.0 <= kelly_resolved <= _KELLY_HARD_MAX,
        "non_negative": kelly_resolved >= 0.0,
    }

    flags = {
        "regime_keys_known": rk_cfg not in ("", "UNKNOWN") or rk_meta not in ("", "UNKNOWN"),
        "regime_aligned": not misaligned,
        "meta_fresh": not degraded,
        "kelly_bounds_ok": kelly["within_hard_max"] and kelly["non_negative"],
        "ssot_isolation_ok": isolation["config_db_is_bitget_sqlite"]
        and isolation["config_db_path_contains_bitget"],
    }

    passed = all(flags.values())
    failed = [k for k, v in flags.items() if not v]

    return {
        "ok": True,
        "passed": passed,
        "isolation": isolation,
        "regime": regime,
        "kelly": kelly,
        "flags": flags,
        "failed": failed,
        "config_db_path": cfg_db,
        "message": "regime/kelly audit PASS" if passed else f"regime/kelly audit FAIL: {failed}",
    }
=======
"""
Bitget Regime / Meta / Kelly 감사 — 주식 config·meta DB와 완전 분리 검증.

로컬 CI(Track A) 및 cutover 전 서버 점검에 사용.
데이터 소스 SSOT:
  - config → bitget.infra.config_manager → bitget_system_config.sqlite
  - meta   → bitget.governance.meta_sync / meta_consumer (Bitget JSON+KV)
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

from bitget.infra.data_paths import meta_governor_state_path, system_config_db_path

_KELLY_HARD_MAX = 0.25


def _basename(path: str) -> str:
    return os.path.basename(str(path or ""))


def run_regime_kelly_audit(
    *,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Regime·Kelly가 Bitget 전용 SQLite/KV에서 읽히고 meta와 정합한지 감사.

    Returns:
        ok: 감사 실행 성공 여부
        passed: 모든 불변식 통과 여부 (CI assert용)
    """
    from bitget.config_hub import load_config
    from bitget.governance.meta_consumer import (
        load_meta_state_resolved,
        resolve_trading_kelly_base,
    )
    from bitget.governance.meta_sync import (
        is_bitget_meta_degraded,
        is_config_regime_misaligned,
        normalize_regime_key,
        resolve_config_regime_key,
    )

    cfg_db = system_config_db_path()
    meta_path = meta_governor_state_path()

    try:
        cfg = sys_config if isinstance(sys_config, dict) else load_config()
        meta_state = meta if isinstance(meta, dict) else load_meta_state_resolved()
    except Exception as exc:
        return {
            "ok": False,
            "passed": False,
            "error": str(exc),
            "message": "regime_kelly_audit: load failed",
        }

    rk_cfg = normalize_regime_key(cfg.get("CURRENT_REGIME_KEY"))
    rk_resolved = normalize_regime_key(resolve_config_regime_key(cfg))
    rk_meta = normalize_regime_key(meta_state.get("META_REGIME_KEY"))
    misaligned = is_config_regime_misaligned(meta_state, sys_config=cfg)
    degraded = is_bitget_meta_degraded(meta_state)

    kelly_cfg = float(cfg.get("DYNAMIC_KELLY_RISK", 0.0) or 0.0)
    kelly_resolved = float(resolve_trading_kelly_base(cfg, meta_state))

    ra = cfg.get("REGIME_ANALYSIS")
    rk_ra = (
        normalize_regime_key(ra.get("regime_key"))
        if isinstance(ra, dict)
        else "UNKNOWN"
    )

    isolation = {
        "config_db_filename": _basename(cfg_db),
        "config_db_is_bitget_sqlite": _basename(cfg_db) == "bitget_system_config.sqlite",
        "config_db_path_contains_bitget": "bitget" in cfg_db.replace("\\", "/").lower(),
        "meta_json_is_bitget": "bitget" in _basename(meta_path).lower()
        or "bitget" in meta_path.replace("\\", "/").lower(),
        "meta_json_path": meta_path,
    }

    regime = {
        "CURRENT_REGIME_KEY": rk_cfg,
        "resolve_config_regime_key": rk_resolved,
        "REGIME_ANALYSIS.regime_key": rk_ra,
        "META_REGIME_KEY": rk_meta,
        "misaligned": misaligned,
        "meta_degraded": degraded,
    }

    kelly = {
        "DYNAMIC_KELLY_RISK": kelly_cfg,
        "resolve_trading_kelly_base": kelly_resolved,
        "within_hard_max": 0.0 <= kelly_resolved <= _KELLY_HARD_MAX,
        "non_negative": kelly_resolved >= 0.0,
    }

    flags = {
        "regime_keys_known": rk_cfg not in ("", "UNKNOWN") or rk_meta not in ("", "UNKNOWN"),
        "regime_aligned": not misaligned,
        "meta_fresh": not degraded,
        "kelly_bounds_ok": kelly["within_hard_max"] and kelly["non_negative"],
        "ssot_isolation_ok": isolation["config_db_is_bitget_sqlite"]
        and isolation["config_db_path_contains_bitget"],
    }

    passed = all(flags.values())
    failed = [k for k, v in flags.items() if not v]

    return {
        "ok": True,
        "passed": passed,
        "isolation": isolation,
        "regime": regime,
        "kelly": kelly,
        "flags": flags,
        "failed": failed,
        "config_db_path": cfg_db,
        "message": "regime/kelly audit PASS" if passed else f"regime/kelly audit FAIL: {failed}",
    }
>>>>>>> a6f17ca59385c6492c35a2f0368a732550fef092
