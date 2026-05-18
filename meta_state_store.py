"""
MetaGovernor 상태 SSOT — system_config.sqlite (config_kv) + JSON 미러.

- git clean 으로 JSON 만 삭제돼도 DB 스냅샷에서 복구
- 리포트 직전 UNKNOWN/NEVER 이면 regime + governor 동기 재실행
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any, Dict, Optional

from meta_governor import default_meta_state, meta_state_path

logger = logging.getLogger(__name__)

META_STATE_KV_KEY = "META_GOVERNOR_STATE"


def is_meta_state_degraded(state: Optional[Dict[str, Any]]) -> bool:
    """UNKNOWN·NEVER·notes 공백·신뢰도 0 — 리포트 전 동기 복구 트리거."""
    if not isinstance(state, dict) or not state:
        return True
    status = str(state.get("META_GOVERNOR_LAST_RUN_STATUS") or "").upper()
    if status in ("NEVER", "", "ERROR", "FAILED"):
        return True
    if not state.get("META_GOVERNOR_LAST_RUN_AT"):
        return True
    rk = str(state.get("META_REGIME_KEY") or "").strip().upper()
    if rk in ("", "UNKNOWN"):
        return True
    try:
        conf = float(state.get("META_REGIME_CONFIDENCE") or 0.0)
    except (TypeError, ValueError):
        conf = 0.0
    if conf <= 0.01 and rk == "UNKNOWN":
        return True
    ra = state.get("META_REGIME_ACTION")
    if not isinstance(ra, dict):
        return True
    notes = ra.get("notes")
    if isinstance(notes, str) and not notes.strip():
        if rk == "UNKNOWN" and conf <= 0.05:
            return True
    return False


def regime_analysis_stale_or_missing(sys_config: Optional[Dict[str, Any]] = None) -> bool:
    """REGIME_ANALYSIS 없음·UNKNOWN·지수 ok=0 이면 regime_meta_analyzer 선행 필요."""
    cfg = sys_config
    if cfg is None:
        try:
            from config_manager import load_system_config

            cfg = load_system_config()
        except Exception:
            cfg = {}
    ra = (cfg or {}).get("REGIME_ANALYSIS")
    if not isinstance(ra, dict) or not ra:
        return True
    rk = str(ra.get("regime_key") or "").strip().upper()
    if rk in ("", "UNKNOWN"):
        return True
    indices = ra.get("indices") if isinstance(ra.get("indices"), dict) else {}
    ok_ct = 0
    for sym in ("GSPC", "KOSPI"):
        blk = indices.get(sym) if isinstance(indices.get(sym), dict) else {}
        if blk.get("ok"):
            ok_ct += 1
    return ok_ct == 0


def _load_from_sqlite() -> Optional[Dict[str, Any]]:
    try:
        from config_manager import get_config_value

        raw = get_config_value(META_STATE_KV_KEY)
        if isinstance(raw, dict) and raw.get("META_SCHEMA_VERSION"):
            return raw
    except Exception as e:
        logger.debug("meta_state_store: sqlite load skip: %s", e)
    return None


def _save_to_sqlite(state: Dict[str, Any]) -> None:
    from config_manager import set_config_value

    set_config_value(META_STATE_KV_KEY, state)


def _load_json_mirror(path: str) -> Dict[str, Any]:
    if not os.path.isfile(path):
        return default_meta_state()
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return default_meta_state()
        out = default_meta_state()
        out.update(raw)
        return out
    except Exception as e:
        logger.warning("meta_state_store: JSON load failed %s: %s", path, e)
        return default_meta_state()


def _save_json_mirror(state: Dict[str, Any], path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    payload = json.dumps(state, ensure_ascii=False, indent=2)
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".meta_governor_", suffix=".json.tmp", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            if os.path.isfile(tmp):
                os.remove(tmp)
        except OSError:
            pass
        raise


def load_meta_governor_state_unified(path: Optional[str] = None) -> Dict[str, Any]:
    """
    1) config_kv META_GOVERNOR_STATE
    2) meta_governor_state.json
    3) default + JSON→SQLite 마이그레이션(유효 스냅샷만)
    """
    sqlite_state = _load_from_sqlite()
    if sqlite_state is not None and not is_meta_state_degraded(sqlite_state):
        out = default_meta_state()
        out.update(sqlite_state)
        return out

    p = path or meta_state_path()
    file_state = _load_json_mirror(p)
    out = default_meta_state()
    out.update(file_state)

    if not is_meta_state_degraded(out):
        try:
            _save_to_sqlite(out)
            logger.info("meta_state_store: migrated JSON snapshot → SQLite %s", META_STATE_KV_KEY)
        except Exception as e:
            logger.warning("meta_state_store: sqlite migrate failed: %s", e)
    elif sqlite_state is not None:
        out = default_meta_state()
        out.update(sqlite_state)
    return out


def save_meta_governor_state_unified(state: Dict[str, Any], path: Optional[str] = None) -> None:
    """SQLite SSOT + JSON 미러(레거시·백업·수동 점검용)."""
    p = path or meta_state_path()
    _save_to_sqlite(state)
    _save_json_mirror(state, p)


def rebuild_meta_state(*, force: bool = False, refresh_regime: bool = True) -> Dict[str, Any]:
    """
    1) REGIME_ANALYSIS 갱신 (yfinance·forward_trades 콜로세움)
    2) MetaGovernor run_governor_cycle
  리포트 [1/9]·[8/9] 직전 동기 호출용.
    """
    result: Dict[str, Any] = {"regime": "skipped", "meta": "skipped"}

    need_regime = refresh_regime and (force or regime_analysis_stale_or_missing())
    if need_regime:
        try:
            from regime_meta_analyzer import analyze_market_regime

            analyze_market_regime()
            result["regime"] = "refreshed"
        except Exception as e:
            result["regime"] = "failed"
            result["regime_error"] = str(e)
            logger.exception("rebuild_meta_state: regime_meta_analyzer failed: %s", e)

    try:
        from factory_artifact_guard import ensure_meta_governor_state

        cur = load_meta_governor_state_unified(meta_state_path())
        need_meta = force or is_meta_state_degraded(cur)
        heal = ensure_meta_governor_state(force=need_meta)
        result["meta"] = heal.get("meta", "ok")
        result["meta_status"] = heal.get("meta_status")
        if heal.get("regime"):
            result["regime"] = heal.get("regime")
    except Exception as e:
        result["meta"] = "failed"
        result["meta_error"] = str(e)
        logger.exception("rebuild_meta_state: ensure_meta_governor_state failed: %s", e)

    try:
        from meta_governor_consumer import invalidate_meta_state_cache

        invalidate_meta_state_cache()
    except Exception:
        pass
    return result


def ensure_meta_state_for_report(*, force: bool = False) -> Dict[str, Any]:
    """리포트 SSOT: degraded 이면 rebuild 후 최신 메타 반환."""
    from meta_governor_consumer import invalidate_meta_state_cache, load_meta_state_resolved

    meta = load_meta_state_resolved()
    if force or is_meta_state_degraded(meta):
        rebuild_meta_state(force=True, refresh_regime=True)
        invalidate_meta_state_cache()
        meta = load_meta_state_resolved()
    return meta
