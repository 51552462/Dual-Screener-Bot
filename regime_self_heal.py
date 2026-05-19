"""
Regime SSOT self-healing — meta/config 국면 불일치 N회 연속 시 백그라운드 rebuild.

상태 키: REGIME_HEAL_STATE (config_kv)
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

HEAL_STATE_KEY = "REGIME_HEAL_STATE"
DEFAULT_MISMATCH_THRESHOLD = 3
_HEAL_LOCK = threading.Lock()
_HEAL_RUNNING = False


def _heal_threshold(sys_config: Optional[Dict[str, Any]] = None) -> int:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    rules = cfg.get("OVERSEER_AUDIT_RULES")
    base = rules if isinstance(rules, dict) else cfg
    try:
        v = int(base.get("REGIME_HEAL_MISMATCH_THRESHOLD", DEFAULT_MISMATCH_THRESHOLD))
        return max(1, min(10, v))
    except (TypeError, ValueError):
        return DEFAULT_MISMATCH_THRESHOLD


def _load_heal_state() -> Dict[str, Any]:
    try:
        from config_manager import get_config_value

        raw = get_config_value(HEAL_STATE_KEY)
        return dict(raw) if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _save_heal_state(state: Dict[str, Any]) -> None:
    try:
        from config_manager import set_config_value

        set_config_value(HEAL_STATE_KEY, state)
    except Exception as e:
        logger.warning("regime_self_heal: state save failed: %s", e)


def is_regime_misaligned(
    meta: Optional[Dict[str, Any]],
    sys_config: Optional[Dict[str, Any]] = None,
) -> bool:
    from meta_state_store import normalize_regime_key, resolve_config_regime_key

    m = meta if isinstance(meta, dict) else {}
    rk_meta = normalize_regime_key(m.get("META_REGIME_KEY"))
    rk_cfg = resolve_config_regime_key(sys_config)
    if rk_meta in ("", "UNKNOWN"):
        return rk_cfg not in ("", "UNKNOWN")
    return rk_cfg in ("", "UNKNOWN") or rk_meta != rk_cfg


def tick_regime_mismatch(
    meta: Optional[Dict[str, Any]],
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    threshold: Optional[int] = None,
    auto_rebuild: bool = True,
) -> Dict[str, Any]:
    """
    불일치 1회 기록. threshold 연속 도달 시 rebuild_meta_state(force=True) 백그라운드 격발.
    """
    global _HEAL_RUNNING
    th = threshold if threshold is not None else _heal_threshold(sys_config)
    aligned = not is_regime_misaligned(meta, sys_config)
    st = _load_heal_state()
    streak = int(st.get("mismatch_streak") or 0)

    if aligned:
        if streak > 0:
            st["mismatch_streak"] = 0
            st["last_aligned_at_utc"] = datetime.now(timezone.utc).isoformat()
            _save_heal_state(st)
        return {"aligned": True, "mismatch_streak": 0, "rebuild_scheduled": False}

    streak += 1
    st["mismatch_streak"] = streak
    st["last_mismatch_at_utc"] = datetime.now(timezone.utc).isoformat()
    from meta_state_store import normalize_regime_key, resolve_config_regime_key

    m = meta if isinstance(meta, dict) else {}
    st["last_meta_regime"] = normalize_regime_key(m.get("META_REGIME_KEY"))
    st["last_config_regime"] = resolve_config_regime_key(sys_config)
    _save_heal_state(st)

    rebuild_scheduled = False
    if auto_rebuild and streak >= th:
        with _HEAL_LOCK:
            if not _HEAL_RUNNING:
                _HEAL_RUNNING = True
                rebuild_scheduled = schedule_background_meta_rebuild(
                    reason=f"mismatch_streak={streak}"
                )

    return {
        "aligned": False,
        "mismatch_streak": streak,
        "threshold": th,
        "rebuild_scheduled": rebuild_scheduled,
    }


def schedule_background_meta_rebuild(*, reason: str = "") -> bool:
    """daemon 스레드에서 rebuild_meta_state(force=True) 실행."""

    def _run() -> None:
        global _HEAL_RUNNING
        try:
            logger.warning(
                "regime_self_heal: background rebuild_meta_state(force=True) — %s",
                reason or "regime_mismatch",
            )
            from meta_state_store import rebuild_meta_state

            out = rebuild_meta_state(force=True, refresh_regime=True)
            logger.info("regime_self_heal: rebuild done: %s", out)
            try:
                from factory_meta_alerts import send_meta_critical_alert

                send_meta_critical_alert(
                    "Regime self-heal rebuild completed",
                    str(out.get("config_regime_sync") or out),
                    prefix="META_HEAL",
                )
            except Exception:
                pass
        except Exception as e:
            logger.exception("regime_self_heal: rebuild failed: %s", e)
            try:
                from factory_meta_alerts import send_meta_critical_alert

                send_meta_critical_alert(
                    "Regime self-heal rebuild FAILED",
                    str(e),
                    prefix="META_HEAL",
                )
            except Exception:
                pass
        finally:
            global _HEAL_RUNNING
            with _HEAL_LOCK:
                _HEAL_RUNNING = False
            st = _load_heal_state()
            st["mismatch_streak"] = 0
            st["last_rebuild_at_utc"] = datetime.now(timezone.utc).isoformat()
            st["last_rebuild_reason"] = reason
            _save_heal_state(st)

    threading.Thread(target=_run, name="regime-self-heal", daemon=True).start()
    return True
