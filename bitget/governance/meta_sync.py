"""
Bitget MetaGovernor 상태 SSOT — config_kv + JSON 미러 (주식 meta_state_store 패턴).

- Bitget `bitget_system_config.sqlite` `META_GOVERNOR_STATE`
- `bitget_meta_governor_state.json` (레거시 미러)
- REGIME_ANALYSIS / CURRENT_REGIME_KEY / DYNAMIC_KELLY_RISK 동기화

scan·daily_audit prelude 직전 `rebuild_bitget_meta_state()` 호출.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from bitget.infra.data_paths import market_data_db_path, meta_governor_state_path, system_config_json_path

logger = logging.getLogger(__name__)

META_STATE_KV_KEY = "META_GOVERNOR_STATE"

_VALID_REGIME_KEYS = frozenset(
    {"BULL", "BEAR", "SIDEWAYS", "HIGH_VOL", "CHOP", "WHIPSAW", "UNKNOWN"}
)

# WHIPSAW → SIDEWAYS for ACTION_BY_REGIME lookup (root meta_governor read-only)
_REGIME_ACTION_KEY = {
    "WHIPSAW": "SIDEWAYS",
    "CHOP": "SIDEWAYS",
}


def normalize_regime_key(value: Any) -> str:
    u = str(value or "").strip().upper()
    if u in ("CHOP", "WHIPSAW"):
        return u
    if u in _VALID_REGIME_KEYS:
        return u
    return "UNKNOWN"


def max_meta_age_hours() -> float:
    raw = (os.environ.get("BITGET_META_MAX_AGE_HOURS") or "24").strip()
    try:
        return max(0.25, float(raw))
    except ValueError:
        return 24.0


def meta_governor_run_age_hours(state: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(state, dict):
        return None
    raw = state.get("META_GOVERNOR_LAST_RUN_AT")
    if not raw:
        return None
    try:
        s = str(raw).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - dt.astimezone(timezone.utc)).total_seconds() / 3600.0
    except (TypeError, ValueError):
        return None


def is_bitget_meta_degraded(state: Optional[Dict[str, Any]]) -> bool:
    """UNKNOWN·NEVER·타임스탬프 초과 — 리포트 전 동기 복구 트리거."""
    if not isinstance(state, dict) or not state:
        return True
    status = str(state.get("META_GOVERNOR_LAST_RUN_STATUS") or "").upper()
    if status in ("NEVER", "", "ERROR", "FAILED"):
        return True
    if not state.get("META_GOVERNOR_LAST_RUN_AT"):
        return True
    age_h = meta_governor_run_age_hours(state)
    if age_h is None:
        return True
    if age_h > max_meta_age_hours():
        return True
    rk = str(state.get("META_REGIME_KEY") or "").strip().upper()
    if rk in ("", "UNKNOWN"):
        return True
    ra = state.get("META_REGIME_ACTION")
    if not isinstance(ra, dict):
        return True
    return False


def _default_meta_state() -> Dict[str, Any]:
    from meta_governor import default_meta_state

    return default_meta_state()


def _action_for_regime(regime_key: str) -> Dict[str, Any]:
    from meta_governor import ACTION_BY_REGIME, default_meta_state

    rk = _REGIME_ACTION_KEY.get(regime_key, regime_key)
    tpl = ACTION_BY_REGIME.get(rk) or ACTION_BY_REGIME.get("UNKNOWN") or {}
    base = default_meta_state().get("META_REGIME_ACTION") or {}
    if isinstance(base, dict):
        out = {**base, **dict(tpl)}
    else:
        out = dict(tpl)
    return out


def _load_json_mirror(path: str) -> Dict[str, Any]:
    if not path or not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_json_mirror(state: Dict[str, Any], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        dir=os.path.dirname(path) or ".", prefix=".bitget_meta_", suffix=".json"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _load_from_config_kv() -> Optional[Dict[str, Any]]:
    try:
        from bitget.infra import config_manager

        raw = config_manager.get_config_value(META_STATE_KV_KEY)
        if isinstance(raw, dict) and raw:
            return raw
    except Exception as e:
        logger.debug("meta_sync: config_kv load skip: %s", e)
    return None


def _save_to_config_kv(state: Dict[str, Any]) -> None:
    from bitget.infra import config_manager

    config_manager.set_config_value(META_STATE_KV_KEY, state)


def load_bitget_meta_unified(path: Optional[str] = None) -> Dict[str, Any]:
    """1) config_kv  2) JSON 미러."""
    for candidate in (_load_from_config_kv(),):
        if candidate is not None and not is_bitget_meta_degraded(candidate):
            out = _default_meta_state()
            out.update(candidate)
            return out

    p = path or meta_governor_state_path()
    file_state = _load_json_mirror(p)
    out = _default_meta_state()
    out.update(file_state)
    if not is_bitget_meta_degraded(out):
        try:
            _save_to_config_kv(out)
        except Exception as e:
            logger.warning("meta_sync: config_kv backfill failed: %s", e)
    return out


def save_bitget_meta_unified(state: Dict[str, Any], path: Optional[str] = None) -> None:
    p = path or meta_governor_state_path()
    _save_to_config_kv(state)
    _save_json_mirror(state, p)


def resolve_config_regime_key(sys_config: Optional[Dict[str, Any]] = None) -> str:
    cfg: Dict[str, Any]
    if isinstance(sys_config, dict):
        cfg = sys_config
    else:
        try:
            from bitget.infra import config_manager

            cfg = config_manager.load_system_config() or {}
        except Exception:
            cfg = {}
    rk_cur = normalize_regime_key(cfg.get("CURRENT_REGIME_KEY"))
    ra = cfg.get("REGIME_ANALYSIS")
    rk_ra = "UNKNOWN"
    if isinstance(ra, dict):
        rk_ra = normalize_regime_key(ra.get("regime_key"))
    if rk_ra not in ("", "UNKNOWN"):
        return rk_ra
    if rk_cur not in ("", "UNKNOWN"):
        return rk_cur
    return "UNKNOWN"


def is_config_regime_misaligned(
    meta: Dict[str, Any],
    sys_config: Optional[Dict[str, Any]] = None,
) -> bool:
    rk_meta = normalize_regime_key(meta.get("META_REGIME_KEY"))
    if rk_meta in ("", "UNKNOWN"):
        return False
    if isinstance(sys_config, dict):
        rk_ra = normalize_regime_key(
            (sys_config.get("REGIME_ANALYSIS") or {}).get("regime_key")
            if isinstance(sys_config.get("REGIME_ANALYSIS"), dict)
            else None
        )
        rk_cur = normalize_regime_key(sys_config.get("CURRENT_REGIME_KEY"))
    else:
        try:
            from bitget.infra import config_manager

            ra = config_manager.get_config_value("REGIME_ANALYSIS")
            rk_ra = (
                normalize_regime_key(ra.get("regime_key"))
                if isinstance(ra, dict)
                else "UNKNOWN"
            )
            rk_cur = normalize_regime_key(config_manager.get_config_value("CURRENT_REGIME_KEY"))
        except Exception:
            return True
    return (
        rk_ra in ("", "UNKNOWN")
        or rk_cur in ("", "UNKNOWN")
        or rk_ra != rk_meta
        or rk_cur != rk_meta
    )


def sync_config_regime_from_meta(
    meta: Dict[str, Any],
    *,
    force: bool = False,
) -> Dict[str, Any]:
    """MetaGovernor 확정 국면 → config_kv REGIME_ANALYSIS + CURRENT_REGIME_KEY."""
    rk_meta = normalize_regime_key(meta.get("META_REGIME_KEY"))
    if rk_meta in ("", "UNKNOWN") and not force:
        return {"synced": False, "reason": "meta_regime_unknown"}

    from bitget.infra import config_manager

    ra = config_manager.get_config_value("REGIME_ANALYSIS")
    ra_dict = dict(ra) if isinstance(ra, dict) else {}
    rk_ra = normalize_regime_key(ra_dict.get("regime_key"))
    rk_cur = normalize_regime_key(config_manager.get_config_value("CURRENT_REGIME_KEY"))

    if not force and rk_ra == rk_meta and rk_cur == rk_meta:
        return {"synced": False, "reason": "already_aligned", "regime": rk_meta}

    ra_out = {**ra_dict, "regime_key": rk_meta, "source": "bitget_meta_sync"}
    config_manager.set_config_value("REGIME_ANALYSIS", ra_out)
    config_manager.set_config_value("CURRENT_REGIME_KEY", rk_meta)

    action = meta.get("META_REGIME_ACTION")
    if isinstance(action, dict):
        kelly_cap = action.get("kelly_cap")
        if kelly_cap is not None:
            try:
                config_manager.set_config_value("DYNAMIC_KELLY_RISK", float(kelly_cap))
            except (TypeError, ValueError):
                pass

    logger.info(
        "bitget_meta_sync: synced regime META→config_kv %s (was ra=%s cur=%s)",
        rk_meta,
        rk_ra,
        rk_cur,
    )
    return {
        "synced": True,
        "regime": rk_meta,
        "previous_REGIME_ANALYSIS": rk_ra,
        "previous_CURRENT_REGIME_KEY": rk_cur,
    }


def ensure_config_regime_aligned(
    meta: Optional[Dict[str, Any]] = None,
    *,
    force: bool = False,
) -> Dict[str, Any]:
    if meta is None:
        meta = load_bitget_meta_unified()
    if not isinstance(meta, dict):
        return {"synced": False, "reason": "no_meta"}
    if not force and not is_config_regime_misaligned(meta):
        rk = normalize_regime_key(meta.get("META_REGIME_KEY"))
        return {"synced": False, "reason": "already_aligned", "regime": rk}
    return sync_config_regime_from_meta(meta, force=force or is_config_regime_misaligned(meta))


def regime_analysis_stale_or_missing(sys_config: Optional[Dict[str, Any]] = None) -> bool:
    cfg = sys_config
    if cfg is None:
        try:
            from bitget.infra import config_manager

            cfg = config_manager.load_system_config() or {}
        except Exception:
            cfg = {}
    ra = cfg.get("REGIME_ANALYSIS")
    if not isinstance(ra, dict):
        return True
    rk = normalize_regime_key(ra.get("regime_key"))
    if rk in ("", "UNKNOWN"):
        return True
    updated = ra.get("updated_at") or ra.get("as_of")
    if not updated:
        return True
    return False


def _refresh_coin_regime() -> Dict[str, Any]:
    """BTC/ETH 기반 코인 국면 — auto_pilot.detect_coin_regime 위임."""
    from bitget.auto_pilot import detect_coin_regime
    from bitget.infra import config_manager

    cfg = config_manager.load_system_config() or {}
    cfg = detect_coin_regime(cfg)
    now = datetime.now(timezone.utc).isoformat()
    rk = normalize_regime_key(cfg.get("CURRENT_REGIME_KEY"))
    detail = cfg.get("CRYPTO_REGIME_DETAIL") if isinstance(cfg.get("CRYPTO_REGIME_DETAIL"), dict) else {}
    ra = {
        "regime_key": rk,
        "updated_at": now,
        "source": "bitget_detect_coin_regime",
        "btc_over_ema200": detail.get("btc_over_ema200"),
        "eth_btc_breadth": detail.get("eth_btc_breadth"),
        "atr_pct": detail.get("atr_pct"),
    }
    cfg["REGIME_ANALYSIS"] = ra
    config_manager.save_system_config(cfg)
    return {"regime_key": rk, "REGIME_ANALYSIS": ra}


def _write_bitget_config_snapshot_for_governor() -> Optional[str]:
    """MetaGovernor JSON reader용 bitget sqlite → ephemeral snapshot."""
    from bitget.infra import config_manager

    cfg = config_manager.load_system_config() or {}
    if not cfg:
        json_path = system_config_json_path()
        if os.path.isfile(json_path):
            return json_path
        return None

    fd, tmp = tempfile.mkstemp(prefix=".bitget_gov_cfg_", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        return tmp
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _run_bitget_meta_governor_cycle() -> str:
    """코인 forward DB + MetaGovernor.run_governor_cycle (주식 factory_artifact_guard 패리티)."""
    from meta_governor import GovernorRunContext, MetaGovernor

    db_path = market_data_db_path()
    meta_path = meta_governor_state_path()
    cfg_snapshot = _write_bitget_config_snapshot_for_governor()

    ctx = GovernorRunContext(
        forward_db_path=None,
        system_config_path=None,
        bitget_db_path=db_path if os.path.isfile(db_path) else None,
        bitget_system_config_path=cfg_snapshot,
    )
    try:
        gov = MetaGovernor(state_path=meta_path)
        state = gov.run_governor_cycle(ctx)
    finally:
        if cfg_snapshot and cfg_snapshot.startswith(tempfile.gettempdir()):
            try:
                os.unlink(cfg_snapshot)
            except OSError:
                pass

    save_bitget_meta_unified(state, meta_path)
    sync_config_regime_from_meta(state, force=True)
    try:
        from bitget.governance.meta_consumer import invalidate_meta_state_cache

        invalidate_meta_state_cache()
    except Exception:
        pass
    return str(state.get("META_GOVERNOR_LAST_RUN_STATUS") or "OK")


def rebuild_bitget_meta_state(
    *,
    force: bool = False,
    refresh_regime: bool = True,
) -> Dict[str, Any]:
    """
    1) 코인 REGIME_ANALYSIS 갱신 (detect_coin_regime)
    2) Bitget MetaGovernor cycle
    3) config regime align
    """
    result: Dict[str, Any] = {"regime": "skipped", "meta": "skipped"}

    need_regime = refresh_regime and (force or regime_analysis_stale_or_missing())
    if need_regime:
        try:
            reg = _refresh_coin_regime()
            result["regime"] = "refreshed"
            result["regime_key"] = reg.get("regime_key")
        except Exception as e:
            result["regime"] = "failed"
            result["regime_error"] = str(e)
            logger.exception("rebuild_bitget_meta_state: regime refresh failed: %s", e)
            try:
                from bitget.governance.meta_alerts import send_meta_critical_alert

                send_meta_critical_alert(
                    "Bitget meta regime refresh failed",
                    str(e),
                    prefix="META_BRAIN",
                )
            except Exception:
                pass

    try:
        cur = load_bitget_meta_unified()
        need_meta = force or is_bitget_meta_degraded(cur)
        if need_meta or result.get("regime") == "refreshed":
            status = _run_bitget_meta_governor_cycle()
            result["meta"] = "rebuilt"
            result["meta_status"] = status
        else:
            result["meta"] = "ok"
            result["meta_status"] = "fresh"
    except Exception as e:
        result["meta"] = "failed"
        result["meta_error"] = str(e)
        logger.exception("rebuild_bitget_meta_state: meta cycle failed: %s", e)
        try:
            from bitget.governance.meta_alerts import send_meta_critical_alert

            send_meta_critical_alert(
                "Bitget MetaGovernor heal failed (UNKNOWN/NEVER risk)",
                str(e),
                prefix="META_BRAIN",
            )
        except Exception:
            pass

    meta_after = load_bitget_meta_unified()
    if not is_bitget_meta_degraded(meta_after):
        result["config_regime_sync"] = ensure_config_regime_aligned(meta_after, force=True)
    elif is_config_regime_misaligned(meta_after):
        result["config_regime_sync"] = sync_config_regime_from_meta(meta_after, force=True)

    if is_bitget_meta_degraded(meta_after) and result.get("meta") != "failed":
        rk = str(meta_after.get("META_REGIME_KEY") or "UNKNOWN")
        st = str(meta_after.get("META_GOVERNOR_LAST_RUN_STATUS") or "NEVER")
        at = str(meta_after.get("META_GOVERNOR_LAST_RUN_AT") or "—")
        try:
            from bitget.governance.meta_alerts import send_meta_critical_alert

            send_meta_critical_alert(
                "Bitget meta state still degraded after rebuild",
                f"regime={rk} status={st} last_at={at}",
                prefix="META_BRAIN",
            )
        except Exception:
            pass
        raise RuntimeError(
            "rebuild_bitget_meta_state: meta still degraded after heal "
            f"(regime={rk} status={st} last_at={at})"
        )

    return result


def load_bitget_meta_resolved() -> Dict[str, Any]:
    """소비자용 — unified load + regime action 정합."""
    meta = load_bitget_meta_unified()
    rk = normalize_regime_key(meta.get("META_REGIME_KEY"))
    if rk not in ("", "UNKNOWN"):
        meta["META_REGIME_ACTION"] = _action_for_regime(rk)
    return meta
