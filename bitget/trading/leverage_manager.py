"""
Leverage + margin mode SSOT for Bitget futures execution.
"""
from __future__ import annotations

from typing import Any, Optional

from bitget.infra.logging_setup import get_logger
from bitget.rate_limit_guard import throttle

logger = get_logger("bitget.trading.leverage_manager")


def resolve_margin_mode(cfg: dict, *, strategy_key=None, margin_mode_explicit=None) -> str:
    if margin_mode_explicit:
        mm = str(margin_mode_explicit).strip().lower()
    elif strategy_key:
        by_strat = cfg.get("MARGIN_MODE_BY_STRATEGY") or {}
        mm = str((by_strat or {}).get(strategy_key, "") or "").strip().lower()
        if not mm:
            eng = cfg.get("MARGIN_MODE_BY_ENGINE") or {}
            mm = str((eng or {}).get(strategy_key, "") or "").strip().lower()
    else:
        mm = ""
    if mm not in ("cross", "isolated"):
        mm = str(cfg.get("DEFAULT_REAL_EXECUTION_MARGIN_MODE", "cross") or "cross").strip().lower()
    if mm not in ("cross", "isolated"):
        mm = "cross"
    return mm


def resolve_leverage(
    cfg: dict,
    *,
    strategy_key=None,
    leverage_explicit=None,
    default: float = 3.0,
) -> float:
    if leverage_explicit is not None:
        try:
            return max(1.0, float(leverage_explicit))
        except (TypeError, ValueError):
            pass
    if strategy_key:
        by_strat = cfg.get("LEVERAGE_BY_STRATEGY") or {}
        raw = (by_strat or {}).get(strategy_key)
        if raw is None:
            by_eng = cfg.get("LEVERAGE_BY_ENGINE") or {}
            raw = (by_eng or {}).get(strategy_key)
        if raw is not None:
            try:
                return max(1.0, float(raw))
            except (TypeError, ValueError):
                pass
    try:
        return max(1.0, float(cfg.get("DEFAULT_REAL_EXECUTION_LEVERAGE", default)))
    except (TypeError, ValueError):
        return max(1.0, float(default))


def current_margin_mode_from_exchange(ex, market_symbol: str) -> Optional[str]:
    try:
        throttle("bitget.fetch_positions", 0.28)
        rows = ex.fetch_positions([market_symbol])
    except Exception:
        return None
    mode = None
    for row in rows or []:
        if row.get("symbol") != market_symbol:
            continue
        m = row.get("marginMode")
        if m is None and isinstance(row.get("info"), dict):
            m = row["info"].get("marginMode") or row["info"].get("posMode")
        if m:
            ms = str(m).lower()
            if "cross" in ms:
                mode = "cross"
            elif "isol" in ms:
                mode = "isolated"
            else:
                mode = ms
            break
    return mode


def enforce_margin_mode(ex, market_symbol: str, desired_mode: str) -> tuple[bool, str, Optional[str]]:
    want = str(desired_mode or "cross").strip().lower()
    if want not in ("cross", "isolated"):
        want = "cross"

    cur = current_margin_mode_from_exchange(ex, market_symbol)
    need_set = cur is None or cur != want
    if need_set and hasattr(ex, "set_margin_mode"):
        try:
            throttle("bitget.set_margin_mode", 0.4)
            ex.set_margin_mode(want, market_symbol)
        except Exception as e:
            logger.warning("set_margin_mode(%s,%s): %s", want, market_symbol, e)

    cur2 = current_margin_mode_from_exchange(ex, market_symbol)
    if cur2 == want:
        return True, want, cur2
    if cur2 is None:
        logger.warning(
            "margin mode verify skipped (no position/account read); requested=%s",
            want,
        )
        return True, want, None
    return False, want, cur2


def apply_futures_leverage(ex, market_symbol: str, leverage: float) -> bool:
    try:
        throttle("bitget.set_leverage", 0.25)
        ex.set_leverage(float(leverage), market_symbol)
        return True
    except Exception as e:
        logger.warning("set_leverage(%s,%s): %s", leverage, market_symbol, e)
        return False


def prepare_futures_order_params(
    ex,
    market_symbol: str,
    cfg: dict,
    *,
    strategy_key=None,
    leverage: float,
    margin_mode: Optional[str] = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Enforce margin mode + leverage on exchange; return (order_params, meta).
    """
    mm = resolve_margin_mode(cfg, strategy_key=strategy_key, margin_mode_explicit=margin_mode)
    meta: dict[str, Any] = {"margin_mode_requested": mm}
    ok_mm, mm_req, mm_ver = enforce_margin_mode(ex, market_symbol, mm)
    meta.update({"margin_mode_verified_ok": ok_mm, "margin_mode_at_exchange": mm_ver})
    if not ok_mm:
        return {}, meta

    lev = resolve_leverage(cfg, strategy_key=strategy_key, leverage_explicit=leverage)
    meta["leverage_applied"] = lev
    meta["leverage_set_ok"] = apply_futures_leverage(ex, market_symbol, lev)
    return {"marginMode": mm}, meta
