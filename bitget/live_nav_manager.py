"""
Bitget Live NAV Manager — SPOT/FUTURES USDT 복리 NAV SSOT.

주식 `live_nav_manager.py` 패턴을 코인에 맞게 이식한다.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from typing import Any, Dict, Optional

from bitget.infra.clock import utc_datetime_str
from bitget.infra.data_paths import bitget_data_dir
from bitget.infra.market_keys import normalize_market_type

TREASURY_STATE_FILENAME = "bitget_treasury_state.json"
DEFAULT_EFFECTIVE_KELLY = 0.02
MIN_EFFECTIVE_KELLY = 0.001
MAX_EFFECTIVE_KELLY = 0.50
_LOCK = threading.RLock()


def normalize_market(market_type: str) -> str:
    return normalize_market_type(market_type)


def base_capital_for(market_type: str, sys_config: Optional[Dict[str, Any]] = None) -> float:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        total = float(cfg.get("ACCOUNT_SIZE_USDT", 100_000.0) or 100_000.0)
    except (TypeError, ValueError):
        total = 100_000.0
    if total <= 0:
        total = 100_000.0
    return total / 2.0


def treasury_state_path() -> str:
    return os.path.join(bitget_data_dir(), TREASURY_STATE_FILENAME)


def _empty_market_state(market_type: str, sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = base_capital_for(market_type, sys_config)
    return {
        "currency": "USDT",
        "base_capital": base,
        "nav": base,
        "hwm": base,
        "mdd_pct": 0.0,
        "n_closed": 0,
        "last_exit_date": None,
        "updated_at": None,
    }


def _default_state(sys_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "schema": "bitget_treasury_state.v1",
        "updated_at": None,
        "spot": _empty_market_state("spot", sys_config),
        "futures": _empty_market_state("futures", sys_config),
    }


def load_treasury_state() -> Dict[str, Any]:
    path = treasury_state_path()
    if not os.path.isfile(path):
        return _default_state()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_state()
        for mkt in ("spot", "futures"):
            if not isinstance(data.get(mkt), dict):
                data[mkt] = _empty_market_state(mkt)
            else:
                base = _empty_market_state(mkt)
                base.update({k: v for k, v in data[mkt].items() if v is not None})
                data[mkt] = base
        return data
    except (OSError, json.JSONDecodeError, ValueError):
        return _default_state()


def save_treasury_state(state: Dict[str, Any]) -> bool:
    path = treasury_state_path()
    state = dict(state)
    state["updated_at"] = utc_datetime_str()
    try:
        with _LOCK:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix=".treasury_bg_", suffix=".json", dir=os.path.dirname(path))
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(state, f, ensure_ascii=False, indent=2)
                os.replace(tmp, path)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
        return True
    except OSError:
        return False


def get_market_state(market_type: str) -> Dict[str, Any]:
    mkt = normalize_market(market_type)
    return load_treasury_state().get(mkt, _empty_market_state(mkt))


def live_nav(market_type: str) -> float:
    st = get_market_state(market_type)
    base = base_capital_for(market_type)
    try:
        v = float(st.get("nav", base))
        return v if v > 0 else base
    except (TypeError, ValueError):
        return base


def portfolio_nav_snapshot() -> Dict[str, Any]:
    """Combined SPOT+FUTURES treasury NAV / HWM / current drawdown %.

    Portfolio MDD uses combined peaks: (hwm_s+hwm_f - nav_s-nav_f) / (hwm_s+hwm_f).
    Soft-fails to zeros — never raises into the live order path.
    """
    try:
        state = load_treasury_state()
        spot = state.get("spot") if isinstance(state.get("spot"), dict) else {}
        fut = state.get("futures") if isinstance(state.get("futures"), dict) else {}

        def _f(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
            try:
                return float(row.get(key, default) or default)
            except (TypeError, ValueError):
                return float(default)

        spot_nav = _f(spot, "nav")
        fut_nav = _f(fut, "nav")
        spot_hwm = max(_f(spot, "hwm", spot_nav), spot_nav)
        fut_hwm = max(_f(fut, "hwm", fut_nav), fut_nav)
        nav = spot_nav + fut_nav
        hwm = spot_hwm + fut_hwm
        mdd_pct = 0.0
        if hwm > 0:
            mdd_pct = max(0.0, (hwm - nav) / hwm * 100.0)
        return {
            "nav": nav,
            "hwm": hwm,
            "mdd_pct": round(mdd_pct, 4),
            "spot_nav": spot_nav,
            "futures_nav": fut_nav,
            "spot_hwm": spot_hwm,
            "futures_hwm": fut_hwm,
        }
    except Exception:
        return {
            "nav": 0.0,
            "hwm": 0.0,
            "mdd_pct": 0.0,
            "spot_nav": 0.0,
            "futures_nav": 0.0,
            "spot_hwm": 0.0,
            "futures_hwm": 0.0,
            "error": "portfolio_nav_snapshot_failed",
        }


def resolve_effective_kelly(
    market_type: str,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> float:
    if isinstance(meta, dict):
        try:
            from bitget.governance.meta_consumer import resolve_trading_kelly_base

            return float(resolve_trading_kelly_base(sys_config or {}, meta))
        except Exception:
            pass
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        base = float(cfg.get("DYNAMIC_KELLY_RISK", DEFAULT_EFFECTIVE_KELLY) or DEFAULT_EFFECTIVE_KELLY)
    except (TypeError, ValueError):
        base = DEFAULT_EFFECTIVE_KELLY
    g = 1.0
    if isinstance(meta, dict):
        try:
            g = float(meta.get("META_GLOBAL_KELLY_MULT", 1.0) or 1.0)
        except (TypeError, ValueError):
            g = 1.0
    eff = base * g
    if eff <= 0:
        eff = DEFAULT_EFFECTIVE_KELLY
    return float(min(MAX_EFFECTIVE_KELLY, max(MIN_EFFECTIVE_KELLY, eff)))


def live_notional(
    market_type: str,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> float:
    return live_nav(market_type) * resolve_effective_kelly(market_type, sys_config, meta)


def apply_realized_pnl(
    market_type: str, net_pnl: float, *, exit_date: Optional[str] = None
) -> Dict[str, Any]:
    mkt = normalize_market(market_type)
    with _LOCK:
        state = load_treasury_state()
        st = dict(state.get(mkt, _empty_market_state(mkt)))
        base = float(st.get("base_capital", base_capital_for(mkt)) or base_capital_for(mkt))
        nav = float(st.get("nav", base) or base)
        hwm = float(st.get("hwm", nav) or nav)
        mdd_pct = float(st.get("mdd_pct", 0.0) or 0.0)
        n_closed = int(st.get("n_closed", 0) or 0)
        nav = max(0.0, nav + float(net_pnl))
        hwm = max(hwm, nav)
        if hwm > 0:
            mdd_pct = max(mdd_pct, (hwm - nav) / hwm * 100.0)
        st.update(
            {
                "nav": nav,
                "hwm": hwm,
                "mdd_pct": mdd_pct,
                "n_closed": n_closed + 1,
                "last_exit_date": exit_date or st.get("last_exit_date"),
            }
        )
        state[mkt] = st
        save_treasury_state(state)
        return st


def record_closure(
    market_type: str,
    *,
    final_ret_pct: float,
    kelly_pct: Optional[float] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    exit_date: Optional[str] = None,
    net_pnl_usdt: Optional[float] = None,
) -> Dict[str, Any]:
    """청산 1건 NAV 반영. net_pnl_usdt 가 있으면 절대액 우선, 없으면 NAV×Kelly×ret."""
    if net_pnl_usdt is not None:
        try:
            return apply_realized_pnl(market_type, float(net_pnl_usdt), exit_date=exit_date)
        except (TypeError, ValueError):
            pass
    try:
        ret = float(final_ret_pct)
    except (TypeError, ValueError):
        return get_market_state(market_type)
    f = None
    if kelly_pct is not None:
        try:
            f = float(kelly_pct)
        except (TypeError, ValueError):
            f = None
    if f is None or f <= 0:
        f = resolve_effective_kelly(market_type, sys_config, meta)
    f = float(min(MAX_EFFECTIVE_KELLY, max(MIN_EFFECTIVE_KELLY, f)))
    nav = live_nav(market_type)
    return apply_realized_pnl(market_type, nav * f * (ret / 100.0), exit_date=exit_date)


def fmt_usdt(value: float, *, signed: bool = False) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        v = 0.0
    body = f"{v:+,.2f}" if signed else f"{v:,.2f}"
    return f"{body} USDT"
