"""
Leverage + margin mode SSOT for Bitget futures execution.

Private WS positions may supply ``marginMode`` only when:
  - the *positions* channel is fresh, AND
  - a row for that market_symbol exists with parseable marginMode
Virgin / missing-row / stale → REST. Never invent mode from flat book.
After ``set_margin_mode``, verify via REST (WS may lag).
"""
from __future__ import annotations

from typing import Any, Optional

from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import PRIVATE_POS_INDEX_MAX_AGE_SEC
from bitget.infra.network_retry import call_with_retry
from bitget.trading.oms_source_stats import record_oms_source
from bitget.trading.position_manager import private_inst_id_to_ccxt_futures

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
    from bitget.trading.execution_safety import max_leverage_cap

    cap = max_leverage_cap(cfg)
    lev = float(default)
    if leverage_explicit is not None:
        try:
            lev = max(1.0, float(leverage_explicit))
        except (TypeError, ValueError):
            lev = float(default)
    elif strategy_key:
        by_strat = cfg.get("LEVERAGE_BY_STRATEGY") or {}
        raw = (by_strat or {}).get(strategy_key)
        if raw is None:
            by_eng = cfg.get("LEVERAGE_BY_ENGINE") or {}
            raw = (by_eng or {}).get(strategy_key)
        if raw is not None:
            try:
                lev = max(1.0, float(raw))
            except (TypeError, ValueError):
                pass
        else:
            try:
                lev = max(1.0, float(cfg.get("DEFAULT_REAL_EXECUTION_LEVERAGE", default)))
            except (TypeError, ValueError):
                lev = max(1.0, float(default))
    else:
        try:
            lev = max(1.0, float(cfg.get("DEFAULT_REAL_EXECUTION_LEVERAGE", default)))
        except (TypeError, ValueError):
            lev = max(1.0, float(default))
    return float(min(lev, cap))


def normalize_margin_mode_token(raw: Any) -> Optional[str]:
    """Map exchange / WS marginMode token → cross|isolated, else None."""
    if raw is None:
        return None
    ms = str(raw).strip().lower()
    if not ms:
        return None
    if "cross" in ms:
        return "cross"
    if "isol" in ms:
        return "isolated"
    if ms in ("cross", "isolated"):
        return ms
    return None


def try_private_ws_margin_mode(
    market_symbol: str,
    *,
    inst_type: str = "USDT-FUTURES",
    max_age_sec: float | None = None,
) -> Optional[str]:
    """Fresh positions-row marginMode for symbol, or None → caller must REST.

    Missing symbol row on a fresh channel is NOT flat-book invention —
    it means no WS evidence; return None.
    """
    want = str(market_symbol or "").strip()
    if not want:
        return None
    try:
        from bitget.data.stream_buffer import get_private_stream_buffer

        buf = get_private_stream_buffer()
        max_age = float(
            max_age_sec if max_age_sec is not None else PRIVATE_POS_INDEX_MAX_AGE_SEC
        )
        if float(buf.channel_age_sec("positions")) > max_age:
            return None

        found: list[str] = []
        for row in buf.list_positions(inst_type=inst_type):
            if not isinstance(row, dict):
                continue
            inst_id = str(row.get("instId") or "").strip()
            if not inst_id:
                continue
            if private_inst_id_to_ccxt_futures(inst_id) != want:
                continue
            # Only marginMode — never treat posMode (hedge/oneway) as margin
            mode = normalize_margin_mode_token(row.get("marginMode"))
            if mode:
                found.append(mode)

        if not found:
            return None
        uniq = set(found)
        if len(uniq) > 1:
            logger.warning(
                "private WS marginMode conflict symbol=%s modes=%s — defer REST",
                want,
                sorted(uniq),
            )
            return None
        return found[0]
    except Exception as e:
        logger.warning("private WS marginMode unavailable: %s", e)
        return None


def _rest_margin_mode_from_exchange(ex, market_symbol: str) -> Optional[str]:
    rows = call_with_retry(
        lambda: ex.fetch_positions([market_symbol]),
        op="oms.fetch_positions.margin",
        throttle_key="bitget.fetch_positions",
        throttle_interval_sec=0.28,
        default=None,
        swallow=True,
    )
    if rows is None:
        return None
    mode = None
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if row.get("symbol") != market_symbol:
            continue
        m = row.get("marginMode")
        if m is None and isinstance(row.get("info"), dict):
            m = row["info"].get("marginMode")
        mode = normalize_margin_mode_token(m)
        if mode:
            break
    return mode


def current_margin_mode_from_exchange(
    ex,
    market_symbol: str,
    *,
    prefer_ws: bool = True,
) -> Optional[str]:
    """Read exchange margin mode — WS when a fresh symbol row exists, else REST."""
    if prefer_ws:
        ws_mode = try_private_ws_margin_mode(market_symbol)
        if ws_mode is not None:
            record_oms_source("margin_mode", "private_ws")
            return ws_mode

    mode = _rest_margin_mode_from_exchange(ex, market_symbol)
    record_oms_source("margin_mode", "rest")
    return mode


def enforce_margin_mode(ex, market_symbol: str, desired_mode: str) -> tuple[bool, str, Optional[str]]:
    want = str(desired_mode or "cross").strip().lower()
    if want not in ("cross", "isolated"):
        want = "cross"

    cur = current_margin_mode_from_exchange(ex, market_symbol, prefer_ws=True)
    if cur == want:
        # Already aligned — skip mutation + second fetch
        return True, want, cur

    if hasattr(ex, "set_margin_mode"):
        ok = call_with_retry(
            lambda: (ex.set_margin_mode(want, market_symbol) or True),
            op="oms.set_margin_mode",
            throttle_key="bitget.set_margin_mode",
            throttle_interval_sec=0.4,
            default=False,
            swallow=True,
        )
        if not ok:
            logger.warning("set_margin_mode(%s,%s) failed after retries", want, market_symbol)

    # Post-mutation verify must not trust possibly-stale WS row
    cur2 = current_margin_mode_from_exchange(ex, market_symbol, prefer_ws=False)
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
    ok = call_with_retry(
        lambda: (ex.set_leverage(float(leverage), market_symbol) or True),
        op="oms.set_leverage",
        throttle_key="bitget.set_leverage",
        throttle_interval_sec=0.25,
        default=False,
        swallow=True,
    )
    if not ok:
        logger.warning("set_leverage(%s,%s) failed after retries", leverage, market_symbol)
    return bool(ok)


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
