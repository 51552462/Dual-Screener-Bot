import json
import os

from bitget.config_hub import load_config
from bitget.infra.clock import utc_datetime_str_tz
from bitget.infra.logging_setup import get_logger, setup_logging
from bitget.oms import create_trade_exchange, generate_client_oid, oms_place_market_order
from bitget.symbol_utils import normalize_market_symbol
from bitget.trading.execution_safety import ExecutionGateOutcome, run_pre_execution_gates
from bitget.trading.leverage_manager import prepare_futures_order_params, resolve_leverage, resolve_margin_mode
from bitget.trading.position_manager import ccxt_order_side, normalize_position_side

setup_logging()
logger = get_logger("bitget.executor")


def _load_config():
    return load_config()


def _normalize_order_from_markets(ex, market_symbol, qty, market_type, ref_price=None):
    """거래소 market 메타·현재가로 수량/명목 규격 맞춤."""
    try:
        m = ex.market(market_symbol)
    except Exception as e:
        return None, ref_price, {"error": "market_lookup", "detail": str(e)}

    limits = m.get("limits") or {}
    lam = limits.get("amount") or {}
    lcm = limits.get("cost") or {}
    try:
        min_amt = float(lam.get("min") or 0.0)
    except (TypeError, ValueError):
        min_amt = 0.0
    max_amt = lam.get("max")
    try:
        max_amt_f = float(max_amt) if max_amt is not None else None
    except (TypeError, ValueError):
        max_amt_f = None
    try:
        min_cost = float(lcm.get("min") or 0.0)
    except (TypeError, ValueError):
        min_cost = 0.0

    px = ref_price
    if px is None or float(px or 0) <= 0:
        from bitget.trading.market_price_snapshot import fetch_ref_price

        px = fetch_ref_price(
            ex, market_symbol, market_type=market_type, prefer_ws=True, explicit=None
        )
    px = float(px or 0.0)

    try:
        q = float(qty)
    except (TypeError, ValueError):
        return None, px, {"error": "qty_nan"}

    try:
        q_str = ex.amount_to_precision(market_symbol, q)
        q_adj = float(q_str)
    except Exception:
        q_adj = q

    if min_amt > 0 and q_adj < min_amt:
        try:
            q_str = ex.amount_to_precision(market_symbol, min_amt)
            q_adj = float(q_str)
        except Exception:
            q_adj = min_amt

    if max_amt_f is not None and max_amt_f > 0 and q_adj > max_amt_f:
        try:
            q_str = ex.amount_to_precision(market_symbol, max_amt_f)
            q_adj = float(q_str)
        except Exception:
            q_adj = max_amt_f

    notion = q_adj * px
    steps = 0
    max_steps = 6
    while min_cost > 0 and notion < min_cost and px > 0 and steps < max_steps:
        steps += 1
        needed = min_cost / px
        bump = needed * (1.0 + 0.008 * steps)
        candidate = max(q_adj, bump, needed)
        if max_amt_f is not None and max_amt_f > 0:
            candidate = min(candidate, max_amt_f)
        try:
            q_str = ex.amount_to_precision(market_symbol, candidate)
            q_adj = float(q_str)
        except Exception:
            q_adj = candidate
        notion = q_adj * px

    diag = {
        "min_cost": min_cost,
        "min_amount": min_amt,
        "ref_px": px,
        "normalized_qty": q_adj,
        "notional": notion,
        "precision_steps": steps,
    }
    if q_adj <= 0:
        diag["error"] = "qty_zero"
        return None, px, diag
    if min_cost > 0 and notion + 1e-9 < min_cost:
        diag["error"] = "below_min_cost"
        return None, px, diag
    return q_adj, px, diag


def _fetch_total_usdt(ex, *, market_type="futures", prefer_ws=True):
    """USDT equity — futures may use private account WS; post-fill must prefer_ws=False."""
    from bitget.trading.account_snapshot import fetch_usdt_balance

    return float(
        fetch_usdt_balance(ex, market_type=market_type, prefer_ws=prefer_ws)
    )

def execute_real_order(
    symbol,
    side,
    amount,
    leverage=3.0,
    market_type="futures",
    strategy_key=None,
    margin_mode=None,
):
    """
    Bitget live order — safety gates → normalization → leverage (futures) → OMS.

    Gate order (execution_safety SSOT):
      1. ENABLE_REAL_EXECUTION
      2. REAL_EXECUTION_DRY_RUN
      3. MetaGovernor KILL_SWITCH
      4. GLOBAL_CIRCUIT_BREAKER
      5. OMS orphan active (exchange-only — no flatten)
      6. Portfolio NAV drawdown (reduce/block/halt)
      7. Portfolio gross notional cap (no flatten)
      8. Tail-risk reserve (underfund size / empty+DD block)
      9. Doomsday DEFCON (≤ block — new LONG only; size dampen)
     10. BTC-proxy concentration (high-β same-side — no flatten)
     11. Bad-tick / flash-crash price sanity
     12. Pre-trade slippage gate
     13. Leverage / margin manager (futures; MAX_LEVERAGE)
     14. OMS market order (oms_core defense-in-depth)
    """
    cfg = _load_config()

    side_u = normalize_position_side(side)
    order_side = ccxt_order_side(side_u, opening=True)
    qty = float(amount or 0.0)
    lev = resolve_leverage(cfg, strategy_key=strategy_key, leverage_explicit=leverage)
    resolved_mm = resolve_margin_mode(cfg, strategy_key=strategy_key, margin_mode_explicit=margin_mode)
    market_symbol = normalize_market_symbol(str(symbol).replace("_", "/"), market_type)
    meta_out = {"margin_mode_requested": resolved_mm, "leverage": lev}

    if qty <= 0:
        return {
            "ok": False,
            "status": "invalid_amount",
            "message": "amount must be > 0",
            "timestamp": utc_datetime_str_tz(),
            "margin_mode_requested": resolved_mm,
            "client_order_id": "",
        }

    gate = run_pre_execution_gates(
        cfg,
        market_symbol=market_symbol,
        market_type=market_type,
        position_side=side_u,
    )
    meta_out.update(gate.meta)

    if gate.outcome == ExecutionGateOutcome.EXECUTION_DISABLED:
        return {
            "ok": False,
            "status": gate.outcome.value,
            "message": gate.message,
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
            "margin_mode_requested": resolved_mm,
            "client_order_id": "",
            **meta_out,
        }

    if gate.is_dry_run:
        dr_prefix = str(cfg.get("EXEC_CLIENT_OID_PREFIX") or "bg")[:12]
        return {
            "ok": True,
            "status": "dry_run",
            "message": gate.message,
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
            "margin_mode_requested": resolved_mm,
            "client_order_id": generate_client_oid(dr_prefix),
            **meta_out,
        }

    if gate.outcome in (
        ExecutionGateOutcome.META_BLOCKED,
        ExecutionGateOutcome.CIRCUIT_BLOCKED,
        ExecutionGateOutcome.ORPHAN_BLOCKED,
        ExecutionGateOutcome.NAV_BLOCKED,
        ExecutionGateOutcome.GROSS_BLOCKED,
        ExecutionGateOutcome.TAIL_RISK_BLOCKED,
        ExecutionGateOutcome.DOOMSDAY_BLOCKED,
        ExecutionGateOutcome.CONCENTRATION_BLOCKED,
        ExecutionGateOutcome.PRICE_SANITY_BLOCKED,
        ExecutionGateOutcome.SLIPPAGE_BLOCKED,
    ):
        return {
            "ok": False,
            "status": gate.outcome.value,
            "message": gate.message,
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
            "client_order_id": "",
            **meta_out,
        }

    # NAV / doomsday / tail-risk reduce — shrink size before normalize (never invent size up)
    try:
        nav_mult = float(meta_out.get("nav_size_mult") or 1.0)
    except (TypeError, ValueError):
        nav_mult = 1.0
    try:
        doom_mult = float(meta_out.get("doomsday_size_mult") or 1.0)
    except (TypeError, ValueError):
        doom_mult = 1.0
    try:
        tail_mult = float(meta_out.get("tail_risk_size_mult") or 1.0)
    except (TypeError, ValueError):
        tail_mult = 1.0
    size_mult = 1.0
    if 0.0 < nav_mult < 1.0:
        size_mult *= nav_mult
    if 0.0 < doom_mult < 1.0:
        size_mult *= doom_mult
    if 0.0 < tail_mult < 1.0:
        size_mult *= tail_mult
    if 0.0 < size_mult < 1.0:
        qty = qty * size_mult
        meta_out["amount_after_risk_reduce"] = qty
        if 0.0 < nav_mult < 1.0:
            meta_out["amount_after_nav_reduce"] = qty
        if 0.0 < doom_mult < 1.0:
            meta_out["amount_after_doomsday_reduce"] = qty
        if 0.0 < tail_mult < 1.0:
            meta_out["amount_after_tail_risk_reduce"] = qty

    try:
        ex = create_trade_exchange(market_type)
        bal_before = _fetch_total_usdt(ex, market_type=market_type, prefer_ws=True)

        qty_norm, px_ref, diag = _normalize_order_from_markets(
            ex, market_symbol, qty, market_type, ref_price=None
        )
        meta_out.update(diag)
        if qty_norm is None:
            return {
                "ok": False,
                "status": "normalize_failed",
                "message": str(diag.get("error") or "order_below_exchange_limits"),
                "symbol": market_symbol,
                "side": order_side,
                "amount": qty,
                "leverage": lev,
                "client_order_id": "",
                **meta_out,
            }
        qty = qty_norm

        params = {}
        if market_type == "futures":
            params, fut_meta = prepare_futures_order_params(
                ex,
                market_symbol,
                cfg,
                strategy_key=strategy_key,
                leverage=lev,
                margin_mode=resolved_mm,
            )
            meta_out.update(fut_meta)
            if not fut_meta.get("margin_mode_verified_ok", True):
                return {
                    "ok": False,
                    "status": "margin_mode_mismatch",
                    "message": (
                        f"desired={fut_meta.get('margin_mode_requested')}, "
                        f"exchange={fut_meta.get('margin_mode_at_exchange')}"
                    ),
                    "symbol": market_symbol,
                    "side": order_side,
                    "amount": qty,
                    "leverage": lev,
                    "client_order_id": "",
                    **meta_out,
                }

        prefix = str(cfg.get("EXEC_CLIENT_OID_PREFIX") or "bg")[:12]
        coid = generate_client_oid(prefix)
        om = oms_place_market_order(
            ex,
            market_symbol,
            order_side,
            qty,
            params_base=params,
            client_oid=coid,
            max_attempts=int(cfg.get("OMS_ORDER_MAX_ATTEMPTS", 3)),
            market_type=market_type,
            position_side=side_u,
        )
        meta_out.update(
            {
                "client_order_id": om.get("client_order_id", coid),
                "oms_status": om.get("status"),
                "filled": om.get("filled"),
                "remaining": om.get("remaining"),
            }
        )

        if not om.get("ok", False):
            return {
                "ok": False,
                "status": om.get("status") or "oms_reject",
                "message": om.get("message") or "OMS order failed",
                "symbol": market_symbol,
                "side": order_side,
                "amount": qty,
                "leverage": lev,
                **meta_out,
                "raw": om.get("raw"),
            }

        # Post-fill: force REST — account WS may still hold pre-trade equity
        bal_after = _fetch_total_usdt(ex, market_type=market_type, prefer_ws=False)
        pnl = float(bal_after - bal_before)
        ret_pct = float((pnl / bal_before) * 100.0) if bal_before > 0 else 0.0
        oid_out = str((om.get("raw") or {}).get("id") or om.get("order_id") or "")
        ok_fill = om.get("status") not in ("rejected", "canceled", "cancelled")

        return {
            "ok": ok_fill,
            "status": om.get("status") or ("filled_submitted" if ok_fill else "partial_or_unknown"),
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
            "order_id": oid_out,
            "client_order_id": str(om.get("client_order_id") or coid),
            "balance_before": bal_before,
            "balance_after": bal_after,
            "realized_pnl_usdt": pnl,
            "realized_ret_pct": ret_pct,
            **meta_out,
            "raw": om.get("raw"),
        }

    except Exception as e:
        logger.warning("execute_real_order failed: %s", e)
        return {
            "ok": False,
            "status": "error",
            "message": str(e),
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
            "client_order_id": "",
            **meta_out,
        }
