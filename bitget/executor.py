import json
import os
from datetime import datetime, timezone

from bitget_logger import get_logger, setup_logging
from bitget.oms import create_trade_exchange, generate_client_oid, oms_place_market_order
from bitget.rate_limit_guard import backoff_sleep, throttle
from bitget.symbol_utils import normalize_market_symbol

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")
setup_logging()
logger = get_logger("bitget.executor")


def _load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _resolve_margin_mode(cfg, strategy_key=None, margin_mode_explicit=None):
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


def _current_margin_mode_from_exchange(ex, market_symbol):
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


def _enforce_margin_mode(ex, market_symbol, desired_mode):
    """거래소 API로 목표 마진모드 확인·설정 후 재검증."""
    want = desired_mode.strip().lower()
    if want not in ("cross", "isolated"):
        want = "cross"

    cur = _current_margin_mode_from_exchange(ex, market_symbol)
    need_set = cur is None or cur != want
    if need_set and hasattr(ex, "set_margin_mode"):
        try:
            throttle("bitget.set_margin_mode", 0.4)
            ex.set_margin_mode(want, market_symbol)
        except Exception as e:
            logger.warning("set_margin_mode(%s,%s): %s", want, market_symbol, e)

    cur2 = _current_margin_mode_from_exchange(ex, market_symbol)
    if cur2 == want:
        return True, want, cur2
    if cur2 is None:
        logger.warning(
            "margin mode verify skipped (no position/account read); requested=%s",
            want,
        )
        return True, want, None
    return False, want, cur2


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
        try:
            throttle("bitget.fetch_ticker", 0.2)
            t = ex.fetch_ticker(market_symbol)
            px = float(t.get("last") or t.get("close") or t.get("bid") or t.get("ask") or 0.0)
        except Exception:
            px = float(px or 0.0)
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


def _fetch_total_usdt(ex):
    try:
        throttle("bitget.fetch_balance", 0.22)
        bal = ex.fetch_balance()
        total = bal.get("total", {}) if isinstance(bal, dict) else {}
        return float(total.get("USDT", 0.0) or 0.0)
    except Exception:
        return 0.0


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
    비트겟 실전 주문 실행 — OMS(clientOid)·정규화·마진 검증 포함.
    """
    cfg = _load_config()
    enabled = bool(cfg.get("ENABLE_REAL_EXECUTION", False))
    dry_run = bool(cfg.get("REAL_EXECUTION_DRY_RUN", True))

    side_u = str(side).upper()
    order_side = "buy" if side_u in ("LONG", "BUY") else "sell"
    qty = float(amount or 0.0)
    lev = float(leverage or 1.0)
    resolved_mm = _resolve_margin_mode(cfg, strategy_key=str(strategy_key), margin_mode_explicit=margin_mode)
    market_symbol = normalize_market_symbol(str(symbol).replace("_", "/"), market_type)
    meta_out = {"margin_mode_requested": resolved_mm}

    if qty <= 0:
        return {
            "ok": False,
            "status": "invalid_amount",
            "message": "amount must be > 0",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "margin_mode_requested": resolved_mm,
            "client_order_id": "",
        }

    if not enabled:
        return {
            "ok": False,
            "status": "execution_disabled",
            "message": "ENABLE_REAL_EXECUTION is false",
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
            "margin_mode_requested": resolved_mm,
            "client_order_id": "",
        }

    if dry_run:
        dr_prefix = str(cfg.get("EXEC_CLIENT_OID_PREFIX") or "bg")[:12]
        return {
            "ok": True,
            "status": "dry_run",
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
            "margin_mode_requested": resolved_mm,
            "client_order_id": generate_client_oid(dr_prefix),
        }

    try:
        ex = create_trade_exchange(market_type)
        throttle("bitget.balance", 0.2)
        bal_before = _fetch_total_usdt(ex)

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

        if market_type == "futures":
            ok_mm, mm_req, mm_ver = _enforce_margin_mode(ex, market_symbol, resolved_mm)
            meta_out.update({"margin_mode_verified_ok": ok_mm, "margin_mode_at_exchange": mm_ver})
            if not ok_mm:
                return {
                    "ok": False,
                    "status": "margin_mode_mismatch",
                    "message": f"desired={mm_req}, exchange={mm_ver}",
                    "symbol": market_symbol,
                    "side": order_side,
                    "amount": qty,
                    "leverage": lev,
                    "client_order_id": "",
                    **meta_out,
                }

        try:
            throttle("bitget.set_leverage", 0.25)
            ex.set_leverage(lev, market_symbol)
        except Exception:
            pass

        params = {}
        if market_type == "futures":
            params["marginMode"] = resolved_mm

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

        bal_after = _fetch_total_usdt(ex)
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
        backoff_sleep(1)
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
