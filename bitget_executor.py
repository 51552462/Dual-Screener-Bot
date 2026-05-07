import json
import os
from datetime import datetime, timezone

try:
    import ccxt
except Exception:  # pragma: no cover
    ccxt = None


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")


def _load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _create_exchange():
    if ccxt is None:
        raise RuntimeError("ccxt not available")

    api_key = os.environ.get("BITGET_API_KEY", "")
    api_secret = os.environ.get("BITGET_API_SECRET", "")
    passphrase = os.environ.get("BITGET_API_PASSPHRASE", "")
    if not api_key or not api_secret or not passphrase:
        raise RuntimeError("missing BITGET_API_KEY / BITGET_API_SECRET / BITGET_API_PASSPHRASE")

    ex = ccxt.bitget(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "password": passphrase,
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
            },
        }
    )
    ex.load_markets()
    return ex


def _fetch_total_usdt(ex):
    try:
        bal = ex.fetch_balance()
        total = bal.get("total", {}) if isinstance(bal, dict) else {}
        usdt = float(total.get("USDT", 0.0) or 0.0)
        return usdt
    except Exception:
        return 0.0


def execute_real_order(symbol, side, amount, leverage=3.0):
    """
    비트겟 실전 주문 실행.
    - symbol 예: BTC_USDT -> BTC/USDT:USDT
    - side: LONG / SHORT / buy / sell
    - amount: 주문 수량(코인 수량 기준)
    - leverage: 선물 레버리지
    """
    cfg = _load_config()
    enabled = bool(cfg.get("ENABLE_REAL_EXECUTION", False))
    dry_run = bool(cfg.get("REAL_EXECUTION_DRY_RUN", True))

    side_u = str(side).upper()
    order_side = "buy" if side_u in ("LONG", "BUY") else "sell"
    qty = float(amount or 0.0)
    lev = float(leverage or 1.0)
    if qty <= 0:
        return {
            "ok": False,
            "status": "invalid_amount",
            "message": "amount must be > 0",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }

    market_symbol = str(symbol).replace("_", "/")
    if ":USDT" not in market_symbol and market_symbol.endswith("/USDT"):
        market_symbol = f"{market_symbol}:USDT"

    if not enabled:
        return {
            "ok": False,
            "status": "execution_disabled",
            "message": "ENABLE_REAL_EXECUTION is false",
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
        }

    if dry_run:
        return {
            "ok": True,
            "status": "dry_run",
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
        }

    try:
        ex = _create_exchange()
        bal_before = _fetch_total_usdt(ex)
        try:
            ex.set_leverage(lev, market_symbol)
        except Exception:
            pass
        params = {
            "marginMode": "cross",
        }
        order = ex.create_order(market_symbol, "market", order_side, qty, None, params)
        bal_after = _fetch_total_usdt(ex)
        pnl = float(bal_after - bal_before)
        ret_pct = float((pnl / bal_before) * 100.0) if bal_before > 0 else 0.0
        return {
            "ok": True,
            "status": "filled_submitted",
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
            "order_id": str(order.get("id", "")),
            "balance_before": bal_before,
            "balance_after": bal_after,
            "realized_pnl_usdt": pnl,
            "realized_ret_pct": ret_pct,
            "raw": order,
        }
    except Exception as e:
        return {
            "ok": False,
            "status": "error",
            "message": str(e),
            "symbol": market_symbol,
            "side": order_side,
            "amount": qty,
            "leverage": lev,
        }
