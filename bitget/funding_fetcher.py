"""
실시간 펀딩비·다음 결제 시각 조회 (Bitget 공개 API, 비인증).
"""
import math
from datetime import datetime, timezone

try:
    import ccxt
except Exception:
    ccxt = None

from bitget.rate_limit_guard import backoff_sleep, throttle
from bitget.symbol_utils import normalize_market_symbol


_pub_ex = None


def _ex():
    global _pub_ex
    if ccxt is None:
        return None
    if _pub_ex is None:
        _pub_ex = ccxt.bitget({"enableRateLimit": True})
        _pub_ex.load_markets()
    return _pub_ex


def fetch_funding_snapshot(symbol: str):
    """
    symbol: BTC_USDT 또는 BTC/USDT 형
    반환: dict 또는 None — funding_rate(float), next_funding_ts(ms int|None),
          next_funding_iso(str), instrument(str)
    """
    ex = _ex()
    if ex is None:
        return None
    raw = str(symbol or "").replace("_", "/")
    ccxt_sym = normalize_market_symbol(raw, "futures")
    try:
        throttle("bitget.pub.fetch_funding_rate", 0.14)
        o = ex.fetch_funding_rate(ccxt_sym)
    except Exception:
        backoff_sleep(1)
        try:
            throttle("bitget.pub.fetch_funding_rate", 0.16)
            o = ex.fetch_funding_rate(ccxt_sym)
        except Exception:
            return None

    info = o if isinstance(o, dict) else {}
    rate = info.get("fundingRate")
    if rate is None and isinstance(info.get("info"), dict):
        rate = info["info"].get("fundingRate")
    try:
        rate_f = float(rate or 0.0)
        if math.isnan(rate_f) or math.isinf(rate_f):
            rate_f = 0.0
    except (TypeError, ValueError):
        rate_f = 0.0

    next_ts = info.get("nextFundingTimestamp") or info.get("nextFundingTime")
    if next_ts is None and isinstance(info.get("info"), dict):
        ni = info["info"]
        next_ts = ni.get("nextSettleTime") or ni.get("nextFundingTime") or ni.get("fundingTime")

    try:
        next_ts_int = int(float(next_ts)) if next_ts is not None else None
    except (TypeError, ValueError):
        next_ts_int = None

    next_iso = ""
    if next_ts_int:
        try:
            next_iso = datetime.fromtimestamp(next_ts_int / 1000.0, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
        except Exception:
            next_iso = str(next_ts_int)

    return {
        "funding_rate": rate_f,
        "next_funding_ts": next_ts_int,
        "next_funding_iso": next_iso,
        "instrument": ccxt_sym,
    }
