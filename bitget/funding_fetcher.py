"""
실시간 펀딩비·다음 결제 시각 조회 (Bitget 공개 API, 비인증).

Network resilience: ``bitget.infra.network_retry`` SSOT (Ch3).
"""
from __future__ import annotations

import math
from datetime import datetime, timezone

try:
    import ccxt
except Exception:
    ccxt = None

from bitget.infra.logging_setup import get_logger, setup_logging
from bitget.infra.network_retry import call_with_retry
from bitget.symbol_utils import normalize_market_symbol

setup_logging()
logger = get_logger("bitget.funding_fetcher")

_pub_ex = None


def _ex():
    global _pub_ex
    if ccxt is None:
        return None
    if _pub_ex is None:
        ex = ccxt.bitget({"enableRateLimit": True})
        ok = call_with_retry(
            lambda: (ex.load_markets() or True),
            op="funding.load_markets",
            throttle_key="bitget.pub.load_markets",
            throttle_interval_sec=0.5,
            default=False,
            swallow=True,
        )
        if not ok:
            logger.warning("funding public exchange load_markets failed after retries")
            return None
        _pub_ex = ex
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
    o = call_with_retry(
        lambda: ex.fetch_funding_rate(ccxt_sym),
        op="funding.fetch_funding_rate",
        throttle_key="bitget.pub.fetch_funding_rate",
        throttle_interval_sec=0.14,
        default=None,
        swallow=True,
    )
    if not isinstance(o, dict):
        return None

    info = o
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
