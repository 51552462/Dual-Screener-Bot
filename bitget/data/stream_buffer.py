"""
In-memory ticker / orderbook / private position cache (thread-safe).

WebSocket writers update; scanners and slippage_guard read for spread / last-price gates.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Optional


def _best_level(levels: Any) -> Optional[float]:
    if not levels or not isinstance(levels, list):
        return None
    first = levels[0]
    if isinstance(first, (list, tuple)) and first:
        try:
            return float(first[0])
        except (TypeError, ValueError):
            return None
    if isinstance(first, dict):
        for k in ("price", "px", "p"):
            if first.get(k) is not None:
                try:
                    return float(first[k])
                except (TypeError, ValueError):
                    pass
    return None


class StreamBuffer:
    """Public market data: ticker + orderbook (top-of-book)."""

    def __init__(self, max_symbols: int = 2000) -> None:
        self._lock = threading.RLock()
        self._tickers: dict[str, dict[str, Any]] = {}
        self._orderbooks: dict[str, dict[str, Any]] = {}
        self._max = max(100, int(max_symbols))
        now = time.monotonic()
        self._last_update_mono = now
        self._last_orderbook_mono = now

    def _evict_oldest(self, store: dict[str, dict[str, Any]]) -> None:
        if len(store) < self._max:
            return
        oldest = min(store.items(), key=lambda kv: kv[1].get("ts_mono", 0))
        store.pop(oldest[0], None)

    def update_ticker(
        self,
        inst_id: str,
        *,
        last: float,
        bid: Optional[float] = None,
        ask: Optional[float] = None,
        quote_volume_24h: Optional[float] = None,
        inst_type: str = "SPOT",
        raw: Optional[dict] = None,
    ) -> None:
        key = f"{inst_type}:{inst_id}".upper()
        row = {
            "inst_id": inst_id,
            "inst_type": inst_type,
            "last": float(last),
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            "quote_volume_24h": float(quote_volume_24h) if quote_volume_24h is not None else None,
            "ts_mono": time.monotonic(),
            "raw": raw or {},
        }
        with self._lock:
            if key not in self._tickers:
                self._evict_oldest(self._tickers)
            self._tickers[key] = row
            self._last_update_mono = time.monotonic()

    def update_orderbook(
        self,
        inst_id: str,
        *,
        bids: Any,
        asks: Any,
        inst_type: str = "SPOT",
        raw: Optional[dict] = None,
    ) -> None:
        key = f"{inst_type}:{inst_id}".upper()
        best_bid = _best_level(bids)
        best_ask = _best_level(asks)
        row = {
            "inst_id": inst_id,
            "inst_type": inst_type,
            "bids": bids,
            "asks": asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "ts_mono": time.monotonic(),
            "raw": raw or {},
        }
        with self._lock:
            if key not in self._orderbooks:
                self._evict_oldest(self._orderbooks)
            self._orderbooks[key] = row
            self._last_orderbook_mono = time.monotonic()
            self._last_update_mono = time.monotonic()

    def get_ticker(self, inst_id: str, inst_type: str = "SPOT") -> Optional[dict[str, Any]]:
        key = f"{inst_type}:{inst_id}".upper()
        with self._lock:
            row = self._tickers.get(key)
            return dict(row) if row else None

    def get_orderbook(self, inst_id: str, inst_type: str = "SPOT") -> Optional[dict[str, Any]]:
        key = f"{inst_type}:{inst_id}".upper()
        with self._lock:
            row = self._orderbooks.get(key)
            return dict(row) if row else None

    def get_last_price(self, inst_id: str, inst_type: str = "SPOT") -> Optional[float]:
        row = self.get_ticker(inst_id, inst_type)
        if not row:
            return None
        return row.get("last")

    @staticmethod
    def _bps_from_bid_ask(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return None
        mid = (float(bid) + float(ask)) / 2.0
        if mid <= 0:
            return None
        return ((float(ask) - float(bid)) / mid) * 10000.0

    def orderbook_spread_bps(self, inst_id: str, inst_type: str = "SPOT") -> Optional[float]:
        row = self.get_orderbook(inst_id, inst_type)
        if not row:
            return None
        return self._bps_from_bid_ask(row.get("best_bid"), row.get("best_ask"))

    def ticker_spread_bps(self, inst_id: str, inst_type: str = "SPOT") -> Optional[float]:
        row = self.get_ticker(inst_id, inst_type)
        if not row:
            return None
        return self._bps_from_bid_ask(row.get("bid"), row.get("ask"))

    def spread_bps(self, inst_id: str, inst_type: str = "SPOT") -> Optional[float]:
        """Prefer orderbook top-of-book; fallback to ticker bid/ask."""
        ob = self.orderbook_spread_bps(inst_id, inst_type)
        if ob is not None:
            return ob
        return self.ticker_spread_bps(inst_id, inst_type)

    def age_sec(self, inst_id: str, inst_type: str = "SPOT", *, source: str = "auto") -> Optional[float]:
        """
        Return data age in seconds.
        source: 'orderbook' | 'ticker' | 'auto' (min of available).
        """
        now = time.monotonic()
        ages: list[float] = []
        if source in ("orderbook", "auto"):
            ob = self.get_orderbook(inst_id, inst_type)
            if ob:
                ages.append(max(0.0, now - float(ob.get("ts_mono", 0))))
        if source in ("ticker", "auto"):
            tk = self.get_ticker(inst_id, inst_type)
            if tk:
                ages.append(max(0.0, now - float(tk.get("ts_mono", 0))))
        if not ages:
            return None
        return min(ages) if source == "auto" else ages[0]

    def is_stale(self, inst_id: str, inst_type: str = "SPOT", *, max_age_sec: float = 30.0) -> bool:
        age = self.age_sec(inst_id, inst_type)
        if age is None:
            return True
        return age > float(max_age_sec)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "tickers": len(self._tickers),
                "orderbooks": len(self._orderbooks),
                "symbols": len(set(self._tickers) | set(self._orderbooks)),
                "last_update_age_sec": max(0.0, time.monotonic() - self._last_update_mono),
                "last_orderbook_age_sec": max(0.0, time.monotonic() - self._last_orderbook_mono),
            }


_GLOBAL_BUFFER = StreamBuffer()
_PRIVATE_BUFFER: Optional["PrivateStreamBuffer"] = None


class PrivateStreamBuffer:
    """In-memory order / position / account cache from private WebSocket."""

    def __init__(self, max_events: int = 500) -> None:
        self._lock = threading.RLock()
        self._orders: dict[str, dict[str, Any]] = {}
        self._positions: dict[str, dict[str, Any]] = {}
        self._account: dict[str, Any] = {}
        self._max = max(50, int(max_events))
        self._last_update_mono = 0.0

    def update_order(self, inst_type: str, order_id: str, row: dict[str, Any]) -> None:
        key = f"{inst_type}:{order_id}".upper()
        with self._lock:
            self._orders[key] = {**row, "ts_mono": time.monotonic(), "inst_type": inst_type}
            if len(self._orders) > self._max:
                oldest = min(self._orders.items(), key=lambda kv: kv[1].get("ts_mono", 0))
                self._orders.pop(oldest[0], None)
            self._last_update_mono = time.monotonic()

    def update_position(self, inst_type: str, inst_id: str, row: dict[str, Any]) -> None:
        key = f"{inst_type}:{inst_id}".upper()
        with self._lock:
            self._positions[key] = {**row, "ts_mono": time.monotonic(), "inst_type": inst_type}
            if len(self._positions) > self._max:
                oldest = min(self._positions.items(), key=lambda kv: kv[1].get("ts_mono", 0))
                self._positions.pop(oldest[0], None)
            self._last_update_mono = time.monotonic()

    def update_account(self, inst_type: str, row: dict[str, Any]) -> None:
        with self._lock:
            self._account[inst_type.upper()] = {**row, "ts_mono": time.monotonic()}
            self._last_update_mono = time.monotonic()

    def get_order(self, order_id: str, inst_type: str = "USDT-FUTURES") -> Optional[dict[str, Any]]:
        key = f"{inst_type}:{order_id}".upper()
        with self._lock:
            row = self._orders.get(key)
            return dict(row) if row else None

    def get_position(self, inst_id: str, inst_type: str = "USDT-FUTURES") -> Optional[dict[str, Any]]:
        key = f"{inst_type}:{inst_id}".upper()
        with self._lock:
            row = self._positions.get(key)
            return dict(row) if row else None

    def list_positions(self, inst_type: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._positions.values())
        if inst_type:
            want = inst_type.upper()
            rows = [r for r in rows if str(r.get("inst_type", "")).upper() == want]
        return [dict(r) for r in rows]

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "orders": len(self._orders),
                "positions": len(self._positions),
                "accounts": len(self._account),
                "last_update_age_sec": max(0.0, time.monotonic() - self._last_update_mono),
            }


def get_stream_buffer() -> StreamBuffer:
    return _GLOBAL_BUFFER


def get_private_stream_buffer() -> PrivateStreamBuffer:
    global _PRIVATE_BUFFER
    if _PRIVATE_BUFFER is None:
        _PRIVATE_BUFFER = PrivateStreamBuffer()
    return _PRIVATE_BUFFER
