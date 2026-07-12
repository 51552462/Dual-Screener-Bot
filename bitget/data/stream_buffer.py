"""
In-memory ticker / orderbook / private position cache (thread-safe).

WebSocket writers update; scanners and slippage_guard read for spread / last-price gates.

Tier-1 HOT RAM — row dict in-place reuse, orderbook depth cap, optional raw drop.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Optional

import memory_bounds

from bitget.infra.memory_policy import (
    PRIVATE_STREAM_MAX_EVENTS,
    STREAM_BUFFER_MAX_SYMBOLS,
    STREAM_BUFFER_ORDERBOOK_DEPTH,
    STREAM_BUFFER_STORE_RAW,
)


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


def _cache_key(inst_id: str, inst_type: str) -> str:
    return f"{inst_type}:{inst_id}".upper()


class StreamBuffer:
    """Public market data: ticker + orderbook (top-of-book)."""

    def __init__(self, max_symbols: int = STREAM_BUFFER_MAX_SYMBOLS) -> None:
        self._lock = threading.RLock()
        self._tickers: dict[str, dict[str, Any]] = {}
        self._orderbooks: dict[str, dict[str, Any]] = {}
        self._max = max(100, int(max_symbols))
        self._book_depth = max(5, int(STREAM_BUFFER_ORDERBOOK_DEPTH))
        self._store_raw = bool(STREAM_BUFFER_STORE_RAW)
        now = time.monotonic()
        self._last_update_mono = now
        self._last_orderbook_mono = now
        self._ages_buf: list[float] = []

    def _ensure_capacity(self, store: dict[str, dict[str, Any]]) -> None:
        if len(store) >= self._max:
            memory_bounds.evict_oldest_dict_keys(
                store,
                self._max - 1,
                ts_getter=lambda k: store[k].get("ts_mono", 0.0),
            )

    def _get_or_create_row(self, store: dict[str, dict[str, Any]], key: str) -> dict[str, Any]:
        row = store.get(key)
        if row is None:
            self._ensure_capacity(store)
            row = {}
            store[key] = row
        return row

    @staticmethod
    def _write_ticker_row(
        row: dict[str, Any],
        *,
        inst_id: str,
        inst_type: str,
        last: float,
        bid: Optional[float],
        ask: Optional[float],
        quote_volume_24h: Optional[float],
        ts_mono: float,
        raw: Optional[dict],
        store_raw: bool,
    ) -> None:
        row["inst_id"] = inst_id
        row["inst_type"] = inst_type
        row["last"] = float(last)
        row["bid"] = float(bid) if bid is not None else None
        row["ask"] = float(ask) if ask is not None else None
        row["quote_volume_24h"] = float(quote_volume_24h) if quote_volume_24h is not None else None
        row["ts_mono"] = ts_mono
        if store_raw and raw:
            slot = row.get("raw")
            if not isinstance(slot, dict):
                slot = {}
                row["raw"] = slot
            slot.clear()
            slot.update(raw)
        else:
            row.pop("raw", None)

    @staticmethod
    def _write_orderbook_row(
        row: dict[str, Any],
        *,
        inst_id: str,
        inst_type: str,
        bids_store: Any,
        asks_store: Any,
        best_bid: Optional[float],
        best_ask: Optional[float],
        ts_mono: float,
        raw: Optional[dict],
        store_raw: bool,
    ) -> None:
        row["inst_id"] = inst_id
        row["inst_type"] = inst_type
        row["bids"] = bids_store
        row["asks"] = asks_store
        row["best_bid"] = best_bid
        row["best_ask"] = best_ask
        row["ts_mono"] = ts_mono
        if store_raw and raw:
            slot = row.get("raw")
            if not isinstance(slot, dict):
                slot = {}
                row["raw"] = slot
            slot.clear()
            slot.update(raw)
        else:
            row.pop("raw", None)

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
        key = _cache_key(inst_id, inst_type)
        ts_mono = time.monotonic()
        with self._lock:
            row = self._get_or_create_row(self._tickers, key)
            self._write_ticker_row(
                row,
                inst_id=inst_id,
                inst_type=inst_type,
                last=last,
                bid=bid,
                ask=ask,
                quote_volume_24h=quote_volume_24h,
                ts_mono=ts_mono,
                raw=raw,
                store_raw=self._store_raw,
            )
            self._last_update_mono = ts_mono

    def update_orderbook(
        self,
        inst_id: str,
        *,
        bids: Any,
        asks: Any,
        inst_type: str = "SPOT",
        raw: Optional[dict] = None,
    ) -> None:
        key = _cache_key(inst_id, inst_type)
        best_bid = _best_level(bids)
        best_ask = _best_level(asks)
        bids_store = memory_bounds.truncate_orderbook_levels(bids, self._book_depth)
        asks_store = memory_bounds.truncate_orderbook_levels(asks, self._book_depth)
        ts_mono = time.monotonic()
        with self._lock:
            row = self._get_or_create_row(self._orderbooks, key)
            self._write_orderbook_row(
                row,
                inst_id=inst_id,
                inst_type=inst_type,
                bids_store=bids_store,
                asks_store=asks_store,
                best_bid=best_bid,
                best_ask=best_ask,
                ts_mono=ts_mono,
                raw=raw,
                store_raw=self._store_raw,
            )
            self._last_orderbook_mono = ts_mono
            self._last_update_mono = ts_mono

    def get_ticker(self, inst_id: str, inst_type: str = "SPOT") -> Optional[dict[str, Any]]:
        key = _cache_key(inst_id, inst_type)
        with self._lock:
            row = self._tickers.get(key)
            return dict(row) if row else None

    def get_orderbook(self, inst_id: str, inst_type: str = "SPOT") -> Optional[dict[str, Any]]:
        key = _cache_key(inst_id, inst_type)
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
        ages = self._ages_buf
        ages.clear()
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

    def __init__(self, max_events: int = PRIVATE_STREAM_MAX_EVENTS) -> None:
        self._lock = threading.RLock()
        self._orders: dict[str, dict[str, Any]] = {}
        self._positions: dict[str, dict[str, Any]] = {}
        self._account: dict[str, dict[str, Any]] = {}
        self._max = max(50, int(max_events))
        self._last_update_mono = 0.0
        # Per-channel freshness — account tick must NOT make orders look fresh
        self._channel_mono: dict[str, float] = {
            "orders": 0.0,
            "positions": 0.0,
            "account": 0.0,
        }

    def _evict_oldest(self, store: dict[str, dict[str, Any]]) -> None:
        memory_bounds.evict_oldest_dict_keys(
            store,
            self._max,
            ts_getter=lambda k: store[k].get("ts_mono", 0.0),
        )

    def _upsert_row(
        self,
        store: dict[str, dict[str, Any]],
        key: str,
        row: dict[str, Any],
        *,
        inst_type: str,
    ) -> None:
        slot = store.get(key)
        if slot is None:
            if len(store) >= self._max:
                self._evict_oldest(store)
            slot = {}
            store[key] = slot
        slot.clear()
        slot.update(row)
        slot["ts_mono"] = time.monotonic()
        slot["inst_type"] = inst_type

    def _touch_channel(self, channel: str) -> None:
        now = time.monotonic()
        self._channel_mono[channel] = now
        self._last_update_mono = now

    def channel_age_sec(self, channel: str) -> float:
        with self._lock:
            mono = float(self._channel_mono.get(str(channel), 0.0) or 0.0)
        if mono <= 0.0:
            return 1e9
        return max(0.0, time.monotonic() - mono)

    def touch_channel(self, channel: str) -> None:
        """Mark channel fresh (e.g. empty snapshot = confirmed flat book)."""
        with self._lock:
            self._touch_channel(str(channel))

    def update_order(self, inst_type: str, order_id: str, row: dict[str, Any]) -> None:
        key = f"{inst_type}:{order_id}".upper()
        with self._lock:
            self._upsert_row(self._orders, key, row, inst_type=inst_type)
            self._touch_channel("orders")

    def update_position(self, inst_type: str, inst_id: str, row: dict[str, Any]) -> None:
        key = f"{inst_type}:{inst_id}".upper()
        with self._lock:
            self._upsert_row(self._positions, key, row, inst_type=inst_type)
            self._touch_channel("positions")

    def update_account(self, inst_type: str, row: dict[str, Any]) -> None:
        want = inst_type.upper()
        with self._lock:
            slot = self._account.get(want)
            if slot is None:
                slot = {}
                self._account[want] = slot
            slot.clear()
            slot.update(row)
            slot["ts_mono"] = time.monotonic()
            self._touch_channel("account")

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

    def get_account(self, inst_type: str = "USDT-FUTURES") -> Optional[dict[str, Any]]:
        """Copy of last account projection for inst_type, or None if never written."""
        want = str(inst_type or "USDT-FUTURES").upper()
        with self._lock:
            row = self._account.get(want)
            return dict(row) if row else None

    def list_positions(self, inst_type: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._positions.values())
        if inst_type:
            want = inst_type.upper()
            rows = [r for r in rows if str(r.get("inst_type", "")).upper() == want]
        return [dict(r) for r in rows]

    def list_orders(self, inst_type: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = list(self._orders.values())
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
                "orders_age_sec": (
                    1e9
                    if self._channel_mono["orders"] <= 0
                    else max(0.0, time.monotonic() - self._channel_mono["orders"])
                ),
                "positions_age_sec": (
                    1e9
                    if self._channel_mono["positions"] <= 0
                    else max(0.0, time.monotonic() - self._channel_mono["positions"])
                ),
                "account_age_sec": (
                    1e9
                    if self._channel_mono["account"] <= 0
                    else max(0.0, time.monotonic() - self._channel_mono["account"])
                ),
            }


def get_stream_buffer() -> StreamBuffer:
    return _GLOBAL_BUFFER


def get_private_stream_buffer() -> PrivateStreamBuffer:
    global _PRIVATE_BUFFER
    if _PRIVATE_BUFFER is None:
        _PRIVATE_BUFFER = PrivateStreamBuffer()
    return _PRIVATE_BUFFER
