"""
Bitget public WebSocket (V2) — ticker + books1 -> StreamBuffer.

Uses aiohttp (already in root requirements). No ccxt pro required.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from typing import Any, Optional

import aiohttp

from bitget.data.stream_buffer import get_stream_buffer
from bitget.infra.logging_setup import get_logger, setup_logging

logger = get_logger("bitget.data.ws_public")

PUBLIC_WS_URL = os.environ.get("BITGET_WS_PUBLIC_URL", "wss://ws.bitget.com/v2/ws/public")
DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
ORDERBOOK_CHANNEL = os.environ.get("BITGET_WS_ORDERBOOK_CHANNEL", "books1")


def _parse_ticker_msg(payload: dict) -> Optional[tuple[str, str, dict]]:
    if not isinstance(payload, dict):
        return None
    arg = payload.get("arg") or {}
    data = payload.get("data")
    if not isinstance(arg, dict) or not data:
        return None
    channel = str(arg.get("channel") or "")
    if channel != "ticker":
        return None
    inst_type = str(arg.get("instType") or "SPOT").upper()
    inst_id = str(arg.get("instId") or "").upper()
    if not inst_id:
        return None
    rows = data if isinstance(data, list) else [data]
    if not rows:
        return None
    row = rows[0] if isinstance(rows[0], dict) else {}
    return inst_type, inst_id, row


def _parse_books_msg(payload: dict) -> Optional[tuple[str, str, dict]]:
    if not isinstance(payload, dict):
        return None
    arg = payload.get("arg") or {}
    data = payload.get("data")
    if not isinstance(arg, dict) or not data:
        return None
    channel = str(arg.get("channel") or "").lower()
    if not channel.startswith("books"):
        return None
    inst_type = str(arg.get("instType") or "SPOT").upper()
    inst_id = str(arg.get("instId") or "").upper()
    if not inst_id:
        return None
    rows = data if isinstance(data, list) else [data]
    if not rows:
        return None
    row = rows[0] if isinstance(rows[0], dict) else {}
    return inst_type, inst_id, row


def _float_or_none(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class BitgetPublicWsClient:
    def __init__(
        self,
        *,
        symbols: tuple[str, ...] = DEFAULT_SYMBOLS,
        inst_types: tuple[str, ...] = ("SPOT", "USDT-FUTURES"),
        ping_interval_sec: float = 25.0,
        orderbook_channel: str = ORDERBOOK_CHANNEL,
    ) -> None:
        self.symbols = tuple(s.upper() for s in symbols if s)
        self.inst_types = tuple(t.upper() for t in inst_types if t)
        self.ping_interval_sec = max(10.0, float(ping_interval_sec))
        self.orderbook_channel = str(orderbook_channel or "books1").strip()
        self._stop = asyncio.Event()
        self._session: Optional[aiohttp.ClientSession] = None

    def stop(self) -> None:
        self._stop.set()

    def _build_subscribe_args(self) -> list[dict]:
        args: list[dict] = []
        for inst_type in self.inst_types:
            for sym in self.symbols:
                args.append({"instType": inst_type, "channel": "ticker", "instId": sym})
                args.append(
                    {
                        "instType": inst_type,
                        "channel": self.orderbook_channel,
                        "instId": sym,
                    }
                )
        return args

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.ping_interval_sec)
            try:
                await ws.send_str(json.dumps({"op": "ping"}))
            except Exception as e:
                logger.warning("ws ping failed: %s", e)
                return

    async def _handle_message(self, text: str) -> None:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return
        if isinstance(payload, dict) and payload.get("event") in ("pong", "subscribe"):
            return

        buf = get_stream_buffer()

        parsed_book = _parse_books_msg(payload) if isinstance(payload, dict) else None
        if parsed_book:
            inst_type, inst_id, row = parsed_book
            bids = row.get("bids") or row.get("bid")
            asks = row.get("asks") or row.get("ask")
            if bids or asks:
                buf.update_orderbook(
                    inst_id,
                    bids=bids or [],
                    asks=asks or [],
                    inst_type=inst_type,
                    raw=row,
                )
            return

        parsed = _parse_ticker_msg(payload) if isinstance(payload, dict) else None
        if not parsed:
            return
        inst_type, inst_id, row = parsed
        last = _float_or_none(row.get("lastPr") or row.get("last") or row.get("close"))
        if last is None:
            return
        buf.update_ticker(
            inst_id,
            last=last,
            bid=_float_or_none(row.get("bidPr") or row.get("bestBid")),
            ask=_float_or_none(row.get("askPr") or row.get("bestAsk")),
            quote_volume_24h=_float_or_none(row.get("quoteVolume") or row.get("usdtVolume")),
            inst_type=inst_type,
            raw=row,
        )

    async def run_forever(self) -> None:
        setup_logging(default_component="bitget.ws_public")
        backoff = 1.0
        buf = get_stream_buffer()
        while not self._stop.is_set():
            try:
                timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=120)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    self._session = session
                    async with session.ws_connect(PUBLIC_WS_URL, heartbeat=None) as ws:
                        logger.info(
                            "connected %s symbols=%s channels=ticker,%s",
                            PUBLIC_WS_URL,
                            len(self.symbols),
                            self.orderbook_channel,
                        )
                        backoff = 1.0
                        sub = {"op": "subscribe", "args": self._build_subscribe_args()}
                        await ws.send_str(json.dumps(sub))
                        ping_task = asyncio.create_task(self._ping_loop(ws))
                        try:
                            async for msg in ws:
                                if self._stop.is_set():
                                    break
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    await self._handle_message(msg.data)
                                elif msg.type in (
                                    aiohttp.WSMsgType.CLOSED,
                                    aiohttp.WSMsgType.ERROR,
                                ):
                                    logger.warning("ws closed/error type=%s", msg.type)
                                    break
                        finally:
                            ping_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await ping_task
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("ws reconnect in %.1fs: %s", backoff, e)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 1.8)
            finally:
                self._session = None
            try:
                from bitget.infra import ops_logger

                ops_logger.record_heartbeat(
                    "bitget.ws_public",
                    extra={"buffer": buf.stats()},
                )
            except Exception:
                pass


async def run_public_ws_supervisor(
    *,
    symbols: Optional[tuple[str, ...]] = None,
) -> None:
    syms = symbols or DEFAULT_SYMBOLS
    client = BitgetPublicWsClient(symbols=syms)
    await client.run_forever()
