"""
Bitget private WebSocket (V2) — orders / positions / account -> PrivateStreamBuffer.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import os
import time
from typing import Any, Optional

import aiohttp

from bitget.data.stream_buffer import get_private_stream_buffer
from bitget.env import bitget_access_key, bitget_passphrase, bitget_secret_key
from bitget.infra.logging_setup import get_logger, setup_logging

logger = get_logger("bitget.data.ws_private")

PRIVATE_WS_URL = os.environ.get("BITGET_WS_PRIVATE_URL", "wss://ws.bitget.com/v2/ws/private")
DEFAULT_INST_TYPES = ("USDT-FUTURES", "SPOT")


def has_private_credentials() -> bool:
    return bool(bitget_access_key() and bitget_secret_key() and bitget_passphrase())


def _ws_login_sign(timestamp: str, secret: str) -> str:
    payload = f"{timestamp}GET/user/verify"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def _build_login_message() -> dict[str, Any]:
    ts = str(int(time.time() * 1000))
    return {
        "op": "login",
        "args": [
            {
                "apiKey": bitget_access_key(),
                "passphrase": bitget_passphrase(),
                "timestamp": ts,
                "sign": _ws_login_sign(ts, bitget_secret_key()),
            }
        ],
    }


def _build_subscribe_args(inst_types: tuple[str, ...]) -> list[dict[str, str]]:
    args: list[dict[str, str]] = []
    for inst_type in inst_types:
        for channel in ("orders", "positions", "account"):
            args.append({"instType": inst_type, "channel": channel, "instId": "default"})
    return args


def _handle_private_push(payload: dict[str, Any]) -> None:
    arg = payload.get("arg") or {}
    if not isinstance(arg, dict):
        return
    channel = str(arg.get("channel") or "").lower()
    inst_type = str(arg.get("instType") or "USDT-FUTURES").upper()
    data = payload.get("data")
    if not data:
        return

    buf = get_private_stream_buffer()
    rows = data if isinstance(data, list) else [data]
    for row in rows:
        if not isinstance(row, dict):
            continue
        if channel == "orders":
            oid = str(row.get("orderId") or row.get("ordId") or row.get("clientOid") or "")
            if oid:
                buf.update_order(inst_type, oid, row)
        elif channel == "positions":
            inst_id = str(row.get("instId") or row.get("symbol") or "default")
            buf.update_position(inst_type, inst_id, row)
        elif channel == "account":
            buf.update_account(inst_type, row)


class BitgetPrivateWsClient:
    def __init__(
        self,
        *,
        inst_types: tuple[str, ...] = DEFAULT_INST_TYPES,
        ping_interval_sec: float = 25.0,
    ) -> None:
        self.inst_types = tuple(t.upper() for t in inst_types if t)
        self.ping_interval_sec = max(10.0, float(ping_interval_sec))
        self._stop = asyncio.Event()
        self._logged_in = False

    def stop(self) -> None:
        self._stop.set()

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.ping_interval_sec)
            try:
                await ws.send_str(json.dumps({"op": "ping"}))
            except Exception as e:
                logger.warning("private ws ping failed: %s", e)
                return

    async def _handle_message(self, text: str) -> None:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return

        event = str(payload.get("event") or "")
        if event == "pong" or payload.get("op") == "pong":
            return
        if event == "login":
            code = str(payload.get("code") or "")
            if code in ("0", ""):
                self._logged_in = True
                logger.info("private ws login ok")
            else:
                logger.warning("private ws login failed: %s", payload)
            return
        if event == "error":
            logger.warning("private ws error event: %s", payload)
            return

        if payload.get("action") in ("snapshot", "update") or payload.get("data"):
            _handle_private_push(payload)

    async def run_forever(self) -> None:
        if not has_private_credentials():
            logger.warning("private ws skipped: missing API credentials")
            return

        setup_logging(default_component="bitget.ws_private")
        backoff = 1.0
        pbuf = get_private_stream_buffer()

        while not self._stop.is_set():
            self._logged_in = False
            try:
                timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=120)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(PRIVATE_WS_URL, heartbeat=None) as ws:
                        logger.info("connected %s", PRIVATE_WS_URL)
                        backoff = 1.0
                        await ws.send_str(json.dumps(_build_login_message()))

                        for _ in range(30):
                            if self._logged_in or self._stop.is_set():
                                break
                            msg = await ws.receive(timeout=2.0)
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._handle_message(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break

                        if not self._logged_in:
                            raise RuntimeError("private ws login timeout")

                        sub = {"op": "subscribe", "args": _build_subscribe_args(self.inst_types)}
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
                                    logger.warning("private ws closed/error type=%s", msg.type)
                                    break
                        finally:
                            ping_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await ping_task
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("private ws reconnect in %.1fs: %s", backoff, e)
                await asyncio.sleep(backoff)
                backoff = min(60.0, backoff * 1.8)
            finally:
                try:
                    from bitget.infra import ops_logger

                    ops_logger.record_heartbeat(
                        "bitget.ws_private",
                        extra={"buffer": pbuf.stats(), "logged_in": self._logged_in},
                    )
                except Exception:
                    pass


async def run_private_ws_supervisor(
    *,
    inst_types: Optional[tuple[str, ...]] = None,
) -> None:
    types = inst_types or DEFAULT_INST_TYPES
    client = BitgetPrivateWsClient(inst_types=types)
    await client.run_forever()
