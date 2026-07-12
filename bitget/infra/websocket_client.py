"""
Bitget WebSocket client SSOT (Chapter 5+) — public + private v2 protocol.

Institutional rules (Bitget docs):
  - Client sends string ``ping`` every 30s; expects string ``pong``
  - No pong / stale recv → force reconnect (server drops idle ~2min; reset ~24h)
  - Re-subscribe after every reconnect (no gap replay)
  - Private: login (HMAC) before subscribe; never log api secrets
  - ≤10 outbound messages/sec (ping + JSON ops); ≤50 channels/connection recommended
  - Never crash the process — reconnect with exponential backoff

Transport is injectable so unit tests do not require a live socket or websocket-client.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import random
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Protocol, Tuple

from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.memory_policy import (
    WS_LOGIN_TIMEOUT_SEC,
    WS_MAX_CHANNELS_PER_CONN,
    WS_MAX_OUTBOUND_PER_SEC,
    WS_PING_INTERVAL_SEC,
    WS_PONG_TIMEOUT_SEC,
    WS_PRIVATE_URL,
    WS_PUBLIC_URL,
    WS_RECONNECT_BASE_SEC,
    WS_RECONNECT_CAP_SEC,
    WS_RECONNECT_JITTER_SEC,
    WS_STALE_RECV_SEC,
    WS_SUBSCRIBE_BATCH_SIZE,
)

logger = get_logger("bitget.infra.websocket_client")

MessageHandler = Callable[[Any], None]
SubscribeArg = dict[str, Any]
CredentialsProvider = Callable[[], Tuple[str, str, str]]  # api_key, secret, passphrase


class WsState(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    AUTHENTICATED = "authenticated"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"


class WsTransport(Protocol):
    def connect(self, url: str) -> None: ...

    def send(self, data: str) -> None: ...

    def recv(self, timeout: float) -> Optional[str]: ...

    def close(self) -> None: ...


@dataclass
class _OutRateLimiter:
    """Sliding 1-second window — Bitget hard cap 10 msgs/sec."""

    max_per_sec: float = WS_MAX_OUTBOUND_PER_SEC
    _ts: list[float] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - 1.0
                self._ts = [t for t in self._ts if t >= cutoff]
                if len(self._ts) < int(self.max_per_sec):
                    self._ts.append(now)
                    return
                wait = max(0.01, 1.0 - (now - self._ts[0]))
            time.sleep(wait)


def compute_ws_reconnect_sec(attempt: int) -> float:
    a = max(0, int(attempt))
    base = float(WS_RECONNECT_BASE_SEC)
    cap = float(WS_RECONNECT_CAP_SEC)
    exp = min(cap, base * (2 ** a))
    return float(exp + random.uniform(0.0, float(WS_RECONNECT_JITTER_SEC)))


def chunk_subscribe_args(
    args: list[SubscribeArg],
    *,
    batch_size: int = WS_SUBSCRIBE_BATCH_SIZE,
    max_channels: int = WS_MAX_CHANNELS_PER_CONN,
) -> list[list[SubscribeArg]]:
    """Split subscribe args into Bitget-safe batches (≤ max_channels total enforced by caller)."""
    if len(args) > int(max_channels):
        raise ValueError(
            f"subscribe args {len(args)} exceed WS_MAX_CHANNELS_PER_CONN={max_channels}"
        )
    bs = max(1, int(batch_size))
    return [args[i : i + bs] for i in range(0, len(args), bs)]


def build_subscribe_payload(args: list[SubscribeArg]) -> str:
    return json.dumps({"op": "subscribe", "args": args}, separators=(",", ":"))


def build_ws_login_sign(
    secret_key: str,
    *,
    timestamp: str,
) -> str:
    """
    Bitget private WS login signature.
    prehash = timestamp + 'GET' + '/user/verify' → HMAC-SHA256 → Base64.
    Timestamp format follows Bitget request examples (unix seconds string).
    """
    prehash = f"{timestamp}GET/user/verify"
    digest = hmac.new(
        str(secret_key).encode("utf-8"),
        prehash.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def build_ws_login_payload(
    api_key: str,
    secret_key: str,
    passphrase: str,
    *,
    timestamp: Optional[str] = None,
) -> str:
    """JSON login op — never log the returned string in production callers."""
    ts = timestamp if timestamp is not None else str(int(time.time()))
    sign = build_ws_login_sign(secret_key, timestamp=ts)
    body = {
        "op": "login",
        "args": [
            {
                "apiKey": str(api_key),
                "passphrase": str(passphrase),
                "timestamp": str(ts),
                "sign": sign,
            }
        ],
    }
    return json.dumps(body, separators=(",", ":"))


class BitgetPublicWsClient:
    """
    Public Bitget WS lifecycle manager.

    Typical use::

        client = BitgetPublicWsClient(on_message=handler)
        client.set_subscriptions([{...}])
        client.start()   # background thread
        ...
        client.stop()
    """

    def __init__(
        self,
        *,
        url: str = WS_PUBLIC_URL,
        transport: Optional[WsTransport] = None,
        on_message: Optional[MessageHandler] = None,
        on_state: Optional[Callable[[WsState], None]] = None,
        ping_interval_sec: float = WS_PING_INTERVAL_SEC,
        pong_timeout_sec: float = WS_PONG_TIMEOUT_SEC,
        stale_recv_sec: float = WS_STALE_RECV_SEC,
        name: str = "bitget.ws.public",
    ) -> None:
        self.url = str(url)
        self._transport_factory = transport
        self._on_message = on_message
        self._on_state = on_state
        self.ping_interval_sec = float(ping_interval_sec)
        self.pong_timeout_sec = float(pong_timeout_sec)
        self.stale_recv_sec = float(stale_recv_sec)
        self.name = name

        self._lock = threading.RLock()
        self._state = WsState.DISCONNECTED
        self._subs: list[SubscribeArg] = []
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._force_reconnect = threading.Event()
        self._rate = _OutRateLimiter()
        self._reconnect_attempt = 0

        self._last_ping_mono = 0.0
        self._last_pong_mono = 0.0
        self._awaiting_pong = False
        self._last_recv_mono = 0.0

    @property
    def state(self) -> WsState:
        with self._lock:
            return self._state

    def set_subscriptions(self, args: list[SubscribeArg]) -> None:
        with self._lock:
            if len(args) > int(WS_MAX_CHANNELS_PER_CONN):
                raise ValueError(
                    f"subscriptions {len(args)} > WS_MAX_CHANNELS_PER_CONN={WS_MAX_CHANNELS_PER_CONN}"
                )
            self._subs = [dict(a) for a in args]

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run_loop, name=self.name, daemon=True
            )
            self._thread.start()

    def stop(self, *, join_timeout: float = 5.0) -> None:
        self._stop.set()
        self._force_reconnect.set()
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=join_timeout)
        self._set_state(WsState.STOPPED)

    def request_reconnect(self) -> None:
        """Force session end so next loop re-subscribes with current ``set_subscriptions``."""
        self._force_reconnect.set()

    def _set_state(self, st: WsState) -> None:
        with self._lock:
            if self._state == st:
                return
            self._state = st
        if self._on_state:
            try:
                self._on_state(st)
            except Exception as e:
                log_exception(logger, "on_state callback failed: %s", e)

    def _make_transport(self) -> WsTransport:
        if self._transport_factory is not None:
            return self._transport_factory
        return WebsocketClientTransport()

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._session()
            except Exception as e:
                log_exception(logger, "[%s] session error: %s", self.name, e)
            if self._stop.is_set():
                break
            self._set_state(WsState.RECONNECTING)
            wait = compute_ws_reconnect_sec(self._reconnect_attempt)
            self._reconnect_attempt += 1
            logger.warning(
                "[%s] reconnect in %.2fs (attempt=%s)",
                self.name,
                wait,
                self._reconnect_attempt,
            )
            if self._stop.wait(wait):
                break
        self._set_state(WsState.STOPPED)

    def _after_connect(self, transport: WsTransport) -> None:
        """Hook for private login; public is a no-op."""
        return None

    def _session(self) -> None:
        self._set_state(WsState.CONNECTING)
        transport = self._make_transport()
        try:
            transport.connect(self.url)
        except Exception:
            try:
                transport.close()
            except Exception:
                pass
            raise

        now = time.monotonic()
        self._last_recv_mono = now
        self._last_pong_mono = now
        self._last_ping_mono = now
        self._awaiting_pong = False
        self._set_state(WsState.CONNECTED)
        self._reconnect_attempt = 0

        try:
            self._after_connect(transport)
            self._send_subscriptions(transport)
            while not self._stop.is_set():
                self._heartbeat(transport)
                if self._should_force_reconnect():
                    logger.warning("[%s] heartbeat/stale force reconnect", self.name)
                    break
                frame = transport.recv(timeout=0.5)
                if frame is None:
                    continue
                self._handle_frame(frame)
        finally:
            try:
                transport.close()
            except Exception:
                pass
            if not self._stop.is_set():
                self._set_state(WsState.DISCONNECTED)

    def _send_subscriptions(self, transport: WsTransport) -> None:
        with self._lock:
            args = [dict(a) for a in self._subs]
        if not args:
            return
        for batch in chunk_subscribe_args(args):
            payload = build_subscribe_payload(batch)
            self._send_raw(transport, payload)

    def _send_raw(self, transport: WsTransport, data: str) -> None:
        self._rate.acquire()
        transport.send(data)

    def _heartbeat(self, transport: WsTransport) -> None:
        now = time.monotonic()
        if now - self._last_ping_mono < self.ping_interval_sec:
            return
        self._send_raw(transport, "ping")
        self._last_ping_mono = now
        self._awaiting_pong = True

    def _should_force_reconnect(self) -> bool:
        if self._force_reconnect.is_set():
            self._force_reconnect.clear()
            return True
        now = time.monotonic()
        if self._awaiting_pong and (now - self._last_ping_mono) > self.pong_timeout_sec:
            return True
        if (now - self._last_recv_mono) > self.stale_recv_sec:
            return True
        return False

    def _handle_control_event(self, payload: dict[str, Any]) -> bool:
        """Return True if frame was fully consumed (do not forward to on_message)."""
        return False

    def _handle_frame(self, frame: str) -> None:
        self._last_recv_mono = time.monotonic()
        text = frame.strip() if isinstance(frame, str) else str(frame)
        if text == "pong":
            self._awaiting_pong = False
            self._last_pong_mono = self._last_recv_mono
            return
        if text == "ping":
            return
        try:
            payload = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            logger.debug("[%s] non-json frame ignored: %s", self.name, text[:80])
            return
        if isinstance(payload, dict) and self._handle_control_event(payload):
            return
        if self._on_message:
            try:
                self._on_message(payload)
            except Exception as e:
                log_exception(logger, "[%s] on_message failed: %s", self.name, e)


class BitgetPrivateWsClient(BitgetPublicWsClient):
    """
    Private Bitget WS — login then subscribe; re-login on every reconnect.
    Credentials are fetched per session via ``credentials_provider`` (never stored long-term).
    """

    def __init__(
        self,
        *,
        credentials_provider: CredentialsProvider,
        url: str = WS_PRIVATE_URL,
        transport: Optional[WsTransport] = None,
        on_message: Optional[MessageHandler] = None,
        on_state: Optional[Callable[[WsState], None]] = None,
        ping_interval_sec: float = WS_PING_INTERVAL_SEC,
        pong_timeout_sec: float = WS_PONG_TIMEOUT_SEC,
        stale_recv_sec: float = WS_STALE_RECV_SEC,
        login_timeout_sec: float = WS_LOGIN_TIMEOUT_SEC,
        name: str = "bitget.ws.private",
    ) -> None:
        super().__init__(
            url=url,
            transport=transport,
            on_message=on_message,
            on_state=on_state,
            ping_interval_sec=ping_interval_sec,
            pong_timeout_sec=pong_timeout_sec,
            stale_recv_sec=stale_recv_sec,
            name=name,
        )
        self._credentials_provider = credentials_provider
        self.login_timeout_sec = float(login_timeout_sec)
        self._login_event = threading.Event()
        self._login_ok = False
        self._login_code = ""

    def _after_connect(self, transport: WsTransport) -> None:
        self._login_event.clear()
        self._login_ok = False
        self._login_code = ""
        try:
            api_key, secret, passphrase = self._credentials_provider()
        except Exception as e:
            raise RuntimeError(f"credentials_provider failed: {e}") from e
        if not api_key or not secret or not passphrase:
            raise RuntimeError("missing Bitget API credentials for private WS")
        login_json = build_ws_login_payload(api_key, secret, passphrase)
        self._send_raw(transport, login_json)
        deadline = time.monotonic() + self.login_timeout_sec
        while time.monotonic() < deadline and not self._stop.is_set():
            if self._login_event.is_set():
                break
            frame = transport.recv(timeout=0.5)
            if frame is None:
                continue
            self._handle_frame(frame)
        if not self._login_ok:
            raise RuntimeError(
                f"private WS login failed/timeout code={self._login_code or 'none'}"
            )
        self._set_state(WsState.AUTHENTICATED)
        logger.info("[%s] private WS login ok", self.name)

    def _handle_control_event(self, payload: dict[str, Any]) -> bool:
        ev = str(payload.get("event") or "")
        if ev == "login":
            code = str(payload.get("code") or "")
            self._login_code = code
            self._login_ok = code in ("0", "00", "")
            self._login_event.set()
            return True
        if ev == "error" and not self._login_event.is_set():
            self._login_code = str(payload.get("code") or "error")
            self._login_ok = False
            self._login_event.set()
            return True
        return False


class WebsocketClientTransport:
    """Optional live transport via ``websocket-client`` (websocket)."""

    def __init__(self) -> None:
        try:
            import websocket  # type: ignore
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                "websocket-client package required for live Bitget WS "
                "(pip install websocket-client)"
            ) from e
        self._ws_mod = websocket
        self._ws = None

    def connect(self, url: str) -> None:
        self._ws = self._ws_mod.create_connection(url, timeout=15)
        self._ws.settimeout(0.5)

    def send(self, data: str) -> None:
        if self._ws is None:
            raise RuntimeError("ws not connected")
        self._ws.send(data)

    def recv(self, timeout: float) -> Optional[str]:
        if self._ws is None:
            raise RuntimeError("ws not connected")
        self._ws.settimeout(timeout)
        try:
            msg = self._ws.recv()
        except Exception as e:
            name = type(e).__name__.lower()
            if "timeout" in name or "timed out" in str(e).lower():
                return None
            raise
        if isinstance(msg, bytes):
            return msg.decode("utf-8", errors="replace")
        return str(msg) if msg is not None else None

    def close(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None


class FakeWsTransport:
    """Deterministic in-memory transport for unit tests."""

    def __init__(self, *, auto_login_ok: bool = True) -> None:
        self.connected = False
        self.closed = False
        self.sent: list[str] = []
        self._inbox: list[Optional[str]] = []
        self.connect_raises: Optional[BaseException] = None
        self.recv_raises: Optional[BaseException] = None
        self.auto_login_ok = bool(auto_login_ok)

    def push(self, frame: Optional[str]) -> None:
        self._inbox.append(frame)

    def connect(self, url: str) -> None:
        if self.connect_raises:
            raise self.connect_raises
        self.connected = True
        self.closed = False
        self.url = url

    def send(self, data: str) -> None:
        self.sent.append(data)
        if data == "ping":
            self._inbox.append("pong")
        elif self.auto_login_ok and '"op":"login"' in data:
            self._inbox.append('{"event":"login","code":"0","msg":""}')

    def recv(self, timeout: float) -> Optional[str]:
        if self.recv_raises:
            raise self.recv_raises
        if self._inbox:
            return self._inbox.pop(0)
        time.sleep(min(0.01, max(0.0, timeout)))
        return None

    def close(self) -> None:
        self.closed = True
        self.connected = False
