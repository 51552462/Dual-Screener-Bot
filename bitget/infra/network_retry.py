"""
Bitget network retry SSOT (Chapter 1) — REST exception classify + exponential backoff.

Institutional rules:
  - Typed failure classes: CONNECTION / TIMEOUT / RATE_LIMIT_429 / HTTP_OTHER / FATAL
  - Never crash the process from a single transport blip — callers use call_with_retry
  - Backoff: 2s → 4s → 8s (RATE_LIMIT_429 uses longer floor/cap + Retry-After when present)
  - Throttle keys remain in ``bitget.rate_limit_guard`` (client-side spacing)

Chapter 2+ wires OMS / MTF / satellite / WebSocket onto this module.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.memory_policy import (
    NETWORK_429_BACKOFF_BASE_SEC,
    NETWORK_429_BACKOFF_CAP_SEC,
    NETWORK_BACKOFF_BASE_SEC,
    NETWORK_BACKOFF_CAP_SEC,
    NETWORK_BACKOFF_JITTER_SEC,
    NETWORK_RETRY_MAX_ATTEMPTS,
)

logger = get_logger("bitget.infra.network_retry")

T = TypeVar("T")


class NetworkErrorKind(str, Enum):
    CONNECTION = "connection"
    TIMEOUT = "timeout"
    RATE_LIMIT_429 = "rate_limit_429"
    HTTP_OTHER = "http_other"
    TRANSIENT = "transient"
    FATAL = "fatal"


@dataclass(frozen=True)
class ClassifiedNetworkError:
    kind: NetworkErrorKind
    retryable: bool
    exc: BaseException
    retry_after_sec: Optional[float] = None
    status_code: Optional[int] = None


class NetworkRetryExhausted(RuntimeError):
    """All retry attempts failed — process must NOT treat this as uncaught crash if caller catches."""

    def __init__(self, message: str, *, last_error: ClassifiedNetworkError | None = None):
        super().__init__(message)
        self.last_error = last_error


def _status_from_exc(exc: BaseException) -> Optional[int]:
    for attr in ("status", "status_code", "http_status", "code"):
        v = getattr(exc, attr, None)
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    resp = getattr(exc, "response", None)
    if resp is not None:
        for attr in ("status_code", "status"):
            v = getattr(resp, attr, None)
            if v is None:
                continue
            try:
                return int(v)
            except (TypeError, ValueError):
                continue
    return None


def _retry_after_from_exc(exc: BaseException) -> Optional[float]:
    resp = getattr(exc, "response", None)
    headers = getattr(resp, "headers", None) if resp is not None else None
    if not headers:
        headers = getattr(exc, "headers", None)
    if not headers:
        return None
    try:
        raw = headers.get("Retry-After") or headers.get("retry-after")
    except Exception:
        return None
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None


def _type_name_chain(exc: BaseException) -> str:
    names = [type(exc).__name__]
    cur: Optional[BaseException] = exc
    seen = 0
    while cur is not None and seen < 4:
        names.append(type(cur).__name__)
        cur = cur.__cause__ or cur.__context__
        seen += 1
    return " ".join(names).lower()


def classify_network_error(exc: BaseException) -> ClassifiedNetworkError:
    """Map transport/exchange exceptions to typed retry policy."""
    status = _status_from_exc(exc)
    retry_after = _retry_after_from_exc(exc)
    type_chain = _type_name_chain(exc)
    msg = str(exc).lower()
    name = type(exc).__name__.lower()

    # --- explicit HTTP status ---
    if status == 429 or "ratelimit" in name or "rate limit" in msg or "too many requests" in msg:
        return ClassifiedNetworkError(
            NetworkErrorKind.RATE_LIMIT_429,
            True,
            exc,
            retry_after_sec=retry_after,
            status_code=status or 429,
        )

    if status in (408, 502, 503, 504):
        return ClassifiedNetworkError(
            NetworkErrorKind.HTTP_OTHER,
            True,
            exc,
            retry_after_sec=retry_after,
            status_code=status,
        )

    if status is not None and 400 <= status < 500 and status != 408:
        # 4xx (except 408/429) — usually non-retryable (auth, bad request)
        return ClassifiedNetworkError(
            NetworkErrorKind.FATAL,
            False,
            exc,
            retry_after_sec=None,
            status_code=status,
        )

    # --- typed exception families (requests / urllib3 / ccxt / builtins) ---
    if isinstance(exc, TimeoutError) or "timeout" in name or "timedout" in name.replace(" ", ""):
        return ClassifiedNetworkError(NetworkErrorKind.TIMEOUT, True, exc, status_code=status)

    if isinstance(exc, ConnectionError) or any(
        k in type_chain
        for k in (
            "connectionerror",
            "connectionreset",
            "connectionaborted",
            "connectionrefused",
            "brokenpipe",
            "protocolerror",
            "sslerro",
            "networkerror",
            "exchange not available",
            "exchangenotavailable",
        )
    ):
        return ClassifiedNetworkError(NetworkErrorKind.CONNECTION, True, exc, status_code=status)

    if "timeouterror" in type_chain or "requesttimeout" in type_chain or "readtimeout" in type_chain:
        return ClassifiedNetworkError(NetworkErrorKind.TIMEOUT, True, exc, status_code=status)

    if "httperror" in type_chain or "http error" in msg:
        if status == 429 or "429" in msg:
            return ClassifiedNetworkError(
                NetworkErrorKind.RATE_LIMIT_429,
                True,
                exc,
                retry_after_sec=retry_after,
                status_code=429,
            )
        if status is not None and 500 <= status <= 599:
            return ClassifiedNetworkError(
                NetworkErrorKind.HTTP_OTHER,
                True,
                exc,
                retry_after_sec=retry_after,
                status_code=status,
            )
        return ClassifiedNetworkError(
            NetworkErrorKind.HTTP_OTHER,
            bool(status is None or status >= 500 or status == 408),
            exc,
            retry_after_sec=retry_after,
            status_code=status,
        )

    # --- message heuristics (ccxt stringy errors) ---
    if any(k in msg for k in ("429", "rate limit", "ratelimit", "too many requests")):
        return ClassifiedNetworkError(
            NetworkErrorKind.RATE_LIMIT_429,
            True,
            exc,
            retry_after_sec=retry_after,
            status_code=status or 429,
        )
    if any(k in msg for k in ("timed out", "timeout", "deadline exceeded")):
        return ClassifiedNetworkError(NetworkErrorKind.TIMEOUT, True, exc, status_code=status)
    if any(
        k in msg
        for k in (
            "connection reset",
            "connection aborted",
            "connection refused",
            "network",
            "econnreset",
            "temporarily unavailable",
            "gateway",
        )
    ):
        return ClassifiedNetworkError(NetworkErrorKind.CONNECTION, True, exc, status_code=status)
    if any(k in msg for k in ("502", "503", "504", "unavailable")):
        return ClassifiedNetworkError(NetworkErrorKind.TRANSIENT, True, exc, status_code=status)

    return ClassifiedNetworkError(NetworkErrorKind.FATAL, False, exc, status_code=status)


def compute_backoff_sec(
    attempt: int,
    kind: NetworkErrorKind,
    *,
    retry_after_sec: Optional[float] = None,
) -> float:
    """Exponential backoff for attempt index 0..N-1 (before next try)."""
    a = max(0, int(attempt))
    if kind == NetworkErrorKind.RATE_LIMIT_429:
        base = float(NETWORK_429_BACKOFF_BASE_SEC)
        cap = float(NETWORK_429_BACKOFF_CAP_SEC)
    else:
        base = float(NETWORK_BACKOFF_BASE_SEC)
        cap = float(NETWORK_BACKOFF_CAP_SEC)
    exp = min(cap, base * (2 ** a))
    if retry_after_sec is not None:
        exp = max(exp, float(retry_after_sec))
    jitter = random.uniform(0.0, float(NETWORK_BACKOFF_JITTER_SEC))
    return float(exp + jitter)


def sleep_backoff(
    attempt: int,
    kind: NetworkErrorKind,
    *,
    retry_after_sec: Optional[float] = None,
) -> float:
    wait = compute_backoff_sec(attempt, kind, retry_after_sec=retry_after_sec)
    time.sleep(wait)
    return wait


def call_with_retry(
    fn: Callable[[], T],
    *,
    op: str = "network_call",
    max_attempts: Optional[int] = None,
    throttle_key: Optional[str] = None,
    throttle_interval_sec: float = 0.2,
    default: Any = None,
    swallow: bool = True,
) -> T:
    """
    Execute ``fn`` with typed retry. Never lets a retryable transport error kill the process.

    - retryable kinds → sleep (2/4/8 or 429 schedule) then retry
    - FATAL / exhausted → return ``default`` when ``swallow=True`` (daemon-safe)
    - ``swallow=False`` → re-raise FATAL as-is, or ``NetworkRetryExhausted`` when out of attempts
    """
    attempts = int(max_attempts if max_attempts is not None else NETWORK_RETRY_MAX_ATTEMPTS)
    attempts = max(1, attempts)
    last: ClassifiedNetworkError | None = None

    for i in range(attempts):
        try:
            if throttle_key:
                from bitget.rate_limit_guard import throttle

                throttle(throttle_key, throttle_interval_sec)
            return fn()
        except Exception as exc:  # noqa: BLE001 — classify then decide
            classified = classify_network_error(exc)
            last = classified
            if not classified.retryable:
                log_exception(
                    logger,
                    "[%s] fatal network error kind=%s status=%s: %s",
                    op,
                    classified.kind.value,
                    classified.status_code,
                    exc,
                )
                if swallow:
                    return default
                raise

            if i >= attempts - 1:
                break

            waited = sleep_backoff(
                i,
                classified.kind,
                retry_after_sec=classified.retry_after_sec,
            )
            logger.warning(
                "[%s] retryable %s attempt=%s/%s wait=%.2fs status=%s err=%s",
                op,
                classified.kind.value,
                i + 1,
                attempts,
                waited,
                classified.status_code,
                exc,
            )

    msg = f"{op} exhausted after {attempts} attempts"
    if last is not None:
        log_exception(
            logger,
            "[%s] exhausted kind=%s status=%s: %s",
            op,
            last.kind.value,
            last.status_code,
            last.exc,
        )
    else:
        logger.error("[%s] exhausted with no captured error", op)

    if swallow:
        return default
    raise NetworkRetryExhausted(msg, last_error=last)


def http_get(
    url: str,
    *,
    op: str = "http.get",
    throttle_key: Optional[str] = None,
    throttle_interval_sec: float = 0.35,
    timeout: float = 15.0,
    headers: Optional[dict] = None,
    params: Any = None,
    default: Any = None,
    swallow: bool = True,
    max_attempts: Optional[int] = None,
):
    """
    Daemon-safe HTTP GET with raise_for_status so 429/5xx enter typed retry.
    Returns ``requests.Response`` or ``default`` when swallowed.
    """
    import requests

    def _call():
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp

    return call_with_retry(
        _call,
        op=op,
        max_attempts=max_attempts,
        throttle_key=throttle_key,
        throttle_interval_sec=throttle_interval_sec,
        default=default,
        swallow=swallow,
    )
