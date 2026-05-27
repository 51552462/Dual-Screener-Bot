"""
Bounded waits for factory-critical network / market data calls.
yfinance and FinanceDataReader do not accept timeout= — wrap in thread pool.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Any, Callable, TypeVar

T = TypeVar("T")

_DEFAULT_SEC = 30.0


def default_network_timeout_sec() -> float:
    raw = (os.environ.get("FACTORY_HTTP_TIMEOUT_SEC") or "").strip()
    if not raw:
        raw = (os.environ.get("FACTORY_NETWORK_TIMEOUT_SEC") or "30").strip()
    try:
        return max(5.0, min(300.0, float(raw)))
    except ValueError:
        return _DEFAULT_SEC


def run_with_timeout(
    fn: Callable[..., T],
    *args: Any,
    timeout_sec: float | None = None,
    **kwargs: Any,
) -> T:
    limit = timeout_sec if timeout_sec is not None else default_network_timeout_sec()
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="net_timeout") as pool:
        fut = pool.submit(fn, *args, **kwargs)
        try:
            return fut.result(timeout=limit)
        except FuturesTimeoutError as ex:
            raise TimeoutError(
                f"{getattr(fn, '__name__', fn)!r} exceeded {limit:.0f}s"
            ) from ex


def fdr_data_reader(
    code: str,
    start: str,
    end: str | None = None,
    *,
    timeout_sec: float | None = None,
):
    import FinanceDataReader as fdr

    if end is not None:
        return run_with_timeout(
            fdr.DataReader, code, start, end, timeout_sec=timeout_sec
        )
    return run_with_timeout(fdr.DataReader, code, start, timeout_sec=timeout_sec)


def yf_download(*args: Any, timeout_sec: float | None = None, **kwargs: Any):
    import yfinance as yf

    kwargs.pop("timeout", None)
    return run_with_timeout(
        yf.download, *args, timeout_sec=timeout_sec, **kwargs
    )
