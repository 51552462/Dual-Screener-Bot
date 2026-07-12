"""
Bitget heavy-cycle RAM return — `mtf_data_updater` / 주식 `time_machine` 패턴 SSOT.

무거운 DataFrame·GMM·백테스트·대규모 스캔 배치가 끝날 때마다 호출해
4GB RAM 서버에서 peak RSS 를 낮춘다.

사용:
    del df
    flush_gc("ohlcv_batch")

    with heavy_data_cycle("gmm_fit"):
        ...
"""
from __future__ import annotations

import gc
import logging
from contextlib import contextmanager
from typing import Iterator, Optional

logger = logging.getLogger(__name__)


def flush_gc(*, label: Optional[str] = None) -> int:
    """사이클 종료 후 gc.collect() — mtf_data_updater.save_ohlcv 동일 패턴."""
    n = gc.collect()
    if label:
        logger.debug("bitget gc_cycle %s collected=%s unreachable", label, n)
    return n


@contextmanager
def heavy_data_cycle(label: str = "") -> Iterator[None]:
    """with 블록 종료 시 자동 flush (백테스트 구간·GMM·cluster mining)."""
    try:
        yield
    finally:
        flush_gc(label=label or "heavy_data_cycle")
