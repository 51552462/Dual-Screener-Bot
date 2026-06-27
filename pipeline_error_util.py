"""
파이프라인 공통 — 침묵 예외 방지용 로깅 헬퍼.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def log_pipeline_exception(
    context: str,
    exc: BaseException,
    *,
    level: int = logging.ERROR,
    component: str = "factory_pipeline",
) -> None:
    """except pass 대체 — 상위 run_step이 PARTIAL_FAIL로 집계할 수 있게 기록."""
    logger.log(level, "[%s] %s: %s", component, context, exc, exc_info=True)
    try:
        import ops_logger

        ops_logger.insert_ops_event(
            component=component,
            severity="ERROR" if level >= logging.ERROR else "WARN",
            event="pipeline.exception",
            payload={"context": context, "error": str(exc)[:500]},
        )
    except Exception as ops_ex:
        logger.debug("ops_logger skip: %s", ops_ex)
