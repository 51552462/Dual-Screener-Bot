"""Bitget regime deep archive — shared regime_memory worker wrapper."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def run_bitget_regime_deep_archive(*, max_tasks: int = 20) -> Dict[str, Any]:
    try:
        from regime_memory import run_deep_archive_worker

        out = run_deep_archive_worker(max_tasks=max_tasks)
        logger.info("bitget regime_deep_archive: %s", out)
        return out if isinstance(out, dict) else {"processed": out}
    except Exception as ex:
        logger.warning("bitget regime_deep_archive skip: %s", ex)
        return {"ok": False, "error": str(ex)}
