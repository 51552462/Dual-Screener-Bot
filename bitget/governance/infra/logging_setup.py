<<<<<<< HEAD
"""
Bitget 전용 로깅 설정.

- stderr StreamHandler (기본)
- 선택적 SQLite ops handler (`bitget.infra.ops_logger`)
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Optional

_CONFIGURED = False


def setup_logging(
    *,
    level: Optional[int] = None,
    default_component: str = "bitget",
    enable_ops_sqlite: bool = True,
) -> None:
    """루트 로거 1회 설정. 중복 호출 시 무시."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    raw = (os.environ.get("BITGET_LOG_LEVEL") or "INFO").strip().upper()
    lvl = level if level is not None else getattr(logging, raw, logging.INFO)

    root = logging.getLogger()
    root.setLevel(lvl)

    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(lvl)
        sh.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        root.addHandler(sh)

    if enable_ops_sqlite and os.environ.get("BITGET_DISABLE_OPS_SQLITE_LOG", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        try:
            from bitget.infra import ops_logger

            ops_logger.configure_root_ops_logging(
                default_component=default_component,
                level=lvl,
            )
        except Exception:
            pass

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """모듈별 logger. setup_logging() 미호출 시에도 StreamHandler만 보장."""
    if not _CONFIGURED:
        setup_logging(enable_ops_sqlite=False)
    return logging.getLogger(name)
=======
"""
Bitget 전용 로깅 설정.

- stderr StreamHandler (기본)
- 선택적 SQLite ops handler (`bitget.infra.ops_logger`)
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Optional

_CONFIGURED = False


def setup_logging(
    *,
    level: Optional[int] = None,
    default_component: str = "bitget",
    enable_ops_sqlite: bool = True,
) -> None:
    """루트 로거 1회 설정. 중복 호출 시 무시."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    raw = (os.environ.get("BITGET_LOG_LEVEL") or "INFO").strip().upper()
    lvl = level if level is not None else getattr(logging, raw, logging.INFO)

    root = logging.getLogger()
    root.setLevel(lvl)

    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(lvl)
        sh.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        root.addHandler(sh)

    if enable_ops_sqlite and os.environ.get("BITGET_DISABLE_OPS_SQLITE_LOG", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        try:
            from bitget.infra import ops_logger

            ops_logger.configure_root_ops_logging(
                default_component=default_component,
                level=lvl,
            )
        except Exception:
            pass

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """모듈별 logger. setup_logging() 미호출 시에도 StreamHandler만 보장."""
    if not _CONFIGURED:
        setup_logging(enable_ops_sqlite=False)
    return logging.getLogger(name)
>>>>>>> a6f17ca59385c6492c35a2f0368a732550fef092
