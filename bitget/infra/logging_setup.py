"""
Bitget production logging SSOT.

Handlers (root, once):
  - stderr StreamHandler
  - RotatingFileHandler → ``logs_dir()/bitget.log`` (50MB × 5 backups)
  - optional SQLite ops handler (`bitget.infra.ops_logger`)

Format (file + stderr): ``[%(asctime)s] [%(levelname)s] %(message)s``

Env:
  BITGET_LOG_LEVEL          — default INFO
  BITGET_LOG_DIR            — file log directory (via data_paths.logs_dir)
  BITGET_DISABLE_FILE_LOG   — 1/true → skip RotatingFileHandler (tests)
  BITGET_DISABLE_OPS_SQLITE_LOG — 1/true → skip ops SQLite handler
"""
from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional

from bitget.infra.memory_policy import (
    LOG_DATEFMT,
    LOG_FILE_NAME,
    LOG_FORMAT,
    LOG_ROTATE_BACKUP_COUNT,
    LOG_ROTATE_MAX_BYTES,
)

_CONFIGURED = False


def _truthy_env(name: str) -> bool:
    return str(os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def _make_formatter() -> logging.Formatter:
    return logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATEFMT)


def _has_stderr_stream_handler(root: logging.Logger) -> bool:
    """FileHandler subclasses StreamHandler — must not treat file handlers as stderr."""
    for h in root.handlers:
        if type(h) is logging.StreamHandler and getattr(h, "stream", None) in (
            sys.stderr,
            sys.stdout,
        ):
            return True
    return False


def _has_rotating_file_handler(root: logging.Logger) -> bool:
    return any(isinstance(h, RotatingFileHandler) for h in root.handlers)


def setup_logging(
    *,
    level: Optional[int] = None,
    default_component: str = "bitget",
    enable_ops_sqlite: bool = True,
    enable_file: Optional[bool] = None,
    log_dir: Optional[str] = None,
) -> None:
    """루트 로거 1회 설정. 중복 호출 시 무시."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    raw = (os.environ.get("BITGET_LOG_LEVEL") or "INFO").strip().upper()
    lvl = level if level is not None else getattr(logging, raw, logging.INFO)
    fmt = _make_formatter()

    root = logging.getLogger()
    root.setLevel(lvl)

    if not _has_stderr_stream_handler(root):
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(lvl)
        sh.setFormatter(fmt)
        root.addHandler(sh)

    use_file = (
        bool(enable_file)
        if enable_file is not None
        else not _truthy_env("BITGET_DISABLE_FILE_LOG")
    )
    if use_file and not _has_rotating_file_handler(root):
        try:
            from bitget.infra.data_paths import logs_dir

            directory = log_dir or logs_dir()
            os.makedirs(directory, exist_ok=True)
            path = os.path.join(directory, LOG_FILE_NAME)
            fh = RotatingFileHandler(
                path,
                maxBytes=LOG_ROTATE_MAX_BYTES,
                backupCount=LOG_ROTATE_BACKUP_COUNT,
                encoding="utf-8",
            )
            fh.setLevel(lvl)
            fh.setFormatter(fmt)
            root.addHandler(fh)
        except Exception:
            # File handler must never block process start — stderr remains.
            root.error("bitget rotating file log setup failed", exc_info=True)

    if enable_ops_sqlite and not _truthy_env("BITGET_DISABLE_OPS_SQLITE_LOG"):
        try:
            from bitget.infra import ops_logger

            ops_logger.configure_root_ops_logging(
                default_component=default_component,
                level=lvl,
            )
        except Exception:
            root.error("bitget ops sqlite log setup failed", exc_info=True)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """모듈별 logger. setup_logging() 미호출 시에도 StreamHandler(+file) 보장."""
    if not _CONFIGURED:
        setup_logging(enable_ops_sqlite=False)
    return logging.getLogger(name)


def log_exception(logger: logging.Logger, msg: str, *args) -> None:
    """Production error path — always include traceback."""
    logger.error(msg, *args, exc_info=True)


def _reset_logging_for_tests() -> None:
    """테스트 전용 — handlers 제거 후 setup_logging 재호출 가능."""
    global _CONFIGURED
    root = logging.getLogger()
    for h in list(root.handlers):
        try:
            h.close()
        except Exception:
            pass
        root.removeHandler(h)
    _CONFIGURED = False
