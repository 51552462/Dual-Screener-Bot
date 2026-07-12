"""bitget.infra.logging_setup — production RotatingFileHandler SSOT."""
from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler

from bitget.infra import logging_setup as ls
from bitget.infra.memory_policy import (
    LOG_FORMAT,
    LOG_ROTATE_BACKUP_COUNT,
    LOG_ROTATE_MAX_BYTES,
)


def setup_function():
    ls._reset_logging_for_tests()
    os.environ["BITGET_DISABLE_OPS_SQLITE_LOG"] = "1"


def teardown_function():
    ls._reset_logging_for_tests()
    os.environ.pop("BITGET_DISABLE_OPS_SQLITE_LOG", None)
    os.environ.pop("BITGET_DISABLE_FILE_LOG", None)


def test_memory_policy_log_rotate_constants():
    assert LOG_ROTATE_MAX_BYTES == 50 * 1024 * 1024
    assert LOG_ROTATE_BACKUP_COUNT == 5
    assert LOG_FORMAT == "[%(asctime)s] [%(levelname)s] %(message)s"


def test_setup_logging_attaches_rotating_file_handler(tmp_path):
    ls.setup_logging(
        enable_ops_sqlite=False,
        enable_file=True,
        log_dir=str(tmp_path),
    )
    root = logging.getLogger()
    rotating = [h for h in root.handlers if isinstance(h, RotatingFileHandler)]
    assert len(rotating) == 1
    fh = rotating[0]
    assert fh.maxBytes == LOG_ROTATE_MAX_BYTES
    assert fh.backupCount == LOG_ROTATE_BACKUP_COUNT
    assert fh.formatter is not None
    assert fh.formatter._fmt == LOG_FORMAT

    log_path = tmp_path / "bitget.log"
    assert log_path.exists() or True  # created on first emit
    logger = ls.get_logger("bitget.test.logging")
    logger.info("hello production log")
    assert log_path.is_file()
    text = log_path.read_text(encoding="utf-8")
    assert "[INFO]" in text
    assert "hello production log" in text
    assert text.startswith("[")


def test_setup_logging_idempotent(tmp_path):
    ls.setup_logging(enable_ops_sqlite=False, enable_file=True, log_dir=str(tmp_path))
    n1 = len(logging.getLogger().handlers)
    ls.setup_logging(enable_ops_sqlite=False, enable_file=True, log_dir=str(tmp_path))
    n2 = len(logging.getLogger().handlers)
    assert n1 == n2


def test_log_exception_includes_traceback(tmp_path, caplog):
    ls.setup_logging(enable_ops_sqlite=False, enable_file=True, log_dir=str(tmp_path))
    logger = ls.get_logger("bitget.test.exc")
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        with caplog.at_level(logging.ERROR):
            ls.log_exception(logger, "failed critical path")
    assert any("failed critical path" in r.message for r in caplog.records)
    assert any(r.exc_info for r in caplog.records)


def test_stderr_handler_not_confused_with_file_handler(tmp_path):
    ls.setup_logging(enable_ops_sqlite=False, enable_file=True, log_dir=str(tmp_path))
    root = logging.getLogger()
    stderr_ok = ls._has_stderr_stream_handler(root)
    file_ok = ls._has_rotating_file_handler(root)
    assert stderr_ok is True
    assert file_ok is True
