import logging
import os
from logging.handlers import RotatingFileHandler


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_FILE = os.path.join(LOG_DIR, "bitget_factory.log")
_MAX_BYTES = 10 * 1024 * 1024
_BACKUP_COUNT = 5
_INITIALIZED = False


def setup_logging(level=logging.INFO):
    global _INITIALIZED
    if _INITIALIZED:
        return logging.getLogger("bitget")

    os.makedirs(LOG_DIR, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level)

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(fmt)
        root.addHandler(stream_handler)

    _INITIALIZED = True
    return logging.getLogger("bitget")


def get_logger(name="bitget"):
    if not _INITIALIZED:
        setup_logging()
    return logging.getLogger(name)
