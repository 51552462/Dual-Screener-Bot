from __future__ import annotations

import json
import os
from datetime import datetime
from threading import Lock
from typing import Dict, Set

import pytz

from system_config_atomic import CONFIG_DIR

_CACHE_LOCK = Lock()


def _cache_path() -> str:
    return os.path.join(CONFIG_DIR, "daily_dispatched_tickers.json")


def _today_str_kst() -> str:
    return datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d")


def _load_payload() -> Dict[str, object]:
    path = _cache_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_payload(payload: Dict[str, object]) -> None:
    path = _cache_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _normalize_ticker(market: str, ticker: object) -> str:
    mk = str(market or "").upper()
    code = str(ticker or "").strip().upper()
    if mk == "KR" and code.isdigit():
        code = code.zfill(6)
    return code


def _today_bucket(payload: Dict[str, object]) -> Dict[str, Set[str]]:
    today = _today_str_kst()
    if str(payload.get("date")) != today:
        payload.clear()
        payload["date"] = today
    tickers = payload.get("tickers")
    if not isinstance(tickers, dict):
        tickers = {}
        payload["tickers"] = tickers
    out: Dict[str, Set[str]] = {}
    for mk in ("KR", "US"):
        vals = tickers.get(mk, [])
        if not isinstance(vals, list):
            vals = []
        out[mk] = {str(v).strip().upper() for v in vals if str(v).strip()}
    return out


def was_dispatched_today(market: str, ticker: object) -> bool:
    mk = str(market or "").upper()
    if mk not in ("KR", "US"):
        return False
    code = _normalize_ticker(mk, ticker)
    if not code:
        return False
    with _CACHE_LOCK:
        payload = _load_payload()
        bucket = _today_bucket(payload)
        return code in bucket.get(mk, set())


def mark_dispatched_today(market: str, ticker: object) -> None:
    mk = str(market or "").upper()
    if mk not in ("KR", "US"):
        return
    code = _normalize_ticker(mk, ticker)
    if not code:
        return
    with _CACHE_LOCK:
        payload = _load_payload()
        bucket = _today_bucket(payload)
        bucket[mk].add(code)
        payload["tickers"] = {
            "KR": sorted(bucket["KR"]),
            "US": sorted(bucket["US"]),
        }
        _save_payload(payload)


# ---------------------------------------------------------------------------
# scanned_today_cache.json — supernova 당일 스캔 이력 (프로세스 재시작·cron 간 영속)
# ---------------------------------------------------------------------------

_SCANNED_CACHE_LOCK = Lock()


def _scanned_cache_path() -> str:
    return os.path.join(CONFIG_DIR, "scanned_today_cache.json")


def _load_scanned_payload() -> Dict[str, object]:
    path = _scanned_cache_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_scanned_payload(payload: Dict[str, object]) -> None:
    path = _scanned_cache_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _scanned_today_bucket(payload: Dict[str, object]) -> Dict[str, Set[str]]:
    """KST 자정 rollover — date 필드 불일치 시 payload 초기화."""
    today = _today_str_kst()
    if str(payload.get("date")) != today:
        payload.clear()
        payload["date"] = today
    tickers = payload.get("tickers")
    if not isinstance(tickers, dict):
        tickers = {}
        payload["tickers"] = tickers
    out: Dict[str, Set[str]] = {}
    for mk in ("KR", "US"):
        vals = tickers.get(mk, [])
        if not isinstance(vals, list):
            vals = []
        out[mk] = {_normalize_ticker(mk, v) for v in vals if str(v).strip()}
    return out


def load_scanned_today_set(market: str) -> Set[str]:
    mk = str(market or "").upper()
    if mk not in ("KR", "US"):
        return set()
    with _SCANNED_CACHE_LOCK:
        payload = _load_scanned_payload()
        prior_date = payload.get("date")
        bucket = _scanned_today_bucket(payload)
        if str(prior_date) != str(payload.get("date")):
            _save_scanned_payload(payload)
        return set(bucket.get(mk, set()))


def was_scanned_today(market: str, ticker: object) -> bool:
    mk = str(market or "").upper()
    if mk not in ("KR", "US"):
        return False
    code = _normalize_ticker(mk, ticker)
    if not code:
        return False
    with _SCANNED_CACHE_LOCK:
        payload = _load_scanned_payload()
        prior_date = payload.get("date")
        bucket = _scanned_today_bucket(payload)
        if str(prior_date) != str(payload.get("date")):
            _save_scanned_payload(payload)
        return code in bucket.get(mk, set())


def mark_scanned_today(market: str, ticker: object) -> None:
    mk = str(market or "").upper()
    if mk not in ("KR", "US"):
        return
    code = _normalize_ticker(mk, ticker)
    if not code:
        return
    with _SCANNED_CACHE_LOCK:
        payload = _load_scanned_payload()
        bucket = _scanned_today_bucket(payload)
        bucket[mk].add(code)
        payload["tickers"] = {
            "KR": sorted(bucket["KR"]),
            "US": sorted(bucket["US"]),
        }
        _save_scanned_payload(payload)
