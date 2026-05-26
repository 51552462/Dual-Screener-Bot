"""
market_data.sqlite 경로 단일 정의 + 읽기 복제본(market_data_snapshot.sqlite) 선택.
무거운 읽기 전용 워크로드는 스냅샷이 있으면 스냅샷을 사용한다.

데이터 루트: `factory_data_paths.factory_data_dir()` (`DB_STORAGE_PATH` 또는 레거시 홈 경로).

스냅샷이 오래되었으면(기본 30분) 메인 DB로 폴백 — CQRS 고착으로 리포트 날짜가 멈추는 문제 방지.
환경변수 `MARKET_SNAPSHOT_MAX_STALE_SEC` 로 임계(초) 조정. `0` 또는 음수면 mtime 검사 생략(항상 스냅샷 우선).
"""
from __future__ import annotations

import os
import time

from factory_data_paths import factory_data_dir

_DATA = factory_data_dir()
MARKET_DATA_DB_PATH = os.path.join(_DATA, "market_data.sqlite")
MARKET_DATA_SNAPSHOT_PATH = os.path.join(_DATA, "market_data_snapshot.sqlite")


def _snapshot_max_stale_seconds() -> float:
    raw = (os.environ.get("MARKET_SNAPSHOT_MAX_STALE_SEC") or "").strip()
    if not raw:
        return 1800.0  # 30분
    try:
        return float(raw)
    except ValueError:
        return 1800.0


def market_db_read_path() -> str:
    """
    스냅샷 파일이 있으면 읽기 부하를 거기로 분산, 없으면 메인 DB.
    스냅샷 mtime 이 임계보다 오래되었으면 메인 DB (스냅샷 갱신 실패·고착 방지).
    """
    if not os.path.isfile(MARKET_DATA_SNAPSHOT_PATH):
        return MARKET_DATA_DB_PATH
    max_age = _snapshot_max_stale_seconds()
    if max_age <= 0:
        return MARKET_DATA_SNAPSHOT_PATH
    try:
        age = time.time() - os.path.getmtime(MARKET_DATA_SNAPSHOT_PATH)
    except OSError:
        return MARKET_DATA_DB_PATH
    if age > max_age:
        return MARKET_DATA_DB_PATH
    return MARKET_DATA_SNAPSHOT_PATH


def report_db_read_path() -> str:
    """
    리포트·딥다이브·듀얼트랙·최우수 성적표 — 항상 메인 DB 우선.
    스냅샷 mtime 신선도 착시로 청산 워터마크가 멈춘 채 읽는 문제 방지.
    """
    force = str(os.environ.get("REPORT_DEEP_DIVE_FORCE_MAIN_DB", "1")).strip().lower()
    if force in ("1", "true", "yes", "on"):
        return MARKET_DATA_DB_PATH
    path = market_db_read_path()
    if path == MARKET_DATA_SNAPSHOT_PATH and os.path.isfile(MARKET_DATA_DB_PATH):
        return MARKET_DATA_DB_PATH
    return path


def report_read_source_label(path: str) -> str:
    if os.path.normpath(path) == os.path.normpath(MARKET_DATA_DB_PATH):
        return "MAIN"
    if os.path.normpath(path) == os.path.normpath(MARKET_DATA_SNAPSHOT_PATH):
        return "SNAPSHOT"
    return "OTHER"
