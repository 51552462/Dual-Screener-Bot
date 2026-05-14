"""
market_data.sqlite 경로 단일 정의 + 읽기 복제본(market_data_snapshot.sqlite) 선택.
무거운 읽기 전용 워크로드는 스냅샷이 있으면 스냅샷을 사용한다.

데이터 루트: `factory_data_paths.factory_data_dir()` (`DB_STORAGE_PATH` 또는 레거시 홈 경로).
"""
from __future__ import annotations

import os

from factory_data_paths import factory_data_dir

_DATA = factory_data_dir()
MARKET_DATA_DB_PATH = os.path.join(_DATA, "market_data.sqlite")
MARKET_DATA_SNAPSHOT_PATH = os.path.join(_DATA, "market_data_snapshot.sqlite")


def market_db_read_path() -> str:
    """스냅샷 파일이 있으면 읽기 부하를 거기로 분산, 없으면 메인 DB."""
    if os.path.isfile(MARKET_DATA_SNAPSHOT_PATH):
        return MARKET_DATA_SNAPSHOT_PATH
    return MARKET_DATA_DB_PATH
