"""
팩토리 데이터 루트 단일 정의.

- 우선순위: 환경 변수 `DB_STORAGE_PATH` → (선택) `system_config.json` 의 키 `DB_STORAGE_PATH`
  → 레거시 `~/dante_bots/Dual-Screener-Bot`

- JSON 조회 경로: `SYSTEM_CONFIG_PATH` 가 있으면 그 파일만, 없으면 레거시 디렉터리의
  `system_config.json` (config_manager 와 동일한 부트스트랩 규칙).

운영에서 데이터 루트를 바꿀 때는 기존 `*.sqlite` 및 `system_config*.json` 을 새 경로로
복사한 뒤 서비스를 재기동한다.
"""
from __future__ import annotations

import json
import os


def _legacy_factory_dir() -> str:
    return os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot")


def _read_db_storage_from_json(path: str) -> str:
    if not path or not os.path.isfile(path):
        return ""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return ""
        v = data.get("DB_STORAGE_PATH")
        return str(v).strip() if v is not None else ""
    except Exception:
        return ""


def factory_data_dir() -> str:
    raw = (os.environ.get("DB_STORAGE_PATH") or "").strip()
    if raw:
        p = os.path.abspath(os.path.expanduser(raw))
        os.makedirs(p, exist_ok=True)
        return p

    legacy = _legacy_factory_dir()
    cfg_candidates: list[str] = []
    sc = (os.environ.get("SYSTEM_CONFIG_PATH") or "").strip()
    if sc:
        cfg_candidates.append(os.path.abspath(os.path.expanduser(sc)))
    cfg_candidates.append(os.path.join(legacy, "system_config.json"))

    for cp in cfg_candidates:
        sub = _read_db_storage_from_json(cp)
        if sub:
            p = os.path.abspath(os.path.expanduser(sub))
            os.makedirs(p, exist_ok=True)
            return p

    return legacy


def install_root() -> str:
    """코드·기본 meta_governor_state.json 위치 (저장소 루트)."""
    raw = (os.environ.get("INSTALL_ROOT") or "").strip()
    if raw:
        return os.path.abspath(os.path.expanduser(raw))
    return os.path.dirname(os.path.abspath(__file__))


def flow_csv_path() -> str:
    """K-Means / 감사관 마스터 플로우 CSV (데이터 루트)."""
    return os.path.join(factory_data_dir(), "Supernova_Flow_Tracking_Master.csv")


def meta_governor_state_path() -> str:
    """MetaGovernor 동적 상태 JSON (환경변수로 덮어쓰기 가능)."""
    env = (os.environ.get("META_GOVERNOR_STATE_PATH") or "").strip()
    if env:
        return os.path.abspath(os.path.expanduser(env))
    return os.path.join(install_root(), "meta_governor_state.json")


def system_config_json_path() -> str:
    sc = (os.environ.get("SYSTEM_CONFIG_PATH") or "").strip()
    if sc:
        return os.path.abspath(os.path.expanduser(sc))
    return os.path.join(factory_data_dir(), "system_config.json")


def validated_live_mutants_path() -> str:
    return os.path.join(install_root(), "validated_live_mutants.json")


def market_data_db_path() -> str:
    """OHLCV + forward_trades SSOT (`market_db_paths.MARKET_DATA_DB_PATH`)."""
    from market_db_paths import MARKET_DATA_DB_PATH

    return MARKET_DATA_DB_PATH


def short_data_db_path() -> str:
    """blackhole_hunter 숏 후보 DB."""
    return os.path.join(factory_data_dir(), "short_data.sqlite")


def alt_data_db_path() -> str:
    return os.path.join(factory_data_dir(), "alt_data.sqlite")
