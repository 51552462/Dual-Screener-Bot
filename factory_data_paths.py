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
