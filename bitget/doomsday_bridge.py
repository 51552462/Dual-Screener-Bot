"""
Doomsday DEFCON → Bitget config_kv SSOT (주식 doomsday_bridge 패턴).

`doomsday_radar` step 이 config 에 DOOMSDAY_DEFCON 을 쓴 뒤,
리포트·게이트가 읽을 JSON 미러를 bitget data dir 에 동기화한다.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict

from bitget.infra.clock import utc_now_iso
from bitget.infra.data_paths import bitget_data_dir
from bitget.infra.logging_setup import get_logger

logger = get_logger("bitget.doomsday_bridge")


def _status_json_path() -> str:
    return os.path.join(bitget_data_dir(), "bitget_doomsday_status.json")


def sync_doomsday_to_bitget_config() -> Dict[str, Any]:
    from bitget.infra import config_manager

    cfg = config_manager.load_system_config() or {}
    dd = cfg.get("DOOMSDAY_DEFCON")
    if not isinstance(dd, dict) or dd.get("level") is None:
        try:
            from bitget.doomsday_bot import run_doomsday_radar

            run_doomsday_radar()
            cfg = config_manager.load_system_config() or {}
            dd = cfg.get("DOOMSDAY_DEFCON") or {}
        except Exception as ex:
            return {"ok": False, "error": str(ex)}

    payload = {
        "regime": "DOOMSDAY" if int(dd.get("level") or 5) <= 2 else "NORMAL",
        "defcon_level": int(dd.get("level") or 5),
        "updated_at": dd.get("updated_at") or utc_now_iso(),
        "signals": dd.get("signals") if isinstance(dd.get("signals"), dict) else {},
        "metrics": dd.get("metrics") if isinstance(dd.get("metrics"), dict) else {},
        "source": "bitget_doomsday_bridge",
    }
    path = _status_json_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    config_manager.set_config_value("DOOMSDAY_DEFCON", dd)
    out = {"ok": True, "level": payload["defcon_level"], "path": path}
    logger.info("doomsday_bridge: %s", out)
    return out
