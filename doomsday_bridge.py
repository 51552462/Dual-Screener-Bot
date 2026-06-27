"""
Doomsday bridge — macro_doomsday_bot JSON → system_config DOOMSDAY_DEFCON SSOT.

- 일일 리포트 [0] 배너용 필드 동기화
- DEFCON 격상 시 비동기 텔레그램 긴급 알림 (일일 리포트 대기 없음)
- INVERSE_MODE 연동 + 인버스 스나이퍼 1사이클(옵션)
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Optional

from config_manager import load_system_config, update_system_config

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
DOOMSDAY_STATUS_JSON = os.path.join(_THIS_DIR, "doomsday_status.json")

# level 1=최악 … 5=안전 (supernova/autopilot: level <= 2 → 롱 차단)
_ALERT_COOLDOWN_SEC = 4 * 3600


def defcon_level_from_payload(payload: dict[str, Any]) -> int:
    """macro_doomsday_bot regime·점수 → DEFCON 1–5."""
    regime = str(payload.get("regime") or "").upper()
    scores = payload.get("scores") if isinstance(payload.get("scores"), dict) else {}
    try:
        g = float(scores.get("Global_Contagion_Score") or 0.0)
    except (TypeError, ValueError):
        g = 0.0
    try:
        kr = float(scores.get("KR_Doom_Score") or 0.0)
    except (TypeError, ValueError):
        kr = 0.0

    if regime == "DOOMSDAY" or g >= 70.0:
        return 1 if g >= 82.0 else 2
    if regime == "DEFENSIVE_KR" or (41.0 <= kr <= 70.0 and g > 40.0):
        return 3
    if regime == "BULL" or g <= 40.0:
        return 5
    return 4


def load_doomsday_status_file(path: Optional[str] = None) -> Optional[dict[str, Any]]:
    p = path or DOOMSDAY_STATUS_JSON
    if not os.path.isfile(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def build_defcon_block(payload: dict[str, Any], level: int) -> dict[str, Any]:
    scores = payload.get("scores") if isinstance(payload.get("scores"), dict) else {}
    return {
        "level": int(level),
        "regime": str(payload.get("regime") or ""),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "source": "doomsday_status.json",
        "generated_at_utc": payload.get("generated_at_utc"),
        "scores": scores,
        "z_scores_latest": payload.get("z_scores_latest") or {},
        "data_last_date": payload.get("data_last_date"),
    }


def _alert_state(cfg: dict[str, Any]) -> dict[str, Any]:
    st = cfg.get("DOOMSDAY_ALERT_STATE")
    return dict(st) if isinstance(st, dict) else {}


def _should_send_escalation_alert(old_level: int, new_level: int, st: dict[str, Any]) -> bool:
    """격상 = 숫자 감소(더 위험)."""
    if new_level >= old_level:
        return False
    if new_level <= 2:
        return True
    try:
        last_at = st.get("last_alert_at_utc") or ""
        last_lvl = int(st.get("last_alert_level", 99))
        if last_lvl == new_level and last_at:
            t0 = datetime.fromisoformat(str(last_at).replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - t0).total_seconds() < _ALERT_COOLDOWN_SEC:
                return False
    except (TypeError, ValueError):
        pass
    return True


def _format_escalation_html(old_level: int, new_level: int, block: dict[str, Any]) -> str:
    scores = block.get("scores") or {}
    g = scores.get("Global_Contagion_Score", "—")
    kr = scores.get("KR_Doom_Score", "—")
    regime = block.get("regime") or "—"
    urgent = "🚨 <b>[긴급] 둠스데이 발동</b>" if new_level <= 2 else "⚠️ <b>[경보] 둠스데이 DEFCON 격상</b>"
    return (
        f"{urgent}\n"
        f"DEFCON <b>{old_level}</b> → <b>{new_level}</b> | 레짐 <code>{regime}</code>\n"
        f"Global={g} · KR={kr}\n"
        f"<i>장중 실시간 — 일일 리포트 대기 없음</i>"
    )


def _send_telegram_async(html: str) -> None:
    def _go() -> None:
        try:
            from auto_forward_tester import send_telegram_msg

            send_telegram_msg(html)
        except Exception as ex:
            print(f"⚠️ [doomsday_bridge] 텔레그램 발송 실패: {ex}")

    threading.Thread(target=_go, daemon=True).start()


def maybe_send_defcon_escalation_alert(
    old_level: int,
    new_level: int,
    defcon_block: dict[str, Any],
    *,
    cfg: Optional[dict[str, Any]] = None,
) -> bool:
    if cfg is None:
        cfg = load_system_config()
    st = _alert_state(cfg)
    if not _should_send_escalation_alert(old_level, new_level, st):
        return False
    _send_telegram_async(_format_escalation_html(old_level, new_level, defcon_block))
    update_system_config(
        {
            "DOOMSDAY_ALERT_STATE": {
                "last_level": new_level,
                "last_alert_level": new_level,
                "last_alert_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        }
    )
    return True


def _apply_inverse_mode_and_cycle(cfg: dict[str, Any], *, run_inverse_cycle: bool) -> dict[str, Any]:
    """DOOMSDAY_DEFCON → INVERSE_MODE_ACTIVE + (옵션) 스나이퍼 1사이클."""
    from system_auto_pilot import _sync_inverse_mode_switch

    vix_last = 0.0
    regime_disp = ""
    meta = cfg.get("REGIME_ANALYSIS") or {}
    if isinstance(meta, dict):
        regime_disp = str(meta.get("regime_display") or meta.get("regime_key") or "")
    macro_row = cfg.get("MACRO_DAILY_LATEST") or {}
    if isinstance(macro_row, dict):
        try:
            vix_last = float(macro_row.get("vix_index") or 0.0)
        except (TypeError, ValueError):
            vix_last = 0.0

    _sync_inverse_mode_switch(cfg, vix_last, regime_disp)
    summary: dict[str, Any] = {"inverse_mode": bool(cfg.get("INVERSE_MODE_ACTIVE"))}
    if run_inverse_cycle and summary["inverse_mode"]:
        try:
            from inverse_etf_sniper import run_inverse_etf_sniper_cycle

            summary["cycle"] = run_inverse_etf_sniper_cycle()
        except Exception as ex:
            summary["cycle"] = {"skipped": f"cycle_error: {ex}"}
    cfg["INVERSE_LAST_CYCLE_SUMMARY"] = {
        "at": datetime.now(timezone.utc).isoformat(),
        **summary,
    }
    return summary


def sync_doomsday_to_system_config(
    *,
    payload: Optional[dict[str, Any]] = None,
    alert_on_escalation: bool = True,
    run_inverse_cycle: bool = False,
    save: bool = True,
) -> dict[str, Any]:
    """
    doomsday_status.json(또는 payload) → DOOMSDAY_DEFCON·DOOMSDAY_RADAR_SSOT.
    반환: {old_level, new_level, payload, alerted, inverse_summary}
    """
    if payload is None:
        payload = load_doomsday_status_file()
    if not payload:
        return {"ok": False, "reason": "no_status_file"}

    cfg = load_system_config()
    old_dd = cfg.get("DOOMSDAY_DEFCON") or {}
    try:
        old_level = int(old_dd.get("level", 5))
    except (TypeError, ValueError):
        old_level = 5

    new_level = defcon_level_from_payload(payload)
    block = build_defcon_block(payload, new_level)
    radar = {
        "regime": block.get("regime"),
        "scores": block.get("scores"),
        "z_scores_latest": block.get("z_scores_latest"),
        "synced_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    updates: dict[str, Any] = {
        "DOOMSDAY_DEFCON": block,
        "DOOMSDAY_RADAR_SSOT": radar,
    }
    cfg.update(updates)

    # [진화형 형상변환 감쇠] 오늘의 GlobalScore·γ·Multiplier 브레이크 스냅샷 일일 기록.
    try:
        from doomsday_dampener import (
            global_score_from_config,
            record_brake_event_into,
        )

        _gs = global_score_from_config(cfg)
        _dampen_state = record_brake_event_into(cfg, global_score=_gs)
        updates["DOOMSDAY_DAMPEN_STATE"] = _dampen_state
        cfg["DOOMSDAY_DAMPEN_STATE"] = _dampen_state
    except Exception as _dmp_ex:
        print(f"⚠️ [doomsday_dampener] brake-log skip: {_dmp_ex}")

    inverse_summary = {}
    if run_inverse_cycle or new_level <= 3:
        inverse_summary = _apply_inverse_mode_and_cycle(cfg, run_inverse_cycle=run_inverse_cycle)
        updates["INVERSE_MODE_ACTIVE"] = cfg.get("INVERSE_MODE_ACTIVE")
        updates["INVERSE_LAST_CYCLE_SUMMARY"] = cfg.get("INVERSE_LAST_CYCLE_SUMMARY")

    alerted = False
    if alert_on_escalation and new_level < old_level:
        alerted = maybe_send_defcon_escalation_alert(old_level, new_level, block, cfg=cfg)

    if save:
        update_system_config(updates)

    return {
        "ok": True,
        "old_level": old_level,
        "new_level": new_level,
        "alerted": alerted,
        "inverse_summary": inverse_summary,
        "regime": block.get("regime"),
    }


def refresh_doomsday_from_file(
    *,
    alert_on_escalation: bool = True,
    run_inverse_cycle: bool = False,
) -> dict[str, Any]:
    """스캔·팩토리 경로: JSON만 읽고 동기화(경량)."""
    return sync_doomsday_to_system_config(
        alert_on_escalation=alert_on_escalation,
        run_inverse_cycle=run_inverse_cycle,
        save=True,
    )


def ingest_doomsday_status_file(
    path: Optional[str] = None,
    *,
    alert_on_escalation: bool = True,
    run_inverse_cycle: bool = True,
) -> dict[str, Any]:
    """macro_doomsday_bot main() 저장 직후 호출."""
    payload = load_doomsday_status_file(path)
    if not payload:
        return {"ok": False, "reason": "no_file"}
    return sync_doomsday_to_system_config(
        payload=payload,
        alert_on_escalation=alert_on_escalation,
        run_inverse_cycle=run_inverse_cycle,
        save=True,
    )
