"""
PIL — ZOMBIE 연속 일수 추적 · N일 시 RETIRED 강제.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

logger = logging.getLogger(__name__)

_KST = pytz.timezone("Asia/Seoul")


def _today_kst() -> str:
    return datetime.now(_KST).strftime("%Y-%m-%d")


def _yesterday_kst() -> str:
    return (datetime.now(_KST).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def zombie_retire_days_for_market(
    market: str,
    sys_config: Optional[dict] = None,
) -> int:
    mk = str(market or "KR").upper()
    if isinstance(sys_config, dict):
        raw = sys_config.get("PIL_ZOMBIE_RETIRE_DAYS")
        if isinstance(raw, dict) and mk in raw:
            try:
                return max(1, int(raw[mk]))
            except (TypeError, ValueError):
                pass
        legacy = sys_config.get(f"PIL_ZOMBIE_RETIRE_DAYS_{mk}")
        if legacy is not None:
            try:
                return max(1, int(legacy))
            except (TypeError, ValueError):
                pass
    defaults = {"KR": 14, "US": 21, "BG": 14, "BG_SPOT": 14, "BG_FUT": 21}
    return int(defaults.get(mk, defaults.get("BG", 10)))


def streak_key(market: str, group_key: str) -> str:
    return f"{str(market or 'KR').upper()}|{str(group_key or '').strip()}"


def update_zombie_streaks(
    entries: List[Dict[str, Any]],
    prior_streaks: Optional[Dict[str, Any]],
    *,
    sys_config: Optional[dict] = None,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    entries: [{market, group_key, is_zombie, vitality_score}, ...]
    Returns (updated_streaks, force_retire_records).
    """
    today = _today_kst()
    yesterday = _yesterday_kst()
    streaks: Dict[str, Any] = dict(prior_streaks) if isinstance(prior_streaks, dict) else {}
    force_retire: List[Dict[str, Any]] = []
    seen_keys = set()

    for ent in entries:
        mk = str(ent.get("market") or "KR").upper()
        gk = str(ent.get("group_key") or "").strip()
        if not gk:
            continue
        sk = streak_key(mk, gk)
        seen_keys.add(sk)
        is_zombie = bool(ent.get("is_zombie"))
        need_days = zombie_retire_days_for_market(mk, sys_config)

        if not is_zombie:
            if sk in streaks:
                del streaks[sk]
            continue

        rec = dict(streaks.get(sk) or {})
        last = str(rec.get("last_zombie_date") or "")
        if last == yesterday:
            streak = int(rec.get("streak_days", 0) or 0) + 1
        elif last == today:
            streak = int(rec.get("streak_days", 1) or 1)
        else:
            streak = 1

        rec.update(
            {
                "market": mk,
                "group_key": gk,
                "streak_days": streak,
                "last_zombie_date": today,
                "since": rec.get("since") or today,
                "retire_after_days": need_days,
                "vitality_score": ent.get("vitality_score"),
            }
        )
        streaks[sk] = rec

        if streak >= need_days:
            force_retire.append(
                {
                    "market": mk,
                    "group_key": gk,
                    "streak_days": streak,
                    "reason": f"PIL_ZOMBIE_STREAK_{streak}D",
                    "as_of": today,
                }
            )
            logger.warning(
                "PIL force RETIRED: %s %s streak=%d need=%d",
                mk,
                gk,
                streak,
                need_days,
            )

    return streaks, force_retire
