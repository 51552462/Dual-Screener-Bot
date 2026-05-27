"""
KR/US 차별 TTL · KR T+1 Fast-Decay · US T+2 승률 재평가.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore


def _kst_now() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("Asia/Seoul"))
    return datetime.now()


def _kst_today() -> str:
    return _kst_now().strftime("%Y-%m-%d")


def default_ttl_days(market: str, sys_config: Optional[Dict[str, Any]] = None) -> int:
    m = str(market).upper()
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if m == "KR":
        try:
            return max(1, int(cfg.get("ACE_EVOLUTION_TTL_DAYS_KR", 1)))
        except (TypeError, ValueError):
            return 1
    try:
        return max(2, int(cfg.get("ACE_EVOLUTION_TTL_DAYS_US", 5)))
    except (TypeError, ValueError):
        return 5


def ttl_mode_for_market(market: str) -> str:
    return "fast_decay_kr" if str(market).upper() == "KR" else "slow_decay_us"


def expires_at_kst(playbook: Dict[str, Any], sys_config: Optional[Dict[str, Any]] = None) -> str:
    as_of = str(playbook.get("as_of_kst") or _kst_today())[:10]
    try:
        ttl = int(playbook.get("ttl_days", default_ttl_days(str(playbook.get("market", "KR")), sys_config)))
    except (TypeError, ValueError):
        ttl = 1
    try:
        d0 = datetime.strptime(as_of, "%Y-%m-%d")
    except ValueError:
        d0 = _kst_now().replace(tzinfo=None)
    exp = d0 + timedelta(days=ttl)
    return exp.strftime("%Y-%m-%d")


def is_playbook_expired(playbook: Dict[str, Any], sys_config: Optional[Dict[str, Any]] = None) -> bool:
    if not isinstance(playbook, dict) or not playbook.get("as_of_kst"):
        return True
    exp = expires_at_kst(playbook, sys_config)
    return _kst_today() > exp


def evaluate_fast_decay_kr(
    *,
    t1_win_rate_pct: Optional[float],
    t1_n: int,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    """
    KR: 익일(T+1) 스캔 직후 승률 저조 시 즉시 가중치 폐기.
    Returns (should_revoke, reason).
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        min_n = int(cfg.get("ACE_EVOLUTION_KR_FAST_DECAY_MIN_N", 3))
    except (TypeError, ValueError):
        min_n = 3
    try:
        wr_floor = float(cfg.get("ACE_EVOLUTION_KR_FAST_DECAY_WR_PCT", 40.0))
    except (TypeError, ValueError):
        wr_floor = 40.0

    if t1_n < min_n:
        return False, f"KR fast-decay 보류 (T+1 표본 {t1_n}<{min_n})"
    if t1_win_rate_pct is None:
        return False, "KR fast-decay 보류 (승률 산출 불가)"
    if float(t1_win_rate_pct) < wr_floor:
        return True, f"KR T+1 승률 {t1_win_rate_pct:.1f}% < {wr_floor:.0f}% — 즉시 폐기"
    return False, f"KR T+1 승률 {t1_win_rate_pct:.1f}% 유지"


def evaluate_slow_decay_us(
    *,
    t2_win_rate_pct: Optional[float],
    t2_n: int,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    """US: T+2 재평가 — 저조 시 observe_only 전환 또는 폐기."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        min_n = int(cfg.get("ACE_EVOLUTION_US_DECAY_MIN_N", 5))
    except (TypeError, ValueError):
        min_n = 5
    try:
        wr_floor = float(cfg.get("ACE_EVOLUTION_US_DECAY_WR_PCT", 45.0))
    except (TypeError, ValueError):
        wr_floor = 45.0

    if t2_n < min_n:
        return False, f"US decay 보류 (T+2 표본 {t2_n}<{min_n})"
    if t2_win_rate_pct is None:
        return False, "US decay 보류 (승률 산출 불가)"
    if float(t2_win_rate_pct) < wr_floor:
        return True, f"US T+2 승률 {t2_win_rate_pct:.1f}% < {wr_floor:.0f}% — 가중치 축소"
    return False, f"US T+2 승률 {t2_win_rate_pct:.1f}% 유지"


def apply_ttl_to_playbook(
    playbook: Dict[str, Any],
    *,
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = dict(playbook)
    out["ttl_days"] = default_ttl_days(market, sys_config)
    out["ttl_mode"] = ttl_mode_for_market(market)
    out["expires_at_kst"] = expires_at_kst(out, sys_config)
    return out
