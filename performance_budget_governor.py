"""
성과예산 거버너(Performance Budget Governor) — 누적 드로다운 예산 소진율 기반
선제적 켈리/포지션 제어 SSOT.

00_마스터_로드맵.md 절대원칙: MDD 10% = 협상 불가 하드 캡.
HWM 대비 현재 낙폭을 그 예산(기본 10%)의 소진율로 환산하고, 소진율 구간별
액션(KELLY_THROTTLE_MULT / POSITION_QUOTA_MULT / 방어arm 가동 / 신규진입 제한)을
config_kv 에 SSOT 로 기록한다.

⚠️ live_nav_manager.treasury_state.json 의 `mdd_pct` 필드는 "역대 최대" 낙폭
   (monotonic max)이므로 이 모듈에서는 절대 재사용하지 않는다. 반드시 nav/hwm
   으로부터 "현재" 낙폭을 매번 새로 계산한다:
       current_dd_pct = (hwm - nav) / hwm * 100
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from config_manager import set_config_value
from live_nav_manager import get_market_state, normalize_market

logger = logging.getLogger(__name__)

CONFIG_KEY_PREFIX = "PERFORMANCE_BUDGET"
DEFAULT_MDD_CAP_PCT = 10.0
DEFAULT_BASE_MAX_OPEN = 20

DEFAULT_BUDGET_BANDS: List[Dict[str, Any]] = [
    {
        "band": "NORMAL",
        "exhaustion_lo": 0.0,
        "exhaustion_hi": 40.0,
        "kelly_throttle_mult": 1.0,
        "position_quota_mult": 1.0,
        "defense_arm_active": False,
        "new_entry_tier_filter": "ALL",
        "block_new_entries": False,
        "notes": "정상 (예산 0~40% 소진)",
    },
    {
        "band": "CAUTION",
        "exhaustion_lo": 40.0,
        "exhaustion_hi": 70.0,
        "kelly_throttle_mult": 0.5,
        "position_quota_mult": 0.6,
        "defense_arm_active": True,
        "new_entry_tier_filter": "ALL",
        "block_new_entries": False,
        "notes": "예산 40~70% 소진: 켈리·쿼터 축소, 방어arm 가동시작",
    },
    {
        "band": "DEFENSE",
        "exhaustion_lo": 70.0,
        "exhaustion_hi": 90.0,
        "kelly_throttle_mult": 0.2,
        "position_quota_mult": 0.35,
        "defense_arm_active": True,
        "new_entry_tier_filter": "TOP_ONLY",
        "block_new_entries": False,
        "notes": "예산 70~90% 소진: 최상위 티어만 신규진입 허용",
    },
    {
        "band": "LOCKDOWN",
        "exhaustion_lo": 90.0,
        "exhaustion_hi": float("inf"),
        "kelly_throttle_mult": 0.0,
        "position_quota_mult": 0.0,
        "defense_arm_active": True,
        "new_entry_tier_filter": "NONE",
        "block_new_entries": True,
        "notes": "예산 90~100%+ 소진: 신규진입 전면중단, 청산만",
    },
]


def resolve_mdd_cap_pct(sys_config: Optional[Dict[str, Any]] = None) -> float:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    try:
        v = float(cfg.get("PERFORMANCE_BUDGET_MDD_CAP_PCT", DEFAULT_MDD_CAP_PCT))
        return v if v > 0 else DEFAULT_MDD_CAP_PCT
    except (TypeError, ValueError):
        return DEFAULT_MDD_CAP_PCT


def _band_for_exhaustion(exhaustion_pct: float) -> Dict[str, Any]:
    for band in DEFAULT_BUDGET_BANDS:
        if band["exhaustion_lo"] <= exhaustion_pct < band["exhaustion_hi"]:
            return band
    return DEFAULT_BUDGET_BANDS[-1]


def _neutral_result(market: str, *, reason: str) -> Dict[str, Any]:
    band = DEFAULT_BUDGET_BANDS[0]
    return {
        "market": market,
        "nav": None,
        "hwm": None,
        "current_dd_pct": 0.0,
        "mdd_cap_pct": DEFAULT_MDD_CAP_PCT,
        "exhaustion_pct": 0.0,
        "band": band["band"],
        "kelly_throttle_mult": 1.0,
        "position_quota_mult": 1.0,
        "defense_arm_active": False,
        "new_entry_tier_filter": "ALL",
        "block_new_entries": False,
        "notes": reason,
        "evaluated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def evaluate_performance_budget(
    market: str,
    *,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    시장 1개(KR 또는 US) 기준 현재 드로다운 예산 소진율 평가.
    treasury_state.json 의 nav/hwm 을 매 호출 시 fresh 하게 읽는다.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    mkt = normalize_market(market)

    if not cfg.get("ENABLE_PERFORMANCE_BUDGET_GOVERNOR", True):
        return _neutral_result(mkt, reason="disabled")

    mst = get_market_state(mkt)
    try:
        nav = float(mst.get("nav", 0.0) or 0.0)
        hwm = float(mst.get("hwm", nav) or nav)
    except (TypeError, ValueError):
        return _neutral_result(mkt, reason="nav_hwm_read_error")

    if hwm <= 0:
        return _neutral_result(mkt, reason="hwm_not_initialized")

    current_dd_pct = max(0.0, (hwm - nav) / hwm * 100.0)
    mdd_cap_pct = resolve_mdd_cap_pct(cfg)
    exhaustion_pct = max(0.0, current_dd_pct / mdd_cap_pct * 100.0)

    band = _band_for_exhaustion(exhaustion_pct)

    return {
        "market": mkt,
        "nav": nav,
        "hwm": hwm,
        "current_dd_pct": round(current_dd_pct, 4),
        "mdd_cap_pct": mdd_cap_pct,
        "exhaustion_pct": round(exhaustion_pct, 2),
        "band": band["band"],
        "kelly_throttle_mult": band["kelly_throttle_mult"],
        "position_quota_mult": band["position_quota_mult"],
        "defense_arm_active": band["defense_arm_active"],
        "new_entry_tier_filter": band["new_entry_tier_filter"],
        "block_new_entries": band["block_new_entries"],
        "notes": band["notes"],
        "evaluated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def sync_performance_budget_to_config_kv(
    *, sys_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    KR/US 각각 평가 후 config_kv 에 SSOT 기록.
    meta_governor.py의 _step_treasury() 및/또는 진입 경로에서 주기 호출.
    """
    cfg = sys_config
    if not isinstance(cfg, dict):
        try:
            from config_manager import load_system_config

            cfg = load_system_config()
        except Exception:
            cfg = {}

    # 패치 D: NAV 드로다운 켈리 감쇠는 성과예산 거버너 일원화 — Ch.4 elasticity NAV축 비활성
    set_config_value("ENABLE_KELLY_NAV_DD_OVERLAY", False)
    if isinstance(cfg, dict):
        cfg["ENABLE_KELLY_NAV_DD_OVERLAY"] = False

    results: Dict[str, Any] = {}
    combined_kelly_mult = 1.0
    for mkt in ("KR", "US"):
        ev = evaluate_performance_budget(mkt, sys_config=cfg)
        results[mkt] = ev
        set_config_value(f"{CONFIG_KEY_PREFIX}_STATE_{mkt}", ev)
        set_config_value(f"KELLY_THROTTLE_MULT_{mkt}", ev["kelly_throttle_mult"])
        set_config_value(f"POSITION_QUOTA_MULT_{mkt}", ev["position_quota_mult"])
        set_config_value(
            f"{CONFIG_KEY_PREFIX}_DEFENSE_ARM_ACTIVE_{mkt}", ev["defense_arm_active"]
        )
        set_config_value(
            f"{CONFIG_KEY_PREFIX}_BLOCK_NEW_ENTRIES_{mkt}", ev["block_new_entries"]
        )
        combined_kelly_mult = min(
            combined_kelly_mult, float(ev["kelly_throttle_mult"])
        )

    set_config_value("KELLY_THROTTLE_MULT", round(combined_kelly_mult, 4))
    results["combined_kelly_throttle_mult"] = round(combined_kelly_mult, 4)

    if isinstance(sys_config, dict):
        sys_config["KELLY_THROTTLE_MULT"] = round(combined_kelly_mult, 4)
        sys_config["ENABLE_KELLY_NAV_DD_OVERLAY"] = False
        for mkt in ("KR", "US"):
            ev = results[mkt]
            sys_config[f"KELLY_THROTTLE_MULT_{mkt}"] = ev["kelly_throttle_mult"]
            sys_config[f"POSITION_QUOTA_MULT_{mkt}"] = ev["position_quota_mult"]
            sys_config[f"{CONFIG_KEY_PREFIX}_BLOCK_NEW_ENTRIES_{mkt}"] = ev[
                "block_new_entries"
            ]

    return results


def is_block_new_entries(
    sys_config: Optional[Mapping[str, Any]],
    market: str,
) -> bool:
    """패치 F — 킬스위치 OFF 시 잔여 LOCKDOWN 플래그 무시."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not cfg.get("ENABLE_PERFORMANCE_BUDGET_GOVERNOR", True):
        return False
    mkt = normalize_market(market)
    v = cfg.get(f"{CONFIG_KEY_PREFIX}_BLOCK_NEW_ENTRIES_{mkt}", False)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().upper() in ("1", "TRUE", "YES", "ON")
    return bool(v)


def resolve_position_quota_mult(
    sys_config: Optional[Mapping[str, Any]],
    market: str,
) -> float:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    mkt = normalize_market(market)
    key = f"POSITION_QUOTA_MULT_{mkt}"
    try:
        return max(0.0, float(cfg.get(key, 1.0) or 1.0))
    except (TypeError, ValueError):
        return 1.0


def resolve_max_open_positions(
    sys_config: Optional[Mapping[str, Any]],
    market: str,
    *,
    base_max: int = DEFAULT_BASE_MAX_OPEN,
) -> int:
    mult = resolve_position_quota_mult(sys_config, market)
    if mult <= 0.0:
        return 0
    return max(0, int(round(float(base_max) * mult)))
