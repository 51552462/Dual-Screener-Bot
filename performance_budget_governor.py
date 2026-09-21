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
import math
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from config_manager import set_config_value
from live_nav_manager import get_market_state, normalize_market
from meta_state_store import normalize_regime_key

logger = logging.getLogger(__name__)

CONFIG_KEY_PREFIX = "PERFORMANCE_BUDGET"
CONFIG_KEY_POSITION_QUOTA_REGIME_MAP = "POSITION_QUOTA_REGIME_MAP"
DEFAULT_MDD_CAP_PCT = 10.0
DEFAULT_BASE_MAX_OPEN = 20
CONFIG_KEY_KR_LOCKDOWN_THAW_ARMED = "KR_LOCKDOWN_THAW_ARMED"
CONFIG_KEY_KR_LOCKDOWN_THAW_EPS = "KR_LOCKDOWN_THAW_EPS_HWM_PCT"
CONFIG_KEY_KR_LOCKDOWN_THAW_RMAX = "KR_LOCKDOWN_THAW_RMAX_PCT"
DEFAULT_KR_LOCKDOWN_THAW_EPS_HWM_PCT = 0.05
DEFAULT_KR_LOCKDOWN_THAW_RMAX_PCT = 15.0
MAX_THAW_KELLY_MULT = 0.50

# A-3 SSOT: 국면별 base max OPEN (POSITION_QUOTA_MULT와 곱연산 — min 아님)
DEFAULT_POSITION_QUOTA_REGIME_MAP: Dict[str, int] = {
    "BULL": 20,
    "SIDEWAYS": 15,
    "HIGH_VOL": 10,
    "BEAR": 8,
    "DEFAULT": DEFAULT_BASE_MAX_OPEN,
}

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
        "kelly_throttle_mult_true": 1.0,
        "stall_thaw_active": False,
        "evaluated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def is_kr_lockdown_thaw_armed(sys_config: Optional[Mapping[str, Any]]) -> bool:
    """KR_LOCKDOWN_THAW_ARMED — 기본 0/없음 = OFF. 1/true/on 만 무장."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if CONFIG_KEY_KR_LOCKDOWN_THAW_ARMED not in cfg:
        return False
    v = cfg.get(CONFIG_KEY_KR_LOCKDOWN_THAW_ARMED)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return int(v) == 1
    if isinstance(v, str):
        return v.strip().upper() in ("1", "TRUE", "YES", "ON")
    return False


def resolve_kr_thaw_eps_hwm_pct(sys_config: Optional[Mapping[str, Any]] = None) -> float:
    v = resolve_config_float(
        sys_config if isinstance(sys_config, dict) else {},
        CONFIG_KEY_KR_LOCKDOWN_THAW_EPS,
        default=DEFAULT_KR_LOCKDOWN_THAW_EPS_HWM_PCT,
    )
    return v if v > 0 else DEFAULT_KR_LOCKDOWN_THAW_EPS_HWM_PCT


def resolve_kr_thaw_rmax_pct(sys_config: Optional[Mapping[str, Any]] = None) -> float:
    v = resolve_config_float(
        sys_config if isinstance(sys_config, dict) else {},
        CONFIG_KEY_KR_LOCKDOWN_THAW_RMAX,
        default=DEFAULT_KR_LOCKDOWN_THAW_RMAX_PCT,
    )
    return v if v > 0 else DEFAULT_KR_LOCKDOWN_THAW_RMAX_PCT


def compute_kr_thaw_kelly_mult(nav: float, hwm: float, sys_config: Optional[Mapping[str, Any]] = None) -> float:
    """f = (ε/100)×HWM / (NAV×|R|_max/100). ε 기본 0.05%p of HWM."""
    if nav <= 0 or hwm <= 0:
        return 0.0
    eps = resolve_kr_thaw_eps_hwm_pct(sys_config)
    rmax = resolve_kr_thaw_rmax_pct(sys_config)
    f = (eps / 100.0) * hwm / (nav * (rmax / 100.0))
    if not math.isfinite(f) or f <= 0:
        return 0.0
    return min(MAX_THAW_KELLY_MULT, f)


def count_kr_open_positions(sys_config: Optional[Mapping[str, Any]] = None) -> int:
    """
    KR OPEN 수. 테스트는 KR_LOCKDOWN_THAW_OPEN_COUNT 주입.
    조회 실패는 -1 (thaw 거부).
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if "KR_LOCKDOWN_THAW_OPEN_COUNT" in cfg:
        try:
            return int(cfg.get("KR_LOCKDOWN_THAW_OPEN_COUNT"))
        except (TypeError, ValueError):
            return -1
    try:
        from market_db_paths import MARKET_DATA_DB_PATH

        if not os.path.isfile(MARKET_DATA_DB_PATH):
            return -1
        conn = sqlite3.connect(f"file:{MARKET_DATA_DB_PATH}?mode=ro", uri=True)
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM forward_trades WHERE market=? AND status='OPEN'",
                ("KR",),
            ).fetchone()
            return int(row[0] or 0) if row else 0
        finally:
            conn.close()
    except Exception:
        return -1


def _maybe_apply_kr_stall_thaw(
    out: Dict[str, Any],
    cfg: Mapping[str, Any],
    *,
    nav: float,
    hwm: float,
) -> None:
    """LOCKDOWN + ARMED + KR OPEN=0 일 때만 층1·2·3 출력을 1슬롯용으로 덮어쓴다. band 이름은 LOCKDOWN 유지."""
    if out.get("market") != "KR":
        return
    if out.get("band") != "LOCKDOWN":
        return
    if not is_kr_lockdown_thaw_armed(cfg):
        return
    open_n = count_kr_open_positions(cfg)
    if open_n != 0:
        return
    f_eps = compute_kr_thaw_kelly_mult(nav, hwm, cfg)
    if f_eps <= 0:
        return
    base = resolve_regime_base_max_open(cfg, "KR")
    if base <= 0:
        base = DEFAULT_BASE_MAX_OPEN
    quota_mult = 1.0 / float(base)
    out["block_new_entries"] = False
    out["kelly_throttle_mult"] = round(f_eps, 8)
    out["position_quota_mult"] = quota_mult
    out["stall_thaw_active"] = True
    out["notes"] = f"{out.get('notes', '')} | stall_thaw_slot f={f_eps:.6f} max_open=1"


def consume_kr_lockdown_thaw_slot(*, market: str) -> bool:
    """KR 청산 1건 후 무장 해제. True면 이번에 0으로 내렸다."""
    if normalize_market(market) != "KR":
        return False
    try:
        from config_manager import load_system_config

        cfg = load_system_config()
        if not is_kr_lockdown_thaw_armed(cfg):
            return False
        set_config_value(CONFIG_KEY_KR_LOCKDOWN_THAW_ARMED, 0)
        if isinstance(cfg, dict):
            cfg[CONFIG_KEY_KR_LOCKDOWN_THAW_ARMED] = 0
        try:
            from ops_logger import insert_ops_event

            insert_ops_event(
                component="performance_budget_governor",
                severity="INFO",
                event="kr_lockdown.stall_thaw_consumed",
                payload={"market": "KR"},
            )
        except Exception:
            pass
        logger.info("KR lockdown stall-thaw slot consumed — ARMED=0")
        return True
    except Exception as exc:
        logger.error("KR stall-thaw consume failed: %s", exc, exc_info=True)
        return False


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
    true_kelly = float(band["kelly_throttle_mult"])
    out = {
        "market": mkt,
        "nav": nav,
        "hwm": hwm,
        "current_dd_pct": round(current_dd_pct, 4),
        "mdd_cap_pct": mdd_cap_pct,
        "exhaustion_pct": round(exhaustion_pct, 2),
        "band": band["band"],
        "kelly_throttle_mult": true_kelly,
        "position_quota_mult": band["position_quota_mult"],
        "defense_arm_active": band["defense_arm_active"],
        "new_entry_tier_filter": band["new_entry_tier_filter"],
        "block_new_entries": band["block_new_entries"],
        "notes": band["notes"],
        "kelly_throttle_mult_true": true_kelly,
        "stall_thaw_active": False,
        "evaluated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _maybe_apply_kr_stall_thaw(out, cfg, nav=nav, hwm=hwm)
    return out


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
    set_config_value(
        CONFIG_KEY_POSITION_QUOTA_REGIME_MAP,
        resolve_position_quota_regime_map(cfg),
    )
    if isinstance(cfg, dict):
        cfg["ENABLE_KELLY_NAV_DD_OVERLAY"] = False
        cfg[CONFIG_KEY_POSITION_QUOTA_REGIME_MAP] = resolve_position_quota_regime_map(cfg)

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
        combined_src = ev.get("kelly_throttle_mult_true", ev["kelly_throttle_mult"])
        combined_kelly_mult = min(combined_kelly_mult, float(combined_src))

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


def resolve_config_float(
    sys_config: Optional[Mapping[str, Any]],
    key: str,
    *,
    default: float = 1.0,
) -> float:
    """
    config dict에서 float 읽기 — `or default` 로 falsy 0.0 을 치환하지 않음 (A-1-R1).
    키 없음 / None → default.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if key not in cfg:
        return default
    raw = cfg.get(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def resolve_kelly_throttle_mult(
    sys_config: Optional[Mapping[str, Any]],
    market: Optional[str] = None,
) -> float:
    """KELLY_THROTTLE_MULT_{KR|US} 우선, 없으면 합성 KELLY_THROTTLE_MULT."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if market:
        mkt = normalize_market(market)
        mkey = f"KELLY_THROTTLE_MULT_{mkt}"
        if mkey in cfg:
            return max(0.0, resolve_config_float(cfg, mkey, default=1.0))
    return max(0.0, resolve_config_float(cfg, "KELLY_THROTTLE_MULT", default=1.0))


def resolve_position_quota_mult(
    sys_config: Optional[Mapping[str, Any]],
    market: str,
) -> float:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    mkt = normalize_market(market)
    key = f"POSITION_QUOTA_MULT_{mkt}"
    return max(0.0, resolve_config_float(cfg, key, default=1.0))


def resolve_position_quota_regime_map(
    sys_config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, int]:
    """A-3 SSOT — config_kv POSITION_QUOTA_REGIME_MAP 또는 모듈 기본값."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    raw = cfg.get(CONFIG_KEY_POSITION_QUOTA_REGIME_MAP)
    if isinstance(raw, dict) and raw:
        out: Dict[str, int] = {}
        for k, v in raw.items():
            try:
                out[str(k).strip().upper()] = max(0, int(v))
            except (TypeError, ValueError):
                continue
        if out:
            out.setdefault("DEFAULT", DEFAULT_BASE_MAX_OPEN)
            return out
    return dict(DEFAULT_POSITION_QUOTA_REGIME_MAP)


def resolve_market_regime_key(
    sys_config: Optional[Mapping[str, Any]],
    market: str,
) -> str:
    """
    시장별 현재 국면 — predictive_regime_ensemble 가 기록한 config 키만 읽음.
    KR_REGIME_KEY / US_REGIME_KEY → REGIME_ENSEMBLE.markets.{KR|US}.regime 폴백.
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    mkt = normalize_market(market)
    mkt_key = f"{mkt}_REGIME_KEY"
    rk = normalize_regime_key(cfg.get(mkt_key))
    if rk not in ("", "UNKNOWN"):
        return rk
    ens = cfg.get("REGIME_ENSEMBLE")
    if isinstance(ens, dict):
        markets = ens.get("markets")
        if isinstance(markets, dict):
            blk = markets.get(mkt)
            if isinstance(blk, dict):
                rk_ens = normalize_regime_key(blk.get("regime"))
                if rk_ens not in ("", "UNKNOWN"):
                    return rk_ens
    return "UNKNOWN"


def resolve_regime_base_max_open(
    sys_config: Optional[Mapping[str, Any]],
    market: str,
) -> int:
    """국면 base max OPEN — 미매핑/UNKNOWN → DEFAULT(20)."""
    regime_map = resolve_position_quota_regime_map(sys_config)
    rk = resolve_market_regime_key(sys_config, market)
    if rk in regime_map:
        return int(regime_map[rk])
    return int(regime_map.get("DEFAULT", DEFAULT_BASE_MAX_OPEN))


def resolve_max_open_positions(
    sys_config: Optional[Mapping[str, Any]],
    market: str,
    *,
    base_max: Optional[int] = None,
) -> int:
    """
    A-3: regime_base × POSITION_QUOTA_MULT_{market} (곱연산, floor).
    mult<=0 (LOCKDOWN) → 0 (min-1 보정 없음).
    mult>0 이고 floor 결과 <1 → 1.
    """
    mult = resolve_position_quota_mult(sys_config, market)
    if mult <= 0.0:
        return 0
    if base_max is not None:
        regime_base = max(0, int(base_max))
    else:
        regime_base = resolve_regime_base_max_open(sys_config, market)
    raw = float(regime_base) * mult
    max_open = int(math.floor(raw))
    if max_open < 1:
        max_open = 1
    return max_open
