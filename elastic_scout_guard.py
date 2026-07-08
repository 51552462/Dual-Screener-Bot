"""
Elastic Scout Guard — BEAR/HIGH_VOL fluid scout conditional live (Architect P1).

표본 기아로 완화된 `_fluid_scout` 신호를 하락장에서 무조건 차단하지 않고,
교차검증 무기(수급·숏스퀴즈·펀더·DART·스마트머니) ≥1 일 때만 Live 허용.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Sequence, Tuple

from bear_defense_booster_guard import is_defensive_regime

WEAPON_EPS = 1e-6
FLOW_DIVERGENCE_MIN = 0.4
SMART_MONEY_CV_TAG = "[🕵️세력매집_교차검증]"


@dataclass(frozen=True)
class ScoutConditionalLiveVerdict:
    """BEAR/HIGH_VOL + fluid_scout 에 대한 조건부 Live 판정."""

    needs_gate: bool
    live_allowed: bool
    weapon_count: int
    weapons: Tuple[str, ...]

    @property
    def weapons_summary(self) -> str:
        return "+".join(self.weapons) if self.weapons else "none"


def count_scout_cross_validation_weapons(
    *,
    sig_type: str = "",
    flow_bonus: float = 0.0,
    flow_divergence: float = 0.0,
    short_net: float = 0.0,
    fund_net: float = 0.0,
    dart_net: float = 0.0,
) -> Tuple[int, Tuple[str, ...]]:
    """
    교차검증 무기 개수 — 1개 이상이면 하락장 Scout Live 예외 허용.

    무기:
      - flow_bonus > 0  (외인·기관 수급 모멘텀)
      - flow_divergence ≥ 0.4  (주가 횡보 + 매집 다이버전스)
      - short_net > 0  (숏스퀴즈 net 가산)
      - fund_net > 0  (펀더멘털 저평가·흑자)
      - dart_net > 0  (DART 호재)
      - sig_type 내 스마트머니 교차검증 태그
    """
    weapons: list[str] = []
    if float(flow_bonus) > WEAPON_EPS:
        weapons.append("flow")
    if float(flow_divergence) >= FLOW_DIVERGENCE_MIN:
        weapons.append("flow_div")
    if float(short_net) > WEAPON_EPS:
        weapons.append("short_sq")
    if float(fund_net) > WEAPON_EPS:
        weapons.append("fund")
    if float(dart_net) > WEAPON_EPS:
        weapons.append("dart")
    if SMART_MONEY_CV_TAG in str(sig_type or ""):
        weapons.append("smart_money")
    return len(weapons), tuple(weapons)


def evaluate_scout_conditional_live(
    *,
    regime_key: Any,
    fluid_scout: bool = True,
    sig_type: str = "",
    flow_bonus: float = 0.0,
    flow_divergence: float = 0.0,
    short_net: float = 0.0,
    fund_net: float = 0.0,
    dart_net: float = 0.0,
) -> ScoutConditionalLiveVerdict:
    """
    fluid_scout + BEAR/HIGH_VOL → 교차검증 기반 조건부 Live.
    방어 국면·비-scout 는 needs_gate=False (기존 경로 유지).
    """
    if not fluid_scout:
        return ScoutConditionalLiveVerdict(False, True, 0, ())
    if not is_defensive_regime(regime_key):
        return ScoutConditionalLiveVerdict(False, True, 0, ())

    weapon_count, weapons = count_scout_cross_validation_weapons(
        sig_type=sig_type,
        flow_bonus=flow_bonus,
        flow_divergence=flow_divergence,
        short_net=short_net,
        fund_net=fund_net,
        dart_net=dart_net,
    )
    return ScoutConditionalLiveVerdict(
        needs_gate=True,
        live_allowed=weapon_count >= 1,
        weapon_count=weapon_count,
        weapons=weapons,
    )


def format_scout_bear_cv_live_tag(weapons: Sequence[str]) -> str:
    """Live 허용 시 sig_type 에 붙일 태그."""
    summary = "+".join(weapons) if weapons else "cv"
    return f" #ScoutBearCVLive({summary})"


SCOUT_SHADOW_STRATEGY_ID = "ELASTIC_SCOUT_SHADOW"
SCOUT_SHADOW_BLOCK_REASON = "ELASTIC_SCOUT_BEAR_SHADOW"
_SCOUT_PREFIX = "[🔭SCOUT]"


def format_scout_bear_shadow_sig_type(sig_type: str, regime_key: Any) -> str:
    """OBSERVE_ONLY 장부용 Scout Shadow sig_type — fluid_evolution_bridge 식별 가능."""
    from bear_defense_booster_guard import normalize_regime_for_guard

    body = str(sig_type or "").strip()
    if _SCOUT_PREFIX not in body:
        body = f"{_SCOUT_PREFIX} {body}"
    rk = normalize_regime_for_guard(regime_key)
    if "#ScoutBearShadow" not in body:
        body = f"{body} #ScoutBearShadow({rk})"
    return body


def build_scout_shadow_observe_facts(
    facts: Optional[dict[str, Any]],
    *,
    regime_key: Any,
    flow_bonus: float = 0.0,
    flow_divergence: float = 0.0,
    short_net: float = 0.0,
    fund_net: float = 0.0,
    dart_net: float = 0.0,
) -> dict[str, Any]:
    """Shadow OBSERVE 편입용 facts — 교차검증·국면·scout DNA 보존 (HTC 진화 입력)."""
    from bear_defense_booster_guard import normalize_regime_for_guard

    out = dict(facts) if isinstance(facts, dict) else {}
    out["entry_regime"] = normalize_regime_for_guard(regime_key)
    out["flow_bonus"] = float(flow_bonus)
    out["flow_divergence"] = float(flow_divergence)
    out["short_net"] = float(short_net)
    out["fund_net"] = float(fund_net)
    out["dart_net"] = float(dart_net)
    out["_fluid_scout"] = True
    out["_scout_shadow_routed"] = True
    return out


def route_fluid_scout_bear_shadow(
    *,
    market: str,
    code: str,
    name: str,
    sig_type: str,
    score: float,
    ep: float,
    sector: str = "유망섹터",
    regime_key: Any,
    flow_bonus: float = 0.0,
    flow_divergence: float = 0.0,
    short_net: float = 0.0,
    fund_net: float = 0.0,
    dart_net: float = 0.0,
    facts: Optional[dict[str, Any]] = None,
    sys_config: Optional[dict[str, Any]] = None,
    satellite_tags: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    BEAR/HIGH_VOL Scout 교차검증 실패 → Live 거부 + OBSERVE_ONLY Shadow 라우팅.

    1) forward_observe_bridge — forward_trades [OBSERVE_ONLY] (PnL 가상 추적)
    2) shadow_tracking.blocked_trade_history — 차단 사유 감사
    3) shadow_tracking.virtual_trade_history — 위성 태그·scout DNA 스냅샷
    """
    mkt = str(market or "KR").upper()
    code_str = str(code).zfill(6) if mkt == "KR" else str(code)
    shadow_sig = format_scout_bear_shadow_sig_type(sig_type, regime_key)
    observe_facts = build_scout_shadow_observe_facts(
        facts,
        regime_key=regime_key,
        flow_bonus=flow_bonus,
        flow_divergence=flow_divergence,
        short_net=short_net,
        fund_net=fund_net,
        dart_net=dart_net,
    )

    observe_ok = False
    observe_msg = ""
    try:
        from forward_observe_bridge import try_add_observe_forward_trade

        observe_ok, observe_msg = try_add_observe_forward_trade(
            market=mkt,
            code=code_str,
            name=str(name),
            sig_type=shadow_sig,
            score=float(score),
            ep=float(ep),
            strategy_id=SCOUT_SHADOW_STRATEGY_ID,
            sector=str(sector),
            facts=observe_facts,
        )
    except Exception as ex:
        observe_msg = f"observe_bridge:{type(ex).__name__}:{ex}"

    try:
        import shadow_tracking

        shadow_tracking.record_blocked_trade(
            code_str,
            str(name),
            SCOUT_SHADOW_BLOCK_REASON,
            float(ep) if ep is not None else 0.0,
        )
    except Exception:
        pass

    _tags = satellite_tags
    if not _tags and isinstance(sys_config, dict):
        try:
            import shadow_tracking

            _tags = shadow_tracking.build_satellite_tags(sys_config)
        except Exception:
            _tags = ""

    try:
        import sqlite3
        from datetime import datetime

        import pytz
        import shadow_tracking

        tz = pytz.timezone("Asia/Seoul") if mkt == "KR" else pytz.timezone("America/New_York")
        logged_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(shadow_tracking.DB_PATH, timeout=60)
        try:
            cur = conn.cursor()
            shadow_tracking.init_shadow_tables(cur)
            shadow_tracking.insert_virtual_trade_row(
                cur,
                mkt,
                code_str,
                str(name),
                float(ep) if ep is not None else 0.0,
                shadow_sig,
                str(_tags or ""),
                logged_at,
            )
            conn.commit()
        finally:
            conn.close()
    except Exception:
        pass

    if observe_ok:
        return (
            True,
            f"🔭 [Elastic Scout Shadow] OBSERVE_ONLY 편입 (Live 거부): {name} — {observe_msg}",
        )
    return (
        False,
        f"🔭 [Elastic Scout Shadow] Live 거부 · OBSERVE 실패: {observe_msg}",
    )
