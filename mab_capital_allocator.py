"""
MAB Capital Allocator — Thompson Sampling / UCB 기반 70% 활용 · 30% 탐험.

데스매치(과거 영광) 오버레이와 곱셈/혼합하지 않고 **병렬 신호**로 낸 뒤
fluid_evolution_bridge 가 MetaGovernor overlay 와 블렌드한다.
"""
from __future__ import annotations

import hashlib
import math
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Optional, Tuple

import numpy as np

MABMode = Literal["thompson", "ucb"]


def _parse_group_key(sig_type: Any) -> str:
    s = str(sig_type or "").strip()
    # 🧬 신형 병렬 진화 템플릿은 버전별로 독립 Arm 이어야 하므로 마커를 보존한다.
    deep = re.findall(r"DEEP_EVOLVED_v\d+", s)
    s = re.sub(r"\[.*?\]", "", s).strip()
    s = re.sub(r"\s+", " ", s)
    if deep:
        s = (s + " " + deep[-1]).strip()
    return s[:64] if s else "UNKNOWN"


@dataclass
class ArmStats:
    group_key: str
    wins: int = 0
    losses: int = 0
    n: int = 0
    mean_ret: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    is_incubator: bool = False
    is_archived: bool = False
    sample_score: float = 0.0
    bucket: str = "observe"


@dataclass
class MABAllocationResult:
    exploit_ratio: float
    explore_ratio: float
    mode: str
    group_mult: Dict[str, float] = field(default_factory=dict)
    exploit_groups: List[str] = field(default_factory=list)
    explore_groups: List[str] = field(default_factory=list)
    arms: List[ArmStats] = field(default_factory=list)
    as_of: str = ""


def _db_path() -> str:
    try:
        from market_db_paths import MARKET_DATA_DB_PATH

        return MARKET_DATA_DB_PATH
    except Exception:
        return os.path.join(os.environ.get("INSTALL_ROOT", "."), "market_data.sqlite")


def _load_closed_arms(
    market: str,
    *,
    lookback_days: int = 90,
) -> Dict[str, ArmStats]:
    mk = str(market or "KR").upper()
    since = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    out: Dict[str, ArmStats] = {}
    db = _db_path()
    if not os.path.isfile(db):
        return out
    try:
        conn = sqlite3.connect(db, timeout=30)
        try:
            rows = conn.execute(
                """
                SELECT sig_type, final_ret FROM forward_trades
                WHERE market=? AND status LIKE 'CLOSED%'
                  AND substr(IFNULL(NULLIF(TRIM(exit_date),''), entry_date),1,10) >= ?
                """,
                (mk, since),
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return out

    for sig, ret in rows:
        if str(sig or "").upper().find("SCOUT") >= 0 or "🔭" in str(sig or ""):
            continue
        gk = _parse_group_key(sig)
        if gk not in out:
            out[gk] = ArmStats(
                group_key=gk,
                is_incubator="INCUBATOR" in str(sig).upper() or "MUTANT_" in str(sig).upper(),
            )
        arm = out[gk]
        try:
            r = float(ret or 0)
        except (TypeError, ValueError):
            r = 0.0
        arm.n += 1
        if r > 0:
            arm.wins += 1
            arm.gross_profit += r
        else:
            arm.losses += 1
            arm.gross_loss += abs(r)
        arm.mean_ret += r

    for arm in out.values():
        if arm.n > 0:
            arm.mean_ret /= arm.n
    return out


def _seed_explore_arms(
    arms: Dict[str, ArmStats],
    sys_config: Dict[str, Any],
    market: str,
) -> None:
    """인큐베이터·도태 보관·언더독 — 표본 0이어도 탐험 후보."""
    mk = str(market or "KR").upper()
    inc = sys_config.get("INCUBATOR_TEMPLATES") or {}
    if isinstance(inc, dict):
        for name in inc:
            if not str(name).upper().startswith(("INCUBATOR", "MUTANT", "UNDERDOG")):
                continue
            gk = f"INCUBATOR_{name}"[:64]
            if gk not in arms:
                arms[gk] = ArmStats(group_key=gk, is_incubator=True)

    arch = (sys_config.get("ARCHIVED_TEMPLATES") or {}).get(mk) or {}
    if isinstance(arch, dict):
        for name in list(arch.keys())[:12]:
            gk = f"ARCHIVED_{name}"[:64]
            if gk not in arms:
                arms[gk] = ArmStats(group_key=gk, is_archived=True, n=0)

    # 🧬 신형 병렬 진화 템플릿(DEEP_EVOLVED) — 거래 표본이 없어도 독립 탐험 Arm 으로 시드.
    deep_reg = sys_config.get("DEEP_EVOLVED_DEPLOYED")
    if isinstance(deep_reg, dict):
        for name, meta in deep_reg.items():
            if not isinstance(meta, dict) or str(meta.get("market", "")).upper() != mk:
                continue
            gk = _parse_group_key(name)
            if gk not in arms:
                arms[gk] = ArmStats(group_key=gk, is_incubator=True, n=0)


def _thompson_sample(arm: ArmStats, *, prior_a: float = 1.0, prior_b: float = 1.0) -> float:
    if arm.n == 0:
        # 탐험 보너스: 불확실 arm 에 높은 분산
        return float(np.random.beta(1.2, 1.2))

    # [Magnitude-Aware] 승패 횟수에 수익금/손실금의 크기(Magnitude)를 스케일링하여 가중치로 합산
    # 예: reward_scale = 0.1 일 때, 10% 수익을 낸 1승은 단순 1승보다 훨씬 높은 알파(a) 파라미터를 얻어 자본 배분 확률이 급증함.
    reward_scale = 0.1

    adj_wins = arm.wins + (arm.gross_profit * reward_scale)
    adj_losses = arm.losses + (arm.gross_loss * reward_scale)

    a = prior_a + max(0.0, adj_wins)
    b = prior_b + max(0.0, adj_losses)

    return float(np.random.beta(a, b))


def _ucb_score(arm: ArmStats, total_n: int, *, c: float = 1.4) -> float:
    if arm.n <= 0:
        return 1.0 + c
    wr = arm.wins / arm.n
    return wr + c * math.sqrt(math.log(max(1, total_n)) / arm.n)


class MABCapitalAllocator:
    def __init__(
        self,
        sys_config: Optional[Dict[str, Any]] = None,
        *,
        exploit_ratio: float = 0.70,
        mode: MABMode = "thompson",
    ) -> None:
        self.cfg = sys_config if isinstance(sys_config, dict) else {}
        self.exploit_ratio = float(self.cfg.get("MAB_EXPLOIT_RATIO", exploit_ratio) or exploit_ratio)
        self.explore_ratio = 1.0 - self.exploit_ratio
        self.mode = str(self.cfg.get("MAB_MODE", mode) or mode).lower()
        if self.mode not in ("thompson", "ucb"):
            self.mode = "thompson"

    def compute(
        self,
        market: str,
        *,
        lookback_days: Optional[int] = None,
    ) -> MABAllocationResult:
        lb = int(lookback_days or self.cfg.get("MAB_LOOKBACK_DAYS", 90) or 90)
        arms_map = _load_closed_arms(market, lookback_days=lb)
        _seed_explore_arms(arms_map, self.cfg, market)

        arms = list(arms_map.values())
        if not arms:
            return MABAllocationResult(
                exploit_ratio=self.exploit_ratio,
                explore_ratio=self.explore_ratio,
                mode=self.mode,
                as_of=datetime.now().strftime("%Y-%m-%d %H:%M"),
            )

        total_n = sum(a.n for a in arms) or 1
        for arm in arms:
            if self.mode == "ucb":
                arm.sample_score = _ucb_score(arm, total_n)
            else:
                arm.sample_score = _thompson_sample(arm)

        ranked = sorted(arms, key=lambda a: (a.sample_score, a.mean_ret, a.n), reverse=True)
        n_arms = len(ranked)
        n_exploit = max(1, int(round(n_arms * self.exploit_ratio))) if n_arms else 0
        n_explore = max(1, n_arms - n_exploit) if n_arms else 0

        exploit_set = {a.group_key for a in ranked[:n_exploit]}
        explore_candidates = [
            a for a in ranked[n_exploit:]
            if a.is_incubator or a.is_archived or a.n < 5
        ]
        if not explore_candidates:
            explore_candidates = ranked[n_exploit : n_exploit + n_explore]
        explore_set = {a.group_key for a in explore_candidates[: max(1, n_explore)]}

        boost = float(self.cfg.get("MAB_EXPLOIT_MULT", 1.22) or 1.22)
        explore_mult = float(self.cfg.get("MAB_EXPLORE_MULT", 1.08) or 1.08)
        neutral = float(self.cfg.get("MAB_NEUTRAL_MULT", 1.0) or 1.0)
        cap = float(self.cfg.get("MAB_OVERLAY_CAP", 1.45) or 1.45)

        group_mult: Dict[str, float] = {}
        for arm in ranked:
            if arm.group_key in exploit_set:
                arm.bucket = "exploit"
                group_mult[arm.group_key] = min(boost, cap)
            elif arm.group_key in explore_set:
                arm.bucket = "explore"
                group_mult[arm.group_key] = min(explore_mult, cap)
            else:
                arm.bucket = "neutral"
                group_mult[arm.group_key] = neutral


# =====================================================================
        # 👑 [초월적 공격 배선] 하락장 잉여 자본 듀얼-트랙 역방향 라우팅
        # =====================================================================
        # 총사령관(MetaGovernor)이 판단한 현재 국면을 가져옴
        regime = str(self.cfg.get("META_REGIME_KEY", "UNKNOWN")).upper()
        # 글로벌 켈리 압착으로 인해 롱(Long) 포지션에 투입되지 못하고 남은 '유휴 현금(Idle Cash)' 비율 산출 (1.0 - 글로벌 켈리 승수)
        global_kelly = float(self.cfg.get("META_GLOBAL_KELLY_MULT", 1.0))
        idle_cash_ratio = max(0.0, 1.0 - global_kelly)
        
        # 하락장이며 유휴 현금이 10% 이상 발생했을 때 숏/인버스로 자금 우회
        if idle_cash_ratio >= 0.10:
            route_result = route_bear_market_capital(
                market=market,
                idle_cash=idle_cash_ratio,
                regime_key=regime,
                sys_config=self.cfg,
                db_path=_db_path()
            )
            # 듀얼 트랙 라우터가 자금을 성공적으로 분배했다면, 결과를 MAB 엔진의 결과에 합산하여 리턴
            if route_result.get("routed", False):
                # 기존 롱(Long) 전략(exploit/explore)의 가중치를 0으로 완전히 무력화시키지 않고, 
                # 남아있는 글로벌 켈리 승수만큼은 비중을 유지하도록 놔둔 채 숏/인버스 비중 정보를 추가함
                route_target = route_result.get("target")
                allocated_cash = route_result.get("allocated_cash")
                
                # 가상 그룹키 'BEAR_ROUTER'를 생성하여 숏/인버스에 가중치를 배분
                group_mult["BEAR_ROUTER"] = allocated_cash
                
                # MAB 모드에 라우팅 정보를 덧붙임
                self.mode = f"{self.mode}_+_{route_target}_({allocated_cash*100:.1f}%)"
        # =====================================================================
        return MABAllocationResult(
            exploit_ratio=self.exploit_ratio,
            explore_ratio=self.explore_ratio,
            mode=self.mode,
            group_mult=group_mult,
            exploit_groups=sorted(exploit_set),
            explore_groups=sorted(explore_set),
            arms=ranked,
            as_of=datetime.now().strftime("%Y-%m-%d %H:%M"),
        )


def blend_deathmatch_and_mab(
    deathmatch_overlay: Dict[str, float],
    mab_overlay: Dict[str, float],
    *,
    exploit_weight: float = 0.70,
) -> Dict[str, float]:
    """
    최종 group overlay = w * DM + (1-w) * MAB (키 합집합).
    """
    w = max(0.0, min(1.0, float(exploit_weight)))
    keys = set(deathmatch_overlay) | set(mab_overlay)
    out: Dict[str, float] = {}
    for gk in keys:
        dm = float(deathmatch_overlay.get(gk, 1.0))
        mb = float(mab_overlay.get(gk, 1.0))
        out[gk] = round(dm * w + mb * (1.0 - w), 4)
    return out


def stable_arm_id(market: str, group_key: str) -> str:
    raw = f"{market.upper()}|{group_key}"
    return "mab:" + hashlib.sha256(raw.encode()).hexdigest()[:12]


# ===========================================================================
# [초월적 공격] 듀얼-트랙 역방향 자본 라우팅 (Dual-Track Reverse Routing)
# ===========================================================================
def route_bear_market_capital(
    market: str, 
    idle_cash: float, 
    regime_key: str, 
    sys_config: dict, 
    db_path: str = None
) -> dict:
    """
    하락장(BEAR) 진입 시 켈리 압착으로 발생한 '유휴 현금(Idle Cash)'을 
    시장의 특성(US/KR)에 맞춰 숏(Short)과 인버스(Inverse)로 자동 분배합니다.
    """
    try:
        from bear_defense_booster_guard import is_defensive_regime
    except ImportError:
        # 모듈 로드 실패 시 보수적 현금 보유
        return {"routed": False, "target": "TREASURY_LOCK", "allocated_cash": 0.0}
    
    # 1. 상승장(BULL)이거나 유휴 현금이 없으면 라우팅 패스 (MAB 본연의 롱 전략 유지)
    if not is_defensive_regime(regime_key) or idle_cash <= 0:
        return {"routed": False, "target": "LONG_MAB", "allocated_cash": 0.0}
        
    mkt = str(market).strip().upper()
    
    # 2. 미국장(US): 공매도가 자유로우므로 숏 전용 엔진(blackhole_hunter)으로 라우팅
    if mkt == "US":
        # 블랙홀 헌터가 독성 주식을 포획할 수 있도록 가상 예산으로 이관
        return {
            "routed": True, 
            "target": "BLACKHOLE_SHORT", 
            "allocated_cash": idle_cash,
            "reason": f"US_BEAR_ROUTING ({regime_key})"
        }
        
    # 3. 한국장(KR): 공매도 불가로 인해 인버스 ETF 엔진으로 라우팅하되, 
    # dynamic_hedge_cap.py의 RL(강화학습) 피드백 밸브를 거쳐 휩쏘(Whipsaw)를 방어
    elif mkt == "KR":
        try:
            from dynamic_hedge_cap import resolve_dynamic_inverse_cap_pct
            
            # RL 기반의 동적 인버스 투입 한도(%) 산출
            final_cap_pct, audit_meta = resolve_dynamic_inverse_cap_pct(
                mkt, sys_config, db_path=db_path
            )
            
            # 유휴 현금 중 강화학습(RL)이 허락한 한도까지만 인버스로 투입
            inverse_allocation = idle_cash * final_cap_pct
            
            return {
                "routed": True,
                "target": "INVERSE_ETF",
                "allocated_cash": inverse_allocation,
                "reason": f"KR_BEAR_INVERSE (Cap: {final_cap_pct*100:.1f}%)",
                "audit": audit_meta
            }
        except ImportError:
            # 안전 장치: 모듈 로드 실패 시 극단적 방어를 위해 현금 100% 보관 (Treasury)
            return {"routed": False, "target": "TREASURY_LOCK", "allocated_cash": 0.0}
            
    # 기타 시장 (방어적 보관)
    return {"routed": False, "target": "TREASURY_LOCK", "allocated_cash": 0.0}