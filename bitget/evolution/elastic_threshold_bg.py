"""Bitget ElasticThreshold — scan/entry 기아 지수 (forward_trades SSOT)."""
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


def _truthy(val: Any, default: bool = True) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")

from bitget.forward.shared import DB_PATH
from bitget.infra.bounded_reads import (
    elastic_vol_closed_rets_sql,
    forward_open_count_sql,
    forward_pri_open_metrics_sql,
)
from bitget.infra.clock import utc_date_days_ago_str
from bitget.infra.memory_policy import ELASTIC_VOL_OPEN_LIMIT
from bitget.infra.proprietary_friction_store_bg import normalize_friction_market


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


@dataclass(frozen=True)
class BitgetElasticThresholdState:
    cos_cutoff: float
    ml_cutoff: float
    stretch_factor: float
    scout_gap: float
    starvation_index: float
    vol_proxy: float


@dataclass(frozen=True)
class ScoutVerdict:
    eligible: bool
    reason: str = ""
    path: str = ""
    best_metric: float = 0.0
    effective_cutoff: float = 0.0


class BitgetElasticThreshold:
    """주식 ElasticThreshold 의 코인 대응 — bitget_forward_trades 사용."""

    def __init__(self, cfg: Dict[str, Any], market_type: str):
        self.cfg = cfg if isinstance(cfg, dict) else {}
        self.market_db = str(market_type or "spot").lower()
        self.market_label = normalize_friction_market(market_type)

    def compute_starvation_index(self, *, lookback_days: int = 7) -> float:
        target_entries = int(self.cfg.get("ELASTIC_TARGET_ENTRIES_PER_WEEK", 12) or 12)
        target_closed = int(self.cfg.get("ELASTIC_TARGET_CLOSED_PER_WEEK", 6) or 6)
        since = utc_date_days_ago_str(lookback_days)
        db = DB_PATH
        if not db or not os.path.isfile(db):
            return 0.85
        try:
            conn = sqlite3.connect(db, timeout=20)
            try:
                ent = conn.execute(
                    """
                    SELECT COUNT(*) FROM bitget_forward_trades
                    WHERE market_type=? AND substr(entry_date,1,10) >= ?
                      AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
                    """,
                    (self.market_db, since),
                ).fetchone()[0]
                closed = conn.execute(
                    """
                    SELECT COUNT(*) FROM bitget_forward_trades
                    WHERE market_type=? AND status LIKE 'CLOSED%'
                      AND substr(IFNULL(NULLIF(TRIM(exit_date),''), entry_date),1,10) >= ?
                      AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
                    """,
                    (self.market_db, since),
                ).fetchone()[0]
                n_open_q, n_open_params = forward_open_count_sql(market_type=self.market_db)
                n_open = conn.execute(n_open_q, n_open_params).fetchone()[0]
            finally:
                conn.close()
        except sqlite3.Error:
            return 0.75

        ent_gap = 1.0 - _clip(int(ent or 0) / max(1, target_entries), 0.0, 1.0)
        cl_gap = 1.0 - _clip(int(closed or 0) / max(1, target_closed), 0.0, 1.0)
        stagnation = 0.35 if (int(n_open or 0) > 0 and int(ent or 0) == 0) else 0.0
        return _clip(0.45 * ent_gap + 0.45 * cl_gap + stagnation, 0.0, 1.0)

    def volatility_proxy(self) -> float:
        return internal_ledger_volatility_proxy(self.market_db)

    def apply_pair(
        self,
        base_cos: float,
        base_ml: float,
        *,
        starvation: Optional[float] = None,
        vol_proxy: Optional[float] = None,
    ) -> BitgetElasticThresholdState:
        """표본 기아 시 커트라인 완화 · 고변동 시 소폭 조임 (주식 ElasticThreshold 동형)."""
        starv = float(starvation if starvation is not None else self.compute_starvation_index())
        vol = float(vol_proxy if vol_proxy is not None else self.volatility_proxy())

        # [아키텍트 수술] 코인 시장의 실시간 유동성 스트레스 융합 (고무줄 허들 강화)
        # Canary 시스템이 감지한 현재의 유동성 마름(Stress) 현상을 변동성 프록시에 직접 주입합니다.
        # 시장에 피바람이 불 조짐이 보이면 진입 커트라인을 대폭 높여 진짜 대장주만 잡게 만듭니다.
        try:
            from bitget.reports.canary_panel_bg import load_canary_state
            stress = float(load_canary_state().get("crypto_liquidity_stress") or 0.0)
            vol = vol + stress  # 스트레스 지수만큼 변동성 프록시를 뻥튀기
        except Exception:
            pass

        max_relief = float(self.cfg.get("ELASTIC_MAX_RELIEF", 0.18) or 0.18)
        vol_tighten = float(self.cfg.get("ELASTIC_VOL_TIGHTEN", 0.06) or 0.06)
        relief = starv * max_relief
        tighten = max(0.0, vol - 1.0) * vol_tighten

        floor = float(self.cfg.get("ELASTIC_CUTOFF_FLOOR", 0.32) or 0.32)
        ceil = float(self.cfg.get("ELASTIC_CUTOFF_CEIL", 0.92) or 0.92)

        cos = _clip(float(base_cos) * (1.0 + tighten) - relief, floor, ceil)
        ml = _clip(float(base_ml) * (1.0 + tighten) - relief, floor, ceil)
        base_gap = float(self.cfg.get("ELASTIC_SCOUT_BASE_GAP", 0.07) or 0.07)
        scout_gap = _clip(base_gap + starv * 0.14, 0.05, 0.22)
        stretch = 1.0 + tighten - relief

        return BitgetElasticThresholdState(
            cos_cutoff=round(cos, 4),
            ml_cutoff=round(ml, 4),
            stretch_factor=round(stretch, 4),
            scout_gap=round(scout_gap, 4),
            starvation_index=round(starv, 4),
            vol_proxy=round(vol, 4),
        )


def internal_ledger_volatility_proxy(market_type: str, *, lookback_days: int = 20) -> float:
    """OPEN MFE 분산 + 청산 σ 프록시 — 1.0=중립."""
    import numpy as np

    mk = str(market_type or "spot").lower()
    since = utc_date_days_ago_str(lookback_days)
    db = DB_PATH
    if not db or not os.path.isfile(db):
        return 1.0
    try:
        conn = sqlite3.connect(db, timeout=20)
        try:
            open_q, open_params = forward_pri_open_metrics_sql(
                market_type=mk,
                limit=ELASTIC_VOL_OPEN_LIMIT,
            )
            open_rows = conn.execute(open_q, open_params).fetchall()
            closed_q, closed_params = elastic_vol_closed_rets_sql(
                market_type=mk,
                since_date=since,
            )
            closed_rets = conn.execute(closed_q, closed_params).fetchall()
        finally:
            conn.close()
    except sqlite3.Error:
        return 1.0

    mfes: list[float] = []
    for ep, mh, ml, side in open_rows:
        ep_f, mh_f, ml_f = float(ep), float(mh or ep), float(ml or ep)
        if ep_f <= 0:
            continue
        if str(side or "LONG").upper() == "SHORT":
            mfes.append(((ep_f - ml_f) / ep_f) * 100.0)
        else:
            mfes.append(((mh_f - ep_f) / ep_f) * 100.0)
    rets = [float(r[0]) for r in closed_rets if r[0] is not None]
    vol_parts: list[float] = []
    if len(mfes) >= 2:
        vol_parts.append(float(np.std(mfes, ddof=0)) / 5.0)
    if len(rets) >= 2:
        vol_parts.append(float(np.std(rets, ddof=0)) / 4.0)
    if not vol_parts:
        return 1.0
    raw = float(np.mean(vol_parts))
    return _clip(0.85 + raw * 0.25, 0.7, 1.5)


def evaluate_scout_candidate(
    *,
    is_pass_cosine: bool,
    is_pass_ml_box: bool,
    best_cos_sim: float,
    eff_cos_cutoff: float,
    ml_score: float,
    eff_ml_cutoff: float,
    state: BitgetElasticThresholdState,
    sys_config: Optional[Dict[str, Any]] = None,
) -> ScoutVerdict:
    """DNA 합격 실패 직후 — 정찰병(Scout) 허용 여부."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not _truthy(cfg.get("ELASTIC_SCOUT_ENABLED"), True):
        return ScoutVerdict(False, "scout_disabled")

    if is_pass_cosine or is_pass_ml_box:
        return ScoutVerdict(False, "already_passed")

    if state.starvation_index < float(cfg.get("ELASTIC_SCOUT_MIN_STARVATION", 0.35) or 0.35):
        return ScoutVerdict(False, "starvation_not_high_enough")

    gap = state.scout_gap
    cos_floor = eff_cos_cutoff - gap
    ml_floor = eff_ml_cutoff - gap

    if best_cos_sim >= cos_floor and best_cos_sim < eff_cos_cutoff:
        return ScoutVerdict(
            True,
            "cosine_near_miss",
            path="COSINE_SCOUT",
            best_metric=best_cos_sim,
            effective_cutoff=eff_cos_cutoff,
        )
    if ml_score >= ml_floor and ml_score < eff_ml_cutoff:
        return ScoutVerdict(
            True,
            "ml_near_miss",
            path="MLBOX_SCOUT",
            best_metric=ml_score,
            effective_cutoff=eff_ml_cutoff,
        )
    return ScoutVerdict(False, "outside_scout_band")


def scout_invest_cap(sys_config: Dict[str, Any], account_size: float) -> float:
    pct = float(sys_config.get("ELASTIC_SCOUT_INVEST_PCT", 0.003) or 0.003)
    pct = _clip(pct, 0.001, 0.003)
    return float(account_size) * pct


def enforce_scout_hard_cap(
    invest_amount: float,
    sim_kelly_invest: float,
    *,
    sys_config: Dict[str, Any],
    account_size: float,
    entry_price: float,
) -> Tuple[float, float, float]:
    """정찰병 비중 절대 상한 — returns (invest_amount, sim_kelly_invest, notional)."""
    cap = scout_invest_cap(sys_config, float(account_size))
    inv = min(float(invest_amount), cap)
    sk = min(float(sim_kelly_invest), cap)
    ep = float(entry_price) if entry_price else 0.0
    if ep > 0 and sk > cap:
        sk = cap
        inv = min(inv, cap)
    return inv, sk, sk
