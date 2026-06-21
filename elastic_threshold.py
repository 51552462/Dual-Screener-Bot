"""
Elastic Threshold — 표본 기아(Sample Starvation) 완화용 탄력 커트라인 + 정찰병(Scout) 판정.

고정 허들 대신:
  - 최근 진입·청산 밀도(기아 지수)가 높을수록 커트라인을 당김
  - 변동성 프록시가 높으면 소폭 허들 상향 (노이즈 필터)
  - 기아가 심하면 커트라인 바로 아래 '정찰 구간' 후보를 허용
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import pytz


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _truthy(val: Any, default: bool = True) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class ElasticThresholdState:
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


class ElasticThreshold:
    """
    시장·설정·장부 밀도에 따라 커트라인을 늘이고 줄이는 고무줄 게이트.
    """

    def __init__(
        self,
        sys_config: Dict[str, Any],
        *,
        market: str = "KR",
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.cfg = sys_config if isinstance(sys_config, dict) else {}
        self.market = str(market or "KR").upper()
        self.meta = meta if isinstance(meta, dict) else {}

    @classmethod
    def from_system_config(
        cls,
        sys_config: Dict[str, Any],
        *,
        market: str = "KR",
        meta: Optional[Dict[str, Any]] = None,
    ) -> "ElasticThreshold":
        return cls(sys_config, market=market, meta=meta)

    def _db_path(self) -> str:
        try:
            from market_db_paths import MARKET_DATA_DB_PATH

            return MARKET_DATA_DB_PATH
        except Exception:
            root = os.environ.get("INSTALL_ROOT", ".")
            return os.path.join(root, "market_data.sqlite")

    def compute_starvation_index(self, *, lookback_days: int = 7) -> float:
        """
        0=표본 충분, 1=극심한 기아.
        - 최근 lookback 일 진입 수 vs 목표
        - 최근 청산 수
        - OPEN 대비 최근 진입 정체
        """
        target_entries = int(self.cfg.get("ELASTIC_TARGET_ENTRIES_PER_WEEK", 8) or 8)
        target_closed = int(self.cfg.get("ELASTIC_TARGET_CLOSED_PER_WEEK", 4) or 4)
        mk = self.market
        since = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        db = self._db_path()
        if not os.path.isfile(db):
            return 0.85

        try:
            conn = sqlite3.connect(db, timeout=20)
            try:
                ent = conn.execute(
                    """
                    SELECT COUNT(*) FROM forward_trades
                    WHERE market=? AND substr(entry_date,1,10) >= ?
                    """,
                    (mk, since),
                ).fetchone()[0]
                closed = conn.execute(
                    """
                    SELECT COUNT(*) FROM forward_trades
                    WHERE market=? AND status LIKE 'CLOSED%'
                      AND substr(IFNULL(NULLIF(TRIM(exit_date),''), entry_date),1,10) >= ?
                    """,
                    (mk, since),
                ).fetchone()[0]
                n_open = conn.execute(
                    "SELECT COUNT(*) FROM forward_trades WHERE market=? AND status='OPEN'",
                    (mk,),
                ).fetchone()[0]
            finally:
                conn.close()
        except sqlite3.Error:
            return 0.75

        ent = int(ent or 0)
        closed = int(closed or 0)
        n_open = int(n_open or 0)

        ent_gap = 1.0 - _clip(ent / max(1, target_entries), 0.0, 1.0)
        cl_gap = 1.0 - _clip(closed / max(1, target_closed), 0.0, 1.0)
        stagnation = 0.35 if (n_open > 0 and ent == 0) else 0.0

        return _clip(0.45 * ent_gap + 0.45 * cl_gap + stagnation, 0.0, 1.0)

    def volatility_proxy(self) -> float:
        """1.0=중립, >1=확대, <1=수축. 실패 시 1.0."""
        try:
            import numpy as np

            if self.market == "US":
                from network_timeout import yf_download

                df = yf_download("SPY", period="1mo", progress=False)
                if df is None or df.empty:
                    return 1.0
                close = df["Close"].squeeze().pct_change().dropna()
            else:
                from network_timeout import fdr_data_reader

                raw = fdr_data_reader("069500", (datetime.now() - timedelta(days=40)).strftime("%Y-%m-%d"))
                if raw is None or raw.empty:
                    return 1.0
                close = raw["Close"].pct_change().dropna()

            if len(close) < 5:
                return 1.0
            vol = float(close.std())
            med = float(np.median(np.abs(close)))
            base = med if med > 1e-9 else 0.01
            return _clip(vol / base, 0.75, 1.45)
        except Exception:
            return 1.0

    def apply_pair(
        self,
        base_cos: float,
        base_ml: float,
        *,
        starvation: Optional[float] = None,
        vol_proxy: Optional[float] = None,
    ) -> ElasticThresholdState:
        starv = float(starvation if starvation is not None else self.compute_starvation_index())
        vol = float(vol_proxy if vol_proxy is not None else self.volatility_proxy())

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

        return ElasticThresholdState(
            cos_cutoff=round(cos, 4),
            ml_cutoff=round(ml, 4),
            stretch_factor=round(stretch, 4),
            scout_gap=round(scout_gap, 4),
            starvation_index=round(starv, 4),
            vol_proxy=round(vol, 4),
        )

    def relief_adjust_autonomous_cutoff(
        self,
        config_key: str,
        current: float,
        *,
        n_closed: int,
        win_rate: Optional[float] = None,
    ) -> Tuple[float, str]:
        """
        system_auto_pilot 엔진 9 대체/강화 — 표본 기아 시 더 공격적으로 허용.
        """
        starv = self.compute_starvation_index()
        state = self.apply_pair(current, current, starvation=starv)
        curr = float(current)

        if n_closed >= 5 and win_rate is not None:
            if win_rate < 0.45:
                new_v = _clip(curr + 0.04 - starv * 0.03, 0.35, 0.90)
                return round(new_v, 2), "defense_wr_low"
            if win_rate > 0.65 and n_closed < 10:
                new_v = _clip(curr - 0.04 - starv * 0.02, 0.32, 0.88)
                return round(new_v, 2), "expand_wr_high"
            return curr, "hold"

        new_v = _clip(state.cos_cutoff, 0.32, 0.88)
        return round(new_v, 2), f"starvation_relief_{starv:.2f}"

    def persist_snapshot(self) -> Dict[str, Any]:
        """system_config.FLUID_ELASTIC_STATE — 리포트·디버그용."""
        cos = float(self.cfg.get("DYNAMIC_SUPERNOVA_CUTOFF", 0.50) or 0.50)
        ml = float(self.cfg.get("DYNAMIC_ML_BOX_CUTOFF", 0.50) or 0.50)
        st = self.apply_pair(cos, ml)
        snap = {
            "market": self.market,
            "at_kst": datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S"),
            "starvation_index": st.starvation_index,
            "vol_proxy": st.vol_proxy,
            "cos_cutoff": st.cos_cutoff,
            "ml_cutoff": st.ml_cutoff,
            "scout_gap": st.scout_gap,
        }
        self.cfg["FLUID_ELASTIC_STATE"] = {self.market: snap}
        return snap


def evaluate_scout_candidate(
    *,
    is_pass_cosine: bool,
    is_pass_ml_box: bool,
    best_cos_sim: float,
    eff_cos_cutoff: float,
    ml_score: float,
    eff_ml_cutoff: float,
    state: ElasticThresholdState,
    sys_config: Optional[Dict[str, Any]] = None,
) -> ScoutVerdict:
    """DNA 합격 실패 직후 — 정찰병 허용 여부."""
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
    """정찰병 투입 상한 — 계좌 대비 최대 0.3% (ELASTIC_SCOUT_INVEST_PCT) 절대 상한."""
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
) -> Tuple[float, float, int]:
    """
  try_add_virtual_position 게이트 — 정찰병 비중 절대 상한.
  Returns (invest_amount, sim_kelly_invest, shares).
    """
    cap = scout_invest_cap(sys_config, float(account_size))
    inv = min(float(invest_amount), cap)
    sk = min(float(sim_kelly_invest), cap)
    ep = float(entry_price) if entry_price else 0.0
    shares = max(1, int(sk / ep)) if ep > 0 else 1
    if ep > 0 and shares * ep > cap:
        shares = max(1, int(cap / ep))
        sk = shares * ep
        inv = min(inv, sk)
    return inv, sk, shares
