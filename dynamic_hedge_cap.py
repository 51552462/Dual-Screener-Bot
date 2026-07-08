"""
Dynamic Hedge Cap — Self-Evolution Hedge Engine (Axes 1–2).

Axis 1 (내재적 패닉 스케일링): self_evolution_hedge_engine → ensemble 5팩터 Base Cap.
Axis 2 (RL Hedge): forward_trades 인버스 슬리브 5일 실현 PnL 피드백.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

# 레거시 폴백(정적 30%) — resolve 실패 시만 사용
INVERSE_CAP_STATIC_FALLBACK = 0.30
INVERSE_SIG_MARKER = "[INVERSE_ETF]"

# Architect 스펙: 완만 / 가속 / 패닉
CAP_BY_BEAR_PHASE: Dict[str, float] = {
    "BEAR_GRIND": 0.20,
    "BEAR_ACCEL": 0.35,
    "BEAR_PANIC": 0.50,
    "NEUTRAL": 0.25,
}

# ensemble score 임계 (predictive_regime_ensemble 와 정합)
DOWN_THRESHOLD = -0.18
ACCEL_SCORE_THRESHOLD = -0.35
VIX_PANIC_ABS = 30.0
VIX_GRIND_CEILING = 25.0
DD_20D_ACCEL_PCT = -8.0

# P2 — Hedge Efficacy RL
HEDGE_EFFICACY_LOOKBACK_DAYS = 5
HEDGE_EFFICACY_MIN_CLOSES = 1
HEDGE_EFFICACY_PROFIT_MULT = 1.0
HEDGE_EFFICACY_LOSS_MULT = 0.50
HEDGE_EFFICACY_WHIPSAW_MAX_CAP = 0.15
HEDGE_EFFICACY_EPS = 1e-9


def classify_bear_stress_phase(
    ensemble_score: float,
    vix_level: float,
    *,
    dd_20d_pct: Optional[float] = None,
    defcon_level: int = 5,
) -> str:
    """
    BEAR 서브타입 분류 — PANIC > ACCEL > GRIND > NEUTRAL.

    BEAR_PANIC: VIX ≥ 30 또는 DEFCON ≤ 2
    BEAR_ACCEL: score ≤ −0.35 또는 20d DD ≤ −8%
    BEAR_GRIND: score ≤ −0.18 (완만 하락), VIX < 25 가정(패닉/가속 선행)
    """
    try:
        vix = float(vix_level)
    except (TypeError, ValueError):
        vix = 18.0
    try:
        score = float(ensemble_score)
    except (TypeError, ValueError):
        score = 0.0
    try:
        defcon = int(defcon_level)
    except (TypeError, ValueError):
        defcon = 5

    if vix >= VIX_PANIC_ABS or defcon <= 2:
        return "BEAR_PANIC"
    if score <= ACCEL_SCORE_THRESHOLD:
        return "BEAR_ACCEL"
    if dd_20d_pct is not None:
        try:
            if float(dd_20d_pct) <= DD_20D_ACCEL_PCT:
                return "BEAR_ACCEL"
        except (TypeError, ValueError):
            pass
    if score <= DOWN_THRESHOLD and vix < VIX_GRIND_CEILING:
        return "BEAR_GRIND"
    if score <= DOWN_THRESHOLD:
        return "BEAR_ACCEL"
    return "NEUTRAL"


def cap_pct_for_bear_phase(phase: str) -> float:
    return float(CAP_BY_BEAR_PHASE.get(str(phase or "").upper(), CAP_BY_BEAR_PHASE["NEUTRAL"]))


def _ensemble_score_from_config(cfg: dict[str, Any], market: str) -> Optional[float]:
    """REGIME_ENSEMBLE 캐시 → 시장별 blended score (읽기 전용)."""
    re = cfg.get("REGIME_ENSEMBLE")
    if not isinstance(re, dict):
        return None
    mk_map = re.get("markets")
    if not isinstance(mk_map, dict):
        return None
    blk = mk_map.get(str(market or "").upper())
    if not isinstance(blk, dict):
        return None
    try:
        return float(blk.get("score"))
    except (TypeError, ValueError):
        return None


def _defcon_level_from_config(cfg: dict[str, Any]) -> int:
    dd = cfg.get("DOOMSDAY_DEFCON")
    if not isinstance(dd, dict):
        return 5
    try:
        return int(dd.get("level", 5))
    except (TypeError, ValueError):
        return 5


def _fetch_vix_level(*, market: str = "US") -> Optional[float]:
    """^VIX (US) 또는 VKOSPI (KR) 최근 종가 — 실패 시 None."""
    mkt = str(market or "US").upper()
    try:
        if mkt == "KR":
            from predictive_regime_ensemble import _fetch_vkospi

            vk, _ = _fetch_vkospi()
            if vk is not None and vk > 0:
                return float(vk)
    except Exception:
        pass
    try:
        import yfinance as yf

        hist = yf.Ticker("^VIX").history(period="5d", auto_adjust=True)
        if hist is not None and not hist.empty and "Close" in hist.columns:
            v = float(hist["Close"].dropna().iloc[-1])
            return v if v > 0 else None
    except Exception:
        pass
    return None


def _fetch_benchmark_20d_return_pct(market: str) -> Optional[float]:
    """SPY / ^KS11 최근 ~20거래일 누적 수익률(%)."""
    sym = "^KS11" if str(market or "").upper() == "KR" else "SPY"
    try:
        import yfinance as yf

        hist = yf.Ticker(sym).history(period="2mo", auto_adjust=True)
        if hist is None or hist.empty or "Close" not in hist.columns:
            return None
        c = hist["Close"].astype(float).dropna()
        if len(c) < 2:
            return None
        tail = c.iloc[-20:] if len(c) >= 20 else c
        ret = (float(tail.iloc[-1]) / float(tail.iloc[0]) - 1.0) * 100.0
        return float(ret)
    except Exception:
        return None


def _default_db_path() -> Optional[str]:
    try:
        from market_db_paths import MARKET_DATA_DB_PATH

        return MARKET_DATA_DB_PATH
    except Exception:
        try:
            from auto_forward_tester import DB_PATH

            return DB_PATH
        except Exception:
            return None


def fetch_inverse_sleeve_realized_stats(
    market: str,
    *,
    lookback_days: int = HEDGE_EFFICACY_LOOKBACK_DAYS,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    최근 N일 인버스 슬리브 실현 성과 — forward_trades [INVERSE_ETF] CLOSED 행 SSOT.

    live_nav_manager 는 롱 NAV SSOT; 인버스는 테일 펀드 + forward_trades 마커로 격리 추적.
    """
    mkt = str(market or "US").upper()
    out: Dict[str, Any] = {
        "market": mkt,
        "lookback_days": int(lookback_days),
        "n_closed": 0,
        "weighted_ret_pct": 0.0,
        "total_invest": 0.0,
        "verdict": "insufficient",
    }
    path = db_path or _default_db_path()
    if not path:
        return out

    cutoff = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")
    inv_like = f"%{INVERSE_SIG_MARKER}%"
    conn = None
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            conn.execute("PRAGMA query_only=ON;")
        except Exception:
            pass
        rows = conn.execute(
            """
            SELECT final_ret, invest_amount, sim_kelly_invest
            FROM forward_trades
            WHERE UPPER(TRIM(market)) = ?
              AND status LIKE 'CLOSED%'
              AND final_ret IS NOT NULL
              AND IFNULL(sig_type,'') LIKE ?
              AND IFNULL(sig_type,'') NOT LIKE '%OPEN_SHADOW%'
              AND COALESCE(NULLIF(TRIM(exit_date), ''), entry_date) >= ?
            """,
            (mkt, inv_like, cutoff),
        ).fetchall()
    except Exception:
        return out
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    weighted_sum = 0.0
    invest_sum = 0.0
    n = 0
    for row in rows or []:
        try:
            ret = float(row[0])
        except (TypeError, ValueError):
            continue
        try:
            inv = float(row[1] if row[1] is not None else row[2] or 0.0)
        except (TypeError, ValueError):
            inv = 0.0
        if inv <= 0:
            inv = 1.0
        weighted_sum += ret * inv
        invest_sum += inv
        n += 1

    out["n_closed"] = n
    out["total_invest"] = round(invest_sum, 2)
    if n < HEDGE_EFFICACY_MIN_CLOSES or invest_sum <= 0:
        return out

    wret = weighted_sum / invest_sum
    out["weighted_ret_pct"] = round(float(wret), 4)
    if wret > HEDGE_EFFICACY_EPS:
        out["verdict"] = "profitable"
    else:
        out["verdict"] = "whipsaw"
    return out


def apply_hedge_efficacy_rl(
    macro_cap_pct: float,
    sleeve_stats: Dict[str, Any],
) -> Tuple[float, Dict[str, Any]]:
    """
    Hedge Efficacy RL — macro stress 캡에 실현 PnL 피드백 적용.

    profitable → macro × 1.0 (100% 부여)
    whipsaw     → min(macro × 0.5, 15%) — 가짜 하락/횡보 휩쏘 방어
    insufficient → macro 그대로 (판단 유보)
    """
    try:
        macro = float(macro_cap_pct)
    except (TypeError, ValueError):
        macro = CAP_BY_BEAR_PHASE["NEUTRAL"]

    verdict = str(sleeve_stats.get("verdict") or "insufficient")
    n_closed = int(sleeve_stats.get("n_closed") or 0)
    wret = float(sleeve_stats.get("weighted_ret_pct") or 0.0)

    if verdict == "profitable":
        mult = HEDGE_EFFICACY_PROFIT_MULT
        final = macro * mult
        mode = "efficacy_full"
    elif verdict == "whipsaw":
        mult = HEDGE_EFFICACY_LOSS_MULT
        final = min(macro * mult, HEDGE_EFFICACY_WHIPSAW_MAX_CAP)
        mode = "efficacy_whipsaw_shrink"
    else:
        mult = 1.0
        final = macro
        mode = "efficacy_neutral"

    audit = {
        "efficacy_mode": mode,
        "efficacy_verdict": verdict,
        "efficacy_mult": round(mult, 4),
        "macro_cap_pct": round(macro, 4),
        "final_cap_pct": round(final, 4),
        "inverse_sleeve_n_closed": n_closed,
        "inverse_sleeve_5d_ret_pct": round(wret, 4),
        "lookback_days": sleeve_stats.get("lookback_days", HEDGE_EFFICACY_LOOKBACK_DAYS),
    }
    return final, audit


def resolve_macro_stress_cap_pct(
    market: str,
    sys_config: Optional[dict[str, Any]] = None,
) -> Tuple[float, Dict[str, Any]]:
    """Axis 1 — ensemble 내재 Base Cap (self_evolution_hedge_engine SSOT)."""
    from self_evolution_hedge_engine import resolve_intrinsic_base_cap_pct

    return resolve_intrinsic_base_cap_pct(market, sys_config)


def resolve_dynamic_inverse_cap_pct(
    market: str,
    sys_config: Optional[dict[str, Any]] = None,
    *,
    db_path: Optional[str] = None,
) -> Tuple[float, Dict[str, Any]]:
    """
    Self-Evolution Hedge Engine — Axis1 Base Cap × Axis2 RL Hedge.

    반환: (final_cap_pct, audit_meta)
    """
    from self_evolution_hedge_engine import resolve_self_evolution_hedge_cap_pct

    return resolve_self_evolution_hedge_cap_pct(market, sys_config, db_path=db_path)


