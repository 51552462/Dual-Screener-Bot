"""
국면(Regime) × 로직(group_key) 교차 수익/방어 랭킹 — 부가(satellite) 모듈.

기존 글로벌 기준 평가(데스매치)는 그대로 두고, 그 위에 '장세별' 교차 리더보드를 얹는다.
메인 매매·메타 파이프라인(system_auto_pilot / meta_governor)을 전혀 건드리지 않으며,
리포팅·텔레그램 단계에서만 BULL/SIDEWAYS/BEAR/HIGH_VOL 리더보드가 분리되어 출력된다.

설계 3원칙(요구사항):
  1) compute_regime_specific_metrics(df): groupby(["entry_regime","group_key"]) 교차 집계
     (거래수 n · 승률 · PF · 평균수익 · MDD · 최대 연패).
  2) 데스매치 재활용: evolution.deathmatch_scorecard.compute_composite_v2 로
     국면별 수익+방어(지수 MDD·변동성 패널티) 결합 종합점수 산출.
  3) 베이지안 수축: 국면 표본 n 이 MIN_SAMPLES 미만이면 국면점수와 글로벌점수를
     n 비례로 가중평균(shrinkage)하여 소표본 점수 폭주를 차단.
"""
from __future__ import annotations

import html
import math
import os
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

from evolution.deathmatch_battle_royale import ledger_group_key, mdd_pct_from_returns
from evolution.deathmatch_config import load_deathmatch_config, market_deathmatch_params
from evolution.deathmatch_report import (
    _effective_final_ret_pct,
    _profit_factor_from_ret,
    classify_strategy_arm,
)
from evolution.deathmatch_scorecard import (
    ArmScorecard,
    build_arm_scorecard_from_df,
    compute_composite_v2,
)

# 베이지안 수축 기본 최소 표본 (config: REGIME_XRANK_MIN_SAMPLES 로 override)
MIN_SAMPLES = 15

# 리더보드 표시 순서 + 한글 라벨 (상승/횡보/하락 3종 + 고변동)
REGIME_DISPLAY: List[tuple[str, str]] = [
    ("BULL", "🔥 상승장 (BULL)"),
    ("SIDEWAYS", "⚖️ 횡보장 (SIDEWAYS)"),
    ("BEAR", "🧊 하락장 (BEAR)"),
    ("HIGH_VOL", "🌪️ 고변동성 (HIGH_VOL)"),
]
DISPLAY_REGIME_KEYS = {k for k, _ in REGIME_DISPLAY}


def _normalize_regime(value: Any) -> str:
    try:
        from meta_state_store import normalize_regime_key

        return normalize_regime_key(value)
    except Exception:
        u = str(value or "").strip().upper()
        if u in ("CHOP", "WHIPSAW"):
            return "SIDEWAYS"
        if u in ("BULL", "BEAR", "SIDEWAYS", "HIGH_VOL"):
            return u
        return "UNKNOWN"


# ---------------------------------------------------------------------------
# Regime Specialization Tag Quarantine (Item 3 — MAB / Kelly SSOT)
# ---------------------------------------------------------------------------
VALID_REGIME_SPECIALIZATION_TAGS = frozenset(
    {"BULL_ONLY", "BEAR_ONLY", "ALL_WEATHER", "UNCLASSIFIED"}
)
REGIME_TAG_ALLOWED_META: dict[str, frozenset[str]] = {
    "BULL_ONLY": frozenset({"BULL"}),
    "BEAR_ONLY": frozenset({"BEAR", "HIGH_VOL"}),
    "ALL_WEATHER": frozenset({"BULL", "BEAR", "SIDEWAYS", "HIGH_VOL", "UNKNOWN"}),
    "UNCLASSIFIED": frozenset({"BULL", "BEAR", "SIDEWAYS", "HIGH_VOL", "UNKNOWN"}),
}
REGIME_TAG_QUARANTINE_MODE_KEY = "REGIME_TAG_QUARANTINE_MODE"  # kelly_zero | reject


def normalize_regime_specialization_tag(tag: Any) -> str:
    t = str(tag or "UNCLASSIFIED").strip().upper()
    return t if t in VALID_REGIME_SPECIALIZATION_TAGS else "UNCLASSIFIED"


def _quarantine_mode(sys_config: Optional[dict]) -> str:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    mode = str(cfg.get(REGIME_TAG_QUARANTINE_MODE_KEY) or "kelly_zero").strip().lower()
    return mode if mode in ("kelly_zero", "reject") else "kelly_zero"


def regime_tag_compatible(meta_regime_key: Any, regime_tag: Any) -> bool:
    """현재 META_REGIME_KEY 가 strategy regime_tag 와 호환되는지."""
    tag = normalize_regime_specialization_tag(regime_tag)
    if tag in ("ALL_WEATHER", "UNCLASSIFIED"):
        return True
    meta = _normalize_regime(meta_regime_key)
    allowed = REGIME_TAG_ALLOWED_META.get(tag, REGIME_TAG_ALLOWED_META["UNCLASSIFIED"])
    return meta in allowed


def evaluate_regime_tag_quarantine(
    meta_regime_key: Any,
    regime_tag: Any,
    *,
    sys_config: Optional[dict] = None,
) -> dict[str, Any]:
    """
    국면 태그 격리 평가.

    Returns:
      quarantined, kelly_mult (0|1), reject_entry, reason, regime_tag, meta_regime
    """
    tag = normalize_regime_specialization_tag(regime_tag)
    meta = _normalize_regime(meta_regime_key)
    compatible = regime_tag_compatible(meta, tag)
    mode = _quarantine_mode(sys_config)
    if compatible:
        return {
            "quarantined": False,
            "kelly_mult": 1.0,
            "reject_entry": False,
            "reason": "regime_tag_compatible",
            "regime_tag": tag,
            "meta_regime": meta,
            "mode": mode,
        }
    reason = f"regime_tag_mismatch:{tag}@{meta}"
    reject = mode == "reject"
    return {
        "quarantined": True,
        "kelly_mult": 0.0,
        "reject_entry": reject,
        "reason": reason,
        "regime_tag": tag,
        "meta_regime": meta,
        "mode": mode,
    }


def regime_tag_quarantine_kelly_mult(
    meta_regime_key: Any,
    regime_tag: Any,
    *,
    sys_config: Optional[dict] = None,
) -> float:
    """MAB / MetaGovernor group Kelly overlay — 불일치 시 0.0."""
    return float(
        evaluate_regime_tag_quarantine(meta_regime_key, regime_tag, sys_config=sys_config)[
            "kelly_mult"
        ]
    )


def apply_regime_tag_quarantine_to_kelly(
    meta_regime_key: Any,
    regime_tag: Any,
    kelly_risk_pct: float,
    *,
    sys_config: Optional[dict] = None,
) -> dict[str, Any]:
    """Kelly sizing 훅 — 격리 시 kelly_risk_pct → 0."""
    ev = evaluate_regime_tag_quarantine(meta_regime_key, regime_tag, sys_config=sys_config)
    base = float(kelly_risk_pct)
    if "kelly_mult" in ev:
        mult = float(ev["kelly_mult"])
    else:
        mult = 1.0
    return {
        **ev,
        "kelly_risk_pct_before": base,
        "kelly_risk_pct": base * mult,
    }


def _extract_incubator_template_keys(sig_type: Any) -> list[str]:
    """sig_type / group_key 에서 INCUBATOR_TEMPLATES 조회 키 후보."""
    sig = str(sig_type or "")
    keys: list[str] = []
    if "INCUBATOR_" in sig:
        for part in sig.replace("]", " ").replace("[", " ").split():
            p = part.strip()
            if not p:
                continue
            if p.startswith("INCUBATOR_"):
                keys.append(p.replace("INCUBATOR_", "", 1))
            elif p.startswith("GP_MUT_") or p.startswith("OOSVAL_"):
                keys.append(p)
    return list(dict.fromkeys(k for k in keys if k))


def lookup_regime_tag_from_incubator_template(
    sys_config: Optional[dict],
    template_key: Any,
) -> Optional[str]:
    """INCUBATOR_TEMPLATES → regime_tag."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    inc = cfg.get("INCUBATOR_TEMPLATES")
    if not isinstance(inc, dict):
        return None
    key = str(template_key or "").strip()
    if not key:
        return None
    candidates = [key, f"GP_MUT_{key}", f"OOSVAL_{key}"]
    if key.startswith("GP_MUT_"):
        candidates.append(key.replace("GP_MUT_", "", 1))
    if key.startswith("OOSVAL_"):
        candidates.append(key.replace("OOSVAL_", "", 1))
    seen: set[str] = set()
    for cand in candidates:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        tpl = inc.get(cand)
        if isinstance(tpl, dict) and tpl.get("regime_tag"):
            return normalize_regime_specialization_tag(tpl.get("regime_tag"))
    return None


def lookup_regime_tag_from_registry(
    meta_state: Optional[dict],
    group_key: Any,
) -> Optional[str]:
    meta = meta_state if isinstance(meta_state, dict) else {}
    reg = meta.get("META_STRATEGY_REGISTRY")
    if not isinstance(reg, list):
        return None
    gk = str(group_key or "").strip()
    if not gk:
        return None
    for row in reg:
        if not isinstance(row, dict):
            continue
        if str(row.get("group_key") or "").strip() == gk and row.get("regime_tag"):
            return normalize_regime_specialization_tag(row.get("regime_tag"))
    return None


def resolve_regime_tag_for_signal(
    sys_config: Optional[dict],
    *,
    sig_type: Any = None,
    incubator_template_name: Any = None,
    group_key: Any = None,
    facts: Optional[dict] = None,
    meta_state: Optional[dict] = None,
) -> Optional[str]:
    """
    실전 신호/템플릿/레지스트리에서 regime_tag SSOT 해석.
    우선순위: facts → incubator template → sig_type 파싱 → registry group_key.
    """
    if isinstance(facts, dict) and facts.get("regime_tag"):
        return normalize_regime_specialization_tag(facts.get("regime_tag"))

    if incubator_template_name:
        tag = lookup_regime_tag_from_incubator_template(sys_config, incubator_template_name)
        if tag:
            return tag

    for key in _extract_incubator_template_keys(sig_type):
        tag = lookup_regime_tag_from_incubator_template(sys_config, key)
        if tag:
            return tag

    gk = group_key or (str(sig_type or "").split(" [")[0].strip() if sig_type else "")
    tag = lookup_regime_tag_from_registry(meta_state, gk)
    if tag:
        return tag

    return None


def regime_tag_mab_group_mult(
    meta_regime_key: Any,
    regime_tag: Any,
    base_group_mult: float,
    *,
    sys_config: Optional[dict] = None,
) -> tuple[float, dict[str, Any]]:
    """
    MAB META_GROUP_KELLY_MULT × regime_tag 호환 overlay.
    불일치 시 그룹 배수를 0 으로 수렴(Quarantine).
    """
    try:
        base = float(base_group_mult)
    except (TypeError, ValueError):
        base = 1.0
    qm = regime_tag_quarantine_kelly_mult(meta_regime_key, regime_tag, sys_config=sys_config)
    ev = evaluate_regime_tag_quarantine(meta_regime_key, regime_tag, sys_config=sys_config)
    return base * qm, ev


# ---------------------------------------------------------------------------
# Ledger-backed regime_tag inference (Ch.2 — legacy groups without explicit tag)
# ---------------------------------------------------------------------------
_REGIME_INFER_CACHE: dict[str, tuple[str, float]] = {}


def _infer_thresholds(sys_config: Optional[dict]) -> dict[str, float]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    return {
        "min_n": float(int(cfg.get("REGIME_TAG_INFER_MIN_N", 5) or 5)),
        "wr_strong": float(cfg.get("REGIME_TAG_INFER_WR_STRONG", 55.0) or 55.0),
        "wr_weak": float(cfg.get("REGIME_TAG_INFER_WR_WEAK", 25.0) or 25.0),
        "spread_min": float(cfg.get("REGIME_TAG_INFER_SPREAD_MIN", 25.0) or 25.0),
        "lookback_days": float(int(cfg.get("REGIME_TAG_INFER_LOOKBACK_DAYS", 90) or 90)),
    }


def classify_regime_tag_from_wr_table(
    regime_wr: dict[str, tuple[int, float]],
    *,
    sys_config: Optional[dict] = None,
) -> Optional[str]:
    """
    entry_regime → (n, win_rate_pct) 에서 BULL_ONLY/BEAR_ONLY 추론.
    표본 부족·차이 미미 시 None (격리 미적용).
    """
    th = _infer_thresholds(sys_config)
    min_n = int(th["min_n"])

    def _wr(reg: str) -> tuple[int, float]:
        n, w = regime_wr.get(reg, (0, 0.0))
        return int(n), float(w)

    n_bull, wr_bull = _wr("BULL")
    n_bear, wr_bear = _wr("BEAR")
    n_side, wr_side = _wr("SIDEWAYS")
    n_hv, wr_hv = _wr("HIGH_VOL")

    if n_bull >= min_n and n_bear >= min_n:
        if (
            wr_bull >= th["wr_strong"]
            and wr_bear <= th["wr_weak"]
            and (wr_bull - wr_bear) >= th["spread_min"]
        ):
            return "BULL_ONLY"
        if (
            wr_bear >= th["wr_strong"]
            and wr_bull <= th["wr_weak"]
            and (wr_bear - wr_bull) >= th["spread_min"]
        ):
            return "BEAR_ONLY"

    if n_bear >= min_n and n_bull >= min_n:
        if wr_bear >= th["wr_strong"] and wr_bull <= th["wr_weak"]:
            return "BEAR_ONLY"
        if wr_bull >= th["wr_strong"] and wr_bear <= th["wr_weak"]:
            return "BULL_ONLY"

    # 고변동 전용 방어형
    if n_hv >= min_n and wr_hv >= th["wr_strong"]:
        if n_bull >= min_n and wr_bull <= th["wr_weak"]:
            return "BEAR_ONLY"

    if n_side >= min_n * 2 and n_bull < min_n and n_bear < min_n:
        return "ALL_WEATHER"

    return None


def infer_regime_tag_from_ledger(
    conn: Any,
    market: str,
    group_key: str,
    *,
    sys_config: Optional[dict] = None,
) -> Optional[str]:
    """forward_trades 청산 원장에서 group_key 국면별 승률로 regime_tag 추론."""
    cfg = sys_config if isinstance(sys_config, dict) else {}
    if not cfg.get("REGIME_TAG_INFER_FROM_LEDGER", True):
        return None
    gk = str(group_key or "").strip()
    mkt = str(market or "").upper()
    if not gk or gk == "UNKNOWN" or conn is None:
        return None

    cache_key = f"{mkt}:{gk}"
    import time

    now = time.time()
    cached = _REGIME_INFER_CACHE.get(cache_key)
    if cached and (now - cached[1]) < 300.0:
        tag = cached[0]
        return tag if tag else None

    th = _infer_thresholds(cfg)
    lookback = int(th["lookback_days"])
    try:
        rows = conn.execute(
            """
            SELECT entry_regime, final_ret
            FROM forward_trades
            WHERE status LIKE 'CLOSED%%'
              AND UPPER(market) = ?
              AND sig_type LIKE ?
              AND entry_date >= date('now', ?)
            """,
            (mkt, f"%{gk}%", f"-{lookback} days"),
        ).fetchall()
    except Exception:
        _REGIME_INFER_CACHE[cache_key] = ("", now)
        return None

    buckets: dict[str, list[float]] = {}
    for er, fr in rows:
        reg = _normalize_regime(er)
        try:
            r = float(fr)
        except (TypeError, ValueError):
            continue
        buckets.setdefault(reg, []).append(r)

    regime_wr: dict[str, tuple[int, float]] = {}
    for reg, rets in buckets.items():
        n = len(rets)
        wins = sum(1 for x in rets if x > 0)
        wr = (wins / n * 100.0) if n else 0.0
        regime_wr[reg] = (n, wr)

    tag = classify_regime_tag_from_wr_table(regime_wr, sys_config=cfg)
    _REGIME_INFER_CACHE[cache_key] = (tag or "", now)
    return tag


def resolve_regime_tag_for_entry(
    sys_config: Optional[dict],
    *,
    sig_type: Any = None,
    incubator_template_name: Any = None,
    group_key: Any = None,
    facts: Optional[dict] = None,
    meta_state: Optional[dict] = None,
    conn: Any = None,
    market: Any = None,
) -> tuple[Optional[str], str]:
    """
    regime_tag SSOT + ledger 추론 폴백.

    Returns:
      (tag or None, source) — source: facts|incubator|registry|ledger_infer|none
    """
    direct = resolve_regime_tag_for_signal(
        sys_config,
        sig_type=sig_type,
        incubator_template_name=incubator_template_name,
        group_key=group_key,
        facts=facts,
        meta_state=meta_state,
    )
    if direct:
        if isinstance(facts, dict) and facts.get("regime_tag"):
            return direct, "facts"
        if incubator_template_name:
            return direct, "incubator"
        return direct, "registry"

    gk = group_key or (str(sig_type or "").split(" [")[0].strip() if sig_type else "")
    inferred = infer_regime_tag_from_ledger(
        conn, str(market or ""), str(gk or ""), sys_config=sys_config
    )
    if inferred:
        return normalize_regime_specialization_tag(inferred), "ledger_infer"
    return None, "none"


def count_regime_mismatch_trades(
    df: pd.DataFrame,
    meta_regime_key: Any,
    *,
    sys_config: Optional[dict] = None,
    meta_state: Optional[dict] = None,
    conn: Any = None,
) -> int:
    """프레임 각 행의 group_key regime_tag vs META_REGIME 불일치 건수."""
    if df is None or df.empty or "sig_type" not in df.columns:
        return 0
    from evolution.deathmatch_battle_royale import ledger_group_key

    meta = _normalize_regime(meta_regime_key)
    hits = 0
    for _, row in df.iterrows():
        sig = str(row.get("sig_type") or "")
        if "INCUBATOR" in sig.upper():
            continue
        gk = ledger_group_key(sig)
        mkt = str(row.get("market") or "KR")
        tag, _src = resolve_regime_tag_for_entry(
            sys_config,
            sig_type=sig,
            group_key=gk,
            meta_state=meta_state,
            conn=conn,
            market=mkt,
        )
        if not tag or tag in ("ALL_WEATHER", "UNCLASSIFIED"):
            continue
        if not regime_tag_compatible(meta, tag):
            hits += 1
    return hits


def _resolve_min_samples(sys_config: Optional[dict]) -> int:
    if isinstance(sys_config, dict):
        try:
            v = int(sys_config.get("REGIME_XRANK_MIN_SAMPLES", MIN_SAMPLES))
            return max(1, v)
        except (TypeError, ValueError):
            pass
    return MIN_SAMPLES


def resolve_regime_crossmatrix_min_samples(
    sys_config: Optional[dict] = None,
) -> int:
    """
    국면 교차매트릭스 베이지안 수축 최소 표본 SSOT.
    Re-Evolution 섀도우 부활전·리더보드 등에서 동일 기준으로 재사용.
    """
    return _resolve_min_samples(sys_config)


def _max_consecutive_losses(ret_ordered: pd.Series) -> int:
    """시간순 정렬된 final_ret 에서 최장 연속 손실(≤0) 길이."""
    streak = 0
    worst = 0
    for v in ret_ordered:
        try:
            x = float(v)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(x):
            continue
        if x <= 0:
            streak += 1
            worst = max(worst, streak)
        else:
            streak = 0
    return int(worst)


def _prepare_frame(
    df: pd.DataFrame,
    *,
    group_key_fn: Callable[[Any], str] = ledger_group_key,
    exclude_incubator: bool = True,
) -> pd.DataFrame:
    """entry_regime(정규화) + group_key 컬럼을 보강한 작업 프레임."""
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    if "sig_type" not in work.columns:
        work["sig_type"] = ""
    # 로직군(group_key) — 데스매치 SSOT 와 동일한 키
    work["group_key"] = work["sig_type"].map(lambda s: group_key_fn(str(s or "")))
    if exclude_incubator:
        # INCUBATOR/SCOUT/관찰용은 라이브 로직 랭킹에서 제외 (classify 가 None 반환)
        keep = work["sig_type"].map(lambda s: classify_strategy_arm(s) is not None)
        work = work[keep]
    if work.empty:
        return work
    rk_col = "entry_regime" if "entry_regime" in work.columns else None
    if rk_col is None:
        work["entry_regime"] = "UNKNOWN"
    work["entry_regime"] = work["entry_regime"].map(_normalize_regime)
    # 정렬 키(연패 계산용)
    order_col = "exit_date" if "exit_date" in work.columns else (
        "entry_date" if "entry_date" in work.columns else None
    )
    work["_order"] = (
        work[order_col].astype(str) if order_col else pd.Series(range(len(work)), index=work.index).astype(str)
    )
    return work


# ===========================================================================
# 요구사항 1 — 국면 × 로직 교차 집계기
# ===========================================================================
def compute_regime_specific_metrics(
    df: pd.DataFrame,
    *,
    group_key_fn: Callable[[Any], str] = ledger_group_key,
    exclude_incubator: bool = True,
) -> pd.DataFrame:
    """
    forward_trades 청산 원장 → groupby(["entry_regime","group_key"]) 교차 집계.

    반환 DataFrame 컬럼:
      entry_regime, group_key, n, n_valid, win_rate(0~1), profit_factor,
      mean_ret(%), mdd_pct(%), vol_pct(%), max_consec_losses
    """
    work = _prepare_frame(df, group_key_fn=group_key_fn, exclude_incubator=exclude_incubator)
    cols = [
        "entry_regime", "group_key", "n", "n_valid", "win_rate",
        "profit_factor", "mean_ret", "mdd_pct", "vol_pct", "max_consec_losses",
    ]
    if work.empty:
        return pd.DataFrame(columns=cols)

    rows: List[Dict[str, Any]] = []
    for (regime, gk), grp in work.groupby(["entry_regime", "group_key"], sort=False):
        grp_ordered = grp.sort_values("_order")
        ret = _effective_final_ret_pct(grp_ordered)
        valid = ret.dropna()
        n = int(len(grp_ordered))
        n_valid = int(len(valid))
        win_rate = float((valid > 0).sum() / n_valid) if n_valid else float("nan")
        pf = _profit_factor_from_ret(valid)
        mean_ret = float(valid.mean()) if n_valid else float("nan")
        vol = float(valid.std()) if n_valid > 1 else 0.0
        mdd = mdd_pct_from_returns(valid.tolist()) if n_valid >= 2 else 0.0
        mcl = _max_consecutive_losses(ret.reindex(grp_ordered.index))
        rows.append({
            "entry_regime": str(regime),
            "group_key": str(gk),
            "n": n,
            "n_valid": n_valid,
            "win_rate": round(win_rate, 4) if math.isfinite(win_rate) else None,
            "profit_factor": round(pf, 4) if pf is not None else None,
            "mean_ret": round(mean_ret, 4) if math.isfinite(mean_ret) else None,
            "mdd_pct": round(mdd, 4),
            "vol_pct": round(vol, 4),
            "max_consec_losses": mcl,
        })
    out = pd.DataFrame(rows, columns=cols)
    return out


# ===========================================================================
# 요구사항 2 + 3 — 데스매치 Composite v2 재활용 + 베이지안 수축
# ===========================================================================
@dataclass
class RegimeLogicCell:
    entry_regime: str
    group_key: str
    label: str
    n: int = 0
    n_valid: int = 0
    win_rate_pct: Optional[float] = None
    profit_factor: Optional[float] = None
    mean_ret: Optional[float] = None
    mdd_pct: float = 0.0
    vol_pct: float = 0.0
    max_consec_losses: int = 0
    regime_composite: float = 0.0     # 국면 풀 내부 Composite v2
    global_composite: float = 0.0     # 전체(글로벌) Composite v2
    shrink_weight: float = 0.0        # n/(n+MIN)
    shrunk_score: float = 0.0         # 베이지안 가중평균 결과(최종 랭킹 기준)
    low_sample: bool = False
    rank: int = 999


@dataclass
class RegimeLeaderboard:
    entry_regime: str
    display_label: str
    cells: List[RegimeLogicCell] = field(default_factory=list)
    benchmark_ret: Optional[float] = None
    min_samples: int = MIN_SAMPLES


def _market_benchmark_ret(df_pool: pd.DataFrame) -> Optional[float]:
    if df_pool is None or df_pool.empty:
        return None
    ret = _effective_final_ret_pct(df_pool).dropna()
    if ret.empty:
        return None
    m = float(ret.mean())
    return m if math.isfinite(m) else None


def _scorecards_for_pool(df_pool: pd.DataFrame, dmcfg: Dict[str, Any]) -> Dict[str, ArmScorecard]:
    """풀(전체 또는 국면) → group_key별 ArmScorecard(Composite v2 적용)."""
    by_group: Dict[str, List[Any]] = {}
    for idx, row in df_pool.iterrows():
        gk = str(row.get("group_key") or "UNKNOWN")
        by_group.setdefault(gk, []).append(idx)

    arms: List[ArmScorecard] = []
    for gk, idxs in by_group.items():
        df_arm = df_pool.loc[idxs].sort_values("_order")
        ret = _effective_final_ret_pct(df_arm).dropna()
        mdd = mdd_pct_from_returns(ret.tolist()) if len(ret) >= 2 else 0.0
        sc = build_arm_scorecard_from_df(
            arm_id=gk, label=gk, group_key=gk, registry_state="OBSERVING",
            df_arm=df_arm, mdd_pct=mdd,
        )
        arms.append(sc)

    bench = _market_benchmark_ret(df_pool)
    compute_composite_v2(arms, dmcfg, market_benchmark=bench)
    return {a.group_key: a for a in arms}


def compute_regime_leaderboards(
    df: pd.DataFrame,
    sys_config: Optional[dict] = None,
    *,
    market: str = "KR",
    group_key_fn: Callable[[Any], str] = ledger_group_key,
    min_samples: Optional[int] = None,
    display_only: bool = True,
) -> List[RegimeLeaderboard]:
    """
    국면별 종합 순위표(수익+방어 결합) — 데스매치 Composite v2 재활용 + 베이지안 수축.

    shrunk = w·regime_composite + (1-w)·global_composite,  w = n_valid/(n_valid+MIN)
    """
    cfg = sys_config if isinstance(sys_config, dict) else {}
    min_n = int(min_samples) if min_samples is not None else _resolve_min_samples(cfg)
    dmcfg = market_deathmatch_params(load_deathmatch_config(cfg), market)

    work = _prepare_frame(df, group_key_fn=group_key_fn)
    if work.empty:
        return []

    # 글로벌 기준 점수(수축의 사전분포 역할) — 전 국면 합산 풀
    global_cards = _scorecards_for_pool(work, dmcfg)

    boards: List[RegimeLeaderboard] = []
    present = list(work["entry_regime"].unique())
    ordered = [k for k, _ in REGIME_DISPLAY if k in present]
    if not display_only:
        ordered += [k for k in present if k not in DISPLAY_REGIME_KEYS]
    label_map = dict(REGIME_DISPLAY)

    for regime in ordered:
        sub = work[work["entry_regime"] == regime]
        if sub.empty:
            continue
        regime_cards = _scorecards_for_pool(sub, dmcfg)
        bench = _market_benchmark_ret(sub)
        board = RegimeLeaderboard(
            entry_regime=regime,
            display_label=label_map.get(regime, regime),
            benchmark_ret=bench,
            min_samples=min_n,
        )
        for gk, rc in regime_cards.items():
            n_valid = int(rc.n_valid)
            w = float(n_valid) / float(n_valid + min_n) if (n_valid + min_n) > 0 else 0.0
            g_comp = float(global_cards[gk].composite_score) if gk in global_cards else 0.0
            shrunk = w * float(rc.composite_score) + (1.0 - w) * g_comp
            board.cells.append(RegimeLogicCell(
                entry_regime=regime,
                group_key=gk,
                label=gk,
                n=int(rc.n_closed),
                n_valid=n_valid,
                win_rate_pct=rc.win_rate_pct,
                profit_factor=rc.profit_factor,
                mean_ret=rc.mean_ret,
                mdd_pct=float(rc.mdd_pct),
                vol_pct=float(rc.vol_pct),
                max_consec_losses=_max_consecutive_losses(
                    _effective_final_ret_pct(sub[sub["group_key"] == gk].sort_values("_order"))
                ),
                regime_composite=float(rc.composite_score),
                global_composite=g_comp,
                shrink_weight=round(w, 4),
                shrunk_score=float(shrunk),
                low_sample=n_valid < min_n,
            ))
        board.cells.sort(key=lambda c: c.shrunk_score, reverse=True)
        for i, c in enumerate(board.cells, start=1):
            c.rank = i
        boards.append(board)
    return boards


# ===========================================================================
# 텔레그램 출력 (국면별 분리)
# ===========================================================================
def format_regime_leaderboards_telegram(
    market_icon: str,
    market: str,
    boards: List[RegimeLeaderboard],
    *,
    lookback_label: str = "전체 청산",
    top_n: int = 8,
) -> str:
    mk = "KR" if str(market).upper() == "KR" else "US" if str(market).upper() == "US" else str(market)
    lines: List[str] = [
        f"{market_icon} <b>[국면별 로직 수익·방어 교차 랭킹 — {mk}]</b>",
        f"📎 {html.escape(lookback_label, quote=False)} · "
        f"베이지안 수축(MIN={boards[0].min_samples if boards else MIN_SAMPLES}) · "
        f"<i>Composite v2(수익+지수MDD·변동성 방어)</i>",
    ]
    if not boards:
        lines.append(" ↳ 국면·로직 분류 가능 청산 표본 없음")
        return "\n".join(lines) + "\n"

    for board in boards:
        bench_s = (
            f" · 국면벤치 {board.benchmark_ret:+.2f}%"
            if board.benchmark_ret is not None and math.isfinite(board.benchmark_ret)
            else ""
        )
        lines.append("")
        lines.append(f"<b>{board.display_label}</b>{bench_s}")
        if not board.cells:
            lines.append(" ↳ 표본 없음")
            continue
        for c in board.cells[:top_n]:
            ret_s = f"{c.mean_ret:+.2f}%" if c.mean_ret is not None else "—"
            wr_s = f"{c.win_rate_pct:.1f}%" if c.win_rate_pct is not None else "—"
            pf_s = f"{c.profit_factor:.2f}" if c.profit_factor is not None else "—"
            icon = "🥇" if c.rank == 1 else f"{c.rank}."
            flag = " 🔬저표본수축" if c.low_sample else ""
            lines.append(
                f" {icon} <b>{html.escape(c.label, quote=False)}</b>{flag}\n"
                f"    {ret_s} · 승률 {wr_s} · PF {pf_s} · MDD {c.mdd_pct:.1f}% · "
                f"최대연패 {c.max_consec_losses} · N={c.n}(유효{c.n_valid})\n"
                f"    <b>점수 {c.shrunk_score:+.2f}</b> "
                f"<i>(국면{c.regime_composite:+.2f}×{c.shrink_weight:.2f} + 글로벌{c.global_composite:+.2f})</i>"
            )
    lines.append("")
    lines.append(
        "<i>※ 부가 분석 — 기존 글로벌 데스매치/메타 파이프라인과 독립. 자본배분 미연동(관측 전용).</i>"
    )
    return "\n".join(lines) + "\n"


# ===========================================================================
# 로더 + 발송 (리포팅 단계 전용, 메인 파이프라인 미접촉)
# ===========================================================================
def load_closed_trades_for_regime_report(
    market: Optional[str] = None,
    *,
    lookback_days: int = 180,
) -> pd.DataFrame:
    """forward_trades 청산 원장 RO 로드 (report DB). 메인 파이프라인 미접촉."""
    try:
        from market_db_paths import report_db_read_path

        db = report_db_read_path()
    except Exception:
        from market_db_paths import MARKET_DATA_DB_PATH as db  # type: ignore

    if not db or not os.path.isfile(db):
        return pd.DataFrame()
    uri = f"file:{str(db).replace(os.sep, '/')}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        try:
            conn.execute("PRAGMA query_only=ON;")
            df = pd.read_sql(
                "SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%'", conn
            )
        finally:
            conn.close()
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    if market and "market" in df.columns:
        df = df[df["market"].astype(str).str.upper().str.strip() == str(market).upper()]
    if lookback_days and lookback_days > 0 and "exit_date" in df.columns:
        cutoff = (pd.Timestamp.now() - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        df = df[df["exit_date"].astype(str) >= cutoff]
    return df.reset_index(drop=True)


def build_regime_logic_report(
    df_closed: Optional[pd.DataFrame] = None,
    sys_config: Optional[dict] = None,
    *,
    market: str = "KR",
    lookback_days: int = 180,
) -> tuple[List[RegimeLeaderboard], str]:
    """국면별 리더보드 + 텔레그램 HTML 한 번에. df 미제공 시 RO 로드."""
    if df_closed is None:
        df_closed = load_closed_trades_for_regime_report(market, lookback_days=lookback_days)
    boards = compute_regime_leaderboards(df_closed, sys_config, market=market)
    icon = "🇰🇷" if str(market).upper() == "KR" else "🇺🇸"
    label = f"최근 {lookback_days}일 청산" if lookback_days else "전체 청산"
    text = format_regime_leaderboards_telegram(icon, market, boards, lookback_label=label)
    return boards, text


def send_regime_logic_leaderboards(
    market: str = "KR",
    *,
    send_fn: Optional[Callable[[str], Any]] = None,
    sys_config: Optional[dict] = None,
    lookback_days: int = 180,
    df_closed: Optional[pd.DataFrame] = None,
) -> str:
    """리포팅/텔레그램 단계 호출용. send_fn 미제공 시 forward.shared.send_telegram_msg."""
    if sys_config is None:
        try:
            from config_manager import load_system_config

            sys_config = load_system_config() or {}
        except Exception:
            sys_config = {}
    _, text = build_regime_logic_report(
        df_closed, sys_config, market=market, lookback_days=lookback_days
    )
    if send_fn is None:
        try:
            from forward.shared import send_telegram_msg as send_fn  # type: ignore
        except Exception:
            send_fn = None
    if send_fn is not None:
        try:
            send_fn(text)
        except Exception:
            pass
    return text


if __name__ == "__main__":
    import sys

    os.environ.setdefault("PYTHONUTF8", "1")
    mk = sys.argv[1].upper() if len(sys.argv) > 1 else "KR"
    boards, text = build_regime_logic_report(market=mk)
    print(text)
