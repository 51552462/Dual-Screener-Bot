"""
AceFactPack — LLM·Validator 입력용 구조화 팩트 (환각 방지).
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import pandas as pd

from ace_evolution_ttl import _kst_today


def _compress_logic(sig: str) -> str:
    return re.sub(r"\[.*?\]", "", str(sig or "")).strip()[:48]


def build_top3_rows(ace_df: pd.DataFrame, *, top_n: int = 3) -> List[Dict[str, Any]]:
    if ace_df is None or ace_df.empty:
        return []
    q = ace_df.copy()
    if "final_ret" in q.columns:
        q = q.sort_values("final_ret", ascending=False)
    rows: List[Dict[str, Any]] = []
    for _, r in q.head(top_n).iterrows():
        rows.append(
            {
                "code": str(r.get("code", "")).strip(),
                "name": str(r.get("name", "")).strip(),
                "final_ret_pct": float(pd.to_numeric(r.get("final_ret"), errors="coerce") or 0),
                "sector": str(r.get("sector", "")).strip(),
                "dyn_cpv": _num(r, "dyn_cpv"),
                "dyn_rs": _num(r, "dyn_rs"),
                "dyn_tb": _num(r, "dyn_tb"),
                "v_energy": _num(r, "v_energy"),
                "total_score": _num(r, "total_score"),
                "sig_type": _compress_logic(r.get("sig_type", "")),
            }
        )
    return rows


def _num(row: Any, col: str) -> Optional[float]:
    try:
        v = pd.to_numeric(row.get(col), errors="coerce")
        if pd.isna(v):
            return None
        return round(float(v), 4)
    except Exception:
        return None


def build_feature_stats(
    insights: List[Any],
    *,
    max_features: int = 3,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for ins in insights[:max_features]:
        out.append(
            {
                "column": getattr(ins, "column", ""),
                "label": getattr(ins, "label", ""),
                "ace_summary": getattr(ins, "ace_summary", ""),
                "baseline_summary": getattr(ins, "baseline_summary", ""),
                "p_value": round(float(getattr(ins, "p_value", 1.0)), 4),
                "effect_size": round(float(getattr(ins, "effect_size", 0.0)), 3),
                "discrimination": round(float(getattr(ins, "discrimination", 0.0)), 3),
            }
        )
    return out


def build_ace_fact_pack(
    *,
    market: str,
    logic_core: str,
    ace_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    feature_insights: List[Any],
    sector_summary: str,
    window_days: int,
    data_anchor: str,
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = sys_config if isinstance(sys_config, dict) else {}
    meta = meta if isinstance(meta, dict) else {}
    m = str(market).upper()

    spill = ""
    pred = ""
    if m == "KR":
        spill = str(cfg.get("US_SPILLOVER_SECTOR") or cfg.get("US_SPILLOVER_SECTOR_LAST_GOOD") or "")
        pred = str(cfg.get("PREDICTED_NEXT_SECTOR_KR") or "")
    else:
        pred = str(cfg.get("PREDICTED_NEXT_SECTOR_US") or "")
        spill = str(cfg.get("US_SPILLOVER_SECTOR") or "")

    return {
        "market": m,
        "as_of_kst": _kst_today(),
        "logic_core": logic_core,
        "window_days": int(window_days),
        "data_anchor": str(data_anchor or "")[:10],
        "n_ace": int(len(ace_df)) if ace_df is not None else 0,
        "n_baseline": int(len(baseline_df)) if baseline_df is not None else 0,
        "sector_summary": sector_summary,
        "top3_trades": build_top3_rows(ace_df),
        "feature_stats": build_feature_stats(feature_insights),
        "regime_key": str(meta.get("META_REGIME_KEY") or cfg.get("CURRENT_REGIME_KEY") or "UNKNOWN"),
        "predicted_sector": pred,
        "us_spillover": spill,
    }
