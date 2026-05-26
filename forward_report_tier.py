"""
일일 통합 리포트 — 티어 버킷 · 데스콤보 판정 SSOT.
"""
from __future__ import annotations

from typing import Any, Union

import pandas as pd

RowLike = Union[dict, pd.Series, Any]


def _row_get(row: RowLike, key: str, default: Any = None) -> Any:
    if hasattr(row, "get"):
        return row.get(key, default)
    return default


def effective_tier_bucket(row: RowLike) -> str:
    """total_score → 10점 버킷 라벨 (예: 85 → 80점대). tier 컬럼 우선."""
    t = str(_row_get(row, "tier", "") or "").strip()
    if t.endswith("점대"):
        return t
    sc = pd.to_numeric(_row_get(row, "total_score"), errors="coerce")
    if pd.isna(sc):
        return ""
    bucket = int(float(sc) // 10) * 10
    if bucket >= 100:
        bucket = 90
    return f"{bucket}점대"


def is_tier_80_bucket(row: RowLike) -> bool:
    """80~89점 구간 (effective_tier_bucket 기준)."""
    t = effective_tier_bucket(row)
    if t == "80점대":
        return True
    sc = pd.to_numeric(_row_get(row, "total_score"), errors="coerce")
    if pd.isna(sc):
        return False
    return 80.0 <= float(sc) < 90.0


def compute_death_combo_flag(row: RowLike, *, market: str = "KR") -> bool:
    """
    DB 플래그 우선, 없으면 dyn/v 팩트로 즉석 재계산.
    KR/US 공통 기본: cpv > 0.85 and rs < 0 (master.py 정합).
    """
    try:
        if int(_row_get(row, "is_death_combo", 0) or 0) == 1:
            return True
    except (TypeError, ValueError):
        pass
    cpv = pd.to_numeric(
        _row_get(row, "dyn_cpv", _row_get(row, "v_cpv", 0)), errors="coerce"
    )
    rs = pd.to_numeric(
        _row_get(row, "dyn_rs", _row_get(row, "v_rs", 0)), errors="coerce"
    )
    if pd.isna(cpv) or pd.isna(rs):
        return False
    return float(cpv) > 0.85 and float(rs) < 0.0


def filter_tier_80_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.iloc[0:0].copy()
    mask = df.apply(is_tier_80_bucket, axis=1)
    return df.loc[mask].copy()


def filter_death_combo_df(df: pd.DataFrame, *, market: str = "KR") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df.iloc[0:0].copy()
    mask = df.apply(lambda r: compute_death_combo_flag(r, market=market), axis=1)
    return df.loc[mask].copy()


def attach_computed_flags(df: pd.DataFrame, *, market: str = "KR") -> pd.DataFrame:
    """리포트용 파생 컬럼 (DB 미변경)."""
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame()
    out = df.copy()
    out["_effective_tier"] = out.apply(effective_tier_bucket, axis=1)
    out["_death_combo_computed"] = out.apply(
        lambda r: int(compute_death_combo_flag(r, market=market)), axis=1
    )
    return out
