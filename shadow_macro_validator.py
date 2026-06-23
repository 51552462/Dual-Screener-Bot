"""
Shadow Macro Validator — MetaGovernor 완전 격리 · 가상 PnL 시뮬레이션.

실전 META_* / config_kv 쓰기 금지. shadow JSON 만 기록.
"""
from __future__ import annotations

import html
import json
import logging
import os
from typing import Any, Dict, Mapping, Optional

import numpy as np
import pandas as pd

from factory_data_paths import factory_data_dir
from macro_matrix_incremental import (
    cell_mean_ret,
    load_macro_matrix,
    update_macro_matrix_incremental,
)
from macro_sentinel_quant import compute_macro_sentinel_snapshot, snapshot_to_dict

logger = logging.getLogger(__name__)

SHADOW_VALIDATION_FILENAME = "SHADOW_MACRO_VALIDATION.json"
_FORBIDDEN_CONFIG_PREFIXES = ("META_", "DEATHMATCH_APPLY")


def shadow_validation_path() -> str:
    return os.path.join(factory_data_dir(), SHADOW_VALIDATION_FILENAME)


def _assert_shadow_isolation(key: str) -> None:
    ku = str(key).upper()
    for prefix in _FORBIDDEN_CONFIG_PREFIXES:
        if ku.startswith(prefix):
            raise RuntimeError(f"shadow isolation violation: cannot write {key}")


def _save_shadow_payload(payload: Dict[str, Any]) -> None:
    for k in payload:
        _assert_shadow_isolation(k)
    p = shadow_validation_path()
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    tmp = f"{p}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, p)


def _effective_ret_series(df: pd.DataFrame) -> pd.Series:
    from evolution.deathmatch_report import _effective_final_ret_pct

    if df is None or df.empty:
        return pd.Series(dtype=float)
    return _effective_final_ret_pct(df)


def _shadow_arm_weights(
    matrix: Mapping[str, Any],
    regime: str,
    arms: list[str],
) -> Dict[str, float]:
    cells = matrix.get("cells") or {}
    raw: Dict[str, float] = {}
    for arm in arms:
        ck = f"{str(regime).upper()}|{arm}"
        mean = cell_mean_ret(cells.get(ck) if isinstance(cells, dict) else None)
        score = max(float(mean), 0.0) + 0.01 if mean is not None else 0.01
        raw[arm] = score
    total = sum(raw.values())
    if total <= 0:
        return {a: 1.0 / len(arms) for a in arms}
    return {a: v / total for a, v in raw.items()}


def simulate_shadow_pnl_improvement(
    df_closed: pd.DataFrame,
    *,
    matrix: Mapping[str, Any],
    liquidity_regime: str,
) -> Dict[str, Any]:
    """
    동일 청산 표본에 대해 equal-weight(actual) vs regime-optimal arm weight(shadow) 비교.
    """
    from evolution.deathmatch_report import classify_strategy_arm

    if df_closed is None or df_closed.empty:
        return {
            "n": 0,
            "actual_mean_pct": 0.0,
            "shadow_mean_pct": 0.0,
            "improvement_pct": 0.0,
        }

    rets = _effective_ret_series(df_closed)
    arms: list[str] = []
    valid_idx: list[int] = []
    for i, (_, row) in enumerate(df_closed.iterrows()):
        arm = classify_strategy_arm(row.get("sig_type"))
        r = rets.iloc[i] if i < len(rets) else np.nan
        if arm and np.isfinite(float(r)):
            arms.append(arm)
            valid_idx.append(i)

    if not valid_idx:
        return {
            "n": 0,
            "actual_mean_pct": 0.0,
            "shadow_mean_pct": 0.0,
            "improvement_pct": 0.0,
        }

    unique_arms = sorted(set(arms))
    weights = _shadow_arm_weights(matrix, liquidity_regime, unique_arms)

    actual_vals: list[float] = []
    shadow_vals: list[float] = []
    for j, idx in enumerate(valid_idx):
        r = float(rets.iloc[idx])
        arm = arms[j]
        w = weights.get(arm, 1.0 / len(unique_arms))
        actual_vals.append(r)
        # shadow: arm 가중치를 표본 내 상대 배분으로 반영
        shadow_vals.append(r * (w * len(unique_arms)))

    actual_mean = float(np.mean(actual_vals))
    shadow_mean = float(np.mean(shadow_vals))
    return {
        "n": len(actual_vals),
        "actual_mean_pct": round(actual_mean, 4),
        "shadow_mean_pct": round(shadow_mean, 4),
        "improvement_pct": round(shadow_mean - actual_mean, 4),
        "arm_weights": {k: round(v, 4) for k, v in weights.items()},
    }


def run_shadow_macro_validation(
    df_closed: pd.DataFrame,
    *,
    market: str,
    refresh_sentinel: bool = True,
    refresh_matrix: bool = True,
) -> Dict[str, Any]:
    """백그라운드 시뮬레이션 — shadow JSON 만 저장."""
    market_u = str(market or "KR").upper()

    snap = None
    sentinel_err = ""
    if refresh_sentinel:
        try:
            snap = compute_macro_sentinel_snapshot()
        except Exception as ex:
            sentinel_err = str(ex)[:120]
            logger.warning("macro sentinel skip: %s", ex)

    matrix = load_macro_matrix()
    matrix_stats: Dict[str, Any] = {}
    if refresh_matrix or snap:
        matrix, matrix_stats = update_macro_matrix_incremental(
            regime_by_date=snap.regime_by_date if snap else None,
        )

    regime = snap.liquidity_regime if snap else "SIDEWAYS"
    ili = snap.institutional_liquidity_index if snap else 50.0
    sim = simulate_shadow_pnl_improvement(
        df_closed,
        matrix=matrix,
        liquidity_regime=regime,
    )

    payload = {
        "shadow_mode": True,
        "market": market_u,
        "liquidity_regime": regime,
        "institutional_liquidity_index": round(float(ili), 2),
        "sentinel": snapshot_to_dict(snap) if snap else {},
        "sentinel_error": sentinel_err,
        "matrix_stats": matrix_stats,
        "simulation": sim,
    }
    _save_shadow_payload(payload)
    return payload


def format_shadow_macro_telegram_html(result: Mapping[str, Any]) -> str:
    sim = result.get("simulation") or {}
    imp = float(sim.get("improvement_pct") or 0.0)
    n = int(sim.get("n") or 0)
    regime = html.escape(str(result.get("liquidity_regime") or "SIDEWAYS"), quote=False)
    ili = float(result.get("institutional_liquidity_index") or 50.0)
    mk = html.escape(str(result.get("market") or ""), quote=False)

    sign = "+" if imp >= 0 else ""
    body = (
        f"\n👻 <b>[기관급 섀도우 매크로 검증]</b> <i>(Shadow · MetaGovernor 미연동)</i>\n"
        f"▪️ ILI <b>{ili:.1f}</b>/100 · 유동성 국면 <b>{regime}</b> · {mk}\n"
        f"▪️ <b>[👻 기관급 섀도우 매크로 검증: 당일 최적화 PnL 개선 {sign}{imp:.2f}%]</b>\n"
        f"<i>표본 {n}건 · equal-weight 대비 regime×arm 매트릭스 가중 시뮬</i>\n"
    )
    err = str(result.get("sentinel_error") or "").strip()
    if err:
        body += f"<i>⚠️ sentinel: {html.escape(err[:72], quote=False)}</i>\n"
    return body


def build_shadow_macro_validation_html(
    market: str,
    df_closed: pd.DataFrame,
    *,
    sys_config: Optional[Mapping[str, Any]] = None,
) -> str:
    """[9/9] 직후 append 용 HTML 블록."""
    _ = sys_config  # shadow: live config 미사용 (격리)
    try:
        result = run_shadow_macro_validation(df_closed, market=market)
        return format_shadow_macro_telegram_html(result)
    except Exception as ex:
        logger.warning("shadow macro validation failed: %s", ex)
        return (
            f"\n👻 <i>[기관급 섀도우 매크로 검증] 스킵: "
            f"{html.escape(str(ex)[:72], quote=False)}</i>\n"
        )


def append_shadow_macro_block(
    deathmatch_html: str,
    *,
    market: str,
    df_closed: pd.DataFrame,
    sys_config: Optional[Mapping[str, Any]] = None,
) -> str:
    """[9/9] 본문 바로 뒤에 섀도우 블록 결합."""
    block = build_shadow_macro_validation_html(
        market, df_closed, sys_config=sys_config
    )
    return str(deathmatch_html or "") + block
