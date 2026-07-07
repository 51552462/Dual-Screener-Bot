"""Bitget shadow macro validator — deathmatch arm weights + canary (Meta 미연동)."""
from __future__ import annotations

import html
import json
import logging
import os
import tempfile
from typing import Any, Dict, Mapping, Optional

import numpy as np
import pandas as pd

from bitget.infra.data_paths import bitget_data_dir
from bitget.infra.market_keys import to_deathmatch_key

logger = logging.getLogger(__name__)

SHADOW_FILENAME = "BITGET_SHADOW_MACRO_VALIDATION.json"


def shadow_validation_path() -> str:
    return os.path.join(bitget_data_dir(), SHADOW_FILENAME)


def _save_shadow(payload: Dict[str, Any]) -> None:
    p = shadow_validation_path()
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".shadow_macro_bg_", suffix=".json", dir=os.path.dirname(p) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp, p)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _classify_arm(sig_type: Any) -> str:
    try:
        from evolution.deathmatch_report import classify_strategy_arm

        return str(classify_strategy_arm(sig_type) or "OTHER")
    except Exception:
        s = str(sig_type or "")
        if "SUPERNOVA" in s.upper():
            return "SUPERNOVA"
        if "STANDARD" in s.upper():
            return "STANDARD"
        return "OTHER"


def _arm_weights_from_closed(df_closed: pd.DataFrame, regime: str) -> Dict[str, float]:
    if df_closed is None or df_closed.empty:
        return {}
    arms: Dict[str, list[float]] = {}
    for _, row in df_closed.iterrows():
        arm = _classify_arm(row.get("sig_type"))
        try:
            r = float(row.get("final_ret", 0) or 0)
        except (TypeError, ValueError):
            continue
        arms.setdefault(arm, []).append(r)
    raw = {a: max(float(np.mean(v)), 0.0) + 0.01 for a, v in arms.items() if v}
    if not raw:
        return {}
    total = sum(raw.values())
    return {a: v / total for a, v in raw.items()}


def simulate_shadow_pnl_improvement(
    df_closed: pd.DataFrame,
    *,
    regime: str,
) -> Dict[str, Any]:
    if df_closed is None or df_closed.empty:
        return {"n": 0, "actual_mean_pct": 0.0, "shadow_mean_pct": 0.0, "improvement_pct": 0.0}
    weights = _arm_weights_from_closed(df_closed, regime)
    if not weights:
        rets = pd.to_numeric(df_closed.get("final_ret"), errors="coerce").dropna()
        m = float(rets.mean()) if len(rets) else 0.0
        return {"n": int(len(rets)), "actual_mean_pct": m, "shadow_mean_pct": m, "improvement_pct": 0.0}

    actual_vals: list[float] = []
    shadow_vals: list[float] = []
    n_arms = len(weights)
    for _, row in df_closed.iterrows():
        arm = _classify_arm(row.get("sig_type"))
        try:
            r = float(row.get("final_ret", 0) or 0)
        except (TypeError, ValueError):
            continue
        w = weights.get(arm, 1.0 / n_arms)
        actual_vals.append(r)
        shadow_vals.append(r * (w * n_arms))
    if not actual_vals:
        return {"n": 0, "actual_mean_pct": 0.0, "shadow_mean_pct": 0.0, "improvement_pct": 0.0}
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
) -> Dict[str, Any]:
    mk = to_deathmatch_key(market)
    regime = "SIDEWAYS"
    ili = 50.0
    try:
        from bitget.reports.canary_panel_bg import load_canary_state

        canary = load_canary_state()
        stress = float(canary.get("crypto_liquidity_stress") or 0.5)
        ili = max(0.0, min(100.0, (1.0 - stress) * 100.0))
        if stress >= 0.7:
            regime = "DOWN"
        elif stress <= 0.35:
            regime = "UP"
    except Exception:
        pass

    sim = simulate_shadow_pnl_improvement(df_closed, regime=regime)
    payload = {
        "shadow_mode": True,
        "market": mk,
        "liquidity_regime": regime,
        "institutional_liquidity_index": round(ili, 2),
        "simulation": sim,
    }
    _save_shadow(payload)
    return payload


def format_shadow_macro_telegram_html(result: Mapping[str, Any]) -> str:
    sim = result.get("simulation") or {}
    imp = float(sim.get("improvement_pct") or 0.0)
    n = int(sim.get("n") or 0)
    regime = html.escape(str(result.get("liquidity_regime") or "SIDEWAYS"), quote=False)
    ili = float(result.get("institutional_liquidity_index") or 50.0)
    mk = html.escape(str(result.get("market") or ""), quote=False)
    sign = "+" if imp >= 0 else ""
    return (
        f"\n👻 <b>[기관급 섀도우 매크로 검증 · Bitget]</b> <i>(Shadow · Meta 미연동)</i>\n"
        f"▪️ ILI <b>{ili:.1f}</b>/100 · 유동성 <b>{regime}</b> · {mk}\n"
        f"▪️ <b>당일 최적화 PnL 개선 {sign}{imp:.2f}%</b> (표본 {n}건)\n"
    )


def build_shadow_macro_validation_html(
    market: str,
    df_closed: pd.DataFrame,
    *,
    sys_config: Optional[Mapping[str, Any]] = None,
) -> str:
    _ = sys_config
    try:
        result = run_shadow_macro_validation(df_closed, market=market)
        return format_shadow_macro_telegram_html(result)
    except Exception as ex:
        logger.warning("bitget shadow macro validation failed: %s", ex)
        return (
            f"\n👻 <i>[섀도우 매크로 검증] 스킵: {html.escape(str(ex)[:72], quote=False)}</i>\n"
        )


def append_shadow_macro_block(
    deathmatch_html: str,
    *,
    market: str,
    df_closed: pd.DataFrame,
    sys_config: Optional[Mapping[str, Any]] = None,
) -> str:
    block = build_shadow_macro_validation_html(market, df_closed, sys_config=sys_config)
    return str(deathmatch_html or "") + block
