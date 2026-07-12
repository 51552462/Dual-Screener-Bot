"""Incubator mutant strategy generation and auto-tune brain."""
from __future__ import annotations

import random
from datetime import datetime

import numpy as np
import pandas as pd

from bitget.forward.shared import load_system_config, save_system_config
from bitget.infra.clock import utc_date_str, utc_datetime_str

def _pf(series):
    if series is None or len(series) == 0:
        return 0.0
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return 0.0
    wins = s[s > 0].sum()
    losses = abs(s[s <= 0].sum()) + 0.1
    return float(wins / losses)

def _calculate_metrics(df: pd.DataFrame, ret_col: str = "final_ret"):
    if df is None or df.empty or ret_col not in df.columns:
        return 0.0, 0.0
    s = pd.to_numeric(df[ret_col], errors="coerce").dropna()
    if s.empty:
        return 0.0, 0.0
    wr = float((s > 0).mean() * 100.0)
    pf = _pf(s)
    return wr, pf

def _coin_asset_group(symbol: str) -> str:
    s = str(symbol or "").upper()
    base = s.split("_")[0] if "_" in s else s.split("/")[0]
    if base in {"BTC", "WBTC"}:
        return "BTC"
    if base in {"ETH", "WETH", "ETC"}:
        return "ETH"
    if base in {"SOL", "BONK", "JTO", "WIF"}:
        return "SOL"
    if base in {"DOGE", "SHIB", "PEPE", "FLOKI", "MEME"}:
        return "MEME"
    if base in {"XRP", "XLM", "HBAR"}:
        return "PAYMENT"
    if base in {"ADA", "DOT", "AVAX", "ATOM", "NEAR"}:
        return "L1_ALT"
    if base in {"LINK", "UNI", "AAVE", "MKR", "SUSHI"}:
        return "DEFI"
    return "OTHER"

def _gaussian_gene_mutate(base_value: float, sigma_ratio: float = 0.10):
    base = float(base_value)
    sigma = max(1e-9, abs(base) * float(sigma_ratio))
    return float(np.random.normal(loc=base, scale=sigma))

def _merge_incubator_templates(existing_incubator: dict, mutants: dict, max_entries: int = 50):
    merged = {}
    if isinstance(existing_incubator, dict):
        for k, v in existing_incubator.items():
            merged[k] = dict(v) if isinstance(v, dict) else v
    if isinstance(mutants, dict):
        for k, v in mutants.items():
            merged[k] = dict(v) if isinstance(v, dict) else v
    if len(merged) <= max_entries:
        return merged
    ranked = []
    for k, v in merged.items():
        if isinstance(v, dict):
            ca = str(v.get("created_at") or "")[:10] or "1970-01-01"
        else:
            ca = "1970-01-01"
        ranked.append((ca, k))
    ranked.sort(key=lambda x: (x[0], x[1]))
    n_drop = len(merged) - max_entries
    for _, k in ranked[:n_drop]:
        merged.pop(k, None)
    return merged

def generate_mutant_strategies():
    """
    코인 MTF 인큐베이터 돌연변이 생성기.
    - TF별(1D/4H/2H/1H)로 유전자(cpv/tb/bbe/rs/cos_cutoff)를 미세 변이
    - 결과를 bitget_system_config.json의 INCUBATOR_TEMPLATES에 누적
    """
    cfg = load_system_config()
    today = utc_date_str()
    if str(cfg.get("INCUBATOR_LAST_GEN_DATE", "")) == today:
        return False, "오늘 인큐베이터 생성 이미 완료"

    mfe_gene = cfg.get("DNA_SUPERNOVA_MFE_WEIGHTED", {})
    base_cpv = float(mfe_gene.get("cpv", 0.55))
    base_tb = float(mfe_gene.get("tb", 8.5))
    base_bbe = float(mfe_gene.get("bbe", 18.0))
    base_rs = float(cfg.get("CRYPTO_BREADTH_ETH_BTC_REL", 1.0) * 100.0)
    cos_parent = float(cfg.get("DYNAMIC_ALPHA_LIMIT", 0.75))

    tf_bias = {
        "1D": {"cpv": 1.00, "tb": 1.10, "bbe": 1.15, "rs": 1.00},
        "4H": {"cpv": 1.00, "tb": 1.00, "bbe": 1.00, "rs": 1.00},
        "2H": {"cpv": 1.02, "tb": 0.95, "bbe": 0.95, "rs": 1.00},
        "1H": {"cpv": 1.05, "tb": 0.90, "bbe": 0.90, "rs": 1.00},
    }

    mutants = {}
    for tf in ("1D", "4H", "2H", "1H"):
        b = tf_bias[tf]
        for i in range(1, 3):  # TF당 2개
            name = f"MUTANT_{tf}_{i}"
            cpv_v = _gaussian_gene_mutate(base_cpv * b["cpv"], sigma_ratio=0.08)
            tb_v = _gaussian_gene_mutate(base_tb * b["tb"], sigma_ratio=0.12)
            bbe_v = _gaussian_gene_mutate(base_bbe * b["bbe"], sigma_ratio=0.15)
            rs_v = _gaussian_gene_mutate(base_rs * b["rs"], sigma_ratio=0.10)
            cos_v = _gaussian_gene_mutate(cos_parent, sigma_ratio=0.08)
            mutants[name] = {
                "cpv": round(float(np.clip(cpv_v, 0.05, 2.0)), 4),
                "tb": round(float(max(0.3, tb_v)), 4),
                "bbe": round(float(max(1.0, bbe_v)), 4),
                "rs": round(float(rs_v), 4),
                "timeframe": tf,
                "cos_cutoff": round(float(np.clip(cos_v, 0.55, 0.95)), 4),
                "created_at": today,
                "status": "INCUBATING",
            }

    existing_incubator = cfg.get("INCUBATOR_TEMPLATES", {})
    if not isinstance(existing_incubator, dict):
        existing_incubator = {}
    cfg["INCUBATOR_TEMPLATES"] = _merge_incubator_templates(existing_incubator, mutants, max_entries=80)
    cfg["INCUBATOR_LAST_GEN_DATE"] = today
    save_system_config(cfg)
    send_telegram_msg("🧪 [Bitget 인큐베이터] MTF 돌연변이 전략 생성 완료 (1D/4H/2H/1H)")
    return True, f"생성 완료: {len(mutants)}개"

def _auto_tune_brain_from_closed_df(cfg: dict, closed_df: pd.DataFrame):
    if cfg is None:
        cfg = {}
    if closed_df is None or closed_df.empty:
        return cfg, []

    msgs = []
    cdf = closed_df.copy()
    cdf["final_ret"] = pd.to_numeric(cdf.get("final_ret"), errors="coerce")
    cdf["mfe"] = pd.to_numeric(cdf.get("mfe"), errors="coerce")
    cdf = cdf.dropna(subset=["final_ret"])
    if cdf.empty:
        return cfg, msgs

    wr = float((cdf["final_ret"] > 0).mean())
    old_ml = float(cfg.get("DYNAMIC_ML_BOX_CUTOFF", 0.50))
    old_alpha = float(cfg.get("DYNAMIC_ALPHA_LIMIT", 0.75))
    new_ml = old_ml
    new_alpha = old_alpha
    if wr < 0.45:
        new_ml = min(0.90, old_ml + 0.05)
        new_alpha = min(0.95, old_alpha + 0.03)
    elif wr > 0.60:
        new_ml = max(0.40, old_ml - 0.03)
        new_alpha = max(0.55, old_alpha - 0.02)
    cfg["DYNAMIC_ML_BOX_CUTOFF"] = round(new_ml, 2)
    cfg["DYNAMIC_ALPHA_LIMIT"] = round(new_alpha, 2)
    msgs.append(f"ML/Alpha 튜닝: WR {wr*100:.1f}% | ML {old_ml:.2f}->{new_ml:.2f}, Alpha {old_alpha:.2f}->{new_alpha:.2f}")

    hi_mfe = cdf[cdf["mfe"].fillna(0.0) >= 10.0].copy()
    if not hi_mfe.empty:
        cpv_m = float(pd.to_numeric(hi_mfe.get("dyn_cpv"), errors="coerce").dropna().mean())
        tb_m = float(pd.to_numeric(hi_mfe.get("dyn_tb"), errors="coerce").dropna().mean())
        bbe_m = float(pd.to_numeric(hi_mfe.get("v_energy"), errors="coerce").dropna().mean())
        old = cfg.get("DNA_SUPERNOVA_MFE_WEIGHTED", {"cpv": cpv_m, "tb": tb_m, "bbe": bbe_m})
        alpha = 0.3
        cfg["DNA_SUPERNOVA_MFE_WEIGHTED"] = {
            "cpv": round((float(old.get("cpv", cpv_m)) * (1 - alpha)) + (cpv_m * alpha), 4),
            "tb": round((float(old.get("tb", tb_m)) * (1 - alpha)) + (tb_m * alpha), 4),
            "bbe": round((float(old.get("bbe", bbe_m)) * (1 - alpha)) + (bbe_m * alpha), 4),
            "updated_at": utc_datetime_str(),
        }
        msgs.append(f"MFE 황금타점 스무딩: 표본 {len(hi_mfe)}건 반영")

    if len(cdf) >= 12:
        ordered = cdf.sort_values("entry_date")
        half = len(ordered) // 2
        early = ordered.iloc[:half]
        late = ordered.iloc[half:]
        early_pf = _pf(early["final_ret"])
        late_pf = _pf(late["final_ret"])
        if early_pf > 0 and (late_pf < early_pf * 0.7 or late_pf < 1.0):
            losses = ordered[ordered["final_ret"] <= 0]
            if not losses.empty:
                adaptive_sl = float(np.percentile(losses["final_ret"].dropna(), 25))
                old_live = cfg.get("1D_LIVE_PARAMS", {"DYNAMIC_MAE_SL": -3.5, "DYNAMIC_MFE_TP": 10.0})
                old_sl = float(old_live.get("DYNAMIC_MAE_SL", -3.5))
                old_live["DYNAMIC_MAE_SL"] = round((old_sl * 0.7) + (adaptive_sl * 0.3), 2)
                cfg["1D_LIVE_PARAMS"] = old_live
            base_k = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01))
            ratio = max(0.2, min(1.0, late_pf / max(early_pf, 1e-9)))
            cfg["DYNAMIC_KELLY_RISK"] = round(max(0.002, base_k * ratio), 4)
            msgs.append(
                f"노화 방어: PF {early_pf:.2f}->{late_pf:.2f}, Kelly {base_k:.4f}->{cfg['DYNAMIC_KELLY_RISK']:.4f}"
            )
    return cfg, msgs

