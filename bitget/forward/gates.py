"""Entry gates, DNA similarity, alpha scoring."""
from __future__ import annotations

import re
import sqlite3

import numpy as np
import pandas as pd

from bitget.forward.shared import DB_PATH, load_system_config

def _tf_weight(tf: str, cfg: dict) -> float:
    custom = cfg.get("TF_RISK_WEIGHTS", {})
    if isinstance(custom, dict) and tf in custom:
        return float(custom[tf])
    default = {"1D": 1.0, "4H": 0.5, "2H": 0.25, "1H": 0.1}
    return float(default.get(tf, 1.0))

def _extract_core_group(sig_type: str) -> str:
    clean_sig = str(sig_type).replace("💀[기각/관찰용] ", "")
    clean_sig = re.sub(r"^\[.*?\]\s*", "", clean_sig)
    return clean_sig.split(" [")[0].strip()

def _thompson_ns_prefix(tf: str, sig_type: str) -> str:
    """
    auto_forward_tester 의 Namespace Thompson Kelly 와 동일 규칙.
    코인 장부는 단일 마켓이므로 KR/US 대신 타임프레임을 접두로 쓴다
    (bitget_system_config.json 예: 4H_MASTER_S1_BETA_PARAMS).
    """
    tfu = str(tf).upper()
    sig = str(sig_type)
    ns_prefix = f"{tfu}_MASTER_S1"
    if "SUPERNOVA" in sig.upper():
        ns_prefix = f"{tfu}_SUPERNOVA_MASTER"
    else:
        if "S4" in sig:
            ns_prefix = f"{tfu}_MASTER_S4"
        if "눌림" in sig:
            ns_prefix = f"{tfu}_NULRIM_S4" if "S4" in sig else f"{tfu}_NULRIM_S1"
        if "5선" in sig or "5EMA" in sig.upper():
            ns_prefix = f"{tfu}_5EMA_S1"
    return ns_prefix

def _apply_thompson_kelly_multiplier(cfg: dict, tf: str, sig_type: str, kelly_risk_pct: float) -> float:
    """
    [NS]_BETA_PARAMS 의 alpha, beta 로 Thompson 샘플 → 켈리 동적 배분 (주식 동형).
    """
    ns_prefix = _thompson_ns_prefix(tf, sig_type)
    try:
        beta_pack = cfg.get(f"{ns_prefix}_BETA_PARAMS", {})
        if not isinstance(beta_pack, dict):
            beta_pack = {}
        alpha = float(beta_pack.get("alpha", 0))
        beta_v = float(beta_pack.get("beta", 0))
        ts_sample = float(np.random.beta(alpha + 1.0, beta_v + 1.0))
        ts_mult = float(np.clip(ts_sample / 0.5, 0.20, 1.80))
        return float(kelly_risk_pct * ts_mult)
    except Exception:
        return float(kelly_risk_pct)

def _table_name(market_type: str, symbol: str, timeframe: str) -> str:
    prefix = "SPOT" if market_type == "spot" else "FUT"
    return f"BITGET_{prefix}_{symbol}_{timeframe}"

def _load_bench_close(conn, symbol: str, timeframe: str = "1D", limit: int = 80):
    for market_type in ("futures", "spot"):
        tbl = _table_name(market_type, symbol, timeframe)
        try:
            df = pd.read_sql(
                f'SELECT Date, Close FROM "{tbl}" ORDER BY Date DESC LIMIT {int(limit)}',
                conn,
            )
            if len(df) >= 30:
                df = df.sort_values("Date")
                df["Date"] = pd.to_datetime(df["Date"])
                return df
        except Exception as e:
            try:
                print(f"🚨 [청산 추적 에러] {r['symbol']}: {e}")
            except Exception:
                print(f"🚨 [청산 추적 에러] unknown_symbol: {e}")
            continue
    return None

def _calc_market_breadth(conn):
    """
    breadth > 1: 알트 확산, breadth < 1: BTC 쏠림
    """
    try:
        btc = _load_bench_close(conn, "BTC_USDT", "1D", 80)
        eth = _load_bench_close(conn, "ETH_USDT", "1D", 80)
        if btc is not None and eth is not None:
            m = btc.merge(eth, on="Date", how="inner", suffixes=("_btc", "_eth"))
            if len(m) >= 30:
                ratio = m["Close_eth"].astype(float) / m["Close_btc"].astype(float)
                ma = ratio.rolling(20).mean().iloc[-1]
                if ma and ma > 0:
                    return float(ratio.iloc[-1] / ma)
    except Exception:
        pass
    return 1.0

def _cosine_similarity(vec_a, vec_b):
    a = np.asarray(vec_a, dtype=float)
    b = np.asarray(vec_b, dtype=float)
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na <= 1e-12 or nb <= 1e-12:
        return 0.0
    return float(np.dot(a, b) / (na * nb))

def _extract_4d_dna_from_facts(facts: dict):
    return np.array(
        [
            float(facts.get("dyn_cpv", 0.0) or 0.0),
            float(facts.get("dyn_tb", 0.0) or 0.0),
            float(facts.get("v_energy", 0.0) or 0.0),
            float(facts.get("dyn_rs", 0.0) or 0.0),
        ],
        dtype=float,
    )

def _is_blocked_by_anti_patterns(cfg: dict, facts: dict, threshold: float = 0.85):
    anti_patterns = cfg.get("ANTI_PATTERNS", [])
    if not isinstance(anti_patterns, list) or not anti_patterns:
        return False, 0.0
    cur_vec = _extract_4d_dna_from_facts(facts or {})
    best_sim = 0.0
    for p in anti_patterns:
        if not isinstance(p, dict):
            continue
        p_vec = np.array(
            [
                float(p.get("dyn_cpv", 0.0) or 0.0),
                float(p.get("dyn_tb", 0.0) or 0.0),
                float(p.get("v_energy", 0.0) or 0.0),
                float(p.get("dyn_rs", 0.0) or 0.0),
            ],
            dtype=float,
        )
        sim = _cosine_similarity(cur_vec, p_vec)
        if sim > best_sim:
            best_sim = sim
    return best_sim >= threshold, best_sim

def _load_hist(conn, market_type: str, symbol: str, timeframe: str, limit=300):
    tbl = _table_name(market_type, symbol, timeframe)
    try:
        df = pd.read_sql(
            f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date DESC LIMIT {int(limit)}',
            conn,
        )
    except Exception:
        return None
    if df.empty:
        return None
    df = df.sort_values("Date")
    df["Date"] = pd.to_datetime(df["Date"])
    return df

def _calc_atr14(df):
    x = df.copy()
    x["prev_c"] = x["Close"].shift(1)
    x["tr"] = np.maximum(
        x["High"] - x["Low"],
        np.maximum(abs(x["High"] - x["prev_c"]), abs(x["Low"] - x["prev_c"])),
    )
    x["atr"] = x["tr"].ewm(span=14, adjust=False).mean()
    return float(x["atr"].iloc[-1]) if len(x) else 0.0

def evaluate_evolved_alpha_formula(df, formula):
    """`auto_forward_tester`와 동일: JSON(AST) 진화 수식을 OHLCV 행렬로 즉석 평가."""
    if df is None or df.empty:
        return None
    try:
        O = df["Open"]
        H = df["High"]
        L = df["Low"]
        C = df["Close"]
        V = df["Volume"]

        def add(a, b):
            return a + b

        def sub(a, b):
            return a - b

        def mul(a, b):
            return a * b

        def div(a, b):
            safe_b = b.replace(0, np.nan) if isinstance(b, pd.Series) else (np.nan if b == 0 else b)
            return a / safe_b

        def rolling_mean(x, w):
            return x.rolling(int(w)).mean()

        def rolling_std(x, w):
            return x.rolling(int(w)).std()

        env = {
            "O": O,
            "H": H,
            "L": L,
            "C": C,
            "V": V,
            "add": add,
            "sub": sub,
            "mul": mul,
            "div": div,
            "rolling_mean": rolling_mean,
            "rolling_std": rolling_std,
        }
        out = eval(str(formula), {"__builtins__": {}}, env)
        if isinstance(out, pd.Series):
            return float(out.replace([np.inf, -np.inf], np.nan).iloc[-1])
    except Exception:
        return None
    return None

def compute_evolved_alpha_bonus_score(sys_config: dict, hist_df: pd.DataFrame) -> float:
    """
    관제탑 EVOLVED_ALPHA_FACTORS → 주식 `try_add_virtual_position`과 동일 배율의 알파 가산점(상한 0.15).
    """
    evolved_factors = sys_config.get("EVOLVED_ALPHA_FACTORS") if isinstance(sys_config, dict) else None
    if not isinstance(evolved_factors, dict) or not evolved_factors:
        evolved_factors = sys_config.get("BITGET_EVOLVED_ALPHA_FACTORS") if isinstance(sys_config, dict) else None
    if not isinstance(evolved_factors, dict) or not evolved_factors:
        return 0.0
    alpha_vals = []
    for _, formula in evolved_factors.items():
        v = evaluate_evolved_alpha_formula(hist_df, formula)
        if v is not None and np.isfinite(v):
            alpha_vals.append(v)
    if not alpha_vals:
        return 0.0
    mv = max(alpha_vals)
    evolved_threshold = float(sys_config.get("EVOLVED_ALPHA_THRESHOLD", sys_config.get("BITGET_EVOLVED_ALPHA_THRESHOLD", 0.0)))
    if mv <= evolved_threshold:
        return 0.0
    denom = max(abs(evolved_threshold), abs(mv) * 1e-9, 1e-12)
    rel_excess = (mv - evolved_threshold) / denom
    return float(min(0.15, rel_excess * 0.15))

def _facts_cos_scalar_01(facts: dict, score_arg) -> float:
    """facts / score에서 템플릿 코사인(또는 이에 해당하는 동적 점수)을 0~1 스케일로 통일."""
    facts = facts or {}
    for k in ("sn_score", "entry_cos_score", "cos_score"):
        if facts.get(k) is None:
            continue
        x = float(facts[k])
        return float(np.clip(x / 100.0 if x > 1.0 else x, -1.0, 1.0))
    s = float(score_arg or 0.0)
    return float(np.clip(s / 100.0 if s > 1.0 else s, 0.0, 1.0))


__all__ = [
    "compute_evolved_alpha_bonus_score",
    "evaluate_evolved_alpha_formula",
]
