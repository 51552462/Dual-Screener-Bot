import json
import os
import sqlite3
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from bitget_forward_tester import (
    generate_mutant_strategies,
    init_forward_db,
    run_deep_dive_analysis,
    send_telegram_msg,
)
from bitget_data_miner import run_bitget_data_miner


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")
TIMEFRAMES = ["1D", "4H", "2H", "1H"]


def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_config_atomic(cfg):
    temp_path = f"{CONFIG_PATH}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp_path, CONFIG_PATH)


def _ensure_defaults(cfg):
    defaults = {
        "DYNAMIC_KELLY_RISK": 0.01,
        "CURRENT_REGIME_KEY": "CHOP",
        "GLOBAL_CIRCUIT_BREAKER": "OFF",
        "TREASURY_SPOT_USDT": 100000.0,
        "TREASURY_FUTURES_USDT": 100000.0,
        "TAIL_RISK_FUND_SPOT": 0.0,
        "TAIL_RISK_FUND_FUTURES": 0.0,
        "BLACKSWAN_GATE": {"enabled": False, "reason": "", "updated_at": ""},
        "TS_KELLY_BY_SIDE": {},
        "ALPHA_DECAY_MONITOR": {},
    }
    for k, v in defaults.items():
        if k not in cfg:
            cfg[k] = v
    return cfg


def _load_bench_1d(conn, symbol):
    for tbl in (f"BITGET_FUT_{symbol}_1D", f"BITGET_SPOT_{symbol}_1D"):
        try:
            df = pd.read_sql(f'SELECT Date, Open, High, Low, Close FROM "{tbl}" ORDER BY Date ASC', conn)
            if len(df) >= 220:
                return df
        except Exception:
            continue
    return None


def detect_coin_regime(cfg):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    btc = _load_bench_1d(conn, "BTC_USDT")
    eth = _load_bench_1d(conn, "ETH_USDT")
    conn.close()

    if btc is None or btc.empty:
        cfg["CURRENT_REGIME_KEY"] = "CHOP"
        return cfg

    c = btc["Close"].astype(float)
    h = btc["High"].astype(float)
    l = btc["Low"].astype(float)
    ema200 = c.ewm(span=200, adjust=False).mean().iloc[-1]
    last = float(c.iloc[-1])
    prev_c = c.shift(1)
    tr = np.maximum(h - l, np.maximum((h - prev_c).abs(), (l - prev_c).abs()))
    atr14 = float(tr.ewm(span=14, adjust=False).mean().iloc[-1])
    atr_pct = (atr14 / last * 100.0) if last > 0 else 0.0

    breadth = 1.0
    if eth is not None and not eth.empty:
        merged = btc[["Date", "Close"]].merge(eth[["Date", "Close"]], on="Date", suffixes=("_btc", "_eth"))
        if len(merged) >= 60:
            ratio = merged["Close_eth"].astype(float) / merged["Close_btc"].astype(float)
            ma50 = float(ratio.rolling(50).mean().iloc[-1])
            if ma50 > 0:
                breadth = float(ratio.iloc[-1] / ma50)

    ema200_s = c.ewm(span=200, adjust=False).mean()
    ema200_prev = float(ema200_s.iloc[-6]) if len(ema200_s) >= 6 else float(ema200_s.iloc[-1])
    ema200_slope_pct = ((float(ema200_s.iloc[-1]) - ema200_prev) / max(abs(ema200_prev), 1e-9)) * 100.0
    dist_from_ema200_pct = ((last - ema200) / max(abs(ema200), 1e-9)) * 100.0

    # 코인 전용 국면 판독:
    # - BULL: BTC가 EMA200 위 + EMA200 우상향 + 알트 확산(breadth)
    # - BEAR: BTC가 EMA200 아래 + EMA200 우하향 + 알트 위축
    # - WHIPSAW: 그 외 혼조/왕복장
    if (last > ema200) and (ema200_slope_pct > 0.15) and (breadth >= 1.00):
        regime = "BULL"
        w1, w4 = 1.25, 0.75
    elif (last < ema200) and (ema200_slope_pct < -0.15) and (breadth < 1.00):
        regime = "BEAR"
        w1, w4 = 0.45, 1.55
    else:
        regime = "WHIPSAW"
        w1, w4 = 0.80, 1.20

    if breadth < 0.97:
        w1 *= 0.5
        w4 *= 1.2
        breadth_state = "NARROW"
    elif breadth > 1.03:
        w1 *= 1.2
        breadth_state = "BROAD"
    else:
        breadth_state = "NEUTRAL"

    cfg["CURRENT_REGIME_KEY"] = regime
    cfg["BTC_EMA200"] = float(ema200)
    cfg["BTC_ATR_PCT"] = float(atr_pct)
    cfg["BTC_EMA200_SLOPE_PCT"] = float(ema200_slope_pct)
    cfg["BTC_DIST_FROM_EMA200_PCT"] = float(dist_from_ema200_pct)
    cfg["CRYPTO_BREADTH_ETH_BTC_REL"] = float(breadth)
    cfg["CRYPTO_BREADTH_STATUS"] = breadth_state
    cfg["CRYPTO_REGIME_DETAIL"] = {
        "btc_over_ema200": bool(last > ema200),
        "ema200_slope_pct": round(float(ema200_slope_pct), 4),
        "dist_from_ema200_pct": round(float(dist_from_ema200_pct), 4),
        "eth_btc_breadth": round(float(breadth), 4),
        "atr_pct": round(float(atr_pct), 4),
    }
    cfg["WEIGHT_S1"] = round(float(np.clip(w1, 0.0, 2.5)), 4)
    cfg["WEIGHT_S4"] = round(float(np.clip(w4, 0.0, 2.5)), 4)
    return cfg


def _pf(returns: pd.Series) -> float:
    if returns is None or returns.empty:
        return 0.0
    s = pd.to_numeric(returns, errors="coerce").dropna()
    if s.empty:
        return 0.0
    wins = s[s > 0].sum()
    losses = abs(s[s <= 0].sum()) + 0.1
    return float(wins / losses)


def _sample_thompson_kelly(df_closed: pd.DataFrame, base_kelly: float):
    if df_closed is None or df_closed.empty:
        return {}
    out = {}
    for side in ("LONG", "SHORT"):
        sub = df_closed[df_closed["position_side"].astype(str).str.upper() == side].copy()
        if len(sub) < 5:
            continue
        ret = pd.to_numeric(sub["final_ret"], errors="coerce").dropna()
        wins = int((ret > 0).sum())
        losses = int((ret <= 0).sum())
        alpha = max(1, wins)
        beta = max(1, losses)
        ts_sample = float(np.random.beta(alpha, beta))
        pf_val = _pf(ret)
        pf_weight = float(np.clip(pf_val / 1.5, 0.5, 1.8))
        risk = float(np.clip(base_kelly * ts_sample * pf_weight, 0.002, 0.03))
        out[side] = {
            "risk": round(risk, 4),
            "alpha": alpha,
            "beta": beta,
            "sample": round(ts_sample, 4),
            "pf": round(pf_val, 4),
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
    return out


def _alpha_half_life_guard(cfg, df_closed: pd.DataFrame):
    if df_closed is None or len(df_closed) < 12:
        return cfg
    ordered = df_closed.sort_values("entry_date").copy()
    half = len(ordered) // 2
    early = ordered.iloc[:half]
    late = ordered.iloc[half:]
    early_pf = _pf(early["final_ret"])
    late_pf = _pf(late["final_ret"])
    ratio = (late_pf / max(early_pf, 1e-9)) if early_pf > 0 else 1.0
    base = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01))
    if early_pf > 0 and (late_pf < early_pf * 0.7 or late_pf < 1.0):
        cfg["DYNAMIC_KELLY_RISK"] = round(max(0.002, base * max(0.2, min(1.0, ratio))), 4)
    cfg["ALPHA_DECAY_MONITOR"] = {
        "early_pf": round(float(early_pf), 4),
        "late_pf": round(float(late_pf), 4),
        "ratio": round(float(ratio), 4),
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    return cfg


def _synthetic_blackswan_gate(cfg, df_closed: pd.DataFrame):
    gate = {"enabled": False, "reason": "", "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}
    if df_closed is None or df_closed.empty:
        cfg["BLACKSWAN_GATE"] = gate
        return cfg
    sim = df_closed.copy()
    ret = pd.to_numeric(sim["final_ret"], errors="coerce").fillna(0.0)
    side = sim["position_side"].astype(str).str.upper().fillna("LONG")
    # LONG: 급락 충격 강화, SHORT: 급등 쇼크 반영
    sim["synthetic_ret"] = np.where(side == "SHORT", (ret * 1.8) - 12.0, (ret * 1.8) - 15.0)
    syn_pf = _pf(sim["synthetic_ret"])
    syn_wr = float((sim["synthetic_ret"] > 0).mean() * 100.0) if len(sim) else 0.0
    if syn_pf < 1.0 or syn_wr < 40.0:
        gate["enabled"] = True
        gate["reason"] = f"Synthetic PF {syn_pf:.2f}, WR {syn_wr:.1f}%"
        cfg["DYNAMIC_KELLY_RISK"] = round(max(0.002, float(cfg.get("DYNAMIC_KELLY_RISK", 0.01)) * 0.5), 4)
    cfg["BLACKSWAN_GATE"] = gate
    cfg["SYNTHETIC_SANDBOX"] = {"pf": round(float(syn_pf), 4), "wr": round(float(syn_wr), 2)}
    return cfg


def _apply_circuit_breaker(cfg, df_closed: pd.DataFrame):
    if df_closed is None or len(df_closed) < 10:
        cfg["GLOBAL_CIRCUIT_BREAKER"] = "OFF"
        return cfg
    recent = df_closed.sort_values("exit_date").tail(20).copy()
    pnl = pd.to_numeric(recent["sim_kelly_invest"], errors="coerce").fillna(0.0) * pd.to_numeric(recent["final_ret"], errors="coerce").fillna(0.0) / 100.0
    eq = pnl.cumsum()
    if eq.empty:
        cfg["GLOBAL_CIRCUIT_BREAKER"] = "OFF"
        return cfg
    dd = eq - eq.cummax()
    max_dd = float(dd.min()) if not dd.empty else 0.0
    trigger = max_dd <= -2000.0
    cfg["GLOBAL_CIRCUIT_BREAKER"] = "ON" if trigger else "OFF"
    cfg["CIRCUIT_BREAKER_STATE"] = {
        "max_drawdown_usdt": round(max_dd, 2),
        "sample_trades": int(len(recent)),
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    if trigger:
        cfg["DYNAMIC_KELLY_RISK"] = 0.002
    return cfg


def _update_tail_risk_fund(cfg):
    for market in ("SPOT", "FUTURES"):
        t_key = f"TREASURY_{market}_USDT"
        f_key = f"TAIL_RISK_FUND_{market}"
        treasury = float(cfg.get(t_key, 0.0) or 0.0)
        fund = float(cfg.get(f_key, 0.0) or 0.0)
        target = max(0.0, treasury * 0.015)
        transfer = min(max(0.0, target - fund), treasury)
        treasury -= transfer
        fund += transfer
        if str(cfg.get("CURRENT_REGIME_KEY", "")).upper() == "BEAR" and float(cfg.get("BTC_ATR_PCT", 0.0)) >= 6.0 and fund > 0:
            treasury += fund * 20.0
            fund = 0.0
        cfg[t_key] = round(max(0.0, treasury), 4)
        cfg[f_key] = round(max(0.0, fund), 4)
    return cfg


def run_autonomous_analysis():
    init_forward_db()
    cfg = _ensure_defaults(load_config())

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    df_closed = pd.read_sql("SELECT * FROM bitget_forward_trades WHERE status LIKE 'CLOSED%'", conn)
    conn.close()

    cfg = detect_coin_regime(cfg)
    base_k = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01))

    ts_kelly = _sample_thompson_kelly(df_closed, base_k)
    if ts_kelly:
        cfg["TS_KELLY_BY_SIDE"] = ts_kelly
        sampled = [float(v.get("risk", base_k)) for v in ts_kelly.values()]
        if sampled:
            cfg["DYNAMIC_KELLY_RISK"] = round(float(np.clip(np.median(sampled), 0.002, 0.03)), 4)

    cfg = _alpha_half_life_guard(cfg, df_closed)
    cfg = _synthetic_blackswan_gate(cfg, df_closed)
    cfg = _apply_circuit_breaker(cfg, df_closed)
    cfg = _update_tail_risk_fund(cfg)

    regime = str(cfg.get("CURRENT_REGIME_KEY", "CHOP")).upper()
    kelly = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01))
    if regime == "BULL":
        kelly = min(0.03, max(0.004, kelly))
    elif regime == "BEAR":
        kelly = max(0.002, min(0.012, kelly))
    else:
        kelly = max(0.002, min(0.018, kelly))
    cfg["DYNAMIC_KELLY_RISK"] = round(kelly, 4)
    cfg["AUTO_PILOT_UPDATED_AT"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    save_config_atomic(cfg)

    gate = cfg.get("BLACKSWAN_GATE", {})
    cb = cfg.get("GLOBAL_CIRCUIT_BREAKER", "OFF")
    send_telegram_msg(
        "🤖 [BITGET AUTO PILOT]\n"
        f"▪️ Regime: {cfg.get('CURRENT_REGIME_KEY', 'CHOP')}\n"
        f"▪️ Kelly: {cfg.get('DYNAMIC_KELLY_RISK', 0.01)*100:.2f}%\n"
        f"▪️ BlackSwan Gate: {'ON' if gate.get('enabled') else 'OFF'} {gate.get('reason', '')}\n"
        f"▪️ Circuit Breaker: {cb}\n"
        f"▪️ Treasury Spot/Fut: {cfg.get('TREASURY_SPOT_USDT', 0):,.2f} / {cfg.get('TREASURY_FUTURES_USDT', 0):,.2f}"
    )
    print("Bitget auto pilot autonomous analysis complete.")


def _judge_incubator_templates(cfg):
    incubator = cfg.get("INCUBATOR_TEMPLATES", {})
    if not isinstance(incubator, dict) or not incubator:
        return cfg, "인큐베이터 템플릿 없음"

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    closed = pd.read_sql(
        "SELECT sig_type, final_ret FROM bitget_forward_trades WHERE status LIKE 'CLOSED%' AND IFNULL(sig_type,'') LIKE '%INCUBATOR%'",
        conn,
    )
    conn.close()
    if closed.empty:
        return cfg, "인큐베이터 청산 표본 부족"

    keep = {}
    removed = []
    promoted = []
    for name, dna in incubator.items():
        tag = f"INCUBATOR_{name}"
        sub = closed[closed["sig_type"].astype(str).str.contains(tag, na=False)].copy()
        if len(sub) < 5:
            keep[name] = dna
            continue
        ret = pd.to_numeric(sub["final_ret"], errors="coerce").dropna()
        wr = float((ret > 0).mean()) if len(ret) else 0.0
        pf = _pf(ret)
        if pf >= 1.3 and wr >= 0.55:
            promoted.append(name)
            cfg[f"PROMOTED_{name}"] = {
                "template": dna,
                "promoted_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "wr": round(wr, 4),
                "pf": round(pf, 4),
            }
        elif pf < 1.0 or wr < 0.40:
            removed.append(name)
        else:
            keep[name] = dna
    cfg["INCUBATOR_TEMPLATES"] = keep
    msg = f"심판 완료: 유지 {len(keep)} / 승격 {len(promoted)} / 도태 {len(removed)}"
    return cfg, msg


def _run_daily_evolution_batch():
    """
    UTC 00:00 코인 전용 자율 진화 배치:
    1) 딥다이브(spot/futures)
    2) 자율분석(켈리/블랙스완/서킷/테일리스크)
    3) 돌연변이 생성
    4) 인큐베이터 심판/승격
    """
    run_deep_dive_analysis("spot")
    run_deep_dive_analysis("futures")
    run_bitget_data_miner(["1D", "4H", "2H", "1H"])
    run_autonomous_analysis()
    ok, m = generate_mutant_strategies()
    cfg = _ensure_defaults(load_config())
    cfg, judge_msg = _judge_incubator_templates(cfg)
    cfg["AUTO_PILOT_DAILY_EVOLUTION_AT"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    save_config_atomic(cfg)
    send_telegram_msg(
        "🧠 [BITGET DAILY EVOLUTION BATCH]\n"
        f"▪️ DeepDive: spot/futures 완료\n"
        f"▪️ DataMiner(AST/GMM): 완료\n"
        f"▪️ Mutant: {'생성' if ok else '스킵'} ({m})\n"
        f"▪️ Incubator Judge: {judge_msg}"
    )


def system_main_loop():
    print("🕒 [Bitget Auto Pilot] loop started")
    print(" - 매일 00:00 UTC: 딥다이브 + 켈리조절 + 돌연변이 + 인큐베이터 심판")
    print(" - 주의: 자율 뇌수술(run_autonomous_analysis)은 과최적화 방지를 위해 하루 1회만 실행")
    last_daily_key = ""
    while True:
        try:
            now = datetime.now(timezone.utc)
            if now.hour == 0 and now.minute == 0:
                daily_key = now.strftime("%Y-%m-%d")
                if daily_key != last_daily_key:
                    _run_daily_evolution_batch()
                    last_daily_key = daily_key
                    time.sleep(60)
            time.sleep(20)
        except Exception as e:
            print(f"bitget_auto_pilot loop error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    system_main_loop()
