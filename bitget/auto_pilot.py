import json
import os
import sqlite3
import time

import numpy as np
import pandas as pd
import memory_bounds

from bitget.infra.gc_cycle import flush_gc
from bitget.infra.clock import (
    parse_utc_iso,
    utc_date_days_ago_str,
    utc_date_key,
    utc_datetime_str_tz,
    utc_now,
)
from bitget.infra.bounded_reads import (
    forward_autopilot_analysis_closed_sql,
    forward_incubator_judge_closed_sql,
    forward_weekly_flow_closed_sql,
    forward_weekly_market_pnl_sum_sql,
    forward_weekly_tf_rotation_sql,
)
from bitget.infra.memory_policy import GC_AFTER_GMM_FIT, OHLCV_REGIME_BAR_LIMIT
from bitget.infra.logging_setup import setup_logging, get_logger, log_exception
from bitget.config_hub import load_config as hub_load_config, save_config_atomic as hub_save_config_atomic
from bitget.schedule_lock import acquire as schedule_acquire

from bitget.forward_tester import (
    generate_mutant_strategies,
    init_forward_db,
    run_deep_dive_analysis,
    send_telegram_msg,
)
from bitget.data_miner import run_bitget_data_miner
from bitget.infra.data_paths import market_data_db_path, system_config_json_path
from bitget.infra.shared_db_connector import get_connection


def send_telegram_report(message):
    """주식 `system_auto_pilot`과 동일한 호출명; Bitget 봇 토큰으로 HTML 발송."""
    send_telegram_msg(message)


DB_PATH = market_data_db_path()
CONFIG_PATH = system_config_json_path()
TIMEFRAMES = ["1D", "4H", "2H", "1H"]
setup_logging()
logger = get_logger("bitget.auto_pilot")


def load_config():
    return hub_load_config()


def save_config_atomic(cfg):
    hub_save_config_atomic(cfg)


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
            df = pd.read_sql(
                f'SELECT Date, Open, High, Low, Close FROM "{tbl}"'
                f"{memory_bounds.ohlcv_limit_sql(bar_limit=OHLCV_REGIME_BAR_LIMIT)}",
                conn,
            )
            if not df.empty:
                df = df.sort_values("Date")
            if len(df) >= 200:
                return df
        except Exception:
            continue
    return None


def detect_coin_regime(cfg):
    conn = get_connection(DB_PATH, read_only=True)
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
    try:
        from bitget.governance.meta_consumer import apply_meta_weight_bounds_clamp, load_meta_state_resolved

        w1, w4 = apply_meta_weight_bounds_clamp(float(w1), float(w4), load_meta_state_resolved())
    except Exception:
        pass
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
            "updated_at": utc_datetime_str_tz(),
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
        "updated_at": utc_datetime_str_tz(),
    }
    return cfg


def _synthetic_blackswan_gate(cfg, df_closed: pd.DataFrame):
    gate = {"enabled": False, "reason": "", "updated_at": utc_datetime_str_tz()}
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
    """Weekly closed-trade MDD advisory — GLOBAL_CIRCUIT_BREAKER 는 ledger track SSOT."""
    if df_closed is None or len(df_closed) < 10:
        cfg["CLOSED_TRADE_CB_ADVISORY"] = {"active": False, "reason": "insufficient_sample"}
        return cfg
    recent = df_closed.sort_values("exit_date").tail(20).copy()
    pnl = pd.to_numeric(recent["sim_kelly_invest"], errors="coerce").fillna(0.0) * pd.to_numeric(recent["final_ret"], errors="coerce").fillna(0.0) / 100.0
    eq = pnl.cumsum()
    if eq.empty:
        cfg["CLOSED_TRADE_CB_ADVISORY"] = {"active": False, "reason": "empty_equity"}
        return cfg
    dd = eq - eq.cummax()
    max_dd = float(dd.min()) if not dd.empty else 0.0
    trigger = max_dd <= -2000.0
    cfg["CLOSED_TRADE_CB_ADVISORY"] = {
        "active": bool(trigger),
        "max_drawdown_usdt": round(max_dd, 2),
        "sample_trades": int(len(recent)),
        "updated_at": utc_datetime_str_tz(),
    }
    cfg["CIRCUIT_BREAKER_STATE"] = dict(cfg["CLOSED_TRADE_CB_ADVISORY"])
    if trigger:
        cfg["DYNAMIC_KELLY_RISK"] = 0.002
    return cfg


def _update_tail_risk_fund(cfg):
    """Delegate to trading.tail_risk_gate SSOT (accrual + 1:1 crisis release; never ×N mint)."""
    from bitget.trading.tail_risk_gate import accrue_tail_risk_fund

    return accrue_tail_risk_fund(cfg)


def _eq_oos_bootstrap_floor(ret_series):
    """system_auto_pilot 엔진6: OOS 복리 부트스트랩 5% 분위."""
    v = pd.to_numeric(ret_series, errors="coerce").dropna().values
    if len(v) < 3:
        return float(np.sum(v))
    return float(
        np.percentile(
            [(np.prod(1.0 + np.random.choice(v, size=len(v), replace=True) / 100.0) - 1.0) * 100.0 for _ in range(1000)],
            5,
        )
    )


def _ts_risk_from_returns_ns(ret_series, base_kelly: float):
    """엔진6 TS 자본 배분 — 주식 `system_auto_pilot`과 동일."""
    try:
        rs = pd.to_numeric(ret_series, errors="coerce").dropna()
        if len(rs) < 3:
            return None
        wins = int((rs > 0).sum())
        losses = int((rs <= 0).sum())
        alpha = max(1, wins)
        beta = max(1, losses)
        ts_sample = float(np.random.beta(alpha, beta))
        win_sum = float(rs[rs > 0].sum())
        loss_sum = abs(float(rs[rs <= 0].sum()))
        pf = float(win_sum / (loss_sum + 0.1))
        pf_weight = float(np.clip(pf / 1.5, 0.5, 1.8))
        risk = float(np.clip(base_kelly * ts_sample * pf_weight, 0.002, 0.030))
        wr = float(wins / max(1, wins + losses))
        return {
            "risk": risk,
            "alpha": alpha,
            "beta": beta,
            "sample": ts_sample,
            "pf": pf,
            "wr": wr,
        }
    except Exception:
        return None


def _row_mae_mfe_pct_series(row):
    """청산 행 단위 MAE%/MFE% (SHORT/LONG 모두 숫부호 정렬)."""
    ep = float(row.get("entry_price", 0) or 0)
    if ep <= 0:
        return float("nan"), float("nan")
    lo = float(row.get("min_low", ep) or ep)
    hi = float(row.get("max_high", ep) or ep)
    side = str(row.get("position_side", "LONG")).upper()
    if side == "SHORT":
        mae_pct = -((hi - ep) / ep) * 100.0
        mfe_pct = ((ep - lo) / ep) * 100.0
    else:
        mae_pct = ((lo - ep) / ep) * 100.0
        mfe_pct = ((hi - ep) / ep) * 100.0
    return mae_pct, mfe_pct


def _default_param_bundle():
    return {
        "DYNAMIC_MAE_SL": -3.5,
        "DYNAMIC_MFE_TP": 10.0,
        "TREE_FATAL_CPV": 0.70,
        "DYNAMIC_ALPHA_LIMIT": 0.75,
        "DYNAMIC_TRAP_LIMIT": 0.70,
        "DYNAMIC_DTW_LIMIT": 2.5,
    }


def _engine6_oos_parallel_champion(cfg, df_closed: pd.DataFrame, report_lines: list):
    """
    system_auto_pilot 엔진6 복제: live_a / cand_b / champ_c OOS 지표 및 챔피언 MAE·MFE 14건 스무딩,
    후보 승격·챔피언 복귀. TF별 `{TF}_LIVE_PARAMS` 등은 `track_daily_positions`와 동명.
    글로벌 DYNAMIC_KELLY_RISK(Thompson·레짐)는 건드리지 않고 TF별 `_DYNAMIC_KELLY_RISK`만 저장.
    """
    report_lines.append("<b>[평행우주 A/B/C · 엔진6 OOS 및 챔피언 스무딩]</b>")
    defaults = _default_param_bundle()
    now_u = utc_now()
    oos_barrier = utc_date_days_ago_str(14, anchor=now_u)

    if df_closed is None or df_closed.empty:
        report_lines.append("▪️ 청산 표본 없음 — 엔진6 스킵.")
        report_lines.append("")
        return

    df = df_closed.copy()
    for col in ("live_a_ret", "cand_b_ret", "champ_c_ret", "entry_date", "entry_price", "min_low", "max_high"):
        if col not in df.columns:
            df[col] = np.nan
    df["namespace"] = df["timeframe"].astype(str).str.upper().fillna("")
    uniq = sorted({str(x).strip().upper() for x in df["namespace"].tolist() if str(x).strip()})

    for target_ns in uniq:
        ns_df = df[df["namespace"] == target_ns].copy()
        if len(ns_df) < 5:
            continue

        cand_key = f"{target_ns}_CANDIDATE_PARAMS"
        champ_key = f"{target_ns}_CHAMPION_PARAMS"
        live_key = f"{target_ns}_LIVE_PARAMS"

        lp = cfg.get(live_key)
        if not isinstance(lp, dict) or not lp:
            cfg[live_key] = dict(defaults)
            lp = cfg[live_key]
        else:
            for k0, v0 in defaults.items():
                lp.setdefault(k0, v0)
            cfg[live_key] = lp

        if not isinstance(cfg.get(cand_key), dict) or not cfg[cand_key]:
            cfg[cand_key] = dict(lp)
        if not isinstance(cfg.get(champ_key), dict) or not cfg[champ_key]:
            cfg[champ_key] = dict(lp)

        ns_df["_ed"] = ns_df["entry_date"].astype(str).str.strip().str[:10]
        train_df = ns_df[ns_df["_ed"] < oos_barrier]
        test_df = ns_df[ns_df["_ed"] >= oos_barrier]

        results = {}
        for col in ("live_a_ret", "cand_b_ret", "champ_c_ret"):
            if col in test_df.columns and not test_df[col].dropna().empty:
                results[col] = _eq_oos_bootstrap_floor(test_df[col])

        if not results:
            report_lines.append(f"▪️ <b>{target_ns}</b>: OOS(최근 14일) 구간 신호 부족 — 대결 스킵.")
            continue

        win_k = max(results, key=results.get)
        report_lines.append(
            f"▪️ <b>{target_ns}</b> OOS(복리 하한 5%): LIVE {results.get('live_a_ret', 0):.2f}% | "
            f"B {results.get('cand_b_ret', 0):.2f}% | C {results.get('champ_c_ret', 0):.2f}%"
        )

        champ_params = cfg.get(champ_key, {})
        if isinstance(champ_params, dict) and champ_params:
            recent_src = (
                test_df.sort_values("_ed").tail(14)
                if "_ed" in test_df.columns
                else test_df.tail(14)
            )
            if "entry_price" in recent_src.columns:
                recent_src = recent_src[pd.to_numeric(recent_src["entry_price"], errors="coerce").fillna(0) > 0]
            mae_vals, mfe_vals = [], []
            for _, prow in recent_src.iterrows():
                mn, mx = _row_mae_mfe_pct_series(prow)
                if np.isfinite(mn) and np.isfinite(mx):
                    mae_vals.append(mn)
                    mfe_vals.append(mx)
            if mae_vals:
                new_mae = float(np.mean(mae_vals))
                new_mfe = float(np.mean(mfe_vals))
                old_mae = float(champ_params.get("DYNAMIC_MAE_SL", new_mae))
                old_mfe = float(champ_params.get("DYNAMIC_MFE_TP", new_mfe))
                obs_key = f"{target_ns}_CHAMPION_OBS_COUNT"
                old_obs = int(cfg.get(obs_key, 0) or 0)
                new_obs = int(len(mae_vals))
                alpha_smooth = float(min(0.4, new_obs / max(1.0, (old_obs + new_obs))))
                champ_params["DYNAMIC_MAE_SL"] = round(
                    (old_mae * (1.0 - alpha_smooth)) + (new_mae * alpha_smooth), 2
                )
                champ_params["DYNAMIC_MFE_TP"] = round(
                    (old_mfe * (1.0 - alpha_smooth)) + (new_mfe * alpha_smooth), 2
                )
                cfg[obs_key] = int(old_obs + new_obs)
                cfg[champ_key] = champ_params
                report_lines.append(
                    f" ▫ 챔피언 스무딩(OOS·최대14): MAE {old_mae:.2f}%→{champ_params['DYNAMIC_MAE_SL']:.2f}% | "
                    f"MFE {old_mfe:.2f}%→{champ_params['DYNAMIC_MFE_TP']:.2f}% | α={alpha_smooth:.3f} (n={new_obs})"
                )

        la = results.get("live_a_ret", 0.0)
        if win_k == "cand_b_ret" and results.get("cand_b_ret", 0.0) > la * 1.05:
            cfg[champ_key] = dict(cfg.get(live_key, {}))
            cfg[live_key] = dict(cfg.get(cand_key, {}))
            report_lines.append(f' ▫ <b>{target_ns}</b> 🏆 후보(B) 실전 배치 — LIVE 교체.')
        elif win_k == "champ_c_ret" and results.get("champ_c_ret", 0.0) > la * 1.05:
            cfg[live_key] = dict(cfg.get(champ_key, {}))
            report_lines.append(f" ▫ <b>{target_ns}</b> ♻ 챔피언(C) LIVE 복귀.")

        base_k_ns = float(cfg.get(f"{target_ns}_DYNAMIC_KELLY_RISK", cfg.get("DYNAMIC_KELLY_RISK", 0.01)))
        pick_col = win_k if win_k in ("live_a_ret", "cand_b_ret", "champ_c_ret") else "live_a_ret"
        ts_series = test_df[pick_col] if pick_col in test_df.columns else pd.Series(dtype=float)
        ts_pack = _ts_risk_from_returns_ns(ts_series, base_k_ns)
        if ts_pack is not None:
            cfg[f"{target_ns}_DYNAMIC_KELLY_RISK"] = round(ts_pack["risk"], 4)
            report_lines.append(
                f" ▫ TF Kelly 보조 <b>{pick_col}</b>: Beta({ts_pack['alpha']},{ts_pack['beta']}) 샘플 {ts_pack['sample']:.3f} | "
                f"PF {ts_pack['pf']:.2f} ➜ <code>{target_ns}_DYNAMIC_KELLY_RISK</code>={ts_pack['risk']*100:.2f}%"
            )
        else:
            report_lines.append(f" ▫ <b>{target_ns}</b> TS 표본 부족 — TF별 Kelly 스킵.")
    report_lines.append("")


def run_autonomous_analysis():
    init_forward_db()
    cfg = _ensure_defaults(load_config())
    report_lines = [
        "<b>🧠 [BITGET 코인 자율 뇌수술 · 관제탑 튜닝 결과지]</b>",
        "",
    ]

    conn = get_connection(DB_PATH, read_only=True)
    n_closed_row = conn.execute(
        "SELECT COUNT(*) FROM bitget_forward_trades WHERE status LIKE 'CLOSED%'"
    ).fetchone()
    n_closed_total = int(n_closed_row[0] or 0) if n_closed_row else 0
    q_closed, p_closed = forward_autopilot_analysis_closed_sql()
    df_closed = pd.read_sql(q_closed, conn, params=p_closed)
    conn.close()
    n_closed = n_closed_total

    cfg = detect_coin_regime(cfg)
    report_lines.append("<b>[1. 동적 국면 판독 — Spot/Futures 레짐]</b>")
    det = cfg.get("CRYPTO_REGIME_DETAIL") or {}
    report_lines.append(
        f"▪️ 상태: <b>{cfg.get('CURRENT_REGIME_KEY', 'CHOP')}</b> · ETH/BTC breadth {cfg.get('CRYPTO_BREADTH_ETH_BTC_REL', '-')}"
        f" ({cfg.get('CRYPTO_BREADTH_STATUS', '-')})"
    )
    report_lines.append(
        f"▪️ BTC vs EMA200 거리 %: {det.get('dist_from_ema200_pct')} · "
        f"EMA200 기울기 %: {det.get('ema200_slope_pct')} · ATR %: {det.get('atr_pct')}"
    )
    report_lines.append(
        f"🚨 액션: S1 비중 <b>{cfg.get('WEIGHT_S1')}</b>배 · S4 비중 <b>{cfg.get('WEIGHT_S4')}</b>배"
    )
    report_lines.append(f"▪️ 청산 표본 수: <b>{n_closed}</b>건 (전체 마켓)")
    report_lines.append("")

    base_k = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01))
    report_lines.append("<b>[2. Thompson–Kelly 쪽별 샘플링]</b>")
    ts_kelly = _sample_thompson_kelly(df_closed, base_k)
    if ts_kelly:
        cfg["TS_KELLY_BY_SIDE"] = ts_kelly
        sampled = [float(v.get("risk", base_k)) for v in ts_kelly.values()]
        if sampled:
            cfg["DYNAMIC_KELLY_RISK"] = round(float(np.clip(np.median(sampled), 0.002, 0.03)), 4)
        for side in ("LONG", "SHORT"):
            d = ts_kelly.get(side)
            if d:
                report_lines.append(
                    f"▪️ {side}: risk <b>{d.get('risk')}</b> · Thompson sample <b>{d.get('sample')}</b> · "
                    f"PF {d.get('pf')} · α/β {d.get('alpha')}/{d.get('beta')}"
                )
        report_lines.append(
            f"▪️ 표본 후 중앙 켈리(Thompson): <b>{float(cfg.get('DYNAMIC_KELLY_RISK', base_k))*100:.2f}%</b>"
        )
    else:
        report_lines.append("▪️ 청산·쪽 표본 부족 — Thompson 단계 유지 스킵(기준 켈리 그대로).")
        report_lines.append(f"▪️ 기준 켈리: <b>{base_k * 100:.2f}%</b>")
    report_lines.append("")

    _engine6_oos_parallel_champion(cfg, df_closed, report_lines)

    cfg = _alpha_half_life_guard(cfg, df_closed)
    report_lines.append("<b>[3. 알파 반감기(Alpha half-life) 가드]</b>")
    adm = cfg.get("ALPHA_DECAY_MONITOR") or {}
    if adm:
        report_lines.append(
            f"▪️ 초기 PF <b>{adm.get('early_pf')}</b> → 후기 PF <b>{adm.get('late_pf')}</b> "
            f"(비율 {adm.get('ratio')})"
        )
        if adm.get("late_pf") is not None and adm.get("early_pf") is not None:
            try:
                if float(adm["early_pf"]) > 0 and float(adm["late_pf"]) < float(adm["early_pf"]) * 0.7:
                    report_lines.append("⚠️ <b>경고:</b> 후기 알파가 초기 대비 30% 이상 약화 구간 — 켈리 축소 적용 가능.")
            except (TypeError, ValueError):
                pass
        report_lines.append(f"▪️ 조정 후 DYNAMIC_KELLY_RISK: <b>{float(cfg.get('DYNAMIC_KELLY_RISK', 0.01))*100:.2f}%</b>")
    else:
        report_lines.append("▪️ 표본 부족으로 반감기 지표 생략.")
    report_lines.append("")

    cfg = _synthetic_blackswan_gate(cfg, df_closed)
    gate = cfg.get("BLACKSWAN_GATE", {}) or {}
    syn = cfg.get("SYNTHETIC_SANDBOX") or {}
    report_lines.append("<b>[4. 합성 블랙스완 샌드박스]</b>")
    report_lines.append(
        f"▪️ 게이트: <b>{'ON' if gate.get('enabled') else 'OFF'}</b> {gate.get('reason', '')}"
    )
    report_lines.append(f"▪️ 합성 PF / 승률: {syn.get('pf')} / {syn.get('wr')}%")
    report_lines.append("")

    cfg = _apply_circuit_breaker(cfg, df_closed)
    cb = str(cfg.get("GLOBAL_CIRCUIT_BREAKER", "OFF")).upper()
    cbs = cfg.get("CLOSED_TRADE_CB_ADVISORY") or cfg.get("CIRCUIT_BREAKER_STATE") or {}
    report_lines.append("<b>[5. 글로벌 서킷 브레이커]</b>")
    report_lines.append(f"▪️ OPEN 손실 기준(ledger SSOT): <b>{cb}</b>")
    if cbs:
        report_lines.append(
            f"▪️ 주간 청산 MDD 참고: {cbs.get('max_drawdown_usdt')} USDT · n={cbs.get('sample_trades')} · advisory={'ON' if cbs.get('active') else 'OFF'}"
        )
    report_lines.append("")

    cfg = _update_tail_risk_fund(cfg)
    report_lines.append("<b>[6. 테일 리스크 펀드 · 국고]</b>")
    report_lines.append(
        f"▪️ Spot 국고 / 테일: {cfg.get('TREASURY_SPOT_USDT', 0):,.2f} / {cfg.get('TAIL_RISK_FUND_SPOT', 0):,.2f} USDT"
    )
    report_lines.append(
        f"▪️ Futures 국고 / 테일: {cfg.get('TREASURY_FUTURES_USDT', 0):,.2f} / {cfg.get('TAIL_RISK_FUND_FUTURES', 0):,.2f} USDT"
    )
    _tail_act = cfg.get("TAIL_RISK_LAST_ACTION") or {}
    if _tail_act.get("actions"):
        report_lines.append(
            f"▪️ 적립/1:1방출: {', '.join(str(a) for a in (_tail_act.get('actions') or [])[:6])}"
            f"{' · CRISIS' if _tail_act.get('crisis') else ''}"
        )
    report_lines.append("")

    regime = str(cfg.get("CURRENT_REGIME_KEY", "CHOP")).upper()
    kelly = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01))
    k_pre_clamp = kelly
    if regime == "BULL":
        kelly = min(0.03, max(0.004, kelly))
    elif regime == "BEAR":
        kelly = max(0.002, min(0.012, kelly))
    else:
        kelly = max(0.002, min(0.018, kelly))
    cfg["DYNAMIC_KELLY_RISK"] = round(kelly, 4)
    cfg["AUTO_PILOT_UPDATED_AT"] = utc_datetime_str_tz()

    report_lines.append("<b>[7. 레짐별 켈리 최종 클램프]</b>")
    report_lines.append(f"▪️ 레짐: <b>{regime}</b> · 클램프 전 {k_pre_clamp*100:.2f}% → 최종 <b>{kelly*100:.2f}%</b>")
    report_lines.append(f"▪️ 기록 시각: {cfg['AUTO_PILOT_UPDATED_AT']}")

    save_config_atomic(cfg)
    send_telegram_report("\n".join(report_lines))
    del df_closed
    flush_gc(label=GC_AFTER_GMM_FIT)
    logger.info("Bitget auto pilot autonomous analysis complete")


def _judge_incubator_templates(cfg):
    incubator = cfg.get("INCUBATOR_TEMPLATES", {})
    if not isinstance(incubator, dict) or not incubator:
        return cfg, "인큐베이터 템플릿 없음"

    conn = get_connection(DB_PATH, read_only=True)
    q_inc, p_inc = forward_incubator_judge_closed_sql()
    closed = pd.read_sql(q_inc, conn, params=p_inc)
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
                "promoted_at": utc_datetime_str_tz(),
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
    cfg["AUTO_PILOT_DAILY_EVOLUTION_AT"] = utc_datetime_str_tz()
    save_config_atomic(cfg)
    send_telegram_msg(
        "🧠 [BITGET DAILY EVOLUTION BATCH]\n"
        f"▪️ DeepDive: spot/futures 완료\n"
        f"▪️ DataMiner(AST/GMM): 완료\n"
        f"▪️ Mutant: {'생성' if ok else '스킵'} ({m})\n"
        f"▪️ Incubator Judge: {judge_msg}"
    )


def send_weekly_flow_master_report():
    """
    지난 7일간 Spot/Futures 일자별 실현·승률·MVP 시그널·타임프레임 궤적을 총결산하고,
    관제탑(bitget_system_config.json) 튜닝 스냅샷을 붙여 텔레그램 발송.
    (주식 V100 리포트와 동등한 역할 / 코인은 24·7 장이므로 월 시작 기준 롤링 7일)
    """
    now = utc_now()
    week_ago = utc_date_days_ago_str(7, anchor=now)
    today_str = utc_date_key(anchor=now)
    cfg = _ensure_defaults(load_config())
    regime = str(cfg.get("CURRENT_REGIME_KEY", "UNKNOWN"))
    gate = cfg.get("BLACKSWAN_GATE", {}) or {}

    report_msg = (
        "🗺️ <b>[BITGET 주간(Flow) 마스터 총결산]</b>\n"
        f"📅 롤링 7일: {week_ago} ~ {today_str} <i>(월요일 09:00 KST = UTC 00:00 발송 대상 구간)</i>\n"
        "<i>※ 본계좌만: sig_type에 INCUBATOR 포함 거래 제외</i>\n"
        "━━━━━━━━━━━━━━━━━━\n"
    )

    try:
        conn = get_connection(DB_PATH, read_only=True)

        for market_type in ("spot", "futures"):
            icon = "🟢" if market_type == "spot" else "🟠"
            report_msg += f"\n{icon} <b>[{market_type.upper()} 일주일 자금·타임프레임 궤적]</b>\n"
            report_msg += "🗓️ <b>[일자별 실현 손익 및 승률 타임라인]</b>\n"
            q_flow, p_flow = forward_weekly_flow_closed_sql(
                market_type=market_type,
                since_date=week_ago,
            )
            flow_df = pd.read_sql(q_flow, conn, params=p_flow)
            weekly_pnl = 0.0
            if not flow_df.empty:
                invest = pd.to_numeric(flow_df["sim_kelly_invest"], errors="coerce").fillna(0.0)
                ret = pd.to_numeric(flow_df["final_ret"], errors="coerce").fillna(0.0)
                flow_df = flow_df.assign(pnl_usdt=invest * ret / 100.0)
                daily = (
                    flow_df.groupby("exit_date", dropna=False)
                    .agg(
                        daily_pnl=("pnl_usdt", "sum"),
                        wins=("final_ret", lambda s: (pd.to_numeric(s, errors="coerce") > 0).sum()),
                        total=("final_ret", "count"),
                    )
                    .reset_index()
                    .sort_values("exit_date")
                )
                for row in daily.itertuples(index=False):
                    d_pnl = float(row.daily_pnl or 0.0)
                    wins = int(row.wins or 0)
                    total = int(row.total or 0)
                    d_wr = (float(wins) / float(total) * 100.0) if total else 0.0
                    weekly_pnl += d_pnl
                    short_date = str(row.exit_date)[5:] if row.exit_date else "--"
                    emo = "🔴" if d_pnl < 0 else "🟢"
                    report_msg += (
                        f" {emo} {short_date}: <b>{d_pnl:+,.2f} USDT</b> "
                        f"(승률 {d_wr:.0f}% / {total}건 청산)\n"
                    )
                report_msg += f" 💰 <b>주간 누적 실현 손익: {weekly_pnl:+,.2f} USDT</b>\n"
            else:
                report_msg += " ↳ 이번 구간 청산 데이터가 없습니다.\n"

            report_msg += "\n🔄 <b>[주간 주도 타임프레임 진화 궤적]</b>\n"
            q_rot, p_rot = forward_weekly_tf_rotation_sql(
                market_type=market_type,
                since_date=week_ago,
            )
            rot_df = pd.read_sql(q_rot, conn, params=p_rot)
            if not rot_df.empty:

                def _dominant_tf(s):
                    m = s.mode()
                    return m.iloc[0] if len(m) else None

                daily_dom = rot_df.groupby("entry_date")["timeframe"].agg(_dominant_tf).dropna()
                flow_path = [f"{str(s)[:8]}({str(d)[5:]})" for d, s in daily_dom.items()]
                report_msg += f" 🌊 <b>흐름:</b> {' ➔ '.join(flow_path)}\n"
            else:
                report_msg += " ↳ 타임프레임 편입 궤적 데이터가 없습니다.\n"

            report_msg += "\n🏆 <b>[구간 MVP 시그널 엔진 TOP3]</b>\n"
            if not flow_df.empty:
                mvp = (
                    flow_df.groupby("sig_type", dropna=False)
                    .agg(profit=("pnl_usdt", "sum"), cnt=("final_ret", "count"))
                    .sort_values("profit", ascending=False)
                    .head(3)
                )
                for i, (sig, row) in enumerate(mvp.iterrows()):
                    clean_sig = str(sig).split("]")[0] + "]" if "]" in str(sig) else str(sig)[:18]
                    medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"
                    report_msg += (
                        f" {medal} {clean_sig}: <b>{float(row['profit'] or 0.0):+,.2f} USDT</b> "
                        f"기여 ({int(row['cnt'])}건)\n"
                    )
            else:
                report_msg += " ↳ MVP 데이터가 없습니다.\n"

        conn.close()

        report_msg += "\n⚙️ <b>[관제탑 자율 튜닝 결과 스냅샷]</b>\n"
        report_msg += f" ▪️ <b>현재 국면:</b> {regime}\n"
        report_msg += f" ▪️ <b>동적 켈리 비중:</b> {float(cfg.get('DYNAMIC_KELLY_RISK', 0.01)) * 100:.1f}%\n"
        report_msg += f" ▪️ <b>블랙스완 게이트:</b> {'ON' if gate.get('enabled') else 'OFF'} {gate.get('reason', '')}\n"
        report_msg += f" ▪️ <b>글로벌 서킷:</b> {cfg.get('GLOBAL_CIRCUIT_BREAKER', 'OFF')}\n"
        report_msg += f" ▪️ <b>S1/S4 가중:</b> {cfg.get('WEIGHT_S1', '-') } / {cfg.get('WEIGHT_S4', '-')}\n"

        alpha_m = cfg.get("ALPHA_DECAY_MONITOR") or {}
        if isinstance(alpha_m, dict) and alpha_m:
            report_msg += (
                f" ▪️ <b>알파 반감기:</b> 초기 PF {alpha_m.get('early_pf')} → 후기 PF {alpha_m.get('late_pf')} "
                f"(비율 {alpha_m.get('ratio')})\n"
            )

        ts = cfg.get("TS_KELLY_BY_SIDE") or {}
        if isinstance(ts, dict) and ts:
            long_r = ts.get("LONG", {}).get("risk")
            short_r = ts.get("SHORT", {}).get("risk")
            report_msg += f" ▪️ <b>Thompson 켈리:</b> LONG {long_r} | SHORT {short_r}\n"

        cos_pct = cfg.get("DYNAMIC_SUPERNOVA_CUTOFF") or cfg.get("DYNAMIC_ALPHA_LIMIT")
        ml_pct = cfg.get("DYNAMIC_ML_BOX_CUTOFF")
        if cos_pct is not None:
            report_msg += f" ▪️ <b>허들(가용 시):</b> 코사인/알파 {float(cos_pct) * 100:.0f}%"
            if ml_pct is not None:
                report_msg += f" | ML박스 {float(ml_pct) * 100:.0f}%"
            report_msg += "\n"

        promo_raw = str(cfg.get("LIVE_A_PROMOTION_DATE", today_str))[:10]
        promo_dt = parse_utc_iso(promo_raw)
        days_live = max(0, (now.date() - (promo_dt.date() if promo_dt else now.date())).days)
        report_msg += f" ▪️ <b>로직 유지 일수:</b> LIVE 기준 {days_live}일차 (기준일 {promo_raw})\n"

    except Exception as e:
        report_msg += f"\n⚠️ 주간 리포트 생성 중 에러: {e}"

    report_msg += (
        "\n━━━━━━━━━━━━━━━━━━\n"
        "<i>💡 일주일간 시장 궤적과 관제탑 튜닝 상태를 한 장으로 묶은 코인용 마스터 결과지입니다.</i>"
    )

    try:
        from bitget.evolution.weekly_proprietary_regime_bg import build_weekly_shadow_pri_html

        spot_pnl = None
        fut_pnl = None
        try:
            conn = get_connection(DB_PATH, read_only=True)
            for mk, var in (("spot", "spot_pnl"), ("futures", "fut_pnl")):
                pnl_q, pnl_p = forward_weekly_market_pnl_sum_sql(
                    market_type=mk, since_date=week_ago
                )
                row = conn.execute(pnl_q, pnl_p).fetchone()
                val = float((row or (0.0,))[0] or 0.0)
                if mk == "spot":
                    spot_pnl = val
                else:
                    fut_pnl = val
            conn.close()
        except Exception:
            pass
        report_msg += build_weekly_shadow_pri_html(
            week_start=week_ago,
            week_end=today_str,
            spot_week_pnl=spot_pnl,
            futures_week_pnl=fut_pnl,
        )
    except Exception as ex:
        report_msg += f"\n⚠️ Shadow PRI 블록 스킵: {ex}"

    send_telegram_msg(report_msg)


def _report_flag_sent_today(cfg, day_key: str) -> bool:
    flags = cfg.get("AUTO_PILOT_DAILY_REPORT_FLAG", {})
    if not isinstance(flags, dict):
        return False
    node = flags.get(day_key, {})
    return bool(isinstance(node, dict) and node.get("sent"))


def _mark_report_flag(day_key: str, reason: str) -> None:
    cfg = load_config()
    flags = cfg.get("AUTO_PILOT_DAILY_REPORT_FLAG", {})
    if not isinstance(flags, dict):
        flags = {}
    flags[day_key] = {
        "sent": True,
        "sent_at": utc_datetime_str_tz(),
        "reason": reason,
    }
    sorted_days = sorted(flags.keys(), reverse=True)
    cfg["AUTO_PILOT_DAILY_REPORT_FLAG"] = {k: flags[k] for k in sorted_days[:14]}
    save_config_atomic(cfg)


def _safe_call_ai_modules_for_report():
    """
    AI 서버 이슈로 스케줄러 전체가 멈추지 않도록 bitget_ai_overseer 호출 보호.
    """
    try:
        import bitget.ai_overseer as bitget_ai_overseer
        if hasattr(bitget_ai_overseer, "run_ai_auditor"):
            bitget_ai_overseer.run_ai_auditor()
    except Exception as e:
        log_exception(logger, "[safety-net] bitget_ai_overseer call failed (ignored): %s", e)


def _safe_run_satellite(lock_key: str, lock_sec: int, module_name: str, func_name: str, *args, **kwargs):
    try:
        if not schedule_acquire(lock_key, lock_sec):
            return
        mod = __import__(module_name)
        fn = getattr(mod, func_name, None)
        if callable(fn):
            fn(*args, **kwargs)
    except Exception as e:
        log_exception(
            logger,
            "[safety-net] %s.%s call failed (ignored): %s",
            module_name,
            func_name,
            e,
        )


def system_main_loop() -> None:
    """
    REMOVED — inline daemon loop caused duplicate pipelines with bitget_auto_pilot.

    SSOT:
      python -m bitget.pipelines.bitget_auto_pilot --daemon
      bitget/deploy/bitget.sh --scan-all|--daily-audit|...
    """
    raise RuntimeError(
        "bitget.auto_pilot.system_main_loop is removed. "
        "Use python -m bitget.pipelines.bitget_auto_pilot --daemon "
        "and bitget/deploy/bitget.sh for cron pipelines."
    )
