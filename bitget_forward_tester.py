import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")
TELEGRAM_TOKEN = os.environ.get("BITGET_TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("BITGET_TELEGRAM_CHAT_ID", "")


def send_telegram_msg(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass


def load_system_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_system_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def _ensure_col(cur, col_name, col_type):
    try:
        cur.execute(f"ALTER TABLE bitget_forward_trades ADD COLUMN {col_name} {col_type}")
    except Exception:
        pass


def init_forward_db():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bitget_forward_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT,
            market_type TEXT,
            symbol TEXT,
            timeframe TEXT,
            sig_type TEXT,
            tier TEXT,
            total_score REAL,
            dyn_rs REAL DEFAULT 0.0,
            dyn_cpv REAL DEFAULT 0.0,
            dyn_tb REAL DEFAULT 0.0,
            entry_price REAL,
            entry_atr REAL,
            atr_sl_mult REAL,
            stop_price REAL,
            leverage REAL DEFAULT 1.0,
            tf_weight REAL DEFAULT 1.0,
            sim_kelly_risk_pct REAL DEFAULT 0.01,
            margin_used REAL DEFAULT 0.0,
            sim_kelly_invest REAL DEFAULT 0.0,
            quantity REAL DEFAULT 0.0,
            v_cpv REAL DEFAULT 0.0,
            v_yang REAL DEFAULT 0.0,
            v_energy REAL DEFAULT 0.0,
            v_rs REAL DEFAULT 0.0,
            max_high REAL,
            min_low REAL,
            bars_held INTEGER DEFAULT 0,
            up_vol_sum REAL DEFAULT 0.0,
            down_vol_sum REAL DEFAULT 0.0,
            status TEXT DEFAULT 'OPEN',
            exit_date TEXT,
            exit_reason TEXT,
            final_ret REAL,
            mfe REAL DEFAULT 0.0,
            exit_type TEXT DEFAULT 'UNKNOWN',
            sim_stat_ret REAL DEFAULT 0.0,
            sim_stat_status TEXT DEFAULT 'OPEN',
            sim_tech_ret REAL DEFAULT 0.0,
            sim_tech_status TEXT DEFAULT 'OPEN',
            sim_breadth_ret REAL DEFAULT 0.0,
            sim_breadth_status TEXT DEFAULT 'OPEN',
            entry_breadth REAL DEFAULT 1.0,
            live_a_ret REAL DEFAULT 0.0,
            live_a_status TEXT DEFAULT 'OPEN',
            cand_b_ret REAL DEFAULT 0.0,
            cand_b_status TEXT DEFAULT 'OPEN',
            champ_c_ret REAL DEFAULT 0.0,
            champ_c_status TEXT DEFAULT 'OPEN',
            flow_tags TEXT DEFAULT ''
        )
        """
    )
    _ensure_col(cur, "mfe", "REAL DEFAULT 0.0")
    _ensure_col(cur, "exit_type", "TEXT DEFAULT 'UNKNOWN'")
    _ensure_col(cur, "sim_stat_ret", "REAL DEFAULT 0.0")
    _ensure_col(cur, "sim_stat_status", "TEXT DEFAULT 'OPEN'")
    _ensure_col(cur, "sim_tech_ret", "REAL DEFAULT 0.0")
    _ensure_col(cur, "sim_tech_status", "TEXT DEFAULT 'OPEN'")
    _ensure_col(cur, "sim_breadth_ret", "REAL DEFAULT 0.0")
    _ensure_col(cur, "sim_breadth_status", "TEXT DEFAULT 'OPEN'")
    _ensure_col(cur, "entry_breadth", "REAL DEFAULT 1.0")
    _ensure_col(cur, "live_a_ret", "REAL DEFAULT 0.0")
    _ensure_col(cur, "live_a_status", "TEXT DEFAULT 'OPEN'")
    _ensure_col(cur, "cand_b_ret", "REAL DEFAULT 0.0")
    _ensure_col(cur, "cand_b_status", "TEXT DEFAULT 'OPEN'")
    _ensure_col(cur, "champ_c_ret", "REAL DEFAULT 0.0")
    _ensure_col(cur, "champ_c_status", "TEXT DEFAULT 'OPEN'")
    _ensure_col(cur, "flow_tags", "TEXT DEFAULT ''")

    cur.execute("DROP VIEW IF EXISTS forward_trades")
    cur.execute(
        """
        CREATE VIEW forward_trades AS
        SELECT
            id,
            entry_date,
            CASE WHEN market_type='spot' THEN 'SPOT' ELSE 'FUT' END AS market,
            symbol AS code,
            symbol AS name,
            timeframe AS sector,
            sig_type,
            tier,
            total_score,
            dyn_rs,
            dyn_cpv,
            dyn_tb,
            entry_price,
            v_cpv,
            v_yang,
            v_rs,
            v_energy,
            max_high,
            min_low,
            bars_held,
            up_vol_sum,
            down_vol_sum,
            status,
            exit_date,
            exit_reason,
            final_ret,
            mfe,
            sim_kelly_risk_pct,
            sim_kelly_invest,
            margin_used AS invest_amount,
            CAST(quantity AS INTEGER) AS shares,
            entry_atr,
            exit_type,
            sim_stat_ret,
            sim_stat_status,
            sim_tech_ret,
            sim_tech_status,
            sim_breadth_ret,
            sim_breadth_status,
            entry_breadth,
            live_a_ret,
            live_a_status,
            cand_b_ret,
            cand_b_status,
            champ_c_ret,
            champ_c_status,
            flow_tags
        FROM bitget_forward_trades
        """
    )
    conn.commit()
    conn.close()


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
        except Exception:
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


def try_add_virtual_position(
    market_type,
    symbol,
    timeframe,
    sig_type,
    score,
    entry_price,
    facts,
):
    init_forward_db()
    cfg = load_system_config()
    tf = str(timeframe).upper()
    market_type = str(market_type).lower()
    symbol = str(symbol)

    score_bucket = int(float(score) // 10) * 10
    if score_bucket >= 100:
        score_bucket = 90
    tier_label = f"{score_bucket}점대"

    conn = sqlite3.connect(DB_PATH, timeout=60)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")

    cur.execute(
        "SELECT id FROM bitget_forward_trades WHERE symbol=? AND timeframe=? AND market_type=? AND status='OPEN'",
        (symbol, tf, market_type),
    )
    if cur.fetchone():
        conn.close()
        return False, "중복 보유 중"

    hist_df = _load_hist(conn, market_type, symbol, tf, limit=300)
    if hist_df is None or len(hist_df) < 60:
        conn.close()
        return False, "ATR 계산용 히스토리 부족"

    entry_atr = _calc_atr14(hist_df)
    atr_sl_mult = float(cfg.get("ATR_SL_MULT", 2.0))
    stop_price = float(entry_price) - (atr_sl_mult * entry_atr)
    risk_distance = float(entry_price) - stop_price
    if risk_distance <= 0:
        conn.close()
        return False, "리스크 거리 계산 실패"

    fixed_risk_pct = float(cfg.get("FIXED_RISK_PCT", 0.02))
    kelly_risk_pct = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01))
    w_s1 = float(cfg.get("WEIGHT_S1", 1.0) or 1.0)
    w_s4 = float(cfg.get("WEIGHT_S4", 1.0) or 1.0)
    breadth_now = _calc_market_breadth(conn)
    # breadth 기반 미세 보정: 알트 확산이면 추세(S1), 쏠림이면 돌파/역추세(S4/S6/S7) 가중
    if breadth_now > 1.03:
        w_s1 *= 1.15
    elif breadth_now < 0.97:
        w_s1 *= 0.85
        w_s4 *= 1.15
    if "S1" in sig_type:
        kelly_risk_pct *= w_s1
    if "S4" in sig_type or "S6" in sig_type or "S7" in sig_type:
        kelly_risk_pct *= w_s4
    tf_weight = _tf_weight(tf, cfg)
    kelly_risk_pct *= tf_weight

    core_group = _extract_core_group(sig_type)
    account_size = float(cfg.get("ACCOUNT_SIZE_USDT", 100000))
    max_position_pct = float(cfg.get("MAX_POSITION_PCT", 0.25))

    cur.execute(
        "SELECT SUM((sim_kelly_invest * final_ret) / 100.0) FROM bitget_forward_trades WHERE status LIKE 'CLOSED%' AND sig_type LIKE ?",
        (f"%{core_group}%",),
    )
    realized_pnl = float(cur.fetchone()[0] or 0.0)
    group_current_seed = account_size + realized_pnl

    cur.execute(
        "SELECT SUM(margin_used) FROM bitget_forward_trades WHERE status='OPEN' AND sig_type LIKE ?",
        (f"%{core_group}%",),
    )
    locked_cash = float(cur.fetchone()[0] or 0.0)
    available_cash = group_current_seed - locked_cash
    if available_cash <= 0:
        conn.close()
        return False, f"예수금 부족: [{core_group}] 가용 자산 없음"

    treasury_key = "TREASURY_SPOT_USDT" if market_type == "spot" else "TREASURY_FUTURES_USDT"
    treasury_balance = float(cfg.get(treasury_key, 100000.0))
    if treasury_balance <= 0:
        conn.close()
        return False, f"{treasury_key} 잔고 부족"

    leverage = 1.0 if market_type == "spot" else float(cfg.get("FUTURES_LEVERAGE", 3.0))
    max_invest_limit = min(group_current_seed * max_position_pct, available_cash, treasury_balance)

    raw_qty = float((group_current_seed * kelly_risk_pct) / risk_distance)
    raw_notional = raw_qty * float(entry_price)
    if market_type == "futures":
        raw_notional *= leverage
    margin_required = raw_notional if market_type == "spot" else raw_notional / max(leverage, 1e-9)

    if margin_required > max_invest_limit:
        margin_used = max_invest_limit
        sim_kelly_invest = margin_used if market_type == "spot" else margin_used * leverage
    else:
        margin_used = margin_required
        sim_kelly_invest = raw_notional

    quantity = sim_kelly_invest / float(entry_price) if float(entry_price) > 0 else 0.0
    if quantity <= 0:
        conn.close()
        return False, "수량 산출 실패"

    fixed_qty = float((group_current_seed * fixed_risk_pct) / risk_distance)
    fixed_notional = fixed_qty * float(entry_price)
    if market_type == "futures":
        fixed_notional *= leverage
    _ = fixed_notional

    cfg[treasury_key] = max(0.0, treasury_balance - margin_used)
    save_system_config(cfg)

    now = datetime.utcnow().strftime("%Y-%m-%d")
    cur.execute(
        """
        INSERT INTO bitget_forward_trades
        (entry_date, market_type, symbol, timeframe, sig_type, tier, total_score, dyn_rs, dyn_cpv, dyn_tb,
         entry_price, entry_atr, atr_sl_mult, stop_price, leverage, tf_weight, sim_kelly_risk_pct, margin_used,
         sim_kelly_invest, quantity, v_cpv, v_yang, v_energy, v_rs, max_high, min_low, status,
         sim_stat_status, sim_tech_status, sim_breadth_status, entry_breadth, live_a_status, cand_b_status, champ_c_status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN',
                'OPEN', 'OPEN', 'OPEN', ?, 'OPEN', 'OPEN', 'OPEN')
        """,
        (
            now,
            market_type,
            symbol,
            tf,
            sig_type,
            tier_label,
            float(score),
            float(facts.get("dyn_rs", 0)),
            float(facts.get("dyn_cpv", 0)),
            float(facts.get("dyn_tb", 0)),
            float(entry_price),
            round(entry_atr, 6),
            atr_sl_mult,
            float(stop_price),
            leverage,
            tf_weight,
            float(kelly_risk_pct),
            float(margin_used),
            float(sim_kelly_invest),
            float(quantity),
            float(facts.get("v_cpv", 0)),
            float(facts.get("v_yang", 0)),
            float(facts.get("v_energy", 0)),
            float(facts.get("v_rs", 0)),
            float(entry_price),
            float(entry_price),
            float(breadth_now),
        ),
    )
    conn.commit()
    conn.close()
    return True, f"편입 성공: {symbol} {tf} [{market_type}] kelly={kelly_risk_pct:.4f} tf_w={tf_weight:.2f} lev={leverage:.1f}"


def _get_latest_bar(conn, market_type, symbol, timeframe):
    df = _load_hist(conn, market_type, symbol, timeframe, limit=3)
    if df is None or len(df) < 2:
        return None
    last = df.iloc[-1]
    prev = df.iloc[-2]
    return {
        "close": float(last["Close"]),
        "open": float(last["Open"]),
        "high": float(last["High"]),
        "low": float(last["Low"]),
        "vol": float(last["Volume"]),
        "prev_close": float(prev["Close"]),
        "date": str(last["Date"].date()),
        "hist_df": _load_hist(conn, market_type, symbol, timeframe, limit=120),
    }


def track_daily_positions(market_type):
    init_forward_db()
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    df_active = pd.read_sql(
        "SELECT * FROM bitget_forward_trades WHERE market_type=? AND status='OPEN'",
        conn,
        params=(str(market_type).lower(),),
    )
    if df_active.empty:
        conn.close()
        return

    cfg = load_system_config()
    breadth_now = _calc_market_breadth(conn)
    print(f"\n🔍 [포워드 테스팅] {market_type} {len(df_active)}개 포지션 추적 중...")

    for _, r in df_active.iterrows():
        try:
            latest = _get_latest_bar(conn, r["market_type"], r["symbol"], r["timeframe"])
            if latest is None:
                continue
            c = latest["close"]
            o = latest["open"]
            h = latest["high"]
            l = latest["low"]
            v = latest["vol"]
            ep = float(r["entry_price"])
            current_ret_pct = ((c - ep) / ep) * 100.0
            low_ret_pct = ((l - ep) / ep) * 100.0
            high_ret_pct = ((h - ep) / ep) * 100.0

            new_max = max(float(r["max_high"]), h)
            new_min = min(float(r["min_low"]), l)
            new_bars = int(r["bars_held"]) + 1
            new_up_vol = float(r["up_vol_sum"]) + (v if c > o else 0.0)
            new_down_vol = float(r["down_vol_sum"]) + (v if c < o else 0.0)

            hist = latest["hist_df"]
            if hist is None or len(hist) < 20:
                continue
            cur_atr = _calc_atr14(hist)
            entry_atr = float(r["entry_atr"] or 0.0)
            if entry_atr <= 0:
                entry_atr = cur_atr

            hist["ema10"] = hist["Close"].ewm(span=10, adjust=False).mean()
            hist["ema20"] = hist["Close"].ewm(span=20, adjust=False).mean()
            z_ema1 = hist["Close"].ewm(span=20, adjust=False).mean()
            z_ema2 = z_ema1.ewm(span=20, adjust=False).mean()
            cur_zlema = float((z_ema1 + (z_ema1 - z_ema2)).iloc[-1])
            is_tech_exit = (c < cur_zlema) or (
                float(hist["ema10"].iloc[-1]) < float(hist["ema20"].iloc[-1])
                and float(hist["ema10"].iloc[-2]) >= float(hist["ema20"].iloc[-2])
            )

            ns_prefix = f"{str(r['timeframe']).upper()}_LIVE_PARAMS"
            live_params = cfg.get(ns_prefix, {"DYNAMIC_MAE_SL": -3.5, "DYNAMIC_MFE_TP": 10.0})
            dyn_mae_sl = float(live_params.get("DYNAMIC_MAE_SL", -3.5))
            dyn_mfe_tp = float(live_params.get("DYNAMIC_MFE_TP", 10.0))
            opt_time_stop = int(cfg.get(f"{str(r['timeframe']).upper()}_TIME_STOP", 10))
            opt_sl_atr = float(r["atr_sl_mult"] or cfg.get("ATR_SL_MULT", 2.0))
            sl_price = ep - (opt_sl_atr * entry_atr)
            entry_breadth = float(r.get("entry_breadth", 1.0) or 1.0)
            breadth_delta = breadth_now - entry_breadth
            breadth_collapse = breadth_now < 0.95 and breadth_delta < -0.03

            do_exit = False
            exit_rsn = ""
            actual_exit_type = "HOLD"
            actual_exit_price = c

            # MFE/MAE 1순위
            if low_ret_pct <= dyn_mae_sl:
                do_exit = True
                exit_rsn = f"수학적 MAE 장중 이탈 칼손절 ({dyn_mae_sl:.2f}%)"
                actual_exit_type = "STAT_MAE"
                actual_exit_price = ep * (1.0 + dyn_mae_sl / 100.0)
            elif high_ret_pct >= dyn_mfe_tp:
                if c < l + (h - l) * 0.7:
                    do_exit = True
                    exit_rsn = f"수학적 MFE 장중 도달 익절 ({dyn_mfe_tp:.2f}%)"
                    actual_exit_type = "STAT_MFE"
                    actual_exit_price = ep * (1.0 + dyn_mfe_tp / 100.0)

            # ATR/TimeStop/TECH 2순위
            if not do_exit:
                if new_bars >= opt_time_stop and current_ret_pct < 3.0:
                    do_exit = True
                    exit_rsn = f"타임스탑 ({opt_time_stop} bars)"
                    actual_exit_type = "TIME_STOP"
                elif breadth_collapse and current_ret_pct < 1.5:
                    do_exit = True
                    exit_rsn = f"시장폭 붕괴 청산 (entry {entry_breadth:.3f} -> now {breadth_now:.3f})"
                    actual_exit_type = "BREADTH_EXIT"
                elif l <= sl_price:
                    do_exit = True
                    exit_rsn = f"ATR {opt_sl_atr:.2f}배 장중 방어 손절"
                    actual_exit_type = "ATR_STOP"
                    actual_exit_price = sl_price
                elif is_tech_exit:
                    do_exit = True
                    exit_rsn = "기술적 추세 이탈 (ZLEMA/EMA10-20 데드)"
                    actual_exit_type = "TECH_EXIT"

            # 장기 좀비 청소
            if not do_exit and new_bars >= max(20, opt_time_stop * 2):
                do_exit = True
                exit_rsn = "장기 거래 정체 포지션 강제 청소"
                actual_exit_type = "ZOMBIE_FORCE_CLOSE"
                actual_exit_price = ep

            # 평행우주 기록
            sim_stat_ret = dyn_mae_sl if low_ret_pct <= dyn_mae_sl else (dyn_mfe_tp if high_ret_pct >= dyn_mfe_tp else current_ret_pct)
            sim_tech_ret = dyn_mae_sl if low_ret_pct <= dyn_mae_sl else (current_ret_pct if not is_tech_exit else current_ret_pct)
            sim_breadth_ret = current_ret_pct
            sim_stat_status = "CLOSED_LOSS" if low_ret_pct <= dyn_mae_sl else ("CLOSED_WIN" if high_ret_pct >= dyn_mfe_tp else "OPEN")
            sim_tech_status = "CLOSED_LOSS" if low_ret_pct <= dyn_mae_sl else ("CLOSED_WIN" if is_tech_exit else "OPEN")
            sim_breadth_status = "CLOSED_LOSS" if breadth_collapse and current_ret_pct < 0 else ("CLOSED_WIN" if breadth_now > 1.05 and current_ret_pct > 0 else "OPEN")

            live_a_ret = sim_tech_ret
            cand_b_ret = sim_stat_ret
            champ_c_ret = current_ret_pct
            live_a_status = sim_tech_status
            cand_b_status = sim_stat_status
            champ_c_status = "CLOSED_WIN" if do_exit and current_ret_pct > 0 else ("CLOSED_LOSS" if do_exit else "OPEN")

            if do_exit:
                ret = round(((actual_exit_price - ep) / ep) * 100.0, 2)
                mfe = round(((new_max - ep) / ep) * 100.0, 2)
                tags = []
                if mfe >= 15.0 and new_bars <= 10:
                    tags.append("#빠른슈팅_완벽")
                elif mfe >= 8.0:
                    tags.append("#지연슈팅_수명연장")
                elif mfe < 3.0:
                    tags.append("#슈팅실패_조기소멸")
                vol_ratio = new_up_vol / (new_down_vol + 1.0)
                if vol_ratio >= 1.5:
                    tags.append("#건전한조정_매집우위")
                elif vol_ratio < 0.8:
                    tags.append("#음봉대량거래_세력이탈")
                flow_tags = " ".join(tags)

                conn.execute(
                    """
                    UPDATE bitget_forward_trades
                    SET status=?, exit_date=?, exit_reason=?, final_ret=?, mfe=?, max_high=?, min_low=?, bars_held=?,
                        up_vol_sum=?, down_vol_sum=?, exit_type=?, sim_stat_ret=?, sim_stat_status=?, sim_tech_ret=?, sim_tech_status=?,
                        sim_breadth_ret=?, sim_breadth_status=?, entry_breadth=?, live_a_ret=?, live_a_status=?, cand_b_ret=?, cand_b_status=?, champ_c_ret=?, champ_c_status=?, flow_tags=?
                    WHERE id=?
                    """,
                    (
                        "CLOSED_WIN" if ret > 0 else "CLOSED_LOSS",
                        datetime.utcnow().strftime("%Y-%m-%d"),
                        exit_rsn,
                        ret,
                        mfe,
                        new_max,
                        new_min,
                        new_bars,
                        new_up_vol,
                        new_down_vol,
                        actual_exit_type,
                        sim_stat_ret,
                        sim_stat_status,
                        sim_tech_ret,
                        sim_tech_status,
                        sim_breadth_ret,
                        sim_breadth_status,
                        float(entry_breadth),
                        live_a_ret,
                        live_a_status,
                        cand_b_ret,
                        cand_b_status,
                        champ_c_ret,
                        champ_c_status,
                        flow_tags,
                        int(r["id"]),
                    ),
                )

                # 국고 환입
                treasury_key = "TREASURY_SPOT_USDT" if r["market_type"] == "spot" else "TREASURY_FUTURES_USDT"
                cur_cfg = load_system_config()
                before = float(cur_cfg.get(treasury_key, 0.0))
                margin_used = float(r.get("margin_used", 0.0) or 0.0)
                pnl = float(r.get("sim_kelly_invest", 0.0) or 0.0) * (ret / 100.0)
                cur_cfg[treasury_key] = max(0.0, before + margin_used + pnl)
                save_system_config(cur_cfg)

                icon = "🔥스마트청산" if ret > 0 else "🛡️방어손절"
                send_telegram_msg(
                    f"🤖 [{str(r['market_type']).upper()} 관제탑] {icon}: {r['symbol']} ({r['sig_type']} | {round(float(r['total_score']),1)}점)\n"
                    f"▪️ 수익: {ret}%\n▪️ 사유: {exit_rsn}\n▪️ 태그: {flow_tags}"
                )
            else:
                conn.execute(
                    """
                    UPDATE bitget_forward_trades
                    SET max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?,
                        sim_stat_ret=?, sim_stat_status=?, sim_tech_ret=?, sim_tech_status=?,
                        sim_breadth_ret=?, sim_breadth_status=?, entry_breadth=?,
                        live_a_ret=?, live_a_status=?, cand_b_ret=?, cand_b_status=?, champ_c_ret=?, champ_c_status=?
                    WHERE id=?
                    """,
                    (
                        new_max,
                        new_min,
                        new_bars,
                        new_up_vol,
                        new_down_vol,
                        sim_stat_ret,
                        sim_stat_status,
                        sim_tech_ret,
                        sim_tech_status,
                        sim_breadth_ret,
                        sim_breadth_status,
                        float(entry_breadth),
                        live_a_ret,
                        live_a_status,
                        cand_b_ret,
                        cand_b_status,
                        champ_c_ret,
                        champ_c_status,
                        int(r["id"]),
                    ),
                )
        except Exception:
            continue

    conn.commit()
    conn.close()


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


def send_comprehensive_daily_report():
    init_forward_db()
    cfg = load_system_config()
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")

    for market_type in ["spot", "futures"]:
        m_icon = "🟢" if market_type == "spot" else "🟠"
        df_all = pd.read_sql(
            "SELECT * FROM bitget_forward_trades WHERE market_type=?",
            conn,
            params=(market_type,),
        )
        if df_all.empty:
            continue
        df_closed = df_all[df_all["status"].str.contains("CLOSED", na=False)].copy()
        df_open = df_all[df_all["status"] == "OPEN"].copy()

        treasury_key = "TREASURY_SPOT_USDT" if market_type == "spot" else "TREASURY_FUTURES_USDT"
        treasury = float(cfg.get(treasury_key, 0.0))
        regime = cfg.get("CURRENT_REGIME_KEY", "UNKNOWN")
        kelly = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01)) * 100.0
        b_status = str(cfg.get("CRYPTO_BREADTH_STATUS", "NEUTRAL"))
        w1 = float(cfg.get("WEIGHT_S1", 1.0))
        w4 = float(cfg.get("WEIGHT_S4", 1.0))

        # [1/6] 거시+국고
        msg1 = f"{m_icon} <b>[1/6] {market_type.upper()} 국면/국고 현황</b>\n"
        msg1 += f"📅 {datetime.utcnow().strftime('%Y-%m-%d')} | 국면: <b>{regime}</b>\n"
        msg1 += f"🏦 잔여 국고: <b>{treasury:,.2f} USDT</b>\n"
        msg1 += f"⚖️ 동적 켈리: {kelly:.2f}%\n"
        msg1 += f"🌊 Breadth: {b_status} | base_w1={w1:.2f}, base_w4={w4:.2f}\n"
        send_telegram_msg(msg1)
        time.sleep(1.0)

        # [2/6] 리더보드
        msg2 = f"{m_icon} <b>[2/6] 로직별 복리 리더보드</b>\n"
        groups = {}
        for _, r in df_all.iterrows():
            g = _extract_core_group(r.get("sig_type", "UNKNOWN"))
            groups.setdefault(g, {"closed": [], "open": 0})
            if str(r.get("status", "")).startswith("OPEN"):
                groups[g]["open"] += 1
            else:
                groups[g]["closed"].append(float(r.get("final_ret", 0.0) or 0.0))
        board = []
        base_seed = float(cfg.get("ACCOUNT_SIZE_USDT", 100000))
        for g, v in groups.items():
            s = pd.Series(v["closed"], dtype=float)
            pnl = ((s / 100.0) * base_seed * 0.01).sum()
            bal = base_seed + pnl
            wr = float((s > 0).mean() * 100.0) if len(s) else 0.0
            board.append((g, bal, wr, v["open"]))
        board.sort(key=lambda x: x[1], reverse=True)
        for i, (g, bal, wr, op) in enumerate(board[:7]):
            medal = "🥇" if i == 0 else ("🥈" if i == 1 else ("🥉" if i == 2 else "🏃"))
            msg2 += f"{medal} <b>{g}</b>: {bal:,.2f} USDT (승률 {wr:.1f}% / OPEN {op})\n"
        send_telegram_msg(msg2)
        time.sleep(1.0)

        # [3/6] 자금관리 결투
        msg3 = f"{m_icon} <b>[3/6] 자금관리 진검승부</b>\n"
        if not df_closed.empty:
            kelly_pnl = float((df_closed["sim_kelly_invest"] * df_closed["final_ret"] / 100.0).sum())
            fixed_pnl = float((df_closed["margin_used"] * df_closed["final_ret"] / 100.0).sum())
        else:
            kelly_pnl = 0.0
            fixed_pnl = 0.0
        msg3 += f"💰 누적 켈리 손익: <b>{kelly_pnl:+,.2f} USDT</b>\n"
        msg3 += f"🛡️ 누적 고정 손익: {fixed_pnl:+,.2f} USDT\n"
        msg3 += f"🏁 우위: {'동적 켈리' if kelly_pnl > fixed_pnl else '고정 리스크'}\n"
        send_telegram_msg(msg3)
        time.sleep(1.0)

        # [4/6] 티어/데스콤보
        msg4 = f"{m_icon} <b>[4/6] 티어/필터 검증</b>\n"
        if not df_closed.empty:
            t1 = df_closed[df_closed["total_score"] >= 80]
            if not t1.empty:
                msg4 += f"💎 1티어 승률: {(t1['final_ret'] > 0).mean()*100:.1f}% | PF {_pf(t1['final_ret']):.2f}\n"
            msg4 += f"⚙️ 전체 PF: {_pf(df_closed['final_ret']):.2f}\n"
        else:
            msg4 += "표본 부족\n"
        send_telegram_msg(msg4)
        time.sleep(1.0)

        # [5/6] TF별 데스매치
        msg5 = f"{m_icon} <b>[5/6] TF별 데스매치</b>\n"
        for tf in ["1D", "4H", "2H", "1H"]:
            sub = df_closed[df_closed["timeframe"].astype(str).str.upper() == tf]
            if sub.empty:
                continue
            st = sub[sub["sig_type"].astype(str).str.contains("STANDARD", na=False)]
            sn = sub[sub["sig_type"].astype(str).str.contains("SUPERNOVA", na=False)]
            st_pf = _pf(st["final_ret"]) if not st.empty else 0.0
            sn_pf = _pf(sn["final_ret"]) if not sn.empty else 0.0
            winner = "SUPERNOVA" if sn_pf > st_pf else "STANDARD"
            msg5 += f"▪️ {tf}: STD {st_pf:.2f} vs SN {sn_pf:.2f} → <b>{winner}</b>\n"
        send_telegram_msg(msg5)
        time.sleep(1.0)

        # [6/6] 오픈포지션 스냅샷
        msg6 = f"{m_icon} <b>[6/6] 오픈 포지션 스냅샷</b>\n"
        msg6 += f"📌 OPEN 개수: {len(df_open)}\n"
        if not df_open.empty:
            top = df_open.sort_values("total_score", ascending=False).head(5)
            for _, r in top.iterrows():
                msg6 += f" - {r['symbol']} [{r['timeframe']}] {float(r['total_score']):.1f}점 / {r['sig_type']}\n"
        send_telegram_msg(msg6)
        time.sleep(1.0)

    conn.close()


def run_deep_dive_analysis(market_type="spot"):
    """
    미래 데이터(포워드 테스팅)를 기반으로 내 시스템의 과최적화를 검증하고,
    대박/참사 종목의 DNA와 티어별 진짜 승률을 텔레그램으로 보고합니다.
    """
    try:
        init_forward_db()
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        df = pd.read_sql(
            "SELECT * FROM bitget_forward_trades WHERE market_type=? AND status LIKE 'CLOSED%'",
            conn,
            params=(str(market_type).lower(),),
        )
        conn.close()

        if len(df) < 10:
            print(f"⚠️ [{market_type}] 아직 통계를 낼 만큼 청산된 데이터가 충분하지 않습니다. (최소 10개 필요)")
            return

        df["Win"] = np.where(pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) > 0, 1, 0)
        report_msg = f"🔬 [{str(market_type).upper()}장 포워드 테스팅 딥 다이브 분석]\n(총 {len(df)}개 실전 검증 데이터 기반)\n\n"

        for t in range(10, 100, 10):
            tier_label = f"{t}점대"
            t_df = df[df["tier"] == tier_label].copy()
            if len(t_df) < 5:
                continue
            report_msg += f"📌 <b>[{tier_label} 구간 심층 분석]</b>\n"
            t_wr, t_pf = _calculate_metrics(t_df, "final_ret")
            report_msg += f"▪️ 성적: 승률 {t_wr:.1f}% | PF {t_pf:.2f}\n"

            winners = t_df[pd.to_numeric(t_df["final_ret"], errors="coerce").fillna(0.0) > 5.0]
            sideways = t_df[
                (pd.to_numeric(t_df["final_ret"], errors="coerce").fillna(0.0) >= -3.0)
                & (pd.to_numeric(t_df["final_ret"], errors="coerce").fillna(0.0) <= 5.0)
            ]
            losers = t_df[pd.to_numeric(t_df["final_ret"], errors="coerce").fillna(0.0) < -3.0]

            def get_dna(sub_df):
                if len(sub_df) == 0:
                    return "표본없음"
                rs = pd.to_numeric(sub_df.get("dyn_rs", pd.Series(dtype=float)), errors="coerce").dropna()
                cpv = pd.to_numeric(sub_df.get("dyn_cpv", pd.Series(dtype=float)), errors="coerce").dropna()
                eng = pd.to_numeric(sub_df.get("v_energy", pd.Series(dtype=float)), errors="coerce").dropna()
                if rs.empty or cpv.empty or eng.empty:
                    return "표본없음"
                return f"RS:{(10-rs.mean())*11.1:.1f}% | CPV:{(10-cpv.mean())*11.1:.1f}% | ENG:{eng.mean():.1f}"

            report_msg += f" ✅ 대박 DNA: {get_dna(winners)}\n"
            report_msg += f" ↔️ 횡보 DNA: {get_dna(sideways)}\n"
            report_msg += f" 💀 참사 DNA: {get_dna(losers)}\n"
            if len(winners) > 0 and len(losers) > 0:
                w_eng = pd.to_numeric(winners.get("v_energy", pd.Series(dtype=float)), errors="coerce").dropna()
                l_eng = pd.to_numeric(losers.get("v_energy", pd.Series(dtype=float)), errors="coerce").dropna()
                if not w_eng.empty and not l_eng.empty and w_eng.mean() > l_eng.mean() + 1.0:
                    report_msg += f" 💡 통찰: {tier_label}는 에너지가 높을 때만 날아갑니다. 에너지 낮은 종목은 거르십시오.\n"
            report_msg += "\n"

        report_msg += "🌍 [전체 티어 통합: 유니버설(Universal) DNA 분석]\n"
        all_winners = df[pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) > 5.0]
        all_sideways = df[
            (pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) >= -3.0)
            & (pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) <= 5.0)
        ]
        all_losers = df[pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) < -3.0]
        if len(all_winners) >= 5 and len(all_losers) >= 5:
            aw_rs = pd.to_numeric(all_winners.get("dyn_rs", pd.Series(dtype=float)), errors="coerce").dropna()
            aw_eng = pd.to_numeric(all_winners.get("v_energy", pd.Series(dtype=float)), errors="coerce").dropna()
            as_cpv = pd.to_numeric(all_sideways.get("dyn_cpv", pd.Series(dtype=float)), errors="coerce").dropna()
            al_cpv = pd.to_numeric(all_losers.get("dyn_cpv", pd.Series(dtype=float)), errors="coerce").dropna()
            al_tb = pd.to_numeric(all_losers.get("dyn_tb", pd.Series(dtype=float)), errors="coerce").dropna()
            if not aw_rs.empty and not aw_eng.empty:
                report_msg += f"✅ [전체 대박주 {len(all_winners)}개 절대 공통점]\n"
                report_msg += f" ↳ 평균 RS: 상위 {(10-aw_rs.mean())*11.1:.1f}% | 평균 에너지: {aw_eng.mean():.1f}\n"
            if not as_cpv.empty:
                report_msg += f"↔️ [전체 횡보주 {len(all_sideways)}개 절대 공통점]\n"
                report_msg += f" ↳ 평균 캔들지배력(CPV): 상위 {(10-as_cpv.mean())*11.1:.1f}% (애매한 매도세가 횡보를 유발함)\n"
            if not al_cpv.empty and not al_tb.empty:
                report_msg += f"💀 [전체 참사주 {len(all_losers)}개 절대 공통점]\n"
                report_msg += f" ↳ 평균 캔들지배력(CPV): 상위 {(10-al_cpv.mean())*11.1:.1f}% | 찐양봉 빈도 하위 {(al_tb.mean())*11.1:.1f}%\n"
                report_msg += f"💡 <b>[관제탑 최종 결론]</b>\n"
                if aw_rs.mean() < al_cpv.mean():
                    report_msg += "현재 시장은 점수와 무관하게 철저히 '상대강도(RS)'가 주도하는 추세장입니다.\n"
                else:
                    report_msg += "현재 시장은 악성 윗꼬리(CPV)에 한 번 걸리면 무조건 계좌가 녹아내리는 변동성 장세입니다.\n"
        else:
            report_msg += "⚠️ 전체 그룹 통합 분석을 위한 표본이 아직 부족합니다.\n"
        report_msg += "\n"

        report_msg += "🏷️ [세부 흐름 태그별 승률 기여도]\n"
        tag_stats = {}
        for _, row in df.iterrows():
            if pd.isna(row.get("flow_tags")):
                continue
            for tag in str(row.get("flow_tags")).split():
                tag_stats.setdefault(tag, {"win": 0, "total": 0})
                tag_stats[tag]["total"] += 1
                if int(row.get("Win", 0)) == 1:
                    tag_stats[tag]["win"] += 1
        for tag, stats in sorted(tag_stats.items(), key=lambda x: x[1]["total"], reverse=True)[:5]:
            if stats["total"] >= 3:
                tag_win_rate = round((stats["win"] / stats["total"]) * 100, 1)
                report_msg += f" ▪️ {tag}: 승률 {tag_win_rate}% (출현 {stats['total']}회)\n"

        if "margin_used" in df.columns and "sim_kelly_invest" in df.columns:
            report_msg += "\n⚖️ <b>[V39.0 자금 관리 평행우주 대결 (누적 실현 손익)]</b>\n"
            df["fixed_profit"] = pd.to_numeric(df["margin_used"], errors="coerce").fillna(0.0) * (
                pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) / 100.0
            )
            total_fixed_profit = float(df["fixed_profit"].sum())
            df["kelly_profit"] = pd.to_numeric(df["sim_kelly_invest"], errors="coerce").fillna(0.0) * (
                pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) / 100.0
            )
            total_kelly_profit = float(df["kelly_profit"].sum())
            report_msg += f"▪️ 고정 2% 베팅 누적 손익: <b>{total_fixed_profit:,.2f}USDT</b>\n"
            report_msg += f"▪️ 국면형 켈리 누적 손익: <b>{total_kelly_profit:,.2f}USDT</b>\n"
            if total_kelly_profit > total_fixed_profit:
                report_msg += "🏆 <b>결론: 동적 켈리가 승리했습니다.</b> 상승장에서 비중을 싣고 하락장에서 방어한 전략이 누적 자본 증식에 훨씬 유리함을 데이터로 증명했습니다.\n"
            else:
                report_msg += "🛡️ <b>결론: 고정 리스크가 유리했습니다.</b> 켈리 베팅이 과도한 리스크를 지거나 휩소에 당했습니다. 켈리 승수를 하향 조정해야 합니다.\n"

        send_telegram_msg(report_msg)
        print(f"✅ [{market_type}] 딥 다이브 분석 리포트 발송 완료.")
    except Exception as e:
        err_msg = f"🚨 <b>[포워드 장부 에러]</b> 딥 다이브 분석 중 에러 발생:\n{e}"
        print(err_msg)
        send_telegram_msg(err_msg)


def send_comprehensive_daily_report():
    """[V104.1] 마켓별 9분할 정밀 리포트 (코인 MTF 버전)"""
    init_forward_db()
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    cfg = load_system_config()
    base_seed = float(cfg.get("ACCOUNT_SIZE_USDT", 100000.0))
    regime = cfg.get("CURRENT_REGIME_KEY", "UNKNOWN")
    kelly_risk = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01)) * 100.0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")

    for market_type in ["spot", "futures"]:
        m_icon = "🟢" if market_type == "spot" else "🟠"
        try:
            df_all = pd.read_sql(
                "SELECT * FROM bitget_forward_trades WHERE market_type=?",
                conn,
                params=(market_type,),
            )
            if df_all.empty:
                continue
            _sig_s = df_all["sig_type"].astype(str)
            _real_only = ~_sig_s.str.contains("INCUBATOR", na=False)
            df_real = df_all.loc[_real_only].copy()
            df_closed = df_real[df_real["status"].str.contains("CLOSED", na=False)].copy()
            df_open = df_real[df_real["status"] == "OPEN"].copy()

            treasury_key = "TREASURY_SPOT_USDT" if market_type == "spot" else "TREASURY_FUTURES_USDT"
            treasury = float(cfg.get(treasury_key, 0.0))

            msg1 = f"{m_icon} <b>[1/9] 거시 국면 및 국고(Treasury) 현황</b>\n"
            msg1 += f"📅 {today_str} | 국면: <b>{regime}</b>\n"
            msg1 += f"🏦 <b>{market_type.upper()} 국고 잔여금:</b> {treasury:,.2f} USDT\n"
            msg1 += f"⚖️ 동적 켈리 비중: {kelly_risk:.2f}%\n"
            msg1 += "<i>※ 아래 누적 손익·리더보드·순환·DNA 통계는 [INCUBATOR_] 섀도우 제외.</i>\n"
            send_telegram_msg(msg1)
            time.sleep(1)

            msg2 = f"{m_icon} <b>[2/9] 로직별 복리 생존 리더보드</b>\n"
            if not df_real.empty:
                df_c = df_real.copy()
                df_c["group"] = df_c["sig_type"].apply(_extract_core_group)
                leaderboard = []
                for group in df_c["group"].unique():
                    g_df = df_c[df_c["group"] == group]
                    g_closed = g_df[g_df["status"].str.contains("CLOSED", na=False)]
                    pnl = float(
                        (
                            pd.to_numeric(g_closed["sim_kelly_invest"], errors="coerce").fillna(0.0)
                            * pd.to_numeric(g_closed["final_ret"], errors="coerce").fillna(0.0)
                            / 100.0
                        ).sum()
                    )
                    wr = (len(g_closed[pd.to_numeric(g_closed["final_ret"], errors="coerce").fillna(0.0) > 0]) / len(g_closed) * 100.0) if len(g_closed) > 0 else 0.0
                    leaderboard.append({"g": group, "bal": base_seed + pnl, "wr": wr, "op": len(g_df[g_df["status"] == "OPEN"])})
                leaderboard = sorted(leaderboard, key=lambda x: x["bal"], reverse=True)
                for i, e in enumerate(leaderboard[:7]):
                    m = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "🏃"
                    if e["bal"] < base_seed * 0.8:
                        m = "📉"
                    if e["bal"] < base_seed * 0.5:
                        m = "💀"
                    msg2 += f"{m} <b>{e['g']}</b>: {e['bal']:,.2f}USDT (승률 {e['wr']:.0f}%)\n"
            else:
                msg2 += " ↳ 매매 데이터 없음\n"
            send_telegram_msg(msg2)
            time.sleep(1)

            kelly_pnl = float((pd.to_numeric(df_closed.get("sim_kelly_invest", 0.0), errors="coerce").fillna(0.0) * pd.to_numeric(df_closed.get("final_ret", 0.0), errors="coerce").fillna(0.0) / 100.0).sum()) if not df_closed.empty else 0.0
            fixed_pnl = float((pd.to_numeric(df_closed.get("margin_used", 0.0), errors="coerce").fillna(0.0) * pd.to_numeric(df_closed.get("final_ret", 0.0), errors="coerce").fillna(0.0) / 100.0).sum()) if not df_closed.empty else 0.0
            msg3 = f"{m_icon} <b>[3/9] 통합 자금 관리 진검승부</b> <i>(본계좌만)</i>\n"
            msg3 += f"💰 누적 켈리 수익: <b>{kelly_pnl:+,.2f} USDT</b>\n"
            msg3 += f"🛡️ 누적 고정 수익: {fixed_pnl:+,.2f} USDT\n"
            msg3 += f"💡 자금관리 우위: {'동적 켈리' if kelly_pnl > fixed_pnl else '고정 리스크 2%'}\n"
            send_telegram_msg(msg3)
            time.sleep(1)

            open_sigs = df_open["sig_type"].astype(str).tolist()
            trend_fleet = sum(1 for s in open_sigs if "S1" in s)
            recon_fleet = sum(1 for s in open_sigs if ("S4" in s or "S6" in s or "S7" in s))
            msg4 = f"{m_icon} <b>[4/9] 섹터 포트폴리오 다중화 현황</b>\n"
            msg4 += f"🎯 편대 현황: 주도주 폭격편대 {trend_fleet}기 | 차기섹터 정찰대 {recon_fleet}기\n"
            send_telegram_msg(msg4)
            time.sleep(1)

            msg5 = f"{m_icon} <b>[5/9] 티어 및 데스콤보 검증</b>\n"
            t1_df = df_closed[df_closed["tier"] == "80점대"] if not df_closed.empty else pd.DataFrame()
            if not t1_df.empty:
                msg5 += f"💎 1티어(80점↑) 승률: {(pd.to_numeric(t1_df['final_ret'], errors='coerce').fillna(0.0) > 0).mean()*100:.1f}%\n"
            if not df_closed.empty:
                msg5 += f"💀 데스콤보 승률: {(pd.to_numeric(df_closed['final_ret'], errors='coerce').fillna(0.0) > 0).mean()*100:.1f}% (필터 작동 중)\n"
            if t1_df.empty and df_closed.empty:
                msg5 += " ↳ 검증 표본 부족\n"
            send_telegram_msg(msg5)
            time.sleep(1)

            msg6 = f"{m_icon} <b>[6/9] 대박주/참사주 4차원 DNA 부검</b>\n"
            winners = df_closed[pd.to_numeric(df_closed.get("final_ret", 0.0), errors="coerce").fillna(0.0) >= 5.0].head(50)
            losers = df_closed[pd.to_numeric(df_closed.get("final_ret", 0.0), errors="coerce").fillna(0.0) <= -3.0].head(50)
            if not winners.empty:
                msg6 += f"✅ 대박 DNA: 윗꼬리 {pd.to_numeric(winners['dyn_cpv'], errors='coerce').fillna(0.0).mean():.2f} | 응축 {pd.to_numeric(winners['v_energy'], errors='coerce').fillna(0.0).mean():.1f}\n"
            if not losers.empty:
                msg6 += f"❌ 참사 DNA: 윗꼬리 {pd.to_numeric(losers['dyn_cpv'], errors='coerce').fillna(0.0).mean():.2f} | 찐양봉 {pd.to_numeric(losers['dyn_tb'], errors='coerce').fillna(0.0).mean():.1f} 미만\n"
            send_telegram_msg(msg6)
            time.sleep(1)

            msg7 = f"{m_icon} <b>[7/9] 섹터 순환매 궤적 및 스필오버</b>\n"
            rot_df = df_real[df_real["entry_date"] >= (datetime.utcnow() - timedelta(days=60)).strftime("%Y-%m-%d")]
            if not rot_df.empty:
                daily_dom = rot_df.groupby("entry_date")["timeframe"].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None).dropna()
                streaks, transitions = {}, {}
                current_sec, current_streak = None, 0
                for _, sec in daily_dom.items():
                    if sec == current_sec:
                        current_streak += 1
                    else:
                        if current_sec is not None:
                            streaks.setdefault(current_sec, []).append(current_streak)
                            t_key = f"{str(current_sec)[:6]}➔{str(sec)[:6]}"
                            transitions[t_key] = transitions.get(t_key, 0) + 1
                        current_sec = sec
                        current_streak = 1
                if current_sec is not None:
                    streaks.setdefault(current_sec, []).append(current_streak)
                msg7 += f"🔥 <b>현재 주도 섹터:</b> {current_sec} ({current_streak}일째 체류 중)\n"
                msg7 += f"🔮 <b>다음 예측 섹터:</b> {cfg.get('PREDICTED_NEXT_SECTOR', '분석중')}\n"
                msg7 += f"⚡ <b>베팅 어드밴티지:</b> {'🔥활성화(200%)' if cfg.get('ROTATION_ADVANTAGE_ACTIVE') else '정상(100%)'}\n\n"
                msg7 += "▪️ <b>섹터별 자금 체류 시간(수명):</b>\n"
                for s, lengths in streaks.items():
                    msg7 += f" - {str(s)[:6]}: 평균 {sum(lengths)/len(lengths):.1f}일\n"
                sorted_trans = sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:2]
                if sorted_trans:
                    msg7 += "\n▪️ <b>빈번한 자금 이동 궤적:</b>\n"
                    for p, c in sorted_trans:
                        msg7 += f" - {p} ({c}회 관측)\n"
            else:
                msg7 += " ↳ 순환매 데이터 부족\n"
            msg7 += "\n🌐 <b>코인 스필오버:</b> BTC 우세/알트 확산 시차 전이 추적 중\n"
            send_telegram_msg(msg7)
            time.sleep(1)

            cos_limit = float(cfg.get("DYNAMIC_ALPHA_LIMIT", 0.75))
            ml_limit = float(cfg.get("DYNAMIC_ML_BOX_CUTOFF", 0.50))
            promo_str = str(cfg.get("LIVE_A_PROMOTION_DATE", today_str))
            try:
                days_alive = (datetime.utcnow() - datetime.strptime(promo_str, "%Y-%m-%d")).days
            except Exception:
                days_alive = 0
            msg8 = f"{m_icon} <b>[8/9] 메타 최적화 및 알파 반감기</b>\n"
            msg8 += f"🦅 커트라인 방어막: 코사인 {cos_limit*100:.0f}% | ML박스 {ml_limit*100:.0f}%\n"
            msg8 += f"⏳ 오토파일럿 수명: <b>{days_alive}일차</b>\n"
            recent_dna = df_real.sort_values("id", ascending=False).head(10)
            if not recent_dna.empty and "entry_breadth" in recent_dna.columns:
                mean_b = pd.to_numeric(recent_dna["entry_breadth"], errors="coerce").dropna().mean()
                if pd.notna(mean_b) and mean_b < 0.98:
                    msg8 += "🚨 <b>[DNA 변위 감지]</b> 대장주 일치율 급감 ➔ 방어 개입 중\n"
            send_telegram_msg(msg8)
            time.sleep(1)

            std_df = df_closed[df_closed["sig_type"].astype(str).str.contains("STANDARD", na=False)]
            sn_df = df_closed[df_closed["sig_type"].astype(str).str.contains("SUPERNOVA", na=False)]
            std_ret = float(pd.to_numeric(std_df.get("live_a_ret", 0.0), errors="coerce").fillna(0.0).mean()) if not std_df.empty else 0.0
            sn_ret = float(pd.to_numeric(sn_df.get("cand_b_ret", 0.0), errors="coerce").fillna(0.0).mean()) if not sn_df.empty else 0.0
            msg9 = f"{m_icon} <b>[9/9] 시스템 데스매치 결산</b>\n"
            msg9 += f"⚔️ 오리지널(A) 평균 성적: {std_ret:+.2f}%\n"
            msg9 += f"⚔️ 초신성(B) 평균 성적: {sn_ret:+.2f}%\n"
            msg9 += f"💡 결론: {'초신성 우위 (시스템 진화 중)' if sn_ret > std_ret else '오리지널 방어 성공'}\n"
            send_telegram_msg(msg9)
            time.sleep(1)
        except Exception as e:
            send_telegram_msg(f"⚠️ {market_type} 리포트 에러: {e}")
    conn.close()


if __name__ == "__main__":
    init_forward_db()
