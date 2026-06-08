"""
DEPRECATED — use `bitget.pipelines.bitget_auto_pilot` or `bitget/auto_pilot.py`.

Legacy duplicate of auto_pilot evolution logic. Kept for import compatibility only.
"""
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests

from bitget.forward_tester import init_forward_db, send_comprehensive_daily_report, send_telegram_msg
from bitget.infra.data_paths import market_data_db_path, system_config_json_path

DB_PATH = market_data_db_path()
CONFIG_PATH = system_config_json_path()
TIMEFRAMES = ["1D", "4H", "2H", "1H"]


def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        cfg = {}
    if "META_GOVERNOR_WINDOWS" not in cfg or not isinstance(cfg.get("META_GOVERNOR_WINDOWS"), dict):
        cfg["META_GOVERNOR_WINDOWS"] = {
            "calibrator_lookback_days": 90,
            "treasury_lookback_days": 90,
            "graveyard_rolling_days": 90,
            "dist_lookback_days": 90,
        }
    if "META_GOVERNOR_SKIP_VIX" not in cfg:
        cfg["META_GOVERNOR_SKIP_VIX"] = False
    return cfg


def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)


def _load_btc_1d(conn):
    for tbl in ("BITGET_FUT_BTC_USDT_1D", "BITGET_SPOT_BTC_USDT_1D"):
        try:
            df = pd.read_sql(f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date ASC', conn)
            if len(df) >= 220:
                return df
        except Exception:
            pass
    return None


def _load_eth_1d(conn):
    for tbl in ("BITGET_FUT_ETH_USDT_1D", "BITGET_SPOT_ETH_USDT_1D"):
        try:
            df = pd.read_sql(f'SELECT Date, Open, High, Low, Close, Volume FROM "{tbl}" ORDER BY Date ASC', conn)
            if len(df) >= 220:
                return df
        except Exception:
            pass
    return None


def _fetch_total2_btc_ratio_api():
    # TOTAL2/BTC 근사치: (전체 시총 - BTC 시총) / BTC 시총
    try:
        res = requests.get("https://api.coingecko.com/api/v3/global", timeout=10)
        data = res.json().get("data", {})
        total_usd = float(data.get("total_market_cap", {}).get("usd", 0.0))
        btc_pct = float(data.get("market_cap_percentage", {}).get("btc", 0.0))
        btc_cap = total_usd * (btc_pct / 100.0)
        total2 = total_usd - btc_cap
        if btc_cap > 0:
            return float(total2 / btc_cap)
    except Exception:
        pass
    return 1.0


def _calc_market_breadth(conn):
    """
    Breadth 복원:
    1) ETH/BTC 상대강도(현재 비율 / 50일 평균)
    2) TOTAL2/BTC(API) 보조
    """
    try:
        btc = _load_btc_1d(conn)
        eth = _load_eth_1d(conn)
        if btc is not None and eth is not None:
            b = btc[["Date", "Close"]].copy()
            e = eth[["Date", "Close"]].copy()
            b["Date"] = pd.to_datetime(b["Date"])
            e["Date"] = pd.to_datetime(e["Date"])
            m = b.merge(e, on="Date", how="inner", suffixes=("_btc", "_eth"))
            if len(m) >= 60:
                ratio = m["Close_eth"].astype(float) / m["Close_btc"].astype(float)
                rel = float(ratio.iloc[-1] / max(ratio.rolling(50).mean().iloc[-1], 1e-12))
                total2_btc = _fetch_total2_btc_ratio_api()
                return rel, total2_btc
    except Exception:
        pass
    total2_btc = _fetch_total2_btc_ratio_api()
    return float(total2_btc), float(total2_btc)


def detect_regime(cfg):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    btc = _load_btc_1d(conn)
    if btc is None or btc.empty:
        conn.close()
        cfg["CURRENT_REGIME_KEY"] = "CHOP"
        return cfg

    c = btc["Close"].astype(float)
    h = btc["High"].astype(float)
    l = btc["Low"].astype(float)
    ema200 = c.ewm(span=200, adjust=False).mean().iloc[-1]
    last = c.iloc[-1]
    prev_c = c.shift(1)
    tr = np.maximum(h - l, np.maximum((h - prev_c).abs(), (l - prev_c).abs()))
    atr14 = tr.ewm(span=14, adjust=False).mean().iloc[-1]
    atr_pct = float((atr14 / last) * 100.0) if last > 0 else 0.0
    breadth, total2_btc = _calc_market_breadth(conn)
    conn.close()

    # 1차: BTC 추세/변동성
    if last > ema200 and atr_pct < 3.5:
        regime = "BULL"
        base_w1, base_w4 = 1.2, 0.8
    elif last < ema200 and atr_pct >= 4.5:
        regime = "BEAR"
        base_w1, base_w4 = 0.5, 1.5
    else:
        regime = "CHOP"
        base_w1, base_w4 = 0.8, 1.2

    # 2차: 시장 폭 기반 알트 확산/쏠림
    if breadth < 0.97:
        breadth_status = "NARROW"
        base_w1 *= 0.5
        base_w4 *= 1.2
    elif breadth > 1.03:
        breadth_status = "BROAD"
        base_w1 *= 1.2
    else:
        breadth_status = "NEUTRAL"

    # 극단 리스크 구간 추가 방어
    if last < ema200 and atr_pct >= 6.0:
        regime = "BEAR"
        base_w1, base_w4 = 0.0, 2.0
        breadth_status = "RISK_OFF"

    cfg["CURRENT_REGIME_KEY"] = regime
    cfg["BTC_EMA200"] = float(ema200)
    cfg["BTC_ATR_PCT"] = float(atr_pct)
    cfg["CRYPTO_BREADTH_TOTAL2_BTC"] = float(total2_btc)
    cfg["CRYPTO_BREADTH_ETH_BTC_REL"] = float(breadth)
    cfg["CRYPTO_TOTAL2_BTC_RATIO"] = float(total2_btc)
    cfg["CRYPTO_BREADTH_STATUS"] = breadth_status
    try:
        from meta_governor_consumer import apply_meta_weight_bounds_clamp, load_meta_state_resolved

        base_w1, base_w4 = apply_meta_weight_bounds_clamp(float(base_w1), float(base_w4), load_meta_state_resolved())
    except Exception:
        pass
    cfg["WEIGHT_S1"] = round(float(max(0.0, min(2.5, base_w1))), 4)
    cfg["WEIGHT_S4"] = round(float(max(0.0, min(2.5, base_w4))), 4)

    base = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01))
    if regime == "BULL":
        cfg["DYNAMIC_KELLY_RISK"] = round(min(0.03, max(0.002, base * 1.15)), 4)
    elif regime == "BEAR":
        cfg["DYNAMIC_KELLY_RISK"] = round(max(0.002, base * 0.70), 4)
    else:
        cfg["DYNAMIC_KELLY_RISK"] = round(max(0.002, base * 0.90), 4)
    return cfg


def _pf(ret_s: pd.Series):
    if ret_s is None or ret_s.empty:
        return 0.0
    wins = ret_s[ret_s > 0].sum()
    losses = abs(ret_s[ret_s <= 0].sum()) + 0.1
    return float(wins / losses)


def _simulate_universe(df, params):
    sl = float(params.get("DYNAMIC_MAE_SL", -3.5))
    tp = float(params.get("DYNAMIC_MFE_TP", 10.0))
    vals = []
    for _, row in df.iterrows():
        ret = float(row.get("final_ret", 0.0) or 0.0)
        mfe = float(row.get("mfe", ret) or ret)
        if ret <= sl:
            vals.append(sl)
        elif mfe >= tp:
            vals.append(tp)
        else:
            vals.append(ret)
    return pd.Series(vals, dtype=float)


def _smooth_live_params(live, recent_df):
    out = dict(live)
    wins = recent_df[recent_df["final_ret"] > 0]
    losses = recent_df[recent_df["final_ret"] <= 0]

    if not wins.empty:
        win_source = wins["mfe"] if "mfe" in wins.columns else wins["final_ret"]
        new_tp = float(np.percentile(win_source, 50))
        old_tp = float(out.get("DYNAMIC_MFE_TP", 10.0))
        out["DYNAMIC_MFE_TP"] = round((old_tp * 0.7) + (new_tp * 0.3), 2)
    if not losses.empty:
        new_sl = float(np.percentile(losses["final_ret"], 25))
        old_sl = float(out.get("DYNAMIC_MAE_SL", -3.5))
        out["DYNAMIC_MAE_SL"] = round((old_sl * 0.7) + (new_sl * 0.3), 2)
    return out


def run_tf_brain_surgery(cfg):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    df = pd.read_sql("SELECT * FROM forward_trades WHERE status LIKE 'CLOSED%'", conn)
    conn.close()
    if df.empty or "sector" not in df.columns:
        return cfg

    for tf in TIMEFRAMES:
        tdf = df[df["sector"].astype(str).str.upper() == tf].copy()
        if len(tdf) < 10:
            continue

        live_key = f"{tf}_LIVE_PARAMS"
        cand_key = f"{tf}_CANDIDATE_PARAMS"
        champ_key = f"{tf}_CHAMPION_PARAMS"
        mode_key = f"{tf}_ACTIVE_EXIT_MODE"

        live = cfg.get(live_key, {"DYNAMIC_MAE_SL": -3.5, "DYNAMIC_MFE_TP": 10.0})
        cand = cfg.get(cand_key, {"DYNAMIC_MAE_SL": -3.0, "DYNAMIC_MFE_TP": 11.0})
        champ = cfg.get(champ_key, {"DYNAMIC_MAE_SL": -2.8, "DYNAMIC_MFE_TP": 12.0})

        recent = tdf.sort_values("entry_date").tail(30)
        live = _smooth_live_params(live, recent)

        live_pf = _pf(_simulate_universe(tdf, live))
        cand_pf = _pf(_simulate_universe(tdf, cand))
        champ_pf = _pf(_simulate_universe(tdf, champ))

        if cand_pf > live_pf * 1.05:
            cfg[champ_key] = live
            cfg[live_key] = cand
            cfg[mode_key] = "CAND"
            cfg[f"{tf}_LAST_PROMOTION"] = "CAND_TO_LIVE"
        elif champ_pf > live_pf * 1.05:
            cfg[live_key] = champ
            cfg[mode_key] = "CHAMP"
            cfg[f"{tf}_LAST_PROMOTION"] = "CHAMP_RETURN"
        else:
            cfg[live_key] = live
            cfg[mode_key] = "LIVE"
            cfg[f"{tf}_LAST_PROMOTION"] = "HOLD"

        ordered = tdf.sort_values("entry_date")
        if len(ordered) >= 12:
            half = len(ordered) // 2
            early_pf = _pf(ordered.iloc[:half]["final_ret"])
            late_pf = _pf(ordered.iloc[half:]["final_ret"])
            base_k = float(cfg.get(f"{tf}_DYNAMIC_KELLY_RISK", cfg.get("DYNAMIC_KELLY_RISK", 0.01)))
            if early_pf > 0 and (late_pf < early_pf * 0.7 or late_pf < 1.0):
                ratio = max(0.2, min(1.0, late_pf / max(early_pf, 1e-9)))
                new_k = round(max(0.002, base_k * ratio), 4)
                cfg[f"{tf}_DYNAMIC_KELLY_RISK"] = new_k
                cfg[f"{tf}_ALPHA_DECAY"] = {
                    "early_pf": round(early_pf, 3),
                    "late_pf": round(late_pf, 3),
                    "ratio": round(ratio, 3),
                    "kelly_after": new_k,
                }

        std_df = tdf[tdf["sig_type"].astype(str).str.contains("STANDARD", na=False)]
        sn_df = tdf[tdf["sig_type"].astype(str).str.contains("SUPERNOVA", na=False)]
        cfg[f"{tf}_DEATHMATCH"] = {
            "STANDARD_PF": round(_pf(std_df["final_ret"]) if not std_df.empty else 0.0, 4),
            "SUPERNOVA_PF": round(_pf(sn_df["final_ret"]) if not sn_df.empty else 0.0, 4),
            "winner": "SUPERNOVA" if _pf(sn_df["final_ret"]) > _pf(std_df["final_ret"]) else "STANDARD",
            "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        }
    return cfg


def run_autonomous_analysis():
    init_forward_db()  # forward_trades 호환 뷰 보장
    cfg = load_config()
    cfg = detect_regime(cfg)
    cfg = run_tf_brain_surgery(cfg)
    # 국면별 켈리 비중 조율(추가 바운딩)
    regime = cfg.get("CURRENT_REGIME_KEY", "CHOP")
    k = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01))
    if regime == "BULL":
        k = min(0.03, max(0.004, k))
    elif regime == "BEAR":
        k = max(0.002, min(0.012, k))
    else:
        k = max(0.002, min(0.018, k))
    cfg["DYNAMIC_KELLY_RISK"] = round(k, 4)
    cfg["AUTO_PILOT_UPDATED_AT"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    save_config(cfg)
    print("Bitget autonomous analysis complete.")


def send_weekly_flow_master_report():
    """일주일간의 하루하루 자금 흐름, 승률 변화, 섹터 이동 궤적을 총결산하는 마스터 결과지"""
    now = datetime.utcnow()
    week_ago = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    today_str = now.strftime("%Y-%m-%d")
    cfg = load_config()
    regime = cfg.get("CURRENT_REGIME_KEY", "UNKNOWN")
    try:
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        report_msg = f"🗺️ <b>[V100.0 퀀트 팩토리 주간 흐름(Flow) 총결산]</b>\n📅 기간: {week_ago} ~ {today_str}\n"
        report_msg += "<i>※ 일자별 실현·MVP·섹터 궤적: sig_type에 INCUBATOR 포함 건 제외(본계좌만).</i>\n"
        report_msg += "━━━━━━━━━━━━━━━━━━\n"

        for market_type in ["spot", "futures"]:
            icon = "🟢" if market_type == "spot" else "🟠"
            report_msg += f"\n{icon} <b>[{market_type.upper()} 일주일 자금 및 섹터 흐름 궤적]</b>\n"
            report_msg += "🗓️ <b>[일자별 실현 손익 및 승률 타임라인]</b>\n"
            cursor = conn.execute(
                """
                SELECT exit_date,
                       SUM((sim_kelly_invest * final_ret) / 100.0) as daily_pnl,
                       SUM(CASE WHEN final_ret > 0 THEN 1 ELSE 0 END) as wins,
                       COUNT(*) as total
                FROM bitget_forward_trades
                WHERE market_type=? AND exit_date >= ? AND status LIKE 'CLOSED%'
                  AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
                GROUP BY exit_date ORDER BY exit_date ASC
                """,
                (market_type, week_ago),
            )
            daily_stats = cursor.fetchall()
            weekly_pnl = 0.0
            if daily_stats:
                for e_date, d_pnl, wins, total in daily_stats:
                    d_pnl = float(d_pnl or 0.0)
                    d_wr = (float(wins) / float(total) * 100.0) if total else 0.0
                    weekly_pnl += d_pnl
                    short_date = str(e_date)[5:] if e_date else "--"
                    emo = "🔴" if d_pnl < 0 else "🟢"
                    report_msg += f" {emo} {short_date}: <b>{d_pnl:+,.2f}USDT</b> (승률 {d_wr:.0f}% / {total}건 청산)\n"
                report_msg += f" 💰 <b>주간 누적 실현 손익: {weekly_pnl:+,.2f} USDT</b>\n"
            else:
                report_msg += " ↳ 이번 주 청산 데이터가 없습니다.\n"

            report_msg += "\n🔄 <b>[주간 주도 섹터 진화 궤적]</b>\n"
            rot_df = pd.read_sql(
                "SELECT entry_date, timeframe FROM bitget_forward_trades WHERE market_type=? AND entry_date >= ? "
                "AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%' ORDER BY entry_date ASC",
                conn,
                params=(market_type, week_ago),
            )
            if not rot_df.empty:
                daily_dom = rot_df.groupby("entry_date")["timeframe"].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None).dropna()
                flow_path = [f"{str(s)[:4]}({str(d)[5:]})" for d, s in daily_dom.items()]
                report_msg += f" 🌊 <b>흐름:</b> {' ➔ '.join(flow_path)}\n"
            else:
                report_msg += " ↳ 섹터 편입 데이터가 없습니다.\n"

            report_msg += "\n🏆 <b>[이번 주 MVP 시그널 엔진]</b>\n"
            top = conn.execute(
                """
                SELECT sig_type, SUM((sim_kelly_invest * final_ret) / 100.0) as profit, COUNT(*)
                FROM bitget_forward_trades
                WHERE market_type=? AND exit_date >= ? AND status LIKE 'CLOSED%'
                  AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
                GROUP BY sig_type ORDER BY profit DESC LIMIT 3
                """,
                (market_type, week_ago),
            ).fetchall()
            if top:
                for i, (sig, pnl, cnt) in enumerate(top):
                    clean_sig = str(sig).split("]")[0] + "]" if "]" in str(sig) else str(sig)[:15]
                    medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"
                    report_msg += f" {medal} {clean_sig}: <b>{float(pnl or 0.0):+,.2f}USDT</b> 기여 ({cnt}건)\n"
            else:
                report_msg += " ↳ MVP 데이터가 없습니다.\n"

        report_msg += "\n⚙️ <b>[주말 관제탑 자율 튜닝 결과 요약]</b>\n"
        report_msg += f" ▪️ <b>현재 국면:</b> {regime}\n"
        report_msg += f" ▪️ <b>동적 켈리 비중:</b> {float(cfg.get('DYNAMIC_KELLY_RISK', 0.01))*100:.1f}%\n"
        report_msg += f" ▪️ <b>초신성 허들:</b> 코사인 {float(cfg.get('DYNAMIC_ALPHA_LIMIT', 0.75))*100:.0f}% | ML박스 {float(cfg.get('DYNAMIC_ML_BOX_CUTOFF', 0.50))*100:.0f}%\n"
        report_msg += f" ▪️ <b>로직 수명:</b> 최초 작동일로부터 {(now - datetime.strptime(str(cfg.get('LIVE_A_PROMOTION_DATE', today_str)), '%Y-%m-%d')).days}일차 유지 중\n"
        conn.close()
    except Exception as e:
        report_msg = f"⚠️ 주간 리포트 생성 중 에러: {e}"
    report_msg += "\n━━━━━━━━━━━━━━━━━━\n💡 <i>시스템이 일주일간 시장의 궤적을 어떻게 흡수하고 진화했는지 증명하는 마스터 결과지입니다.</i>"
    send_telegram_msg(report_msg)


def system_main_loop():
    print("🕒 [Bitget 관제탑] 24/7 뇌수술 루프 가동")
    print(" - 6시간마다: 국면 판독 + TF별 챔피언 승격 + 알파 반감기")
    print(" - 매일 00:05 UTC: 9분할 결과지")
    print(" - 매주 월요일 00:05 UTC: 주간 마스터 리포트")
    while True:
        try:
            now = datetime.utcnow()
            if now.hour % 6 == 0 and now.minute == 0:
                run_autonomous_analysis()
                time.sleep(60)
            if now.hour == 0 and now.minute == 5:
                send_comprehensive_daily_report()
                if now.weekday() == 0:
                    send_weekly_flow_master_report()
                time.sleep(60)
            time.sleep(30)
        except Exception as e:
            print(f"auto pilot loop error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    run_autonomous_analysis()
