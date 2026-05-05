import os
import queue
import sqlite3
import threading
import time
import gc
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from bitget_ai_report import generate_ai_report
from bitget_charting import save_chart
from bitget_forward_tester import try_add_virtual_position
from bitget_signal_engines import compute_ema5_signal, compute_master_signal, compute_nulrim_signal


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
TIMEFRAMES = ["1D", "4H", "2H", "1H"]
BENCHMARK = "BTC_USDT"
TELEGRAM_TOKEN_MAIN = os.environ.get("BITGET_TELEGRAM_TOKEN_MAIN", "")
TELEGRAM_TOKEN_PROMO = os.environ.get("BITGET_TELEGRAM_TOKEN_PROMO", "")
TELEGRAM_CHAT_ID = os.environ.get("BITGET_TELEGRAM_CHAT_ID", "")
SEND_TELEGRAM = bool(TELEGRAM_TOKEN_MAIN and TELEGRAM_CHAT_ID)
q_main = queue.Queue()
q_promo = queue.Queue()
sent_today = set()
last_run_date = ""
LOG_FILE = os.path.join(BASE_DIR, "sent_log_bitget_master.txt")
MAX_SCAN_WORKERS = 4


def telegram_sender_daemon(target_queue, token):
    while True:
        item = target_queue.get()
        if item is None:
            break
        img_path, caption = item
        safe_caption = caption[:1000] + "\n...(글자수 제한으로 요약됨)" if len(caption) > 1000 else caption
        if SEND_TELEGRAM and token:
            for _ in range(3):
                try:
                    if img_path and os.path.exists(img_path):
                        with open(img_path, "rb") as f:
                            res = requests.post(
                                f"https://api.telegram.org/bot{token}/sendPhoto",
                                data={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption, "parse_mode": "HTML"},
                                files={"photo": f},
                                timeout=60,
                            )
                    else:
                        res = requests.post(
                            f"https://api.telegram.org/bot{token}/sendMessage",
                            json={"chat_id": TELEGRAM_CHAT_ID, "text": safe_caption, "parse_mode": "HTML"},
                            timeout=60,
                        )
                    if res.status_code == 200:
                        break
                    if res.status_code == 429:
                        time.sleep(3)
                except Exception:
                    time.sleep(2)
            time.sleep(1.2)
        target_queue.task_done()


threading.Thread(target=telegram_sender_daemon, args=(q_main, TELEGRAM_TOKEN_MAIN), daemon=True).start()
threading.Thread(target=telegram_sender_daemon, args=(q_promo, TELEGRAM_TOKEN_PROMO or TELEGRAM_TOKEN_MAIN), daemon=True).start()


def _load_table(conn, table_name):
    df = pd.read_sql(f'SELECT Date, Open, High, Low, Close, Volume FROM "{table_name}"', conn)
    if df.empty:
        return None
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.set_index("Date").sort_index()
    return df


def _benchmark_series(conn, timeframe: str):
    fut_tbl = f"BITGET_FUT_{BENCHMARK}_{timeframe}"
    spot_tbl = f"BITGET_SPOT_{BENCHMARK}_{timeframe}"
    for tbl in (fut_tbl, spot_tbl):
        try:
            df = _load_table(conn, tbl)
            if df is not None and not df.empty:
                return df["Close"]
        except Exception:
            continue
    return pd.Series(dtype=float)


def _scan_one_table(tbl: str, tf: str, idx_close: pd.Series, hit_rank_start: int):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        time.sleep(0.03)  # DB burst 완화
        df = _load_table(conn, tbl)
        if df is None or len(df) < 500:
            return []
        symbol = "_".join(tbl.split("_")[2:-1])
        hits = []
        rank = hit_rank_start
        for engine_name, engine in (
            ("EMA5", compute_ema5_signal),
            ("MASTER", compute_master_signal),
            ("NULRIM", compute_nulrim_signal),
        ):
            hit, sig_type, out_df, dbg = engine(df, idx_close, tf)
            if hit:
                rank += 1
                chart_main = save_chart(out_df, f"{symbol}_{tf}_{engine_name}", rank, show_volume=True, is_promo=False)
                chart_promo = save_chart(out_df, f"{symbol}_{tf}_{engine_name}", rank, show_volume=False, is_promo=True)
                time.sleep(0.05)  # 외부 AI 호출/차트 I/O 사이 간격
                ai = generate_ai_report(symbol, tf)
                last_close = float(out_df["Close"].iloc[-1]) if out_df is not None and not out_df.empty else 0.0
                hits.append((engine_name, sig_type, float(dbg.get("score", 0.0)), chart_main, chart_promo, ai, dbg, last_close, rank))
                del out_df
                gc.collect()
        del df
        gc.collect()
        return hits
    except Exception as e:
        print(f"scan error {tbl}: {e}")
        return []
    finally:
        conn.close()


def run_scan():
    global sent_today, last_run_date
    if not os.path.exists(DB_PATH):
        print("DB not found. Run bitget_mtf_data_updater.py first.")
        return
    today_str = datetime_now_utc_date()
    if today_str != last_run_date:
        sent_today.clear()
        last_run_date = today_str
        if os.path.exists(LOG_FILE):
            try:
                with open(LOG_FILE, "r", encoding="utf-8") as f:
                    lines = f.read().splitlines()
                    if lines and lines[0] == today_str:
                        sent_today = set(lines[1:])
            except Exception:
                pass

    conn = sqlite3.connect(DB_PATH, timeout=30)
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = [r[0] for r in rows]
    hit_rank = 0

    for tf in TIMEFRAMES:
        idx_close = _benchmark_series(conn, tf)
        tf_tables = [t for t in table_names if t.startswith("BITGET_") and t.endswith(f"_{tf}") and BENCHMARK not in t]
        with ThreadPoolExecutor(max_workers=min(MAX_SCAN_WORKERS, max(1, len(tf_tables)))) as pool:
            futures = {pool.submit(_scan_one_table, tbl, tf, idx_close, hit_rank): tbl for tbl in tf_tables}
            for fut in as_completed(futures):
                tbl = futures[fut]
                hits = fut.result()
                symbol = "_".join(tbl.split("_")[2:-1])
                for engine_name, sig_type, score, chart_main, chart_promo, ai, dbg, last_close, rank in hits:
                    hit_rank = max(hit_rank, int(rank))
                    unique_key = f"{symbol}:{tf}:{engine_name}"
                    if unique_key in sent_today:
                        continue
                    sent_today.add(unique_key)
                    try:
                        with open(LOG_FILE, "w", encoding="utf-8") as f:
                            f.write(today_str + "\n")
                            for k in sorted(sent_today):
                                f.write(k + "\n")
                    except Exception:
                        pass

                    market_type = "futures" if "_FUT_" in tbl else "spot"
                    facts = {
                        "v_cpv": dbg.get("v_cpv", 0.0),
                        "v_yang": dbg.get("v_yang", 0.0),
                        "v_energy": dbg.get("v_energy", 0.0),
                        "v_rs": dbg.get("v_rs", 0.0),
                        "dyn_rs": dbg.get("dyn_rs_score", 0.0),
                        "dyn_cpv": dbg.get("dyn_cpv_score", 0.0),
                        "dyn_tb": dbg.get("dyn_tb_score", 0.0),
                    }
                    time.sleep(0.04)  # DB/API 연속 호출 완화
                    ok, db_msg = try_add_virtual_position(
                        market_type=market_type,
                        symbol=symbol,
                        timeframe=tf,
                        sig_type=f"[STANDARD][{engine_name}] {sig_type}",
                        score=score,
                        entry_price=float(dbg.get("last_close", last_close)),
                        facts=facts,
                    )

                    main_caption = (
                        f"🎯 [{sig_type}]\n"
                        f"🪙 {symbol} | TF {tf} | 엔진 {engine_name}\n"
                        f"📈 점수: {score:.1f}\n\n"
                        f"{dbg.get('v11_comment', '')}\n"
                        f"📒 장부 기록: {db_msg}\n\n"
                        f"💡 [AI 코인 브리핑]\n{ai}"
                    )
                    promo_caption = (
                        f"📈 [Bitget Signal]\n\n"
                        f"🪙 {symbol}\n"
                        f"🧭 TF: {tf} | {engine_name}\n"
                        f"⭐ 점수: {score:.1f}"
                    )
                    q_main.put((chart_main, main_caption))
                    q_promo.put((chart_promo, promo_caption))
                    print(f"[{engine_name}] {symbol} {tf} -> {db_msg} | charts queued")
                    del dbg
                    gc.collect()
                del hits
                gc.collect()
    conn.close()
    del table_names
    gc.collect()
    q_main.join()
    q_promo.join()


def datetime_now_utc_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _is_candle_close_time(now_utc: datetime, tf: str) -> bool:
    h = now_utc.hour
    m = now_utc.minute
    if tf == "1H":
        return m == 0
    if tf == "2H":
        return m == 0 and h % 2 == 0
    if tf == "4H":
        return m == 0 and h % 4 == 0
    if tf == "1D":
        return m == 0 and h == 0
    return False


def run_mtf_scheduler():
    print("🕒 [Bitget MTF 스캐너] UTC 캔들 마감 스케줄 대기 중...")
    print(" - 1H: 매시 정각")
    print(" - 2H: 짝수시 정각")
    print(" - 4H: 0/4/8/12/16/20시 정각")
    print(" - 1D: 00:00 UTC")
    last_trigger_key = ""
    while True:
        try:
            now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            due_tfs = [tf for tf in TIMEFRAMES if _is_candle_close_time(now_utc, tf)]
            if due_tfs:
                trigger_key = f"{now_utc.isoformat()}|{','.join(due_tfs)}"
                if trigger_key != last_trigger_key:
                    print(f"🚀 [스캔 시작] {now_utc.strftime('%Y-%m-%d %H:%M')} UTC | TF: {', '.join(due_tfs)}")
                    run_scan()
                    last_trigger_key = trigger_key
                    time.sleep(60)
            time.sleep(10)
        except Exception as e:
            print(f"scheduler error: {e}")
            time.sleep(30)


if __name__ == "__main__":
    run_mtf_scheduler()
