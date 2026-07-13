from __future__ import annotations

import json
import os
import sqlite3
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

import numpy as np
import pandas as pd
import memory_bounds
import requests

from bitget.infra.gc_cycle import flush_gc
from bitget.infra.clock import utc_date_str, utc_hm_key
from bitget.infra.memory_policy import GC_AFTER_SCAN_TABLE, OHLCV_SIGNAL_BAR_LIMIT

import bitget.shadow_tracking as bitget_shadow_tracking
from bitget.ai_report import generate_ai_report
from bitget.charting import save_chart
from bitget.env import bitget_telegram_chat_id, bitget_telegram_token, bitget_telegram_token_promo
from bitget.executor import execute_real_order
from bitget.forward_tester import generate_mutant_strategies, log_real_execution, track_daily_positions, try_add_virtual_position
import bitget.signal_engines as bse
from bitget.signal_engines import (
    compute_ema5_signal,
    compute_master_signal,
    compute_nulrim_signal,
    compute_tv_short_v1,
    compute_tv_short_v2,
)

from bitget.infra.data_paths import logs_dir, market_data_db_path, market_db_read_path
from bitget.infra.logging_setup import get_logger, log_exception
from bitget.infra.shared_db_connector import get_connection

logger = get_logger("bitget.master_scanner")

DB_PATH = market_data_db_path()
DB_READ_PATH = market_db_read_path()
TIMEFRAMES = ["1D", "4H", "2H", "1H"]
BENCHMARK = "BTC_USDT"
TELEGRAM_TOKEN_MAIN = bitget_telegram_token()
TELEGRAM_TOKEN_PROMO = bitget_telegram_token_promo()
TELEGRAM_CHAT_ID = bitget_telegram_chat_id()
SEND_TELEGRAM = bool(TELEGRAM_TOKEN_MAIN and TELEGRAM_CHAT_ID)

# [코인/주식 텔레그램 방 격리] 루트 telegram_message_queue 는 import 시점에 큐 DB 경로를
# 주식 factory_data_dir()/message_queue.sqlite 로 고정한다. bitget 스캐너가 이대로
# enqueue 하면 메시지가 "주식 큐"에 쌓이고, 주식 dante-async 가 (메시지에 챗ID가 없으므로)
# 자기 바인딩 챗 = 주식방으로 발송해 코인 결과지가 주식방으로 샌다.
# → bitget 전용 큐(bitget_message_queue.sqlite)로 재바인딩한 뒤 사용한다
#   (bitget/async_telegram_daemon._patch_bitget_queue_paths 와 동일 규약).
import telegram_message_queue as _tmq
from bitget.infra.data_paths import bitget_data_dir as _bitget_data_dir
from bitget.infra.data_paths import message_queue_db_path as _bitget_message_queue_db_path

_tmq._BOT_DIR = _bitget_data_dir()
_tmq.MESSAGE_QUEUE_DB_PATH = _bitget_message_queue_db_path()
_tmq._schema_ready = False

from telegram_message_queue import (
    enqueue_telegram,
    start_telegram_queue_daemons,
    wait_telegram_queue_drained,
)

start_telegram_queue_daemons(
    TELEGRAM_TOKEN_MAIN,
    TELEGRAM_TOKEN_PROMO or TELEGRAM_TOKEN_MAIN,
    TELEGRAM_CHAT_ID,
    SEND_TELEGRAM,
)

sent_today = set()
last_run_date = ""
LOG_FILE = os.path.join(logs_dir(), "sent_log_bitget_master.txt")
MAX_SCAN_WORKERS = 4


_SCANNER_ENGINE_ALLOWLIST = {
    "nulrim": frozenset({"NULRIM"}),
    "dante": frozenset({"TV_SHORT_V1", "TV_SHORT_V2"}),
    "ema5": frozenset({"EMA5"}),
    "master": frozenset({"MASTER"}),
}


def _build_engine_pool(engine_filter: str | None = None):
    """
    Scanner engine SSOT — base engines + optional PRACT_01..30 pool.
    Full scan (empty filter) must resolve practitioner fns via bitget.signal_engines.
    """
    base_engines = [
        ("EMA5", compute_ema5_signal),
        ("MASTER", compute_master_signal),
        ("NULRIM", compute_nulrim_signal),
        ("TV_SHORT_V1", compute_tv_short_v1),
        ("TV_SHORT_V2", compute_tv_short_v2),
    ]
    ef = str(engine_filter or "").strip().lower()
    if ef in _SCANNER_ENGINE_ALLOWLIST:
        allowed = _SCANNER_ENGINE_ALLOWLIST[ef]
        base_engines = [(n, e) for n, e in base_engines if n in allowed]
    practitioner_engines = []
    if not ef:
        for i in range(1, 31):
            fn_name = f"compute_practitioner_{i:02d}"
            fn = getattr(bse, fn_name, None)
            if callable(fn):
                practitioner_engines.append((f"PRACT_{i:02d}", fn))
    return base_engines + practitioner_engines


def _lookup_virtual_trade_id(market_type: str, symbol: str, timeframe: str, sig_type: str, side: str):
    try:
        conn = get_connection(DB_PATH, read_only=True)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id
            FROM bitget_forward_trades
            WHERE market_type=? AND symbol=? AND timeframe=? AND sig_type=? AND position_side=? AND status='OPEN'
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(market_type).lower(), str(symbol), str(timeframe).upper(), str(sig_type), str(side).upper()),
        )
        row = cur.fetchone()
        conn.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def _lookup_virtual_trade_quantity(
    market_type: str, symbol: str, timeframe: str, sig_type: str, side: str
) -> float:
    """Paper Kelly quantity SSOT for live fill — ARCR/NAV already baked in."""
    try:
        conn = get_connection(DB_PATH, read_only=True)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT quantity
            FROM bitget_forward_trades
            WHERE market_type=? AND symbol=? AND timeframe=? AND sig_type=?
              AND position_side=? AND status='OPEN'
            ORDER BY id DESC
            LIMIT 1
            """,
            (
                str(market_type).lower(),
                str(symbol),
                str(timeframe).upper(),
                str(sig_type),
                str(side).upper(),
            ),
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return 0.0
        return float(row[0] or 0.0)
    except Exception:
        return 0.0


def _load_table(conn, table_name):
    tail = memory_bounds.ohlcv_limit_sql(bar_limit=OHLCV_SIGNAL_BAR_LIMIT)
    df = pd.read_sql(
        f'SELECT Date, Open, High, Low, Close, Volume FROM "{table_name}"{tail}',
        conn,
    )
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


def _load_system_config():
    from bitget.config_hub import load_config

    return load_config()


def _cosine_similarity(a, b):
    va = np.asarray(a, dtype=float).reshape(-1)
    vb = np.asarray(b, dtype=float).reshape(-1)
    d = int(min(va.size, vb.size))
    if d <= 0:
        return 0.0
    va = np.nan_to_num(va[:d], nan=0.0, posinf=0.0, neginf=0.0)
    vb = np.nan_to_num(vb[:d], nan=0.0, posinf=0.0, neginf=0.0)
    na = np.linalg.norm(va)
    nb = np.linalg.norm(vb)
    if na <= 1e-12 or nb <= 1e-12:
        return 0.0
    return float(np.dot(va, vb) / (na * nb))


def _calc_dtw(s, t):
    if s is None or t is None:
        return 999.0
    s = np.asarray(s, dtype=float)
    t = np.asarray(t, dtype=float)
    n, m = len(s), len(t)
    if n == 0 or m == 0:
        return 999.0
    dtw = np.full((n + 1, m + 1), np.inf)
    dtw[0, 0] = 0.0
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = abs(float(s[i - 1]) - float(t[j - 1]))
            dtw[i, j] = cost + min(dtw[i - 1, j], dtw[i, j - 1], dtw[i - 1, j - 1])
    return float(dtw[n, m])


def _supernova_hit(df: pd.DataFrame, symbol: str, tf: str):
    if df is None or len(df) < 120:
        return None
    cfg = _load_system_config()
    live_clusters = cfg.get("LIVE_CLUSTER_TEMPLATES", {})
    if not isinstance(live_clusters, dict):
        live_clusters = {}

    c = df["Close"].astype(float).values
    o = df["Open"].astype(float).values
    h = df["High"].astype(float).values
    l = df["Low"].astype(float).values
    v = df["Volume"].astype(float).values

    v_ma20 = pd.Series(v).rolling(20).mean().values
    cpv = float(np.where(h != l, (c - o) / (h - l), 0.5)[-1])
    vol_mult = float(v[-1] / v_ma20[-1]) if len(v_ma20) and v_ma20[-1] > 0 else 1.0
    tb = float(vol_mult / max(cpv, 0.01) if cpv > 0 else vol_mult / 0.01)
    bb_std = float(pd.Series(c).rolling(20).std().iloc[-1])
    bb_mid = float(pd.Series(c).rolling(20).mean().iloc[-1])
    bb_width = (4.0 * bb_std) / bb_mid if bb_mid > 0 else 0.01
    bbe = float((1.0 / bb_width) * vol_mult if bb_width > 0 else 0.0)
    rs = float(((c[-1] - c[max(0, len(c) - 20)]) / max(c[max(0, len(c) - 20)], 1e-9)) * 100.0)

    cur_vec = np.array([cpv, tb, bbe], dtype=float)
    c_norm = (c - np.min(c)) / (np.max(c) - np.min(c) + 1e-9)
    target_arr = c_norm[-200:] if len(c_norm) >= 200 else c_norm
    cur_shape = np.array([np.mean(x) for x in np.array_split(target_arr, 20)])

    # 1) ML BOX 매칭
    ml_cutoff = float(cfg.get("DYNAMIC_ML_BOX_CUTOFF", 0.50))
    for name, b in live_clusters.items():
        if not isinstance(b, dict):
            continue
        dims, hit = 0, 0
        for k in ("cpv", "tb", "bbe"):
            lo = b.get(f"{k}_min")
            hi = b.get(f"{k}_max")
            if lo is None or hi is None:
                continue
            dims += 1
            val = cpv if k == "cpv" else (tb if k == "tb" else bbe)
            if float(lo) <= float(val) <= float(hi):
                hit += 1
        if dims == 0:
            continue
        score = hit / float(dims)
        if score >= ml_cutoff:
            return {
                "engine_name": "SUPERNOVA_MLBOX",
                "sig_type": f"[SUPERNOVA_MLBOX] 🤖 {name}",
                "score": float(score * 100.0),
                "facts": {
                    "v_cpv": cpv,
                    "v_yang": tb,
                    "v_energy": bbe,
                    "v_rs": rs,
                    "dyn_rs": rs,
                    "dyn_cpv": cpv,
                    "dyn_tb": tb,
                    "sn_score": float(score),
                    "dtw_score": 0.0,
                },
                "side": "LONG",
            }

    # 2) COSINE 매칭 (DNA_SUPERNOVA* + MFE_WEIGHTED)
    cosine_cut = float(cfg.get("DYNAMIC_SUPERNOVA_CUTOFF", 0.50))
    candidates = {}
    for key, val in cfg.items():
        if not isinstance(val, dict):
            continue
        if key.startswith("DNA_SUPERNOVA"):
            vec = [float(val.get("cpv", 0.0)), float(val.get("tb", 0.0)), float(val.get("bbe", 0.0))]
            candidates[key] = {"vec": np.array(vec, dtype=float), "shape": val.get("shape")}
    if isinstance(cfg.get("DNA_SUPERNOVA_MFE_WEIGHTED"), dict):
        mv = cfg["DNA_SUPERNOVA_MFE_WEIGHTED"]
        candidates["MFE_WEIGHTED"] = {
            "vec": np.array([float(mv.get("cpv", 0.0)), float(mv.get("tb", 0.0)), float(mv.get("bbe", 0.0))], dtype=float),
            "shape": mv.get("shape"),
        }

    best_name, best_cos, best_dtw = "UNKNOWN", 0.0, 999.0
    for name, t in candidates.items():
        cos = _cosine_similarity(cur_vec, t.get("vec"))
        shp = t.get("shape")
        dtw = _calc_dtw(cur_shape, shp) if isinstance(shp, list) and len(shp) == 20 else 999.0
        if cos > best_cos:
            best_cos, best_dtw, best_name = cos, dtw, name
    if best_cos >= cosine_cut:
        return {
            "engine_name": "SUPERNOVA_COSINE",
            "sig_type": f"[SUPERNOVA_COSINE] 🦅 {best_name}",
            "score": float(best_cos * 100.0),
            "facts": {
                "v_cpv": cpv,
                "v_yang": tb,
                "v_energy": bbe,
                "v_rs": rs,
                "dyn_rs": rs,
                "dyn_cpv": cpv,
                "dyn_tb": tb,
                "sn_score": float(best_cos),
                "dtw_score": float(best_dtw if np.isfinite(best_dtw) else 0.0),
            },
            "side": "LONG",
        }
    return None


def _scan_one_table(
    tbl: str,
    tf: str,
    idx_close: pd.Series,
    hit_rank_start: int,
    *,
    engine_filter: str | None = None,
    include_embedded_supernova: bool = True,
):
    conn = get_connection(DB_READ_PATH, read_only=True)
    try:
        time.sleep(0.03)  # DB burst 완화
        df = _load_table(conn, tbl)
        if df is None or len(df) < 240:
            return []
        symbol = "_".join(tbl.split("_")[2:-1])
        hits = []
        rank = hit_rank_start
        engine_pool = _build_engine_pool(engine_filter)
        for engine_name, engine in engine_pool:
            hit, sig_type, out_df, dbg = engine(df, idx_close, tf)
            if hit:
                rank += 1
                signal_side = str(dbg.get("side", "LONG")).upper()
                chart_main = save_chart(out_df, f"{symbol}_{tf}_{engine_name}", rank, show_volume=True, is_promo=False, side=signal_side)
                chart_promo = save_chart(out_df, f"{symbol}_{tf}_{engine_name}", rank, show_volume=False, is_promo=True, side=signal_side)
                time.sleep(0.05)  # 외부 AI 호출/차트 I/O 사이 간격
                ai = generate_ai_report(symbol, tf)
                last_close = float(out_df["Close"].iloc[-1]) if out_df is not None and not out_df.empty else 0.0
                hits.append((engine_name, sig_type, float(dbg.get("score", 0.0)), chart_main, chart_promo, ai, dbg, last_close, rank, signal_side))
                del out_df
                flush_gc(label=GC_AFTER_SCAN_TABLE)
        if include_embedded_supernova:
            sn = _supernova_hit(df, symbol, tf)
        else:
            sn = None
        if sn is not None:
            rank += 1
            signal_side = str(sn.get("side", "LONG")).upper()
            dbg = {
                "score": float(sn.get("score", 0.0)),
                "v_cpv": float(sn["facts"].get("v_cpv", 0.0)),
                "v_yang": float(sn["facts"].get("v_yang", 0.0)),
                "v_energy": float(sn["facts"].get("v_energy", 0.0)),
                "v_rs": float(sn["facts"].get("v_rs", 0.0)),
                "dyn_rs_score": float(sn["facts"].get("dyn_rs", 0.0)),
                "dyn_cpv_score": float(sn["facts"].get("dyn_cpv", 0.0)),
                "dyn_tb_score": float(sn["facts"].get("dyn_tb", 0.0)),
                "sn_score": float(sn["facts"].get("sn_score", 0.0)),
                "dtw_score": float(sn["facts"].get("dtw_score", 0.0)),
                "side": signal_side,
                "entry_high": float(df["High"].iloc[-1]),
                "v11_comment": f"🦅 Supernova Sniper | CPV {sn['facts'].get('v_cpv',0):.3f} | TB {sn['facts'].get('v_yang',0):.3f} | BBE {sn['facts'].get('v_energy',0):.3f}",
            }
            chart_main = save_chart(df, f"{symbol}_{tf}_{sn['engine_name']}", rank, show_volume=True, is_promo=False, side=signal_side)
            chart_promo = save_chart(df, f"{symbol}_{tf}_{sn['engine_name']}", rank, show_volume=False, is_promo=True, side=signal_side)
            ai = generate_ai_report(symbol, tf)
            last_close = float(df["Close"].iloc[-1])
            hits.append((sn["engine_name"], sn["sig_type"], float(sn["score"]), chart_main, chart_promo, ai, dbg, last_close, rank, signal_side))
        del df
        flush_gc(label=GC_AFTER_SCAN_TABLE)
        return hits
    except Exception as e:
        log_exception(logger, "scan error %s: %s", tbl, e)
        return []
    finally:
        conn.close()


def run_scan(
    market_filter: str | None = None,
    *,
    engine_filter: str | None = None,
    include_embedded_supernova: bool | None = None,
):
    """
    MTF table scan. market_filter: None | 'spot' | 'futures'
    engine_filter: nulrim | dante | ema5 | master — staggered slot SSOT.
    """
    if include_embedded_supernova is None:
        include_embedded_supernova = not engine_filter
    global sent_today, last_run_date
    if not os.path.exists(DB_PATH):
        logger.error("DB not found. Run bitget_mtf_data_updater.py first.")
        return
    today_str = utc_date_str()
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

    conn = get_connection(DB_READ_PATH, read_only=True)
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%__tmp%'"
    ).fetchall()
    table_names = [r[0] for r in rows if "__tmp" not in str(r[0])]
    hit_rank = 0
    funnel_stats: dict[str, dict[str, int]] = {
        "spot": {"universe": 0, "survivors": 0},
        "futures": {"universe": 0, "survivors": 0},
    }

    for tf in TIMEFRAMES:
        idx_close = _benchmark_series(conn, tf)
        tf_tables = [t for t in table_names if t.startswith("BITGET_") and t.endswith(f"_{tf}") and BENCHMARK not in t]
        mf = (market_filter or "").strip().lower()
        if mf == "spot":
            tf_tables = [t for t in tf_tables if "_SPOT_" in t]
        elif mf in ("futures", "fut", "linear"):
            tf_tables = [t for t in tf_tables if "_FUT_" in t]
        for tbl in tf_tables:
            mkt_key = "futures" if "_FUT_" in tbl else "spot"
            funnel_stats[mkt_key]["universe"] += 1
        with ThreadPoolExecutor(max_workers=min(MAX_SCAN_WORKERS, max(1, len(tf_tables)))) as pool:
            futures = {
                pool.submit(
                    _scan_one_table,
                    tbl,
                    tf,
                    idx_close,
                    hit_rank,
                    engine_filter=engine_filter,
                    include_embedded_supernova=include_embedded_supernova,
                ): tbl
                for tbl in tf_tables
            }
            for fut in as_completed(futures):
                tbl = futures[fut]
                hits = fut.result()
                symbol = "_".join(tbl.split("_")[2:-1])
                for engine_name, sig_type, score, chart_main, chart_promo, ai, dbg, last_close, rank, signal_side in hits:
                    hit_rank = max(hit_rank, int(rank))
                    mkt_key = "futures" if "_FUT_" in tbl else "spot"
                    funnel_stats[mkt_key]["survivors"] += 1
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
                    try:
                        _process_scan_hit(
                            tbl=tbl,
                            tf=tf,
                            symbol=symbol,
                            engine_name=engine_name,
                            sig_type=sig_type,
                            score=score,
                            chart_main=chart_main,
                            chart_promo=chart_promo,
                            ai=ai,
                            dbg=dbg,
                            last_close=last_close,
                            signal_side=signal_side,
                        )
                    except Exception as e:
                        # DB 락(락 재시도 소진) 등 단일 히트 처리 실패가 전체 스캔(다른 심볼/TF)을
                        # 통째로 죽이지 않도록 격리한다. (2026-07-04 scan_spot_dante FAIL 원인)
                        log_exception(
                            logger,
                            "[%s] %s %s -> hit processing failed (isolated): %s",
                            engine_name,
                            symbol,
                            tf,
                            e,
                        )
                    del dbg
                    flush_gc(label=GC_AFTER_SCAN_TABLE)
                del hits
                flush_gc(label=GC_AFTER_SCAN_TABLE)
    conn.close()
    try:
        from bitget.infra.proprietary_friction_store_bg import insert_scan_funnel_snapshot

        ts = utc_hm_key()
        for mkt, st in funnel_stats.items():
            uni = int(st.get("universe", 0) or 0)
            surv = int(st.get("survivors", 0) or 0)
            if uni <= 0:
                continue
            pr = 100.0 * surv / uni
            insert_scan_funnel_snapshot(
                ts=ts,
                market=mkt,
                universe_size=uni,
                survivors=surv,
                pass_rate_pct=pr,
            )
    except Exception:
        pass
    del table_names
    flush_gc(label="scan_complete")
    wait_telegram_queue_drained(("MAIN", "PROMO"), timeout_sec=7200.0)


def _process_scan_hit(
    *,
    tbl: str,
    tf: str,
    symbol: str,
    engine_name: str,
    sig_type: str,
    score: float,
    chart_main,
    chart_promo,
    ai,
    dbg: dict,
    last_close: float,
    signal_side: str,
) -> None:
    """단일 스캔 히트(신호) 처리 — 장부 기록 + 실주문 + 텔레그램 큐잉.

    run_scan 의 히트 루프에서 분리해, 이 함수 안에서 예외(예: DB 락 재시도 소진)가
    나도 호출부에서 격리·로깅만 하고 나머지 심볼/타임프레임 스캔은 계속 진행되게 한다.
    """
    market_type = "futures" if "_FUT_" in tbl else "spot"
    facts = {
        "v_cpv": dbg.get("v_cpv", 0.0),
        "v_yang": dbg.get("v_yang", 0.0),
        "v_energy": dbg.get("v_energy", 0.0),
        "v_rs": dbg.get("v_rs", 0.0),
        "dyn_rs": dbg.get("dyn_rs_score", 0.0),
        "dyn_cpv": dbg.get("dyn_cpv_score", 0.0),
        "dyn_tb": dbg.get("dyn_tb_score", 0.0),
        "sn_score": dbg.get("sn_score", 0.0),
        "dtw_score": dbg.get("dtw_score", 0.0),
        "trade_value_24h": float(dbg.get("trade_value_24h", 0.0) or 0.0),
        "marcap_eok": float(dbg.get("marcap_eok", 0.0) or 0.0),
        "is_top_dna": bool(dbg.get("is_top_dna", False)),
        "is_worst_dna": bool(dbg.get("is_worst_dna", False)),
        "is_death_combo": bool(dbg.get("is_death_combo", False)),
        "is_tenbagger": bool(dbg.get("is_tenbagger", False)),
    }
    sig_for_db = sig_type if str(engine_name).startswith("SUPERNOVA_") else f"[STANDARD][{engine_name}] {sig_type}"
    time.sleep(0.04)  # DB/API 연속 호출 완화
    ok, db_msg = try_add_virtual_position(
        market_type=market_type,
        symbol=symbol,
        timeframe=tf,
        sig_type=sig_for_db,
        score=score,
        entry_price=float(dbg.get("last_close", last_close)),
        facts=facts,
        side=dbg.get("side", "LONG"),
        entry_high=dbg.get("entry_high", float(dbg.get("last_close", last_close))),
    )
    if not ok:
        rsn = str(db_msg)
        if ("ANTI_PATTERNS" in rsn) or ("TOXIC" in rsn) or ("DOOMSDAY" in rsn):
            try:
                bitget_shadow_tracking.record_blocked_trade(
                    symbol=symbol,
                    reason=rsn,
                    entry_price=float(dbg.get("last_close", last_close) or 0.0),
                    market_type=market_type,
                    name=symbol,
                    position_side=str(dbg.get("side", "LONG")).upper(),
                    timeframe=tf,
                )
            except Exception:
                pass
    if ok:
        try:
            order_side = str(dbg.get("side", "LONG")).upper()
            order_amount = float(dbg.get("order_amount", 0.0) or 0.0)
            amount_source = "raw"
            if order_amount <= 0:
                # Paper≈live: prefer virtual Kelly quantity (ARCR/NAV already applied)
                vqty = _lookup_virtual_trade_quantity(
                    market_type=market_type,
                    symbol=symbol,
                    timeframe=tf,
                    sig_type=sig_for_db,
                    side=order_side,
                )
                if vqty > 0:
                    order_amount = float(vqty)
                    amount_source = "virtual_kelly"
                else:
                    # last-resort exchange-min approximation — not institutional sizing
                    px = float(dbg.get("last_close", last_close) or 0.0)
                    default_notional = 20.0
                    order_amount = (default_notional / px) if px > 0 else 0.0
                    amount_source = "fallback_min_notional"
            order_lev = float(dbg.get("leverage", 3.0) or 3.0)
            exec_result = execute_real_order(
                symbol=symbol,
                side=order_side,
                amount=order_amount,
                leverage=order_lev,
                market_type=market_type,
                strategy_key=str(engine_name or ""),
                amount_source=amount_source,
            )
            vt_id = _lookup_virtual_trade_id(
                market_type=market_type,
                symbol=symbol,
                timeframe=tf,
                sig_type=sig_for_db,
                side=order_side,
            )
            log_real_execution(
                market_type=market_type,
                symbol=symbol,
                timeframe=tf,
                engine_name=engine_name,
                sig_type=sig_for_db,
                side=order_side,
                amount=order_amount,
                leverage=order_lev,
                entry_price=float(dbg.get("last_close", last_close) or 0.0),
                exec_result=exec_result,
                virtual_trade_id=vt_id,
            )
            if exec_result.get("ok"):
                db_msg = f"{db_msg} | 실전주문:{exec_result.get('status')}"
            else:
                db_msg = f"{db_msg} | 실전주문실패:{exec_result.get('status')}"
        except Exception as e:
            db_msg = f"{db_msg} | 실전주문예외:{e}"

    main_caption = (
        f"🎯 [{signal_side}] [{sig_type}]\n"
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
    enqueue_telegram(
        "MAIN",
        chart_main,
        main_caption,
        enabled=SEND_TELEGRAM,
        send_profile="html",
    )
    enqueue_telegram(
        "PROMO",
        chart_promo,
        promo_caption,
        enabled=SEND_TELEGRAM,
        send_profile="html",
    )
    logger.info("[%s] %s %s -> %s | charts queued", engine_name, symbol, tf, db_msg)


def _is_candle_close_time(now_utc: datetime, tf: str) -> bool:
    return _is_candle_close_parts(now_utc.hour, now_utc.minute, tf)


def _is_candle_close_parts(hour: int, minute: int, tf: str) -> bool:
    if tf == "1H":
        return minute == 0
    if tf == "2H":
        return minute == 0 and hour % 2 == 0
    if tf == "4H":
        return minute == 0 and hour % 4 == 0
    if tf == "1D":
        return minute == 0 and hour == 0
    return False


class _TrackWorkerPool:
    """Persistent track workers — no per-15min Thread() allocation."""

    _MARKETS = ("spot", "futures")

    def __init__(self) -> None:
        self._events = {m: threading.Event() for m in self._MARKETS}
        for market in self._MARKETS:
            threading.Thread(
                target=self._loop,
                args=(market,),
                daemon=True,
                name=f"bitget_track_{market}",
            ).start()

    def _loop(self, market_type: str) -> None:
        evt = self._events[market_type]
        while True:
            evt.wait()
            evt.clear()
            try:
                track_daily_positions(market_type=market_type)
            except Exception as e:
                log_exception(logger, "track worker error [%s]: %s", market_type, e)

    def trigger(self) -> None:
        for evt in self._events.values():
            evt.set()


_TIMEFRAMES_TUPLE = tuple(TIMEFRAMES)


def run_mtf_scheduler():
    from bitget.infra.daemon_loop import (
        MTF_SCHEDULER_ERROR_SEC,
        MTF_SCHEDULER_POLL_SEC,
        MTF_SCHEDULER_POST_SCAN_SLEEP_SEC,
        DaemonLoopFrame,
        collect_due_timeframes,
        sleep_or_backoff,
    )

    logger.info("Bitget MTF scanner waiting (UTC candle close schedule)")
    logger.info(" - 1H: every hour")
    logger.info(" - 2H: even hours")
    logger.info(" - 4H: 0/4/8/12/16/20")
    logger.info(" - 1D: 00:00 UTC")
    frame = DaemonLoopFrame()
    track_pool = _TrackWorkerPool()

    while True:
        try:
            frame.refresh_utc(truncate_minute=True)
            tick = frame.tick
            if tick.minute % 15 == 0 and frame.dedup.track_once(tick.hm_key):
                track_pool.trigger()
            if tick.hour == 0 and tick.minute == 10 and frame.dedup.mutant_day_once(tick.day_key):
                ok, m = generate_mutant_strategies()
                logger.info("[incubator] %s", m)
                if not ok:
                    frame.dedup.reset_mutant_day()
            collect_due_timeframes(
                tick.hour,
                tick.minute,
                _TIMEFRAMES_TUPLE,
                frame.due_buf,
                is_close_fn=_is_candle_close_parts,
            )
            if frame.due_buf:
                trigger_key = f"{tick.now.isoformat()}|{frame.due_buf.join()}"
                if frame.dedup.trigger_once(trigger_key):
                    logger.info(
                        "[scan start] %s UTC | TF: %s",
                        tick.hm_key,
                        ", ".join(frame.due_buf.items),
                    )
                    run_scan()
                    frame.mark_ok()
                    sleep_or_backoff(
                        normal_sec=MTF_SCHEDULER_POST_SCAN_SLEEP_SEC,
                        after_error=False,
                    )
                    continue
            frame.mark_ok()
            sleep_or_backoff(normal_sec=MTF_SCHEDULER_POLL_SEC, after_error=frame.loop_error)
        except Exception as e:
            log_exception(logger, "scheduler error: %s", e)
            frame.mark_error()
            sleep_or_backoff(
                normal_sec=MTF_SCHEDULER_POLL_SEC,
                after_error=frame.loop_error,
                error_sec=MTF_SCHEDULER_ERROR_SEC,
            )


if __name__ == "__main__":
    run_mtf_scheduler()
