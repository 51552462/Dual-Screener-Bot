"""Shared DB paths, config, Telegram for Bitget forward ledger."""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time

import pandas as pd
import requests

from bitget.env import bitget_telegram_chat_id, bitget_telegram_token
from bitget.funding_fetcher import fetch_funding_snapshot
from bitget.infra.data_paths import market_data_db_path, system_config_json_path

DB_PATH = market_data_db_path()
CONFIG_PATH = system_config_json_path()
TELEGRAM_TOKEN = bitget_telegram_token()
TELEGRAM_CHAT_ID = bitget_telegram_chat_id()

DEFAULT_MAX_OPEN_POSITIONS = 20
_FUNDING_SNAP_CACHE: dict = {}


def _telegram_plain_from_html(chunk: str) -> str:
    return re.sub(r"</?([a-zA-Z][a-zA-Z0-9]*)[^>]*>", "", chunk)


def send_telegram_msg(text, *, parse_mode: str = "HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        max_len = 4000
        chunks = [text[i : i + max_len] for i in range(0, len(text), max_len)]
        use_html = str(parse_mode or "").upper() == "HTML"
        for chunk in chunks:
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": chunk}
            if use_html:
                payload["parse_mode"] = "HTML"
            res = requests.post(url, json=payload, timeout=10)
            if use_html and res.status_code == 400:
                plain = _telegram_plain_from_html(chunk)
                requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": plain}, timeout=10)
            time.sleep(0.5)
    except Exception:
        pass


def load_system_config() -> dict:
    from bitget.infra import config_manager

    return config_manager.load_system_config() or {}


def save_system_config(cfg: dict) -> None:
    from bitget.infra import config_manager

    config_manager.save_system_config(cfg)


def _ensure_col(cur, col_name, col_type):
    try:
        cur.execute(f"ALTER TABLE bitget_forward_trades ADD COLUMN {col_name} {col_type}")
    except Exception:
        pass

def _cached_funding_snapshot(symbol: str):
    k = str(symbol or "")
    now = time.time()
    ent = _FUNDING_SNAP_CACHE.get(k)
    if ent:
        ts0, snap0 = ent
        if snap0 is not None and (now - ts0) < 55.0:
            return snap0
    snap = fetch_funding_snapshot(k)
    _FUNDING_SNAP_CACHE[k] = (now, snap)
    return snap

def _deathmatch_min_n_cfg(cfg: dict) -> int:
    v = cfg.get("DEATHMATCH_MIN_TRADES_PER_ARM", 5)
    try:
        n = int(v)
    except (TypeError, ValueError):
        n = 5
    return max(1, n)

def _fmt_deathmatch_ret(ret, n_closed: int, *, n_valid=None) -> str:
    from evolution.deathmatch_report import fmt_deathmatch_ret

    return fmt_deathmatch_ret(ret, n_closed, n_valid=n_valid)

def _deathmatch_ab_verdict(n_std: int, n_sn: int, std_ret, sn_ret, n_min: int) -> str:
    from evolution.deathmatch_report import deathmatch_ab_verdict

    return deathmatch_ab_verdict(n_std, n_sn, std_ret, sn_ret, n_min)

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
            is_tenbagger INTEGER DEFAULT 0,
            is_top_dna INTEGER DEFAULT 0,
            is_worst_dna INTEGER DEFAULT 0,
            is_death_combo INTEGER DEFAULT 0,
            entry_price REAL,
            position_side TEXT DEFAULT 'LONG',
            entry_atr REAL,
            entry_high REAL DEFAULT 0.0,
            atr_sl_mult REAL,
            stop_price REAL,
            leverage REAL DEFAULT 1.0,
            tf_weight REAL DEFAULT 1.0,
            sim_kelly_risk_pct REAL DEFAULT 0.01,
            margin_used REAL DEFAULT 0.0,
            sim_kelly_invest REAL DEFAULT 0.0,
            quantity REAL DEFAULT 0.0,
            entry_cos_score REAL DEFAULT 0.0,
            entry_dtw_score REAL DEFAULT 0.0,
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
    _ensure_col(cur, "funding_rate_last", "REAL DEFAULT 0.0")
    _ensure_col(cur, "funding_next_settle_ts", "TEXT DEFAULT ''")
    _ensure_col(cur, "funding_accum_usdt_est", "REAL DEFAULT 0.0")
    _ensure_col(cur, "position_side", "TEXT DEFAULT 'LONG'")
    _ensure_col(cur, "entry_high", "REAL DEFAULT 0.0")
    _ensure_col(cur, "entry_cos_score", "REAL DEFAULT 0.0")
    _ensure_col(cur, "entry_dtw_score", "REAL DEFAULT 0.0")
    _ensure_col(cur, "is_tenbagger", "INTEGER DEFAULT 0")
    _ensure_col(cur, "is_top_dna", "INTEGER DEFAULT 0")
    _ensure_col(cur, "is_worst_dna", "INTEGER DEFAULT 0")
    _ensure_col(cur, "is_death_combo", "INTEGER DEFAULT 0")
    try:
        import bitget.shadow_tracking as bitget_shadow_tracking
        bitget_shadow_tracking.init_shadow_tables(cur)
    except Exception as e:
        print(f"⚠️ 그림자 장부 스키마 초기화 스킵: {e}")

    # 실전 체결/리더보드 동기화 로그
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bitget_real_execution (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            updated_at TEXT,
            market_type TEXT,
            symbol TEXT,
            timeframe TEXT,
            practitioner_key TEXT,
            engine_name TEXT,
            sig_type TEXT,
            position_side TEXT,
            amount REAL DEFAULT 0.0,
            leverage REAL DEFAULT 1.0,
            entry_price REAL DEFAULT 0.0,
            order_id TEXT DEFAULT '',
            exec_status TEXT DEFAULT '',
            exec_ok INTEGER DEFAULT 0,
            is_dry_run INTEGER DEFAULT 0,
            notional_usdt REAL DEFAULT 0.0,
            balance_before REAL DEFAULT 0.0,
            balance_after REAL DEFAULT 0.0,
            realized_pnl_usdt REAL DEFAULT 0.0,
            realized_ret_pct REAL DEFAULT 0.0,
            virtual_trade_id INTEGER DEFAULT 0,
            virtual_final_ret REAL,
            virtual_mfe REAL,
            exec_payload TEXT DEFAULT ''
        )
        """
    )
    _ensure_col(cur, "client_order_id", "TEXT DEFAULT ''")

    cur.execute("DROP VIEW IF EXISTS forward_trades")
    try:
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
                is_tenbagger,
                is_top_dna,
                is_worst_dna,
                is_death_combo,
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
    except sqlite3.OperationalError:
        # 멀티스레드 동시 초기화 시 뷰 생성 경합(table/view already exists) 무시
        pass
    conn.commit()
    # 실시간 뇌수술: 최근 청산 데이터를 기반으로 실제 설정을 자율 튜닝/저장
    try:
        df_recent_closed = pd.read_sql(
            "SELECT * FROM bitget_forward_trades WHERE status LIKE 'CLOSED%' ORDER BY id DESC LIMIT 120",
            conn,
        )
        if not df_recent_closed.empty:
            from bitget.forward.mutant import _auto_tune_brain_from_closed_df

            cfg_live = load_system_config()
            cfg_live, _ = _auto_tune_brain_from_closed_df(cfg_live, df_recent_closed)
            save_system_config(cfg_live)
    except Exception:
        pass
    conn.close()

