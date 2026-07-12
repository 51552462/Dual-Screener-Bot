"""Shared DB paths, config, Telegram for Bitget forward ledger."""
from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time

import pandas as pd
import requests

from bitget.env import bitget_telegram_chat_id, bitget_telegram_token
from bitget.funding_fetcher import fetch_funding_snapshot
from bitget.infra.bounded_reads import (
    forward_brain_tune_closed_sql,
    forward_zombie_fact_close_ids_sql,
    forward_zombie_zero_invest_ids_sql,
)
from bitget.infra.clock import utc_date_key
from bitget.infra.data_paths import market_data_db_path, system_config_json_path
from bitget.infra.shared_db_connector import get_connection
import memory_bounds

from bitget.infra.memory_policy import (
    FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT,
    FUNDING_SNAP_CACHE_MAX_KEYS,
    FUNDING_SNAP_TTL_SEC,
)
from bitget.infra.logging_setup import get_logger, log_exception

DB_PATH = market_data_db_path()
CONFIG_PATH = system_config_json_path()
TELEGRAM_TOKEN = bitget_telegram_token()
TELEGRAM_CHAT_ID = bitget_telegram_chat_id()
logger = get_logger("bitget.forward.shared")

DEFAULT_MAX_OPEN_POSITIONS = 20
_FUNDING_SNAP_CACHE: dict = {}

# init_forward_db()는 거의 모든 forward 함수 호출 시작부에서 호출된다.
# 스키마 DDL(CREATE TABLE/ALTER/DROP+CREATE VIEW)을 매번 재실행하면, 배포 후
# 여러 bitget 서비스(ws/factory/async/queue-worker)가 동시에 기동하는 순간
# DDL 락 경합("database is locked")이 폭증한다. DB 경로별로 프로세스당 1회만
# 실행하도록 메모이즈한다(신규 컬럼/뷰는 재기동 시 각 프로세스의 첫 호출에서
# 반영됨; 경로별 캐시라 테스트에서 DB_PATH 를 바꿔도 매번 정상 초기화된다).
_FORWARD_DB_SCHEMA_READY_PATHS: set = set()
_FORWARD_DB_INIT_LOCK = threading.Lock()


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


def _ensure_col(cur, col_name, col_type, table: str = "bitget_forward_trades"):
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
    except Exception:
        pass

def _cached_funding_snapshot(symbol: str):
    k = str(symbol or "")
    now = time.time()
    ent = _FUNDING_SNAP_CACHE.get(k)
    if ent:
        ts0, snap0 = ent
        if snap0 is not None and (now - ts0) < FUNDING_SNAP_TTL_SEC:
            return snap0
    snap = fetch_funding_snapshot(k)
    _FUNDING_SNAP_CACHE[k] = (now, snap)
    memory_bounds.evict_oldest_dict_keys(
        _FUNDING_SNAP_CACHE,
        FUNDING_SNAP_CACHE_MAX_KEYS,
        ts_getter=lambda key: (_FUNDING_SNAP_CACHE.get(key) or (0.0, None))[0],
    )
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
    conn = get_connection(DB_PATH)
    path_key = os.path.abspath(str(DB_PATH))
    if path_key not in _FORWARD_DB_SCHEMA_READY_PATHS:
        with _FORWARD_DB_INIT_LOCK:
            if path_key not in _FORWARD_DB_SCHEMA_READY_PATHS:
                _init_forward_db_schema(conn)
                _FORWARD_DB_SCHEMA_READY_PATHS.add(path_key)
    # 실시간 뇌수술: 최근 청산 데이터를 기반으로 실제 설정을 자율 튜닝/저장
    try:
        tune_q, tune_params = forward_brain_tune_closed_sql()
        df_recent_closed = pd.read_sql(tune_q, conn, params=tune_params)
        if not df_recent_closed.empty:
            from bitget.forward.mutant import _auto_tune_brain_from_closed_df

            cfg_live = load_system_config()
            cfg_live, _ = _auto_tune_brain_from_closed_df(cfg_live, df_recent_closed)
            save_system_config(cfg_live)
    except Exception:
        pass
    conn.close()


def _init_forward_db_schema(conn):
    """프로세스당 1회만 실행되는 스키마 부트스트랩/마이그레이션 본체."""
    cur = conn.cursor()
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
    _ensure_col(cur, "pyramid_adds", "INTEGER DEFAULT 0")
    _ensure_col(cur, "parent_trade_id", "INTEGER DEFAULT 0")
    _ensure_col(cur, "scaled_out_frac", "REAL DEFAULT 0.0")
    _ensure_col(cur, "realized_partial_ret", "REAL DEFAULT 0.0")
    _ensure_col(cur, "free_runner", "INTEGER DEFAULT 0")
    try:
        import bitget.shadow_tracking as bitget_shadow_tracking
        bitget_shadow_tracking.init_shadow_tables(cur)
    except Exception as e:
        log_exception(logger, "shadow ledger schema init skipped: %s", e)

    try:
        from bitget.infra.proprietary_friction_store_bg import ensure_proprietary_friction_schema

        ensure_proprietary_friction_schema(cursor=cur)
    except Exception as e:
        log_exception(logger, "PRI friction schema init skipped: %s", e)

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
    _ensure_col(cur, "client_order_id", "TEXT DEFAULT ''", table="bitget_real_execution")

    try:
        # DROP/CREATE 를 하나의 try 로 묶어야 한다: 다른 프로세스가 동시에 같은
        # 시퀀스를 실행 중이면 DROP 단계에서도 "database is locked" 이 날 수 있다
        # (기존엔 CREATE 만 보호되어 DROP 실패가 그대로 상위로 전파돼 파이프라인이
        # 크래시했다).
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
        # 멀티스레드/멀티프로세스 동시 초기화 시 DROP/CREATE VIEW 경합 무시
        # (다음 프로세스 재기동 때 다시 시도되므로 안전)
        pass
    conn.commit()


_EXIT_REASON_ZOMBIE_DB = "REPORTER_ZOMBIE_CLEANUP"
_EXIT_REASON_FACT_CLOSE_DB = "REPORTER_FACT_CLOSE"


def reporter_cleanup_zombie_forward_trades() -> int:
    """
    OPEN/ACTIVE 인데 수량·투입 0 → CLOSED_ZOMBIE (주식 forward/shared 동일 규칙).
    bitget_forward_trades 직접 갱신 — DROP 없음.
    """
    if not os.path.isfile(DB_PATH):
        return 0
    init_forward_db()
    conn = get_connection(DB_PATH)
    total = 0
    exit_day = utc_date_key()
    batch_lim = FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT
    try:
        while True:
            z_q, z_params = forward_zombie_zero_invest_ids_sql(limit=batch_lim)
            cur = conn.execute(z_q, z_params)
            ids = [int(r[0]) for r in cur.fetchall() if r and r[0] is not None]
            if not ids:
                break
            conn.executemany(
                """
                UPDATE bitget_forward_trades
                SET status='CLOSED_ZOMBIE', exit_date=?, exit_reason=?,
                    final_ret=COALESCE(final_ret, 0.0)
                WHERE id=?
                """,
                [(exit_day, _EXIT_REASON_ZOMBIE_DB, i) for i in ids],
            )
            total += len(ids)
            conn.commit()
            if len(ids) < batch_lim:
                break

        while True:
            f_q, f_params = forward_zombie_fact_close_ids_sql(limit=batch_lim)
            cur2 = conn.execute(f_q, f_params)
            ids2 = [int(r[0]) for r in cur2.fetchall() if r and r[0] is not None]
            if not ids2:
                break
            conn.executemany(
                """
                UPDATE bitget_forward_trades
                SET status='CLOSED_AUTO', exit_reason=?,
                    final_ret=COALESCE(final_ret, 0.0)
                WHERE id=?
                """,
                [(_EXIT_REASON_FACT_CLOSE_DB, i) for i in ids2],
            )
            total += len(ids2)
            conn.commit()
            if len(ids2) < batch_lim:
                break

        if total:
            conn.commit()
        logger.info("reporter_cleanup_zombie_forward_trades: %s", total)
        return total
    finally:
        conn.close()


def get_exploration_role_scaler(sys_config: dict, group_key: str):
    """[동적 탐험예산] Kelly 최종 비중에 곱할 챔피언/탐험 역할 스케일러.

    실제 로직은 bitget.governance.exploration_budget 에 있다 — 여기서는
    ledger.py 등 forward 경로에서 편하게 임포트할 수 있도록 얇게 재노출한다.
    실패 시 항상 (1.0, "NEUTRAL") 폴백 — 기존 Kelly 동작 무변경 보장.
    """
    try:
        from bitget.governance.exploration_budget import (
            get_exploration_role_scaler as _impl,
        )

        return _impl(sys_config, group_key)
    except Exception:
        return 1.0, "NEUTRAL"

