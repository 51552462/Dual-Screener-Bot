import json
import os
import re
import sqlite3
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests

from bitget_env import bitget_telegram_chat_id, bitget_telegram_token
from bitget_funding_fetcher import fetch_funding_snapshot
from meta_governor_consumer import (
    apply_meta_kelly_merge,
    effective_max_position_pct,
    load_meta_state_resolved,
)
from report_state_binder import build_macro_treasury_block, format_macro_treasury_section_html

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")
TELEGRAM_TOKEN = bitget_telegram_token()
TELEGRAM_CHAT_ID = bitget_telegram_chat_id()

# 동시 오픈 포지션 상한(기본): 연쇄 청산(붓다빔) 리스크 완충. `BITGET_MAX_OPEN_POSITIONS` in bitget_system_config.json 로 변경.
_DEFAULT_BITGET_MAX_OPEN_POSITIONS = 20


_FUNDING_SNAP_CACHE = {}


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


def send_telegram_msg(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        # 텔레그램 4096자 제한 방어: 4000자씩 분할
        max_len = 4000
        chunks = [text[i:i+max_len] for i in range(0, len(text), max_len)]
        
        for chunk in chunks:
            # 1차 시도: HTML 모드
            res = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk, "parse_mode": "HTML"}, timeout=10)
            # 2차 시도: HTML 파싱 에러(400) 발생 시 일반 텍스트 모드로 재전송
            if res.status_code == 400:
                requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk}, timeout=10)
            import time
            time.sleep(0.5)
    except Exception:
        pass


def load_system_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_system_config(cfg):
    temp_path = f"{CONFIG_PATH}.temp"
    try:
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, CONFIG_PATH)
    except Exception:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass
        raise


def _deathmatch_min_n_cfg(cfg: dict) -> int:
    v = cfg.get("DEATHMATCH_MIN_TRADES_PER_ARM", 5)
    try:
        n = int(v)
    except (TypeError, ValueError):
        n = 5
    return max(1, n)


def _fmt_deathmatch_ret(ret: float, n_closed: int) -> str:
    if n_closed <= 0:
        return "산출 불가 (청산 0건)"
    return f"{float(ret):+.2f}%"


def _deathmatch_ab_verdict(n_std: int, n_sn: int, std_ret: float, sn_ret: float, n_min: int) -> str:
    ok_a = n_std >= n_min
    ok_b = n_sn >= n_min
    if not ok_a and not ok_b:
        return "거래 표본 부족 (양측 A/B 비교 보류)"
    if not ok_a and ok_b:
        return "거래 표본 부족 (오리지널 관망 대기 — A/B 대결 판정 보류)"
    if ok_a and not ok_b:
        return "거래 표본 부족 (초신성 관망 대기)"
    if sn_ret > std_ret:
        return "표본 충족: 청산 평균은 초신성(B)이 오리지널(A)보다 높음"
    if sn_ret < std_ret:
        return "표본 충족: 청산 평균은 오리지널(A)이 초신성(B)보다 높음"
    return "표본 충족: 양측 청산 평균 동일"


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
        import bitget_shadow_tracking
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
            "SELECT * FROM bitget_forward_trades WHERE market_type=? AND status LIKE 'CLOSED%' ORDER BY id DESC LIMIT 120",
            conn,
            params=(str(market_type).lower(),),
        )
        if not df_recent_closed.empty:
            cfg_live = load_system_config()
            cfg_live, _ = _auto_tune_brain_from_closed_df(cfg_live, df_recent_closed)
            save_system_config(cfg_live)
    except Exception:
        pass
    conn.close()


def _extract_practitioner_key(sig_type: str) -> str:
    s = str(sig_type or "")
    m = re.search(r"\[STANDARD\]\[(PRACT_\d{2})\]", s, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m2 = re.search(r"(PRACT_\d{2})", s, re.IGNORECASE)
    if m2:
        return m2.group(1).upper()
    return _extract_core_group(s) or "UNKNOWN"


def log_real_execution(
    market_type: str,
    symbol: str,
    timeframe: str,
    engine_name: str,
    sig_type: str,
    side: str,
    amount: float,
    leverage: float,
    entry_price: float,
    exec_result: dict,
    virtual_trade_id: int = 0,
):
    init_forward_db()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    ex = exec_result if isinstance(exec_result, dict) else {}
    status = str(ex.get("status", "unknown"))
    exec_ok = 1 if bool(ex.get("ok", False)) else 0
    is_dry_run = 1 if status == "dry_run" else 0
    order_id = str(ex.get("order_id", "") or "")
    px = float(entry_price or 0.0)
    qty = float(amount or 0.0)
    notional = float(px * qty) if px > 0 and qty > 0 else 0.0
    prac_key = _extract_practitioner_key(sig_type)
    client_oid = str(ex.get("client_order_id", "") or "")
    payload = json.dumps(ex, ensure_ascii=False)[:4000]
    bal_before = float(ex.get("balance_before", 0.0) or 0.0)
    bal_after = float(ex.get("balance_after", 0.0) or 0.0)
    pnl_usdt = float(ex.get("realized_pnl_usdt", 0.0) or 0.0)
    ret_pct = float(ex.get("realized_ret_pct", 0.0) or 0.0)

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    insert_sql = """
        INSERT INTO bitget_real_execution (
            created_at, updated_at, market_type, symbol, timeframe, practitioner_key, engine_name, sig_type,
            position_side, amount, leverage, entry_price, order_id, client_order_id, exec_status, exec_ok, is_dry_run,
            notional_usdt, balance_before, balance_after, realized_pnl_usdt, realized_ret_pct, virtual_trade_id, exec_payload
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        now, now, str(market_type).lower(), str(symbol), str(timeframe).upper(), str(prac_key),
        str(engine_name), str(sig_type), str(side).upper(), qty, float(leverage or 1.0), px,
        order_id, client_oid, status, exec_ok, is_dry_run, notional, bal_before, bal_after, pnl_usdt, ret_pct,
        int(virtual_trade_id or 0), payload
    )

    max_retry = 5
    for attempt in range(max_retry):
        try:
            conn.execute(insert_sql, params)
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower():
                if attempt >= max_retry - 1:
                    raise
                import time
                time.sleep(0.5 * (2 ** attempt))
                continue
            raise
    conn.close()


def sync_real_leaderboard_with_virtual():
    """
    실전 체결 로그와 가상 청산 결과를 연결해 practitioner별 실전/리서치 비교가 가능하도록 동기화.
    """
    init_forward_db()
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        UPDATE bitget_real_execution
        SET
            virtual_final_ret = (
                SELECT ft.final_ret FROM bitget_forward_trades ft
                WHERE ft.id = bitget_real_execution.virtual_trade_id
                  AND ft.status LIKE 'CLOSED%'
                LIMIT 1
            ),
            virtual_mfe = (
                SELECT ft.mfe FROM bitget_forward_trades ft
                WHERE ft.id = bitget_real_execution.virtual_trade_id
                  AND ft.status LIKE 'CLOSED%'
                LIMIT 1
            ),
            realized_ret_pct = CASE
                WHEN ABS(COALESCE(realized_ret_pct, 0.0)) > 1e-9 THEN realized_ret_pct
                ELSE COALESCE(
                    (
                        SELECT ft.final_ret FROM bitget_forward_trades ft
                        WHERE ft.id = bitget_real_execution.virtual_trade_id
                          AND ft.status LIKE 'CLOSED%'
                        LIMIT 1
                    ),
                    0.0
                )
            END,
            updated_at = ?
        WHERE IFNULL(virtual_trade_id, 0) > 0
          AND (
              virtual_final_ret IS NULL
              OR virtual_mfe IS NULL
          )
        """,
        (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),),
    )
    conn.commit()
    conn.close()


def build_practitioner_reality_leaderboard(market_type: str = "all", limit_rows: int = 30):
    init_forward_db()
    sync_real_leaderboard_with_virtual()
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    where_m = ""
    params = []
    if str(market_type).lower() in ("spot", "futures"):
        where_m = "WHERE market_type=?"
        params = [str(market_type).lower()]
    df_real = pd.read_sql(
        f"""
        SELECT market_type, practitioner_key, exec_ok, is_dry_run, notional_usdt, realized_ret_pct, virtual_final_ret
        FROM bitget_real_execution
        {where_m}
        ORDER BY id DESC
        LIMIT 5000
        """,
        conn,
        params=params,
    )
    conn.close()
    if df_real.empty:
        return pd.DataFrame()

    df_real["notional_usdt"] = pd.to_numeric(df_real["notional_usdt"], errors="coerce").fillna(0.0)
    df_real["realized_ret_pct"] = pd.to_numeric(df_real["realized_ret_pct"], errors="coerce").fillna(0.0)
    df_real["virtual_final_ret"] = pd.to_numeric(df_real["virtual_final_ret"], errors="coerce")
    g_rows = []
    for (mkt, pk), g in df_real.groupby(["market_type", "practitioner_key"], dropna=False):
        n = int(len(g))
        exec_ok_n = int((g["exec_ok"] > 0).sum())
        real_ret = float(g["realized_ret_pct"].mean()) if n > 0 else 0.0
        vir = g["virtual_final_ret"].dropna()
        virtual_ret = float(vir.mean()) if len(vir) > 0 else 0.0
        gap = real_ret - virtual_ret
        sum_notional = float(g["notional_usdt"].sum())
        reality_score = float(real_ret * np.log1p(max(sum_notional, 0.0) / 100.0))
        g_rows.append(
            {
                "market_type": str(mkt).upper(),
                "practitioner_key": str(pk),
                "samples": n,
                "exec_ok": exec_ok_n,
                "real_ret_pct": round(real_ret, 3),
                "virtual_ret_pct": round(virtual_ret, 3),
                "reality_gap_pct": round(gap, 3),
                "notional_usdt": round(sum_notional, 2),
                "reality_score": round(reality_score, 4),
            }
        )
    out = pd.DataFrame(g_rows)
    if out.empty:
        return out
    out = out.sort_values(["reality_score", "real_ret_pct", "samples"], ascending=[False, False, False]).head(int(limit_rows))
    return out.reset_index(drop=True)


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


def try_add_virtual_position(
    market_type,
    symbol,
    timeframe,
    sig_type,
    score,
    entry_price,
    facts,
    side="LONG",
    entry_high=0.0,
):
    init_forward_db()
    cfg = load_system_config()
    if str(cfg.get("GLOBAL_CIRCUIT_BREAKER", "OFF")).upper() == "ON":
        return False, "🚫 글로벌 서킷 브레이커 ON: 계좌 통합 동결 — 신규 진입 차단."
    tf = str(timeframe).upper()
    market_type = str(market_type).lower()
    symbol = str(symbol)
    position_side = str(side or "LONG").upper()
    if position_side not in ("LONG", "SHORT"):
        position_side = "LONG"
    if market_type == "spot" and position_side == "SHORT":
        return False, "현물(Spot) 시장 숏(Short) 진입 불가"
    entry_high_val = float(entry_high) if entry_high is not None else float(entry_price)

    score_bucket = int(float(score) // 10) * 10
    if score_bucket >= 100:
        score_bucket = 90
    tier_label = f"{score_bucket}점대"
    is_incubator_shadow = "[INCUBATOR_" in str(sig_type).upper()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")

    cur.execute(
        "SELECT id FROM bitget_forward_trades WHERE symbol=? AND timeframe=? AND market_type=? AND position_side=? AND status='OPEN'",
        (symbol, tf, market_type, position_side),
    )
    if cur.fetchone():
        conn.close()
        return False, "중복 보유 중"

    try:
        _q = cfg.get("BITGET_MAX_OPEN_POSITIONS", _DEFAULT_BITGET_MAX_OPEN_POSITIONS)
        max_open_quota = max(1, int(float(_q)))
    except (TypeError, ValueError):
        max_open_quota = _DEFAULT_BITGET_MAX_OPEN_POSITIONS
    cur.execute("SELECT COUNT(*) FROM bitget_forward_trades WHERE status='OPEN'")
    _open_quota_n = cur.fetchone()[0] or 0
    if int(_open_quota_n) >= max_open_quota:
        conn.close()
        return False, "🚨 시장 쿼터 초과"

    blocked, sim = _is_blocked_by_anti_patterns(cfg, facts, threshold=0.85)
    if blocked:
        try:
            import bitget_shadow_tracking
            bitget_shadow_tracking.record_blocked_trade(
                symbol=symbol,
                reason=f"TOXIC_ANTI_PATTERN(sim={sim:.3f})",
                entry_price=float(entry_price),
                market_type=market_type,
                name=symbol,
                position_side=position_side,
                timeframe=tf,
            )
        except Exception:
            pass
        conn.close()
        return False, f"ANTI_PATTERNS 차단: 참사 DNA 유사도 {sim:.3f} >= 0.850"

    hist_df = _load_hist(conn, market_type, symbol, tf, limit=300)
    if hist_df is None or len(hist_df) < 60:
        conn.close()
        return False, "ATR 계산용 히스토리 부족"

    evaluate_df = hist_df.copy()
    for col in ("Open", "High", "Low", "Close", "Volume"):
        evaluate_df[col] = pd.to_numeric(evaluate_df[col], errors="coerce")
    evaluate_df = evaluate_df.dropna(subset=("Open", "High", "Low", "Close", "Volume"), how="any")
    alpha_bonus_score = compute_evolved_alpha_bonus_score(cfg, evaluate_df)

    max_alpha_cos = _facts_cos_scalar_01(facts or {}, score)
    max_alpha_cos_effective = min(1.0, float(max_alpha_cos) + float(alpha_bonus_score))

    dyn_cos_limit = float(cfg.get("DYNAMIC_ALPHA_LIMIT", cfg.get("DYNAMIC_SUPERNOVA_CUTOFF", 0.75)))
    dyn_dtw_limit = float(cfg.get("DYNAMIC_DTW_LIMIT", 2.5))
    raw_dtw = None if not facts else facts.get("dtw_score")
    if raw_dtw is None or (isinstance(raw_dtw, str) and raw_dtw.strip() == ""):
        dtw_ok = True
    else:
        try:
            fd = float(raw_dtw)
            dtw_ok = fd <= dyn_dtw_limit
        except (TypeError, ValueError):
            dtw_ok = True

    cutoff_passed = (max_alpha_cos_effective >= dyn_cos_limit) and dtw_ok

    sig_type_row = sig_type
    if cutoff_passed and alpha_bonus_score > 0:
        sig_type_row = f"{sig_type_row} [🧬알파 융합 AST]"

    if not is_incubator_shadow and not cutoff_passed:
        conn.close()
        return False, (
            f"시계열 게이트: AST 융합 Cos_eff={max_alpha_cos_effective:.3f} (기준≥{dyn_cos_limit}) 또는 "
            f"DTW 조건 불만족(DTW cutoff≤{dyn_dtw_limit})"
        )

    entry_atr = _calc_atr14(hist_df)
    atr_sl_mult = float(cfg.get("ATR_SL_MULT", 2.0))
    if position_side == "SHORT":
        stop_price = float(entry_price) + (atr_sl_mult * entry_atr)
        risk_distance = stop_price - float(entry_price)
    else:
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

    # 💡 [Namespace Thompson Kelly Sampler] auto_forward_tester 와 동일: [TF]_*_BETA_PARAMS 로 자본 동적 배분
    kelly_risk_pct = _apply_thompson_kelly_multiplier(cfg, tf, sig_type, float(kelly_risk_pct))
    sector = _coin_asset_group(symbol)
    predicted_sector = str(cfg.get("PREDICTED_NEXT_SECTOR", "UNKNOWN"))
    is_rotation_prebuy = (sector == predicted_sector)
    sys_config = cfg

    # 👇👇 [V105.0 자율 진화] 순환매 선취매 태깅 및 베팅 어드밴티지 코인 이식 👇👇
    if is_rotation_prebuy:
        sig_type_row += " #순환매_선취매"
        # 관제탑이 주말 데스매치를 통해 우위를 증명했다면 켈리 비중 2배 뻥튀기
        if sys_config.get("ROTATION_ADVANTAGE_ACTIVE", False):
            kelly_risk_pct *= 2.0 

    core_group = _extract_core_group(sig_type)
    _meta_state = load_meta_state_resolved()
    ns_prefix = _thompson_ns_prefix(tf, sig_type)
    kelly_risk_pct = apply_meta_kelly_merge(
        kelly_risk_pct,
        _meta_state,
        ns_prefix=ns_prefix,
        core_group_name=core_group,
        sys_config=sys_config,
        entry_facts=facts if isinstance(facts, dict) else {},
        sector_mapped=str(sector),
    )
    account_size = float(cfg.get("ACCOUNT_SIZE_USDT", 100000))
    max_position_pct = float(effective_max_position_pct(cfg, _meta_state))

    cur.execute(
        "SELECT SUM((sim_kelly_invest * final_ret) / 100.0) FROM bitget_forward_trades WHERE status LIKE 'CLOSED%' AND sig_type LIKE ?",
        (f"%{core_group}%",),
    )
    realized_pnl = float(cur.fetchone()[0] or 0.0)
    group_current_seed = account_size + realized_pnl

    # 주식 auto_forward_tester AUM 스케일링 브레이크 이식 — USDT 규모: 대형 복리시드 + 소액 거래대금 = 슬리피지 차단
    if not is_incubator_shadow:
        fv = facts or {}
        seed_slip_thr = float(cfg.get("SEED_SLIPPAGE_GUARD_USDT", 50000.0))
        min_tv24_usdt = float(cfg.get("MIN_TRADE_VALUE_24H_SLIP_USDT", 5_000_000.0))
        has_liq = "trade_value_24h" in fv or "marcap_eok" in fv
        if group_current_seed > seed_slip_thr and has_liq:
            if fv.get("trade_value_24h") is not None and str(fv.get("trade_value_24h")).strip() != "":
                try:
                    tv24_usdt = float(fv["trade_value_24h"])
                except (TypeError, ValueError):
                    tv24_usdt = 0.0
            else:
                tv24_usdt = float(fv.get("marcap_eok", 0) or 0) * 100_000_000.0
            if tv24_usdt < min_tv24_usdt:
                conn.close()
                return False, (
                    f"🛑 시드 비대화 슬리피지 방어: [{core_group}] 복리시드 "
                    f"{group_current_seed:,.0f} USDT > {seed_slip_thr:,.0f} USDT 인데 "
                    f"24h 거래대금 {tv24_usdt:,.0f} USDT < {min_tv24_usdt:,.0f} USDT "
                    f"(trade_value_24h·marcap_eok 기준 소형 종목 차단)"
                )

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

    if is_incubator_shadow:
        # 인큐베이터 섀도우 트레이딩: 국고 손실 원천 차단 (가상 기록만 유지)
        margin_used = 0.0
        sim_kelly_invest = 0.0

    quantity = sim_kelly_invest / float(entry_price) if float(entry_price) > 0 else 0.0
    if quantity <= 0 and not is_incubator_shadow:
        conn.close()
        return False, "수량 산출 실패"
    if is_incubator_shadow:
        quantity = 0.0

    fixed_qty = float((group_current_seed * fixed_risk_pct) / risk_distance)
    fixed_notional = fixed_qty * float(entry_price)
    if market_type == "futures":
        fixed_notional *= leverage
    _ = fixed_notional

    now = datetime.utcnow().strftime("%Y-%m-%d")
    entry_cos_score = float(max_alpha_cos_effective * 100.0)
    entry_dtw_score = float(facts.get("dtw_score", facts.get("entry_dtw_score", 0.0)) or 0.0)
    fr0 = 0.0
    fts0 = ""
    acc0 = 0.0
    if str(market_type).lower() == "futures":
        try:
            _s = fetch_funding_snapshot(symbol)
            if _s:
                fr0 = float(_s.get("funding_rate") or 0.0)
                fts0 = str(_s.get("next_funding_iso") or _s.get("next_funding_ts") or "").strip()
        except Exception:
            pass
    cur.execute(
        """
        INSERT INTO bitget_forward_trades
        (entry_date, market_type, symbol, timeframe, sig_type, tier, total_score, dyn_rs, dyn_cpv, dyn_tb,
         entry_price, position_side, entry_atr, entry_high, atr_sl_mult, stop_price, leverage, tf_weight, sim_kelly_risk_pct, margin_used,
         sim_kelly_invest, quantity, entry_cos_score, entry_dtw_score, v_cpv, v_yang, v_energy, v_rs, max_high, min_low, status,
         sim_stat_status, sim_tech_status, sim_breadth_status, entry_breadth, live_a_status, cand_b_status, champ_c_status,
         funding_rate_last, funding_next_settle_ts, funding_accum_usdt_est)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            now,
            market_type,
            symbol,
            tf,
            sig_type_row,
            tier_label,
            float(score),
            float(facts.get("dyn_rs", 0)),
            float(facts.get("dyn_cpv", 0)),
            float(facts.get("dyn_tb", 0)),
            float(entry_price),
            position_side,
            round(entry_atr, 6),
            entry_high_val,
            atr_sl_mult,
            float(stop_price),
            leverage,
            tf_weight,
            float(kelly_risk_pct),
            float(margin_used),
            float(sim_kelly_invest),
            float(quantity),
            entry_cos_score,
            entry_dtw_score,
            float(facts.get("v_cpv", 0)),
            float(facts.get("v_yang", 0)),
            float(facts.get("v_energy", 0)),
            float(facts.get("v_rs", 0)),
            float(entry_price),
            float(entry_price),
            "OPEN",
            "OPEN",
            "OPEN",
            "OPEN",
            float(breadth_now),
            "OPEN",
            "OPEN",
            "OPEN",
            fr0,
            fts0,
            acc0,
        ),
    )
    satellite_tags = None
    try:
        import bitget_shadow_tracking
        satellite_tags = bitget_shadow_tracking.build_satellite_tags(cfg)
    except Exception:
        satellite_tags = None
    if satellite_tags is not None:
        try:
            import bitget_shadow_tracking
            logged_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            bitget_shadow_tracking.insert_virtual_trade_row(
                cur,
                market_type,
                symbol,
                symbol,
                float(entry_price),
                sig_type_row,
                str(satellite_tags),
                logged_at,
                position_side=position_side,
                timeframe=tf,
            )
        except Exception:
            pass
    conn.commit()
    # DB INSERT/COMMIT 성공 이후에만 국고 차감 저장 (자금 증발 방지)
    cfg[treasury_key] = max(0.0, treasury_balance - margin_used)
    save_system_config(cfg)
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
        "hist_df": _load_hist(conn, market_type, symbol, timeframe, limit=300),
    }


def _floating_pnl_usdt_open_row(conn, r) -> float:
    """OPEN 한 줄 기준 현재 평가 USDT 손익(주식 sim_kelly_invest·수익률 곱 패턴과 동일). 양수/음수 반환."""
    latest = _get_latest_bar(conn, r["market_type"], r["symbol"], r["timeframe"])
    if latest is None:
        return 0.0
    ep = float(r["entry_price"] or 0.0)
    if ep <= 0:
        return 0.0
    c = float(latest["close"])
    pos_side = str(r.get("position_side", "LONG")).upper()
    if pos_side == "SHORT":
        current_ret_pct = ((ep - c) / ep) * 100.0
    else:
        current_ret_pct = ((c - ep) / ep) * 100.0
    notion = float(r.get("sim_kelly_invest", 0.0) or 0.0)
    if notion <= 0.0:
        margin_used = float(r.get("margin_used", 0.0) or 0.0)
        lev = float(r.get("leverage", 1.0) or 1.0)
        mkt = str(r.get("market_type", "")).lower()
        if margin_used > 0 and mkt == "futures" and lev > 0:
            notion = margin_used * lev
        else:
            notion = margin_used
    return float(notion) * (current_ret_pct / 100.0)


def _aggregate_global_open_loss_usdt(conn) -> tuple[float, int]:
    """
    status=OPEN 인 전 종목 미실현 손익 중 손실분만 합산 (양수 포지션 PnL은 제외 → 주식 total_open_loss_amount 와 동일).
    반환: (total_open_loss_amount, open_count).
    """
    df_open = pd.read_sql(
        "SELECT * FROM bitget_forward_trades WHERE status='OPEN'",
        conn,
    )
    total_open_loss_amount = 0.0
    for _, row in df_open.iterrows():
        pnl = _floating_pnl_usdt_open_row(conn, row)
        if pnl < 0:
            total_open_loss_amount += pnl
    return total_open_loss_amount, len(df_open)


def _finalize_global_circuit_breaker_track(conn, cfg):
    """OPEN 전역 미실현 손실 기준 글로벌 서킷 ON + 커밋.(주식 track_daily_positions 패턴)"""
    base_seed = float(cfg.get("ACCOUNT_SIZE_USDT", 100000.0) or 0.0)
    total_open_loss_amount, n_open_global = _aggregate_global_open_loss_usdt(conn)
    conn.commit()
    if base_seed > 0:
        loss_ratio = total_open_loss_amount / base_seed
        if loss_ratio <= -0.05:
            latest_config = load_system_config()
            if str(latest_config.get("GLOBAL_CIRCUIT_BREAKER", "OFF")).upper() != "ON":
                latest_config["GLOBAL_CIRCUIT_BREAKER"] = "ON"
                latest_config["GLOBAL_CIRCUIT_BREAKER_TRIGGERED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                latest_config["GLOBAL_CIRCUIT_BREAKER_LAST_LOSS_RATIO"] = round(float(loss_ratio), 6)
                save_system_config(latest_config)
                send_telegram_msg(
                    f"🚨 <b>[GLOBAL CIRCUIT BREAKER — Bitget]</b>\n"
                    f"▪ 미실현 손실 합(OPEN만): <b>{total_open_loss_amount:,.2f} USDT</b>\n"
                    f"▪ 기준 시드(ACCOUNT_SIZE_USDT): <b>{base_seed:,.2f} USDT</b>\n"
                    f"▪ 손실/시드: <b>{loss_ratio * 100:.2f}%</b> (한계 ≤-5.0%)\n"
                    f"▪ 현재 OPEN 수: <b>{n_open_global}</b>\n"
                    f"조치: <code>GLOBAL_CIRCUIT_BREAKER=ON</code> — 신규 진입 전면 차단."
                )


def _days_since_entry_date(entry_date_val):
    try:
        if entry_date_val is None:
            return None
        s = str(entry_date_val).strip()[:10]
        if len(s) < 10:
            return None
        ed = datetime.strptime(s, "%Y-%m-%d").date()
        return (datetime.utcnow().date() - ed).days
    except Exception:
        return None


def _force_close_zombie_delist_or_halt(conn, r):
    """
    DB/캔들 단절이 14일+ 지속되면 상장폐지·장기 거래정지로 간주하고 좀비 포지션 강제 청산 + 국고 환입.
    """
    ret = -100.0
    exit_rsn = "상폐/거래정지 강제청산"
    exit_type = "DELIST_OR_HALT"
    exit_d = datetime.utcnow().strftime("%Y-%m-%d")
    ep = float(r.get("entry_price") or 0.0)
    new_max = float(r.get("max_high") or ep)
    new_min = float(r.get("min_low") or ep)
    new_bars = int(r.get("bars_held") or 0)
    new_up = float(r.get("up_vol_sum") or 0.0)
    new_down = float(r.get("down_vol_sum") or 0.0)
    eb = float(r.get("entry_breadth") or 1.0)
    flow_tags = "#상폐_거래정지_좀비해제"
    neg = float(ret)
    update_sql = """
        UPDATE bitget_forward_trades
        SET status='CLOSED_LOSS', exit_date=?, exit_reason=?, final_ret=?, mfe=?,
            max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?,
            exit_type=?,
            sim_stat_ret=?, sim_stat_status='CLOSED_LOSS',
            sim_tech_ret=?, sim_tech_status='CLOSED_LOSS',
            sim_breadth_ret=?, sim_breadth_status='CLOSED_LOSS',
            entry_breadth=?,
            live_a_ret=?, live_a_status='CLOSED_LOSS',
            cand_b_ret=?, cand_b_status='CLOSED_LOSS',
            champ_c_ret=?, champ_c_status='CLOSED_LOSS',
            flow_tags=?
        WHERE id=?
    """
    params = (
        exit_d,
        exit_rsn,
        ret,
        0.0,
        new_max,
        new_min,
        new_bars,
        new_up,
        new_down,
        exit_type,
        neg,
        neg,
        neg,
        eb,
        neg,
        neg,
        neg,
        flow_tags,
        int(r["id"]),
    )
    max_retry = 5
    for attempt in range(max_retry):
        try:
            conn.execute(update_sql, params)
            break
        except sqlite3.OperationalError as e:
            em = str(e).lower()
            if "database is locked" in em:
                if attempt >= max_retry - 1:
                    raise
                wait_s = 0.5 * (2 ** attempt)
                print(
                    f"⏳ [DB LOCK 재시도] 좀비청산 {r['symbol']} #{attempt + 1}/{max_retry} wait={wait_s:.2f}s"
                )
                time.sleep(wait_s)
                continue
            raise

    treasury_key = "TREASURY_SPOT_USDT" if r["market_type"] == "spot" else "TREASURY_FUTURES_USDT"
    cur_cfg = load_system_config()
    before = float(cur_cfg.get(treasury_key, 0.0))
    margin_used = float(r.get("margin_used", 0.0) or 0.0)
    pnl = float(r.get("sim_kelly_invest", 0.0) or 0.0) * (ret / 100.0)
    cur_cfg[treasury_key] = max(0.0, before + margin_used + pnl)
    save_system_config(cur_cfg)

    send_telegram_msg(
        f"☠️ <b>[좀비 해제]</b> {str(r['market_type']).upper()} <code>{r['symbol']}</code> #{r['timeframe']}\n"
        f"▪ {exit_rsn} (진입 후 14일+ 데이터 단절)\n"
        f"▪ final_ret <b>{ret}%</b> · 국고 환입 반영 ({treasury_key})"
    )


def track_daily_positions(market_type):
    init_forward_db()
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    cfg = load_system_config()
    df_active = pd.read_sql(
        "SELECT * FROM bitget_forward_trades WHERE market_type=? AND status='OPEN'",
        conn,
        params=(str(market_type).lower(),),
    )
    if df_active.empty:
        print(f"\n🔍 [포워드 테스팅] {market_type} OPEN 0건 — 글로브 손실/서킷만 점검")
        try:
            _finalize_global_circuit_breaker_track(conn, cfg)
        finally:
            conn.close()
        return

    breadth_now = _calc_market_breadth(conn)
    # 주식 auto_forward_tester: breadth < 0.97 → MAE 손절·타임스탑 0.5배 비상 조임
    breadth_collapse_tightening = breadth_now < 0.97
    if breadth_collapse_tightening:
        print(
            f"🛡️ [포워드 Bitget] 시장 폭 붕괴 연동 (breadth={breadth_now:.3f} < 0.97): "
            f"기보유 MAE 손절선·타임스탑 0.5배 타이트닝"
        )

    print(f"\n🔍 [포워드 테스팅] {market_type} {len(df_active)}개 포지션 추적 중...")

    for _, r in df_active.iterrows():
        try:
            days_in_pos = _days_since_entry_date(r.get("entry_date"))
            latest = _get_latest_bar(conn, r["market_type"], r["symbol"], r["timeframe"])
            hist_df = latest.get("hist_df") if latest is not None else None
            data_insufficient = latest is None or hist_df is None or len(hist_df) < 20
            if data_insufficient:
                if days_in_pos is not None and days_in_pos >= 14:
                    _force_close_zombie_delist_or_halt(conn, r)
                continue

            c = latest["close"]
            o = latest["open"]
            h = latest["high"]
            l = latest["low"]
            v = latest["vol"]
            ep = float(r["entry_price"])
            if ep <= 0:
                continue
            pos_side = str(r.get("position_side", "LONG")).upper()
            if pos_side == "SHORT":
                current_ret_pct = ((ep - c) / ep) * 100.0
                low_ret_pct = ((ep - h) / ep) * 100.0   # SHORT: high = max loss
                high_ret_pct = ((ep - l) / ep) * 100.0  # SHORT: low = max profit
            else:
                current_ret_pct = ((c - ep) / ep) * 100.0
                low_ret_pct = ((l - ep) / ep) * 100.0
                high_ret_pct = ((h - ep) / ep) * 100.0

            new_max = max(float(r["max_high"]), h)
            new_min = min(float(r["min_low"]), l)
            new_bars = int(r["bars_held"]) + 1
            new_up_vol = float(r["up_vol_sum"]) + (v if c > o else 0.0)
            new_down_vol = float(r["down_vol_sum"]) + (v if c < o else 0.0)

            is_futures_row = str(r["market_type"]).lower() == "futures"
            snap = _cached_funding_snapshot(str(r["symbol"])) if is_futures_row else None
            notion_open = float(r.get("sim_kelly_invest", 0.0) or 0.0)
            if notion_open <= 0:
                mu_o = float(r.get("margin_used", 0.0) or 0.0)
                lev_o = float(r.get("leverage", 1.0) or 1.0)
                if mu_o > 0 and is_futures_row and lev_o > 0:
                    notion_open = mu_o * lev_o
                else:
                    notion_open = mu_o
            period_h_fund = float(cfg.get("FUNDING_INTERVAL_HOURS_DEFAULT", 8.0))
            tf_hours_fund_map = {"1H": 1.0, "2H": 2.0, "4H": 4.0, "1D": 24.0}
            tf_hours_fund = float(tf_hours_fund_map.get(str(r["timeframe"]).upper(), 4.0))
            rate_used = float(snap.get("funding_rate") or 0.0) if snap else float(r.get("funding_rate_last", 0) or 0)
            prev_accum = float(r.get("funding_accum_usdt_est", 0) or 0)
            accum_fund = prev_accum
            fts_store = str(r.get("funding_next_settle_ts", "") or "")
            if snap:
                fts_store = str(snap.get("next_funding_iso") or snap.get("next_funding_ts") or "").strip()
                rate_used = float(snap.get("funding_rate") or 0.0)
            fr_row = rate_used
            if is_futures_row and period_h_fund > 1e-9 and notion_open > 0:
                if pos_side == "LONG":
                    accum_fund = prev_accum - notion_open * rate_used * (tf_hours_fund / period_h_fund)
                else:
                    accum_fund = prev_accum + notion_open * rate_used * (tf_hours_fund / period_h_fund)

            hist = hist_df
            cur_atr = _calc_atr14(hist)
            # 주식 forward_trades와 동형: 백필 시 entry_atr을 DB에 박제 — 캔들마다 cur_atr로 손절선이 밀리는 고무줄 버그 방지
            entry_atr = r.get("entry_atr", 0.0)
            if entry_atr == 0.0 or pd.isna(entry_atr):
                entry_atr = float(cur_atr)
                conn.execute(
                    "UPDATE bitget_forward_trades SET entry_atr=? WHERE id=?",
                    (round(entry_atr, 6), int(r["id"])),
                )
            else:
                entry_atr = float(entry_atr)

            hist["ema10"] = hist["Close"].ewm(span=10, adjust=False).mean()
            hist["ema20"] = hist["Close"].ewm(span=20, adjust=False).mean()
            hist["ema34"] = hist["Close"].ewm(span=34, adjust=False).mean()
            hist["ema60"] = hist["Close"].ewm(span=60, adjust=False).mean()
            hist["ema75"] = hist["Close"].ewm(span=75, adjust=False).mean()
            hist["ema160"] = hist["Close"].ewm(span=160, adjust=False).mean()
            z_ema1 = hist["Close"].ewm(span=20, adjust=False).mean()
            z_ema2 = z_ema1.ewm(span=20, adjust=False).mean()
            cur_zlema = float((z_ema1 + (z_ema1 - z_ema2)).iloc[-1])
            ema10_now = float(hist["ema10"].iloc[-1])
            ema20_now = float(hist["ema20"].iloc[-1])
            ema10_prev = float(hist["ema10"].iloc[-2])
            ema20_prev = float(hist["ema20"].iloc[-2])
            is_tech_exit_long = (c < cur_zlema) or (ema10_now < ema20_now and ema10_prev >= ema20_prev)
            is_tech_exit_short = (c > cur_zlema) or (ema10_now > ema20_now and ema10_prev <= ema20_prev)
            is_tech_exit = is_tech_exit_short if pos_side == "SHORT" else is_tech_exit_long

            entry_breadth = float(r.get("entry_breadth", 1.0) or 1.0)
            breadth_delta = breadth_now - entry_breadth
            breadth_collapse = breadth_now < 0.95 and breadth_delta < -0.03

            ns_prefix = f"{str(r['timeframe']).upper()}_LIVE_PARAMS"
            live_params = cfg.get(ns_prefix, {"DYNAMIC_MAE_SL": -3.5, "DYNAMIC_MFE_TP": 10.0})
            dyn_mae_sl = float(live_params.get("DYNAMIC_MAE_SL", -3.5))
            dyn_mfe_tp = float(live_params.get("DYNAMIC_MFE_TP", 10.0))
            opt_time_stop = int(cfg.get(f"{str(r['timeframe']).upper()}_TIME_STOP", 10))
            # 주식 패턴: 시장폭 붕괴(<0.97) 또는 포지션 단위 폭 급변 → MAE·타임스탑 절반
            if breadth_collapse_tightening or breadth_collapse:
                dyn_mae_sl *= 0.5
                opt_time_stop = max(1, int(round(float(opt_time_stop) * 0.5)))
            opt_sl_atr = float(r["atr_sl_mult"] or cfg.get("ATR_SL_MULT", 2.0))
            if pos_side == "SHORT":
                sl_price = ep + (opt_sl_atr * entry_atr)
            else:
                sl_price = ep - (opt_sl_atr * entry_atr)

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
                tf_u = str(r["timeframe"]).upper()
                funding_stop_bars_map = {"1H": 24, "2H": 18, "4H": 12, "1D": 5}
                funding_stop_bars = int(funding_stop_bars_map.get(tf_u, 12))
                is_futures = is_futures_row
                bleed_max_profit = float(cfg.get("FUNDING_BLEED_MAX_ROE_PCT", 1.5))
                th_fb = float(cfg.get("FUNDING_BLEED_RATE_THRESHOLD", 0.0003))
                funding_bleed = False
                if is_futures and snap is not None and new_bars >= funding_stop_bars and current_ret_pct < bleed_max_profit:
                    rate_api = float(snap.get("funding_rate") or 0.0)
                    if pos_side == "LONG" and rate_api > th_fb:
                        funding_bleed = True
                    elif pos_side == "SHORT" and rate_api < -th_fb:
                        funding_bleed = True
                if funding_bleed:
                    do_exit = True
                    exit_rsn = (
                        f"펀딩비(API) 불리 출혈 방어 rate={float(snap.get('funding_rate') or 0):.6f} "
                        f"next={str(snap.get('next_funding_iso') or snap.get('next_funding_ts') or '').strip()} "
                        f"({tf_u} ≥{funding_stop_bars} bars)"
                    )
                    actual_exit_type = "FUNDING_BLEED_STOP"
                elif new_bars >= opt_time_stop and current_ret_pct < 3.0:
                    do_exit = True
                    exit_rsn = f"타임스탑 ({opt_time_stop} bars)"
                    actual_exit_type = "TIME_STOP"
                elif breadth_collapse and current_ret_pct < 1.5:
                    do_exit = True
                    exit_rsn = f"시장폭 붕괴 청산 (entry {entry_breadth:.3f} -> now {breadth_now:.3f})"
                    actual_exit_type = "BREADTH_EXIT"
                elif (h >= sl_price if pos_side == "SHORT" else l <= sl_price):
                    do_exit = True
                    exit_rsn = f"ATR {opt_sl_atr:.2f}배 장중 방어 손절"
                    actual_exit_type = "ATR_STOP"
                    actual_exit_price = sl_price
                elif is_tech_exit:
                    do_exit = True
                    exit_rsn = "기술적 추세 이탈 (ZLEMA/EMA10-20 데드)"
                    actual_exit_type = "TECH_EXIT"

            # SHORT 전용 즉시청산/트레일링 익절 (Pine Script)
            if not do_exit and pos_side == "SHORT":
                ema34_now = float(hist["ema34"].iloc[-1])
                ema60_now = float(hist["ema60"].iloc[-1])
                ema75_now = float(hist["ema75"].iloc[-1])
                ema160_now = float(hist["ema160"].iloc[-1])
                ema34_prev = float(hist["ema34"].iloc[-2]) if len(hist) >= 2 else ema34_now
                ema60_prev = float(hist["ema60"].iloc[-2]) if len(hist) >= 2 else ema60_now
                ema75_prev = float(hist["ema75"].iloc[-2]) if len(hist) >= 2 else ema75_now
                ema160_prev = float(hist["ema160"].iloc[-2]) if len(hist) >= 2 else ema160_now

                cross_up_ema160 = (latest["prev_close"] <= ema160_prev) and (c > ema160_now)
                entry_high = float(r.get("entry_high", 0.0) or 0.0)
                break_entry_high = entry_high > 0 and c > entry_high

                if cross_up_ema160 or break_entry_high:
                    do_exit = True
                    actual_exit_type = "SHORT_PINE_STOP"
                    if cross_up_ema160 and break_entry_high:
                        actual_exit_price = max(ema160_now, entry_high)
                        exit_rsn = "숏 즉시손절: EMA160 상향돌파 + entry_high 돌파 (Pine)"
                    elif cross_up_ema160:
                        actual_exit_price = ema160_now
                        exit_rsn = "숏 즉시손절: EMA160 상향돌파 (Pine)"
                    else:
                        actual_exit_price = entry_high
                        exit_rsn = "숏 즉시손절: entry_high 돌파 (Pine)"
                else:
                    cross_up_ema34 = (latest["prev_close"] <= ema34_prev) and (c > ema34_now)
                    cross_up_ema60 = (latest["prev_close"] <= ema60_prev) and (c > ema60_now)
                    cross_up_ema75 = (latest["prev_close"] <= ema75_prev) and (c > ema75_now)
                    if cross_up_ema34:
                        do_exit = True
                        actual_exit_type = "SHORT_TRAIL_TP_34"
                        exit_rsn = "숏 트레일링 익절: EMA34 상향 돌파 (Pine)"
                    elif cross_up_ema60:
                        do_exit = True
                        actual_exit_type = "SHORT_TRAIL_TP_60"
                        exit_rsn = "숏 트레일링 익절: EMA60 상향 돌파 (Pine)"
                    elif cross_up_ema75:
                        do_exit = True
                        actual_exit_type = "SHORT_TRAIL_TP_75"
                        exit_rsn = "숏 트레일링 익절: EMA75 상향 돌파 (Pine)"

            # 🚨 [코인 생태계 특화] 레버리지 감안 강제 청산 (ROE -100% 이하 도달 시)
            leverage = float(r.get("leverage", 1.0) or 1.0)
            if leverage > 0 and (current_ret_pct * leverage) <= -100.0:
                do_exit, exit_rsn, actual_exit_type = True, f"레버리지({leverage}x) 한도 초과 강제청산", "LIQUIDATION"
                actual_exit_price = ep * (1.0 + (100.0 / leverage / 100.0)) if pos_side == "SHORT" else ep * (1.0 - (100.0 / leverage / 100.0))

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
                if pos_side == "SHORT":
                    ret = round(((ep - actual_exit_price) / ep) * 100.0, 2)
                else:
                    ret = round(((actual_exit_price - ep) / ep) * 100.0, 2)
                if pos_side == "SHORT":
                    mfe = round(((ep - new_min) / ep) * 100.0, 2)
                else:
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

                # 🧟 [핵심 추가] 언더독(0~60점대) 전용 정밀 부검 꼬리표 부착
                if float(r.get("total_score", 100)) <= 60.0:
                    _rs = float(r.get("dyn_rs", 0) or r.get("v_rs", 0))
                    _eng = float(r.get("v_energy", 0) or 0)
                    _cpv = float(r.get("dyn_cpv", 0) or r.get("v_cpv", 0))

                    if ret > 0 or mfe >= 10.0:  # 수익으로 마감했거나 장중 10% 이상 대시세를 준 경우
                        if _rs < 0:
                            tags.append("#저득점_역배열_반등성공")
                        elif _rs > 30:
                            tags.append("#저득점_이격과다_추가폭발")

                        if _eng > 15.0:
                            tags.append("#저득점_수급깡패_성공")
                    else:  # 손실 마감 (참사주)
                        if _cpv > 0.75:
                            tags.append("#저득점_윗꼬리_참사")
                        elif vol_ratio < 0.6:
                            tags.append("#저득점_투매_수급붕괴")

                flow_tags = " ".join(tags)

                update_sql = """
                    UPDATE bitget_forward_trades
                    SET status=?, exit_date=?, exit_reason=?, final_ret=?, mfe=?, max_high=?, min_low=?, bars_held=?,
                        up_vol_sum=?, down_vol_sum=?, exit_type=?, sim_stat_ret=?, sim_stat_status=?, sim_tech_ret=?, sim_tech_status=?,
                        sim_breadth_ret=?, sim_breadth_status=?, entry_breadth=?, live_a_ret=?, live_a_status=?, cand_b_ret=?, cand_b_status=?, champ_c_ret=?, champ_c_status=?, flow_tags=?
                    WHERE id=?
                """
                update_params = (
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
                )

                max_retry = 5
                for attempt in range(max_retry):
                    try:
                        conn.execute(update_sql, update_params)
                        break
                    except sqlite3.OperationalError as e:
                        em = str(e).lower()
                        if "database is locked" in em:
                            if attempt >= max_retry - 1:
                                raise
                            wait_s = 0.5 * (2 ** attempt)
                            print(
                                f"⏳ [DB LOCK 재시도] {r['symbol']} #{attempt + 1}/{max_retry} "
                                f"wait={wait_s:.2f}s"
                            )
                            time.sleep(wait_s)
                            continue
                        raise

                # 국고 환입
                treasury_key = "TREASURY_SPOT_USDT" if r["market_type"] == "spot" else "TREASURY_FUTURES_USDT"
                cur_cfg = load_system_config()
                before = float(cur_cfg.get(treasury_key, 0.0))
                margin_used = float(r.get("margin_used", 0.0) or 0.0)
                raw_pnl = float(r.get("sim_kelly_invest", 0.0) or 0.0) * (ret / 100.0)
                # 💡 [레버리지 강제청산 방어] 잃을 수 있는 최대 금액은 투입한 증거금(margin_used)으로 철저히 제한
                pnl = max(-margin_used, raw_pnl)
                cur_cfg[treasury_key] = max(0.0, before + margin_used + pnl)
                save_system_config(cur_cfg)

                icon = "🔥스마트청산" if ret > 0 else "🛡️방어손절"
                send_telegram_msg(
                    f"🤖 [{str(r['market_type']).upper()} 관제탑] {icon}: {r['symbol']} ({r['sig_type']} | {round(float(r['total_score']),1)}점)\n"
                    f"▪️ 수익: {ret}%\n▪️ 사유: {exit_rsn}\n▪️ 태그: {flow_tags}"
                )
            else:
                update_sql = """
                    UPDATE bitget_forward_trades
                    SET max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?,
                        sim_stat_ret=?, sim_stat_status=?, sim_tech_ret=?, sim_tech_status=?,
                        sim_breadth_ret=?, sim_breadth_status=?, entry_breadth=?,
                        live_a_ret=?, live_a_status=?, cand_b_ret=?, cand_b_status=?, champ_c_ret=?, champ_c_status=?,
                        funding_rate_last=?, funding_next_settle_ts=?, funding_accum_usdt_est=?
                    WHERE id=?
                """
                update_params = (
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
                    fr_row,
                    fts_store,
                    accum_fund,
                    int(r["id"]),
                )

                max_retry = 5
                for attempt in range(max_retry):
                    try:
                        conn.execute(update_sql, update_params)
                        break
                    except sqlite3.OperationalError as e:
                        em = str(e).lower()
                        if "database is locked" in em:
                            if attempt >= max_retry - 1:
                                raise
                            wait_s = 0.5 * (2 ** attempt)
                            print(
                                f"⏳ [DB LOCK 재시도] {r['symbol']} #{attempt + 1}/{max_retry} "
                                f"wait={wait_s:.2f}s"
                            )
                            time.sleep(wait_s)
                            continue
                        raise
        except Exception as e:
            try:
                print(f"🚨 [청산 추적 에러] {r['symbol']}: {e}")
            except Exception:
                print(f"🚨 [청산 추적 에러] unknown_symbol: {e}")
            continue

    try:
        _finalize_global_circuit_breaker_track(conn, cfg)
    finally:
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
    today = datetime.utcnow().strftime("%Y-%m-%d")
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
            "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
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


def send_group_practitioner_reports():
    """
    코인 실무자 30명 개별 리포트 (spot/futures 분리).
    최근 청산 데이터를 30개 그룹으로 나눠 각 그룹 성과를 발송한다.
    """
    init_forward_db()
    sync_real_leaderboard_with_virtual()
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    try:
        for market_type in ["spot", "futures"]:
            df = pd.read_sql(
                """
                SELECT symbol, sig_type, total_score, final_ret, mfe, exit_type, position_side
                FROM bitget_forward_trades
                WHERE market_type=? AND status LIKE 'CLOSED%'
                  AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
                ORDER BY id DESC
                LIMIT 1200
                """,
                conn,
                params=(market_type,),
            )
            if df.empty:
                continue

            df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0)
            df["mfe"] = pd.to_numeric(df["mfe"], errors="coerce").fillna(0.0)
            df["total_score"] = pd.to_numeric(df["total_score"], errors="coerce").fillna(0.0)
            # 💡 [버그 픽스] 임의의 점수 등분이 아닌, 실제 실무자(Engine) 명찰을 기준으로 그룹화
            def _get_prac_key(sig):
                import re
                m = re.search(r"(PRACT_\d{2})", str(sig), re.IGNORECASE)
                return m.group(1).upper() if m else "UNKNOWN"
            
            df["practitioner_key"] = df["sig_type"].apply(_get_prac_key)
            prac_keys = sorted([k for k in df["practitioner_key"].unique() if k != "UNKNOWN"])

            icon = "🟢" if market_type == "spot" else "🟠"
            for p_key in prac_keys:
                g = df[df["practitioner_key"] == p_key].copy()
                if g.empty: continue
                wins = int((g["final_ret"] > 0).sum())
                total = int(len(g))
                wr = (wins / total) * 100.0 if total > 0 else 0.0
                pf = _pf(g["final_ret"])
                avg_ret = float(g["final_ret"].mean()) if total > 0 else 0.0
                avg_mfe = float(g["mfe"].mean()) if total > 0 else 0.0
                top_row = g.sort_values("final_ret", ascending=False).iloc[0]
                # 동일 실무자 키의 실전 로그를 병합해 "입실력 vs 실전실력" 비교
                prac_key = str(p_key)
                real_df = pd.read_sql(
                    """
                    SELECT realized_ret_pct, virtual_final_ret, notional_usdt, exec_ok, is_dry_run
                    FROM bitget_real_execution
                    WHERE market_type=? AND practitioner_key=?
                    ORDER BY id DESC
                    LIMIT 500
                    """,
                    conn,
                    params=(market_type, prac_key),
                )
                real_ret = 0.0
                real_notion = 0.0
                real_samples = 0
                if not real_df.empty:
                    real_df["realized_ret_pct"] = pd.to_numeric(real_df["realized_ret_pct"], errors="coerce").fillna(0.0)
                    real_df["notional_usdt"] = pd.to_numeric(real_df["notional_usdt"], errors="coerce").fillna(0.0)
                    real_ret = float(real_df["realized_ret_pct"].mean())
                    real_notion = float(real_df["notional_usdt"].sum())
                    real_samples = int(len(real_df))

                msg = (
                    f"{icon} <b>[{market_type.upper()} 실무자 {prac_key}]</b>\n"
                    f"▪️ 성적: 승률 {wr:.1f}% | PF {pf:.2f}\n"
                    f"▪️ 평균: RET {avg_ret:+.2f}% | MFE {avg_mfe:+.2f}% | 표본 {total}\n"
                    f"▪️ 실전: RET {real_ret:+.2f}% | 노셔널 {real_notion:,.1f} USDT | 체결 {real_samples}\n"
                    f"▪️ 괴리: 실전-가상 {real_ret - avg_ret:+.2f}pp\n"
                    f"▪️ 대표 트레이드: {top_row['symbol']} ({top_row['position_side']}) {float(top_row['final_ret']):+.2f}%"
                )
                send_telegram_msg(msg)
                time.sleep(0.35)
    finally:
        conn.close()


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
    # 일일 종합 리포트와 실무자 30인 개별 리포트를 연동 실행
    try:
        send_group_practitioner_reports()
    except Exception as e:
        send_telegram_msg(f"⚠️ practitioner report error: {e}")


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

        cfg = load_system_config()

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

        # ANTI_PATTERNS 누적: -10% 이하 참사주의 4D DNA를 면역 메모리로 저장
        fatal_df = df[pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) <= -10.0].copy()
        if not fatal_df.empty:
            anti_patterns = cfg.get("ANTI_PATTERNS", [])
            if not isinstance(anti_patterns, list):
                anti_patterns = []
            for _, row in fatal_df.iterrows():
                anti_patterns.append(
                    {
                        "market_type": str(market_type).lower(),
                        "symbol": str(row.get("symbol", "")),
                        "sig_type": str(row.get("sig_type", "")),
                        "dyn_cpv": float(row.get("dyn_cpv", 0.0) or 0.0),
                        "dyn_tb": float(row.get("dyn_tb", 0.0) or 0.0),
                        "v_energy": float(row.get("v_energy", 0.0) or 0.0),
                        "dyn_rs": float(row.get("dyn_rs", 0.0) or 0.0),
                        "final_ret": float(row.get("final_ret", 0.0) or 0.0),
                        "recorded_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
            # 폭주 방지: 최신 500개만 유지
            cfg["ANTI_PATTERNS"] = anti_patterns[-500:]
            report_msg += f"\n🧬 [ANTI_PATTERNS] 참사주 DNA {len(fatal_df)}건 누적 저장 완료\n"

        # 딥다이브 결과를 실제 설정값으로 반영하는 자율 뇌수술(Brain Surgery)
        cfg, tune_msgs = _auto_tune_brain_from_closed_df(cfg, df)
        save_system_config(cfg)
        if tune_msgs:
            report_msg += "\n🧠 [자율 튜닝 적용]\n"
            for m in tune_msgs:
                report_msg += f"▪️ {m}\n"

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
    try:
        meta_state_coin = load_meta_state_resolved()
    except Exception:
        meta_state_coin = {}
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
            block_coin = build_macro_treasury_block(
                meta=meta_state_coin,
                sys_config=cfg,
                df_closed_real=df_closed,
                treasury_config_key=treasury_key,
                ledger_zero_invest_fallback=None,
            )
            msg1 = format_macro_treasury_section_html(
                block_coin,
                display_label=market_type.upper(),
                market_icon=m_icon,
                today_str=today_str,
                lead_in_html="",
                currency_suffix="USDT",
                amount_decimals=2,
            )
            send_telegram_msg(msg1)
            time.sleep(1)

            msg2 = f"{m_icon} <b>[2/9] 실전-가상 동기화 리더보드</b>\n"
            lb = build_practitioner_reality_leaderboard(market_type=market_type, limit_rows=12)
            if lb is not None and not lb.empty:
                msg2 += "🏆 <b>실전 수익률 TOP 7</b>\n"
                top_real = lb.sort_values(["real_ret_pct", "reality_score", "samples"], ascending=[False, False, False]).head(7)
                for i, row in top_real.iterrows():
                    rank = i + 1
                    medal = "🥇" if rank == 1 else ("🥈" if rank == 2 else ("🥉" if rank == 3 else "🏃"))
                    msg2 += (
                        f"{medal} <b>{row['practitioner_key']}</b>: "
                        f"실전 {float(row['real_ret_pct']):+,.2f}% | "
                        f"가상 {float(row['virtual_ret_pct']):+,.2f}% | "
                        f"괴리 {float(row['reality_gap_pct']):+,.2f}pp | "
                        f"노셔널 {float(row['notional_usdt']):,.0f}U\n"
                    )
                worst_gap = lb.sort_values(["reality_gap_pct", "samples"], ascending=[True, False]).head(3)
                if not worst_gap.empty:
                    msg2 += "\n🧪 <b>입실력 과장(실전-가상 괴리 Worst 3)</b>\n"
                    for _, row in worst_gap.iterrows():
                        msg2 += (
                            f" - {row['practitioner_key']}: "
                            f"{float(row['reality_gap_pct']):+,.2f}pp "
                            f"(실전 {float(row['real_ret_pct']):+,.2f}% vs 가상 {float(row['virtual_ret_pct']):+,.2f}%)\n"
                        )
            else:
                msg2 += " ↳ 실전-가상 동기화 데이터 부족\n"
            send_telegram_msg(msg2)
            time.sleep(1)

            # 가상 장부 기준(기존)
            kelly_pnl = float(
                (
                    pd.to_numeric(df_closed.get("sim_kelly_invest", 0.0), errors="coerce").fillna(0.0)
                    * pd.to_numeric(df_closed.get("final_ret", 0.0), errors="coerce").fillna(0.0)
                    / 100.0
                ).sum()
            ) if not df_closed.empty else 0.0
            fixed_pnl = float(
                (
                    pd.to_numeric(df_closed.get("margin_used", 0.0), errors="coerce").fillna(0.0)
                    * pd.to_numeric(df_closed.get("final_ret", 0.0), errors="coerce").fillna(0.0)
                    / 100.0
                ).sum()
            ) if not df_closed.empty else 0.0

            # 실전 체결 기준
            real_exec_df = pd.read_sql(
                """
                SELECT realized_pnl_usdt, notional_usdt, exec_ok, is_dry_run
                FROM bitget_real_execution
                WHERE market_type=?
                ORDER BY id DESC
                LIMIT 5000
                """,
                conn,
                params=(market_type,),
            )
            if real_exec_df is not None and not real_exec_df.empty:
                real_exec_df["realized_pnl_usdt"] = pd.to_numeric(real_exec_df["realized_pnl_usdt"], errors="coerce").fillna(0.0)
                real_exec_df["notional_usdt"] = pd.to_numeric(real_exec_df["notional_usdt"], errors="coerce").fillna(0.0)
                real_exec_df["exec_ok"] = pd.to_numeric(real_exec_df["exec_ok"], errors="coerce").fillna(0).astype(int)
                real_exec_df["is_dry_run"] = pd.to_numeric(real_exec_df["is_dry_run"], errors="coerce").fillna(0).astype(int)
                real_exec_live = real_exec_df[(real_exec_df["exec_ok"] == 1) & (real_exec_df["is_dry_run"] == 0)].copy()
                real_pnl = float(real_exec_live["realized_pnl_usdt"].sum()) if not real_exec_live.empty else 0.0
                real_notional = float(real_exec_live["notional_usdt"].sum()) if not real_exec_live.empty else 0.0
                real_count = int(len(real_exec_live))
            else:
                real_pnl = 0.0
                real_notional = 0.0
                real_count = 0

            msg3 = f"{m_icon} <b>[3/9] 통합 자금 관리 진검승부 (실전+가상)</b>\n"
            msg3 += "🧪 <b>가상 장부 기준</b>\n"
            msg3 += f" - 동적 켈리 수익: <b>{kelly_pnl:+,.2f} USDT</b>\n"
            msg3 += f" - 고정 리스크 수익: {fixed_pnl:+,.2f} USDT\n"
            msg3 += f" - 가상 판정: {'동적 켈리 우위' if kelly_pnl > fixed_pnl else '고정 리스크 우위'}\n\n"
            msg3 += "💸 <b>실전 체결 기준</b>\n"
            msg3 += f" - 누적 실현손익: <b>{real_pnl:+,.2f} USDT</b>\n"
            msg3 += f" - 누적 노셔널: {real_notional:,.2f} USDT\n"
            msg3 += f" - 실체결 건수: {real_count}\n"
            msg3 += f" - 실가 괴리(PnL): {real_pnl - kelly_pnl:+,.2f} USDT\n"
            send_telegram_msg(msg3)
            time.sleep(1)

            open_sigs = df_open["sig_type"].astype(str).tolist()
            trend_fleet = sum(1 for s in open_sigs if "S1" in s)
            recon_fleet = sum(1 for s in open_sigs if ("S4" in s or "S6" in s or "S7" in s))
            msg4 = f"{m_icon} <b>[4/9] 섹터 포트폴리오 다중화 현황</b>\n"
            msg4 += f"🎯 편대 현황: 주도주 폭격편대 {trend_fleet}기 | 차기섹터 정찰대 {recon_fleet}기\n"
            msg4 += "\n🗣️ <b>[관제탑 시선]</b> "
            if trend_fleet == 0 and recon_fleet > 0:
                msg4 += "주도 코인이 불명확하여, 바닥 탈출(S4/S6) 및 역추세 타점 위주로 소액 정찰 중입니다.\n"
            elif trend_fleet > 0 and recon_fleet == 0:
                msg4 += "시장에 확실한 주도 추세가 존재하여, 돌파(S1) 타점에 화력을 집중하고 있습니다.\n"
            elif trend_fleet == 0 and recon_fleet == 0:
                msg4 += "모든 타점 기준 미달로 완벽한 현금 관망 중입니다.\n"
            else:
                msg4 += "추세 추종(롱)과 역추세/바닥(숏/스윙)을 동시에 파견하며 포트폴리오 밸런스를 맞추고 있습니다.\n"
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
                rot_df = rot_df.copy()
                rot_df["asset_group"] = rot_df["symbol"].apply(_coin_asset_group)
                daily_dom = rot_df.groupby("entry_date")["asset_group"].agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None).dropna()
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
                msg7 += f"🔥 <b>현재 주도 자산군:</b> {current_sec} ({current_streak}일째 체류 중)\n"
                msg7 += f"🔮 <b>다음 예측 자산군:</b> {cfg.get('PREDICTED_NEXT_SECTOR', '분석중')}\n"
                msg7 += f"⚡ <b>베팅 어드밴티지:</b> {'🔥활성화(200%)' if cfg.get('ROTATION_ADVANTAGE_ACTIVE') else '정상(100%)'}\n\n"
                msg7 += "▪️ <b>자산군별 자금 체류 시간(수명):</b>\n"
                for s, lengths in streaks.items():
                    msg7 += f" - {str(s)[:6]}: 평균 {sum(lengths)/len(lengths):.1f}일\n"
                sorted_trans = sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:2]
                if sorted_trans:
                    msg7 += "\n▪️ <b>빈번한 자산군 이동 궤적:</b>\n"
                    for p, c in sorted_trans:
                        msg7 += f" - {p} ({c}회 관측)\n"
            else:
                msg7 += " ↳ 자산군 순환 데이터 부족\n"
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
            mean_b = np.nan
            if not recent_dna.empty and "entry_breadth" in recent_dna.columns:
                mean_b = pd.to_numeric(recent_dna["entry_breadth"], errors="coerce").dropna().mean()
                if pd.notna(mean_b) and mean_b < 0.98:
                    msg8 += "🚨 <b>[DNA 변위 감지]</b> 대장주 일치율 급감 ➔ 방어 개입 중\n"
            msg8 += "\n🗣️ <b>[관제탑 시선]</b> "
            if pd.notna(mean_b) and mean_b < 0.98:
                msg8 += "알트코인 수급이 말라죽고 비트코인 쏠림(Breadth 붕괴) 현상이 심화되어, 기존 알트 로직을 멈추고 방어 모드에 개입했습니다.\n"
            elif days_alive == 0:
                msg8 += "시스템이 방금 코인 시장의 새로운 메타에 맞춰 뇌수술(진화)을 마쳤습니다. 수명(0일차)을 리셋합니다.\n"
            else:
                msg8 += "현재 팩토리의 매매 로직이 코인 시장 트렌드와 잘 맞물려 돌아가며 통계적 우위를 점하고 있습니다.\n"
            send_telegram_msg(msg8)
            time.sleep(1)

            std_df = df_closed[df_closed["sig_type"].astype(str).str.contains("STANDARD", na=False)]
            sn_df = df_closed[df_closed["sig_type"].astype(str).str.contains("SUPERNOVA", na=False)]
            n_std = int(len(std_df))
            n_sn = int(len(sn_df))
            std_virtual_ret = (
                float(pd.to_numeric(std_df["final_ret"], errors="coerce").fillna(0.0).mean())
                if n_std > 0
                else 0.0
            )
            sn_virtual_ret = (
                float(pd.to_numeric(sn_df["final_ret"], errors="coerce").fillna(0.0).mean())
                if n_sn > 0
                else 0.0
            )
            n_dm = _deathmatch_min_n_cfg(cfg)
            v_verdict = _deathmatch_ab_verdict(n_std, n_sn, std_virtual_ret, sn_virtual_ret, n_dm)

            lb_all = build_practitioner_reality_leaderboard(market_type=market_type, limit_rows=40)
            if lb_all is not None and not lb_all.empty:
                real_system_ret = float(pd.to_numeric(lb_all["real_ret_pct"], errors="coerce").fillna(0.0).mean())
                virtual_system_ret = float(pd.to_numeric(lb_all["virtual_ret_pct"], errors="coerce").fillna(0.0).mean())
                top_real = lb_all.sort_values(["real_ret_pct", "samples"], ascending=[False, False]).head(3)
                top_virtual = lb_all.sort_values(["virtual_ret_pct", "samples"], ascending=[False, False]).head(3)
            else:
                real_system_ret = 0.0
                virtual_system_ret = 0.0
                top_real = pd.DataFrame()
                top_virtual = pd.DataFrame()

            msg9 = f"{m_icon} <b>[9/9] 시스템 데스매치 결산 (실전+가상)</b>\n"
            msg9 += "🧠 <b>가상 리서치 기준(A/B)</b>\n"
            msg9 += f"📎 청산 표본: 오리지널(A) {n_std}건 | 초신성(B) {n_sn}건 (비교 최소 각 {n_dm}건)\n"
            msg9 += f" - 오리지널(A): {_fmt_deathmatch_ret(std_virtual_ret, n_std)}\n"
            msg9 += f" - 초신성(B): {_fmt_deathmatch_ret(sn_virtual_ret, n_sn)}\n"
            msg9 += f" - 판정: {v_verdict}\n\n"

            msg9 += "💸 <b>실전 체결 기준(전체 PRACT 풀)</b>\n"
            msg9 += f" - 실전 평균: {real_system_ret:+.2f}%\n"
            msg9 += f" - 가상 평균(동기화): {virtual_system_ret:+.2f}%\n"
            msg9 += f" - 실가 괴리: {real_system_ret - virtual_system_ret:+.2f}pp\n"

            if top_real is not None and not top_real.empty:
                msg9 += "\n🏆 <b>실전 TOP3 실무자</b>\n"
                for _, row in top_real.iterrows():
                    msg9 += (
                        f" - {row['practitioner_key']}: "
                        f"실전 {float(row['real_ret_pct']):+,.2f}% | "
                        f"가상 {float(row['virtual_ret_pct']):+,.2f}%\n"
                    )

            if top_virtual is not None and not top_virtual.empty:
                msg9 += "\n🧪 <b>가상 TOP3 실무자</b>\n"
                for _, row in top_virtual.iterrows():
                    msg9 += (
                        f" - {row['practitioner_key']}: "
                        f"가상 {float(row['virtual_ret_pct']):+,.2f}% | "
                        f"실전 {float(row['real_ret_pct']):+,.2f}%\n"
                    )

            send_telegram_msg(msg9)
            time.sleep(1)
        except Exception as e:
            send_telegram_msg(f"⚠️ {market_type} 리포트 에러: {e}")
    conn.close()


if __name__ == "__main__":
    init_forward_db()
