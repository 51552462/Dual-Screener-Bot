"""Real execution logging and practitioner leaderboard."""
from __future__ import annotations

import sqlite3
from datetime import datetime

import pandas as pd

from bitget.forward.shared import DB_PATH, init_forward_db, load_system_config, save_system_config

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

