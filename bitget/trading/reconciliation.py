"""
Scheduled OMS reconciliation — phantom virtual OPEN cleanup, order hydration, orphan alerts.
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any

from bitget.forward_tester import init_forward_db, load_system_config, save_system_config, send_telegram_msg
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.logging_setup import get_logger, setup_logging
from bitget.rate_limit_guard import throttle
from bitget.symbol_utils import normalize_market_symbol
from bitget.trading.execution_safety import meta_kill_switch_active
from bitget.trading.oms_core import create_trade_exchange
from bitget.trading.position_manager import build_open_position_index, row_ccxt_future_symbol
from bitget.trading.slippage_guard import audit_post_trade_slippage

setup_logging()
logger = get_logger("bitget.trading.reconciliation")


def _close_phantom_virtual(conn, r, exit_rsn: str, exit_type: str = "RECON_GHOST"):
    exit_d = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    ret = 0.0
    tid = int(r["id"])
    ep = float(r.get("entry_price") or 0.0)
    new_max = float(r.get("max_high") or ep)
    new_min = float(r.get("min_low") or ep)
    new_bars = int(r.get("bars_held") or 0)
    new_up = float(r.get("up_vol_sum") or 0.0)
    new_down = float(r.get("down_vol_sum") or 0.0)
    eb = float(r.get("entry_breadth") or 1.0)
    flow_tags = "#recon_ghost"
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
    z = float(ret)
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
        z,
        z,
        z,
        eb,
        z,
        z,
        z,
        flow_tags,
        tid,
    )
    for attempt in range(5):
        try:
            conn.execute(update_sql, params)
            break
        except sqlite3.OperationalError as e:
            if "database is locked" not in str(e).lower():
                raise
            if attempt >= 4:
                raise
            time.sleep(0.5 * (2**attempt))
    treasury_key = "TREASURY_FUTURES_USDT" if str(r.get("market_type")).lower() != "spot" else "TREASURY_SPOT_USDT"
    cur_cfg = load_system_config()
    before = float(cur_cfg.get(treasury_key, 0.0) or 0.0)
    margin_used = float(r.get("margin_used", 0.0) or 0.0)
    raw_pnl = float(r.get("sim_kelly_invest", 0.0) or 0.0) * (ret / 100.0)
    pnl = max(-margin_used, raw_pnl)
    cur_cfg[treasury_key] = max(0.0, before + margin_used + pnl)
    save_system_config(cur_cfg)


def _hydrate_recent_executions(ex, conn, *, max_slippage_bps: float = 50.0) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, symbol, market_type, order_id, entry_price
        FROM bitget_real_execution
        WHERE IFNULL(exec_ok,0)=1 AND IFNULL(order_id,'')!=''
        ORDER BY id DESC
        LIMIT 60
        """
    )
    rows = cur.fetchall()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    audited = 0
    for rid, sym, mtype, oid, entry_px in rows:
        if not oid:
            continue
        mt = str(mtype or "").lower()
        try:
            ccxt_sym = normalize_market_symbol(str(sym).replace("_", "/"), "futures" if mt == "futures" else "spot")
        except Exception:
            continue
        try:
            throttle("bitget.oms.fetch_order", 0.35)
            od = ex.fetch_order(str(oid), ccxt_sym)
        except Exception as e:
            logger.info("hydrate skip id=%s oid=%s: %s", rid, oid, e)
            continue
        st = str(od.get("status") or "")
        filled = float(od.get("filled") or 0.0)
        remaining = float(od.get("remaining") or 0.0)
        fill_px = od.get("average") or od.get("price")
        slip_audit = {}
        if fill_px is not None and entry_px is not None:
            slip_audit = audit_post_trade_slippage(
                float(entry_px),
                float(fill_px),
                max_bps=max_slippage_bps,
            )
            if slip_audit.get("exceeded"):
                audited += 1
        payload = json.dumps(
            {
                "recon_hydrate": True,
                "exchange_status": st,
                "filled": filled,
                "remaining": remaining,
                "slippage_audit": slip_audit,
                "raw": od,
            },
            ensure_ascii=False,
        )[:4000]
        cur.execute(
            """
            UPDATE bitget_real_execution
            SET updated_at=?, exec_status=?, exec_payload=?
            WHERE id=?
            """,
            (now, st or "hydrated", payload, int(rid)),
        )
    conn.commit()
    return audited


def _scan_open_orders_and_notify(ex) -> int:
    try:
        throttle("bitget.oms.fetch_open_orders", 0.45)
        oo = ex.fetch_open_orders()
    except Exception as e:
        logger.warning("fetch_open_orders: %s", e)
        return 0
    if not oo:
        return 0
    lines = []
    for o in oo[:25]:
        sym = o.get("symbol", "")
        oid = o.get("id", "")
        st = o.get("status", "")
        side = o.get("side", "")
        rem = o.get("remaining", "")
        info = o.get("info") or {}
        coid = info.get("clientOid") or o.get("clientOrderId") or ""
        lines.append(f"- {sym} {side} id={oid} coid={coid} st={st} rem={rem}")
    send_telegram_msg(
        "[OMS] Open orders snapshot\n"
        + (f"total {len(oo)} (top {min(len(oo), 25)})\n" + "\n".join(lines))
    )
    return len(oo)


def detect_orphan_positions(
    ex,
    conn,
) -> list[str]:
    """
    Exchange positions with no matching virtual OPEN row (Orphan).
    Returns human-readable alert lines.
    """
    pos_map = build_open_position_index(ex)
    cur = conn.cursor()
    cur.execute(
        "SELECT symbol, position_side FROM bitget_forward_trades WHERE lower(market_type)='futures' AND status='OPEN'"
    )
    open_keys = set()
    for sym, ps in cur.fetchall():
        open_keys.add((row_ccxt_future_symbol(sym), str(ps or "LONG").upper()))

    orphans: list[str] = []
    for k, contracts in pos_map.items():
        if k not in open_keys:
            orphans.append(f"{k[0]} {k[1]} ~{contracts}")
    return orphans


def reconcile_phantom_opens(ex, conn) -> int:
    """
    Virtual OPEN rows with no matching exchange position (Phantom OPEN).
    Closes ghosts in forward ledger and returns count fixed.
    """
    pos_map = build_open_position_index(ex)
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM bitget_forward_trades WHERE lower(market_type)='futures' AND status='OPEN'"
    )
    colnames = [d[0] for d in cur.description]
    n_fixed = 0
    for row in cur.fetchall():
        r = dict(zip(colnames, row))
        ccxt_sym = row_ccxt_future_symbol(r.get("symbol", ""))
        side = str(r.get("position_side", "LONG")).upper()
        key = (ccxt_sym, side)
        sz = float(pos_map.get(key, 0.0))
        if sz < 1e-12:
            _close_phantom_virtual(
                conn,
                r,
                "recon: no matching exchange position (RECON)",
                "RECON_GHOST",
            )
            n_fixed += 1
    conn.commit()
    return n_fixed


def _reconcile_futures_opens(ex, conn) -> int:
    n_fixed = reconcile_phantom_opens(ex, conn)
    orphans = detect_orphan_positions(ex, conn)
    if orphans:
        send_telegram_msg(
            "[OMS] Exchange-only positions (no virtual OPEN)\n"
            + "\n".join(f"- {x}" for x in orphans[:20])
        )
    conn.commit()
    return n_fixed


def _fetch_my_trades_snapshot(ex) -> tuple[int, list]:
    try:
        since_ms = int((time.time() - 72 * 3600) * 1000)
        throttle("bitget.oms.fetch_my_trades", 0.5)
        trs = ex.fetch_my_trades(since=since_ms, limit=200)
        return len(trs or []), trs or []
    except Exception as e:
        logger.warning("fetch_my_trades: %s", e)
        return -1, []


def run_scheduled_reconciliation() -> dict[str, Any]:
    cfg = load_system_config()
    if meta_kill_switch_active():
        logger.warning("MetaGovernor KILL_SWITCH: scheduled reconciliation skipped")
        return {"skipped": True, "reason": "meta_kill_switch"}
    if not bool(cfg.get("ENABLE_REAL_EXECUTION", False)):
        return {"skipped": True, "reason": "execution_disabled"}
    if bool(cfg.get("REAL_EXECUTION_DRY_RUN", True)):
        return {"skipped": True, "reason": "dry_run"}

    init_forward_db()
    try:
        ex = create_trade_exchange("futures")
    except Exception as e:
        logger.warning("recon skip: cannot create exchange: %s", e)
        return {"skipped": True, "reason": str(e)}

    db_path = market_data_db_path()
    conn = sqlite3.connect(db_path, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=15000;")
    max_slip = float(cfg.get("POST_TRADE_MAX_SLIPPAGE_BPS", 50.0))
    report: dict[str, Any] = {
        "phantoms_closed": 0,
        "open_orders": 0,
        "hydrated": 0,
        "slippage_alerts": 0,
        "my_trades": 0,
    }
    try:
        report["slippage_alerts"] = int(
            _hydrate_recent_executions(ex, conn, max_slippage_bps=max_slip)
        )
        report["hydrated"] = 1
        report["phantoms_closed"] = int(_reconcile_futures_opens(ex, conn))
        report["open_orders"] = int(_scan_open_orders_and_notify(ex))
        n_tr, _ = _fetch_my_trades_snapshot(ex)
        report["my_trades"] = int(n_tr)
        cfg2 = load_system_config()
        cfg2["LAST_OMS_RECON_AT_UTC"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        cfg2["LAST_OMS_RECON_PHANTOMS"] = report["phantoms_closed"]
        cfg2["LAST_OMS_RECON_OPEN_ORDERS"] = report["open_orders"]
        cfg2["LAST_OMS_RECON_SLIPPAGE_ALERTS"] = report["slippage_alerts"]
        cfg2["LAST_OMS_MY_TRADES_WINDOW_COUNT"] = report["my_trades"]
        save_system_config(cfg2)
    finally:
        conn.close()

    logger.info(
        "OMS reconciliation done phantoms=%s open_orders=%s slippage_alerts=%s",
        report["phantoms_closed"],
        report["open_orders"],
        report["slippage_alerts"],
    )
    return report
