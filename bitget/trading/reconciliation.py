"""
Scheduled OMS reconciliation — phantom virtual OPEN cleanup, order hydration,
orphan escalation (block new entries; never auto-flatten exchange orphans).
"""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Optional

import pandas as pd

from bitget.infra.bounded_reads import forward_open_recon_futures_sql
from bitget.infra.clock import parse_utc_iso, utc_date_str, utc_datetime_str, utc_datetime_str_tz, utc_now
from bitget.infra.memory_policy import (
    OMS_ORPHAN_ALERT_MIN_INTERVAL_SEC,
    OMS_ORPHAN_STREAK_PROPOSE_KILL,
)

from bitget.forward_tester import init_forward_db, load_system_config, save_system_config, send_telegram_msg
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.logging_setup import get_logger, setup_logging
from bitget.infra.network_retry import call_with_retry
from bitget.infra.shared_db_connector import get_connection
from bitget.symbol_utils import normalize_market_symbol
from bitget.trading.execution_safety import meta_kill_switch_active
from bitget.trading.oms_core import create_trade_exchange
from bitget.trading.oms_source_stats import get_oms_source_counters
from bitget.trading.order_snapshot import fetch_order_snapshot, list_open_orders
from bitget.trading.position_manager import build_open_position_index, row_ccxt_future_symbol
from bitget.trading.slippage_guard import audit_post_trade_slippage

setup_logging()
logger = get_logger("bitget.trading.reconciliation")


def _close_phantom_virtual(conn, r, exit_rsn: str, exit_type: str = "RECON_GHOST"):
    exit_d = utc_date_str()
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
    now = utc_datetime_str()
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
            od = fetch_order_snapshot(
                ex,
                str(oid),
                ccxt_sym,
                market_type=mt or "futures",
                prefer_private_ws=True,
            )
            if od is None:
                logger.info("hydrate skip id=%s oid=%s: fetch_order failed", rid, oid)
                continue
        except Exception as e:
            logger.info("hydrate skip id=%s oid=%s: %s", rid, oid, e)
            continue
        st = str(od.get("status") or "")
        filled = float(od.get("filled") or 0.0)
        remaining = float(od.get("remaining") or 0.0) if od.get("remaining") not in ("", None) else 0.0
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
                "source": od.get("_source") or "unknown",
                "raw": {k: v for k, v in od.items() if k != "raw"},
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
    oo = list_open_orders(ex, prefer_private_ws=True)
    if oo is None:
        logger.warning("list_open_orders failed after retries")
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
    recon_q, recon_params = forward_open_recon_futures_sql()
    cur = conn.cursor()
    cur.execute(recon_q, recon_params)
    open_keys = set()
    for row in cur.fetchall():
        if hasattr(row, "keys"):
            sym, ps = row["symbol"], row["position_side"]
        else:
            sym = row[0]
            ps = row[1] if len(row) > 1 else "LONG"
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
    df_open = pd.read_sql(*forward_open_recon_futures_sql(), conn)
    n_fixed = 0
    for _, r in df_open.iterrows():
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


def apply_orphan_escalation(
    cfg: dict,
    orphans: list[str],
    *,
    now_iso: Optional[str] = None,
    alert: bool = True,
) -> dict[str, Any]:
    """
    Persist orphan gate state for live execution_safety.

    Invariants:
      - Never place exchange reduce/close for orphans
      - Never auto-arm MetaGovernor KILL_SWITCH (propose flag only)
      - Clear ACTIVE when orphan list is empty
    """
    stamp = now_iso or utc_datetime_str_tz()
    patch: dict[str, Any] = {
        "OMS_ORPHAN_LAST_AT_UTC": stamp,
        "OMS_ORPHAN_COUNT": len(orphans),
        "OMS_ORPHAN_SYMBOLS": " | ".join(orphans[:20]),
    }
    if not orphans:
        patch.update(
            {
                "OMS_ORPHAN_ACTIVE": "OFF",
                "OMS_ORPHAN_STREAK": 0,
                "OMS_ORPHAN_KILL_SWITCH_PROPOSED": "OFF",
            }
        )
        if str(cfg.get("OMS_ORPHAN_ACTIVE", "OFF") or "OFF").strip().upper() == "ON":
            logger.warning("OMS orphans cleared — new entries unblocked")
            if alert:
                try:
                    from bitget.governance.meta_alerts import send_meta_critical_alert

                    send_meta_critical_alert(
                        "OMS orphans cleared",
                        "Exchange book matches virtual OPEN again — orphan gate OFF",
                        prefix="OMS_ORPHAN_CLEAR",
                    )
                except Exception:
                    pass
        return patch

    try:
        prev_streak = int(cfg.get("OMS_ORPHAN_STREAK") or 0)
    except (TypeError, ValueError):
        prev_streak = 0
    streak = prev_streak + 1
    propose_at = max(1, int(OMS_ORPHAN_STREAK_PROPOSE_KILL))
    propose = streak >= propose_at
    patch.update(
        {
            "OMS_ORPHAN_ACTIVE": "ON",
            "OMS_ORPHAN_STREAK": streak,
            "OMS_ORPHAN_KILL_SWITCH_PROPOSED": "ON" if propose else "OFF",
        }
    )

    if not alert:
        return patch

    # Throttle CRITICAL alerts (config last-alert epoch)
    send_alert = True
    try:
        last = parse_utc_iso(str(cfg.get("OMS_ORPHAN_LAST_ALERT_AT_UTC") or ""))
        if last is not None:
            age = (utc_now() - last).total_seconds()
            if age < float(OMS_ORPHAN_ALERT_MIN_INTERVAL_SEC):
                send_alert = False
    except Exception:
        send_alert = True

    if send_alert:
        body = (
            f"orphans={len(orphans)} streak={streak}/{propose_at}\n"
            + "\n".join(f"- {x}" for x in orphans[:20])
            + "\n→ new live entries BLOCKED (OMS_ORPHAN_ACTIVE=ON)"
            + "\n→ NEVER auto-flatten — human must resolve exchange book"
        )
        if propose:
            body += (
                "\n→ PROPOSE MetaGovernor KILL_SWITCH "
                "(operator confirm; not auto-armed)"
            )
        try:
            from bitget.governance.meta_alerts import send_meta_critical_alert

            send_meta_critical_alert(
                "OMS exchange-only orphans",
                body,
                prefix="OMS_ORPHAN",
            )
            patch["OMS_ORPHAN_LAST_ALERT_AT_UTC"] = stamp
        except Exception:
            try:
                send_telegram_msg(
                    "[OMS] Exchange-only positions (no virtual OPEN)\n"
                    + "\n".join(f"- {x}" for x in orphans[:20])
                )
                patch["OMS_ORPHAN_LAST_ALERT_AT_UTC"] = stamp
            except Exception:
                pass
    return patch


def _reconcile_futures_opens(ex, conn) -> tuple[int, list[str]]:
    n_fixed = reconcile_phantom_opens(ex, conn)
    orphans = detect_orphan_positions(ex, conn)
    conn.commit()
    return n_fixed, orphans


def _fetch_my_trades_snapshot(ex) -> tuple[int, list]:
    since_ms = int((time.time() - 72 * 3600) * 1000)
    trs = call_with_retry(
        lambda: ex.fetch_my_trades(since=since_ms, limit=200),
        op="oms.fetch_my_trades",
        throttle_key="bitget.oms.fetch_my_trades",
        throttle_interval_sec=0.5,
        default=None,
        swallow=True,
    )
    if trs is None:
        logger.warning("fetch_my_trades failed after retries")
        return -1, []
    return len(trs or []), trs or []


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
    conn = get_connection(db_path)
    max_slip = float(cfg.get("POST_TRADE_MAX_SLIPPAGE_BPS", 50.0))
    report: dict[str, Any] = {
        "phantoms_closed": 0,
        "orphans": 0,
        "orphan_active": False,
        "open_orders": 0,
        "hydrated": 0,
        "slippage_alerts": 0,
        "my_trades": 0,
    }
    counters = get_oms_source_counters()
    counters.begin_window()
    pending_cfg: dict[str, Any] = {}
    try:
        report["slippage_alerts"] = int(
            _hydrate_recent_executions(ex, conn, max_slippage_bps=max_slip)
        )
        report["hydrated"] = 1
        phantoms, orphans = _reconcile_futures_opens(ex, conn)
        report["phantoms_closed"] = int(phantoms)
        report["orphans"] = int(len(orphans))
        orphan_patch = apply_orphan_escalation(cfg, orphans)
        report["orphan_active"] = str(orphan_patch.get("OMS_ORPHAN_ACTIVE")) == "ON"
        report["open_orders"] = int(_scan_open_orders_and_notify(ex))
        n_tr, _ = _fetch_my_trades_snapshot(ex)
        report["my_trades"] = int(n_tr)
        pending_cfg = {
            "LAST_OMS_RECON_AT_UTC": utc_datetime_str_tz(),
            "LAST_OMS_RECON_PHANTOMS": report["phantoms_closed"],
            "LAST_OMS_RECON_ORPHANS": report["orphans"],
            "LAST_OMS_RECON_OPEN_ORDERS": report["open_orders"],
            "LAST_OMS_RECON_SLIPPAGE_ALERTS": report["slippage_alerts"],
            "LAST_OMS_MY_TRADES_WINDOW_COUNT": report["my_trades"],
        }
        pending_cfg.update(orphan_patch)
    finally:
        source_window = counters.end_window()
        report["source_counts"] = source_window
        try:
            cfg2 = load_system_config()
            cfg2.update(pending_cfg)
            cfg2["LAST_OMS_SOURCE_COUNTS"] = source_window
            save_system_config(cfg2)
        except Exception as e:
            logger.warning("OMS recon config persist failed: %s", e)
        conn.close()

    logger.info(
        "OMS reconciliation done phantoms=%s orphans=%s orphan_active=%s "
        "open_orders=%s slippage_alerts=%s source=%s",
        report["phantoms_closed"],
        report["orphans"],
        report["orphan_active"],
        report["open_orders"],
        report["slippage_alerts"],
        report.get("source_counts"),
    )
    return report
