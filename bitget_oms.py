"""
Bitget 전용 OMS(주문 관리) + 주기적 체결대사(Reconciliation).
- clientOid 기반 멱등·재시도
- 가상 장부 OPEN vs 거래소 포지션 유령 제거
- 미체결/부분체결 주문 fetch_order 하이드레이션
"""
import json
import os
import re
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from bitget_env import bitget_access_key, bitget_passphrase, bitget_secret_key
from bitget_logger import get_logger, setup_logging
from bitget_rate_limit_guard import backoff_sleep, throttle
from bitget_symbol_utils import normalize_market_symbol

try:
    import ccxt
except Exception:
    ccxt = None

setup_logging()
logger = get_logger("bitget.oms")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "bitget_market_data.sqlite")

# forward_tester 의 설정/통신 재사용 (순환 import 방지: oms 만 executor 역상속)
from bitget_forward_tester import (  # noqa: E402
    init_forward_db,
    load_system_config,
    save_system_config,
    send_telegram_msg,
)


def create_trade_exchange(market_type="futures"):
    if ccxt is None:
        raise RuntimeError("ccxt not available")
    api_key = bitget_access_key()
    api_secret = bitget_secret_key()
    passphrase = bitget_passphrase()
    if not api_key or not api_secret or not passphrase:
        raise RuntimeError(
            "missing Bitget API credentials: set BITGET_ACCESS_KEY, BITGET_SECRET_KEY, BITGET_PASSPHRASE "
            "(or legacy BITGET_API_KEY / BITGET_API_SECRET / BITGET_API_PASSPHRASE)"
        )
    ex = ccxt.bitget(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "password": passphrase,
            "enableRateLimit": True,
            "options": {"defaultType": "spot" if market_type == "spot" else "swap"},
        }
    )
    ex.load_markets()
    return ex


def generate_client_oid(prefix="bg"):
    """Bitget clientOid: 영숫자, 중복 방지."""
    core = uuid.uuid4().hex + uuid.uuid4().hex
    s = f"{prefix}{core}"[:40]
    return re.sub(r"[^a-zA-Z0-9]", "x", s)


def _transient_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    keys = (
        "timeout",
        "timed out",
        "network",
        "gateway",
        "502",
        "503",
        "504",
        "429",
        "ratelimit",
        "unavailable",
        "econnreset",
        "temporar",
    )
    return any(k in msg for k in keys)


def oms_place_market_order(
    ex,
    market_symbol: str,
    order_side: str,
    amount: float,
    params_base: Optional[dict] = None,
    client_oid: Optional[str] = None,
    max_attempts: int = 3,
):
    """
    시장가 주문: 동일 clientOid 로 네트워크 일시 오류 시 재시도(거절 시 OID 재발급은 호출측).
    반환: ok, order_id, client_order_id, raw, filled, remaining, status, message
    """
    params_base = dict(params_base or {})
    oid_in = client_oid or generate_client_oid()
    merged = dict(params_base)
    merged["clientOid"] = oid_in

    last_err = None
    used_oid = oid_in

    for attempt in range(max(1, int(max_attempts))):
        try:
            throttle("bitget.oms.create_order", 0.38)
            order = ex.create_order(market_symbol, "market", order_side, float(amount), None, merged)
            filled = float(order.get("filled") or 0.0)
            remaining = float(order.get("remaining") or 0.0)
            amt = float(amount)
            st = str(order.get("status") or "")
            oid_out = str(order.get("id") or "")
            ok = True
            stat = "filled_submitted"
            if remaining > 0 and filled > 0 and filled + 1e-12 < amt:
                stat = "partial_fill"
            if st in ("rejected", "canceled", "cancelled"):
                ok = False
                stat = st
            return {
                "ok": ok,
                "order_id": oid_out,
                "client_order_id": used_oid,
                "raw": order,
                "filled": filled,
                "remaining": remaining,
                "status": stat,
                "message": "",
            }
        except Exception as e:
            last_err = e
            if _transient_error(e) and attempt < max_attempts - 1:
                backoff_sleep(attempt + 1)
                continue
            logger.warning("OMS create_order fail attempt %s: %s", attempt + 1, e)
            return {
                "ok": False,
                "order_id": "",
                "client_order_id": used_oid,
                "raw": None,
                "filled": 0.0,
                "remaining": float(amount),
                "status": "error",
                "message": str(e),
            }
    return {
        "ok": False,
        "order_id": "",
        "client_order_id": used_oid,
        "raw": None,
        "filled": 0.0,
        "remaining": float(amount),
        "status": "error",
        "message": str(last_err or "unknown"),
    }


def _row_ccxt_future_symbol(internal_sym: str) -> str:
    return normalize_market_symbol(str(internal_sym).replace("_", "/"), "futures")


def _build_open_position_index(ex):
    """{(ccxt_symbol, 'LONG'|'SHORT'): contracts}"""
    throttle("bitget.oms.fetch_positions", 0.4)
    rows = ex.fetch_positions()
    out = {}
    for p in rows or []:
        try:
            c = float(p.get("contracts") or p.get("contractSize") or 0.0)
            if c is None or abs(c) < 1e-12:
                # 일부 응답은 size 키
                c = float(p.get("size") or 0.0)
        except (TypeError, ValueError):
            c = 0.0
        if abs(c) < 1e-12:
            continue
        sym = p.get("symbol")
        if not sym:
            continue
        sd = str(p.get("side") or "").lower()
        if sd in ("long", "short"):
            side = "LONG" if sd == "long" else "SHORT"
        else:
            # net 방식 폴백
            side = "LONG" if c > 0 else "SHORT"
        out[(sym, side)] = abs(c)
    return out


def _close_phantom_virtual(conn, r, exit_rsn: str, exit_type: str = "RECON_GHOST"):
    """거래소에 포지션 없음 → 가상 OPEN 강제 정리 + 증거금 국고 환입(ret=0)."""
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
    flow_tags = "#체결대사_유령제거"
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
            time.sleep(0.5 * (2 ** attempt))
    treasury_key = "TREASURY_FUTURES_USDT" if str(r.get("market_type")).lower() != "spot" else "TREASURY_SPOT_USDT"
    cur_cfg = load_system_config()
    before = float(cur_cfg.get(treasury_key, 0.0) or 0.0)
    margin_used = float(r.get("margin_used", 0.0) or 0.0)
    raw_pnl = float(r.get("sim_kelly_invest", 0.0) or 0.0) * (ret / 100.0)
    pnl = max(-margin_used, raw_pnl)
    cur_cfg[treasury_key] = max(0.0, before + margin_used + pnl)
    save_system_config(cur_cfg)


def _hydrate_recent_executions(ex, conn):
    """order_id 있는 최근 행 fetch_order 로 동기화(재부팅 후 미체결/지연 체결 인식)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, symbol, market_type, order_id
        FROM bitget_real_execution
        WHERE IFNULL(exec_ok,0)=1 AND IFNULL(order_id,'')!=''
        ORDER BY id DESC
        LIMIT 60
        """
    )
    rows = cur.fetchall()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for rid, sym, mtype, oid in rows:
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
        payload = json.dumps(
            {"recon_hydrate": True, "exchange_status": st, "filled": filled, "remaining": remaining, "raw": od},
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


def _scan_open_orders_and_notify(ex):
    """서버 재시작 직후 등 남은 미체결 주문 인지 + 텔레그램."""
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
        lines.append(f"• {sym} {side} id={oid} coid={coid} st={st} rem={rem}")
    send_telegram_msg(
        "📋 <b>[OMS] 미체결 주문 스냅샷</b>\n"
        + (f"총 {len(oo)}건 (상위 {min(len(oo),25)})\n" + "\n".join(lines))
    )
    return len(oo)


def _reconcile_futures_opens(ex, conn):
    pos_map = _build_open_position_index(ex)
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM bitget_forward_trades WHERE lower(market_type)='futures' AND status='OPEN'"
    )
    colnames = [d[0] for d in cur.description]
    n_fixed = 0
    for row in cur.fetchall():
        r = dict(zip(colnames, row))
        ccxt_sym = _row_ccxt_future_symbol(r.get("symbol", ""))
        side = str(r.get("position_side", "LONG")).upper()
        key = (ccxt_sym, side)
        sz = float(pos_map.get(key, 0.0))
        if sz < 1e-12:
            _close_phantom_virtual(conn, r, "체결대사: 거래소에 동일방향 포지션 없음(RECON)", "RECON_GHOST")
            n_fixed += 1
    conn.commit()

    cur.execute(
        "SELECT symbol, position_side FROM bitget_forward_trades WHERE lower(market_type)='futures' AND status='OPEN'"
    )
    open_keys = set()
    for sym, ps in cur.fetchall():
        open_keys.add((_row_ccxt_future_symbol(sym), str(ps or "LONG").upper()))

    orphans = []
    for k, contracts in pos_map.items():
        if k not in open_keys:
            orphans.append(f"{k[0]} {k[1]} ~{contracts}")
    if orphans:
        send_telegram_msg(
            "⚠️ <b>[OMS] 거래소 전용 포지션(장부 무OPEN)</b>\n" + "\n".join(f"• {x}" for x in orphans[:20])
        )
    conn.commit()
    return n_fixed


def _fetch_my_trades_snapshot(ex):
    """최근 체결 내역 스냅샷(장부 감사용)."""
    try:
        since_ms = int((time.time() - 72 * 3600) * 1000)
        throttle("bitget.oms.fetch_my_trades", 0.5)
        trs = ex.fetch_my_trades(since=since_ms, limit=200)
        return len(trs or []), trs or []
    except Exception as e:
        logger.warning("fetch_my_trades: %s", e)
        return -1, []


def run_scheduled_reconciliation():
    cfg = load_system_config()
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

    conn = sqlite3.connect(DB_PATH, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=15000;")
    report = {"phantoms_closed": 0, "open_orders": 0, "hydrated": 0, "my_trades": 0}
    try:
        _hydrate_recent_executions(ex, conn)
        report["hydrated"] = 1
        report["phantoms_closed"] = int(_reconcile_futures_opens(ex, conn))
        report["open_orders"] = int(_scan_open_orders_and_notify(ex))
        n_tr, _ = _fetch_my_trades_snapshot(ex)
        report["my_trades"] = int(n_tr)
        cfg2 = load_system_config()
        cfg2["LAST_OMS_RECON_AT_UTC"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        cfg2["LAST_OMS_RECON_PHANTOMS"] = report["phantoms_closed"]
        cfg2["LAST_OMS_RECON_OPEN_ORDERS"] = report["open_orders"]
        cfg2["LAST_OMS_MY_TRADES_WINDOW_COUNT"] = report["my_trades"]
        save_system_config(cfg2)
    finally:
        conn.close()

    logger.info(
        "OMS reconciliation done phantoms=%s open_orders=%s", report["phantoms_closed"], report["open_orders"]
    )
    return report
