"""FULL-BT harness — CAT-C/D/E import only; isolated DB write.

IV L1 전체이식 가상매매 — 격리 리플레이 결과, LIVE 승격·R6 대체·B1「달성」 판정 금지.

FULL-BT-HIST-1: run_replay uses real OHLCV bar walk (universe_bt._load_ohlcv reuse).
"""
from __future__ import annotations

import os
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timezone
from typing import Any, Iterator, List, Optional, Union
from unittest import mock

import pandas as pd

from bitget.full_bt.paths import full_bt_db_path
from bitget.infra.logging_setup import get_logger
from bitget.infra.memory_policy import OHLCV_SIGNAL_BAR_LIMIT

logger = get_logger("bitget.full_bt.harness")

# Live scanner TF SSOT (report-only; no invention) — master_scanner / auto_pilot
REUSED_SCANNER_TIMEFRAMES = ["1D", "4H", "2H", "1H"]
STEP11_POLICY = "N/A_skip"  # execution_safety real-only — Adapter context, not gate edit
# Warmup bars before engine call — reuse memory_policy (룰5, 신규 상수 금지)
REUSED_MIN_BARS = int(OHLCV_SIGNAL_BAR_LIMIT)
# 재사용 소스: bitget.analysis.universe_bt.replay._load_ohlcv
OHLCV_LOADER = "bitget.analysis.universe_bt.replay._load_ohlcv"

DateLike = Union[date, datetime, int, float, str]


@contextmanager
def isolated_full_bt_book(db_path: Optional[str] = None) -> Iterator[str]:
    """Route ledger/shared/shadow DB_PATH → bitget_full_bt.sqlite; no-op config/telegram."""
    path = db_path or full_bt_db_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    import bitget.forward.ledger as ledger
    import bitget.forward.shared as shared
    import bitget.shadow_tracking as shadow

    from bitget.forward.shared import _init_forward_db_schema
    from bitget.infra.shared_db_connector import get_connection

    conn = get_connection(path)
    try:
        _init_forward_db_schema(conn)  # clone bitget_forward_trades schema (no new cols)
        conn.commit()
    finally:
        conn.close()

    def _save_noop(_cfg: Any) -> None:
        return None

    def _tg_noop(*_a: Any, **_k: Any) -> None:
        return None

    with ExitStack() as stack:
        stack.enter_context(mock.patch.object(shared, "DB_PATH", path))
        stack.enter_context(mock.patch.object(ledger, "DB_PATH", path))
        stack.enter_context(mock.patch.object(shadow, "DB_PATH", path))
        stack.enter_context(mock.patch.object(shared, "save_system_config", _save_noop))
        stack.enter_context(mock.patch.object(ledger, "save_system_config", _save_noop))
        stack.enter_context(mock.patch.object(shared, "send_telegram_msg", _tg_noop))
        yield path


def _engine_pool_full() -> list:
    """CAT-C: signal_engines 5 + master_scanner pool helper — originals unmodified."""
    from bitget.master_scanner import _build_engine_pool

    return list(_build_engine_pool(None))


def _resolve_engine(engine: str):
    name = str(engine or "").strip().upper()
    pool = _engine_pool_full()
    for n, fn in pool:
        if n == name or n.replace("_", "") == name.replace("_", ""):
            return n, fn
    if pool:
        return pool[0]
    raise RuntimeError("empty engine pool")


def _adapter_step11_note() -> dict:
    """try_add step11 = execution_safety(real only) → paper/FULL-BT N/A skip (Adapter only)."""
    return {
        "step": 11,
        "name": "execution_safety",
        "policy": STEP11_POLICY,
        "note": "CAT-N real path; Adapter skips invoke — try_add internals untouched",
    }


def _normalize_mt(market_type: str) -> str:
    mt = str(market_type).strip().lower()
    if mt in ("fut", "linear"):
        return "futures"
    if mt not in ("spot", "futures"):
        raise ValueError(f"market_type must be spot|futures, got {market_type!r}")
    return mt


def _to_date(val: DateLike) -> date:
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(int(val), tz=timezone.utc).date()
    return pd.Timestamp(val).date()


def _candle_date_str(val: Any) -> str:
    return pd.Timestamp(val).strftime("%Y-%m-%d")


def _facts_from_dbg(dbg: dict) -> dict:
    return {
        "v_cpv": dbg.get("v_cpv", 0.0),
        "v_yang": dbg.get("v_yang", 0.0),
        "v_energy": dbg.get("v_energy", 0.0),
        "v_rs": dbg.get("v_rs", 0.0),
        "dyn_rs": dbg.get("dyn_rs_score", 0.0),
        "dyn_cpv": dbg.get("dyn_cpv_score", 0.0),
        "dyn_tb": dbg.get("dyn_tb_score", 0.0),
        "trade_value_24h": float(dbg.get("trade_value_24h", 1e7) or 1e7),
        "marcap_eok": float(dbg.get("marcap_eok", 0.0) or 0.0),
        "entry_cos_score": float(dbg.get("entry_cos_score", 0.9) or 0.9),
        "entry_dtw_score": float(dbg.get("entry_dtw_score", 0.1) or 0.1),
    }


def _load_open_row(db_path: str, market_type: str, symbol: str) -> Optional[dict]:
    from bitget.infra.shared_db_connector import get_connection

    conn = get_connection(db_path, read_only=True)
    try:
        row = conn.execute(
            """
            SELECT id, entry_date, entry_price, position_side, stop_price, status
            FROM bitget_forward_trades
            WHERE market_type=? AND symbol=? AND status='OPEN'
            ORDER BY id DESC LIMIT 1
            """,
            (market_type, symbol),
        ).fetchone()
        if not row:
            return None
        return {
            "id": int(row[0]),
            "entry_date": row[1],
            "entry_price": float(row[2] or 0.0),
            "position_side": str(row[3] or "LONG").upper(),
            "stop_price": float(row[4] or 0.0),
            "status": row[5],
        }
    finally:
        conn.close()


def _evaluate_cate_exit(*, position_side: str) -> dict:
    """CAT-E 3파일 evaluate 원본 호출 — 바마다 재평가 (원본 diff 없음)."""
    from bitget.trading.mega_trend_kill_bg import evaluate_crypto_climax_kill_switch
    from bitget.trading.position_manager import normalize_position_side
    from bitget.trading.tail_risk_gate import evaluate_tail_fund_gate

    side = normalize_position_side(position_side)
    kill = evaluate_crypto_climax_kill_switch({}, position_side=side)
    tail = evaluate_tail_fund_gate(0.0, "NORMAL")
    kill_d = kill if isinstance(kill, dict) else {"raw": str(kill)}
    tail_d = tail if isinstance(tail, dict) else {"raw": str(tail)}
    trigger = bool(kill_d.get("kill_active")) or bool(tail_d.get("escalate_block"))
    reason = "CATE_KILL" if kill_d.get("kill_active") else (
        "CATE_TAIL" if tail_d.get("escalate_block") else ""
    )
    return {
        "position_side_norm": side,
        "climax_kill": kill_d,
        "tail_fund_gate": tail_d,
        "trigger": trigger,
        "reason": reason,
    }


def _stop_hit(open_row: dict, high: float, low: float) -> bool:
    """Reuse try_add stop_price (no new threshold invent)."""
    sp = float(open_row.get("stop_price") or 0.0)
    if sp <= 0:
        return False
    side = str(open_row.get("position_side") or "LONG").upper()
    if side == "SHORT":
        return float(high) >= sp
    return float(low) <= sp


def _adapter_close_trade(
    db_path: str,
    *,
    trade_id: int,
    exit_date: str,
    exit_price: float,
    entry_price: float,
    side: str,
    exit_reason: str,
) -> float:
    """CLOSED write on isolated DB only — candle exit_date (wall-clock 아님)."""
    from bitget.infra.shared_db_connector import get_connection

    ep = float(entry_price) or 1.0
    xp = float(exit_price)
    if str(side).upper() == "SHORT":
        ret = round((ep - xp) / ep * 100.0, 4)
    else:
        ret = round((xp - ep) / ep * 100.0, 4)
    status = "CLOSED_WIN" if ret >= 0 else "CLOSED_LOSS"
    conn = get_connection(db_path)
    try:
        conn.execute(
            """
            UPDATE bitget_forward_trades
            SET status=?, exit_date=?, exit_reason=?, final_ret=?
            WHERE id=? AND status='OPEN'
            """,
            (status, exit_date, exit_reason, ret, int(trade_id)),
        )
        conn.commit()
    finally:
        conn.close()
    return ret


def count_forward_trades(db_path: str) -> int:
    if not os.path.isfile(db_path):
        return 0
    from bitget.infra.shared_db_connector import get_connection

    conn = get_connection(db_path, read_only=True)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_forward_trades'"
        ).fetchone()
        if not row:
            return 0
        return int(conn.execute("SELECT COUNT(*) FROM bitget_forward_trades").fetchone()[0])
    finally:
        conn.close()


def run_replay(
    market_type: str,
    symbol: str,
    engine: str,
    start: DateLike,
    end: DateLike,
    db_path: str,
    *,
    market_db: Optional[str] = None,
) -> list[dict]:
    """FULL-BT-HIST-1: real OHLCV bar walk → try_add / CAT-E exit on isolated DB.

    Signature kept batch-compatible (FULL-BT-1/2). Returns event list (entries/exits).
    ``entry_date``/``exit_date`` = candle axis (not wall-clock).
    """
    from bitget.analysis.universe_bt.replay import _bar_ts_from_date, _load_ohlcv
    from bitget.forward.ledger import try_add_virtual_position
    from bitget.trading.execution_safety import ExecutionGateOutcome, GateResult

    mt = _normalize_mt(market_type)
    path = db_path or full_bt_db_path()
    eng_name, eng_fn = _resolve_engine(engine)
    step11 = _adapter_step11_note()
    tf = REUSED_SCANNER_TIMEFRAMES[0]
    start_d = _to_date(start)
    end_d = _to_date(end)
    events: List[dict] = []

    logger.info(
        "full_bt run_replay HIST mt=%s sym=%s engine=%s start=%s end=%s step11=%s ohlcv=%s",
        mt,
        symbol,
        eng_name,
        start_d,
        end_d,
        step11["policy"],
        OHLCV_LOADER,
    )

    if not callable(eng_fn):
        raise TypeError(f"engine not callable: {eng_name}")

    df = _load_ohlcv(str(symbol), mt, db_path=market_db, timeframe=tf)
    if df is None or df.empty:
        logger.warning("full_bt no OHLCV for %s %s", mt, symbol)
        return events

    # Benchmark for engine(window, bench, tf) — BTC close align (U1 선례 재사용)
    bench_df = _load_ohlcv("BTC_USDT", mt, db_path=market_db, timeframe=tf)
    if bench_df is not None and not bench_df.empty:
        idx_close = bench_df.set_index("Date")["Close"].astype(float)
        idx_close.index = pd.to_datetime(idx_close.index)
    else:
        idx_close = pd.Series(dtype=float)

    def _nav_ok(_cfg=None):
        return GateResult(
            ExecutionGateOutcome.APPROVED, "full_bt_hist_nav_ok", {"nav_size_mult": 1.0}
        )

    def _gross_ok(_cfg=None):
        return GateResult(ExecutionGateOutcome.APPROVED, "full_bt_hist_gross_ok", {})

    min_i = max(0, REUSED_MIN_BARS - 1)

    with isolated_full_bt_book(path):
        with ExitStack() as stack:
            stack.enter_context(
                mock.patch(
                    "bitget.trading.execution_safety.evaluate_nav_risk_gate",
                    _nav_ok,
                )
            )
            stack.enter_context(
                mock.patch(
                    "bitget.trading.execution_safety.evaluate_gross_notional_gate",
                    _gross_ok,
                )
            )

            for i in range(min_i, len(df)):
                bar_ts = _bar_ts_from_date(df["Date"].iloc[i])
                bar_d = pd.Timestamp(df["Date"].iloc[i]).date()
                if start_d and bar_d < start_d:
                    continue
                if end_d and bar_d > end_d:
                    break

                window = df.iloc[: i + 1].copy()
                bar_date = window["Date"].iloc[-1]
                candle = _candle_date_str(bar_date)
                o = float(window["Open"].iloc[-1])
                h = float(window["High"].iloc[-1])
                l = float(window["Low"].iloc[-1])
                c = float(window["Close"].iloc[-1])
                _ = o

                if not idx_close.empty:
                    bench = idx_close.reindex(window["Date"]).ffill().bfill()
                else:
                    bench = window["Close"].astype(float)

                # Patch hist loader to window (candle replay, not live table)
                stack_bar = mock.patch(
                    "bitget.forward.ledger._load_hist", return_value=window
                )
                stack_bar.start()
                try:
                    open_row = _load_open_row(path, mt, str(symbol))
                    if open_row is not None:
                        cate = _evaluate_cate_exit(
                            position_side=open_row["position_side"]
                        )
                        do_close = bool(cate["trigger"]) or _stop_hit(open_row, h, l)
                        reason = cate["reason"] or (
                            "STOP_PRICE" if _stop_hit(open_row, h, l) else ""
                        )
                        if do_close:
                            xp = (
                                float(open_row["stop_price"])
                                if reason == "STOP_PRICE"
                                and float(open_row.get("stop_price") or 0) > 0
                                else c
                            )
                            ret = _adapter_close_trade(
                                path,
                                trade_id=int(open_row["id"]),
                                exit_date=candle,
                                exit_price=xp,
                                entry_price=float(open_row["entry_price"]),
                                side=open_row["position_side"],
                                exit_reason=f"FULL_BT_HIST|{reason}|{cate.get('position_side_norm')}",
                            )
                            events.append(
                                {
                                    "event": "exit",
                                    "symbol": str(symbol),
                                    "market_type": mt,
                                    "bar_ts": bar_ts,
                                    "exit_date": candle,
                                    "final_ret": ret,
                                    "reason": reason,
                                }
                            )
                        continue

                    # Flat: CAT-C engine → try_add (step11 N/A)
                    try:
                        hit, sig_type, _out_df, dbg = eng_fn(window, bench, tf)
                    except Exception as ex:
                        logger.debug("engine skip %s %s: %s", eng_name, symbol, ex)
                        continue
                    if not hit:
                        continue
                    dbg = dbg if isinstance(dbg, dict) else {}
                    side = str(dbg.get("side", "LONG")).upper()
                    if side not in ("LONG", "SHORT"):
                        side = "LONG"
                    entry = float(dbg.get("last_close", c) or c)
                    entry_high = float(dbg.get("entry_high", h) or h)
                    score = float(dbg.get("score", 55.0) or 55.0)
                    facts = _facts_from_dbg(dbg)

                    with mock.patch(
                        "bitget.forward.ledger.utc_date_str", return_value=candle
                    ):
                        ok, msg = try_add_virtual_position(
                            market_type=mt,
                            symbol=str(symbol),
                            timeframe=tf,
                            sig_type=f"FULL_BT_HIST|{eng_name}|{sig_type}",
                            score=score,
                            entry_price=entry,
                            facts=facts,
                            side=side,
                            entry_high=entry_high,
                        )
                    if ok:
                        events.append(
                            {
                                "event": "entry",
                                "symbol": str(symbol),
                                "market_type": mt,
                                "bar_ts": bar_ts,
                                "entry_date": candle,
                                "side": side,
                                "engine": eng_name,
                            }
                        )
                    else:
                        logger.debug("try_add reject %s: %s", symbol, msg)
                finally:
                    stack_bar.stop()

    logger.info(
        "full_bt HIST done sym=%s events=%s step11=%s",
        symbol,
        len(events),
        step11["policy"],
    )
    return events


def report_tf_and_funding() -> dict[str, str]:
    """Investigation-only lines for OUTBOX (rule 5 — no invented constants)."""
    from bitget.master_scanner import TIMEFRAMES as MS_TF

    tf_line = f"재사용 TF: {list(MS_TF)}"
    funding_line = (
        "funding: (a) 거래소 API 재조회=fetch_funding_snapshot 실시간만 가능 · "
        "(b) 로컬 OHLCV/별도 funding 히스토리 테이블 미확인 · "
        "(c) 과거 rate 시계열 소스 없음 → FULL-BT-1은 funding 추적 없이 진행 "
        "(P1-3 미차감 라이브 승계, 근사 금지)"
    )
    return {"tf": tf_line, "funding": funding_line, "ohlcv_loader": OHLCV_LOADER}
