"""FULL-BT harness — CAT-C/D/E import only; isolated DB write.

IV L1 전체이식 가상매매 — 격리 리플레이 결과, LIVE 승격·R6 대체·B1「달성」 판정 금지.

FULL-BT-HIST-1: run_replay uses real OHLCV bar walk (universe_bt._load_ohlcv reuse).
FULL-BT-HIST-2: harness wrappers → full_bt_diag (engine_hit / gate_reject); CAT-C/D 원본 비접촉.
FULL-BT-HIST-3: engine_call / outcome(candidate|none|exception) / tf_ohlcv_coverage — full_bt_diag 확장.
"""
from __future__ import annotations

import os
from collections import Counter
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

# FULL-BT-HIST-2/3 — 진단 전용 테이블 (결과 스키마 비접촉). HIST-3: tf 컬럼 확장.
_DIAG_SCHEMA = """
CREATE TABLE IF NOT EXISTS full_bt_diag (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL DEFAULT '',
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL DEFAULT '',
    metric TEXT NOT NULL,
    engine_name TEXT NOT NULL DEFAULT '',
    step INTEGER,
    count INTEGER NOT NULL DEFAULT 1,
    detail TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
"""


def ensure_diag_schema(db_path: Optional[str] = None) -> str:
    """Create full_bt_diag; HIST-3 adds optional ``tf`` column (ALTER, no new table)."""
    path = db_path or full_bt_db_path()
    from bitget.infra.shared_db_connector import get_connection

    conn = get_connection(path)
    try:
        conn.executescript(_DIAG_SCHEMA)
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(full_bt_diag)").fetchall()}
        if "tf" not in cols:
            conn.execute(
                "ALTER TABLE full_bt_diag ADD COLUMN tf TEXT NOT NULL DEFAULT ''"
            )
        conn.commit()
    finally:
        conn.close()
    return path


def map_reject_msg_to_step(msg: str) -> int:
    """CAT-D §4 step 1~10; unmapped → 0 (invent 금지, 관측만)."""
    s = str(msg or "")
    for step, keys in _REJECT_STEP_KEYWORDS:
        for k in keys:
            if k.lower() in s.lower() or k in s:
                return int(step)
    return 0


# CAT-D §4 거절 메시지 → step 매핑 (원본 미수정 · 관측만). 미매칭=0
# 키워드 = ledger try_add 실제 반환문 기준 (broad false-positive 금지)
_REJECT_STEP_KEYWORDS: list[tuple[int, tuple[str, ...]]] = [
    (1, ("서킷", "circuit")),
    (2, ("둠스데이", "doomsday", "DEFCON", "defcon")),
    (3, ("ANTI_PATTERNS", "참사 DNA", "시계열 게이트")),
    (4, ("KILL_SWITCH", "킬스위치")),
    (5, ("중복 보유", "중복")),
    (6, ("시장 쿼터", "max open", "보유 한도")),
    (7, ("daily", "일일 진입", "per-logic")),
    (8, ("집중도", "concentration", "BTC-proxy", "명목노출")),
    (9, ("예수금", "잔고 부족", "가용 자산", "treasury")),
    (
        10,
        (
            "수량 산출",
            "ATR",
            "히스토리 부족",
            "리스크 거리",
            "배드틱",
            "price_sanity",
            "notional",
        ),
    ),
]


def record_diag(
    db_path: str,
    *,
    run_id: str,
    market_type: str,
    metric: str,
    symbol: str = "",
    engine_name: str = "",
    step: Optional[int] = None,
    count: int = 1,
    detail: str = "",
    tf: str = "",
) -> None:
    """Insert one diag row into full_bt_diag (harness Adapter only)."""
    if int(count) <= 0 and metric != "tf_ohlcv_coverage":
        return
    ensure_diag_schema(db_path)
    from bitget.infra.clock import utc_datetime_str
    from bitget.infra.shared_db_connector import get_connection

    conn = get_connection(db_path)
    try:
        conn.execute(
            """
            INSERT INTO full_bt_diag (
                run_id, market_type, symbol, metric, engine_name, step, count, detail, updated_at, tf
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(run_id or ""),
                str(market_type).lower(),
                str(symbol or ""),
                str(metric),
                str(engine_name or ""),
                step,
                int(count),
                str(detail or "")[:500],
                utc_datetime_str(),
                str(tf or ""),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def summarize_diag(db_path: str, run_id: str, market_type: str) -> dict[str, Any]:
    """Read-only aggregate for OUTBOX (SPOT/FUT 분리). HIST-2 + HIST-3 keys."""
    ensure_diag_schema(db_path)
    mt = str(market_type).lower()
    from bitget.infra.shared_db_connector import get_connection

    conn = get_connection(db_path, read_only=True)
    try:
        hits = conn.execute(
            """
            SELECT engine_name, symbol, SUM(count)
            FROM full_bt_diag
            WHERE run_id=? AND market_type=? AND metric='engine_hit'
            GROUP BY engine_name, symbol
            """,
            (run_id, mt),
        ).fetchall()
        rejects = conn.execute(
            """
            SELECT COALESCE(step, 0), SUM(count)
            FROM full_bt_diag
            WHERE run_id=? AND market_type=? AND metric='gate_reject'
            GROUP BY COALESCE(step, 0)
            ORDER BY 1
            """,
            (run_id, mt),
        ).fetchall()
        calls = conn.execute(
            """
            SELECT engine_name, symbol, COALESCE(tf, ''), SUM(count)
            FROM full_bt_diag
            WHERE run_id=? AND market_type=? AND metric='engine_call'
            GROUP BY engine_name, symbol, COALESCE(tf, '')
            """,
            (run_id, mt),
        ).fetchall()
        outcomes = conn.execute(
            """
            SELECT metric, engine_name, symbol, COALESCE(tf, ''), SUM(count),
                   GROUP_CONCAT(DISTINCT detail)
            FROM full_bt_diag
            WHERE run_id=? AND market_type=? AND metric LIKE 'engine_outcome_%'
            GROUP BY metric, engine_name, symbol, COALESCE(tf, '')
            """,
            (run_id, mt),
        ).fetchall()
        cov = conn.execute(
            """
            SELECT COALESCE(tf, ''), MAX(count), GROUP_CONCAT(DISTINCT symbol)
            FROM full_bt_diag
            WHERE run_id=? AND market_type=? AND metric='tf_ohlcv_coverage'
            GROUP BY COALESCE(tf, '')
            """,
            (run_id, mt),
        ).fetchall()
    finally:
        conn.close()

    engine_hit_count: dict[str, dict[str, int]] = {}
    for eng, sym, n in hits:
        engine_hit_count.setdefault(str(eng), {})[str(sym)] = int(n or 0)
    gate_reject_count = {int(s): int(n or 0) for s, n in rejects}

    engine_call_count: dict[str, dict[str, dict[str, int]]] = {}
    call_total = 0
    for eng, sym, tfx, n in calls:
        n_i = int(n or 0)
        call_total += n_i
        engine_call_count.setdefault(str(eng), {}).setdefault(str(sym), {})[
            str(tfx or "")
        ] = n_i

    engine_call_outcome: dict[str, dict[str, dict[str, dict[str, int]]]] = {}
    outcome_totals = {"candidate": 0, "none": 0, "exception": 0}
    exception_types: dict[str, int] = {}
    for metric, eng, sym, tfx, n, detail in outcomes:
        key = str(metric).replace("engine_outcome_", "")
        n_i = int(n or 0)
        outcome_totals[key] = outcome_totals.get(key, 0) + n_i
        engine_call_outcome.setdefault(str(eng), {}).setdefault(str(sym), {}).setdefault(
            str(tfx or ""), {}
        )[key] = n_i
        if key == "exception" and detail:
            for part in str(detail).split(","):
                p = part.strip()
                if p:
                    exception_types[p] = exception_types.get(p, 0) + n_i

    tf_ohlcv_coverage: dict[str, bool] = {}
    for tfx, mx, _syms in cov:
        # count==1 means present for that probe row; OR across symbols via MAX
        tf_ohlcv_coverage[str(tfx or "")] = bool(int(mx or 0) > 0)

    return {
        "run_id": run_id,
        "market_type": mt,
        "engine_hit_count": engine_hit_count,
        "gate_reject_count": gate_reject_count,
        "engine_hit_total": sum(sum(v.values()) for v in engine_hit_count.values()),
        "gate_reject_total": sum(gate_reject_count.values()),
        # HIST-3
        "engine_call_count": engine_call_count,
        "engine_call_total": call_total,
        "engine_call_outcome": engine_call_outcome,
        "engine_call_outcome_totals": outcome_totals,
        "engine_exception_types": exception_types,
        "tf_ohlcv_coverage": tf_ohlcv_coverage,
        "walk_tf": REUSED_SCANNER_TIMEFRAMES[0],
        "probed_tfs": list(REUSED_SCANNER_TIMEFRAMES),
    }


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
    run_id: Optional[str] = None,
) -> list[dict]:
    """FULL-BT-HIST-1: real OHLCV bar walk → try_add / CAT-E exit on isolated DB.

    FULL-BT-HIST-2/3: harness diag → ``full_bt_diag`` (hit/reject + call/outcome/tf).
    Signature kept batch-compatible. ``entry_date``/``exit_date`` = candle axis.
    """
    from bitget.analysis.universe_bt.replay import _bar_ts_from_date, _load_ohlcv
    from bitget.forward.ledger import try_add_virtual_position
    from bitget.trading.execution_safety import ExecutionGateOutcome, GateResult

    mt = _normalize_mt(market_type)
    path = db_path or full_bt_db_path()
    rid = str(run_id or os.environ.get("BITGET_FULL_BT_DIAG_RUN_ID") or "")
    eng_name, eng_fn = _resolve_engine(engine)
    step11 = _adapter_step11_note()
    tf = REUSED_SCANNER_TIMEFRAMES[0]
    start_d = _to_date(start)
    end_d = _to_date(end)
    events: List[dict] = []
    ensure_diag_schema(path)

    logger.info(
        "full_bt run_replay HIST mt=%s sym=%s engine=%s start=%s end=%s step11=%s ohlcv=%s run_id=%s",
        mt,
        symbol,
        eng_name,
        start_d,
        end_d,
        step11["policy"],
        OHLCV_LOADER,
        rid or "-",
    )

    if not callable(eng_fn):
        raise TypeError(f"engine not callable: {eng_name}")

    # HIST-3: TF coverage probe (loader read-only; no loader edit)
    for probe_tf in REUSED_SCANNER_TIMEFRAMES:
        probe_df = _load_ohlcv(
            str(symbol), mt, db_path=market_db, timeframe=probe_tf
        )
        present = probe_df is not None and not probe_df.empty
        record_diag(
            path,
            run_id=rid,
            market_type=mt,
            metric="tf_ohlcv_coverage",
            symbol=str(symbol),
            tf=str(probe_tf),
            count=1 if present else 0,
            detail=f"bars={0 if not present else len(probe_df)}",
        )

    df = _load_ohlcv(str(symbol), mt, db_path=market_db, timeframe=tf)
    if df is None or df.empty:
        logger.warning("full_bt no OHLCV for %s %s", mt, symbol)
        # HIST-3: call_count=0 explicit (경로 미실행 vs none 구분)
        record_diag(
            path,
            run_id=rid,
            market_type=mt,
            metric="engine_call",
            symbol=str(symbol),
            engine_name=eng_name,
            tf=tf,
            count=0,
            detail="no_walk_ohlcv",
        )
        for kind in ("candidate", "none", "exception"):
            record_diag(
                path,
                run_id=rid,
                market_type=mt,
                metric=f"engine_outcome_{kind}",
                symbol=str(symbol),
                engine_name=eng_name,
                tf=tf,
                count=0,
            )
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
    # HIST-3: in-memory flush per symbol (avoid per-bar INSERT storm)
    call_n = 0
    outcome_n = {"candidate": 0, "none": 0, "exception": 0}
    exc_types: Counter[str] = Counter()

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

                    # Flat: CAT-C engine → try_add (step11 N/A) + HIST-2/3 diag
                    call_n += 1
                    try:
                        hit, sig_type, _out_df, dbg = eng_fn(window, bench, tf)
                    except Exception as ex:
                        outcome_n["exception"] += 1
                        exc_types[type(ex).__name__] += 1
                        logger.debug("engine skip %s %s: %s", eng_name, symbol, ex)
                        continue
                    if not hit:
                        outcome_n["none"] += 1
                        continue
                    outcome_n["candidate"] += 1
                    # HIST-2: candidate 생성 계측 (원본 엔진 미수정)
                    record_diag(
                        path,
                        run_id=rid,
                        market_type=mt,
                        metric="engine_hit",
                        symbol=str(symbol),
                        engine_name=eng_name,
                        count=1,
                        detail=str(sig_type or "")[:200],
                        tf=tf,
                    )
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
                        # HIST-2: try_add 거절 계측 (반환 관측만)
                        step = map_reject_msg_to_step(str(msg))
                        record_diag(
                            path,
                            run_id=rid,
                            market_type=mt,
                            metric="gate_reject",
                            symbol=str(symbol),
                            engine_name=eng_name,
                            step=step,
                            count=1,
                            detail=str(msg or "")[:500],
                            tf=tf,
                        )
                        logger.debug("try_add reject %s step=%s: %s", symbol, step, msg)
                finally:
                    stack_bar.stop()

    # HIST-3 flush (call / outcome) — once per symbol walk
    record_diag(
        path,
        run_id=rid,
        market_type=mt,
        metric="engine_call",
        symbol=str(symbol),
        engine_name=eng_name,
        tf=tf,
        count=call_n,
    )
    for kind, n in outcome_n.items():
        record_diag(
            path,
            run_id=rid,
            market_type=mt,
            metric=f"engine_outcome_{kind}",
            symbol=str(symbol),
            engine_name=eng_name,
            tf=tf,
            count=int(n),
            detail=",".join(sorted(exc_types.keys()))
            if kind == "exception" and exc_types
            else "",
        )

    logger.info(
        "full_bt HIST done sym=%s events=%s step11=%s calls=%s outcome=%s",
        symbol,
        len(events),
        step11["policy"],
        call_n,
        dict(outcome_n),
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
