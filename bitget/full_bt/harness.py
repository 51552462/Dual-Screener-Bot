"""FULL-BT-1 read-only harness — CAT-C/D/E import only; isolated DB write.

IV L1 전체이식 가상매매 — 격리 리플레이 결과, LIVE 승격·R6 대체·B1「달성」 판정 금지.
"""
from __future__ import annotations

import os
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timezone
from typing import Any, Iterator, Optional
from unittest import mock

from bitget.full_bt.paths import full_bt_db_path
from bitget.infra.logging_setup import get_logger

logger = get_logger("bitget.full_bt.harness")

# Live scanner TF SSOT (report-only; no invention) — master_scanner / auto_pilot
REUSED_SCANNER_TIMEFRAMES = ["1D", "4H", "2H", "1H"]
STEP11_POLICY = "N/A_skip"  # execution_safety real-only — Adapter context, not gate edit


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
    # default first base engine
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


def _invoke_exit_imports(*, market_type: str, symbol: str) -> dict:
    """CAT-E read-only import smoke — evaluate entrypoints, no original rewrite."""
    _ = (market_type, symbol)
    from bitget.trading.mega_trend_kill_bg import evaluate_crypto_climax_kill_switch
    from bitget.trading.position_manager import normalize_position_side
    from bitget.trading.tail_risk_gate import evaluate_tail_fund_gate

    side = normalize_position_side("LONG")
    kill = evaluate_crypto_climax_kill_switch({}, position_side=side)
    tail = evaluate_tail_fund_gate(0.0, "NORMAL")
    return {
        "position_side_norm": side,
        "climax_kill": kill if isinstance(kill, dict) else {"raw": str(kill)},
        "tail_fund_gate": tail if isinstance(tail, dict) else {"raw": str(tail)},
    }


def _adapter_close_open_rows(db_path: str, *, exit_reason: str) -> int:
    """CLOSED write routed to isolated DB only (ledger.py logic untouched)."""
    from bitget.infra.shared_db_connector import get_connection

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_connection(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bitget_forward_trades
            SET status='CLOSED_WIN', exit_date=?, exit_reason=?, final_ret=0.0
            WHERE status='OPEN'
            """,
            (now, exit_reason),
        )
        n = int(cur.rowcount or 0)
        conn.commit()
        return n
    finally:
        conn.close()


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
    start: date,
    end: date,
    db_path: str,
) -> None:
    """FULL-BT-1 entry: CAT-C candidate → try_add (step11 N/A) → CAT-E import → CLOSED on isolated DB.

    ``start``/``end`` reserved for FULL-BT-2 bar walk; smoke may use same-day window.
    """
    _ = (start, end)  # window wired in FULL-BT-2
    mt = str(market_type).strip().lower()
    if mt in ("fut", "linear"):
        mt = "futures"
    path = db_path or full_bt_db_path()
    eng_name, eng_fn = _resolve_engine(engine)
    step11 = _adapter_step11_note()
    logger.info(
        "full_bt run_replay mt=%s sym=%s engine=%s step11=%s tf_reused=%s",
        mt,
        symbol,
        eng_name,
        step11["policy"],
        REUSED_SCANNER_TIMEFRAMES,
    )

    if not callable(eng_fn):
        raise TypeError(f"engine not callable: {eng_name}")

    import pandas as pd

    from bitget.forward.ledger import try_add_virtual_position
    from bitget.trading.execution_safety import ExecutionGateOutcome, GateResult

    def _nav_ok(_cfg=None):
        return GateResult(
            ExecutionGateOutcome.APPROVED, "full_bt_smoke_nav_ok", {"nav_size_mult": 1.0}
        )

    def _gross_ok(_cfg=None):
        return GateResult(ExecutionGateOutcome.APPROVED, "full_bt_smoke_gross_ok", {})

    # Synthetic hist for smoke when market OHLCV absent — Adapter only, not a new TF invent
    dates = pd.date_range("2025-01-01", periods=120, freq="D")
    hist = pd.DataFrame(
        {
            "Date": dates,
            "Open": 100.0,
            "High": 101.0,
            "Low": 99.0,
            "Close": 100.0,
            "Volume": 1e6,
        }
    )

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
            stack.enter_context(
                mock.patch("bitget.forward.ledger._load_hist", return_value=hist)
            )
            # step11 real-only chain: never call run_pre_execution_gates here (N/A skip)
            ok, msg = try_add_virtual_position(
                market_type=mt,
                symbol=str(symbol),
                timeframe=REUSED_SCANNER_TIMEFRAMES[0],
                sig_type=f"FULL_BT_SMOKE|{eng_name}",
                score=55.0,
                entry_price=100.0,
                facts={
                    "v_cpv": 0.0,
                    "v_yang": 0.0,
                    "v_energy": 0.0,
                    "v_rs": 0.0,
                    "trade_value_24h": 1e7,
                    "marcap_eok": 0.0,
                    "entry_cos_score": 0.9,
                    "entry_dtw_score": 0.1,
                },
                side="LONG",
                entry_high=101.0,
            )
        logger.info("try_add ok=%s msg=%s step11=%s", ok, msg, step11["policy"])
        exit_meta = _invoke_exit_imports(market_type=mt, symbol=str(symbol))
        closed_n = 0
        if ok:
            closed_n = _adapter_close_open_rows(
                path,
                exit_reason=f"FULL_BT_SMOKE_CLOSE|{exit_meta.get('position_side_norm')}",
            )
        logger.info("exit_imports=%s closed_rows=%s", exit_meta.keys(), closed_n)
        if not ok:
            raise RuntimeError(f"full_bt smoke try_add failed: {msg}")


def report_tf_and_funding() -> dict[str, str]:
    """Investigation-only lines for OUTBOX (rule 5 — no invented constants)."""
    from bitget.master_scanner import TIMEFRAMES as MS_TF

    tf_line = f"재사용 TF: {list(MS_TF)}"
    # funding: live snapshot API exists; historical rate series not in local OHLCV schema
    funding_line = (
        "funding: (a) 거래소 API 재조회=fetch_funding_snapshot 실시간만 가능 · "
        "(b) 로컬 OHLCV/별도 funding 히스토리 테이블 미확인 · "
        "(c) 과거 rate 시계열 소스 없음 → FULL-BT-1은 funding 추적 없이 진행 "
        "(P1-3 미차감 라이브 승계, 근사 금지)"
    )
    return {"tf": tf_line, "funding": funding_line}
