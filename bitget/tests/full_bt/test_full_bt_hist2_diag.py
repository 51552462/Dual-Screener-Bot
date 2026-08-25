"""FULL-BT-HIST-2 — engine_hit / gate_reject diag (read-only harness wrappers)."""
from __future__ import annotations

from unittest import mock

import pytest

from bitget.forward.shared import _init_forward_db_schema
from bitget.infra.shared_db_connector import get_connection


def _init_full(path: str) -> None:
    conn = get_connection(path)
    try:
        _init_forward_db_schema(conn)
        conn.commit()
    finally:
        conn.close()


@pytest.mark.parametrize(
    "msg,step",
    [
        ("🚫 글로벌 서킷 브레이커 ON", 1),
        ("🛑 둠스데이 DEFCON — 신규 LONG 차단", 2),
        ("ANTI_PATTERNS 차단: 참사 DNA 유사도 0.9", 3),
        ("중복 보유 중", 5),
        ("🚨 시장 쿼터 초과", 6),
        ("🚨 BTC-proxy 집중도 상한 (concentration cap)", 8),
        ("예수금 부족: [CORE] 가용 자산 없음", 9),
        ("ATR 계산용 히스토리 부족", 10),
        ("수량 산출 실패", 10),
        ("알려지지 않은 거절 XYZ", 0),
    ],
)
def test_map_reject_msg_to_step(msg, step):
    from bitget.full_bt.harness import map_reject_msg_to_step

    assert map_reject_msg_to_step(msg) == step


def test_diag_records_engine_hit_and_reject(tmp_path):
    """Fake hit + try_add reject → full_bt_diag rows; trade schema untouched."""
    import pandas as pd

    from bitget.full_bt import harness as H

    market = str(tmp_path / "market.sqlite")
    full = str(tmp_path / "bitget_full_bt.sqlite")
    _init_full(full)

    # minimal OHLCV seed (reuse hist1 helper pattern)
    import sqlite3

    conn = sqlite3.connect(market)
    try:
        conn.execute(
            'CREATE TABLE "BITGET_SPOT_BTC_USDT_1D" '
            "(Date TEXT, Open REAL, High REAL, Low REAL, Close REAL, Volume REAL)"
        )
        dates = pd.date_range("2025-01-01", periods=80, freq="D")
        for i, d in enumerate(dates):
            px = 100.0 + i * 0.1
            conn.execute(
                'INSERT INTO "BITGET_SPOT_BTC_USDT_1D" VALUES (?,?,?,?,?,?)',
                (d.strftime("%Y-%m-%d"), px, px * 1.01, px * 0.99, px, 1e6),
            )
        conn.commit()
    finally:
        conn.close()

    def _hit_engine(window, bench, tf):
        c = float(window["Close"].iloc[-1])
        return True, "TEST", window, {
            "side": "LONG",
            "last_close": c,
            "entry_high": c * 1.01,
            "score": 60.0,
            "trade_value_24h": 1e7,
        }

    rid = "hist2-diag-ut"

    with mock.patch.object(H, "REUSED_MIN_BARS", 60), mock.patch.object(
        H, "_resolve_engine", return_value=("MASTER", _hit_engine)
    ), mock.patch(
        "bitget.forward.ledger.try_add_virtual_position",
        return_value=(False, "ATR 계산용 히스토리 부족"),
    ):
        H.run_replay(
            "spot",
            "BTC_USDT",
            "MASTER",
            "2025-01-01",
            "2025-12-31",
            full,
            market_db=market,
            run_id=rid,
        )

    summary = H.summarize_diag(full, rid, "spot")
    assert summary["engine_hit_total"] >= 1
    assert summary["gate_reject_total"] >= 1
    assert summary["gate_reject_count"].get(10, 0) >= 1
    assert "MASTER" in summary["engine_hit_count"]
    assert summary["engine_hit_count"]["MASTER"].get("BTC_USDT", 0) >= 1

    # result trade table must remain empty (reject path)
    conn = get_connection(full, read_only=True)
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM bitget_forward_trades WHERE status!='OPEN' OR 1=1"
        ).fetchone()[0]
        # schema exists; no successful entry expected
        open_n = conn.execute(
            "SELECT COUNT(*) FROM bitget_forward_trades WHERE status='OPEN'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert open_n == 0
    assert n == 0


def test_ensure_diag_schema_idempotent(tmp_path):
    from bitget.full_bt.harness import ensure_diag_schema, record_diag, summarize_diag

    path = str(tmp_path / "bitget_full_bt.sqlite")
    ensure_diag_schema(path)
    ensure_diag_schema(path)
    record_diag(
        path,
        run_id="r1",
        market_type="futures",
        metric="engine_hit",
        symbol="ETH_USDT",
        engine_name="V1",
    )
    s = summarize_diag(path, "r1", "futures")
    assert s["engine_hit_total"] == 1
    assert s["market_type"] == "futures"
