"""execution_bridge.py 누락 import 회귀 테스트 + 실전 vs 가상 리더보드 연동 테스트.

과거 버그: `re`, `json`, `numpy`, `_extract_core_group` 를 import하지 않아
`log_real_execution`/`build_practitioner_reality_leaderboard` 호출 시 NameError.
master_scanner.py 쪽 호출부가 try/except 로 감싸고 있어 조용히 삼켜졌고, 그
결과 `bitget_real_execution` 테이블이 항상 비어 있었다. 또한 이 리더보드는
어떤 리포트에서도 호출되지 않는 죽은 기능이었다.
"""
from __future__ import annotations

import os
import tempfile
from unittest import mock

import pandas as pd
import pytest


class TestLogRealExecutionNoNameError:
    def test_log_real_execution_inserts_row_without_nameerror(self):
        from bitget.forward import execution_bridge, shared

        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "bitget_market_data.sqlite")
            with mock.patch.object(shared, "DB_PATH", db_path), mock.patch.object(
                execution_bridge, "DB_PATH", db_path
            ):
                execution_bridge.log_real_execution(
                    market_type="spot",
                    symbol="BTC_USDT",
                    timeframe="1D",
                    engine_name="MASTER",
                    sig_type="[STANDARD][PRACT_01] S1",
                    side="LONG",
                    amount=0.01,
                    leverage=1.0,
                    entry_price=50000.0,
                    exec_result={"ok": True, "status": "filled", "realized_ret_pct": 1.5, "realized_pnl_usdt": 5.0},
                    virtual_trade_id=1,
                )
                import sqlite3

                conn = sqlite3.connect(db_path)
                row = conn.execute(
                    "SELECT practitioner_key, exec_ok, realized_ret_pct FROM bitget_real_execution"
                ).fetchone()
                conn.close()
        assert row is not None
        assert row[0] == "PRACT_01"
        assert row[1] == 1
        assert row[2] == pytest.approx(1.5)

    def test_extract_practitioner_key_uses_shared_core_group_fallback(self):
        from bitget.forward.execution_bridge import _extract_practitioner_key

        assert _extract_practitioner_key("[STANDARD][PRACT_07] S1") == "PRACT_07"
        # PRACT_xx 패턴이 없으면 gates._extract_core_group 폴백으로 NameError 없이 처리돼야 함.
        assert _extract_practitioner_key("[STANDARD][MASTER] S1") != ""


class TestBuildPractitionerRealityLeaderboard:
    def test_leaderboard_computes_without_nameerror(self):
        from bitget.forward import execution_bridge, shared

        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "bitget_market_data.sqlite")
            with mock.patch.object(shared, "DB_PATH", db_path), mock.patch.object(
                execution_bridge, "DB_PATH", db_path
            ):
                for i in range(3):
                    execution_bridge.log_real_execution(
                        market_type="spot",
                        symbol="BTC_USDT",
                        timeframe="1D",
                        engine_name="MASTER",
                        sig_type="[STANDARD][PRACT_01] S1",
                        side="LONG",
                        amount=0.01,
                        leverage=1.0,
                        entry_price=50000.0,
                        exec_result={
                            "ok": True,
                            "status": "filled",
                            "realized_ret_pct": 1.0 + i,
                            "realized_pnl_usdt": 5.0,
                        },
                        virtual_trade_id=0,
                    )
                df = execution_bridge.build_practitioner_reality_leaderboard(market_type="spot")
        assert not df.empty
        assert "reality_score" in df.columns
        row = df.iloc[0]
        assert row["practitioner_key"] == "PRACT_01"
        assert row["samples"] == 3


class TestPractitionerRealityLeaderboardFormatting:
    def test_format_empty_df_returns_empty_string(self):
        from bitget.forward.reports import _format_practitioner_reality_leaderboard_html

        assert _format_practitioner_reality_leaderboard_html(pd.DataFrame()) == ""
        assert _format_practitioner_reality_leaderboard_html(None) == ""

    def test_format_nonempty_df_includes_key_fields(self):
        from bitget.forward.reports import _format_practitioner_reality_leaderboard_html

        df = pd.DataFrame(
            [
                {
                    "market_type": "SPOT",
                    "practitioner_key": "PRACT_01",
                    "samples": 5,
                    "exec_ok": 5,
                    "real_ret_pct": 1.23,
                    "virtual_ret_pct": 2.0,
                    "reality_gap_pct": -0.77,
                    "notional_usdt": 1000.0,
                    "reality_score": 0.5,
                }
            ]
        )
        html = _format_practitioner_reality_leaderboard_html(df)
        assert "PRACT_01" in html
        assert "SPOT" in html
        assert "1.23" in html


class TestSendGroupPractitionerReportsCallsLeaderboard:
    def test_send_group_practitioner_reports_invokes_leaderboard(self):
        """send_group_practitioner_reports() 가 리더보드를 실제로 호출·전송하는지 확인
        (과거엔 build_practitioner_reality_leaderboard 가 import만 되고 어디서도
        호출되지 않는 죽은 기능이었다)."""
        from bitget.forward import reports

        sent_messages = []
        leaderboard_df = pd.DataFrame(
            [
                {
                    "market_type": "SPOT",
                    "practitioner_key": "PRACT_01",
                    "samples": 5,
                    "exec_ok": 5,
                    "real_ret_pct": 1.0,
                    "virtual_ret_pct": 1.0,
                    "reality_gap_pct": 0.0,
                    "notional_usdt": 100.0,
                    "reality_score": 0.1,
                }
            ]
        )
        with mock.patch.object(reports, "init_forward_db"), mock.patch.object(
            reports, "sync_real_leaderboard_with_virtual"
        ), mock.patch.object(reports, "load_system_config", return_value={}), mock.patch(
            "bitget.forward.practitioner_bitget_adapter.send_bitget_practitioner_reports_pil",
            return_value={"briefs": 0, "penalties": {}},
        ), mock.patch.object(
            reports, "build_practitioner_reality_leaderboard", return_value=leaderboard_df
        ) as mock_lb, mock.patch.object(
            reports, "send_telegram_msg", side_effect=lambda m: sent_messages.append(m)
        ):
            reports.send_group_practitioner_reports()

        mock_lb.assert_called_once()
        assert any("PRACT_01" in m for m in sent_messages)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
