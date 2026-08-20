"""Bitget Track B north star panel (read-only)."""
from __future__ import annotations

from bitget.observability import north_star_panel_bg as ns
from bitget.observability import post_deploy_obs_digest_bg as bg


def _fake_ns(**overrides):
    base = {
        "available": True,
        "cadence": "daily",
        "date_kst": "2026-08-20",
        "tracks": {
            "B": {
                "label": "Bitget 코인",
                "phase": "B0",
                "phase_label": "검증·측정",
                "mdd_cap_pct": 5.0,
                "cagr_target_lo": 12.0,
                "cagr_target_hi": 25.0,
                "available": True,
                "c2_funding_complete": False,
                "forward_trades_count": 4,
                "portfolio": {
                    "nav": 10000.0,
                    "spot_nav": 4000.0,
                    "futures_nav": 6000.0,
                    "mdd_tier": "NORMAL",
                },
                "aggregate": {
                    "max_mdd_pct": 2.5,
                    "avg_return_pct": -1.2,
                    "return_pace_score": 0.0,
                    "mdd_safety_score": 80.0,
                    "composite_score": 40.0,
                    "measure_only": True,
                },
            }
        },
        "period_returns": {
            "B": {
                "day_pct": 0.1,
                "week_pct": -0.5,
                "month_pct": -1.0,
                "year_pct": None,
                "total_pct": -1.2,
            }
        },
        "ledger": {"B": {"gate": "G0", "gate_label": "측정·구조"}},
        "meta": {
            "daily_n": 5,
            "daily_snapshot_count": 5,
            "obs_hold_recall_n": 20,
            "obs_hold_remaining": 15,
            "show_r3_bitget_banner": True,
            "r3_banner": "⚠️ Bitget paper 미검증",
            "show_r1_caveat": True,
            "r1_banner": "초기 관측 중",
        },
    }
    base.update(overrides)
    return base


class TestBitgetNorthStarPanel:
    def test_easy_board_and_detail(self):
        snap = _fake_ns()
        d = ns.build_bitget_goal_dashboard(snap)
        assert d["light"] in ("🟢", "🟡", "🔴")
        assert d["mdd_cap"] == 5.0
        assert d["gate"] == "G0"
        assert d["n"] == 5
        html = ns.format_bitget_north_star_html(snap)
        assert "Bitget 북극성" in html
        assert "[쉬운판] Track B" in html
        assert "목표 MDD ≤5%" in html
        assert "연복리 12~25%" in html
        assert "B0" in html
        assert "잘 되고 있어요" in html
        assert "나중이에요" in html
        assert "Track B · Bitget" in html

    def test_ledger_unavailable(self):
        d = ns.build_bitget_goal_dashboard({"available": False, "error": "boom"})
        assert d["light"] == "🔴"
        assert "못 읽" in d["headline"]
        html = ns.format_bitget_north_star_html({"available": False, "error": "boom", "date_kst": "2026-08-20"})
        assert "Bitget 북극성" in html
        assert "boom" in html

    def test_mdd_over_cap_is_problem(self):
        snap = _fake_ns()
        snap["tracks"]["B"]["aggregate"]["max_mdd_pct"] = 6.0
        d = ns.build_bitget_goal_dashboard(snap)
        assert any(x["id"] == "mdd" for x in d["problem"])


class TestPostDeployObsNorthStarWire:
    def test_compute_includes_north_star(self, tmp_path, monkeypatch):
        fwd = str(tmp_path / "m.sqlite")
        # minimal fwd table via existing helper pattern
        import sqlite3

        conn = sqlite3.connect(fwd)
        conn.execute(
            "CREATE TABLE bitget_forward_trades ("
            "id INTEGER PRIMARY KEY, entry_date TEXT, market_type TEXT, "
            "symbol TEXT, status TEXT, timeframe TEXT, mfe REAL)"
        )
        conn.execute(
            "INSERT INTO bitget_forward_trades (entry_date, market_type, symbol, status) "
            "VALUES (?,?,?,?)",
            ("2026-08-17T00:00:00+00:00", "futures", "BTC_USDT", "OPEN"),
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr(
            "bitget.observability.gmm_dna_alpha_report_bg._dna_rank_and_shape",
            lambda: ({"RANK1": True, "RANK2": False, "RANK3": False}, {}),
        )
        monkeypatch.setattr(
            bg,
            "collect_dna_diagnosis",
            lambda **_k: {
                "state": "DATA_WAIT_LOW_MFE",
                "cursor_action": "OBSERVE_HOLD",
                "plain": "wait",
                "checked_at": "t",
                "n_closed_by_tf": {},
                "n_mfe8_by_tf": {},
                "templates_present": False,
                "gmm_cluster_n": 0,
                "last_error": None,
                "gmm_min_rows": 12,
            },
        )
        monkeypatch.setattr(
            "bitget.observability.north_star_panel_bg.collect_bitget_north_star_snap",
            lambda **_k: _fake_ns(),
        )
        snap = bg.compute_post_deploy_obs_digest(
            window_days=2,
            forward_db_path=fwd,
            log_text="Cos_eff=0.5\n",
            include_server_probes=False,
        )
        assert "north_star" in snap
        assert snap["north_star"]["available"] is True
        assert snap["north_star"]["dashboard"]["gate"] == "G0"
        assert "목표 MDD" in ns.format_bitget_north_star_html(snap["north_star"]["_snap"])
