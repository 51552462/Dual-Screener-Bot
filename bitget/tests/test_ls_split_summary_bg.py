"""LS-GOAL-UX-01 — LONG/SHORT display split (read-only)."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from bitget.observability import ls_split_summary_bg as ls
from bitget.observability import north_star_panel_bg as ns
from bitget.observability import post_deploy_obs_digest_bg as bg


def _seed_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE bitget_forward_trades (
            id INTEGER PRIMARY KEY,
            position_side TEXT,
            status TEXT,
            exit_date TEXT,
            final_ret REAL,
            sim_kelly_invest REAL
        )
        """
    )
    rows = [
        ("LONG", "OPEN", None, None, None),
        ("LONG", "CLOSED_WIN", "2026-08-23", 2.0, 100.0),
        ("LONG", "CLOSED_LOSS", "2026-08-20", -1.0, 100.0),
        ("SHORT", "OPEN", None, None, None),
        ("SHORT", "CLOSED_LOSS", "2026-08-23", -3.0, 50.0),
    ]
    for i, (side, st, ex, fr, inv) in enumerate(rows, start=1):
        conn.execute(
            "INSERT INTO bitget_forward_trades VALUES (?,?,?,?,?,?)",
            (i, side, st, ex, fr, inv),
        )
    conn.commit()
    conn.close()


def test_collect_ls_split_summary_keys_and_blocked_import(tmp_path, monkeypatch):
    db = tmp_path / "md.sqlite"
    _seed_db(db)
    sf = {"blocked_short_total": 7, "blocked_short_by_bucket": {"cos_gate": 7}}
    out = ls.collect_ls_split_summary(
        forward_db_path=str(db),
        short_funnel=sf,
        today_kst="2026-08-23",
    )
    assert "LONG" in out and "SHORT" in out
    assert "blocked_today" not in out["LONG"]
    assert out["SHORT"]["blocked_today"] == 7
    assert out["LONG"]["open_count"] == 1
    assert out["LONG"]["win_cum"] == 1
    assert out["LONG"]["loss_cum"] == 1
    assert out["LONG"]["closed_today"] == 1
    assert out["SHORT"]["open_count"] == 1
    assert out["SHORT"]["closed_cum"] == 1
    assert out["SHORT"]["closed_today"] == 1
    # ledger formula: 100*2/100 + 100*(-1)/100 = 1.0 for LONG
    assert out["LONG"]["pnl_cum_usdt"] == 1.0
    assert "SPOT" in (out.get("footnote_spot") or "")


def test_kill_switch_false_skips_ls_line_in_digest(monkeypatch, tmp_path):
    monkeypatch.setenv("POST_DEPLOY_OBS_LS_SPLIT_ENABLED", "0")
    assert ls.ls_split_enabled() is False
    snap = {
        "date_kst": "2026-08-23",
        "checks": {
            "forward_book": {"ok": True, "closed_total": 1, "open_total": 0},
            "cos_eff": {"ok": True, "sample_count": 1},
            "dna_rank": {"ok": True},
            "r01b": {"ok": True, "weekly_report_count": 1},
        },
        "server_ops": {},
    }
    dash = bg.build_kid_dashboard(snap)
    assert not (dash.get("ls_plain") or "").strip()
    html = bg.format_digest_html({**snap, "dashboard": dash})
    assert "롱 OPEN" not in html


def test_north_star_html_includes_ls_when_enabled(monkeypatch):
    monkeypatch.setenv("POST_DEPLOY_OBS_LS_SPLIT_ENABLED", "1")
    fake = {
        "available": True,
        "date_kst": "2026-08-23",
        "tracks": {"B": {}},
        "period_returns": {"B": {}},
        "ledger": {"B": {}},
        "meta": {},
        "ls_split": {
            "LONG": {
                "open_count": 1,
                "closed_today": 0,
                "closed_cum": 2,
                "win_cum": 1,
                "loss_cum": 1,
            },
            "SHORT": {
                "open_count": 0,
                "closed_today": 0,
                "closed_cum": 0,
                "win_cum": 0,
                "loss_cum": 0,
                "blocked_today": 3,
            },
            "detail_hint": "차단 상세 → digest 「숏(선물) 연습」퍼널 참고",
            "footnote_spot": "현물(SPOT)은 구조상 숏 불가 · 숏은 선물만 (SPOT SHORT=0이 정상)",
        },
    }
    html = ns.format_bitget_north_star_html(fake)
    assert "롱 / 숏 진행" in html
    assert "차단 3" in html
    assert "SPOT SHORT=0" in html or "숏 불가" in html
    assert "📊 주식 북극성" not in html
