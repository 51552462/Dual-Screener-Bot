"""듀얼 트랙 LIVE/HIST 쿼리·staleness·포맷 단위 테스트."""
from __future__ import annotations

import sqlite3
from datetime import date

import pandas as pd

from forward_dual_track_queries import (
    assess_live_staleness,
    fetch_hist_baseline_closed,
    fetch_live_today_closed,
    load_dual_track_frames,
    recent_business_day_kst,
    DualTrackQueryMeta,
)
from forward_score_bucket_deep_dive import (
    DualTrackBucketBlock,
    ForwardScoreBucketDeepDive,
    _compute_drift_comment,
    build_dual_track_bucket_blocks,
    format_dual_track_micro_dna_html,
)
def _init_ft(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE forward_trades (
            id INTEGER PRIMARY KEY,
            entry_date TEXT,
            exit_date TEXT,
            market TEXT,
            code TEXT,
            name TEXT,
            sig_type TEXT,
            tier TEXT,
            total_score REAL,
            status TEXT,
            final_ret REAL,
            dyn_cpv REAL,
            dyn_tb REAL,
            v_energy REAL,
            dyn_rs REAL,
            sector TEXT
        )
        """
    )


def test_recent_business_day_skips_weekend():
    sat = date(2026, 5, 23)
    assert recent_business_day_kst(ref=sat).weekday() == 4


def test_live_hist_query_separation():
    conn = sqlite3.connect(":memory:")
    _init_ft(conn)
    rows = [
        ("2026-05-20", "2026-05-20", "KR", "A", "알파", "LIVE", "50점대", 55.0, "CLOSED_WIN", 12.0),
        ("2026-05-21", "2026-05-21", "KR", "B", "베타", "LIVE", "50점대", 52.0, "CLOSED_WIN", 10.0),
        ("2026-05-22", "2026-05-22", "KR", "C", "감마", "LIVE", "50점대", 51.0, "CLOSED_LOSS", -2.0),
        ("2026-05-10", "2026-05-10", "KR", "D", "델타", "SIM", "50점대", 50.0, "CLOSED_WIN", 10.0),
        ("2026-05-11", "2026-05-11", "KR", "E", "엡실", "SIM", "50점대", 49.0, "CLOSED_WIN", 10.0),
        ("2026-05-12", "2026-05-12", "KR", "F", "제타", "SIM", "50점대", 48.0, "CLOSED_WIN", 10.0),
        ("2026-05-13", "2026-05-13", "KR", "G", "에타", "SIM", "50점대", 47.0, "CLOSED_WIN", 10.0),
        ("2026-05-14", "2026-05-14", "KR", "H", "세타", "SIM", "50점대", 46.0, "CLOSED_WIN", 10.0),
    ]
    for i, r in enumerate(rows, 1):
        conn.execute(
            """
            INSERT INTO forward_trades
            (id, entry_date, exit_date, market, code, name, sig_type, tier, total_score,
             status, final_ret, dyn_cpv, dyn_tb, v_energy, dyn_rs, sector)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (i, *r, 0.5, 0.5, 8.0, 0.5, "반도체"),
        )
    conn.commit()

    anchor = "2026-05-22"
    live = fetch_live_today_closed(conn, "KR", anchor)
    hist = fetch_hist_baseline_closed(conn, "KR", anchor, "2026-01-01")
    from forward_dual_track_queries import fetch_champion_rolling_closed

    champ = fetch_champion_rolling_closed(conn, "KR", anchor, "2026-01-01")

    assert len(live) == 1
    assert len(champ) == 8
    assert live.iloc[0]["code"] == "C"
    assert len(hist) == 7
    assert all(h["code"] != "C" for _, h in hist.iterrows())


def test_staleness_when_live_empty():
    meta = DualTrackQueryMeta(
        market="KR",
        calendar_today="2026-05-22",
        anchor_business_day="2026-05-22",
        rolling_cutoff="2026-02-22",
        live_row_count=0,
        hist_row_count=50,
        latest_closed_trade_date="2026-05-21",
    )
    v = assess_live_staleness(meta)
    assert v.is_stale
    assert "당일 실전 데이터 0건" in v.banner_html


def test_dual_track_format_includes_tracks():
    from forward_score_bucket_deep_dive import BucketBlock

    live_b = BucketBlock(
        bucket_label="50점대",
        n_rows=3,
        win_rate_pct=66.7,
        profit_factor=1.5,
        dominant_sector="반도체",
        top_stocks_html="A(+5%)",
        exit_date_min="2026-05-22",
        exit_date_max="2026-05-22",
        key_drivers_html="A(+5%)",
        dna_compact_html="[CPV]",
        dna_contrast_lines=(),
    )
    hist_b = BucketBlock(
        bucket_label="50점대",
        n_rows=20,
        win_rate_pct=19.0,
        profit_factor=1.1,
        dominant_sector="철강",
        top_stocks_html="한일철강(+14%)",
        exit_date_min="2026-01-01",
        exit_date_max="2026-05-21",
        key_drivers_html="한일철강(+14%)",
        dna_compact_html="[TB]",
        dna_contrast_lines=(),
    )
    blk = DualTrackBucketBlock(
        bucket_label="50점대",
        live=live_b,
        hist=hist_b,
        drift_comment=_compute_drift_comment(live_b, hist_b),
    )
    html = format_dual_track_micro_dna_html([blk], anchor_day="2026-05-22")
    assert "당일 실전" in html
    assert "과거 기준(Sim)" in html
    assert "19.0%" in html
    assert "DNA 대조" in html
