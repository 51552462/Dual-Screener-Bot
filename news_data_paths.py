"""
news_data.sqlite 경로 SSOT + 일일 리포트용 센티먼트 조회 (KST 당일 검증).
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

from factory_data_paths import factory_data_dir


def news_db_path() -> str:
    return os.path.join(factory_data_dir(), "news_data.sqlite")


def today_kst_str() -> str:
    return datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")


def _row_to_record(row: Tuple[Any, ...], *, today: str) -> Dict[str, Any]:
    d = str(row[0] or "").strip()
    k1 = str(row[1] or "").strip() if row[1] is not None else ""
    k2 = str(row[2] or "").strip() if row[2] is not None else ""
    k3 = str(row[3] or "").strip() if row[3] is not None else ""
    try:
        score = float(row[4]) if row[4] is not None else None
    except (TypeError, ValueError):
        score = None
    has_keywords = any((k1, k2, k3))
    stale = d != today
    return {
        "date": d,
        "top_keyword_1": k1,
        "top_keyword_2": k2,
        "top_keyword_3": k3,
        "sentiment_score": score,
        "stale": stale,
        "missing_content": not has_keywords and score is None,
    }


def load_latest_daily_sentiment() -> Optional[Dict[str, Any]]:
    """
    daily_sentiment 조회 — 당일(KST) 행 우선, 없으면 최신 1행(stale=True).
    Returns None if DB/row missing.
    """
    path = news_db_path()
    if not os.path.isfile(path):
        return None
    today = today_kst_str()
    try:
        conn = sqlite3.connect(path, timeout=30)
        try:
            row_today = conn.execute(
                """
                SELECT date, top_keyword_1, top_keyword_2, top_keyword_3, sentiment_score
                FROM daily_sentiment
                WHERE date = ?
                """,
                (today,),
            ).fetchone()
            if row_today:
                return _row_to_record(row_today, today=today)

            row = conn.execute(
                """
                SELECT date, top_keyword_1, top_keyword_2, top_keyword_3, sentiment_score
                FROM daily_sentiment
                ORDER BY date DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return None

    if not row:
        return None
    return _row_to_record(row, today=today)


def format_sentiment_satellite_line(*, hide_stale_keywords: bool = True) -> str:
    """
    텔레그램 [팩토리 위성망 통합 첩보] 센티먼트 한 줄.
    당일(KST) 미갱신 시 과거 키워드를 '오늘 데이터'처럼 노출하지 않음.
    """
    rec = load_latest_daily_sentiment()
    if rec is None:
        return " ▪️ 🧠 센티먼트: 데이터 없음\n"

    today = today_kst_str()
    d = rec.get("date") or "—"

    if rec.get("stale"):
        if hide_stale_keywords:
            return (
                f" ▪️ 🧠 센티먼트: 데이터 없음 "
                f"(당일 {today} 미갱신 · 마지막 스냅샷 <b>{d}</b>)\n"
            )
        k1 = rec.get("top_keyword_1") or "—"
        k2 = rec.get("top_keyword_2") or "—"
        k3 = rec.get("top_keyword_3") or "—"
        score = rec.get("sentiment_score")
        score_s = f"{score:.1f}" if score is not None else "—"
        return (
            f" ▪️ 🧠 센티먼트: {k1}, {k2}, {k3} (온도 {score_s}점) "
            f"· <i>과거 스냅샷 {d}</i>\n"
        )

    if rec.get("missing_content"):
        return f" ▪️ 🧠 센티먼트: 데이터 없음 [{d}]\n"

    k1 = rec.get("top_keyword_1") or "—"
    k2 = rec.get("top_keyword_2") or "—"
    k3 = rec.get("top_keyword_3") or "—"
    score = rec.get("sentiment_score")
    score_s = f"{score:.1f}" if score is not None else "—"
    return f" ▪️ 🧠 센티먼트: {k1}, {k2}, {k3} (온도 {score_s}점) [{d}]\n"


def assert_sentiment_fresh_for_report() -> bool:
    """리포트 직전 검증 — 당일 행 존재 여부."""
    rec = load_latest_daily_sentiment()
    return rec is not None and not rec.get("stale") and not rec.get("missing_content")
