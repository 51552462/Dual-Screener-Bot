"""
섹터 순환매 시계열 SSOT — daily dominance · transition events · rollup · prediction log.

P1: 30~90일 누적 궤적 · Markov 예측 · Whipsaw smoothing 연동.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections import Counter
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

from rotation_sector_filter import (
    dominant_sector_for_series,
    filter_eligible_daily_series,
    is_rotation_eligible_sector,
)
from sector_spillover_refresh import map_standard_sector

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_JUNK_FRAGMENTS = ("유망", "포착", "분석", "none", "unknown")

_DDL = """
CREATE TABLE IF NOT EXISTS sector_daily_leader (
    trade_date TEXT NOT NULL,
    market TEXT NOT NULL,
    dominant_sector_std TEXT NOT NULL,
    n_entries INTEGER NOT NULL DEFAULT 0,
    source TEXT DEFAULT 'forward_trades',
    updated_at TEXT,
    PRIMARY KEY (trade_date, market)
);
CREATE INDEX IF NOT EXISTS idx_sector_daily_leader_mkt_date
    ON sector_daily_leader(market, trade_date DESC);

CREATE TABLE IF NOT EXISTS sector_transition_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_date TEXT NOT NULL,
    market TEXT NOT NULL,
    from_sector TEXT NOT NULL,
    to_sector TEXT NOT NULL,
    from_streak_days INTEGER NOT NULL DEFAULT 1,
    UNIQUE(event_date, market, from_sector, to_sector)
);
CREATE INDEX IF NOT EXISTS idx_sector_trans_evt_mkt
    ON sector_transition_event(market, event_date DESC);

CREATE TABLE IF NOT EXISTS sector_transition_rollup (
    market TEXT NOT NULL,
    from_sector TEXT NOT NULL,
    to_sector TEXT NOT NULL,
    count_7d INTEGER NOT NULL DEFAULT 0,
    count_30d INTEGER NOT NULL DEFAULT 0,
    count_90d INTEGER NOT NULL DEFAULT 0,
    last_seen TEXT,
    confidence REAL DEFAULT 0,
    PRIMARY KEY (market, from_sector, to_sector)
);

CREATE TABLE IF NOT EXISTS sector_rotation_prediction_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    predict_date TEXT NOT NULL,
    market TEXT NOT NULL,
    from_sector TEXT,
    predicted_sector TEXT,
    actual_sector TEXT,
    hit INTEGER,
    confidence_before REAL,
    miss_streak_after INTEGER,
    recorded_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_sector_pred_log_mkt
    ON sector_rotation_prediction_log(market, predict_date DESC);
"""


def _db_path() -> str:
    from market_db_paths import market_db_read_path

    return market_db_read_path()


def _kst_today() -> str:
    return datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d")


def _now_str() -> str:
    return datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")


def ensure_sector_rotation_schema(db_path: Optional[str] = None) -> None:
    path = db_path or _db_path()
    if not path:
        return
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            conn.executescript(_DDL)
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("sector_rotation schema DDL skip: %s", ex)


def _sector_row_ok(s: Any, *, market: str = "KR") -> bool:
    if not is_rotation_eligible_sector(s, market=market):
        return False
    t = str(s or "").strip().lower()
    return not any(j in t for j in _JUNK_FRAGMENTS)


def _load_forward_trades(
    market: str,
    *,
    rolling_cutoff: Optional[str] = None,
    session_anchor: Optional[str] = None,
    lookback_days: Optional[int] = None,
    db_path: Optional[str] = None,
) -> pd.DataFrame:
    """forward_trades entry_date ∈ [rolling_cutoff, session_anchor] (Timekeeper SSOT)."""
    path = db_path or _db_path()
    if not path or not os.path.isfile(path):
        return pd.DataFrame()
    mkt = str(market).upper()
    if rolling_cutoff:
        cutoff = str(rolling_cutoff)[:10]
        anchor = str(session_anchor or rolling_cutoff)[:10]
    else:
        lb = int(lookback_days if lookback_days is not None else 90)
        cutoff = (datetime.now() - timedelta(days=lb)).strftime("%Y-%m-%d")
        anchor = datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            df = pd.read_sql(
                """
                SELECT entry_date, sector, status, market
                FROM forward_trades
                WHERE market = ?
                  AND substr(IFNULL(entry_date,''), 1, 10) >= ?
                  AND substr(IFNULL(entry_date,''), 1, 10) <= ?
                  AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
                ORDER BY entry_date ASC
                """,
                conn,
                params=(mkt, cutoff, anchor),
            )
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("sector_rotation load trades: %s", ex)
        return pd.DataFrame()
    return df


def ingest_sector_daily_leaders(
    market: str,
    *,
    rolling_cutoff: Optional[str] = None,
    session_anchor: Optional[str] = None,
    lookback_days: int = 90,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """forward_trades → sector_daily_leader upsert (Junk Hard Block)."""
    mkt = str(market).upper()
    path = db_path or _db_path()
    ensure_sector_rotation_schema(path)
    if rolling_cutoff:
        df = _load_forward_trades(
            mkt,
            rolling_cutoff=rolling_cutoff,
            session_anchor=session_anchor,
            db_path=path,
        )
    else:
        df = _load_forward_trades(mkt, lookback_days=lookback_days, db_path=path)
    out: Dict[str, Any] = {"market": mkt, "days_written": 0, "reason": ""}
    if df.empty or "entry_date" not in df.columns:
        out["reason"] = "no_rows"
        return out

    df = df.copy()
    df["day"] = df["entry_date"].astype(str).str.slice(0, 10)
    df["sector_std"] = df["sector"].apply(lambda s: map_standard_sector(s, market=mkt))
    df = df[df["sector_std"].apply(lambda s: _sector_row_ok(s, market=mkt))]

    if df.empty:
        out["reason"] = "no_valid_sectors"
        return out

    daily = (
        df.groupby("day")["sector_std"]
        .agg(lambda x: dominant_sector_for_series(x, market=mkt))
        .dropna()
    )
    counts = df.groupby("day").size()

    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            for day, dom in daily.items():
                n = int(counts.get(day, 0))
                conn.execute(
                    """
                    INSERT INTO sector_daily_leader
                    (trade_date, market, dominant_sector_std, n_entries, source, updated_at)
                    VALUES (?, ?, ?, ?, 'forward_trades', ?)
                    ON CONFLICT(trade_date, market) DO UPDATE SET
                        dominant_sector_std=excluded.dominant_sector_std,
                        n_entries=excluded.n_entries,
                        updated_at=excluded.updated_at
                    """,
                    (str(day), mkt, str(dom), n, _now_str()),
                )
            conn.commit()
            out["days_written"] = len(daily)
            out["reason"] = "ok"
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        out["reason"] = f"db_error:{ex}"
    return out


def _detect_transition_events(market: str, db_path: str) -> int:
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        rows = conn.execute(
            """
            SELECT trade_date, dominant_sector_std
            FROM sector_daily_leader
            WHERE market = ?
            ORDER BY trade_date ASC
            """,
            (str(market).upper(),),
        ).fetchall()
    finally:
        conn.close()

    if len(rows) < 2:
        return 0

    n_ins = 0
    prev_sec: Optional[str] = None
    streak = 0
    events: List[Tuple[str, str, str, int]] = []

    for day, sec in rows:
        sec = str(sec).strip()
        if prev_sec is None:
            prev_sec = sec
            streak = 1
            continue
        if sec == prev_sec:
            streak += 1
            continue
        events.append((str(day), prev_sec, sec, streak))
        prev_sec = sec
        streak = 1

    conn = sqlite3.connect(db_path, timeout=60)
    try:
        for ev_date, fr, to, st in events:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO sector_transition_event
                (event_date, market, from_sector, to_sector, from_streak_days)
                VALUES (?, ?, ?, ?, ?)
                """,
                (ev_date, str(market).upper(), fr, to, int(st)),
            )
            if cur.rowcount:
                n_ins += 1
        conn.commit()
    finally:
        conn.close()
    return n_ins


def rebuild_transition_rollup(market: str, *, db_path: Optional[str] = None) -> Dict[str, Any]:
    mkt = str(market).upper()
    path = db_path or _db_path()
    ensure_sector_rotation_schema(path)
    _detect_transition_events(mkt, path)

    today = datetime.now(pytz.timezone("Asia/Seoul")).date()
    cut_7 = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    cut_30 = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    cut_90 = (today - timedelta(days=90)).strftime("%Y-%m-%d")

    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            events = conn.execute(
                """
                SELECT event_date, from_sector, to_sector
                FROM sector_transition_event
                WHERE market = ?
                """,
                (mkt,),
            ).fetchall()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        return {"market": mkt, "pairs": 0, "reason": str(ex)}

    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for ev_date, fr, to in events:
        key = (str(fr), str(to))
        if key not in agg:
            agg[key] = {"c7": 0, "c30": 0, "c90": 0, "last": ev_date}
        bucket = agg[key]
        d = str(ev_date)[:10]
        if d >= cut_90:
            bucket["c90"] += 1
        if d >= cut_30:
            bucket["c30"] += 1
        if d >= cut_7:
            bucket["c7"] += 1
        if d > bucket["last"]:
            bucket["last"] = d

    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            conn.execute("DELETE FROM sector_transition_rollup WHERE market = ?", (mkt,))
            for (fr, to), b in agg.items():
                conf = min(1.0, float(b["c30"]) / 5.0)
                conn.execute(
                    """
                    INSERT INTO sector_transition_rollup
                    (market, from_sector, to_sector, count_7d, count_30d, count_90d, last_seen, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (mkt, fr, to, b["c7"], b["c30"], b["c90"], b["last"], conf),
                )
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        return {"market": mkt, "pairs": 0, "reason": str(ex)}

    return {"market": mkt, "pairs": len(agg), "reason": "ok"}


def _load_daily_series(
    market: str,
    *,
    rolling_cutoff: Optional[str] = None,
    session_anchor: Optional[str] = None,
    days: Optional[int] = None,
    db_path: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """sector_daily_leader — Timekeeper 윈도우 또는 레거시 days."""
    path = db_path or _db_path()
    if not path:
        return []
    mkt = str(market).upper()
    if rolling_cutoff:
        cutoff = str(rolling_cutoff)[:10]
        anchor = str(session_anchor or rolling_cutoff)[:10]
        sql = """
            SELECT trade_date, dominant_sector_std
            FROM sector_daily_leader
            WHERE market = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date ASC
        """
        params: Tuple[Any, ...] = (mkt, cutoff, anchor)
    else:
        lb = int(days if days is not None else 60)
        cutoff = (datetime.now() - timedelta(days=lb)).strftime("%Y-%m-%d")
        sql = """
            SELECT trade_date, dominant_sector_std
            FROM sector_daily_leader
            WHERE market = ? AND trade_date >= ?
            ORDER BY trade_date ASC
        """
        params = (mkt, cutoff)
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return []
    series = [(str(r[0]), str(r[1])) for r in rows]
    return filter_eligible_daily_series(series, market=mkt)


def compute_streaks_from_series(
    series: List[Tuple[str, str]],
    *,
    market: str = "KR",
) -> Tuple[Optional[str], int, Dict[str, List[int]], Dict[str, int]]:
    """현재 주도, 체류일, 섹터별 체류 길이 목록, 전이 카운트 (Junk 제외)."""
    series = filter_eligible_daily_series(series, market=market)
    if not series:
        return None, 0, {}, {}
    streaks: Dict[str, List[int]] = {}
    transitions: Dict[str, int] = {}
    current_sec: Optional[str] = None
    current_streak = 0
    for _day, sec in series:
        if not is_rotation_eligible_sector(sec, market=market):
            continue
        if sec == current_sec:
            current_streak += 1
        else:
            if current_sec is not None:
                streaks.setdefault(current_sec, []).append(current_streak)
                t_key = f"{current_sec[:15]}➔{sec[:15]}"
                transitions[t_key] = transitions.get(t_key, 0) + 1
            current_sec = sec
            current_streak = 1
    if current_sec is not None:
        streaks.setdefault(current_sec, []).append(current_streak)
    return current_sec, current_streak, streaks, transitions


def top_rollup_transitions(
    market: str,
    *,
    window: str = "30d",
    min_count: int = 2,
    limit: int = 5,
    db_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    path = db_path or _db_path()
    if not path:
        return []
    col = {"7d": "count_7d", "30d": "count_30d", "90d": "count_90d"}.get(window, "count_30d")
    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            rows = conn.execute(
                f"""
                SELECT from_sector, to_sector, {col} AS cnt, confidence, last_seen
                FROM sector_transition_rollup
                WHERE market = ? AND {col} >= ?
                ORDER BY {col} DESC, confidence DESC
                LIMIT ?
                """,
                (str(market).upper(), int(min_count), int(limit)),
            ).fetchall()
        finally:
            conn.close()
    except (OSError, sqlite3.Error):
        return []
    out = []
    for fr, to, cnt, conf, last in rows:
        out.append(
            {
                "from": fr,
                "to": to,
                "count": int(cnt),
                "confidence": float(conf or 0),
                "last_seen": last,
                "label": f"{fr[:15]}➔{to[:15]}",
            }
        )
    return out


def predict_next_sector_markov(
    market: str,
    *,
    db_path: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], float]:
    """rollup + daily series → (predicted, from_state, confidence)."""
    series = _load_daily_series(market, days=90, db_path=db_path)
    if not series:
        return None, None, 0.0

    rollups = top_rollup_transitions(market, window="30d", min_count=1, limit=200, db_path=db_path)
    transitions: Dict[Tuple[str, str], int] = {}
    for r in rollups:
        transitions[(r["from"], r["to"])] = r["count"]

    if not transitions:
        return None, series[-1][1], 0.0

    states = sorted({a for a, _ in transitions} | {b for _, b in transitions} | {s for _, s in series})
    s2i = {s: i for i, s in enumerate(states)}
    n = len(states)
    M = np.zeros((n, n), dtype=float)
    for (a, b), cnt in transitions.items():
        ia, ib = s2i.get(a), s2i.get(b)
        if ia is not None and ib is not None:
            M[ia, ib] += float(cnt)
    for i in range(n):
        rs = float(M[i].sum())
        if rs > 0:
            M[i] /= rs

    today_state = series[-1][1]
    predicted = None
    i0 = s2i.get(today_state)
    if i0 is not None and n > 0:
        M2 = np.matmul(M, M)
        row2 = M2[i0]
        if np.isfinite(row2).any() and float(np.nansum(row2)) > 0:
            predicted = states[int(np.nanargmax(row2))]

    if not predicted and transitions:
        predicted = max(transitions.items(), key=lambda kv: kv[1])[0][1]

    top_cnt = max(transitions.values()) if transitions else 0
    conf = min(0.95, 0.4 + 0.08 * top_cnt)
    return predicted, today_state, conf


def record_prediction_outcome(
    market: str,
    *,
    predict_date: str,
    predicted: str,
    from_sector: str,
    actual: str,
    confidence_before: float,
    sys_config: Optional[Dict[str, Any]] = None,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """전일 예측 vs 실제 → log + smoothing."""
    mkt = str(market).upper()
    path = db_path or _db_path()
    ensure_sector_rotation_schema(path)
    hit = 1 if str(predicted).strip() == str(actual).strip() else 0

    state = {"confidence": confidence_before, "miss_streak": 0, "ema_accuracy": 0.5}
    if isinstance(sys_config, dict):
        key = f"SECTOR_ROTATION_STATE_{mkt}"
        raw = sys_config.get(key)
        if isinstance(raw, dict):
            state.update(raw)

    try:
        from sector_rotation_smoothing import apply_prediction_miss_smoothing

        state = apply_prediction_miss_smoothing(state, hit=bool(hit), sys_config=sys_config)
    except Exception:
        pass

    try:
        conn = sqlite3.connect(path, timeout=60)
        try:
            conn.execute(
                """
                INSERT INTO sector_rotation_prediction_log
                (predict_date, market, from_sector, predicted_sector, actual_sector,
                 hit, confidence_before, miss_streak_after, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    predict_date[:10],
                    mkt,
                    from_sector,
                    predicted,
                    actual,
                    hit,
                    confidence_before,
                    int(state.get("miss_streak", 0)),
                    _now_str(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as ex:
        logger.warning("prediction log insert: %s", ex)

    return state


def refresh_predicted_sector_via_store(
    cfg: Dict[str, Any],
    market: str,
    *,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """PREDICTED_NEXT_SECTOR_* 갱신 — rollup SSOT, 실패 시 LAST_GOOD."""
    mkt = str(market).upper()
    key = f"PREDICTED_NEXT_SECTOR_{mkt}"
    out: Dict[str, Any] = {"market": mkt, "updated": False, "predicted": None, "reason": ""}

    series = _load_daily_series(mkt, days=90, db_path=db_path)
    if len(series) >= 2:
        prev_day, prev_pred_sector = series[-2]
        _yesterday_pred = str(cfg.get(key) or cfg.get(f"{key}_LAST_GOOD") or "")
        if _yesterday_pred and _yesterday_pred not in ("분석중", "NONE", ""):
            try:
                conf_b = float(cfg.get(f"{key}_CONFIDENCE") or 0.5)
            except (TypeError, ValueError):
                conf_b = 0.5
            st = record_prediction_outcome(
                mkt,
                predict_date=prev_day,
                predicted=_yesterday_pred,
                from_sector=str(cfg.get(f"{key}_FROM") or ""),
                actual=series[-1][1],
                confidence_before=conf_b,
                sys_config=cfg,
                db_path=db_path,
            )
            cfg[f"SECTOR_ROTATION_STATE_{mkt}"] = st
            cfg[f"{key}_CONFIDENCE"] = st.get("confidence", conf_b)
            if st.get("suggest_disable_rotation_advantage"):
                cfg["ROTATION_ADVANTAGE_ACTIVE"] = False

    predicted, from_st, conf = predict_next_sector_markov(mkt, db_path=db_path)
    if not predicted:
        lg = str(cfg.get(f"{key}_LAST_GOOD") or "").strip()
        if lg and lg not in ("분석중", "NONE", ""):
            out.update({"updated": False, "predicted": lg, "reason": "rollup_empty_last_good"})
            return out
        out["reason"] = "no_prediction"
        return out

    cfg[key] = predicted
    cfg[f"{key}_AS_OF"] = _today_kst()
    cfg[f"{key}_FROM"] = from_st or ""
    cfg[f"{key}_LAST_GOOD"] = predicted
    cfg[f"{key}_LAST_GOOD_AS_OF"] = _today_kst()
    cfg[f"{key}_CONFIDENCE"] = round(conf, 2)
    out.update({"updated": True, "predicted": predicted, "reason": "ok", "confidence": conf})
    return out


def run_sector_rotation_pipeline(
    *,
    markets: Optional[List[str]] = None,
    lookback_days: int = 90,
    db_path: Optional[str] = None,
    cfg: Optional[Dict[str, Any]] = None,
    save_config: bool = True,
) -> Dict[str, Any]:
    """ingest → rollup → predict (KR, US)."""
    from config_manager import load_system_config, save_system_config

    config = dict(cfg) if isinstance(cfg, dict) else load_system_config() or {}
    mkts = markets or ["KR", "US"]
    summary: Dict[str, Any] = {"markets": {}, "lookback_days": lookback_days}

    for m in mkts:
        ing = ingest_sector_daily_leaders(m, lookback_days=lookback_days, db_path=db_path)
        rup = rebuild_transition_rollup(m, db_path=db_path)
        pred = refresh_predicted_sector_via_store(config, m, db_path=db_path)
        summary["markets"][m] = {"ingest": ing, "rollup": rup, "predict": pred}

    if save_config:
        save_system_config(config)
    summary["saved"] = save_config
    return summary


def format_rotation_telegram_block(
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    rolling_cutoff: Optional[str] = None,
    session_anchor: Optional[str] = None,
    db_path: Optional[str] = None,
    prefiltered_series: Optional[List[Tuple[str, str]]] = None,
) -> str:
    """[7/9] 순환매 DB 블록 (rollup 기반, Timekeeper 윈도우)."""
    mkt = str(market).upper()
    if prefiltered_series is not None:
        series = list(prefiltered_series)
    elif rolling_cutoff:
        series = _load_daily_series(
            mkt,
            rolling_cutoff=rolling_cutoff,
            session_anchor=session_anchor,
            db_path=db_path,
        )
    else:
        series = _load_daily_series(mkt, days=60, db_path=db_path)
    current, streak, streaks, _legacy_trans = compute_streaks_from_series(
        series, market=mkt
    )
    rollups = top_rollup_transitions(mkt, window="30d", min_count=2, limit=5)

    lines: List[str] = []
    if current:
        lines.append(f"🔥 <b>현재 주도 섹터:</b> {current} ({streak}일째 체류 · DB 누적)\n")
    else:
        lines.append("🔥 <b>현재 주도 섹터:</b> 데이터 없음\n")

    try:
        from sector_spillover_refresh import resolve_predicted_sector_display

        pred = resolve_predicted_sector_display(sys_config or {}, mkt)
    except Exception:
        pred = "데이터 없음"
    lines.append(f"🔮 <b>다음 예측 섹터:</b> {pred}\n")

    cfg = sys_config if isinstance(sys_config, dict) else {}
    adv = "🔥활성화(200%)" if cfg.get("ROTATION_ADVANTAGE_ACTIVE") else "정상(100%)"
    lines.append(f"⚡ <b>베팅 어드밴티지:</b> {adv}\n\n")

    if streaks:
        lines.append("▪️ <b>섹터별 자금 체류(수명, DB):</b>\n")
        for s, lengths in sorted(streaks.items(), key=lambda x: -sum(x[1]) / len(x[1]))[:8]:
            if not is_rotation_eligible_sector(s, market=mkt):
                continue
            avg = sum(lengths) / len(lengths)
            lines.append(f" - {s[:15]}: 평균 {avg:.1f}일 ({len(lengths)}구간)\n")

    if rollups:
        lines.append("\n▪️ <b>빈번한 자금 이동 궤적 (30일 DB):</b>\n")
        for r in rollups:
            lines.append(
                f" - {r['label']} (<b>{r['count']}</b>회 · 신뢰 {r['confidence']:.0%} · 최근 {r['last_seen']})\n"
            )
    elif series:
        lines.append("\n▪️ <i>30일 DB 전이 2회 미만 — 표본 축적 중</i>\n")

    return "".join(lines)


def format_v29_rotation_section(
    market: str,
    *,
    lookback_days: int = 60,
    db_path: Optional[str] = None,
    sys_config: Optional[Dict[str, Any]] = None,
    open_db_fn: Optional[Any] = None,
) -> str:
    """
    [V29.0] 주도 섹터 순환매 — taxonomy 전 섹터 패널 + DB rollup + 미분류 breakdown.
    deep_dive 엔진 8 SSOT (레거시 6버킷 인라인 제거).
    """
    import html as _html

    from sector_taxonomy import (
        UNMAPPED_KR,
        UNMAPPED_US,
        rollup_sector_entries,
        standard_sectors_for_market,
    )

    mkt = str(market).upper()
    cfg = sys_config if isinstance(sys_config, dict) else {}
    path = db_path or _db_path()
    unmapped = UNMAPPED_US if mkt == "US" else UNMAPPED_KR

    lines: List[str] = [
        f"\n🔄 <b>[V29.0 {mkt}장 주도 섹터 순환매 자금 추적]</b>\n",
        "📐 <i>표준 taxonomy 전 섹터 패널 — 진입건·주도일·체류수명. "
        "미분류는 원시 라벨 breakdown 별도.</i>\n",
    ]

    cutoff = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")
    rot_df = pd.DataFrame()
    try:
        if open_db_fn is not None:
            conn = open_db_fn()
            try:
                rot_df = pd.read_sql(
                    """
                    SELECT entry_date, sector, status
                    FROM forward_trades
                    WHERE market=? AND entry_date >= ?
                      AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
                    ORDER BY entry_date ASC
                    """,
                    conn,
                    params=(mkt, cutoff),
                )
            finally:
                conn.close()
        elif path and os.path.isfile(path):
            conn = sqlite3.connect(path, timeout=60)
            try:
                rot_df = pd.read_sql(
                    """
                    SELECT entry_date, sector, status
                    FROM forward_trades
                    WHERE market=? AND entry_date >= ?
                      AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
                    ORDER BY entry_date ASC
                    """,
                    conn,
                    params=(mkt, cutoff),
                )
            finally:
                conn.close()
    except (OSError, sqlite3.Error) as ex:
        lines.append(f"⚠️ 순환매 데이터 로드 실패: {_html.escape(str(ex)[:80], quote=False)}\n")
        return "".join(lines)

    if rot_df.empty:
        lines.append(
            f"⚠️ 표본 부족 (최근 {lookback_days}일 {mkt}장 진입 0건) — 순환매 추적 생략.\n"
        )
        return "".join(lines)

    try:
        ingest_sector_daily_leaders(mkt, lookback_days=lookback_days, db_path=path)
        rebuild_transition_rollup(mkt, db_path=path)
    except Exception as ex:
        logger.debug("v29 ingest skip: %s", ex)

    panel, unmapped_bd, _raw_map = rollup_sector_entries(rot_df, market=mkt)
    standards = list(standard_sectors_for_market(mkt))
    panel_by_name = {p.sector: p for p in panel}

    series = _load_daily_series(mkt, days=lookback_days, db_path=path)
    current, streak, _streaks, _trans = compute_streaks_from_series(series, market=mkt)

    if current:
        lines.append(
            f"🔥 <b>현재 주도:</b> {_html.escape(current, quote=False)} "
            f"({streak}일째 · DB {lookback_days}일)\n"
        )
    try:
        from sector_spillover_refresh import resolve_predicted_sector_display

        pred = resolve_predicted_sector_display(cfg, mkt)
        lines.append(f"🔮 <b>다음 예측:</b> {_html.escape(pred, quote=False)}\n")
    except Exception:
        pass

    lines.append(f"\n▪️ <b>섹터별 자금 체류·진입 패널 (최근 {lookback_days}일):</b>\n")
    shown = 0
    for bucket in standards:
        p = panel_by_name.get(bucket)
        if p is None or p.n_entries == 0:
            continue
        lines.append(
            f" - {_html.escape(bucket, quote=False)}: "
            f"진입 <b>{p.n_entries}</b>건 · 주도일 <b>{p.n_days_dominant}</b> · "
            f"체류 평균 <b>{p.avg_streak_days:.1f}</b>일 (최장 {p.max_streak_days}일)\n"
        )
        shown += 1

    for p in panel:
        if p.sector in standards or p.n_entries == 0:
            continue
        if not is_rotation_eligible_sector(p.sector, market=mkt) and p.sector != unmapped:
            continue
        fine = " ·원시보존" if not p.is_standard_bucket else ""
        lines.append(
            f" - {_html.escape(p.sector[:18], quote=False)}{fine}: "
            f"진입 <b>{p.n_entries}</b>건 · 주도일 <b>{p.n_days_dominant}</b> · "
            f"체류 평균 <b>{p.avg_streak_days:.1f}</b>일\n"
        )
        shown += 1

    if shown == 0:
        lines.append(" - <i>유효 섹터 진입 0건 (섹터 태깅·normalize 점검)</i>\n")

    if unmapped_bd:
        total_um = sum(unmapped_bd.values())
        lines.append(
            f"\n▪️ <b>미분류 원시 라벨 breakdown</b> (taxonomy 미매핑 <b>{total_um}</b>건):\n"
        )
        for raw, cnt in sorted(unmapped_bd.items(), key=lambda x: -x[1])[:12]:
            mapped = _raw_map.get(raw, unmapped)
            lines.append(
                f" - {_html.escape(raw[:20], quote=False)} → {_html.escape(mapped, quote=False)} "
                f"(<b>{cnt}</b>건)\n"
            )

    rollups = top_rollup_transitions(mkt, window="30d", min_count=1, limit=3, db_path=path)
    lines.append("\n▪️ <b>빈번한 자금 이동 경로 (30일 DB):</b>\n")
    if rollups:
        for r in rollups:
            lines.append(
                f" - {_html.escape(r['label'], quote=False)} "
                f"(<b>{r['count']}</b>회 · 신뢰 {r['confidence']:.0%})\n"
            )
        if current and rollups:
            nxt = rollups[0]["to"]
            lines.append(
                f"💡 <b>관제탑:</b> [{_html.escape(current, quote=False)}] 수명 말기 시 "
                f"[{_html.escape(str(nxt), quote=False)}] 선취매 후보.\n"
            )
    else:
        lines.append(" - <i>전이 패턴 축적 중 (2회 미만)</i>\n")

    dormant = [b for b in standards if panel_by_name.get(b) is None or panel_by_name[b].n_entries == 0]
    if dormant and shown > 0:
        preview = ", ".join(_html.escape(d, quote=False) for d in dormant[:8])
        if len(dormant) > 8:
            preview += "…"
        lines.append(f"\n📋 <i>표본 없음(대기): {preview}</i>\n")

    return "".join(lines)
