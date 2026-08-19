"""
OPS-LIQ-TG-01 — [LIQ_BAND] daily observation panel (CAT-J, read-only).

Reuses Phase1 percentile bucket boundaries and scan_funnel_drop_event LIQUIDITY rows.
Does NOT touch LIQUIDITY gate thresholds, config_kv liquidity keys, or north-star ledger.
"""
from __future__ import annotations

import html
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# Claude-specified report-only thresholds (OPS-LIQ-TG-01) — not gate cutoffs
LIQ_BAND_ENABLED = True
LIQ_BAND_MIN_N = 20
LIQ_BAND_PHASE2_SHARE_THRESHOLD = 0.50
LIQ_BAND_PHASE2_CONSECUTIVE_DAYS = 5
LIQ_BAND_SAMPLE_FREQ = "daily"  # or "every_other_day"
LIQ_BAND_HISTORY_KEEP_DAYS = 10
LIQ_BAND_HISTORY_FILENAME = "liq_band_history.json"
LIQ_BAND_HISTORY_SCHEMA = "liq_band_history.v1"

# Phase1 display buckets (ops_liq_fork_01) — do not redefine
PCT_LOW = 33.333
PCT_HIGH = 66.667

_SKIP_SUFFIX = {
    "KOSPI_IDX",
    "KOSDAQ_IDX",
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
}


def _esc(v: Any) -> str:
    return html.escape(str(v) if v is not None else "", quote=False)


def percentile_of_universe(value: float, universe: Sequence[float]) -> Optional[float]:
    """Phase1 empirical CDF: % of universe strictly below value."""
    if not universe or not np.isfinite(value):
        return None
    arr = np.asarray(universe, dtype=float)
    return float(100.0 * np.mean(arr < value))


def bucket_percentile(pct: Optional[float]) -> str:
    """Phase1 buckets: low < PCT_LOW, mid < PCT_HIGH, else high."""
    if pct is None:
        return "unknown"
    if pct < PCT_LOW:
        return "low"
    if pct < PCT_HIGH:
        return "mid"
    return "high"


def liq_band_history_path() -> str:
    try:
        from factory_data_paths import factory_data_dir

        root = factory_data_dir()
    except Exception:  # pragma: no cover
        root = os.path.join(os.path.expanduser("~"), "dante_bots", "Dual-Screener-Bot")
        os.makedirs(root, exist_ok=True)
    return os.path.join(root, LIQ_BAND_HISTORY_FILENAME)


def load_liq_band_history(*, path: Optional[str] = None) -> List[Dict[str, Any]]:
    p = path or liq_band_history_path()
    if not os.path.isfile(p):
        return []
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    if isinstance(data, dict):
        days = data.get("days")
        if isinstance(days, list):
            return [d for d in days if isinstance(d, dict)]
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    return []


def append_liq_band_history(
    snapshot: dict,
    *,
    path: Optional[str] = None,
    keep_days: int = LIQ_BAND_HISTORY_KEEP_DAYS,
) -> None:
    """Additive JSON side-file (CAT-J). Never writes dual_north_star_ledger.json."""
    p = path or liq_band_history_path()
    days = load_liq_band_history(path=p)
    scan_date = str(snapshot.get("scan_date") or "")
    # upsert by scan_date
    days = [d for d in days if str(d.get("scan_date") or "") != scan_date]
    days.append(dict(snapshot))
    days.sort(key=lambda d: str(d.get("scan_date") or ""))
    if keep_days > 0 and len(days) > keep_days:
        days = days[-keep_days:]
    payload = {"schema": LIQ_BAND_HISTORY_SCHEMA, "days": days}
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="liq_band_", suffix=".json", dir=os.path.dirname(p) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, p)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _connect_market_db() -> sqlite3.Connection:
    from market_db_paths import MARKET_DATA_DB_PATH

    conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    return conn


def _liquidity_codes_for_day(
    conn: sqlite3.Connection, market: str, scan_date: str
) -> List[Tuple[str, str]]:
    day = str(scan_date)[:10]
    rows = conn.execute(
        """
        SELECT code, ts
        FROM scan_funnel_drop_event
        WHERE market = ?
          AND UPPER(TRIM(reason)) = 'LIQUIDITY'
          AND substr(ts, 1, 10) = ?
          AND code IS NOT NULL
          AND TRIM(code) != ''
        ORDER BY ts DESC, id DESC
        """,
        (market, day),
    ).fetchall()
    out: List[Tuple[str, str]] = []
    seen: set[str] = set()
    for r in rows:
        code = str(r["code"]).strip()
        if code in seen:
            continue
        seen.add(code)
        out.append((code, str(r["ts"])))
    return out


def _window(ts: str) -> Tuple[str, str]:
    day = str(ts)[:10]
    try:
        end = datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        end = datetime.utcnow()
        day = end.strftime("%Y-%m-%d")
    start = (end - timedelta(days=21)).strftime("%Y-%m-%d")
    return start, day


def _metrics_from_df(df: Optional[pd.DataFrame]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "close": None,
        "avg_vol_5d": None,
        "avg_dollar_vol_5d": None,
        "fetch": "fail",
    }
    if df is None or df.empty:
        return out
    try:
        closes = pd.to_numeric(df["Close"], errors="coerce")
        vols = pd.to_numeric(df["Volume"], errors="coerce")
        tail_c = closes.iloc[-5:]
        tail_v = vols.iloc[-5:]
        if tail_c.empty or bool(tail_c.isna().all()):
            return out
        close = float(tail_c.iloc[-1])
        vol5 = float(np.nanmean(tail_v.values))
        dollar = float(np.nanmean((tail_c * tail_v).values))
        out.update(
            {
                "close": close,
                "avg_vol_5d": vol5,
                "avg_dollar_vol_5d": dollar,
                "fetch": "ok",
            }
        )
    except Exception as ex:  # noqa: BLE001
        out["fetch"] = f"exc:{type(ex).__name__}"
    return out


def _table_code(market: str, table: str) -> Optional[str]:
    prefix = f"{market}_"
    if not table.startswith(prefix):
        return None
    suffix = table[len(prefix) :]
    if suffix in _SKIP_SUFFIX or suffix.endswith("_IDX"):
        return None
    return suffix


def universe_dollar_vols(
    conn: sqlite3.Connection, market: str, asof: str
) -> List[float]:
    """Phase1 proxy: all KR_/US_ OHLCV tables in prod DB."""
    like = f"{market}_%"
    tables = [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (like,),
        ).fetchall()
    ]
    vals: List[float] = []
    for tname in tables:
        if _table_code(market, tname) is None:
            continue
        try:
            rows = conn.execute(
                f'SELECT Close, Volume FROM "{tname}" '
                f"WHERE Date <= ? ORDER BY Date DESC LIMIT 5",
                (asof,),
            ).fetchall()
        except Exception:
            continue
        if not rows:
            continue
        dollars: List[float] = []
        for cl, vol in rows:
            try:
                c = float(cl)
                v = float(vol)
            except (TypeError, ValueError):
                continue
            if not np.isfinite(c) or not np.isfinite(v) or v < 0:
                continue
            dollars.append(c * v)
        if not dollars:
            continue
        vals.append(float(np.mean(dollars)))
    return vals


def midhigh_share_from_counts(low_n: int, mid_n: int, high_n: int, unknown_n: int = 0) -> float:
    n = int(low_n) + int(mid_n) + int(high_n) + int(unknown_n)
    if n <= 0:
        return 0.0
    return float(mid_n + high_n) / float(n)


def compute_liq_band_snapshot(
    market: str,
    scan_date: str,
    *,
    min_n: int = LIQ_BAND_MIN_N,
    conn: Optional[sqlite3.Connection] = None,
    universe: Optional[Sequence[float]] = None,
    code_dollar_vols: Optional[Dict[str, float]] = None,
) -> dict:
    """
    Read-only LIQUIDITY drop sample → Phase1 percentile buckets.

    Optional conn/universe/code_dollar_vols enable unit tests without full DB scan.
    """
    market_u = str(market or "").upper().strip()
    day = str(scan_date)[:10]
    owns_conn = conn is None
    if owns_conn:
        conn = _connect_market_db()
    assert conn is not None
    try:
        samples = _liquidity_codes_for_day(conn, market_u, day)
        if code_dollar_vols is not None:
            # test/injection path: bucket provided dollar vols only
            univ = list(universe) if universe is not None else []
            low_n = mid_n = high_n = unknown_n = 0
            for code, _ts in samples:
                if code not in code_dollar_vols:
                    unknown_n += 1
                    continue
                pct = percentile_of_universe(float(code_dollar_vols[code]), univ)
                b = bucket_percentile(pct)
                if b == "low":
                    low_n += 1
                elif b == "mid":
                    mid_n += 1
                elif b == "high":
                    high_n += 1
                else:
                    unknown_n += 1
            n = low_n + mid_n + high_n + unknown_n
            # if injection dict keys used without samples, allow synthetic n from dict
            if not samples and code_dollar_vols:
                low_n = mid_n = high_n = unknown_n = 0
                for _code, dvol in code_dollar_vols.items():
                    pct = percentile_of_universe(float(dvol), univ)
                    b = bucket_percentile(pct)
                    if b == "low":
                        low_n += 1
                    elif b == "mid":
                        mid_n += 1
                    elif b == "high":
                        high_n += 1
                    else:
                        unknown_n += 1
                n = low_n + mid_n + high_n + unknown_n
        else:
            from market_data_fetcher import fetch_market_data

            if not samples:
                n = low_n = mid_n = high_n = unknown_n = 0
            else:
                univ = (
                    list(universe)
                    if universe is not None
                    else universe_dollar_vols(conn, market_u, day)
                )
                low_n = mid_n = high_n = unknown_n = 0
                for code, ts in samples:
                    start, end = _window(ts)
                    df = fetch_market_data(code, market_u, start, end)
                    m = _metrics_from_df(df)
                    if m["avg_dollar_vol_5d"] is None:
                        unknown_n += 1
                        continue
                    pct = percentile_of_universe(float(m["avg_dollar_vol_5d"]), univ)
                    b = bucket_percentile(pct)
                    if b == "low":
                        low_n += 1
                    elif b == "mid":
                        mid_n += 1
                    elif b == "high":
                        high_n += 1
                    else:
                        unknown_n += 1
                n = low_n + mid_n + high_n + unknown_n

        share = midhigh_share_from_counts(low_n, mid_n, high_n, unknown_n)
        insufficient = int(n) < int(min_n)
        return {
            "market": market_u,
            "scan_date": day,
            "n": int(n),
            "low_n": int(low_n),
            "mid_n": int(mid_n),
            "high_n": int(high_n),
            "unknown_n": int(unknown_n),
            "midhigh_share": float(share),
            "insufficient": bool(insufficient),
            "sample_source": "scan_funnel_drop_event",
        }
    finally:
        if owns_conn:
            conn.close()


def _market_streak_count(
    market: str,
    history: List[Dict[str, Any]],
    *,
    share_threshold: float = LIQ_BAND_PHASE2_SHARE_THRESHOLD,
) -> int:
    """Count consecutive valid days with midhigh_share >= threshold (newest first).
    Insufficient days are skipped (not a reset).
    """
    market_u = str(market).upper()
    # flatten day records → market snapshots newest-first
    snaps: List[Dict[str, Any]] = []
    for day in sorted(history, key=lambda d: str(d.get("scan_date") or ""), reverse=True):
        markets = day.get("markets") if isinstance(day.get("markets"), dict) else None
        if markets is not None:
            m = markets.get(market_u) or markets.get(market)
            if isinstance(m, dict):
                snaps.append(m)
            continue
        # allow flat per-market history entries
        if str(day.get("market") or "").upper() == market_u:
            snaps.append(day)

    streak = 0
    for s in snaps:
        if bool(s.get("insufficient")):
            continue  # skip — do not reset
        try:
            share = float(s.get("midhigh_share") or 0.0)
        except (TypeError, ValueError):
            share = 0.0
        if share >= float(share_threshold):
            streak += 1
        else:
            break
    return streak


def resolve_liq_band_cursor_action(
    kr: dict,
    us: dict,
    history: list[dict],
    *,
    min_n: int = LIQ_BAND_MIN_N,
    share_threshold: float = LIQ_BAND_PHASE2_SHARE_THRESHOLD,
    consecutive_days: int = LIQ_BAND_PHASE2_CONSECUTIVE_DAYS,
) -> Tuple[str, dict]:
    """§4 judgment table. Market streaks independent (Rule 8)."""
    today = {
        "scan_date": str(kr.get("scan_date") or us.get("scan_date") or ""),
        "markets": {"KR": dict(kr), "US": dict(us)},
    }
    hist_with_today = list(history) + [today]

    phase2_markets: List[str] = []
    streaks: Dict[str, int] = {}
    for mk, snap in (("KR", kr), ("US", us)):
        st = _market_streak_count(
            mk, hist_with_today, share_threshold=share_threshold
        )
        streaks[mk] = st
        if st >= int(consecutive_days):
            phase2_markets.append(mk)

    detail: Dict[str, Any] = {
        "streaks": streaks,
        "phase2_markets": phase2_markets,
        "kr_insufficient": bool(kr.get("insufficient")),
        "us_insufficient": bool(us.get("insufficient")),
        "kr_n": int(kr.get("n") or 0),
        "us_n": int(us.get("n") or 0),
    }

    if phase2_markets:
        return "PHASE2_CANDIDATE", detail

    kr_ok = not bool(kr.get("insufficient")) and int(kr.get("n") or 0) >= int(min_n)
    us_ok = not bool(us.get("insufficient")) and int(us.get("n") or 0) >= int(min_n)
    if kr_ok and us_ok:
        return "OBSERVE_LIQ_BAND", detail
    return "NONE", detail


def format_liq_band_panel_html(
    kr: dict,
    us: dict,
    cursor_action: str,
    *,
    detail: Optional[dict] = None,
) -> str:
    action = str(cursor_action or "NONE")
    detail = detail or {}
    kr_n = int(kr.get("n") or 0)
    us_n = int(us.get("n") or 0)

    if action == "PHASE2_CANDIDATE":
        mkts = detail.get("phase2_markets") or []
        if not mkts:
            mkts = ["KR"] if float(kr.get("midhigh_share") or 0) >= LIQ_BAND_PHASE2_SHARE_THRESHOLD else ["US"]
        label = "+".join(str(m) for m in mkts)
        emoji = "🟠"
        status = (
            f"{_esc(label)}에서 정상 유동 종목이 5일 연속 과반 잘려나가고 있습니다"
            " — Phase2 논의하러 컴퓨터를 켜주세요."
        )
    elif action == "OBSERVE_LIQ_BAND":
        emoji = "🟢"
        kr_pct = 100.0 * float(kr.get("midhigh_share") or 0.0)
        us_pct = 100.0 * float(us.get("midhigh_share") or 0.0)
        status = (
            "유동성 컷 표본 정상 범위 — 지켜보기만 하면 됩니다. "
            f"(KR 정상비중 {kr_pct:.0f}% · US 정상비중 {us_pct:.0f}%)"
        )
    else:
        emoji = "🟡"
        # 규칙4: 표본부족 — 숫자 판정(정상비중 등) 문구 없음
        status = (
            "오늘은 유동성 컷 표본이 부족해 판단을 쉽니다. "
            f"(KR n={kr_n} · US n={us_n})"
        )

    cursor_block = (
        "[CAT-J] LIQ_BAND 관측 공유 — 코드/threshold 변경 없음, 계속 관측만."
    )
    parts = [
        f"{emoji} <b>[LIQ_BAND]</b> {_esc(status)}",
        f"· cursor_action=<code>{_esc(action)}</code>",
        "",
        "<b>---CURSOR---</b> <i>(아래 전부 Cursor 새 채팅 첫 메시지)</i>",
        f"<pre>{_esc(cursor_block)}</pre>",
    ]
    if action == "PHASE2_CANDIDATE":
        mkts = detail.get("phase2_markets") or ["?"]
        mlabel = "+".join(str(m) for m in mkts)
        claude_block = (
            f"OPS-LIQ-TG-01 LIQ_BAND {mlabel} 5일 연속 과반 — "
            "OPS-LIQ-FORK-01 갈림길(A′/B/C) 재소집 요청.\n"
            "docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 LIQ_BAND 이력 읽고 판단."
        )
        parts.extend(
            [
                "",
                "<b>---CLAUDE---</b> <i>(아래 전부 Claude Pro 창 첫 메시지)</i>",
                f"<pre>{_esc(claude_block)}</pre>",
            ]
        )
    return "\n".join(parts)


def should_sample_today(scan_date: str, *, freq: str = LIQ_BAND_SAMPLE_FREQ) -> bool:
    f = str(freq or "daily").strip().lower()
    if f in ("daily", "day", ""):
        return True
    if f in ("every_other_day", "eod", "2d"):
        try:
            d = datetime.strptime(str(scan_date)[:10], "%Y-%m-%d")
        except ValueError:
            return True
        return (d.toordinal() % 2) == 0
    return True


def build_liq_band_payload_for_digest(
    *,
    scan_date: str,
    persist_history: bool = True,
    history_path: Optional[str] = None,
    enabled: bool = LIQ_BAND_ENABLED,
    kr_snapshot: Optional[dict] = None,
    us_snapshot: Optional[dict] = None,
) -> Optional[Dict[str, Any]]:
    """Orchestrate snapshot → resolve → optional history append. Returns panel payload or None."""
    if not enabled:
        return None
    if not should_sample_today(scan_date):
        return None

    kr = kr_snapshot or compute_liq_band_snapshot("KR", scan_date)
    us = us_snapshot or compute_liq_band_snapshot("US", scan_date)
    hist = load_liq_band_history(path=history_path)
    action, detail = resolve_liq_band_cursor_action(kr, us, hist)
    day_snap = {
        "scan_date": str(scan_date)[:10],
        "markets": {"KR": kr, "US": us},
        "cursor_action": action,
        "detail": detail,
    }
    if persist_history:
        append_liq_band_history(day_snap, path=history_path)
    html_panel = format_liq_band_panel_html(kr, us, action, detail=detail)
    return {
        "enabled": True,
        "cursor_action": action,
        "detail": detail,
        "KR": kr,
        "US": us,
        "html": html_panel,
    }


def format_liq_band_section_from_snap(snap: Dict[str, Any]) -> str:
    """Additive digest section. Empty unless snap carries precomputed liq_band."""
    payload = snap.get("liq_band")
    if not isinstance(payload, dict):
        return ""
    if payload.get("enabled") is False:
        return ""
    html_panel = payload.get("html")
    if isinstance(html_panel, str) and html_panel.strip():
        return html_panel
    kr = payload.get("KR") or {}
    us = payload.get("US") or {}
    action = str(payload.get("cursor_action") or "NONE")
    detail = payload.get("detail") if isinstance(payload.get("detail"), dict) else {}
    if not kr and not us:
        return ""
    return format_liq_band_panel_html(kr, us, action, detail=detail)
