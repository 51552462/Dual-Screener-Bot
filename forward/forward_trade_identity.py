"""
forward_trades 식별자(종목명·코드) 진단·백필 — 리포트 '종목미상' 역추적 SSOT.

우선순위(이름 해석):
  1) 동일 market+code 의 최근 유효 name (장부 자기참조)
  2) KR: krx_list_cache.csv / US: us_list_survival
  3) KR/US sqlite OHLCV 테이블명 (KR_005930 / US_AAPL) — 코드만 확보 시 코드 표시

구조(리포트·태그 블록)는 숨기지 않음 — 데이터만 수리.
"""
from __future__ import annotations

import os
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Tuple

import pandas as pd
import pytz

from forward_dual_track_queries import query_latest_closed_trade_date
from market_db_paths import MARKET_DATA_DB_PATH
from report_date_utils import closed_event_dates, in_date_window, normalize_date_series
from reports.report_staleness_gate import evaluate_staleness
from reports.report_timekeeper import (
    ReportTimekeeper,
    business_lag_days,
    resolve_data_candle_watermark,
)

_KR_TZ = pytz.timezone("Asia/Seoul")
_BLANK_NAMES = frozenset({"", "nan", "none", "null", "nat", "—", "-", "종목미상", "unknown"})
_KR_TABLE_RE = re.compile(r"^KR_(\d{6})$")
_US_TABLE_RE = re.compile(r"^US_([A-Z0-9.\-]+)$", re.I)

IdentityClass = Literal[
    "ok",
    "name_missing_code_ok",
    "code_missing",
    "both_missing",
    "junk_name",
]


@dataclass(frozen=True)
class NameLookupStats:
    ledger: int = 0
    universe: int = 0
    table_codes: int = 0


@dataclass(frozen=True)
class IdentityGapRow:
    id: int
    market: str
    code: str
    name: str
    status: str
    entry_date: str
    exit_date: str
    final_ret: Optional[float]
    flow_tags: str
    gap_class: IdentityClass
    resolved_name: str
    resolve_source: str


@dataclass(frozen=True)
class FlowTagGapSummary:
    tag: str
    gap_rows: int
    n_closed_window: int
    worst_final_ret: Optional[float]
    sample_codes: Tuple[str, ...]


@dataclass(frozen=True)
class PipelineHealth:
    market: str
    session_anchor: str
    rolling_cutoff: str
    db_watermark_exit: Optional[str]
    lag_business_days: int
    staleness_grade: str
    n_open: int
    n_closed_all: int
    n_closed_window: int
    n_live_today: int
    max_entry_open: Optional[str]


@dataclass
class IdentityDiagnosticReport:
    market: str
    db_path: str
    generated_at_kst: str
    pipeline: PipelineHealth
    lookup_stats: NameLookupStats
    n_gap_all: int
    n_gap_window: int
    n_gap_open: int
    gap_rows: List[IdentityGapRow] = field(default_factory=list)
    flow_tag_gaps: List[FlowTagGapSummary] = field(default_factory=list)
    verdict: str = ""
    notes: List[str] = field(default_factory=list)


@dataclass
class BackfillResult:
    market: str
    dry_run: bool
    candidates: int
    updated: int
    skipped_no_lookup: int
    skipped_already_ok: int
    sample_updates: List[Tuple[int, str, str, str]] = field(default_factory=list)


def is_blank_identity_name(name: Any) -> bool:
    if name is None:
        return True
    if isinstance(name, float) and pd.isna(name):
        return True
    t = str(name).strip()
    if not t:
        return True
    return t.lower() in _BLANK_NAMES


def normalize_ticker_code(market: str, code: Any) -> str:
    mk = str(market or "").upper()
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return ""
    raw = str(code).strip()
    if not raw or raw.lower() in ("nan", "none"):
        return ""
    if mk == "KR":
        digits = re.sub(r"\D", "", raw)
        if not digits:
            return raw.upper()
        return digits.zfill(6)[-6:]
    return raw.upper().replace(".", "-")


def classify_identity_row(name: Any, code: Any, *, market: str = "KR") -> IdentityClass:
    code_ok = bool(normalize_ticker_code(market, code))
    name_ok = not is_blank_identity_name(name)
    if name_ok and code_ok:
        return "ok"
    if name_ok and not code_ok:
        return "code_missing"
    if not name_ok and code_ok:
        return "name_missing_code_ok"
    nm = str(name or "").strip().lower()
    if nm in _BLANK_NAMES and not code_ok:
        return "both_missing"
    if not name_ok:
        return "junk_name"
    return "both_missing"


def _load_ledger_name_lookup(conn: sqlite3.Connection, market: str) -> Dict[str, str]:
    mk = str(market).upper()
    rows = conn.execute(
        """
        SELECT code, name FROM forward_trades
        WHERE market=? AND name IS NOT NULL AND TRIM(name) != ''
        ORDER BY id DESC
        """,
        (mk,),
    ).fetchall()
    out: Dict[str, str] = {}
    for code, name in rows:
        if is_blank_identity_name(name):
            continue
        key = normalize_ticker_code(mk, code)
        if key and key not in out:
            out[key] = str(name).strip()
    return out


def _load_kr_cache_lookup(db_path: str) -> Dict[str, str]:
    try:
        from krx_list_survival import default_krx_list_cache_path

        path = default_krx_list_cache_path(db_path)
        if not path or not os.path.isfile(path):
            return {}
        df = pd.read_csv(path)
        if df.empty or "Code" not in df.columns:
            return {}
        name_col = "Name" if "Name" in df.columns else None
        if not name_col:
            return {}
        out: Dict[str, str] = {}
        for _, row in df.iterrows():
            code = normalize_ticker_code("KR", row.get("Code"))
            nm = row.get(name_col)
            if code and not is_blank_identity_name(nm):
                out[code] = str(nm).strip()
        return out
    except Exception:
        return {}


def _load_us_universe_lookup(db_path: str) -> Dict[str, str]:
    try:
        from us_list_survival import collect_us_list_survival

        df, _src = collect_us_list_survival(db_path=db_path, fdr_module=None)
        if df is None or df.empty or "Code" not in df.columns:
            return {}
        out: Dict[str, str] = {}
        name_col = "Name" if "Name" in df.columns else None
        for _, row in df.iterrows():
            code = normalize_ticker_code("US", row.get("Code"))
            if not code:
                continue
            if name_col and not is_blank_identity_name(row.get(name_col)):
                out[code] = str(row[name_col]).strip()
            elif code not in out:
                out[code] = code
        return out
    except Exception:
        return {}


def _load_table_code_set(conn: sqlite3.Connection, market: str) -> Dict[str, str]:
    mk = str(market).upper()
    prefix = "KR_" if mk == "KR" else "US_"
    pat_re = _KR_TABLE_RE if mk == "KR" else _US_TABLE_RE
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (f"{prefix}%",),
        ).fetchall()
    except sqlite3.Error:
        return {}
    out: Dict[str, str] = {}
    for (tname,) in tables:
        m = pat_re.match(str(tname).strip())
        if not m:
            continue
        code = normalize_ticker_code(mk, m.group(1))
        if code and code not in out:
            out[code] = code
    return out


def build_name_lookup(
    conn: sqlite3.Connection,
    market: str,
    *,
    db_path: Optional[str] = None,
) -> Tuple[Dict[str, str], NameLookupStats, Dict[str, str], Dict[str, str], Dict[str, str]]:
    """통합 lookup + (ledger, universe, table_codes) 분리."""
    mk = str(market).upper()
    path = db_path or MARKET_DATA_DB_PATH
    ledger = _load_ledger_name_lookup(conn, mk)
    universe: Dict[str, str] = {}
    if mk == "KR":
        universe = _load_kr_cache_lookup(path)
    else:
        universe = _load_us_universe_lookup(path)
    tables = _load_table_code_set(conn, mk)

    combined: Dict[str, str] = {}
    for src in (tables, universe, ledger):
        for k, v in src.items():
            if k and k not in combined and not is_blank_identity_name(v):
                combined[k] = v
    for k, v in ledger.items():
        if k and not is_blank_identity_name(v):
            combined[k] = v

    stats = NameLookupStats(
        ledger=len(ledger),
        universe=len(universe),
        table_codes=len(tables),
    )
    return combined, stats, ledger, universe, tables


def resolve_trade_name(
    market: str,
    code: Any,
    name: Any,
    *,
    ledger: Optional[Dict[str, str]] = None,
    universe: Optional[Dict[str, str]] = None,
    table_codes: Optional[Dict[str, str]] = None,
    combined: Optional[Dict[str, str]] = None,
) -> Tuple[str, str]:
    """
    (표시명, source) — row | ledger | universe | table_code | unresolved
    """
    if not is_blank_identity_name(name):
        return str(name).strip(), "row"
    key = normalize_ticker_code(market, code)
    if not key:
        return "종목미상", "unresolved"
    led = ledger or {}
    uni = universe or {}
    tbl = table_codes or {}
    if key in led and not is_blank_identity_name(led[key]):
        return led[key], "ledger"
    if key in uni and not is_blank_identity_name(uni[key]):
        return uni[key], "universe"
    if key in tbl:
        return key, "table_code"
    if combined and key in combined and combined[key] != key:
        return combined[key], "universe"
    return "종목미상", "unresolved"


def _fetch_forward_trades_df(conn: sqlite3.Connection, market: str) -> pd.DataFrame:
    mk = str(market).upper()
    df = pd.read_sql(
        """
        SELECT id, market, code, name, status, entry_date, exit_date,
               final_ret, flow_tags, sig_type
        FROM forward_trades
        WHERE market=?
          AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
        ORDER BY id DESC
        """,
        conn,
        params=(mk,),
    )
    if df.empty:
        return df
    if "exit_date" in df.columns:
        df["exit_date"] = normalize_date_series(df["exit_date"])
    if "entry_date" in df.columns:
        df["entry_date"] = normalize_date_series(df["entry_date"])
    df["final_ret"] = pd.to_numeric(df["final_ret"], errors="coerce")
    return df


def _pipeline_health(
    conn: sqlite3.Connection,
    market: str,
    *,
    rolling_days: int = 90,
    ref_kst: Optional[datetime] = None,
) -> PipelineHealth:
    mk = str(market).upper()
    wm = query_latest_closed_trade_date(conn, mk)
    tk = ReportTimekeeper.for_market(
        mk, rolling_days=rolling_days, ref_kst=ref_kst, db_watermark_exit=wm
    )
    lag = business_lag_days(wm, tk.session_anchor, market=mk)
    _candle_wm: Optional[str] = resolve_data_candle_watermark(mk)
    st = evaluate_staleness(
        tk,
        live_row_count=_count_live_today(conn, mk, tk.session_anchor),
        data_candle_watermark=_candle_wm,
    )

    n_open = conn.execute(
        "SELECT COUNT(*) FROM forward_trades WHERE market=? AND UPPER(TRIM(status))='OPEN'",
        (mk,),
    ).fetchone()[0]
    n_closed_all = conn.execute(
        "SELECT COUNT(*) FROM forward_trades WHERE market=? AND UPPER(status) LIKE 'CLOSED%'",
        (mk,),
    ).fetchone()[0]
    max_entry_open = conn.execute(
        """
        SELECT MAX(substr(TRIM(entry_date),1,10)) FROM forward_trades
        WHERE market=? AND UPPER(TRIM(status))='OPEN'
        """,
        (mk,),
    ).fetchone()[0]

    df = _fetch_forward_trades_df(conn, mk)
    n_closed_window = 0
    if not df.empty:
        status_s = df["status"].astype(str).str.upper()
        closed_mask = status_s.str.contains("CLOSED", na=False)
        closed_day = closed_event_dates(df)
        win = closed_mask & in_date_window(
            closed_day, tk.rolling_cutoff, tk.session_anchor
        )
        n_closed_window = int(win.sum())

    return PipelineHealth(
        market=mk,
        session_anchor=tk.session_anchor,
        rolling_cutoff=tk.rolling_cutoff,
        db_watermark_exit=wm,
        lag_business_days=lag,
        staleness_grade=st.grade,
        n_open=int(n_open or 0),
        n_closed_all=int(n_closed_all or 0),
        n_closed_window=n_closed_window,
        n_live_today=_count_live_today(conn, mk, tk.session_anchor),
        max_entry_open=str(max_entry_open)[:10] if max_entry_open else None,
    )


def _count_live_today(conn: sqlite3.Connection, market: str, anchor: str) -> int:
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) FROM forward_trades
            WHERE market=? AND UPPER(status) LIKE 'CLOSED%'
              AND substr(COALESCE(NULLIF(TRIM(exit_date),''), entry_date),1,10)=?
              AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
            """,
            (market, anchor),
        ).fetchone()
        return int(row[0] if row else 0)
    except sqlite3.Error:
        return 0


def diagnose_forward_trade_identity(
    conn: sqlite3.Connection,
    market: str,
    *,
    rolling_days: int = 90,
    ref_kst: Optional[datetime] = None,
    db_path: Optional[str] = None,
    row_limit: int = 40,
) -> IdentityDiagnosticReport:
    mk = str(market).upper()
    path = db_path or MARKET_DATA_DB_PATH
    now_kst = (ref_kst or datetime.now(_KR_TZ)).astimezone(_KR_TZ).strftime(
        "%Y-%m-%d %H:%M:%S KST"
    )
    pipeline = _pipeline_health(conn, mk, rolling_days=rolling_days, ref_kst=ref_kst)
    lookup, lookup_stats, ledger, universe, tables = build_name_lookup(conn, mk, db_path=path)

    df = _fetch_forward_trades_df(conn, mk)
    gap_rows_all: List[IdentityGapRow] = []
    gap_window_ids: set[int] = set()
    gap_open_ids: set[int] = set()

    if not df.empty:
        status_s = df["status"].astype(str).str.upper()
        closed_mask = status_s.str.contains("CLOSED", na=False)
        open_mask = status_s == "OPEN"
        closed_day = closed_event_dates(df)
        window_mask = closed_mask & in_date_window(
            closed_day, pipeline.rolling_cutoff, pipeline.session_anchor
        )

        for _, row in df.iterrows():
            gap_class = classify_identity_row(row.get("name"), row.get("code"), market=mk)
            if gap_class == "ok":
                continue
            rid = int(row["id"])
            resolved, rsrc = resolve_trade_name(
                mk,
                row.get("code"),
                row.get("name"),
                ledger=ledger,
                universe=universe,
                table_codes=tables,
                combined=lookup,
            )
            gap = IdentityGapRow(
                id=rid,
                market=mk,
                code=normalize_ticker_code(mk, row.get("code")) or "—",
                name=str(row.get("name") or "").strip(),
                status=str(row.get("status") or ""),
                entry_date=str(row.get("entry_date") or "")[:10],
                exit_date=str(row.get("exit_date") or "")[:10],
                final_ret=float(row["final_ret"]) if pd.notna(row.get("final_ret")) else None,
                flow_tags=str(row.get("flow_tags") or "").strip(),
                gap_class=gap_class,
                resolved_name=resolved,
                resolve_source=rsrc,
            )
            gap_rows_all.append(gap)
            if bool(window_mask.loc[row.name]):
                gap_window_ids.add(rid)
            if bool(open_mask.loc[row.name]):
                gap_open_ids.add(rid)

    gap_rows_all.sort(key=lambda g: (g.exit_date or g.entry_date, g.id), reverse=True)
    display_rows = gap_rows_all[: max(0, int(row_limit))]

    flow_tag_gaps = _summarize_flow_tag_gaps(conn, mk, gap_rows_all, pipeline)

    notes: List[str] = []
    if pipeline.lag_business_days >= 2:
        notes.append(
            f"청산 워터마크 {pipeline.db_watermark_exit or '—'} 가 앵커 "
            f"{pipeline.session_anchor} 대비 {pipeline.lag_business_days}영업일 지연 — "
            "태그 리포트는 과거 청산 스냅샷일 수 있음."
        )
    if pipeline.n_open == 0 and pipeline.lag_business_days >= 1:
        notes.append(
            "OPEN=0 이면서 워터마크 지연 — 스캔/청산 파이프라인 점검 "
            "(SessionDeduplicationGuard · track_daily_positions · cron TZ)."
        )
    if gap_rows_all and lookup_stats.ledger == 0 and lookup_stats.universe == 0:
        notes.append("이름 lookup 소스(장부·유니버스) 비어 있음 — data_updater / krx_list 실행 확인.")

    verdict = _build_verdict(
        pipeline=pipeline,
        n_gap_window=len(gap_window_ids),
        n_gap_all=len(gap_rows_all),
        lookup_stats=lookup_stats,
    )

    return IdentityDiagnosticReport(
        market=mk,
        db_path=path,
        generated_at_kst=now_kst,
        pipeline=pipeline,
        lookup_stats=lookup_stats,
        n_gap_all=len(gap_rows_all),
        n_gap_window=len(gap_window_ids),
        n_gap_open=len(gap_open_ids),
        gap_rows=display_rows,
        flow_tag_gaps=flow_tag_gaps,
        verdict=verdict,
        notes=notes,
    )


def _summarize_flow_tag_gaps(
    conn: sqlite3.Connection,
    market: str,
    gaps: List[IdentityGapRow],
    pipeline: PipelineHealth,
) -> List[FlowTagGapSummary]:
    if not gaps:
        return []
    gap_ids = {g.id for g in gaps}
    df = _fetch_forward_trades_df(conn, market)
    if df.empty:
        return []
    status_s = df["status"].astype(str).str.upper()
    closed_mask = status_s.str.contains("CLOSED", na=False)
    closed_day = closed_event_dates(df)
    window_mask = closed_mask & in_date_window(
        closed_day, pipeline.rolling_cutoff, pipeline.session_anchor
    )
    sub = df.loc[window_mask & df["id"].isin(gap_ids)].copy()
    if sub.empty:
        return []

    records: List[Tuple[str, int, float, str]] = []
    for _, row in sub.iterrows():
        tags = str(row.get("flow_tags") or "").split()
        fr = row.get("final_ret")
        fr_f = float(fr) if pd.notna(fr) else 0.0
        code = normalize_ticker_code(market, row.get("code")) or "?"
        for tag in tags:
            t = tag.strip()
            if not t or t.lower() in ("nan", "none"):
                continue
            records.append((t, int(row["id"]), fr_f, code))

    if not records:
        return []

    tag_df = pd.DataFrame(records, columns=["tag", "id", "final_ret", "code"])
    n_by_tag: Dict[str, int] = {}
    df_win = df.loc[window_mask]
    for _, wrow in df_win.iterrows():
        for t in str(wrow.get("flow_tags") or "").split():
            ts = t.strip()
            if ts and ts.lower() not in ("nan", "none", ""):
                n_by_tag[ts] = n_by_tag.get(ts, 0) + 1

    out: List[FlowTagGapSummary] = []
    for tag, grp in tag_df.groupby("tag"):
        codes = tuple(sorted(set(grp["code"].astype(str).tolist()))[:5])
        out.append(
            FlowTagGapSummary(
                tag=str(tag),
                gap_rows=int(len(grp)),
                n_closed_window=int(n_by_tag.get(tag, 0)),
                worst_final_ret=float(grp["final_ret"].min()) if not grp.empty else None,
                sample_codes=codes,
            )
        )
    out.sort(key=lambda x: x.gap_rows, reverse=True)
    return out[:15]


def _build_verdict(
    *,
    pipeline: PipelineHealth,
    n_gap_window: int,
    n_gap_all: int,
    lookup_stats: NameLookupStats,
) -> str:
    if pipeline.lag_business_days >= 2 and pipeline.n_open == 0:
        return "PIPELINE_STALL — 워터마크·OPEN 정체 우선 복구 후 이름 백필"
    if n_gap_window > 0 and lookup_stats.ledger + lookup_stats.universe > 0:
        return "IDENTITY_REPAIRABLE — 롤링 윈도우 내 이름 공백, lookup으로 백필 가능"
    if n_gap_all > 0 and n_gap_window == 0:
        return "IDENTITY_HISTORICAL — 과거 행만 공백, 현재 리포트 영향 제한적"
    if n_gap_all == 0:
        return "IDENTITY_OK"
    return "IDENTITY_GAP — 수동 코드·INSERT 경로 점검 필요"


def backfill_forward_trade_names(
    conn: sqlite3.Connection,
    market: str,
    *,
    dry_run: bool = True,
    db_path: Optional[str] = None,
    only_open: bool = False,
    only_window: bool = True,
    rolling_days: int = 90,
    ref_kst: Optional[datetime] = None,
) -> BackfillResult:
    mk = str(market).upper()
    path = db_path or MARKET_DATA_DB_PATH
    lookup, _stats, ledger, universe, tables = build_name_lookup(conn, mk, db_path=path)
    pipeline = _pipeline_health(conn, mk, rolling_days=rolling_days, ref_kst=ref_kst)

    df = _fetch_forward_trades_df(conn, mk)
    result = BackfillResult(
        market=mk,
        dry_run=bool(dry_run),
        candidates=0,
        updated=0,
        skipped_no_lookup=0,
        skipped_already_ok=0,
    )
    if df.empty:
        return result

    status_s = df["status"].astype(str).str.upper()
    closed_mask = status_s.str.contains("CLOSED", na=False)
    open_mask = status_s == "OPEN"
    closed_day = closed_event_dates(df)
    window_mask = closed_mask & in_date_window(
        closed_day, pipeline.rolling_cutoff, pipeline.session_anchor
    )
    if only_open:
        scope = open_mask
    elif only_window:
        scope = window_mask | open_mask
    else:
        scope = pd.Series(True, index=df.index)

    for _, row in df.loc[scope].iterrows():
        if classify_identity_row(row.get("name"), row.get("code"), market=mk) == "ok":
            result.skipped_already_ok += 1
            continue
        key = normalize_ticker_code(mk, row.get("code"))
        resolved, src = resolve_trade_name(
            mk,
            row.get("code"),
            row.get("name"),
            ledger=ledger,
            universe=universe,
            table_codes=tables,
            combined=lookup,
        )
        if src in ("table_code", "unresolved"):
            result.skipped_no_lookup += 1
            continue
        if is_blank_identity_name(resolved):
            result.skipped_no_lookup += 1
            continue
        if str(row.get("name") or "").strip() == resolved:
            result.skipped_already_ok += 1
            continue

        result.candidates += 1
        rid = int(row["id"])
        if len(result.sample_updates) < 12:
            result.sample_updates.append((rid, key, str(row.get("name") or ""), resolved))

        if dry_run:
            continue
        conn.execute(
            """
            UPDATE forward_trades SET name=?
            WHERE id=? AND market=?
              AND (name IS NULL OR TRIM(name)='' OR LOWER(TRIM(name)) IN (
                'nan','none','null','nat','—','-','종목미상','unknown'
              ))
            """,
            (resolved, rid, mk),
        )
        result.updated += 1

    if not dry_run and result.updated > 0:
        conn.commit()
    return result


def format_diagnostic_report_text(rep: IdentityDiagnosticReport) -> str:
    p = rep.pipeline
    lines = [
        f"=== forward_trades 식별자 진단 [{rep.market}] ===",
        f"generated: {rep.generated_at_kst}",
        f"db: {rep.db_path}",
        "",
        "--- 파이프라인 헬스 ---",
        f"anchor={p.session_anchor} cutoff={p.rolling_cutoff} watermark={p.db_watermark_exit or '—'}",
        f"staleness={p.staleness_grade} lag={p.lag_business_days}영업일",
        f"OPEN={p.n_open} max_entry_open={p.max_entry_open or '—'}",
        f"CLOSED all={p.n_closed_all} window={p.n_closed_window} live_today={p.n_live_today}",
        "",
        "--- 이름 lookup 소스 ---",
        f"ledger={rep.lookup_stats.ledger} universe={rep.lookup_stats.universe} "
        f"table_codes={rep.lookup_stats.table_codes}",
        "",
        f"VERDICT: {rep.verdict}",
        f"gap_all={rep.n_gap_all} gap_window={rep.n_gap_window} gap_open={rep.n_gap_open}",
    ]
    for note in rep.notes:
        lines.append(f"NOTE: {note}")

    if rep.flow_tag_gaps:
        lines.append("")
        lines.append("--- flow_tags x 이름공백 (롤링 윈도우) ---")
        for ft in rep.flow_tag_gaps:
            pct = (
                100.0 * ft.gap_rows / ft.n_closed_window
                if ft.n_closed_window
                else 0.0
            )
            lines.append(
                f"  {ft.tag}: gap={ft.gap_rows}/{ft.n_closed_window} ({pct:.1f}%) "
                f"worst={ft.worst_final_ret:+.1f}% codes={','.join(ft.sample_codes)}"
            )

    if rep.gap_rows:
        lines.append("")
        lines.append(f"--- 상위 gap 행 (max {len(rep.gap_rows)}) ---")
        for g in rep.gap_rows:
            lines.append(
                f"  id={g.id} {g.gap_class} code={g.code} name='{g.name}' "
                f"→ resolve={g.resolved_name}({g.resolve_source}) "
                f"exit={g.exit_date} ret={g.final_ret} tags={g.flow_tags[:60]}"
            )
    return "\n".join(lines)


def diagnostic_report_to_dict(rep: IdentityDiagnosticReport) -> Dict[str, Any]:
    d = asdict(rep)
    d["pipeline"] = asdict(rep.pipeline)
    d["lookup_stats"] = asdict(rep.lookup_stats)
    d["gap_rows"] = [asdict(g) for g in rep.gap_rows]
    d["flow_tag_gaps"] = [asdict(ft) for ft in rep.flow_tag_gaps]
    return d


def run_identity_repair_for_market(
    market: str,
    *,
    db_path: Optional[str] = None,
    dry_run: Optional[bool] = None,
    rolling_days: int = 90,
) -> Tuple[IdentityDiagnosticReport, BackfillResult]:
    """팩토리·CLI 공용 — 진단 후 선택적 백필."""
    path = db_path or MARKET_DATA_DB_PATH
    if dry_run is None:
        auto = str(os.environ.get("FORWARD_IDENTITY_AUTO_REPAIR", "0")).strip().lower()
        dry_run = auto not in ("1", "true", "yes", "on")

    conn = sqlite3.connect(path, timeout=60)
    try:
        diag = diagnose_forward_trade_identity(
            conn, market, rolling_days=rolling_days, db_path=path
        )
        backfill = backfill_forward_trade_names(
            conn,
            market,
            dry_run=dry_run,
            db_path=path,
            only_window=True,
            rolling_days=rolling_days,
        )
        return diag, backfill
    finally:
        conn.close()


def format_repair_log_line(
    diag: IdentityDiagnosticReport,
    backfill: BackfillResult,
) -> str:
    mode = "DRY-RUN" if backfill.dry_run else "APPLY"
    return (
        f"[identity/{diag.market}] {mode} verdict={diag.verdict} "
        f"gap_win={diag.n_gap_window} candidates={backfill.candidates} "
        f"updated={backfill.updated} skip_lookup={backfill.skipped_no_lookup} "
        f"wm={diag.pipeline.db_watermark_exit or '—'} lag={diag.pipeline.lag_business_days}"
    )
