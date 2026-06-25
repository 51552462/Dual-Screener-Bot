"""
bitget_forward_trades 식별자 진단·백필 — 주식 forward_trade_identity 패턴 (코인 적응).

코인 SSOT: `symbol` 단일 필드 (주식 code+name 대체).
lookup: 장부 자기참조 → market_data.sqlite BITGET_{SPOT|FUT}_* 테이블명.
"""
from __future__ import annotations

import os
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

import pandas as pd

from bitget.forward.shared import DB_PATH, init_forward_db
from bitget.infra.data_paths import market_data_db_path
from bitget.reports.bitget_report_context import BitgetReportContext

IdentityClass = Literal[
    "ok",
    "symbol_missing",
    "junk_symbol",
    "synthetic_label",
]

_BLANK = frozenset({"", "nan", "none", "null", "nat", "—", "-", "unknown", "종목미상"})
_SYNTH_RE = re.compile(r"^(spot|futures|crypto):", re.I)
_BITGET_TABLE_RE = re.compile(r"^BITGET_(SPOT|FUT)_(.+)_(\d+D|\d+H|\d+M)$", re.I)


@dataclass(frozen=True)
class NameLookupStats:
    ledger: int = 0
    table_symbols: int = 0


@dataclass(frozen=True)
class IdentityGapRow:
    id: int
    market_type: str
    symbol: str
    status: str
    entry_date: str
    exit_date: str
    final_ret: Optional[float]
    flow_tags: str
    gap_class: IdentityClass
    resolved_symbol: str
    resolve_source: str


@dataclass(frozen=True)
class PipelineHealth:
    market_type: str
    session_anchor: str
    rolling_cutoff: str
    db_watermark_exit: Optional[str]
    lag_days: int
    n_open: int
    n_closed_all: int
    n_closed_window: int
    n_live_today: int
    max_entry_open: Optional[str]


@dataclass
class IdentityDiagnosticReport:
    market_type: str
    db_path: str
    generated_at_utc: str
    pipeline: PipelineHealth
    lookup_stats: NameLookupStats
    n_gap_all: int
    n_gap_window: int
    n_gap_open: int
    gap_rows: List[IdentityGapRow] = field(default_factory=list)
    verdict: str = ""
    notes: List[str] = field(default_factory=list)


@dataclass
class BackfillResult:
    market_type: str
    dry_run: bool
    candidates: int
    updated: int
    skipped_no_lookup: int
    skipped_already_ok: int
    sample_updates: List[Tuple[int, str, str, str]] = field(default_factory=list)


def is_blank_symbol(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    return str(val).strip().lower() in _BLANK


def classify_identity_row(symbol: Any) -> IdentityClass:
    if is_blank_symbol(symbol):
        return "symbol_missing"
    s = str(symbol).strip()
    if s.lower() in _BLANK:
        return "junk_symbol"
    if _SYNTH_RE.match(s):
        return "synthetic_label"
    return "ok"


def _norm_market_type(market: str) -> str:
    m = str(market or "all").strip().lower()
    if m in ("fut", "future", "futures"):
        return "futures"
    if m in ("sp", "spot"):
        return "spot"
    return m


def _market_filter_sql(market: str) -> Tuple[str, tuple]:
    mk = _norm_market_type(market)
    if mk == "futures":
        return "LOWER(IFNULL(market_type,'')) = 'futures'", ()
    if mk == "spot":
        return "LOWER(IFNULL(market_type,'')) = 'spot'", ()
    return "1=1", ()


def _load_ledger_symbol_lookup(conn: sqlite3.Connection, market: str) -> Dict[str, str]:
    where, _ = _market_filter_sql(market)
    rows = conn.execute(
        f"""
        SELECT symbol, MAX(id) AS mid
        FROM bitget_forward_trades
        WHERE {where}
          AND symbol IS NOT NULL AND TRIM(symbol) != ''
          AND LOWER(TRIM(symbol)) NOT IN ('unknown','종목미상','nan','none')
        GROUP BY symbol
        ORDER BY mid DESC
        LIMIT 5000
        """
    ).fetchall()
    return {str(r[0]).strip(): str(r[0]).strip() for r in rows if r and r[0]}


def _load_table_symbol_lookup(market: str) -> Dict[str, str]:
    """BITGET_SPOT_BTC_USDT_1H → BTC_USDT"""
    out: Dict[str, str] = {}
    path = market_data_db_path()
    if not os.path.isfile(path):
        return out
    want = "SPOT" if _norm_market_type(market) == "spot" else "FUT"
    try:
        conn = sqlite3.connect(path, timeout=30)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (f"BITGET_{want}_%",),
        ).fetchall()
        conn.close()
    except sqlite3.Error:
        return out
    for (tbl,) in rows:
        m = _BITGET_TABLE_RE.match(str(tbl))
        if not m:
            continue
        sym = m.group(2).replace("__", "_")
        out[sym.upper()] = sym
    return out


def build_symbol_lookup(
    conn: sqlite3.Connection,
    market: str,
) -> Tuple[Dict[str, str], NameLookupStats]:
    ledger = _load_ledger_symbol_lookup(conn, market)
    tables = _load_table_symbol_lookup(market)
    combined = dict(tables)
    combined.update(ledger)
    stats = NameLookupStats(ledger=len(ledger), table_symbols=len(tables))
    return combined, stats


def resolve_trade_symbol(
    symbol: Any,
    *,
    combined: Dict[str, str],
) -> Tuple[str, str]:
    if not is_blank_symbol(symbol):
        s = str(symbol).strip()
        return s, "ok"
    return "—", "unresolved"


def _fetch_trades_df(conn: sqlite3.Connection, market: str) -> pd.DataFrame:
    where, _ = _market_filter_sql(market)
    df = pd.read_sql(
        f"""
        SELECT id, market_type, symbol, status, entry_date, exit_date,
               final_ret, flow_tags, sig_type, timeframe
        FROM bitget_forward_trades
        WHERE {where}
          AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
        ORDER BY id DESC
        """,
        conn,
    )
    if df.empty:
        return df
    for col in ("entry_date", "exit_date"):
        if col in df.columns:
            df[col] = df[col].astype(str).str[:10]
    df["final_ret"] = pd.to_numeric(df.get("final_ret"), errors="coerce")
    return df


def _pipeline_health(conn: sqlite3.Connection, market: str, *, rolling_days: int = 90) -> PipelineHealth:
    mk = _norm_market_type(market)
    ctx = BitgetReportContext.build(rolling_days=rolling_days)
    tk = ctx.timekeeper_for(mk)
    where, _ = _market_filter_sql(market)

    wm_row = conn.execute(
        f"""
        SELECT MAX(substr(CAST(exit_date AS TEXT),1,10))
        FROM bitget_forward_trades
        WHERE {where}
          AND status LIKE 'CLOSED%'
          AND exit_date IS NOT NULL AND TRIM(CAST(exit_date AS TEXT)) != ''
        """
    ).fetchone()
    wm = wm_row[0] if wm_row else None
    lag = ctx.lag_for(mk)

    n_open = conn.execute(
        f"SELECT COUNT(*) FROM bitget_forward_trades WHERE {where} AND UPPER(TRIM(status)) IN ('OPEN','ACTIVE')"
    ).fetchone()[0]
    n_closed_all = conn.execute(
        f"SELECT COUNT(*) FROM bitget_forward_trades WHERE {where} AND UPPER(status) LIKE 'CLOSED%'"
    ).fetchone()[0]
    max_entry_open = conn.execute(
        f"""
        SELECT MAX(substr(TRIM(entry_date),1,10)) FROM bitget_forward_trades
        WHERE {where} AND UPPER(TRIM(status)) IN ('OPEN','ACTIVE')
        """
    ).fetchone()[0]

    df = _fetch_trades_df(conn, market)
    n_closed_window = 0
    if not df.empty:
        ent = df["entry_date"].astype(str).str[:10]
        st = df["status"].astype(str).str.upper()
        win = (ent >= tk.rolling_cutoff) & (ent <= tk.session_anchor) & st.str.contains("CLOSED", na=False)
        n_closed_window = int(win.sum())

    anchor = tk.session_anchor
    n_live_today = conn.execute(
        f"""
        SELECT COUNT(*) FROM bitget_forward_trades
        WHERE {where} AND UPPER(status) LIKE 'CLOSED%'
          AND substr(COALESCE(NULLIF(TRIM(exit_date),''), entry_date),1,10)=?
        """,
        (anchor,),
    ).fetchone()[0]

    return PipelineHealth(
        market_type=mk,
        session_anchor=anchor,
        rolling_cutoff=tk.rolling_cutoff,
        db_watermark_exit=wm,
        lag_days=lag,
        n_open=int(n_open or 0),
        n_closed_all=int(n_closed_all or 0),
        n_closed_window=n_closed_window,
        n_live_today=int(n_live_today or 0),
        max_entry_open=str(max_entry_open)[:10] if max_entry_open else None,
    )


def _build_verdict(
    *,
    pipeline: PipelineHealth,
    n_gap_window: int,
    n_gap_all: int,
    lookup_stats: NameLookupStats,
) -> str:
    if pipeline.lag_days >= 2 and pipeline.n_open == 0:
        return "PIPELINE_STALL — 워터마크·OPEN 정체 우선 복구"
    if n_gap_window > 0 and lookup_stats.ledger + lookup_stats.table_symbols > 0:
        return "IDENTITY_REPAIRABLE — 윈도우 내 symbol 공백, lookup 백필 가능"
    if n_gap_all > 0 and n_gap_window == 0:
        return "IDENTITY_HISTORICAL — 과거 행만 공백"
    if n_gap_all == 0:
        return "IDENTITY_OK"
    return "IDENTITY_GAP — INSERT·스캐너 symbol 경로 점검"


def diagnose_forward_trade_identity(
    conn: sqlite3.Connection,
    market: str,
    *,
    rolling_days: int = 90,
    db_path: Optional[str] = None,
    row_limit: int = 40,
) -> IdentityDiagnosticReport:
    mk = _norm_market_type(market)
    path = db_path or DB_PATH
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    pipeline = _pipeline_health(conn, mk, rolling_days=rolling_days)
    lookup, lookup_stats = build_symbol_lookup(conn, mk)

    df = _fetch_trades_df(conn, mk)
    gap_rows_all: List[IdentityGapRow] = []
    gap_window_ids: set[int] = set()
    gap_open_ids: set[int] = set()

    if not df.empty:
        st = df["status"].astype(str).str.upper()
        ent = df["entry_date"].astype(str).str[:10]
        closed_mask = st.str.contains("CLOSED", na=False)
        open_mask = st.isin(["OPEN", "ACTIVE"])
        window_mask = closed_mask & (ent >= pipeline.rolling_cutoff) & (ent <= pipeline.session_anchor)

        for _, row in df.iterrows():
            gap_class = classify_identity_row(row.get("symbol"))
            if gap_class == "ok":
                continue
            rid = int(row["id"])
            resolved, rsrc = resolve_trade_symbol(row.get("symbol"), combined=lookup)
            if gap_class == "symbol_missing" and lookup:
                for sym in lookup.values():
                    resolved, rsrc = sym, "ledger_or_table"
                    break
            gap = IdentityGapRow(
                id=rid,
                market_type=str(row.get("market_type") or mk),
                symbol=str(row.get("symbol") or "").strip(),
                status=str(row.get("status") or ""),
                entry_date=str(row.get("entry_date") or "")[:10],
                exit_date=str(row.get("exit_date") or "")[:10],
                final_ret=float(row["final_ret"]) if pd.notna(row.get("final_ret")) else None,
                flow_tags=str(row.get("flow_tags") or "").strip(),
                gap_class=gap_class,
                resolved_symbol=resolved,
                resolve_source=rsrc,
            )
            gap_rows_all.append(gap)
            if bool(window_mask.loc[row.name]):
                gap_window_ids.add(rid)
            if bool(open_mask.loc[row.name]):
                gap_open_ids.add(rid)

    gap_rows_all.sort(key=lambda g: (g.exit_date or g.entry_date, g.id), reverse=True)
    notes: List[str] = []
    if pipeline.lag_days >= 2:
        notes.append(
            f"청산 워터마크 {pipeline.db_watermark_exit or '—'} lag {pipeline.lag_days}d — "
            "리포트는 과거 스냅샷일 수 있음."
        )
    verdict = _build_verdict(
        pipeline=pipeline,
        n_gap_window=len(gap_window_ids),
        n_gap_all=len(gap_rows_all),
        lookup_stats=lookup_stats,
    )
    return IdentityDiagnosticReport(
        market_type=mk,
        db_path=path,
        generated_at_utc=now_utc,
        pipeline=pipeline,
        lookup_stats=lookup_stats,
        n_gap_all=len(gap_rows_all),
        n_gap_window=len(gap_window_ids),
        n_gap_open=len(gap_open_ids),
        gap_rows=gap_rows_all[: max(0, int(row_limit))],
        verdict=verdict,
        notes=notes,
    )


def backfill_forward_trade_symbols(
    conn: sqlite3.Connection,
    market: str,
    *,
    dry_run: bool = True,
    only_window: bool = True,
    rolling_days: int = 90,
) -> BackfillResult:
    mk = _norm_market_type(market)
    lookup, _stats = build_symbol_lookup(conn, mk)
    pipeline = _pipeline_health(conn, mk, rolling_days=rolling_days)
    df = _fetch_trades_df(conn, mk)
    result = BackfillResult(
        market_type=mk,
        dry_run=bool(dry_run),
        candidates=0,
        updated=0,
        skipped_no_lookup=0,
        skipped_already_ok=0,
    )
    if df.empty:
        return result

    st = df["status"].astype(str).str.upper()
    ent = df["entry_date"].astype(str).str[:10]
    closed_mask = st.str.contains("CLOSED", na=False)
    open_mask = st.isin(["OPEN", "ACTIVE"])
    window_mask = closed_mask & (ent >= pipeline.rolling_cutoff) & (ent <= pipeline.session_anchor)
    scope = window_mask | open_mask if only_window else pd.Series(True, index=df.index)

    for _, row in df.loc[scope].iterrows():
        gap_class = classify_identity_row(row.get("symbol"))
        if gap_class == "ok":
            result.skipped_already_ok += 1
            continue

        old_sym = str(row.get("symbol") or "").strip()
        resolved = old_sym
        src = "ok"

        if gap_class == "symbol_missing":
            tf = str(row.get("timeframe") or "?")
            mtype = str(row.get("market_type") or mk)
            resolved = f"{mtype}:{tf}"
            src = "synthetic_fallback"
        elif gap_class == "junk_symbol":
            resolved, src = resolve_trade_symbol(row.get("symbol"), combined=lookup)
            if src == "unresolved":
                result.skipped_no_lookup += 1
                continue

        if is_blank_symbol(resolved) or resolved == old_sym:
            result.skipped_no_lookup += 1
            continue

        result.candidates += 1
        rid = int(row["id"])
        if len(result.sample_updates) < 12:
            result.sample_updates.append((rid, old_sym, resolved, src))

        if dry_run:
            continue
        conn.execute(
            """
            UPDATE bitget_forward_trades SET symbol=?
            WHERE id=?
              AND (symbol IS NULL OR TRIM(symbol)='' OR LOWER(TRIM(symbol)) IN (
                'nan','none','null','nat','—','-','종목미상','unknown'
              ))
            """,
            (resolved, rid),
        )
        result.updated += 1

    if not dry_run and result.updated > 0:
        conn.commit()
    return result


def format_diagnostic_report_text(rep: IdentityDiagnosticReport) -> str:
    p = rep.pipeline
    lines = [
        f"=== bitget_forward_trades 식별자 진단 [{rep.market_type}] ===",
        f"generated: {rep.generated_at_utc}",
        f"db: {rep.db_path}",
        "",
        "--- 파이프라인 헬스 ---",
        f"anchor={p.session_anchor} cutoff={p.rolling_cutoff} watermark={p.db_watermark_exit or '—'}",
        f"lag={p.lag_days}d OPEN={p.n_open} max_entry_open={p.max_entry_open or '—'}",
        f"CLOSED all={p.n_closed_all} window={p.n_closed_window} live_today={p.n_live_today}",
        "",
        f"lookup ledger={rep.lookup_stats.ledger} table_symbols={rep.lookup_stats.table_symbols}",
        f"VERDICT: {rep.verdict}",
        f"gap_all={rep.n_gap_all} gap_window={rep.n_gap_window} gap_open={rep.n_gap_open}",
    ]
    for note in rep.notes:
        lines.append(f"NOTE: {note}")
    for g in rep.gap_rows[:15]:
        lines.append(
            f"  id={g.id} {g.gap_class} sym='{g.symbol}' → {g.resolved_symbol}({g.resolve_source}) "
            f"exit={g.exit_date} ret={g.final_ret}"
        )
    return "\n".join(lines)


def format_repair_log_line(diag: IdentityDiagnosticReport, backfill: BackfillResult) -> str:
    return (
        f"🛰️ [Bitget] identity/{diag.market_type}: {diag.verdict} · "
        f"gap_w={diag.n_gap_window} · backfill candidates={backfill.candidates} "
        f"updated={backfill.updated} dry_run={backfill.dry_run}"
    )


def count_identity_gaps(market: str = "all") -> Dict[str, Any]:
    mk = _norm_market_type(market)
    if not os.path.isfile(DB_PATH):
        return {"market": mk, "blank_symbol": 0, "junk_symbol": 0, "skipped": "no_db"}
    init_forward_db()
    conn = sqlite3.connect(DB_PATH, timeout=60)
    try:
        diag = diagnose_forward_trade_identity(conn, mk if mk != "all" else "spot")
        diag_f = (
            diagnose_forward_trade_identity(conn, "futures")
            if mk == "all"
            else None
        )
    finally:
        conn.close()
    blank = sum(1 for g in diag.gap_rows if g.gap_class == "symbol_missing")
    junk = sum(1 for g in diag.gap_rows if g.gap_class == "junk_symbol")
    if diag_f:
        blank += sum(1 for g in diag_f.gap_rows if g.gap_class == "symbol_missing")
        junk += sum(1 for g in diag_f.gap_rows if g.gap_class == "junk_symbol")
    return {
        "market": mk,
        "blank_symbol": blank,
        "junk_symbol": junk,
        "verdict": diag.verdict,
    }


def run_identity_repair_for_market(
    market: str,
    *,
    db_path: Optional[str] = None,
    dry_run: Optional[bool] = None,
    rolling_days: int = 90,
) -> Dict[str, Any]:
    """진단 + 백필 (주식 run_identity_repair_for_market 패턴)."""
    path = db_path or DB_PATH
    if not os.path.isfile(path):
        return {"market": market, "repaired": 0, "skipped": "no_db"}

    if dry_run is None:
        auto = str(os.environ.get("BITGET_IDENTITY_AUTO_REPAIR", "1")).strip().lower()
        dry_run = auto not in ("1", "true", "yes", "on")

    init_forward_db()
    conn = sqlite3.connect(path, timeout=60)
    try:
        diag = diagnose_forward_trade_identity(
            conn, market, rolling_days=rolling_days, db_path=path
        )
        backfill = backfill_forward_trade_symbols(
            conn,
            market,
            dry_run=dry_run,
            only_window=True,
            rolling_days=rolling_days,
        )
        # blank symbol rows — synthetic label repair (always apply)
        mk = _norm_market_type(market)
        where, _ = _market_filter_sql(mk if mk != "all" else "spot")
        extra = 0
        if not dry_run:
            rows = conn.execute(
                f"""
                SELECT id, market_type, timeframe FROM bitget_forward_trades
                WHERE {where} AND (symbol IS NULL OR TRIM(symbol) = '')
                """
            ).fetchall()
            for rid, mtype, tf in rows:
                label = f"{mtype or 'crypto'}:{tf or '?'}"
                conn.execute(
                    "UPDATE bitget_forward_trades SET symbol = ? WHERE id = ?",
                    (label, int(rid)),
                )
                extra += 1
            if extra:
                conn.commit()
        line = format_repair_log_line(diag, backfill)
        print(line)
        print(format_diagnostic_report_text(diag))
        return {
            "market": mk,
            "repaired": int(backfill.updated) + int(extra),
            "backfill": asdict(backfill),
            "verdict": diag.verdict,
            "diag": {"n_gap_window": diag.n_gap_window, "n_gap_all": diag.n_gap_all},
        }
    finally:
        conn.close()


def run_identity_repair_all() -> Dict[str, Any]:
    s = run_identity_repair_for_market("spot")
    f = run_identity_repair_for_market("futures")
    gaps = count_identity_gaps("all")
    return {
        "spot": s,
        "futures": f,
        "repaired": int(s.get("repaired", 0)) + int(f.get("repaired", 0)),
        "gaps_after": gaps,
    }
