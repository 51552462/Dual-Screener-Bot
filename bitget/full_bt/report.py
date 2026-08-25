"""FULL-BT-3 L1 report — read-only against bitget_full_bt.sqlite (§2 schema).

CAT-J 비편입: reports/ pipeline 미등록 · 자동 트리거 없음.
"""
from __future__ import annotations

import os
import sqlite3
from collections import Counter
from typing import Any, Optional

from bitget.full_bt.checkpoint import ensure_checkpoint_schema, load_full_bt_checkpoint
from bitget.full_bt.harness import REUSED_SCANNER_TIMEFRAMES
from bitget.full_bt.paths import full_bt_db_path

# 15_FULL-BT §3 — 원문 그대로 (요약·재작성 금지)
L1_BANNER = (
    "IV L1 전체이식 가상매매 — 격리 리플레이 결과, LIVE 승격·R6 대체·B1「달성」 판정 금지. "
    "공식 B1 판정은 R6(L2 forward 56일+)만."
)

# 13_B1 §1 인용만 — 수치 재계산·재해석 금지
B1_REFERENCE_BAND = "12~18%/≤5%, 참고용 — 판정 아님"

INCOMPLETE_WARN = "미완료 run — 부분 결과"

# §3 Kill — 실측 0 vs 미측정 구분 (수치 재계산 없음, 표기만)
UNMEASURED_FOOTNOTE = "미측정(거절 이벤트 미저장, 0 아님)"

# paths.py 조사: full_bt_db_path() = 단일 파일(run_id 미포함) → 공유
FULL_BT_DB_PATH_MODE = "공유"

# FULL-BT-HIST-1: entry_date/exit_date = candle axis → wall updated_at 일자 창 비적용
CANDLE_ENTRY_AXIS = True

# CAT-D §4 try_add 11단계 라벨 (거절 카운트 슬롯; step11=N/A 고정)
_TRY_ADD_STEP_NAMES = {
    1: "global_portfolio_circuit",
    2: "doomsday_defcon",
    3: "toxic_anti_pattern",
    4: "kill_switch",
    5: "duplicate_reentry",
    6: "max_open_count",
    7: "per_logic_daily_cap",
    8: "concentration_sector",
    9: "treasury_available",
    10: "zero_notional_guard",
    11: "execution_safety",
}

QUANT_KEYS = (
    "run_id",
    "market_type",
    "symbol_or_agg",
    "period_start",
    "period_end",
    "total_return_pct",
    "mdd_pct",
    "trade_count",
    "b1_reference_band",
)

CLUE_KEYS = (
    "gate_bottleneck_by_step",
    "side_asymmetry",
    "symbol_breakdown",
    "tf_note",
)


def _normalize_mt(market_type: str) -> str:
    mt = str(market_type or "").strip().lower()
    if mt in ("fut", "linear"):
        return "futures"
    if mt not in ("spot", "futures"):
        raise ValueError(f"market_type must be spot|futures, got {market_type!r}")
    return mt


def _tf_note() -> str:
    return f"재사용 TF: {list(REUSED_SCANNER_TIMEFRAMES)}"


def _empty_gate_bottleneck() -> dict[str, Any]:
    """Persisted reject log 없음 → 관측 카운트 0; step11은 N/A 고정 (FULL-BT-1 정책)."""
    out: dict[str, Any] = {}
    for step, name in _TRY_ADD_STEP_NAMES.items():
        key = f"step{step}_{name}"
        out[key] = "N/A" if step == 11 else 0
    return out


def _mdd_pct(equity: list[float]) -> float:
    if not equity:
        return 0.0
    peak = equity[0]
    max_dd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak * 100.0
            if dd > max_dd:
                max_dd = dd
    return round(max_dd, 4)


def _equity_metrics(final_rets: list[float]) -> tuple[float, float]:
    """Closed-trade final_ret(%) → cumulative equity total_return_pct + mdd_pct."""
    if not final_rets:
        return 0.0, 0.0
    eq = [100.0]
    for r in final_rets:
        eq.append(eq[-1] * (1.0 + float(r or 0.0) / 100.0))
    total = round((eq[-1] / eq[0] - 1.0) * 100.0, 4)
    return total, _mdd_pct(eq)


def _load_completed_symbols(
    run_id: str, market_type: str, *, db_path: str
) -> tuple[set[str], set[tuple[str, int]], bool]:
    """Return (symbols, completed pairs, has_any_checkpoint)."""
    ckpt = load_full_bt_checkpoint(run_id, market_type, db_path=db_path)
    if not ckpt:
        return set(), set(), False
    completed: set[tuple[str, int]] = set(ckpt["completed"])
    symbols = {s for s, _ in completed}
    return symbols, completed, True


def _checkpoint_updated_at_window(
    run_id: str, market_type: str, *, db_path: str
) -> tuple[str, str] | None:
    """MIN/MAX(updated_at) for this run_id — shared-DB trade date filter (read-only)."""
    ensure_checkpoint_schema(db_path)
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            """
            SELECT MIN(updated_at), MAX(updated_at)
            FROM bitget_full_bt_checkpoint
            WHERE run_id=? AND market_type=?
            """,
            (run_id, market_type),
        ).fetchone()
    finally:
        conn.close()
    if not row or row[0] is None or row[1] is None:
        return None
    return str(row[0]), str(row[1])


def _date_key(val: str) -> str:
    return str(val or "").strip()[:10]


def _is_incomplete(
    *,
    has_checkpoint: bool,
    completed: set[tuple[str, int]],
    symbols: set[str],
    market_type: str,
    market_db: Optional[str],
) -> bool:
    """Checkpoint 완료 플래그 기준 — 심볼별 기대 batch 대비 누락 시 미완료.

    신규 판정 발명 아님: get_full_bt_window_batches + checkpoint completed set만 대조.
    market_db 없거나 배치 0이면 checkpoint 유무만으로 판단(없음=미완료).
    """
    if not has_checkpoint or not completed:
        return True
    if not symbols:
        return True
    try:
        from bitget.full_bt.batch import get_full_bt_window_batches
        from bitget.infra.memory_policy import TIME_MACHINE_MAX_BARS_PER_TABLE
    except Exception:
        return True
    for sym in symbols:
        batches = get_full_bt_window_batches(
            sym,
            market_type,
            int(TIME_MACHINE_MAX_BARS_PER_TABLE),
            db_path=market_db,
        )
        if not batches:
            # OHLCV 없으면 배치 대조 불가 → checkpoint rows만으로 완료 간주하지 않음
            continue
        expected = {(sym, i) for i in range(len(batches))}
        if not expected.issubset(completed):
            return True
    return False


def _fetch_trades(
    db_path: str,
    market_type: str,
    symbols: set[str],
    *,
    updated_at_window: tuple[str, str] | None = None,
) -> list[dict[str, Any]]:
    """symbol ∩ market_type + (공유 DB) checkpoint updated_at 일자 범위로 entry/exit 제한."""
    if not symbols or not os.path.isfile(db_path):
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='bitget_forward_trades'"
        ).fetchone()
        if not row:
            return []
        placeholders = ",".join("?" for _ in symbols)
        sql = f"""
            SELECT symbol, market_type, position_side, status, entry_date, exit_date, final_ret
            FROM bitget_forward_trades
            WHERE market_type=? AND symbol IN ({placeholders})
        """
        params: list[Any] = [market_type, *sorted(symbols)]
        if updated_at_window is not None:
            lo_d = _date_key(updated_at_window[0])
            hi_d = _date_key(updated_at_window[1])
            # entry_date 또는 exit_date가 체크포인트 일자 구간에 겹치면 포함 (신규 컬럼 없음)
            sql += """
            AND (
                (substr(COALESCE(entry_date,''),1,10) >= ? AND substr(COALESCE(entry_date,''),1,10) <= ?)
                OR (
                    exit_date IS NOT NULL AND exit_date != ''
                    AND substr(exit_date,1,10) >= ? AND substr(exit_date,1,10) <= ?
                )
            )
            """
            params.extend([lo_d, hi_d, lo_d, hi_d])
        sql += " ORDER BY entry_date ASC, id ASC"
        cur = conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _side_asymmetry(market_type: str, trades: list[dict[str, Any]]) -> Any:
    """U3 선례: SPOT = null. FUTURES = LONG/SHORT 진입 카운트(거절 로그 미저장 → rejected=0)."""
    if market_type == "spot":
        return None
    long_e = short_e = 0
    for t in trades:
        side = str(t.get("position_side") or "LONG").upper()
        if side == "SHORT":
            short_e += 1
        else:
            long_e += 1
    return {
        "long_entered": long_e,
        "short_entered": short_e,
        "long_rejected": 0,
        "short_rejected": 0,
    }


def _symbol_breakdown(trades: list[dict[str, Any]]) -> dict[str, Any]:
    entered = Counter(str(t.get("symbol") or "") for t in trades)
    top_entered = entered.most_common(10)
    # reject persist 테이블 없음 (FULL-BT-1/2) — top_rejected 빈 목록
    return {"top_entered": top_entered, "top_rejected": []}


def generate_full_bt_l1_report(
    market_type: str,
    run_id: str,
    *,
    db_path: Optional[str] = None,
    market_db: Optional[str] = None,
) -> dict:
    """§2 PnL/MDD 정량표 + 개선 단서 슬롯 (단일 market_type)."""
    mt = _normalize_mt(market_type)
    path = ensure_checkpoint_schema(db_path or full_bt_db_path())
    symbols, completed, has_ckpt = _load_completed_symbols(run_id, mt, db_path=path)
    incomplete = _is_incomplete(
        has_checkpoint=has_ckpt,
        completed=completed,
        symbols=symbols,
        market_type=mt,
        market_db=market_db,
    )
    # 공유 DB wall 창: candle entry_date와 축 불일치(HIST-1) → 심볼∩market_type만 사용
    time_window = None
    if has_ckpt and not CANDLE_ENTRY_AXIS:
        time_window = _checkpoint_updated_at_window(run_id, mt, db_path=path)
    trades = _fetch_trades(path, mt, symbols, updated_at_window=time_window)
    closed_rets = [
        float(t.get("final_ret") or 0.0)
        for t in trades
        if str(t.get("status") or "").startswith("CLOSED")
    ]
    total_ret, mdd = _equity_metrics(closed_rets)

    dates = [str(t.get("entry_date") or "")[:10] for t in trades if t.get("entry_date")]
    period_start = min(dates) if dates else None
    period_end = max(dates) if dates else None
    for t in trades:
        ed = str(t.get("exit_date") or "")[:10]
        if ed:
            if period_end is None or ed > period_end:
                period_end = ed

    quantitative = {
        "run_id": run_id,
        "market_type": mt,
        "symbol_or_agg": "AGG",
        "period_start": period_start,
        "period_end": period_end,
        "total_return_pct": total_ret,
        "mdd_pct": mdd,
        "trade_count": len(trades),
        "b1_reference_band": B1_REFERENCE_BAND,
    }
    clues = {
        "gate_bottleneck_by_step": _empty_gate_bottleneck(),
        "side_asymmetry": _side_asymmetry(mt, trades),
        "symbol_breakdown": _symbol_breakdown(trades),
        "tf_note": _tf_note(),
    }
    warnings: list[str] = []
    if incomplete:
        warnings.append(INCOMPLETE_WARN)

    return {
        "banner": L1_BANNER,
        "run_id": run_id,
        "market_type": mt,
        "quantitative": quantitative,
        "clues": clues,
        "warnings": warnings,
        "checkpoint_completed_count": len(completed),
        "db_path_mode": FULL_BT_DB_PATH_MODE,
        "updated_at_window": time_window,
    }


def build_full_bt_l1_side_by_side(
    run_id: str,
    *,
    db_path: Optional[str] = None,
    market_db: Optional[str] = None,
) -> dict:
    """§4 SPOT·FUT 분리 집계 후 나란히 — 합산 금지."""
    return {
        "banner": L1_BANNER,
        "run_id": run_id,
        "markets": {
            "spot": generate_full_bt_l1_report(
                "spot", run_id, db_path=db_path, market_db=market_db
            ),
            "futures": generate_full_bt_l1_report(
                "futures", run_id, db_path=db_path, market_db=market_db
            ),
        },
    }


def _quant_table_md(label: str, report: dict) -> str:
    q = report.get("quantitative") or {}
    c = report.get("clues") or {}
    lines = [
        f"### {label}",
        "",
        "| key | value |",
        "|-----|-------|",
    ]
    for k in QUANT_KEYS:
        lines.append(f"| {k} | {q.get(k)} |")
    lines += ["", "| clue | value |", "|------|-------|"]
    for k in CLUE_KEYS:
        val = c.get(k)
        if k == "gate_bottleneck_by_step":
            lines.append(f"| {k} | {val} · {UNMEASURED_FOOTNOTE} |")
        elif k == "symbol_breakdown":
            lines.append(
                f"| {k} | {val} · top_rejected: {UNMEASURED_FOOTNOTE} |"
            )
        else:
            lines.append(f"| {k} | {val} |")
    warns = report.get("warnings") or []
    if warns:
        lines.append("")
        for w in warns:
            lines.append(f"- {w}")
    lines.append("")
    return "\n".join(lines)


def render_full_bt_l1_report_md(report: dict) -> str:
    """배너 고정 + §2 정량표/개선단서만 — 해석성 자유서술 금지."""
    banner = str(report.get("banner") or L1_BANNER)
    parts = [banner, "", f"run_id: `{report.get('run_id', '')}`", ""]
    markets = report.get("markets")
    if isinstance(markets, dict):
        for label in ("spot", "futures"):
            if label in markets:
                parts.append(_quant_table_md(label.upper(), markets[label]))
    else:
        mt = str(report.get("market_type") or "").upper() or "MARKET"
        parts.append(_quant_table_md(mt, report))
    return "\n".join(parts).rstrip() + "\n"
