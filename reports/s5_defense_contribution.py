"""S5 페이퍼 게이트 — read-only 기여 로그 (S5-HARNESS-SCOPE-01).

신규 테이블/컬럼/config_kv/주문 경로 없음. 기존 원장·게이트 함수만 소비.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional

from meta_governor_consumer import is_s5_sig_type, resolve_defense_arm_weight
from meta_state_store import normalize_regime_key

S5_GATE_REGIMES = frozenset({"BEAR", "HIGH_VOL"})
SAMPLE_MIN_N = 20
_PROBE_SIG = "Dante[INVERSE_ETF]"
_CLOSED_PREFIX = "CLOSED"

_FORBIDDEN_VERDICT_TOKENS = (
    "pass",
    "fail",
    "cagr",
    "period_return_pct",
    "near_miss",
)


def _as_utc_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    s = str(value or "").strip()
    if not s:
        raise ValueError("empty timestamp")
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _normalize_market(market: Optional[str]) -> Optional[str]:
    if market is None:
        return None
    m = str(market).strip().upper()
    if not m:
        return None
    if m not in ("KR", "US"):
        raise ValueError(f"market must be KR, US, or None — got {market!r}")
    return m


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(r[1]) for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()}


def _s5_gate_active(market: str, regime: str, sys_config: Optional[dict[str, Any]]) -> bool:
    rk = normalize_regime_key(regime)
    if rk not in S5_GATE_REGIMES:
        return False
    weight = resolve_defense_arm_weight(market, rk, _PROBE_SIG, sys_config)
    return float(weight) > 0.0


def _regime_from_state_json(state_json: Any, market: str) -> str:
    if not isinstance(state_json, str) or not state_json.strip():
        return "UNKNOWN"
    try:
        payload = json.loads(state_json)
    except json.JSONDecodeError:
        return "UNKNOWN"
    if not isinstance(payload, dict):
        return "UNKNOWN"
    ens = payload.get("REGIME_ENSEMBLE")
    if isinstance(ens, dict):
        markets = ens.get("markets")
        if isinstance(markets, dict):
            blk = markets.get(market)
            if isinstance(blk, dict):
                rk = normalize_regime_key(blk.get("regime"))
                if rk not in ("", "UNKNOWN"):
                    return rk
    for key in (f"{market}_REGIME_KEY", "META_REGIME_KEY"):
        rk = normalize_regime_key(payload.get(key))
        if rk not in ("", "UNKNOWN"):
            return rk
    return "UNKNOWN"


def _load_regime_timeline(
    conn: sqlite3.Connection,
    market: str,
    start: datetime,
    end: datetime,
) -> list[tuple[datetime, str]]:
    if not _table_exists(conn, "meta_state_log"):
        return []
    cols = _columns(conn, "meta_state_log")
    select_cols = ["updated_at_utc"]
    if "regime_key" in cols:
        select_cols.append("regime_key")
    if "state_json" in cols:
        select_cols.append("state_json")
    q = (
        "SELECT "
        + ", ".join(select_cols)
        + " FROM meta_state_log WHERE updated_at_utc <= ? ORDER BY updated_at_utc ASC, id ASC"
    )
    rows = conn.execute(q, (_iso(end),)).fetchall()
    timeline: list[tuple[datetime, str]] = []
    for row in rows:
        try:
            ts = _as_utc_dt(row[0])
        except (TypeError, ValueError):
            continue
        rk = "UNKNOWN"
        if "regime_key" in cols:
            rk = normalize_regime_key(row[1] if len(row) > 1 else "")
        if (rk in ("", "UNKNOWN")) and "state_json" in cols:
            sj = row[-1]
            rk = _regime_from_state_json(sj, market)
        timeline.append((ts, rk or "UNKNOWN"))
    return [(ts, rk) for ts, rk in timeline if ts <= end]


def _gate_active_minutes(
    timeline: list[tuple[datetime, str]],
    market: str,
    start: datetime,
    end: datetime,
    sys_config: Optional[dict[str, Any]],
) -> tuple[float, str]:
    if end <= start:
        return 0.0, "empty_window"
    if not timeline:
        return 0.0, "none"
    points = [(ts, rk) for ts, rk in timeline if ts <= end]
    if not points:
        return 0.0, "none"
    last_before = None
    for ts, rk in points:
        if ts <= start:
            last_before = (ts, rk)
        else:
            break
    active = [p for p in points if start < p[0] <= end]
    if last_before is not None:
        seq = [(start, last_before[1])] + active
    elif active:
        seq = [(start, "UNKNOWN")] + active
    else:
        return 0.0, "none"
    seq.append((end, seq[-1][1]))
    minutes = 0.0
    for i in range(len(seq) - 1):
        ts_a, rk = seq[i]
        ts_b = seq[i + 1][0]
        if ts_b <= ts_a:
            continue
        if _s5_gate_active(market, rk, sys_config):
            minutes += (ts_b - ts_a).total_seconds() / 60.0
    return round(minutes, 4), "meta_state_log"


def _asof_regime(timeline: list[tuple[datetime, str]], when: datetime) -> str:
    rk = "UNKNOWN"
    for ts, key in timeline:
        if ts <= when:
            rk = key
        else:
            break
    return rk


def _in_window(raw: Any, start: datetime, end: datetime) -> bool:
    s = str(raw or "").strip()
    if not s:
        return False
    try:
        ts = _as_utc_dt(s[:19] if len(s) >= 19 else s)
    except ValueError:
        try:
            ts = _as_utc_dt(s[:10])
        except ValueError:
            return False
    return start <= ts <= end


def _load_forward_s5_rows(
    conn: sqlite3.Connection,
    market: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    if not _table_exists(conn, "forward_trades"):
        return []
    cols = _columns(conn, "forward_trades")
    needed = {"market", "sig_type"}
    if not needed.issubset(cols):
        return []
    select = [
        "market",
        "sig_type",
        "entry_date" if "entry_date" in cols else "NULL AS entry_date",
        "exit_date" if "exit_date" in cols else "NULL AS exit_date",
        "status" if "status" in cols else "NULL AS status",
        "entry_regime" if "entry_regime" in cols else "NULL AS entry_regime",
        "final_ret" if "final_ret" in cols else "NULL AS final_ret",
        "code" if "code" in cols else "NULL AS code",
    ]
    q = f"SELECT {', '.join(select)} FROM forward_trades WHERE UPPER(IFNULL(market,'')) = ?"
    out: list[dict[str, Any]] = []
    for row in conn.execute(q, (market,)):
        mapping = dict(row)
        if not is_s5_sig_type(mapping.get("sig_type")):
            continue
        if not (
            _in_window(mapping.get("entry_date"), start, end)
            or _in_window(mapping.get("exit_date"), start, end)
        ):
            continue
        out.append(mapping)
    return out


def _load_short_s5_rows(
    conn: sqlite3.Connection,
    market: str,
    start: datetime,
    end: datetime,
) -> tuple[list[dict[str, Any]], bool]:
    """Adapter: short_forward_trades 는 후보 원장. PnL 컬럼 없으면 0."""
    if not _table_exists(conn, "short_forward_trades"):
        return [], False
    cols = _columns(conn, "short_forward_trades")
    if "entry_date" not in cols:
        return [], False
    has_pnl = "final_ret" in cols
    select = [
        "entry_date",
        "market" if "market" in cols else f"'{market}' AS market",
        "status" if "status" in cols else "NULL AS status",
        "final_ret" if has_pnl else "NULL AS final_ret",
        "code" if "code" in cols else "NULL AS code",
        "matched_pattern" if "matched_pattern" in cols else "NULL AS matched_pattern",
    ]
    q = f"SELECT {', '.join(select)} FROM short_forward_trades"
    params: tuple[Any, ...] = ()
    if "market" in cols:
        q += " WHERE UPPER(IFNULL(market,'')) = ?"
        params = (market,)
    out: list[dict[str, Any]] = []
    for row in conn.execute(q, params):
        mapping = dict(row)
        mkt = str(mapping.get("market") or market).strip().upper()
        if mkt != market:
            continue
        if not _in_window(mapping.get("entry_date"), start, end):
            continue
        mapping["sig_type"] = str(mapping.get("matched_pattern") or "BLACKHOLE")
        if not is_s5_sig_type(mapping["sig_type"]):
            mapping["sig_type"] = "BLACKHOLE"
        mapping["entry_regime"] = None
        mapping["exit_date"] = mapping.get("entry_date")
        out.append(mapping)
    return out, has_pnl


def _trade_regime(row: dict[str, Any], timeline: list[tuple[datetime, str]]) -> str:
    raw = row.get("entry_regime")
    rk = normalize_regime_key(raw)
    if rk not in ("", "UNKNOWN"):
        return rk
    try:
        when = _as_utc_dt(str(row.get("entry_date") or "")[:19] or str(row.get("entry_date") or "")[:10])
    except ValueError:
        return "UNKNOWN"
    return _asof_regime(timeline, when)


def _realized_pnl(row: dict[str, Any]) -> float:
    status = str(row.get("status") or "").upper()
    if status and not status.startswith(_CLOSED_PREFIX):
        return 0.0
    return _row_float_dict(row, "final_ret")


def _row_float_dict(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _build_window(
    market: str,
    start: datetime,
    end: datetime,
    *,
    forward_rows: Iterable[dict[str, Any]],
    short_rows: Iterable[dict[str, Any]],
    timeline: list[tuple[datetime, str]],
    sys_config: Optional[dict[str, Any]],
    short_pnl_present: bool,
) -> dict[str, Any]:
    minutes, src = _gate_active_minutes(timeline, market, start, end, sys_config)
    seen: set[tuple[Any, ...]] = set()
    kept: list[dict[str, Any]] = []
    for row in list(forward_rows) + list(short_rows):
        key = (
            str(row.get("market") or market),
            str(row.get("code") or ""),
            str(row.get("entry_date") or ""),
            str(row.get("sig_type") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        rk = _trade_regime(row, timeline)
        if not _s5_gate_active(market, rk, sys_config):
            continue
        kept.append(row)
    n = len(kept)
    pnl = round(sum(_realized_pnl(r) for r in kept), 6)
    sample_insufficient = n < SAMPLE_MIN_N
    notes: list[str] = []
    if sample_insufficient:
        notes.append("표본 부족")
    if not short_pnl_present:
        notes.append("short_forward_trades PnL 컬럼 없음 - 실현손익은 forward_trades.final_ret만")
    window: dict[str, Any] = {
        "market": market,
        "start_ts": _iso(start),
        "end_ts": _iso(end),
        "gate_active_minutes": minutes,
        "gate_minutes_source": src,
        "s5_trade_count": n,
        "realized_pnl_sum": pnl,
        "pnl_unit": "final_ret_pct",
        "short_pnl_column_present": short_pnl_present,
        "contributed": n > 0,
        "sample_insufficient": sample_insufficient,
        "notes": notes,
    }
    return window


def _connect_ro(path: str) -> sqlite3.Connection:
    if os.path.isfile(path):
        conn = sqlite3.connect(path, timeout=30)
    else:
        conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def compute_s5_defense_contribution_log(
    start_ts: Any,
    end_ts: Any,
    *,
    market: str | None = None,
    forward_db_path: Optional[str] = None,
    short_db_path: Optional[str] = None,
    sys_config: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """BEAR/HIGH_VOL ∩ s5_arm_active 구간의 S5 기여 로그 (read-only)."""
    start = _as_utc_dt(start_ts)
    end = _as_utc_dt(end_ts)
    if end < start:
        raise ValueError("end_ts must be >= start_ts")
    if isinstance(end_ts, date) and not isinstance(end_ts, datetime):
        end = end.replace(hour=23, minute=59, second=59, microsecond=0)
    elif isinstance(end_ts, str) and len(end_ts.strip()) == 10:
        end = end.replace(hour=23, minute=59, second=59, microsecond=0)

    mkt = _normalize_market(market)
    markets = [mkt] if mkt else ["KR", "US"]

    if forward_db_path is None:
        from market_db_paths import report_db_read_path

        forward_db_path = report_db_read_path()
    if short_db_path is None:
        from factory_data_paths import short_data_db_path

        short_db_path = short_data_db_path()

    cfg = sys_config if isinstance(sys_config, dict) else {}
    windows: list[dict[str, Any]] = []
    fwd_conn = _connect_ro(forward_db_path)
    short_exists = os.path.isfile(short_db_path)
    short_conn = _connect_ro(short_db_path) if short_exists else None
    try:
        for mk in markets:
            timeline = _load_regime_timeline(fwd_conn, mk, start, end)
            fwd_rows = _load_forward_s5_rows(fwd_conn, mk, start, end)
            if short_conn is not None:
                short_rows, short_pnl = _load_short_s5_rows(short_conn, mk, start, end)
            else:
                short_rows, short_pnl = [], False
            windows.append(
                _build_window(
                    mk,
                    start,
                    end,
                    forward_rows=fwd_rows,
                    short_rows=short_rows,
                    timeline=timeline,
                    sys_config=cfg,
                    short_pnl_present=short_pnl,
                )
            )
    finally:
        fwd_conn.close()
        if short_conn is not None:
            short_conn.close()

    payload = {
        "sub_phase": "S5-HARNESS-SCOPE-01",
        "kind": "s5_defense_contribution_log",
        "read_only": True,
        "start_ts": _iso(start),
        "end_ts": _iso(end),
        "markets": markets,
        "gate_regimes": sorted(S5_GATE_REGIMES),
        "sample_min_n": SAMPLE_MIN_N,
        "windows": windows,
        "numeric_judgment_omitted": True,
    }
    _assert_no_verdict_language(payload)
    return payload


def _assert_no_verdict_language(payload: dict[str, Any]) -> None:
    if "verdict" in payload:
        raise ValueError("verdict key forbidden")
    blob = json.dumps(payload, ensure_ascii=False).lower()
    for tok in _FORBIDDEN_VERDICT_TOKENS:
        if tok in blob:
            raise ValueError(f"verdict language forbidden in contribution log: {tok}")


def write_s5_contribution_json(
    payload: dict[str, Any],
    *,
    as_of: Optional[str] = None,
    out_dir: Optional[str] = None,
) -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dest_dir = out_dir or os.path.join(root, "reports", "s5_defense")
    os.makedirs(dest_dir, exist_ok=True)
    day = as_of or datetime.now(timezone.utc).strftime("%Y%m%d")
    path = os.path.join(dest_dir, f"s5_contribution_{day}.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
    return path
