"""
A-EFFECTVERIFY-01 — A-1~A-5 effect-verify snapshot (read-only).

Does not write ops_events, config_kv, or gate thresholds.
A-3 counts FUT rows only via ``normalize_market_key`` (no hardcoded market_type).
"""
from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

_A1_EVENTS = ("portfolio_mdd_tier_change", "portfolio_mdd_tier", "mdd_tier_transition")
_A2_EVENTS = ("tail_fund_debit", "tail_fund_consumption")
_A3_EVENTS = ("max_leverage_clamp", "leverage_clamp")
_A4_EVENTS = ("gross_notional_block", "gross_notional_cap")
_A5_EVENTS = ("config_write_rejected", "config_write_reject")

_NO_SOURCE = "로그 소스 없음"


@dataclass
class SubMetric:
    sub: str
    value: Any
    source: Optional[str]
    source_available: bool
    note: str
    extras: Dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> Dict[str, Any]:
        return {
            "sub": self.sub,
            "value": self.value,
            "source": self.source,
            "source_available": self.source_available,
            "note": self.note,
            **self.extras,
        }


@dataclass
class A1A5EffectSnapshot:
    window_start: str
    window_end: str
    enabled: bool
    a1: SubMetric
    a2: SubMetric
    a3: SubMetric
    a4: SubMetric
    a5: SubMetric
    current_config: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        return {
            "window_start": self.window_start,
            "window_end": self.window_end,
            "enabled": self.enabled,
            "current_config": self.current_config,
            "subs": [self.a1.to_row(), self.a2.to_row(), self.a3.to_row(), self.a4.to_row(), self.a5.to_row()],
        }


def a1a5_effect_verify_enabled(config: Optional[dict] = None) -> bool:
    if config and "A1A5_EFFECT_VERIFY_ENABLED" in config:
        raw = config.get("A1A5_EFFECT_VERIFY_ENABLED")
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    env = os.environ.get("A1A5_EFFECT_VERIFY_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("A1A5_EFFECT_VERIFY_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    from bitget.infra.memory_policy import A1A5_EFFECT_VERIFY_ENABLED

    return bool(A1A5_EFFECT_VERIFY_ENABLED)


def _empty_disabled(window_start: date, window_end: date) -> A1A5EffectSnapshot:
    note = "A1A5_EFFECT_VERIFY_ENABLED=false"
    empty = lambda sub: SubMetric(sub, None, None, False, note)
    return A1A5EffectSnapshot(
        window_start=str(window_start),
        window_end=str(window_end),
        enabled=False,
        a1=empty("A-1"),
        a2=empty("A-2"),
        a3=empty("A-3"),
        a4=empty("A-4"),
        a5=empty("A-5"),
    )


def _connect_ro(path: str) -> Optional[sqlite3.Connection]:
    if not path or not os.path.isfile(path):
        return None
    try:
        from bitget.infra.shared_db_connector import get_connection

        return get_connection(path, read_only=True, check_same_thread=False)
    except Exception:
        try:
            return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        except Exception:
            return None


def _events_ever_present(conn: sqlite3.Connection, names: Sequence[str]) -> bool:
    q = ",".join("?" * len(names))
    try:
        row = conn.execute(
            f"SELECT 1 FROM ops_events WHERE event IN ({q}) LIMIT 1",
            tuple(names),
        ).fetchone()
        return bool(row)
    except sqlite3.Error:
        return False


def _fetch_window_rows(
    conn: sqlite3.Connection,
    names: Sequence[str],
    start_iso: str,
    end_iso: str,
) -> List[Dict[str, Any]]:
    q = ",".join("?" * len(names))
    try:
        rows = conn.execute(
            f"""
            SELECT ts_utc, event, payload_json
            FROM ops_events
            WHERE event IN ({q}) AND ts_utc >= ? AND ts_utc < ?
            ORDER BY ts_utc
            """,
            tuple(names) + (start_iso, end_iso),
        ).fetchall()
    except sqlite3.Error:
        return []
    out: List[Dict[str, Any]] = []
    for ts, event, payload in rows:
        try:
            body = json.loads(payload) if payload else {}
        except json.JSONDecodeError:
            body = {}
        if not isinstance(body, dict):
            body = {}
        out.append({"ts_utc": ts, "event": event, "payload": body})
    return out


def _payload_market_raw(payload: dict) -> Optional[str]:
    for k in ("market", "market_type", "market_key", "mt"):
        v = payload.get(k)
        if v is not None and str(v).strip():
            return str(v)
    return None


def _is_fut_payload(payload: dict) -> bool:
    raw = _payload_market_raw(payload)
    if raw is None:
        return False
    from bitget.evolution.market_key_normalize import normalize_market_key

    return normalize_market_key(raw) == "FUT"


def _window_iso(window_start: date, window_end: date) -> tuple[str, str]:
    start = datetime(window_start.year, window_start.month, window_start.day, tzinfo=timezone.utc)
    end = datetime(window_end.year, window_end.month, window_end.day, tzinfo=timezone.utc)
    # inclusive end-date: next midnight exclusive
    from datetime import timedelta

    end_excl = end + timedelta(days=1)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ"), end_excl.strftime("%Y-%m-%dT%H:%M:%SZ")


def _current_config_snapshot(config: dict) -> Dict[str, Any]:
    cfg = dict(config or {})
    try:
        from bitget.infra import config_manager as cm

        for key in (
            "PORTFOLIO_MDD_CURRENT_TIER",
            "PORTFOLIO_NAV_PEAK",
            "TAIL_FUND_CONSUMPTION_ENABLED",
            "CONFIG_WRITE_VALIDATION_ENABLED",
        ):
            if key not in cfg:
                cfg[key] = cm.get_config_value(key, None)
    except Exception:
        pass
    return {
        "PORTFOLIO_MDD_CURRENT_TIER": cfg.get("PORTFOLIO_MDD_CURRENT_TIER"),
        "PORTFOLIO_NAV_PEAK": cfg.get("PORTFOLIO_NAV_PEAK"),
        "TAIL_FUND_CONSUMPTION_ENABLED": cfg.get("TAIL_FUND_CONSUMPTION_ENABLED"),
        "CONFIG_WRITE_VALIDATION_ENABLED": cfg.get("CONFIG_WRITE_VALIDATION_ENABLED"),
    }


def _metric_from_ops(
    *,
    sub: str,
    conn: Optional[sqlite3.Connection],
    names: Sequence[str],
    start_iso: str,
    end_iso: str,
    fut_only: bool,
    value_fn,
    source_label: str,
) -> SubMetric:
    if conn is None:
        return SubMetric(sub, None, None, False, _NO_SOURCE + " (ops_events DB 없음)")
    if not _events_ever_present(conn, names):
        return SubMetric(sub, None, None, False, _NO_SOURCE)
    rows = _fetch_window_rows(conn, names, start_iso, end_iso)
    if fut_only:
        rows = [r for r in rows if _is_fut_payload(r["payload"])]
    return value_fn(rows, source_label)


def collect_a1_a5_effect_snapshot(
    window_start: date,
    window_end: date,
    config: Optional[dict] = None,
) -> A1A5EffectSnapshot:
    cfg = dict(config or {})
    if not a1a5_effect_verify_enabled(cfg):
        return _empty_disabled(window_start, window_end)

    start_iso, end_iso = _window_iso(window_start, window_end)
    ops_path = str(cfg.get("ops_db_path") or "").strip()
    if not ops_path:
        from bitget.infra.data_paths import ops_events_db_path

        ops_path = ops_events_db_path()
    conn = _connect_ro(ops_path)

    def a1_fn(rows: List[dict], src: str) -> SubMetric:
        transitions = []
        max_dd = None
        for r in rows:
            p = r["payload"]
            dd = p.get("dd_pct")
            nav = p.get("nav") or p.get("nav_current")
            if dd is not None:
                try:
                    ddf = float(dd)
                    if ddf > 1.0:
                        ddf = ddf / 100.0
                    max_dd = ddf if max_dd is None else max(max_dd, ddf)
                except (TypeError, ValueError):
                    pass
            transitions.append(
                {
                    "ts_utc": r["ts_utc"],
                    "from": p.get("from_tier") or p.get("prev_tier"),
                    "to": p.get("to_tier") or p.get("tier"),
                    "nav": nav,
                    "dd_pct": dd,
                }
            )
        return SubMetric(
            "A-1",
            {"transition_count": len(rows), "max_nav_mdd_pct": None if max_dd is None else round(max_dd * 100.0, 4)},
            src,
            True,
            "ops_events 창 내 전이" if rows else "창 내 전이 0건",
            extras={"transitions": transitions[:50]},
        )

    def a2_fn(rows: List[dict], src: str) -> SubMetric:
        total = 0.0
        n = 0
        for r in rows:
            amt = r["payload"].get("amount") or r["payload"].get("debit") or r["payload"].get("debit_usdt")
            if amt is None:
                continue
            try:
                total += float(amt)
                n += 1
            except (TypeError, ValueError):
                continue
        return SubMetric(
            "A-2",
            {"event_count": len(rows), "debit_sum": round(total, 4) if n else (0.0 if rows else 0.0)},
            src,
            True,
            "창 내 debit 0건" if not rows else f"debit 이벤트 {len(rows)}건",
        )

    def a3_fn(rows: List[dict], src: str) -> SubMetric:
        return SubMetric(
            "A-3",
            {"clamp_count": len(rows)},
            src,
            True,
            "FUT only via normalize_market_key; 창 내 0건" if not rows else f"FUT clamp {len(rows)}건",
        )

    def a4_fn(rows: List[dict], src: str) -> SubMetric:
        samples = []
        for r in rows[:20]:
            p = r["payload"]
            samples.append(
                {
                    "ts_utc": r["ts_utc"],
                    "gross": p.get("gross") or p.get("gross_notional"),
                    "nav": p.get("nav") or p.get("nav_current"),
                }
            )
        return SubMetric(
            "A-4",
            {"block_count": len(rows)},
            src,
            True,
            "창 내 block 0건" if not rows else f"block {len(rows)}건",
            extras={"samples": samples},
        )

    def a5_fn(rows: List[dict], src: str) -> SubMetric:
        keys: List[str] = []
        for r in rows:
            k = r["payload"].get("key") or r["payload"].get("rejected_key")
            if k is not None:
                keys.append(str(k))
        uniq = sorted(set(keys))
        return SubMetric(
            "A-5",
            {"reject_count": len(rows), "rejected_keys": uniq},
            src,
            True,
            "창 내 reject 0건" if not rows else f"reject {len(rows)}건",
        )

    a1 = _metric_from_ops(
        sub="A-1", conn=conn, names=_A1_EVENTS, start_iso=start_iso, end_iso=end_iso, fut_only=False,
        value_fn=a1_fn, source_label="ops_events",
    )
    a2 = _metric_from_ops(
        sub="A-2", conn=conn, names=_A2_EVENTS, start_iso=start_iso, end_iso=end_iso, fut_only=False,
        value_fn=a2_fn, source_label="ops_events",
    )
    a3 = _metric_from_ops(
        sub="A-3", conn=conn, names=_A3_EVENTS, start_iso=start_iso, end_iso=end_iso, fut_only=True,
        value_fn=a3_fn, source_label="ops_events",
    )
    a4 = _metric_from_ops(
        sub="A-4", conn=conn, names=_A4_EVENTS, start_iso=start_iso, end_iso=end_iso, fut_only=False,
        value_fn=a4_fn, source_label="ops_events",
    )
    a5 = _metric_from_ops(
        sub="A-5", conn=conn, names=_A5_EVENTS, start_iso=start_iso, end_iso=end_iso, fut_only=False,
        value_fn=a5_fn, source_label="ops_events",
    )

    a1_db_missing = conn is None
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

    if not a1.source_available and not a1_db_missing:
        a1.note = (
            _NO_SOURCE
            + " — config_kv는 현재값만(상태이력 테이블 없음). "
            + "A-1 persist는 set_config_value overwrite, ops_events 미기록."
        )

    snap = A1A5EffectSnapshot(
        window_start=str(window_start),
        window_end=str(window_end),
        enabled=True,
        a1=a1,
        a2=a2,
        a3=a3,
        a4=a4,
        a5=a5,
        current_config=_current_config_snapshot(cfg),
    )
    return snap


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="A-1~A-5 effect verify (read-only)")
    p.add_argument("--start", default="2026-08-01")
    p.add_argument("--end", default="")
    args = p.parse_args(argv)
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if str(args.end).strip() else datetime.now(timezone.utc).date()
    snap = collect_a1_a5_effect_snapshot(start, end, {})
    print(json.dumps(snap.to_payload(), ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
