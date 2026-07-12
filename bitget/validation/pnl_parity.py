"""
Virtual PnL / forward_trades fingerprint parity vs baseline snapshot.
"""
from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from bitget.infra.bounded_reads import (
    forward_open_count_sql,
    forward_open_pnl_parity_sql,
    warn_if_open_exceeds_safety,
)
from bitget.infra.clock import utc_datetime_str_tz
from bitget.infra.data_paths import market_data_db_path, validation_state_dir
from bitget.infra.memory_policy import FORWARD_OPEN_MAX_SAFETY
from bitget.infra.shared_db_connector import get_connection

PNL_BASELINE_NAME = "pnl_baseline.json"


def _fingerprint_db(db_path: str | None = None) -> dict[str, Any]:
    path = db_path or market_data_db_path()
    if not os.path.isfile(path):
        return {"error": "db_missing", "path": path}
    conn = get_connection(path, read_only=True)
    try:
        warn_if_open_exceeds_safety(conn)
        open_q, open_params = forward_open_pnl_parity_sql()
        open_rows = conn.execute(open_q, open_params).fetchall()
        cnt_q, cnt_params = forward_open_count_sql()
        cnt_row = conn.execute(cnt_q, cnt_params).fetchone()
        open_total = int(cnt_row[0] or 0) if cnt_row else 0
        closed_stats = conn.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(sim_kelly_invest * final_ret / 100.0), 0)
            FROM bitget_forward_trades
            WHERE status LIKE 'CLOSED%'
            """
        ).fetchone()
    finally:
        conn.close()

    canonical = json.dumps(open_rows, ensure_ascii=False, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    return {
        "open_count": open_total,
        "open_fingerprint_rows": len(open_rows),
        "open_fingerprint_truncated": open_total > FORWARD_OPEN_MAX_SAFETY,
        "open_fingerprint": digest,
        "closed_count": int(closed_stats[0] or 0) if closed_stats else 0,
        "closed_pnl_sum": float(closed_stats[1] or 0.0) if closed_stats else 0.0,
        "db_path": path,
    }


def baseline_path() -> str:
    return os.path.join(validation_state_dir(), PNL_BASELINE_NAME)


def save_pnl_baseline(*, db_path: str | None = None) -> dict[str, Any]:
    fp = _fingerprint_db(db_path)
    payload = {
        "recorded_at_utc": utc_datetime_str_tz(),
        **fp,
    }
    os.makedirs(validation_state_dir(), exist_ok=True)
    with open(baseline_path(), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def load_pnl_baseline() -> dict[str, Any] | None:
    path = baseline_path()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def compare_pnl_parity(*, require_zero_open_diff: bool = True) -> dict[str, Any]:
    base = load_pnl_baseline()
    if not base or base.get("error"):
        return {
            "ok": False,
            "passed": False,
            "reason": "no_baseline",
            "message": f"Run record_baseline first ({baseline_path()})",
        }
    cur = _fingerprint_db()
    if cur.get("error"):
        return {"ok": False, "passed": False, "reason": cur["error"], "message": str(cur)}

    open_match = cur["open_fingerprint"] == base.get("open_fingerprint")
    open_count_match = cur["open_count"] == base.get("open_count", -1)
    pnl_drift = abs(cur["closed_pnl_sum"] - float(base.get("closed_pnl_sum", 0.0)))
    passed = open_match and open_count_match
    if require_zero_open_diff and not passed:
        msg = (
            f"OPEN fingerprint mismatch: base={base.get('open_fingerprint')} "
            f"cur={cur['open_fingerprint']} counts {base.get('open_count')}/{cur['open_count']}"
        )
    else:
        msg = "PASS"
    return {
        "ok": True,
        "passed": passed,
        "open_fingerprint_match": open_match,
        "open_count_match": open_count_match,
        "baseline": {
            "open_count": base.get("open_count"),
            "open_fingerprint": base.get("open_fingerprint"),
            "closed_pnl_sum": base.get("closed_pnl_sum"),
            "recorded_at": base.get("recorded_at_utc"),
        },
        "current": cur,
        "closed_pnl_drift_usdt": round(pnl_drift, 4),
        "message": msg,
    }
