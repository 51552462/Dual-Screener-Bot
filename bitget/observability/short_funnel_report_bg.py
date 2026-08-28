"""
SHORT funnel observatory (SHORT-FUNNEL-01 / SHORT-OBS-GATE-01).

Read-only: forward OPEN/CLOSED by side + blocked_trade_history reason buckets.
Does not change Cos/funding thresholds or execution gates.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

from bitget.infra.bounded_reads import shadow_blocked_history_sql
from bitget.infra.clock import utc_datetime_str
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.shared_db_connector import get_connection

# Reason → bucket (first match wins). Labels only — not gate knobs.
_REASON_BUCKETS: Tuple[Tuple[str, str], ...] = (
    ("현물", "spot_short_hard_block"),
    ("Spot", "spot_short_hard_block"),
    ("Cos_eff", "cos_gate"),
    ("시계열 게이트", "cos_gate"),
    ("ANTI_PATTERN", "anti_pattern"),
    ("TOXIC", "anti_pattern"),
    ("DOOMSDAY", "doomsday"),
    ("둠스데이", "doomsday"),
    ("펀딩", "funding_squeeze"),
    ("funding", "funding_squeeze"),
    ("Squeeze", "funding_squeeze"),
    ("쿼터", "quota"),
    ("MAX_OPEN", "quota"),
    ("오픈 포지션", "quota"),
    ("중복", "dup_open"),
    ("테일", "tail_risk"),
    ("집중도", "concentration"),
    ("배드틱", "price_sanity"),
    ("명목노출", "gross_notional"),
    ("서킷", "circuit"),
)


def classify_block_reason(reason: str) -> str:
    r = str(reason or "")
    for needle, bucket in _REASON_BUCKETS:
        if needle in r:
            return bucket
    return "other"


def _side_counts_sql() -> str:
    return """
        SELECT UPPER(COALESCE(position_side, 'LONG')) AS side,
               LOWER(COALESCE(market_type, 'spot')) AS mkt,
               status,
               COUNT(*) AS n
        FROM bitget_forward_trades
        GROUP BY 1, 2, 3
    """


def collect_short_funnel_report(
    *,
    forward_db_path: Optional[str] = None,
    blocked_limit: int = 300,
) -> Dict[str, Any]:
    """Aggregate LONG/SHORT book + SHORT block reason funnel."""
    path = forward_db_path or market_data_db_path()
    open_by_side: Dict[str, int] = {"LONG": 0, "SHORT": 0}
    closed_by_side: Dict[str, int] = {"LONG": 0, "SHORT": 0}
    open_short_by_mkt: Dict[str, int] = {"spot": 0, "futures": 0}
    closed_short_by_mkt: Dict[str, int] = {"spot": 0, "futures": 0}
    block_buckets: Counter = Counter()
    block_samples: List[Dict[str, str]] = []
    err: Optional[str] = None

    try:
        conn = get_connection(path, read_only=True)
        try:
            try:
                rows = conn.execute(_side_counts_sql()).fetchall()
            except Exception as exc:
                err = f"forward_trades:{exc}"
                rows = []
            for side, mkt, status, n in rows:
                side_u = str(side or "LONG").upper()
                if side_u not in ("LONG", "SHORT"):
                    side_u = "LONG"
                mkt_l = str(mkt or "spot").lower()
                if mkt_l not in ("spot", "futures"):
                    mkt_l = "spot"
                n_i = int(n or 0)
                st = str(status or "").upper()
                if st.startswith("OPEN"):
                    open_by_side[side_u] = open_by_side.get(side_u, 0) + n_i
                    if side_u == "SHORT":
                        open_short_by_mkt[mkt_l] = open_short_by_mkt.get(mkt_l, 0) + n_i
                elif st.startswith("CLOSED"):
                    closed_by_side[side_u] = closed_by_side.get(side_u, 0) + n_i
                    if side_u == "SHORT":
                        closed_short_by_mkt[mkt_l] = closed_short_by_mkt.get(mkt_l, 0) + n_i

            try:
                q, params = shadow_blocked_history_sql(limit=blocked_limit)
                blocked = conn.execute(q, params).fetchall()
            except Exception as exc:
                err = (err + ";" if err else "") + f"blocked:{exc}"
                blocked = []
            for row in blocked:
                if len(row) < 5:
                    continue
                side = str(row[4] or "LONG").upper()
                if side != "SHORT":
                    continue
                reason = str(row[3] or "")
                bucket = classify_block_reason(reason)
                block_buckets[bucket] += 1
                if len(block_samples) < 8:
                    block_samples.append(
                        {
                            "market": str(row[1] or ""),
                            "symbol": str(row[2] or ""),
                            "bucket": bucket,
                            "reason": reason[:120],
                        }
                    )
        finally:
            conn.close()
    except Exception as exc:
        err = str(exc)

    short_open = int(open_by_side.get("SHORT", 0))
    long_open = int(open_by_side.get("LONG", 0))
    short_closed = int(closed_by_side.get("SHORT", 0))
    blocked_short_n = int(sum(block_buckets.values()))
    top_bucket = block_buckets.most_common(1)[0][0] if block_buckets else None

    if short_open > 0 or short_closed > 0:
        plain = (
            f"숏 연습 OPEN {short_open} · CLOSED {short_closed} "
            f"(롱 OPEN {long_open})"
        )
        light = "🟢"
        state = "SHORT_ACTIVE"
    elif blocked_short_n > 0:
        plain = (
            f"숏 신호는 있는데 막혔어요 · 차단 {blocked_short_n}건 "
            f"(맨 위={top_bucket or 'other'})"
        )
        light = "🟡"
        state = "SHORT_BLOCKED_ONLY"
    else:
        plain = "숏 연습 자리·차단 기록이 아직 없어요"
        light = "🟡"
        state = "SHORT_EMPTY"

    return {
        "checked_at": utc_datetime_str(),
        "state": state,
        "light": light,
        "plain": plain,
        "open_by_side": open_by_side,
        "closed_by_side": closed_by_side,
        "open_short_by_market": open_short_by_mkt,
        "closed_short_by_market": closed_short_by_mkt,
        "blocked_short_total": blocked_short_n,
        "blocked_short_by_bucket": dict(block_buckets),
        "blocked_short_top_bucket": top_bucket,
        "blocked_short_samples": block_samples,
        "predicted_sector": None,
        "last_error": err,
        "cursor_action": (
            "NONE"
            if state == "SHORT_ACTIVE"
            else ("OBSERVE_HOLD" if state == "SHORT_EMPTY" else "REPORT_TO_CLAUDE")
        ),
    }


def attach_predicted_sector(report: Dict[str, Any], cfg: Optional[dict]) -> Dict[str, Any]:
    out = dict(report or {})
    sector = "UNKNOWN"
    if isinstance(cfg, dict):
        sector = str(cfg.get("PREDICTED_NEXT_SECTOR") or "UNKNOWN")
    out["predicted_sector"] = sector
    return out
