"""Streamlit ops gauge panel (Phase 7)."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from bitget.infra import ops_logger


def _parse_ts(ts: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None


def render_ops_gauge_panel(*, hours: float = 6.0) -> None:
    st.subheader("Ops Gauges (bitget_ops_events)")
    rows = ops_logger.fetch_recent_rows(hours=hours, limit=800)
    gauges = [r for r in rows if r.get("event") == "gauge.snapshot"]
    heartbeats = ops_logger.fetch_heartbeat_ticks(hours=min(hours, 2.0), limit=500)

    if not gauges and not heartbeats:
        st.caption("No ops_events yet — start factory / WS / cron jobs.")
        return

    if gauges:
        latest_by_comp: dict[str, dict] = {}
        for g in gauges:
            comp = str(g.get("component") or "unknown")
            if comp not in latest_by_comp:
                latest_by_comp[comp] = g
        st.caption(f"Latest gauge.snapshot per component ({len(latest_by_comp)} components)")
        for comp, row in sorted(latest_by_comp.items()):
            payload = row.get("payload") or {}
            ts = str(row.get("ts_utc", ""))[:19]
            with st.expander(f"{comp} @ {ts}", expanded=comp.startswith("bitget.")):
                st.json(payload)

    if heartbeats:
        hb_rows = []
        seen: set[str] = set()
        for hb in heartbeats:
            comp = str(hb.get("component") or "")
            if comp in seen:
                continue
            seen.add(comp)
            ts = _parse_ts(str(hb.get("ts_utc", "")))
            age_min = None
            if ts:
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age_min = (datetime.now(timezone.utc) - ts).total_seconds() / 60.0
            hb_rows.append(
                {
                    "component": comp,
                    "last_utc": str(hb.get("ts_utc", ""))[:19],
                    "age_min": round(age_min, 1) if age_min is not None else None,
                }
            )
        if hb_rows:
            st.markdown("**Heartbeats (latest per component)**")
            st.dataframe(pd.DataFrame(hb_rows), use_container_width=True)

    errs = ops_logger.recent_error_summaries_for_console(hours=1.0, limit=5)
    if errs:
        st.markdown("**Recent warnings/errors (1h)**")
        for line in errs:
            st.text(line)
