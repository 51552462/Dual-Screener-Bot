"""Streamlit ops gauge panel (Phase 7)."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from bitget.infra import ops_logger
from bitget.infra.clock import parse_utc_iso, utc_now
from bitget.trading.oms_source_stats import pick_latest_oms_heartbeat


def _pct(share: float | None) -> str:
    if share is None:
        return "—"
    return f"{100.0 * float(share):.0f}%"


def _render_oms_book_panel(heartbeats: list[dict]) -> None:
    picked = pick_latest_oms_heartbeat(heartbeats)
    if not picked:
        return

    analysis = picked.get("analysis") or {}
    st.markdown("**OMS book source (latest heartbeat)**")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("private", str(analysis.get("private_status") or analysis.get("status") or "—"))
    c2.metric("private REST", _pct(analysis.get("private_rest_share") or analysis.get("rest_share")))
    c3.metric("public", str(analysis.get("public_status") or "—"))
    c4.metric("public REST", _pct(analysis.get("public_rest_share")))

    if analysis.get("private_status") == "rest_heavy":
        st.warning(
            "Private plane REST-heavy while private WS should be feeding — "
            "check BITGET_DAEMON_PRIVATE_WS, login, and channel freshness."
        )
    if analysis.get("public_status") == "rest_heavy":
        st.warning(
            "Public plane (ticker ref) REST-heavy while public WS should be feeding — "
            "check BITGET_DAEMON_PUBLIC_WS and StreamBuffer universe."
        )

    detail = {
        "component": picked.get("component"),
        "last_utc": str(picked.get("ts_utc") or "")[:19],
        "private_n": int(analysis.get("private_n_total") or analysis.get("n_total") or 0),
        "public_n": int(analysis.get("public_n_total") or 0),
        "pos_rest_share": _pct(analysis.get("pos_rest_share")),
        "oo_rest_share": _pct(analysis.get("oo_rest_share")),
        "fo_rest_share": _pct(analysis.get("fo_rest_share")),
        "bal_rest_share": _pct(analysis.get("bal_rest_share")),
        "mm_rest_share": _pct(analysis.get("mm_rest_share")),
        "tk_rest_share": _pct(analysis.get("tk_rest_share")),
        "warn_at": _pct(analysis.get("warn_threshold")),
        "min_samples": analysis.get("min_samples"),
    }
    priv = picked.get("private_ws") or {}
    pub = picked.get("public_ws") or {}
    if isinstance(priv, dict):
        detail["private_ws_enabled"] = bool(priv.get("enabled"))
        if "age_sec" in priv:
            detail["private_ws_age_sec"] = priv.get("age_sec")
        if "connected" in priv:
            detail["private_ws_connected"] = priv.get("connected")
    if isinstance(pub, dict):
        detail["public_ws_enabled"] = bool(pub.get("enabled"))
        if "age_sec" in pub:
            detail["public_ws_age_sec"] = pub.get("age_sec")

    st.dataframe(pd.DataFrame([detail]), use_container_width=True)


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
            ts = parse_utc_iso(str(hb.get("ts_utc", "")))
            age_min = None
            if ts:
                age_min = (utc_now() - ts).total_seconds() / 60.0
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

        _render_oms_book_panel(heartbeats)

    errs = ops_logger.recent_error_summaries_for_console(hours=1.0, limit=5)
    if errs:
        st.markdown("**Recent warnings/errors (1h)**")
        for line in errs:
            st.text(line)
