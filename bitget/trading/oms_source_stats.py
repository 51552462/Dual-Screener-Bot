"""
OMS book source telemetry SSOT — private_ws / public_ws vs REST hit counts.

Institutional rules:
  - Flat int counters only (ops heartbeat 32KB budget)
  - Thread-safe; never raise into trading hot path
  - Dual-plane health:
      private plane = pos + oo + fo + bal + mm  (private WS consumers)
      public plane  = tk                         (public StreamBuffer ref price)
  - Private REST-heavy alert MUST ignore tk (public-off must not contaminate)
  - Public REST-heavy alert only when public WS is enabled
"""
from __future__ import annotations

import threading
import time
from typing import Any, Optional

from bitget.infra.memory_policy import (
    OMS_REST_SHARE_ALERT_MIN_INTERVAL_SEC,
    OMS_REST_SHARE_MIN_SAMPLES,
    OMS_REST_SHARE_WARN,
)

_KINDS = (
    "position_index",
    "open_orders",
    "fetch_order",
    "fetch_balance",
    "margin_mode",
    "fetch_ticker",
)
_SOURCES = ("private_ws", "public_ws", "rest")

_OMS_PRIVATE_REST_ALERT_MONO: float = 0.0
_OMS_PUBLIC_REST_ALERT_MONO: float = 0.0
# Back-compat alias for tests that reset the private throttle
_OMS_REST_ALERT_MONO = 0.0


def _key(kind: str, source: str) -> str:
    return f"{kind}.{source}"


def _nonneg_int(d: dict[str, Any], key: str) -> int:
    try:
        return max(0, int(d.get(key) or 0))
    except (TypeError, ValueError):
        return 0


def _rest_share(rest: int, ws: int) -> Optional[float]:
    total = int(rest) + int(ws)
    if total <= 0:
        return None
    return float(rest) / float(total)


def _plane_status(n_total: int, rest_share: Optional[float]) -> str:
    if n_total <= 0:
        return "no_data"
    if rest_share is not None and rest_share >= float(OMS_REST_SHARE_WARN):
        return "rest_heavy"
    return "ok"


class OmsSourceCounters:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._lifetime: dict[str, int] = {_key(k, s): 0 for k in _KINDS for s in _SOURCES}
        self._window: Optional[dict[str, int]] = None

    def record(self, kind: str, source: str) -> None:
        k = str(kind or "").strip()
        s = str(source or "").strip()
        if k not in _KINDS or s not in _SOURCES:
            return
        key = _key(k, s)
        try:
            with self._lock:
                self._lifetime[key] = int(self._lifetime.get(key, 0)) + 1
                if self._window is not None:
                    self._window[key] = int(self._window.get(key, 0)) + 1
        except Exception:
            return

    def begin_window(self) -> None:
        with self._lock:
            self._window = {_key(k, s): 0 for k in _KINDS for s in _SOURCES}

    def end_window(self) -> dict[str, int]:
        with self._lock:
            out = dict(self._window or {})
            self._window = None
        return {k: int(v) for k, v in out.items()}

    def lifetime_snapshot(self) -> dict[str, int]:
        with self._lock:
            return {k: int(v) for k, v in self._lifetime.items()}

    def heartbeat_snapshot(self) -> dict[str, Any]:
        """Flat scalars for ops heartbeat (short keys)."""
        life = self.lifetime_snapshot()
        return {
            "pos_ws": life.get("position_index.private_ws", 0),
            "pos_rest": life.get("position_index.rest", 0),
            "oo_ws": life.get("open_orders.private_ws", 0),
            "oo_rest": life.get("open_orders.rest", 0),
            "fo_ws": life.get("fetch_order.private_ws", 0),
            "fo_rest": life.get("fetch_order.rest", 0),
            "bal_ws": life.get("fetch_balance.private_ws", 0),
            "bal_rest": life.get("fetch_balance.rest", 0),
            "mm_ws": life.get("margin_mode.private_ws", 0),
            "mm_rest": life.get("margin_mode.rest", 0),
            "tk_ws": life.get("fetch_ticker.public_ws", 0),
            "tk_rest": life.get("fetch_ticker.rest", 0),
        }


_COUNTERS = OmsSourceCounters()


def get_oms_source_counters() -> OmsSourceCounters:
    return _COUNTERS


def record_oms_source(kind: str, source: str) -> None:
    get_oms_source_counters().record(kind, source)


def oms_source_heartbeat_snapshot() -> dict[str, Any]:
    try:
        return get_oms_source_counters().heartbeat_snapshot()
    except Exception as e:
        return {"error": str(e)[:80]}


def _empty_analysis(*, status: str = "no_data", error: Optional[str] = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        # status / rest_share / n_* == private plane (alert + smoke SSOT)
        "status": status,
        "private_status": status,
        "public_status": "no_data",
        "n_total": 0,
        "n_ws": 0,
        "n_rest": 0,
        "rest_share": None,
        "private_n_total": 0,
        "private_n_ws": 0,
        "private_n_rest": 0,
        "private_rest_share": None,
        "public_n_total": 0,
        "public_n_ws": 0,
        "public_n_rest": 0,
        "public_rest_share": None,
        "combined_n_total": 0,
        "combined_rest_share": None,
        "pos_rest_share": None,
        "oo_rest_share": None,
        "fo_rest_share": None,
        "bal_rest_share": None,
        "mm_rest_share": None,
        "tk_rest_share": None,
        "warn_threshold": float(OMS_REST_SHARE_WARN),
        "min_samples": int(OMS_REST_SHARE_MIN_SAMPLES),
    }
    if error:
        out["error"] = str(error)[:80]
    return out


def analyze_oms_book(oms_book: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Pure dual-plane health view of flat oms_book heartbeat scalars."""
    if not isinstance(oms_book, dict):
        return _empty_analysis(status="no_data")
    if oms_book.get("error"):
        return _empty_analysis(status="error", error=str(oms_book.get("error")))

    pos_ws = _nonneg_int(oms_book, "pos_ws")
    pos_rest = _nonneg_int(oms_book, "pos_rest")
    oo_ws = _nonneg_int(oms_book, "oo_ws")
    oo_rest = _nonneg_int(oms_book, "oo_rest")
    fo_ws = _nonneg_int(oms_book, "fo_ws")
    fo_rest = _nonneg_int(oms_book, "fo_rest")
    bal_ws = _nonneg_int(oms_book, "bal_ws")
    bal_rest = _nonneg_int(oms_book, "bal_rest")
    mm_ws = _nonneg_int(oms_book, "mm_ws")
    mm_rest = _nonneg_int(oms_book, "mm_rest")
    tk_ws = _nonneg_int(oms_book, "tk_ws")
    tk_rest = _nonneg_int(oms_book, "tk_rest")

    priv_ws = pos_ws + oo_ws + fo_ws + bal_ws + mm_ws
    priv_rest = pos_rest + oo_rest + fo_rest + bal_rest + mm_rest
    priv_total = priv_ws + priv_rest
    priv_share = _rest_share(priv_rest, priv_ws)
    priv_status = _plane_status(priv_total, priv_share)

    pub_ws = tk_ws
    pub_rest = tk_rest
    pub_total = pub_ws + pub_rest
    pub_share = _rest_share(pub_rest, pub_ws)
    pub_status = _plane_status(pub_total, pub_share)

    comb_ws = priv_ws + pub_ws
    comb_rest = priv_rest + pub_rest
    comb_total = comb_ws + comb_rest
    comb_share = _rest_share(comb_rest, comb_ws)

    def _round(v: Optional[float]) -> Optional[float]:
        return None if v is None else round(float(v), 4)

    return {
        # Alert / smoke SSOT = private plane
        "status": priv_status,
        "private_status": priv_status,
        "public_status": pub_status,
        "n_total": priv_total,
        "n_ws": priv_ws,
        "n_rest": priv_rest,
        "rest_share": _round(priv_share),
        "private_n_total": priv_total,
        "private_n_ws": priv_ws,
        "private_n_rest": priv_rest,
        "private_rest_share": _round(priv_share),
        "public_n_total": pub_total,
        "public_n_ws": pub_ws,
        "public_n_rest": pub_rest,
        "public_rest_share": _round(pub_share),
        "combined_n_total": comb_total,
        "combined_rest_share": _round(comb_share),
        "pos_rest_share": _round(_rest_share(pos_rest, pos_ws)),
        "oo_rest_share": _round(_rest_share(oo_rest, oo_ws)),
        "fo_rest_share": _round(_rest_share(fo_rest, fo_ws)),
        "bal_rest_share": _round(_rest_share(bal_rest, bal_ws)),
        "mm_rest_share": _round(_rest_share(mm_rest, mm_ws)),
        "tk_rest_share": _round(_rest_share(tk_rest, tk_ws)),
        "pos_ws": pos_ws,
        "pos_rest": pos_rest,
        "oo_ws": oo_ws,
        "oo_rest": oo_rest,
        "fo_ws": fo_ws,
        "fo_rest": fo_rest,
        "bal_ws": bal_ws,
        "bal_rest": bal_rest,
        "mm_ws": mm_ws,
        "mm_rest": mm_rest,
        "tk_ws": tk_ws,
        "tk_rest": tk_rest,
        "warn_threshold": float(OMS_REST_SHARE_WARN),
        "min_samples": int(OMS_REST_SHARE_MIN_SAMPLES),
    }


def pick_latest_oms_heartbeat(heartbeats: list[dict[str, Any]]) -> dict[str, Any]:
    """First heartbeat (DESC) that carries oms_book / ws snapshots — for ops panel."""
    for hb in heartbeats or []:
        payload = hb.get("payload") if isinstance(hb, dict) else None
        if not isinstance(payload, dict):
            continue
        if not any(k in payload for k in ("oms_book", "private_ws", "public_ws")):
            continue
        oms = payload.get("oms_book")
        analysis = analyze_oms_book(oms if isinstance(oms, dict) else None)
        return {
            "component": str(hb.get("component") or ""),
            "ts_utc": str(hb.get("ts_utc") or ""),
            "oms_book": oms if isinstance(oms, dict) else None,
            "public_ws": payload.get("public_ws") if isinstance(payload.get("public_ws"), dict) else None,
            "private_ws": payload.get("private_ws") if isinstance(payload.get("private_ws"), dict) else None,
            "analysis": analysis,
        }
    return {}


def maybe_warn_oms_rest_share(
    oms_book: Optional[dict[str, Any]],
    *,
    private_ws_enabled: bool,
    public_ws_enabled: bool = False,
    alert: bool = True,
) -> dict[str, Any]:
    """Gauge + throttled CRITICAL per plane when REST dominates while that WS is on.

    Never raises. Private alert ignores public ``tk_*`` contamination.
    """
    global _OMS_PRIVATE_REST_ALERT_MONO, _OMS_PUBLIC_REST_ALERT_MONO, _OMS_REST_ALERT_MONO
    analysis = analyze_oms_book(oms_book)
    analysis["alerted"] = False
    analysis["alerted_public"] = False
    analysis["private_ws_enabled"] = bool(private_ws_enabled)
    analysis["public_ws_enabled"] = bool(public_ws_enabled)

    min_samples = int(OMS_REST_SHARE_MIN_SAMPLES)
    min_gap = float(OMS_REST_SHARE_ALERT_MIN_INTERVAL_SEC)

    # --- private plane ---
    if not private_ws_enabled:
        analysis["skip_reason"] = "private_ws_disabled"
    elif analysis.get("private_status") != "rest_heavy":
        analysis["skip_reason"] = "not_rest_heavy"
    elif int(analysis.get("private_n_total") or 0) < min_samples:
        analysis["skip_reason"] = "below_min_samples"
    else:
        try:
            from bitget.infra.logging_setup import get_logger

            get_logger("bitget.oms.source").warning(
                "OMS private plane REST-heavy while private WS enabled: "
                "rest_share=%.2f n=%s (pos %s/%s oo %s/%s fo %s/%s bal %s/%s mm %s/%s)",
                float(analysis.get("private_rest_share") or 0.0),
                analysis.get("private_n_total"),
                analysis.get("pos_rest"),
                analysis.get("pos_ws"),
                analysis.get("oo_rest"),
                analysis.get("oo_ws"),
                analysis.get("fo_rest"),
                analysis.get("fo_ws"),
                analysis.get("bal_rest"),
                analysis.get("bal_ws"),
                analysis.get("mm_rest"),
                analysis.get("mm_ws"),
            )
        except Exception:
            pass
        try:
            from bitget.infra import ops_logger

            ops_logger.record_gauge_snapshot(
                "bitget.oms_book",
                {
                    "warn": True,
                    "plane": "private",
                    "rest_share": analysis.get("private_rest_share"),
                    "n_total": analysis.get("private_n_total"),
                    "n_rest": analysis.get("private_n_rest"),
                    "n_ws": analysis.get("private_n_ws"),
                    "status": analysis.get("private_status"),
                },
            )
        except Exception:
            pass
        if alert:
            now = time.monotonic()
            if now - _OMS_PRIVATE_REST_ALERT_MONO >= min_gap:
                _OMS_PRIVATE_REST_ALERT_MONO = now
                _OMS_REST_ALERT_MONO = now
                try:
                    from bitget.governance.meta_alerts import send_meta_critical_alert

                    send_meta_critical_alert(
                        "OMS private book REST-heavy",
                        (
                            f"private_rest_share={analysis.get('private_rest_share')} "
                            f"n={analysis.get('private_n_total')} "
                            f"(private WS on — check login/channels)"
                        ),
                        prefix="OMS_REST_SHARE",
                    )
                    analysis["alerted"] = True
                except Exception:
                    pass
            else:
                analysis["skip_reason"] = "throttled"

    # --- public plane ---
    if not public_ws_enabled:
        analysis["public_skip_reason"] = "public_ws_disabled"
    elif analysis.get("public_status") != "rest_heavy":
        analysis["public_skip_reason"] = "not_rest_heavy"
    elif int(analysis.get("public_n_total") or 0) < min_samples:
        analysis["public_skip_reason"] = "below_min_samples"
    else:
        try:
            from bitget.infra.logging_setup import get_logger

            get_logger("bitget.oms.source").warning(
                "OMS public plane REST-heavy while public WS enabled: "
                "tk_rest_share=%.2f n=%s (tk %s/%s)",
                float(analysis.get("public_rest_share") or 0.0),
                analysis.get("public_n_total"),
                analysis.get("tk_rest"),
                analysis.get("tk_ws"),
            )
        except Exception:
            pass
        try:
            from bitget.infra import ops_logger

            ops_logger.record_gauge_snapshot(
                "bitget.oms_book",
                {
                    "warn": True,
                    "plane": "public",
                    "rest_share": analysis.get("public_rest_share"),
                    "n_total": analysis.get("public_n_total"),
                    "n_rest": analysis.get("public_n_rest"),
                    "n_ws": analysis.get("public_n_ws"),
                    "status": analysis.get("public_status"),
                },
            )
        except Exception:
            pass
        if alert:
            now = time.monotonic()
            if now - _OMS_PUBLIC_REST_ALERT_MONO >= min_gap:
                _OMS_PUBLIC_REST_ALERT_MONO = now
                try:
                    from bitget.governance.meta_alerts import send_meta_critical_alert

                    send_meta_critical_alert(
                        "OMS public ref REST-heavy",
                        (
                            f"public_rest_share={analysis.get('public_rest_share')} "
                            f"n={analysis.get('public_n_total')} "
                            f"(public WS on — check ticker universe / StreamBuffer)"
                        ),
                        prefix="OMS_TK_REST_SHARE",
                    )
                    analysis["alerted_public"] = True
                except Exception:
                    pass
            else:
                analysis["public_skip_reason"] = "throttled"

    return analysis
