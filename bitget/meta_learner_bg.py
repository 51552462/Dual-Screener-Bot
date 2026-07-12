"""Bitget meta-learner — 내부 PRI ↔ 외부 REGIME 신뢰 매트릭스 (bitget SSOT)."""
from __future__ import annotations

import json
import math
import os
import sqlite3
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from bitget.forward.shared import DB_PATH
from bitget.infra.clock import parse_utc_iso, utc_date_key, utc_datetime_str, utc_now
from bitget.infra.data_paths import bitget_data_dir

TRUST_MATRIX_FILENAME = "BITGET_META_TRUST_MATRIX.json"
W_INIT = 0.50
W_MIN = 0.20
W_MAX = 0.80
DEFAULT_ALPHA = 0.05
PNL_SCALE_PCT = 5.0
DIVERGENCE_EVAL_DAYS = 7
PRI_Z_UP = 0.45
PRI_Z_DOWN = -0.45
_LOCK = threading.RLock()

_EXTERNAL_UP = {"BULL", "RISK_ON", "BULL_TREND", "UP"}
_EXTERNAL_DOWN = {"BEAR", "HIGH_VOL", "RISK_OFF", "DOWN"}


def trust_matrix_path() -> str:
    return os.path.join(bitget_data_dir(), TRUST_MATRIX_FILENAME)


def _default_matrix() -> Dict[str, Any]:
    return {
        "schema": "bitget_meta_trust_matrix.v1",
        "w_internal": W_INIT,
        "w_external": W_INIT,
        "alpha": DEFAULT_ALPHA,
        "updated_at": None,
        "pending_events": [],
        "history": [],
    }


def load_trust_matrix() -> Dict[str, Any]:
    path = trust_matrix_path()
    if not os.path.isfile(path):
        return _default_matrix()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_matrix()
        base = _default_matrix()
        base.update({k: v for k, v in data.items() if v is not None})
        wi = float(base.get("w_internal", W_INIT) or W_INIT)
        base["w_internal"] = min(W_MAX, max(W_MIN, wi))
        base["w_external"] = round(1.0 - base["w_internal"], 6)
        return base
    except (OSError, json.JSONDecodeError, ValueError):
        return _default_matrix()


def save_trust_matrix(state: Dict[str, Any]) -> bool:
    path = trust_matrix_path()
    state = dict(state)
    state["updated_at"] = utc_datetime_str()
    state["w_external"] = round(1.0 - float(state.get("w_internal", W_INIT)), 6)
    try:
        with _LOCK:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            fd, tmp = tempfile.mkstemp(prefix=".trust_bg_", suffix=".json", dir=os.path.dirname(path) or ".")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(state, f, ensure_ascii=False, indent=2)
                os.replace(tmp, path)
            finally:
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
        return True
    except OSError:
        return False


def _dir_from_z(z: float) -> str:
    if z >= PRI_Z_UP:
        return "UP"
    if z <= PRI_Z_DOWN:
        return "DOWN"
    return "NEUTRAL"


def internal_direction() -> Tuple[str, Optional[float]]:
    try:
        from bitget.evolution.weekly_proprietary_regime_bg import load_weekly_shadow_pri

        data = load_weekly_shadow_pri()
        blended = data.get("blended") if isinstance(data, dict) else None
        if isinstance(blended, dict):
            z = float(blended.get("composite_z") or 0.0)
            reg = str(blended.get("regime") or "").upper()
            if reg in ("UP", "DOWN"):
                return reg, z
            if reg == "SIDEWAYS":
                return "NEUTRAL", z
            return _dir_from_z(z), z
    except Exception:
        pass
    return "NEUTRAL", None


def external_direction(
    meta: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    rk = ""
    if isinstance(meta, dict):
        rk = str(meta.get("META_REGIME_KEY") or "").upper()
    if not rk:
        try:
            from bitget.governance.meta_sync import load_bitget_meta_resolved

            rk = str(load_bitget_meta_resolved().get("META_REGIME_KEY") or "").upper()
        except Exception:
            pass
    if not rk and isinstance(sys_config, dict):
        rk = str(sys_config.get("CURRENT_REGIME_KEY") or "").upper()
    if rk in _EXTERNAL_UP:
        return "UP", rk
    if rk in _EXTERNAL_DOWN:
        return "DOWN", rk
    return "NEUTRAL", rk or "UNKNOWN"


def _forward_net_pnl_pct(conn: sqlite3.Connection, start_date: str, end_date: str) -> Tuple[float, int]:
    row = conn.execute(
        """
        SELECT COALESCE(SUM(final_ret), 0.0), COUNT(*)
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%'
          AND final_ret IS NOT NULL
          AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
          AND COALESCE(NULLIF(TRIM(exit_date),''), entry_date) >= ?
          AND COALESCE(NULLIF(TRIM(exit_date),''), entry_date) <= ?
        """,
        (start_date, end_date),
    ).fetchone()
    try:
        return float(row[0] or 0.0), int(row[1] or 0)
    except (TypeError, ValueError):
        return 0.0, 0


def _winner_of(internal_dir: str, external_dir: str, fwd_pnl_pct: float) -> Optional[str]:
    if fwd_pnl_pct > 0:
        realized = "UP"
    elif fwd_pnl_pct < 0:
        realized = "DOWN"
    else:
        return None
    if internal_dir == realized:
        return "internal"
    if external_dir == realized:
        return "external"
    return None


def _apply_weight_update(w_internal: float, winner: str, fwd_pnl_pct: float, alpha: float) -> float:
    magnitude = math.tanh(abs(float(fwd_pnl_pct)) / PNL_SCALE_PCT)
    step = alpha * magnitude
    if winner == "internal":
        w_internal += step
    elif winner == "external":
        w_internal -= step
    return float(min(W_MAX, max(W_MIN, w_internal)))


def run_bitget_meta_learning_cycle(
    sys_config: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    now = now or utc_now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    today = utc_date_key(anchor=now)
    db_path = DB_PATH

    with _LOCK:
        m = load_trust_matrix()
        alpha = float(m.get("alpha", DEFAULT_ALPHA) or DEFAULT_ALPHA)
        idir, iz = internal_direction()
        edir, ekey = external_direction(meta, sys_config)
        recorded = False
        if {idir, edir} == {"UP", "DOWN"}:
            already = any(
                isinstance(e, dict) and e.get("date") == today for e in m.get("pending_events", [])
            )
            if not already:
                m.setdefault("pending_events", []).append(
                    {
                        "date": today,
                        "internal_dir": idir,
                        "external_dir": edir,
                        "external_key": ekey,
                        "internal_z": iz,
                    }
                )
                recorded = True

        evaluated = 0
        if db_path and os.path.isfile(db_path):
            conn = sqlite3.connect(db_path, timeout=60)
            try:
                still_pending: List[Dict[str, Any]] = []
                for ev in m.get("pending_events", []):
                    if not isinstance(ev, dict) or not ev.get("date"):
                        continue
                    ev_dt = parse_utc_iso(str(ev["date"])[:10])
                    if ev_dt is None:
                        continue
                    mature_on = ev_dt + timedelta(days=DIVERGENCE_EVAL_DAYS)
                    if now < mature_on:
                        still_pending.append(ev)
                        continue
                    start = str(ev["date"])[:10]
                    end = utc_date_key(anchor=mature_on)
                    fwd_pnl, n = _forward_net_pnl_pct(conn, start, end)
                    winner = _winner_of(ev.get("internal_dir", ""), ev.get("external_dir", ""), fwd_pnl)
                    w_before = float(m["w_internal"])
                    if winner is not None:
                        m["w_internal"] = _apply_weight_update(w_before, winner, fwd_pnl, alpha)
                    m.setdefault("history", []).append(
                        {
                            "date": start,
                            "eval_end": end,
                            "winner": winner,
                            "fwd_pnl_pct": fwd_pnl,
                            "n_trades": n,
                            "w_before": w_before,
                            "w_after": m["w_internal"],
                        }
                    )
                    evaluated += 1
                m["pending_events"] = still_pending
            finally:
                conn.close()

        save_trust_matrix(m)
        return {
            "ok": True,
            "recorded_divergence": recorded,
            "evaluated": evaluated,
            "w_internal": m["w_internal"],
            "w_external": m["w_external"],
        }


def build_meta_cognition_line() -> str:
    m = load_trust_matrix()
    wi = float(m.get("w_internal", W_INIT) or W_INIT)
    we = float(m.get("w_external", W_INIT) or W_INIT)
    hist = m.get("history") if isinstance(m.get("history"), list) else []
    last = hist[-1] if hist else {}
    winner = last.get("winner") if isinstance(last, dict) else None
    tail = f" · 최근 채점 {winner}" if winner else ""
    return (
        f"🧠 Meta-Trust: 내부 PRI <b>{wi*100:.0f}%</b> · 외부 REGIME <b>{we*100:.0f}%</b>{tail}"
    )
