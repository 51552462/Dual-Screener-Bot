"""US-COSINE-AXIS-01 — RANK 3D μ/σ · p90 컷 · config_kv COSINE_AXIS_STATS.

당일 횡단면 μσ 금지. MULTI 24D 무접촉. NA 미완봉 대체 없음.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

COSINE_AXIS_STATS_KEY = "COSINE_AXIS_STATS"
WINDOW_SESSIONS = 5
MIN_HIST_N = 30
US_DOLLAR_FLOOR = 30_000.0
KR_USD_FX = 1350.0
SD_FLOOR = 1e-9


def fit_mu_sd(rows: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    arr = np.asarray(rows, dtype=float)
    if arr.size == 0:
        return np.zeros(3), np.ones(3)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    mu = np.nanmean(arr, axis=0)
    sd = np.nanstd(arr, axis=0)
    sd = np.where(~np.isfinite(sd) | (sd < SD_FLOOR), 1.0, sd)
    mu = np.where(np.isfinite(mu), mu, 0.0)
    return mu.astype(float), sd.astype(float)


def apply_z(vec: np.ndarray, mu: np.ndarray, sd: np.ndarray) -> np.ndarray:
    return (np.asarray(vec, dtype=float) - mu) / sd


def percentile_p90(scores: Sequence[float]) -> Optional[float]:
    arr = np.asarray(list(scores), dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < 10:
        return None
    return float(np.percentile(arr, 90))


def empty_market_slot() -> Dict[str, Any]:
    return {
        "mu": [0.0, 0.0, 0.0],
        "sd": [1.0, 1.0, 1.0],
        "n": 0,
        "as_of": "",
        "window_sessions": WINDOW_SESSIONS,
        "source": "liq_pass_dna",
        "p90_z": None,
        "days": [],
    }


def _norm_blob(raw: Any) -> Dict[str, Any]:
    out = {"KR": empty_market_slot(), "US": empty_market_slot()}
    if not isinstance(raw, dict):
        return out
    for mk in ("KR", "US"):
        src = raw.get(mk)
        if not isinstance(src, dict):
            continue
        slot = empty_market_slot()
        try:
            mu = [float(x) for x in (src.get("mu") or slot["mu"])][:3]
            sd = [float(x) for x in (src.get("sd") or slot["sd"])][:3]
            while len(mu) < 3:
                mu.append(0.0)
            while len(sd) < 3:
                sd.append(1.0)
            slot["mu"] = mu
            slot["sd"] = sd
            slot["n"] = int(src.get("n") or 0)
            slot["as_of"] = str(src.get("as_of") or "")
            slot["window_sessions"] = int(
                src.get("window_sessions") or WINDOW_SESSIONS
            )
            slot["source"] = str(src.get("source") or "liq_pass_dna")
            p90 = src.get("p90_z")
            slot["p90_z"] = float(p90) if p90 is not None else None
            days = src.get("days")
            if isinstance(days, list):
                slot["days"] = days[-WINDOW_SESSIONS:]
        except (TypeError, ValueError):
            continue
        out[mk] = slot
    return out


def load_axis_blob() -> Dict[str, Any]:
    try:
        from config_manager import get_config_value

        return _norm_blob(get_config_value(COSINE_AXIS_STATS_KEY, {}))
    except Exception:
        return _norm_blob({})


def save_axis_blob(blob: Dict[str, Any]) -> None:
    from config_manager import set_config_value

    set_config_value(COSINE_AXIS_STATS_KEY, _norm_blob(blob))


def hist_rows_from_slot(slot: Dict[str, Any]) -> np.ndarray:
    rows: List[List[float]] = []
    for day in slot.get("days") or []:
        if not isinstance(day, dict):
            continue
        for vec in day.get("rows") or []:
            try:
                trip = [float(vec[0]), float(vec[1]), float(vec[2])]
            except (TypeError, ValueError, IndexError):
                continue
            if all(np.isfinite(trip)):
                rows.append(trip)
    if not rows:
        return np.zeros((0, 3))
    return np.asarray(rows, dtype=float)


def stats_ready(slot: Dict[str, Any]) -> bool:
    return int(hist_rows_from_slot(slot).shape[0]) >= MIN_HIST_N


def fit_slot_mu_sd(slot: Dict[str, Any]) -> Tuple[np.ndarray, np.ndarray, int]:
    hist = hist_rows_from_slot(slot)
    mu, sd = fit_mu_sd(hist)
    return mu, sd, int(hist.shape[0])


def append_market_day(
    blob: Dict[str, Any],
    *,
    market: str,
    as_of: str,
    rows: Sequence[Sequence[float]],
    p90_z: Optional[float],
) -> Dict[str, Any]:
    mk = str(market or "").upper()
    out = _norm_blob(blob)
    packed = []
    for vec in rows:
        try:
            trip = [float(vec[0]), float(vec[1]), float(vec[2])]
        except (TypeError, ValueError, IndexError):
            continue
        if all(np.isfinite(trip)):
            packed.append(trip)
    days = [
        d
        for d in (out[mk].get("days") or [])
        if isinstance(d, dict) and str(d.get("as_of") or "") != str(as_of)
    ]
    days.append({"as_of": str(as_of), "rows": packed})
    days = days[-WINDOW_SESSIONS:]
    out[mk]["days"] = days
    mu, sd, n = fit_slot_mu_sd(out[mk])
    out[mk]["mu"] = [float(x) for x in mu]
    out[mk]["sd"] = [float(x) for x in sd]
    out[mk]["n"] = n
    out[mk]["as_of"] = str(as_of)
    out[mk]["p90_z"] = float(p90_z) if p90_z is not None else None
    out[mk]["window_sessions"] = WINDOW_SESSIONS
    out[mk]["source"] = "liq_pass_dna"
    return out


def z_templates_3d(
    templates: Dict[str, np.ndarray], mu: np.ndarray, sd: np.ndarray
) -> Dict[str, np.ndarray]:
    out: Dict[str, np.ndarray] = {}
    for name, vec in templates.items():
        arr = np.asarray(vec, dtype=float).reshape(-1)
        if arr.size == 3:
            out[str(name)] = apply_z(arr, mu, sd)
    return out


def best_rank3_cosine(
    vec: np.ndarray, templates_3d: Dict[str, np.ndarray]
) -> float:
    v = np.asarray(vec, dtype=float).reshape(-1)
    if v.size != 3 or not templates_3d:
        return 0.0
    best = 0.0
    n1 = float(np.linalg.norm(v))
    if n1 <= 0:
        return 0.0
    for tv in templates_3d.values():
        t = np.asarray(tv, dtype=float).reshape(-1)
        if t.size != 3:
            continue
        n2 = float(np.linalg.norm(t))
        if n2 <= 0:
            continue
        best = max(best, float(np.dot(v, t) / (n1 * n2)))
    return best


def session_as_of(market: str) -> str:
    if str(market).upper() == "KR":
        tz_name = "Asia/Seoul"
    else:
        tz_name = "America/New_York"
    try:
        import pytz

        return datetime.now(pytz.timezone(tz_name)).strftime("%Y-%m-%d")
    except Exception:
        return datetime.utcnow().strftime("%Y-%m-%d")
