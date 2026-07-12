"""
Bitget Champion Genesis — 코인 챔피언/독성 로직 탄생 전조(前兆) 자율 축적·검증.

주식 `evolution/champion_genesis.py`의 알고리즘(유동적 T0 앙상블·국면 연동 동적 윈도·
신뢰도 감가상각·마할라노비스+DTW 매칭)을 그대로 유지하되, 데이터 소스만 코인 전용으로
교체한다(Two-Track 격리 — 주식 SSOT 전혀 미참조):
  - forward_trades(KR/US) → bitget_forward_trades(SPOT/FUTURES)
  - sector 컬럼(GICS)     → 심볼 기반 자산군(BTC/ETH/SOL/MEME/... via _coin_asset_group)
  - REGIME_VECTOR_HISTORY(6차원, SPX/KOSPI/VIX/PRI/macro)
        → REGIME_VECTOR_HISTORY_BG(4차원, BTC EMA200 이격·기울기·ATR·ETH/BTC 브레드스)
  - 저장 위치: Bitget 자체 DB(bitget_market_data.sqlite) — 신규 테이블 2개만.
  - deathmatch_arm_snapshot(수익 우위 전환점 보조 신호): Bitget DB에 persist
    (``bitget.evolution.deathmatch_bg`` 경유 — 주식 SSOT 미참조).

❗ 비침습 원칙: 스캔·포지션·Kelly 경로 불간섭. 모든 공개 함수는 예외를 삼키고
안전 폴백한다(리포트/거버넌스 경로에 부하·락·예외 전파 0).
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from bitget.evolution.coin_regime_vector import (
    N_DIMS as _N_DIMS,
    build_current_coin_regime_vector,
    load_vector_history as _vector_history,
    load_vector_history_arrays as _vector_history_arrays,
    regime_index as _regime_index,
)
from bitget.infra.clock import utc_date_days_ago_str, utc_date_key, utc_datetime_str

GENESIS_TABLE = "champion_precursor_genesis"
PREDICTION_TABLE = "precursor_prediction_log"

ADVANTAGE_KEY = "GENESIS_PRECURSOR_ADVANTAGE_BG"
SIM_THRESHOLD_KEY = "GENESIS_SIMILARITY_THRESHOLD_BG"
ENABLED_KEY = "GENESIS_PRECURSOR_ENABLED_BG"

DAILY_CONFIRMED_CAP = 80


def _advantage_key(market: str) -> str:
    """SPOT/FUTURES 독립 어드밴티지 플래그(시장 간 자산군 오염 방지)."""
    return f"{ADVANTAGE_KEY}_{str(market).upper()}"


LOOKBACK_DAYS = 30
HORIZON_DAYS = 30
DEFAULT_SIM_THRESHOLD = 0.80
SIM_FLOOR, SIM_CEIL = 0.70, 0.95
TOXIC_BOTTOM_FRAC = 0.10
DEFAULT_KELLY_BOOST = 1.20
MIN_WINDOW_POINTS = 8

GS_WEIGHTS = {
    "ret_adv": 0.40,
    "venergy_z": 0.30,
    "sector_dom": 0.30,
}
GS_THRESHOLD_K = 0.5
VENERGY_LOOKBACK = 60

WINDOW_MIN_DAYS = 10
WINDOW_MAX_DAYS = 45

CONF_DECAY_FAIL = 0.5
CONF_DECAY_PRED_MISS = 0.6
CONF_RECOVER = 1.15
CONF_FLOOR = 0.05
CONF_PRIOR_STRENGTH = 4.0

_RIDGE_LAMBDA = 1e-3
W_MAHALANOBIS = 0.6
W_DTW = 0.4
_MAHA_SCALE = 3.0
_DTW_SCALE = 2.5

_MARKET_ICON = {"spot": "🟢", "futures": "🟠"}

_DDL = f"""
CREATE TABLE IF NOT EXISTS {GENESIS_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market TEXT NOT NULL,
    champion_label TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'champion',   -- champion | toxic
    ignition_date TEXT NOT NULL,
    crowned_date TEXT,
    window_start TEXT,
    window_end TEXT,
    precursor_json TEXT NOT NULL,
    champion_composite REAL,
    realized_fwd_ret REAL,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending|confirmed|failed|toxic|thin
    confidence REAL NOT NULL DEFAULT 1.0,
    decay_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT,
    resolved_at TEXT,
    UNIQUE(market, champion_label, ignition_date, kind)
);
CREATE INDEX IF NOT EXISTS idx_bg_genesis_status ON {GENESIS_TABLE}(status, kind);

CREATE TABLE IF NOT EXISTS {PREDICTION_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    predict_date TEXT NOT NULL,
    market TEXT NOT NULL,
    matched_champion_label TEXT,
    matched_sector TEXT,
    similarity REAL,
    threshold_used REAL,
    horizon_days INTEGER DEFAULT {HORIZON_DAYS},
    hit INTEGER,
    realized_ret REAL,
    recorded_at TEXT,
    resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_bg_pred_open ON {PREDICTION_TABLE}(hit, predict_date);
"""


# ---------------------------------------------------------------------------
# DB 헬퍼 — Bitget 자체 DB만 사용(주식 SSOT 미참조), 표준 커넥터로 락 경합 최소화
# ---------------------------------------------------------------------------
def _db_path() -> str:
    from bitget.infra.data_paths import market_data_db_path

    return market_data_db_path()


def _ro_conn() -> Optional[sqlite3.Connection]:
    path = _db_path()
    if not path or not os.path.isfile(path):
        return None
    try:
        from bitget.infra.shared_db_connector import get_connection

        conn = get_connection(path, read_only=True)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def _rw_conn() -> Optional[sqlite3.Connection]:
    path = _db_path()
    if not path or not os.path.isfile(path):
        return None
    try:
        from bitget.infra.shared_db_connector import get_connection

        conn = get_connection(path, read_only=False)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def _migrate_genesis_columns(conn: sqlite3.Connection) -> None:
    try:
        cur = conn.execute(f"PRAGMA table_info({GENESIS_TABLE})")
        existing = {str(r[1]) for r in cur.fetchall()}
        if "confidence" not in existing:
            conn.execute(
                f"ALTER TABLE {GENESIS_TABLE} ADD COLUMN confidence REAL NOT NULL DEFAULT 1.0"
            )
        if "decay_count" not in existing:
            conn.execute(
                f"ALTER TABLE {GENESIS_TABLE} ADD COLUMN decay_count INTEGER NOT NULL DEFAULT 0"
            )
    except sqlite3.Error:
        pass


def ensure_genesis_schema() -> bool:
    conn = _rw_conn()
    if conn is None:
        return False
    try:
        conn.executescript(_DDL)
        _migrate_genesis_columns(conn)
        conn.commit()
        return True
    except sqlite3.Error:
        return False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# config 헬퍼 (lazy, Bitget config_kv)
# ---------------------------------------------------------------------------
def _load_cfg(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(cfg, dict):
        return cfg
    try:
        from bitget.infra.config_manager import load_system_config

        return load_system_config() or {}
    except Exception:
        return {}


def _set_cfg(key: str, value: Any) -> None:
    try:
        from bitget.infra.config_manager import set_config_value

        set_config_value(key, value)
    except Exception:
        pass


def _is_enabled(cfg: Dict[str, Any]) -> bool:
    raw = cfg.get(ENABLED_KEY, "1")
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _now() -> str:
    return utc_datetime_str()


def _parse_date(s: Any) -> Optional[datetime]:
    if not s:
        return None
    txt = str(s).strip()[:10]
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# 마할라노비스 공분산 추정 (코인 자체 롤링 히스토리만 사용 — 주식 에피소드 미혼입)
# ---------------------------------------------------------------------------
def _estimate_inv_cov_bg(history: List[List[float]]) -> Tuple[Any, str]:
    import numpy as np

    rows = [
        np.asarray(h, dtype=float)
        for h in history
        if isinstance(h, list) and len(h) == _N_DIMS
    ]
    if len(rows) < _N_DIMS + 2:
        return np.eye(_N_DIMS), "euclidean_fallback"
    mat = np.vstack(rows)
    try:
        cov = np.cov(mat, rowvar=False)
        cov = cov + _RIDGE_LAMBDA * np.eye(_N_DIMS)
        inv = np.linalg.pinv(cov)
        if not np.all(np.isfinite(inv)):
            return np.eye(_N_DIMS), "euclidean_fallback"
        return inv, "mahalanobis"
    except np.linalg.LinAlgError:
        return np.eye(_N_DIMS), "euclidean_fallback"


def _window_env_features(
    cfg: Dict[str, Any], t0: datetime, lookback: int = LOOKBACK_DAYS
) -> Dict[str, Any]:
    """[T0-lookback, T0] 구간 REGIME_VECTOR_HISTORY_BG 평균/기울기/궤적."""
    try:
        import numpy as np
    except Exception:
        np = None  # type: ignore

    lo = t0 - timedelta(days=lookback)
    rows: List[List[float]] = []
    for item in _vector_history(cfg):
        ts = _parse_date(item.get("ts"))
        if ts is None or ts > t0 or ts < lo:
            continue
        vec = item.get("vector")
        if isinstance(vec, list) and len(vec) == _N_DIMS:
            try:
                rows.append([float(v) for v in vec])
            except (TypeError, ValueError):
                continue

    n = len(rows)
    if n == 0:
        return {"n_points": 0}

    if np is not None:
        mat = np.asarray(rows, dtype=float)
        mean_vec = [round(float(x), 6) for x in mat.mean(axis=0)]
        k = max(1, n // 3)
        slope = [round(float(x), 6) for x in (mat[-k:].mean(axis=0) - mat[:k].mean(axis=0))]
    else:
        cols = list(zip(*rows))
        mean_vec = [round(sum(c) / n, 6) for c in cols]
        k = max(1, n // 3)
        slope = [round(sum(c[-k:]) / k - sum(c[:k]) / k, 6) for c in cols]

    trajectory = [round(_regime_index(r), 6) for r in rows]
    return {
        "n_points": n,
        "regime_vec_mean": mean_vec,
        "regime_vec_slope": slope,
        "regime_centroid": mean_vec,
        "regime_trajectory": trajectory,
        "pri_z_trend": round(slope[3], 6) if len(slope) > 3 else 0.0,
    }


# ---------------------------------------------------------------------------
# bitget_forward_trades 윈도 지문 (자산군/수급/태동) — RO 읽기, group_key 매핑
# ---------------------------------------------------------------------------
def _group_key(sig: str) -> str:
    try:
        from evolution.deathmatch_battle_royale import ledger_group_key

        return ledger_group_key(sig)
    except Exception:
        return str(sig or "UNKNOWN")


def _fetch_label_trades(
    conn: sqlite3.Connection, market: str, label: str
) -> List[Dict[str, Any]]:
    """해당 group_key(label)에 매핑되는 CLOSED 거래(시간순). sector는 심볼 기반 자산군."""
    from bitget.forward.mutant import _coin_asset_group
    from bitget.infra.bounded_reads import genesis_closed_trades_sql

    try:
        sql, params = genesis_closed_trades_sql(market_type=market)
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
    except sqlite3.Error:
        return []
    out: List[Dict[str, Any]] = []
    for r in reversed(rows):
        if _group_key(str(r["sig_type"] or "")) != label:
            continue
        d = dict(r)
        d["sector"] = _coin_asset_group(d.get("symbol"))
        out.append(d)
    return out


def _fetch_market_trades(conn: sqlite3.Connection, market: str) -> List[Dict[str, Any]]:
    """v_energy Z-score·자산군 주도력 산정을 위한 시장 전체 CLOSED 거래(경량)."""
    from bitget.forward.mutant import _coin_asset_group
    from bitget.infra.bounded_reads import genesis_market_energy_closed_sql

    try:
        sql, params = genesis_market_energy_closed_sql(market_type=market)
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
    except sqlite3.Error:
        return []
    out: List[Dict[str, Any]] = []
    for r in reversed(rows):
        d = _parse_date(r["entry_date"])
        if d is None:
            continue
        out.append(
            {"d": d, "sector": _coin_asset_group(r["symbol"]), "ve": _safe_float(r["v_energy"])}
        )
    return out


def _fetch_arm_snapshot_series(
    conn: sqlite3.Connection, market: str, label: str
) -> List[Tuple[datetime, float]]:
    """deathmatch_arm_snapshot composite_score 시계열 (Bitget DB — deathmatch_bg persist)."""
    from bitget.infra.bounded_reads import genesis_arm_snapshot_sql

    try:
        sql, params = genesis_arm_snapshot_sql(market=market, label=label)
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
    except sqlite3.Error:
        return []
    out: List[Tuple[datetime, float]] = []
    for r in reversed(rows):
        if hasattr(r, "keys"):
            td_raw = r["trade_date"]
            score = r["composite_score"]
            mean_ret = r["mean_ret"]
        else:
            td_raw = r[0]
            score = r[1] if len(r) > 1 else None
            mean_ret = r[2] if len(r) > 2 else None
        d = _parse_date(td_raw)
        if d is None:
            continue
        if score is None:
            score = mean_ret
        out.append((d, _safe_float(score)))
    return out


def _logistic(x: float, scale: float = 1.0) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-float(x) / max(scale, 1e-6)))
    except (OverflowError, ValueError):
        return 0.0 if x < 0 else 1.0


def _trace_ignition_simple(rows: List[Dict[str, Any]], kind: str) -> Optional[datetime]:
    """폴백: 표본 빈약 시 첫 수익(champion)/첫 손실(toxic) 진입일."""
    dated = [(r, _parse_date(r.get("entry_date"))) for r in rows]
    dated = [(r, d) for (r, d) in dated if d is not None]
    if not dated:
        return None
    dated.sort(key=lambda x: x[1])
    if kind == "champion":
        for r, d in dated:
            if _safe_float(r.get("final_ret")) > 0:
                return d
        return dated[0][1]
    for r, d in dated:
        if _safe_float(r.get("final_ret")) < 0:
            return d
    return dated[0][1]


def _fluid_ignition(
    conn: sqlite3.Connection,
    market: str,
    label: str,
    rows: List[Dict[str, Any]],
    kind: str,
) -> Tuple[Optional[datetime], Dict[str, Any]]:
    """[고도화1] 다중 앙상블 유동적 T0(Fluid Ignition) — 주식판과 동일 수식."""
    dated = [(r, _parse_date(r.get("entry_date"))) for r in rows]
    dated = [(r, d) for (r, d) in dated if d is not None]
    if len(dated) < 3:
        return _trace_ignition_simple(rows, kind), {"method": "fallback_sparse"}
    dated.sort(key=lambda x: x[1])

    sign = 1.0 if kind == "champion" else -1.0
    market_trades = _fetch_market_trades(conn, market)
    arm_series = _fetch_arm_snapshot_series(conn, market, label)

    sec_counts: Dict[str, int] = {}
    for r, _d in dated:
        s = str(r.get("sector") or "").strip() or "OTHER"
        sec_counts[s] = sec_counts.get(s, 0) + 1
    label_sector = max(sec_counts.items(), key=lambda kv: kv[1])[0] if sec_counts else "OTHER"

    def _venergy_z(d: datetime, ve: float) -> Optional[float]:
        lo = d - timedelta(days=VENERGY_LOOKBACK)
        base = [t["ve"] for t in market_trades if lo <= t["d"] <= d and t["ve"] > 0]
        if len(base) < 5:
            return None
        mu = sum(base) / len(base)
        var = sum((v - mu) ** 2 for v in base) / len(base)
        sd = math.sqrt(var)
        if sd < 1e-9:
            return 0.0
        return (ve - mu) / sd

    def _sector_dom(d: datetime) -> Optional[float]:
        lo = d - timedelta(days=15)
        win = [t for t in market_trades if lo <= t["d"] <= d]
        if len(win) < 3:
            return None
        same = sum(1 for t in win if t["sector"] == label_sector)
        return same / len(win)

    def _arm_adv(d: datetime) -> Optional[float]:
        near = [(abs((sd - d).days), sc) for sd, sc in arm_series if sd <= d]
        if not near:
            return None
        near.sort(key=lambda x: x[0])
        return near[0][1]

    cum_sum, cum_n = 0.0, 0
    raw: List[Dict[str, Any]] = []
    for r, d in dated:
        cum_sum += _safe_float(r.get("final_ret"))
        cum_n += 1
        cum_avg = cum_sum / cum_n
        ret_signal = sign * cum_avg
        arm = _arm_adv(d)
        if arm is not None:
            ret_signal = 0.5 * ret_signal + 0.5 * (sign * arm)
        raw.append(
            {
                "d": d,
                "ret": ret_signal,
                "vez": _venergy_z(d, _safe_float(r.get("v_energy"))),
                "sec": _sector_dom(d),
            }
        )

    scores: List[float] = []
    comps_at: List[Dict[str, float]] = []
    for item in raw:
        comp: Dict[str, float] = {}
        comp["ret_adv"] = _logistic(item["ret"], scale=3.0)
        if item["vez"] is not None:
            comp["venergy_z"] = _logistic(item["vez"], scale=1.0)
        if item["sec"] is not None:
            comp["sector_dom"] = float(max(0.0, min(1.0, item["sec"])))
        wsum = sum(GS_WEIGHTS[k] for k in comp)
        gscore = (sum(GS_WEIGHTS[k] * v for k, v in comp.items()) / wsum) if wsum > 0 else 0.0
        scores.append(gscore)
        comps_at.append(comp)

    if not scores:
        return _trace_ignition_simple(rows, kind), {"method": "fallback_empty"}

    mean_s = sum(scores) / len(scores)
    var_s = sum((s - mean_s) ** 2 for s in scores) / len(scores)
    std_s = math.sqrt(var_s)
    dyn_thr = mean_s + GS_THRESHOLD_K * std_s

    t0_idx = None
    for i, s in enumerate(scores):
        if s >= dyn_thr:
            t0_idx = i
            break
    if t0_idx is None:
        t0_idx = max(range(len(scores)), key=lambda i: scores[i])

    t0 = raw[t0_idx]["d"]
    meta = {
        "method": "fluid_ensemble",
        "genesis_score": round(float(scores[t0_idx]), 4),
        "dynamic_threshold": round(float(dyn_thr), 4),
        "score_mean": round(float(mean_s), 4),
        "score_std": round(float(std_s), 4),
        "components": {k: round(float(v), 4) for k, v in comps_at[t0_idx].items()},
        "label_sector": label_sector,
        "n_candidates": len(scores),
    }
    return t0, meta


def _dynamic_window_days(cfg: Dict[str, Any], t0: datetime) -> Tuple[int, Dict[str, Any]]:
    """[고도화2] 국면 연동 동적 전조 윈도 — atr_z(코인 변동성 국면 벡터의 3번째 축)."""
    atr_z = 0.0
    best_gap = None
    for item in _vector_history(cfg):
        ts = _parse_date(item.get("ts"))
        vec = item.get("vector")
        if ts is None or not (isinstance(vec, list) and len(vec) == _N_DIMS):
            continue
        gap = abs((ts - t0).days)
        if best_gap is None or gap < best_gap:
            best_gap = gap
            atr_z = _safe_float(vec[2])

    regime = str(cfg.get("CURRENT_REGIME_KEY", "") or "").upper()
    crisis = any(k in regime for k in ("BEAR", "WHIPSAW", "HIGH_VOL"))
    calm = any(k in regime for k in ("BULL", "SIDEWAYS"))

    window = 30.0 - 18.0 * math.tanh(atr_z)
    if crisis:
        window = min(window, 15.0)
    elif calm and atr_z < 0.5:
        window = max(window, 35.0)
    window_days = int(max(WINDOW_MIN_DAYS, min(WINDOW_MAX_DAYS, round(window))))
    return window_days, {
        "vix_z": round(float(atr_z), 4),
        "regime": regime or "UNKNOWN",
        "window_days": window_days,
    }


def _window_footprint(
    rows: List[Dict[str, Any]], t0: datetime, lookback: int = LOOKBACK_DAYS
) -> Dict[str, Any]:
    """[T0-lookback, T0] 윈도 내 자산군/수급/로직 태동 지문."""
    lo = t0 - timedelta(days=lookback)
    win = [
        r
        for r in rows
        if (_parse_date(r.get("entry_date")) is not None and lo <= _parse_date(r.get("entry_date")) <= t0)
    ]
    n = len(win)
    if n == 0:
        return {"n_trades": 0}

    sec_counts: Dict[str, int] = {}
    for r in win:
        s = str(r.get("sector") or "").strip() or "OTHER"
        sec_counts[s] = sec_counts.get(s, 0) + 1
    top_sector, top_n = max(sec_counts.items(), key=lambda kv: kv[1])

    def _avg(col: str) -> float:
        vals = [_safe_float(r.get(col)) for r in win if r.get(col) is not None]
        return round(sum(vals) / len(vals), 4) if vals else 0.0

    scout = 0
    for r in win:
        tag = (str(r.get("sig_type") or "") + " " + str(r.get("flow_tags") or "")).upper()
        if any(k in tag for k in ("OBSERVE", "SCOUT", "INCUBATOR")):
            scout += 1

    return {
        "n_trades": n,
        "rotation": {
            "sector": top_sector,
            "window_dominance": round(top_n / n, 3),
        },
        "smart_money": {
            "sector_trade_count": top_n,
            "entry_breadth_mean": _avg("entry_breadth"),
        },
        "genesis_footprint": {
            "scout_trades": scout,
            "incubator_seen": scout > 0,
            "entry_dna_mean": {
                "cpv": _avg("dyn_cpv"),
                "tb": _avg("dyn_tb"),
                "v_energy": _avg("v_energy"),
            },
        },
    }


# ---------------------------------------------------------------------------
# 1) 전조 축적 (champion + toxic 양방향)
# ---------------------------------------------------------------------------
def _build_precursor(
    conn_ro: sqlite3.Connection,
    cfg: Dict[str, Any],
    market: str,
    label: str,
    kind: str,
) -> Optional[Dict[str, Any]]:
    rows = _fetch_label_trades(conn_ro, market, label)
    if not rows:
        return None
    t0, ign_meta = _fluid_ignition(conn_ro, market, label, rows, kind)
    if t0 is None:
        return None

    window_days, win_meta = _dynamic_window_days(cfg, t0)

    env = _window_env_features(cfg, t0, lookback=window_days)
    fp = _window_footprint(rows, t0, lookback=window_days)

    precursor = {
        "env": env,
        **fp,
        "ignition_meta": ign_meta,
        "window_meta": win_meta,
    }
    status = "thin" if int(env.get("n_points", 0)) < MIN_WINDOW_POINTS else (
        "toxic" if kind == "toxic" else "pending"
    )
    return {
        "ignition_date": t0.strftime("%Y-%m-%d"),
        "window_start": (t0 - timedelta(days=window_days)).strftime("%Y-%m-%d"),
        "window_end": t0.strftime("%Y-%m-%d"),
        "precursor": precursor,
        "status": status,
    }


def _insert_genesis(
    conn_rw: sqlite3.Connection,
    *,
    market: str,
    label: str,
    kind: str,
    built: Dict[str, Any],
    crowned_date: Optional[str],
    composite: Optional[float],
) -> bool:
    try:
        conn_rw.execute(
            f"""
            INSERT INTO {GENESIS_TABLE} (
                market, champion_label, kind, ignition_date, crowned_date,
                window_start, window_end, precursor_json, champion_composite,
                realized_fwd_ret, status, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(market, champion_label, ignition_date, kind) DO NOTHING
            """,
            (
                str(market).lower(),
                label,
                kind,
                built["ignition_date"],
                crowned_date,
                built["window_start"],
                built["window_end"],
                json.dumps(built["precursor"], ensure_ascii=False),
                composite,
                None,
                built["status"],
                _now(),
            ),
        )
        return True
    except sqlite3.Error:
        return False


def capture_champion_precursors(
    br: Any,
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    market: str = "spot",
) -> Dict[str, Any]:
    """
    데스매치(run_battle_royal) 1사이클 종료 후 호출(리포트 파이프라인 훅).
    챔피언 + 하위 10% 독성 로직의 전조를 Bitget 자체 DB에 박제한다. 항상 안전 폴백.
    """
    out: Dict[str, Any] = {"captured": 0, "toxic": 0, "skipped": True}
    cfg = _load_cfg(sys_config)
    if not _is_enabled(cfg):
        return out

    try:
        if not ensure_genesis_schema():
            return out
        conn_ro = _ro_conn()
        conn_rw = _rw_conn()
        if conn_ro is None or conn_rw is None:
            if conn_ro:
                conn_ro.close()
            if conn_rw:
                conn_rw.close()
            return out
        out["skipped"] = False
        crowned = utc_date_key()
        mk = str(market).lower()
        try:
            champ = getattr(br, "champion", None)
            if champ is not None:
                label = getattr(champ, "group_key", None) or getattr(champ, "label", "")
                built = _build_precursor(conn_ro, cfg, mk, label, "champion")
                if built and _insert_genesis(
                    conn_rw,
                    market=mk,
                    label=label,
                    kind="champion",
                    built=built,
                    crowned_date=crowned,
                    composite=_safe_float(getattr(champ, "composite_score", 0.0)),
                ):
                    out["captured"] += 1

            arms = [a for a in (getattr(br, "arms", []) or []) if getattr(a, "rank", 999) < 999]
            if arms:
                arms_sorted = sorted(arms, key=lambda a: _safe_float(getattr(a, "composite_score", 0.0)))
                k = max(1, int(len(arms_sorted) * TOXIC_BOTTOM_FRAC))
                for a in arms_sorted[:k]:
                    mret = _safe_float(getattr(a, "mean_ret", 0.0))
                    if mret >= 0 and not bool(getattr(a, "below_floor", False)):
                        continue
                    label = getattr(a, "group_key", None) or getattr(a, "label", "")
                    built = _build_precursor(conn_ro, cfg, mk, label, "toxic")
                    if built and _insert_genesis(
                        conn_rw,
                        market=mk,
                        label=label,
                        kind="toxic",
                        built=built,
                        crowned_date=crowned,
                        composite=_safe_float(getattr(a, "composite_score", 0.0)),
                    ):
                        out["toxic"] += 1
            conn_rw.commit()
        finally:
            conn_ro.close()
            conn_rw.close()
    except Exception as ex:
        out["error"] = str(ex)[:120]
    return out


# ---------------------------------------------------------------------------
# 2) 선행 예측 (마할라노비스 + DTW) → GENESIS_PRECURSOR_ADVANTAGE_BG_<MKT>
# ---------------------------------------------------------------------------
def _confirmed_precursors(conn_ro: sqlite3.Connection, market: str) -> List[sqlite3.Row]:
    try:
        cur = conn_ro.execute(
            f"""
            SELECT champion_label, precursor_json, champion_composite,
                   confidence, decay_count
            FROM {GENESIS_TABLE}
            WHERE market = ? AND kind = 'champion' AND status = 'confirmed'
              AND confidence >= ?
            ORDER BY confidence DESC, id DESC
            LIMIT ?
            """,
            (str(market).lower(), CONF_FLOOR, DAILY_CONFIRMED_CAP),
        )
        return cur.fetchall()
    except sqlite3.Error:
        return []


def run_precursor_prediction(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    market: str = "spot",
) -> Dict[str, Any]:
    """현재 코인 국면벡터 vs confirmed 전조 유사도 비교. 임계 초과 시 어드밴티지 플래그 ON."""
    out: Dict[str, Any] = {"active": False, "best_sim": 0.0, "matches": 0}
    cfg = _load_cfg(sys_config)
    if not _is_enabled(cfg):
        return out

    try:
        import numpy as np

        from regime_analog_engine import dtw_distance, mahalanobis_distance, _sim_from_dist

        if not ensure_genesis_schema():
            return out
        conn_ro = _ro_conn()
        if conn_ro is None:
            return out
        try:
            confirmed = _confirmed_precursors(conn_ro, market)
        finally:
            conn_ro.close()
        out["matches"] = len(confirmed)
        if not confirmed:
            return out

        x = np.asarray(build_current_coin_regime_vector(cfg)["vector"], dtype=float)
        history = _vector_history_arrays(cfg)
        inv_cov, _mode = _estimate_inv_cov_bg(history)
        cur_series = np.asarray(
            [_regime_index(v) for v in history[-14:]] + [_regime_index(list(x))],
            dtype=float,
        )
        dtw_ok = cur_series.size >= 5

        threshold = _safe_float(cfg.get(SIM_THRESHOLD_KEY), DEFAULT_SIM_THRESHOLD)

        best_sim, best_label, best_sector, best_conf = -1.0, None, None, 1.0
        best_raw_sim = 0.0
        for row in confirmed:
            try:
                pre = json.loads(row["precursor_json"])
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            env = pre.get("env", {}) if isinstance(pre, dict) else {}
            mu = env.get("regime_centroid")
            if not (isinstance(mu, list) and len(mu) == _N_DIMS):
                continue
            conf = _safe_float(row["confidence"], 1.0)
            if conf < CONF_FLOOR:
                continue
            decay_n = int(row["decay_count"] or 0)
            eff_conf = (decay_n * conf + CONF_PRIOR_STRENGTH * 1.0) / (decay_n + CONF_PRIOR_STRENGTH)

            maha = _sim_from_dist(
                mahalanobis_distance(x, np.asarray(mu, dtype=float), inv_cov), _MAHA_SCALE
            )
            traj = env.get("regime_trajectory")
            if dtw_ok and isinstance(traj, list) and len(traj) >= 5:
                dtw = _sim_from_dist(
                    dtw_distance(cur_series, np.asarray(traj, dtype=float)), _DTW_SCALE
                )
                raw_sim = W_MAHALANOBIS * maha + W_DTW * dtw
            else:
                raw_sim = maha
            sim = raw_sim * eff_conf
            if sim > best_sim:
                best_sim = sim
                best_raw_sim = raw_sim
                best_conf = conf
                best_label = row["champion_label"]
                best_sector = (
                    (pre.get("rotation", {}) or {}).get("sector") if isinstance(pre, dict) else None
                )

        out["best_sim"] = round(float(best_sim), 4)
        out["best_raw_sim"] = round(float(best_raw_sim), 4)
        out["best_confidence"] = round(float(best_conf), 4)
        out["best_label"] = best_label
        out["best_sector"] = best_sector

        active = best_sim >= threshold and best_sector not in (None, "", "OTHER")
        out["active"] = bool(active)

        sectors = [best_sector] if active and best_sector else []
        _set_cfg(
            _advantage_key(market),
            {
                "active": bool(active),
                "sectors": sectors,
                "boost": DEFAULT_KELLY_BOOST,
                "best_sim": out["best_sim"],
                "best_confidence": out["best_confidence"],
                "best_label": best_label,
                "threshold": round(threshold, 4),
                "market": str(market).lower(),
                "updated_at": _now(),
            },
        )

        if active:
            conn_rw = _rw_conn()
            if conn_rw is not None:
                try:
                    conn_rw.execute(
                        f"""
                        INSERT INTO {PREDICTION_TABLE} (
                            predict_date, market, matched_champion_label, matched_sector,
                            similarity, threshold_used, horizon_days, hit, realized_ret, recorded_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            utc_date_key(),
                            str(market).lower(),
                            best_label,
                            best_sector,
                            out["best_sim"],
                            round(threshold, 4),
                            HORIZON_DAYS,
                            None,
                            None,
                            _now(),
                        ),
                    )
                    conn_rw.commit()
                except sqlite3.Error:
                    pass
                finally:
                    conn_rw.close()
    except Exception as ex:
        out["error"] = str(ex)[:120]
    return out


# ---------------------------------------------------------------------------
# 3) 사후 인과 검증 + 유사도 임계값 자가 학습
# ---------------------------------------------------------------------------
def _realized_after(conn_ro: sqlite3.Connection, market: str, label: str, after: str) -> Optional[float]:
    rows = _fetch_label_trades(conn_ro, market, label)
    rets = [
        _safe_float(r.get("final_ret"))
        for r in rows
        if str(r.get("exit_date") or "")[:10] > after and r.get("final_ret") is not None
    ]
    if not rets:
        return None
    return round(sum(rets) / len(rets), 4)


def backfill_and_learn(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    market: str = "spot",
) -> Dict[str, Any]:
    """(a) pending 전조 인과 검증 + confidence 감가/회복. (b) 예측 적중 검증 + 임계값 EMA 자가학습."""
    out = {"resolved": 0, "predictions_resolved": 0, "new_threshold": None}
    cfg = _load_cfg(sys_config)
    if not _is_enabled(cfg):
        return out
    try:
        if not ensure_genesis_schema():
            return out
        conn_ro = _ro_conn()
        conn_rw = _rw_conn()
        if conn_ro is None or conn_rw is None:
            if conn_ro:
                conn_ro.close()
            if conn_rw:
                conn_rw.close()
            return out
        mk = str(market).lower()
        cutoff = utc_date_days_ago_str(HORIZON_DAYS)
        from bitget.infra.bounded_reads import (
            genesis_pending_champions_sql,
            genesis_unresolved_predictions_sql,
        )

        try:
            pend_sql, pend_params = genesis_pending_champions_sql(market=mk, cutoff=cutoff)
            cur = conn_ro.execute(pend_sql, pend_params)
            pend = cur.fetchall()
            for row in pend:
                rret = _realized_after(conn_ro, mk, row["champion_label"], str(row["crowned_date"]))
                if rret is None:
                    continue
                new_status = "confirmed" if rret > 0 else "failed"
                conf = _safe_float(row["confidence"], 1.0)
                dcnt = int(row["decay_count"] or 0)
                if new_status == "failed":
                    conf = max(CONF_FLOOR, conf * CONF_DECAY_FAIL)
                    dcnt += 1
                else:
                    conf = min(1.0, conf * CONF_RECOVER)
                conn_rw.execute(
                    f"""UPDATE {GENESIS_TABLE}
                        SET realized_fwd_ret=?, status=?, confidence=?, decay_count=?, resolved_at=?
                        WHERE id=?""",
                    (rret, new_status, round(conf, 4), dcnt, _now(), row["id"]),
                )
                out["resolved"] += 1

            pred_sql, pred_params = genesis_unresolved_predictions_sql(market=mk, cutoff=cutoff)
            cur2 = conn_ro.execute(pred_sql, pred_params)
            preds = cur2.fetchall()
            for p in preds:
                rret = _realized_after(conn_ro, mk, p["matched_champion_label"], str(p["predict_date"]))
                hit = 1 if (rret is not None and rret > 0) else 0
                conn_rw.execute(
                    f"UPDATE {PREDICTION_TABLE} SET hit=?, realized_ret=?, resolved_at=? WHERE id=?",
                    (hit, rret, _now(), p["id"]),
                )
                lbl = p["matched_champion_label"]
                if lbl:
                    crow = conn_ro.execute(
                        f"SELECT id, confidence, decay_count FROM {GENESIS_TABLE} "
                        f"WHERE market=? AND champion_label=? AND kind='champion' AND status='confirmed' "
                        f"ORDER BY id DESC LIMIT 1",
                        (mk, lbl),
                    ).fetchone()
                    if crow is not None:
                        c_conf = _safe_float(crow["confidence"], 1.0)
                        c_dcnt = int(crow["decay_count"] or 0)
                        if hit == 0:
                            c_conf = max(CONF_FLOOR, c_conf * CONF_DECAY_PRED_MISS)
                            c_dcnt += 1
                        else:
                            c_conf = min(1.0, c_conf * CONF_RECOVER)
                        conn_rw.execute(
                            f"UPDATE {GENESIS_TABLE} SET confidence=?, decay_count=? WHERE id=?",
                            (round(c_conf, 4), c_dcnt, crow["id"]),
                        )
                out["predictions_resolved"] += 1
            conn_rw.commit()

            cur3 = conn_ro.execute(
                f"SELECT hit FROM {PREDICTION_TABLE} WHERE market=? AND hit IS NOT NULL "
                f"ORDER BY id DESC LIMIT 40",
                (mk,),
            )
            hits = [int(r[0]) for r in cur3.fetchall()]
            if len(hits) >= 8:
                hit_rate = sum(hits) / len(hits)
                cur_thr = _safe_float(cfg.get(SIM_THRESHOLD_KEY), DEFAULT_SIM_THRESHOLD)
                if hit_rate > 0.6:
                    target = cur_thr - 0.03
                elif hit_rate < 0.4:
                    target = cur_thr + 0.05
                else:
                    target = cur_thr
                new_thr = round(min(SIM_CEIL, max(SIM_FLOOR, 0.8 * cur_thr + 0.2 * target)), 4)
                _set_cfg(SIM_THRESHOLD_KEY, new_thr)
                out["new_threshold"] = new_thr
                out["hit_rate"] = round(hit_rate, 3)
        finally:
            conn_ro.close()
            conn_rw.close()
    except Exception as ex:
        out["error"] = str(ex)[:120]
    return out


# ---------------------------------------------------------------------------
# 켈리 브릿지 헬퍼(선택적 미래 확장 — 현재 포지션 사이징 경로에는 미연결)
# ---------------------------------------------------------------------------
def genesis_kelly_boost(sys_config: Dict[str, Any], sector: str, market: str = "spot") -> float:
    """GENESIS_PRECURSOR_ADVANTAGE_BG_<MKT> 활성 + sector 일치일 때만 켈리 가산배수(아니면 1.0)."""
    try:
        adv = sys_config.get(_advantage_key(market))
        if not isinstance(adv, dict) or not adv.get("active"):
            return 1.0
        secs = adv.get("sectors") or []
        if sector and sector in secs:
            return float(adv.get("boost", DEFAULT_KELLY_BOOST))
    except Exception:
        pass
    return 1.0


# ---------------------------------------------------------------------------
# [평일 경량 레이더] + 리포트 블록
# ---------------------------------------------------------------------------
def run_daily_genesis_radar(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    market: str = "spot",
) -> Dict[str, Any]:
    """후행 PnL/전수 백필 없음 — 예측(run_precursor_prediction)만 수행. 무거운 검증은 주말에만."""
    return run_precursor_prediction(sys_config, market=market)


def _genesis_hit_rate(conn_ro: sqlite3.Connection, market: str) -> Dict[str, Any]:
    out = {"resolved": 0, "hits": 0, "hit_rate": None, "open_alerts": 0}
    try:
        row = conn_ro.execute(
            f"""SELECT COUNT(*) AS n, SUM(CASE WHEN hit=1 THEN 1 ELSE 0 END) AS h
                FROM {PREDICTION_TABLE} WHERE market=? AND hit IS NOT NULL""",
            (str(market).lower(),),
        ).fetchone()
        if row and row["n"]:
            out["resolved"] = int(row["n"])
            out["hits"] = int(row["h"] or 0)
            out["hit_rate"] = round(out["hits"] / out["resolved"], 3)
        op = conn_ro.execute(
            f"SELECT COUNT(*) AS n FROM {PREDICTION_TABLE} WHERE market=? AND hit IS NULL",
            (str(market).lower(),),
        ).fetchone()
        if op:
            out["open_alerts"] = int(op["n"] or 0)
    except sqlite3.Error:
        pass
    return out


def genesis_radar_report_block(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    market: str = "spot",
    market_icon: str = "",
) -> str:
    """[🚀 챔피언 탄생 전조(Genesis Precursor) 레이더] 텔레그램 섹션(시장별)."""
    if not _is_enabled(_load_cfg(sys_config)):
        return ""
    try:
        import html as _html

        cfg = _load_cfg(sys_config)
        mk = str(market).lower()
        adv = cfg.get(_advantage_key(mk))
        if not isinstance(adv, dict):
            adv = {}

        conn_ro = _ro_conn()
        hr = {"resolved": 0, "hits": 0, "hit_rate": None, "open_alerts": 0}
        if conn_ro is not None:
            try:
                hr = _genesis_hit_rate(conn_ro, mk)
            finally:
                conn_ro.close()

        icon = market_icon or _MARKET_ICON.get(mk, "🪙")
        lines = [f"{icon} <b>[🚀 챔피언 탄생 전조(Genesis Precursor) 레이더 · {mk.upper()}]</b>"]

        if adv.get("active") and adv.get("sectors"):
            secs = ", ".join(_html.escape(str(s), quote=False) for s in adv.get("sectors", []))
            lines.append(
                f"🟢 <b>예비 챔피언 자산군 감지:</b> <b>{secs}</b>\n"
                f"   ↳ 유사도 <b>{adv.get('best_sim', 0)}</b> "
                f"(신뢰도 {adv.get('best_confidence', 1.0)} · 임계 {adv.get('threshold', DEFAULT_SIM_THRESHOLD)}) "
                f"➔ 해당 자산군 선취매 <b>켈리 ×{adv.get('boost', DEFAULT_KELLY_BOOST)}</b> 가산(참고용)"
            )
        else:
            _bs = adv.get("best_sim", 0.0)
            lines.append(f"⚪ 전조 유사도 미달(best={_bs}) — 관망(켈리 가산 잠금)")

        if hr["hit_rate"] is not None:
            lines.append(
                f"🎯 <b>전조 적중률:</b> {hr['hits']}/{hr['resolved']} "
                f"(<b>{hr['hit_rate']*100:.0f}%</b>) · 검증대기 {hr['open_alerts']}건"
            )
        else:
            lines.append(f"🎯 전조 적중률: 검증 표본 누적 중(대기 {hr['open_alerts']}건)")

        return "\n".join(lines) + "\n"
    except Exception as ex:
        return f"<i>⚠️ [전조 레이더 · {market}] 스킵: {str(ex)[:64]}</i>\n"
