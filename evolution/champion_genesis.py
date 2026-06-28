"""
Champion Genesis Precursor Engine — 챔피언 전조현상(증조현상) 자율 축적·검증.

설계 철학(읽기 전용 + 신규 테이블만):
  - 데스매치가 '챔피언 로직'을 뽑을 때마다, 그 로직이 돈을 벌기 시작한 점화일(T0)을
    forward_trades 에서 역추적하고, 그 **직전 30일** 동안 우리 내부 환경
    (REGIME_VECTOR_HISTORY 국면벡터 + forward_trades 섹터/수급/태동 지문)을
    JSON 전조 벡터로 박제한다(status='pending').
  - (양방향) 계좌를 녹인 하위 10% '독성 로직'도 동일하게 붕괴 직전 30일 환경을
    status='toxic' 으로 박제한다(나쁜 전조 라이브러리).
  - 매일 현재 국면벡터를 과거 confirmed 전조들과 **마할라노비스+DTW**(regime_analog_engine
    수식 재활용)로 비교해, 우수 챔피언 전조와 고유사 시 GENESIS_PRECURSOR_ADVANTAGE 플래그를
    켜서 해당 섹터 초기 진입 켈리를 소폭 가산한다(증명된 것만 베팅↑).
  - 챔피언 등극 후 실수익(realized_fwd_ret)을 후속 배치로 채우고(confirmed/failed),
    예측 적중률에 따라 유사도 임계값(GENESIS_SIMILARITY_THRESHOLD)을 EMA 로 자가 학습한다.

❗ 비훼손 원칙
  - 5-Factor 앙상블/데스매치/NAV 경로 **불간섭**. 읽기는 RO URI, 쓰기는 신규 2개 테이블/플래그뿐.
  - 모든 공개 함수는 예외를 삼키고 안전 폴백한다(리포트 파이프라인에 부하/락/예외 전파 0).
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# 상수 / config 키
# ---------------------------------------------------------------------------
GENESIS_TABLE = "champion_precursor_genesis"
PREDICTION_TABLE = "precursor_prediction_log"

ADVANTAGE_KEY = "GENESIS_PRECURSOR_ADVANTAGE"      # dict: active/sectors/boost/... (시장별 접미사)
SIM_THRESHOLD_KEY = "GENESIS_SIMILARITY_THRESHOLD"  # float, 자가 학습
ENABLED_KEY = "GENESIS_PRECURSOR_ENABLED"

DAILY_CONFIRMED_CAP = 80    # 평일 경량 레이더: confirmed 전조 스캔 상한(런타임 병목 차단)


def _advantage_key(market: str) -> str:
    """KR/US 독립 어드밴티지 플래그(시장 간 섹터 오염 방지)."""
    return f"{ADVANTAGE_KEY}_{str(market).upper()}"

LOOKBACK_DAYS = 30            # 전조 윈도 기본값(동적 윈도 폴백)
HORIZON_DAYS = 30            # 등극 후 실수익/예측 검증 지평
DEFAULT_SIM_THRESHOLD = 0.80  # 선행 예측 활성 최소 유사도(자가 학습 기준점)
SIM_FLOOR, SIM_CEIL = 0.70, 0.95
TOXIC_BOTTOM_FRAC = 0.10      # 하위 10% = 독성
DEFAULT_KELLY_BOOST = 1.20    # 전조 적중 섹터 초기 진입 켈리 가산
MIN_WINDOW_POINTS = 8         # 환경 벡터 최소 표본(아니면 status='thin')

# ── [고도화 1] 유동적 T0(Fluid Ignition) — Genesis Score 합산식 가중치 ──
GS_WEIGHTS = {
    "ret_adv": 0.40,    # 데스매치/누적수익 우위 전환점
    "venergy_z": 0.30,  # v_energy 60일 Z-score 이상치
    "sector_dom": 0.30,  # 섹터 주도력 변화
}
GS_THRESHOLD_K = 0.5     # 동적 임계 = mean + K*std (Soft)
VENERGY_LOOKBACK = 60    # v_energy Z-score 기준 트레일링 일수

# ── [고도화 2] 국면 연동 동적 윈도(Dynamic Window) 범위 ──
WINDOW_MIN_DAYS = 10     # 고변동/위기: 짧고 민첩
WINDOW_MAX_DAYS = 45     # 횡보/상승: 길게 매집 관측

# ── [고도화 3] 신뢰도 감가상각(Confidence Decay / Bayesian Shrinkage) ──
CONF_DECAY_FAIL = 0.5    # 사후검증 failed → 신뢰도 ×0.5(기하급수 감소)
CONF_DECAY_PRED_MISS = 0.6  # 예측 빗나감(hit=0) → 매칭 전조 신뢰도 ×0.6
CONF_RECOVER = 1.15      # confirmed/적중 → 신뢰도 ×1.15(상한 1.0)
CONF_FLOOR = 0.05        # 이 밑이면 예측 매칭에서 사실상 무시
CONF_PRIOR_STRENGTH = 4.0  # 베이지안 수축 prior 강도(표본 적을수록 1.0으로 수축)

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
    confidence REAL NOT NULL DEFAULT 1.0,     -- [고도화3] 예측 영향력(감가상각)
    decay_count INTEGER NOT NULL DEFAULT 0,   -- 감가 횟수(투명성)
    created_at TEXT,
    resolved_at TEXT,
    UNIQUE(market, champion_label, ignition_date, kind)
);
CREATE INDEX IF NOT EXISTS idx_genesis_status ON {GENESIS_TABLE}(status, kind);

CREATE TABLE IF NOT EXISTS {PREDICTION_TABLE} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    predict_date TEXT NOT NULL,
    market TEXT NOT NULL,
    matched_champion_label TEXT,
    matched_sector TEXT,
    similarity REAL,
    threshold_used REAL,
    horizon_days INTEGER DEFAULT {HORIZON_DAYS},
    hit INTEGER,                 -- NULL=미검증, 1=적중, 0=실패
    realized_ret REAL,
    recorded_at TEXT,
    resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_pred_open ON {PREDICTION_TABLE}(hit, predict_date);
"""


# ---------------------------------------------------------------------------
# DB 헬퍼 — 읽기는 RO URI, 쓰기는 메인 DB 짧은 트랜잭션(WAL)
# ---------------------------------------------------------------------------
def _write_db_path() -> str:
    try:
        from market_db_paths import MARKET_DATA_DB_PATH

        return MARKET_DATA_DB_PATH
    except Exception:
        return ""


def _read_db_path() -> str:
    """리포트와 동일하게 항상 메인 DB(최신) 우선."""
    try:
        from market_db_paths import report_db_read_path

        return report_db_read_path()
    except Exception:
        return _write_db_path()


def _ro_conn() -> Optional[sqlite3.Connection]:
    path = _read_db_path()
    if not path or not os.path.isfile(path):
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=20)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def _rw_conn() -> Optional[sqlite3.Connection]:
    path = _write_db_path()
    if not path or not os.path.isfile(path):
        return None
    try:
        conn = sqlite3.connect(path, timeout=30)
        conn.row_factory = sqlite3.Row
        # WAL + 짧은 busy timeout: 기존 리더/라이터와 충돌 시 빠르게 양보.
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=8000;")
        return conn
    except sqlite3.Error:
        return None


def _migrate_genesis_columns(conn: sqlite3.Connection) -> None:
    """기존 테이블에 신규 컬럼(confidence/decay_count) 안전 추가."""
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
# config 헬퍼 (lazy)
# ---------------------------------------------------------------------------
def _load_cfg(cfg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(cfg, dict):
        return cfg
    try:
        from config_manager import load_system_config

        return load_system_config() or {}
    except Exception:
        return {}


def _set_cfg(key: str, value: Any) -> None:
    try:
        from config_manager import set_config_value

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
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
# 국면 인덱스 / 환경 벡터 윈도 집계 (regime_analog_engine 차원 호환)
# ---------------------------------------------------------------------------
def _regime_index(vec: List[float]) -> float:
    """regime_analog_engine._current_regime_index_series 와 동일 가중식."""
    try:
        return (
            5.0 * (float(vec[0]) + float(vec[1]))
            + 0.5 * float(vec[3])
            + 0.5 * float(vec[4])
            - 0.3 * float(vec[2])
        )
    except (IndexError, TypeError, ValueError):
        return 0.0


def _vector_history(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = cfg.get("REGIME_VECTOR_HISTORY")
    out: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict) and isinstance(item.get("vector"), list):
                out.append(item)
    return out


def _window_env_features(
    cfg: Dict[str, Any], t0: datetime, lookback: int = LOOKBACK_DAYS
) -> Dict[str, Any]:
    """[T0-lookback, T0] 구간 REGIME_VECTOR_HISTORY 평균/기울기/궤적."""
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
        if isinstance(vec, list) and len(vec) == 6:
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
        # 기울기 = (후반 1/3 평균 - 전반 1/3 평균)
        k = max(1, n // 3)
        slope = [round(float(x), 6) for x in (mat[-k:].mean(axis=0) - mat[:k].mean(axis=0))]
    else:
        cols = list(zip(*rows))
        mean_vec = [round(sum(c) / n, 6) for c in cols]
        k = max(1, n // 3)
        slope = [
            round(sum(c[-k:]) / k - sum(c[:k]) / k, 6) for c in cols
        ]

    trajectory = [round(_regime_index(r), 6) for r in rows]
    return {
        "n_points": n,
        "regime_vec_mean": mean_vec,
        "regime_vec_slope": slope,
        "regime_centroid": mean_vec,         # 마할라노비스 mu
        "regime_trajectory": trajectory,     # DTW 시퀀스
        "pri_z_trend": round(slope[3], 6),
    }


# ---------------------------------------------------------------------------
# forward_trades 윈도 지문 (섹터/수급/태동) — RO 읽기, group_key 매핑
# ---------------------------------------------------------------------------
def _group_key(sig: str) -> str:
    try:
        from evolution.deathmatch_battle_royale import ledger_group_key

        return ledger_group_key(sig)
    except Exception:
        return str(sig or "UNKNOWN")


def _fetch_label_trades(
    conn: sqlite3.Connection, market: str, label: str
) -> List[sqlite3.Row]:
    """해당 group_key(label)에 매핑되는 CLOSED 거래(시간순)."""
    try:
        cur = conn.execute(
            """
            SELECT entry_date, exit_date, sector, sig_type, status, final_ret,
                   dyn_cpv, dyn_tb, v_energy, entry_breadth, flow_tags
            FROM forward_trades
            WHERE market = ? AND status LIKE 'CLOSED%'
            ORDER BY entry_date ASC
            """,
            (str(market).upper(),),
        )
        rows = cur.fetchall()
    except sqlite3.Error:
        return []
    return [r for r in rows if _group_key(r["sig_type"]) == label]


def _logistic(x: float, scale: float = 1.0) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-float(x) / max(scale, 1e-6)))
    except (OverflowError, ValueError):
        return 0.0 if x < 0 else 1.0


def _trace_ignition_simple(rows: List[sqlite3.Row], kind: str) -> Optional[datetime]:
    """폴백: 표본 빈약 시 첫 수익(champion)/첫 손실(toxic) 진입일."""
    dated = [(r, _parse_date(r["entry_date"])) for r in rows]
    dated = [(r, d) for (r, d) in dated if d is not None]
    if not dated:
        return None
    dated.sort(key=lambda x: x[1])
    if kind == "champion":
        for r, d in dated:
            if _safe_float(r["final_ret"]) > 0:
                return d
        return dated[0][1]
    for r, d in dated:
        if _safe_float(r["final_ret"]) < 0:
            return d
    return dated[0][1]


def _fetch_market_trades(conn: sqlite3.Connection, market: str) -> List[Dict[str, Any]]:
    """v_energy Z-score·섹터 주도력 기준 산정을 위한 시장 전체 CLOSED 거래(경량)."""
    try:
        cur = conn.execute(
            """
            SELECT entry_date, sector, v_energy FROM forward_trades
            WHERE market = ? AND status LIKE 'CLOSED%' AND entry_date IS NOT NULL
            ORDER BY entry_date ASC
            """,
            (str(market).upper(),),
        )
        rows = cur.fetchall()
    except sqlite3.Error:
        return []
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = _parse_date(r["entry_date"])
        if d is None:
            continue
        out.append({"d": d, "sector": str(r["sector"] or "").strip() or "기타/혼합",
                    "ve": _safe_float(r["v_energy"])})
    return out


def _fetch_arm_snapshot_series(
    conn: sqlite3.Connection, market: str, label: str
) -> List[Tuple[datetime, float]]:
    """deathmatch_arm_snapshot 의 composite_score 시계열(수익률 우위 전환점)."""
    try:
        cur = conn.execute(
            """
            SELECT trade_date, composite_score, mean_ret FROM deathmatch_arm_snapshot
            WHERE market = ? AND (label = ? OR arm_id = ?)
            ORDER BY trade_date ASC
            """,
            (str(market).upper(), label, label),
        )
        rows = cur.fetchall()
    except sqlite3.Error:
        return []
    out: List[Tuple[datetime, float]] = []
    for r in rows:
        d = _parse_date(r["trade_date"])
        if d is None:
            continue
        score = r["composite_score"]
        if score is None:
            score = r["mean_ret"]
        out.append((d, _safe_float(score)))
    return out


def _fluid_ignition(
    conn: sqlite3.Connection,
    market: str,
    label: str,
    rows: List[sqlite3.Row],
    kind: str,
) -> Tuple[Optional[datetime], Dict[str, Any]]:
    """
    [고도화1] 다중 앙상블 유동적 T0(Fluid Ignition).
      여러 변수를 0~1 정규화·가중합한 'Genesis Score' 시계열을 만들고,
      동적 임계(mean + K*std)를 처음 돌파하는 날을 T0 로 Soft 추정한다.
      구성 변수:
        · ret_adv    : 누적 수익 우위(+데스매치 composite 전환점)
        · venergy_z  : 당일 v_energy 의 60일 트레일링 Z-score(이상치)
        · sector_dom : 직전 15일 시장 내 해당 섹터 주도력(자금 쏠림)
      champion 은 '상승 점화', toxic 은 '붕괴 점화'(수익 부호 반전)로 본다.
    """
    dated = [(r, _parse_date(r["entry_date"])) for r in rows]
    dated = [(r, d) for (r, d) in dated if d is not None]
    if len(dated) < 3:
        return _trace_ignition_simple(rows, kind), {"method": "fallback_sparse"}
    dated.sort(key=lambda x: x[1])

    sign = 1.0 if kind == "champion" else -1.0
    market_trades = _fetch_market_trades(conn, market)
    arm_series = _fetch_arm_snapshot_series(conn, market, label)

    # 라벨 대표 섹터(최빈)
    sec_counts: Dict[str, int] = {}
    for r, _d in dated:
        s = str(r["sector"] or "").strip() or "기타/혼합"
        sec_counts[s] = sec_counts.get(s, 0) + 1
    label_sector = max(sec_counts.items(), key=lambda kv: kv[1])[0] if sec_counts else "기타/혼합"

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

    # 후보일별 원시 구성값 수집
    cum_sum, cum_n = 0.0, 0
    raw: List[Dict[str, Any]] = []
    for r, d in dated:
        cum_sum += _safe_float(r["final_ret"])
        cum_n += 1
        cum_avg = cum_sum / cum_n
        ret_signal = sign * cum_avg
        arm = _arm_adv(d)
        if arm is not None:
            ret_signal = 0.5 * ret_signal + 0.5 * (sign * arm)
        raw.append({
            "d": d,
            "ret": ret_signal,
            "vez": _venergy_z(d, _safe_float(r["v_energy"])),
            "sec": _sector_dom(d),
        })

    # 0~1 정규화(컴포넌트별 가용분만, 가중치 재정규화)
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

    # Soft: 임계 첫 돌파일. 없으면 최고점일.
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
    """
    [고도화2] 국면 연동 동적 전조 윈도.
      점화 당시 VIX 수준(REGIME_VECTOR_HISTORY 의 vix_z, T0 최근접) + CURRENT_REGIME_KEY 로
      변동성↑(위기/하락)면 10~15일 짧게, 횡보/상승이면 30~45일 길게 매집 관측.
    """
    # T0 최근접 국면벡터의 vix_z(=vector[2])
    vix_z = 0.0
    best_gap = None
    for item in _vector_history(cfg):
        ts = _parse_date(item.get("ts"))
        vec = item.get("vector")
        if ts is None or not (isinstance(vec, list) and len(vec) == 6):
            continue
        gap = abs((ts - t0).days)
        if best_gap is None or gap < best_gap:
            best_gap = gap
            vix_z = _safe_float(vec[2])

    regime = str(cfg.get("CURRENT_REGIME_KEY", "") or "").upper()
    crisis = any(k in regime for k in ("BEAR", "HIGH_VOL", "CRISIS", "PANIC", "DOWN"))
    calm = any(k in regime for k in ("BULL", "SIDEWAYS", "UP", "CHOP"))

    # 연속식: VIX 높을수록 윈도 단축(tanh 로 부드럽게)
    window = 30.0 - 18.0 * math.tanh(vix_z)
    if crisis:
        window = min(window, 15.0)   # 위기: 민첩
    elif calm and vix_z < 0.5:
        window = max(window, 35.0)   # 평온 상승/횡보: 길게 관측
    window_days = int(max(WINDOW_MIN_DAYS, min(WINDOW_MAX_DAYS, round(window))))
    return window_days, {
        "vix_z": round(float(vix_z), 4),
        "regime": regime or "UNKNOWN",
        "window_days": window_days,
    }


def _window_footprint(
    rows: List[sqlite3.Row], t0: datetime, lookback: int = LOOKBACK_DAYS
) -> Dict[str, Any]:
    """[T0-lookback, T0] 윈도 내 섹터/수급/로직 태동 지문."""
    lo = t0 - timedelta(days=lookback)
    win = [
        r for r in rows
        if (_parse_date(r["entry_date"]) is not None
            and lo <= _parse_date(r["entry_date"]) <= t0)  # type: ignore[operator]
    ]
    n = len(win)
    if n == 0:
        return {"n_trades": 0}

    # 섹터 주도(최빈)
    sec_counts: Dict[str, int] = {}
    for r in win:
        s = str(r["sector"] or "").strip() or "기타/혼합"
        sec_counts[s] = sec_counts.get(s, 0) + 1
    top_sector, top_n = max(sec_counts.items(), key=lambda kv: kv[1])

    def _avg(col: str) -> float:
        vals = [_safe_float(r[col]) for r in win if r[col] is not None]
        return round(sum(vals) / len(vals), 4) if vals else 0.0

    # 로직 태동 흔적(관측/스카웃/인큐베이터 태그)
    scout = 0
    for r in win:
        tag = (str(r["sig_type"] or "") + " " + str(r["flow_tags"] or "")).upper()
        if any(k in tag for k in ("OBSERVE", "SCOUT", "INCUBATOR", "관망", "스카웃")):
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
    # [고도화1] 유동적 T0(Genesis Score 앙상블)
    t0, ign_meta = _fluid_ignition(conn_ro, market, label, rows, kind)
    if t0 is None:
        return None

    # [고도화2] 국면 연동 동적 윈도
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
                str(market).upper(),
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
    market: str = "KR",
) -> Dict[str, Any]:
    """
    데스매치 1사이클 종료 후 호출(리포트 파이프라인 훅).
    챔피언 + 하위 10% 독성 로직의 전조를 박제한다. 항상 안전 폴백.
    """
    out = {"captured": 0, "toxic": 0, "skipped": True}
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
        crowned = datetime.now().strftime("%Y-%m-%d")
        mk = str(market).upper()
        try:
            # --- 챔피언 ---
            champ = getattr(br, "champion", None)
            if champ is not None:
                label = getattr(champ, "group_key", None) or getattr(champ, "label", "")
                built = _build_precursor(conn_ro, cfg, mk, label, "champion")
                if built and _insert_genesis(
                    conn_rw, market=mk, label=label, kind="champion",
                    built=built, crowned_date=crowned,
                    composite=_safe_float(getattr(champ, "composite_score", 0.0)),
                ):
                    out["captured"] += 1

            # --- 독성(하위 10%) ---
            arms = [a for a in (getattr(br, "arms", []) or []) if getattr(a, "rank", 999) < 999]
            if arms:
                arms_sorted = sorted(arms, key=lambda a: _safe_float(getattr(a, "composite_score", 0.0)))
                k = max(1, int(len(arms_sorted) * TOXIC_BOTTOM_FRAC))
                for a in arms_sorted[:k]:
                    # 진짜 계좌를 녹인 케이스만(평균수익<0 또는 floor 미달)
                    mret = _safe_float(getattr(a, "mean_ret", 0.0))
                    if mret >= 0 and not bool(getattr(a, "below_floor", False)):
                        continue
                    label = getattr(a, "group_key", None) or getattr(a, "label", "")
                    built = _build_precursor(conn_ro, cfg, mk, label, "toxic")
                    if built and _insert_genesis(
                        conn_rw, market=mk, label=label, kind="toxic",
                        built=built, crowned_date=crowned,
                        composite=_safe_float(getattr(a, "composite_score", 0.0)),
                    ):
                        out["toxic"] += 1
            conn_rw.commit()
        finally:
            conn_ro.close()
            conn_rw.close()
    except Exception as ex:  # 절대 상위로 전파 금지
        out["error"] = str(ex)[:120]
    return out


# ---------------------------------------------------------------------------
# 2) 선행 예측 (마할라노비스 + DTW 재활용) → GENESIS_PRECURSOR_ADVANTAGE
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
            (str(market).upper(), CONF_FLOOR, DAILY_CONFIRMED_CAP),
        )
        return cur.fetchall()
    except sqlite3.Error:
        return []


def run_precursor_prediction(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    market: str = "KR",
) -> Dict[str, Any]:
    """
    현재 국면벡터 vs confirmed 전조 유사도 비교. 임계 초과 시 어드밴티지 플래그 ON.
    regime_analog_engine 의 마할라노비스/DTW/공분산 수식을 그대로 재활용한다.
    """
    out: Dict[str, Any] = {"active": False, "best_sim": 0.0, "matches": 0}
    cfg = _load_cfg(sys_config)
    if not _is_enabled(cfg):
        return out

    try:
        import numpy as np
        from regime_analog_engine import (
            W_DTW, W_MAHALANOBIS, _DTW_SCALE, _MAHA_SCALE,
            _estimate_inv_cov, _sim_from_dist, build_current_regime_vector,
            dtw_distance, mahalanobis_distance,
        )

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

        # 현재 벡터/공분산/궤적
        x = np.asarray(build_current_regime_vector(cfg)["vector"], dtype=float)
        history = [
            it["vector"] for it in _vector_history(cfg)
            if isinstance(it.get("vector"), list) and len(it["vector"]) == 6
        ]
        inv_cov, _mode = _estimate_inv_cov(history)
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
            if not (isinstance(mu, list) and len(mu) == 6):
                continue
            # [고도화3] 신뢰도 감가상각 + 베이지안 수축: 영향력이 바닥난 전조는 제외
            conf = _safe_float(row["confidence"], 1.0)
            if conf < CONF_FLOOR:
                continue
            decay_n = int(row["decay_count"] or 0)
            # 증거(decay_count)가 적으면 prior(1.0)로 수축, 많을수록 실측 confidence 반영
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
            sim = raw_sim * eff_conf  # 신뢰도 가중(기하급수 감가 반영)
            if sim > best_sim:
                best_sim = sim
                best_raw_sim = raw_sim
                best_conf = conf
                best_label = row["champion_label"]
                best_sector = (
                    (pre.get("rotation", {}) or {}).get("sector")
                    if isinstance(pre, dict) else None
                )

        out["best_sim"] = round(float(best_sim), 4)
        out["best_raw_sim"] = round(float(best_raw_sim), 4)
        out["best_confidence"] = round(float(best_conf), 4)
        out["best_label"] = best_label
        out["best_sector"] = best_sector

        active = best_sim >= threshold and best_sector not in (None, "", "기타/혼합")
        out["active"] = bool(active)

        # 어드밴티지 플래그 갱신(KR/US 독립 키, 섹터 한정)
        sectors = [best_sector] if active and best_sector else []
        _set_cfg(_advantage_key(market), {
            "active": bool(active),
            "sectors": sectors,
            "boost": DEFAULT_KELLY_BOOST,
            "best_sim": out["best_sim"],
            "best_confidence": out["best_confidence"],
            "best_label": best_label,
            "threshold": round(threshold, 4),
            "market": str(market).upper(),
            "updated_at": _now(),
        })

        # 예측 로그 적재(검증 대기)
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
                            datetime.now().strftime("%Y-%m-%d"),
                            str(market).upper(), best_label, best_sector,
                            out["best_sim"], round(threshold, 4), HORIZON_DAYS,
                            None, None, _now(),
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
    """등극일(after) 이후 청산된 해당 group_key 거래의 평균 실현수익(%)."""
    rows = _fetch_label_trades(conn_ro, market, label)
    rets = [
        _safe_float(r["final_ret"]) for r in rows
        if str(r["exit_date"] or "")[:10] > after and r["final_ret"] is not None
    ]
    if not rets:
        return None
    return round(sum(rets) / len(rets), 4)


def backfill_and_learn(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    market: str = "KR",
) -> Dict[str, Any]:
    """
    (a) pending 전조의 realized_fwd_ret 를 등극 후 horizon 경과분에 한해 채우고 confirmed/failed.
    (b) 예측 로그 적중(hit)을 horizon 경과분에 한해 채우고, 적중률로 임계값 EMA 자가 학습.
    """
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
        mk = str(market).upper()
        cutoff = (datetime.now() - timedelta(days=HORIZON_DAYS)).strftime("%Y-%m-%d")
        try:
            # (a) 전조 인과 검증
            cur = conn_ro.execute(
                f"""
                SELECT id, champion_label, crowned_date, confidence, decay_count
                FROM {GENESIS_TABLE}
                WHERE market = ? AND kind='champion' AND status='pending'
                  AND crowned_date IS NOT NULL AND crowned_date <= ?
                """,
                (mk, cutoff),
            )
            pend = cur.fetchall()
            for row in pend:
                rret = _realized_after(conn_ro, mk, row["champion_label"], str(row["crowned_date"]))
                if rret is None:
                    continue
                new_status = "confirmed" if rret > 0 else "failed"
                # [고도화3] 사후검증 결과로 신뢰도 기하급수 감가/회복(Bayesian Shrinkage)
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

            # (b) 예측 적중 검증
            cur2 = conn_ro.execute(
                f"""
                SELECT id, matched_champion_label, matched_sector, predict_date
                FROM {PREDICTION_TABLE}
                WHERE market=? AND hit IS NULL AND predict_date <= ?
                """,
                (mk, cutoff),
            )
            preds = cur2.fetchall()
            for p in preds:
                rret = _realized_after(conn_ro, mk, p["matched_champion_label"], str(p["predict_date"]))
                hit = 1 if (rret is not None and rret > 0) else 0
                conn_rw.execute(
                    f"UPDATE {PREDICTION_TABLE} SET hit=?, realized_ret=?, resolved_at=? WHERE id=?",
                    (hit, rret, _now(), p["id"]),
                )
                # [고도화3] 예측을 만든 전조에 성과 피드백: 빗나가면 기하급수 감가, 적중이면 회복.
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

            # (b-2) 적중률 기반 임계값 EMA 자가 학습
            cur3 = conn_ro.execute(
                f"SELECT hit FROM {PREDICTION_TABLE} WHERE market=? AND hit IS NOT NULL "
                f"ORDER BY id DESC LIMIT 40",
                (mk,),
            )
            hits = [int(r["hit"]) for r in cur3.fetchall()]
            if len(hits) >= 8:
                hit_rate = sum(hits) / len(hits)
                cur_thr = _safe_float(cfg.get(SIM_THRESHOLD_KEY), DEFAULT_SIM_THRESHOLD)
                # 정확하면(>0.6) 약간 완화해 더 포착, 부정확하면(<0.4) 강화. EMA(α=0.2).
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
# 켈리 브릿지 헬퍼 (forward/shared.py 가 소비)
# ---------------------------------------------------------------------------
def genesis_kelly_boost(sys_config: Dict[str, Any], sector: str, market: str = "KR") -> float:
    """
    GENESIS_PRECURSOR_ADVANTAGE_<MKT> 가 활성 + sector 일치일 때만 켈리 가산배수 반환(아니면 1.0).
    forward/shared.py 진입 켈리 산정부에서 호출(읽기 전용, 예외 안전, KR/US 독립).
    """
    try:
        adv = sys_config.get(_advantage_key(market))
        if not isinstance(adv, dict):  # 레거시 단일 키 폴백
            adv = sys_config.get(ADVANTAGE_KEY)
        if not isinstance(adv, dict) or not adv.get("active"):
            return 1.0
        secs = adv.get("sectors") or []
        if sector and sector in secs:
            return float(adv.get("boost", DEFAULT_KELLY_BOOST))
    except Exception:
        pass
    return 1.0


# ---------------------------------------------------------------------------
# [투트랙] 평일 경량 레이더 + 텔레그램 리포트 블록
# ---------------------------------------------------------------------------
def run_daily_genesis_radar(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    market: str = "KR",
) -> Dict[str, Any]:
    """
    [평일 레이더] 현재 환경벡터 vs confirmed 전조 경량 유사도 스캔.
    후행 PnL/전수 백필 없음 — 예측(run_precursor_prediction)만 수행한다.
    고유사 시 precursor_prediction_log 경보 인서트 + GENESIS_PRECURSOR_ADVANTAGE_<MKT> ON.
    무거운 인과 검증/감가는 주말(backfill_and_learn)에서만 수행.
    """
    return run_precursor_prediction(sys_config, market=market)


def _genesis_hit_rate(conn_ro: sqlite3.Connection, market: str) -> Dict[str, Any]:
    """이미 resolved 된 hit 컬럼만 집계(후행 PnL 재계산 없음 — 평일에도 경량)."""
    out = {"resolved": 0, "hits": 0, "hit_rate": None, "open_alerts": 0}
    try:
        row = conn_ro.execute(
            f"""SELECT COUNT(*) AS n, SUM(CASE WHEN hit=1 THEN 1 ELSE 0 END) AS h
                FROM {PREDICTION_TABLE} WHERE market=? AND hit IS NOT NULL""",
            (str(market).upper(),),
        ).fetchone()
        if row and row["n"]:
            out["resolved"] = int(row["n"])
            out["hits"] = int(row["h"] or 0)
            out["hit_rate"] = round(out["hits"] / out["resolved"], 3)
        op = conn_ro.execute(
            f"SELECT COUNT(*) AS n FROM {PREDICTION_TABLE} WHERE market=? AND hit IS NULL",
            (str(market).upper(),),
        ).fetchone()
        if op:
            out["open_alerts"] = int(op["n"] or 0)
    except sqlite3.Error:
        pass
    return out


def genesis_radar_report_block(
    sys_config: Optional[Dict[str, Any]] = None,
    *,
    market: str = "KR",
    market_icon: str = "",
) -> str:
    """
    [🚀 챔피언 탄생 전조(Genesis Precursor) 레이더] 텔레그램 섹션(시장별).
    읽기 전용 — 평일 레이더가 갱신해 둔 어드밴티지 플래그 + 적중률을 직관 표시.
    """
    if not _is_enabled(_load_cfg(sys_config)):
        return ""
    try:
        import html as _html

        cfg = _load_cfg(sys_config)
        mk = str(market).upper()
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

        icon = market_icon or ("🇰🇷" if mk == "KR" else "🇺🇸")
        lines = [f"{icon} <b>[🚀 챔피언 탄생 전조(Genesis Precursor) 레이더 · {mk}]</b>"]

        if adv.get("active") and adv.get("sectors"):
            secs = ", ".join(_html.escape(str(s), quote=False) for s in adv.get("sectors", []))
            lines.append(
                f"🟢 <b>예비 챔피언 섹터 감지:</b> <b>{secs}</b>\n"
                f"   ↳ 유사도 <b>{adv.get('best_sim', 0)}</b> "
                f"(신뢰도 {adv.get('best_confidence', 1.0)} · 임계 {adv.get('threshold', DEFAULT_SIM_THRESHOLD)}) "
                f"➔ 내일 해당 섹터 선취매 <b>켈리 ×{adv.get('boost', DEFAULT_KELLY_BOOST)}</b> 가산"
            )
        else:
            _bs = adv.get("best_sim", 0.0)
            lines.append(
                f"⚪ 전조 유사도 미달(best={_bs}) — 관망(켈리 가산 잠금)"
            )

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


if __name__ == "__main__":
    # 격리 스모크: 스키마 + 빈 예측/백필 안전 폴백
    print("ensure_schema:", ensure_genesis_schema())
    print("daily_radar:", run_daily_genesis_radar(market="KR"))
    print("report_block:", genesis_radar_report_block(market="KR"))
    print("backfill:", backfill_and_learn(market="KR"))
