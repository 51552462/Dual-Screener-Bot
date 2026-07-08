"""
Mutant OOS Validator — 합성 인큐베이터 챔피언의 실데이터(읽기 전용) 검증 게이트.

- incubator_engine.py / 스크리너 미수정.
- 입력: 동일 디렉터리 mutant_hall_of_fame.json (hall_of_fame)
- 출력: validated_live_mutants.json (합격 전략만)
- DB: market_data.sqlite 는 URI mode=ro + PRAGMA query_only=ON 만 사용.

[Architect · Regime Specialization]
- Hard Block 폐기: BEAR/BLACK_SWAN synthetic fail → Drop 금지.
- Item 2 Regime Tagging: BULL/BEAR/SIDEWAYS 합성 채점 → BULL_ONLY | BEAR_ONLY | ALL_WEATHER.
- 승격 pass = 실데이터 OOS; regime_tag 는 LIVE 레지스트리 메타로 보존(Item 3 MAB 격리 입력).
"""
from __future__ import annotations

import json
import os
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
import requests

from market_db_paths import market_db_read_path

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
HALL_OF_FAME_JSON = os.path.join(_THIS_DIR, "mutant_hall_of_fame.json")
VALIDATED_JSON = os.path.join(_THIS_DIR, "validated_live_mutants.json")
TELEGRAM_ERROR_LOG = os.path.join(_THIS_DIR, "telegram_error_log.txt")
SYNTHETIC_DB = os.path.join(_THIS_DIR, "synthetic_market.sqlite")

# [Mission 1] 361행 NameError 수정: market_db_paths 의 단일 경로 SSOT 를 모듈 상수로 노출.
MARKET_DB = market_db_read_path()

# ---------------------------------------------------------------------------
# Regime Specialization SSOT — Hard Block 폐기 (Architect P0)
# ---------------------------------------------------------------------------
# 합성 BEAR/BLACK_SWAN/방어 MDD 미달로 Mutant Drop 금지. BULL 전문가는 하락장 약점으로 죽지 않음.
REGIME_HARD_BLOCK_ENABLED = False  # SSOT: 항상 False — env 로 재활성화 불가(진화 퇴행 방지).
REGIME_STRESS_BUCKETS: dict[str, frozenset[str]] = {
    "BULL": frozenset({"BULL"}),
    "BEAR": frozenset({"BEAR", "BLACK_SWAN", "HIGH_VOL", "CRASH"}),
    "SIDEWAYS": frozenset({"SIDEWAYS"}),
}
# 레거시 Hard Block 임계(deprecated) — audit 리포트용만, pass 에 미반영.
DEPRECATED_BEAR_MIN_EXCESS_ALPHA = float(
    os.environ.get("OOS_BEAR_MIN_EXCESS_ALPHA", "-0.001") or "-0.001"
)
DEPRECATED_BEAR_MAX_MDD_PCT = float(
    os.environ.get("OOS_BEAR_MAX_MDD_PCT", "-25") or "-25"
)
SYNTHETIC_REGIME_MIN_BARS = 40
SYNTHETIC_REGIME_MAX_TICKERS = 30

# ---------------------------------------------------------------------------
# Regime Specialization Tagging (Item 2)
# ---------------------------------------------------------------------------
REGIME_TAG_MIN_SIGNALS = int(os.environ.get("OOS_REGIME_TAG_MIN_SIGNALS", "15") or "15")
BULL_STRONG_EXCESS_ALPHA = float(os.environ.get("OOS_BULL_STRONG_EXCESS", "0.00025") or "0.00025")
BULL_STRONG_MIN_WIN_RATE = float(os.environ.get("OOS_BULL_STRONG_WR", "0.52") or "0.52")
BEAR_STRONG_EXCESS_ALPHA = float(os.environ.get("OOS_BEAR_STRONG_EXCESS", "0.00015") or "0.00015")
BEAR_STRONG_MIN_WIN_RATE = float(os.environ.get("OOS_BEAR_STRONG_WR", "0.51") or "0.51")
BEAR_STRONG_MIN_AVG_RETURN = float(os.environ.get("OOS_BEAR_STRONG_AVG_RET", "0.00005") or "0.00005")
REGIME_WEAK_EXCESS_ALPHA = float(os.environ.get("OOS_REGIME_WEAK_EXCESS", "-0.00005") or "-0.00005")
REGIME_WEAK_MAX_WIN_RATE = float(os.environ.get("OOS_REGIME_WEAK_WR", "0.49") or "0.49")
VALID_REGIME_TAGS = frozenset({"BULL_ONLY", "BEAR_ONLY", "ALL_WEATHER", "UNCLASSIFIED"})

# 최근 약 6개월 영업일(여유)
OOS_MIN_BARS = 130
MAX_TICKERS_SAMPLE = 100
# [Mission 5] 합격 기준 지능화: '실데이터 무조건 드리프트(baseline)' 대비 초과 알파로 게이트.
PROMOTE_MIN_WIN_RATE = 0.50
PROMOTE_MIN_EXCESS_ALPHA = float(os.environ.get("OOS_MIN_EXCESS_ALPHA", "0.00005") or "0.00005")
PROMOTE_MIN_SIGNALS = int(os.environ.get("OOS_MIN_SIGNALS", "30") or "30")


def _atomic_write_json(path: str, obj: Mapping[str, Any]) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _load_dotenv_optional() -> None:
    p = os.path.join(_THIS_DIR, ".env")
    if not os.path.isfile(p):
        return
    try:
        with open(p, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip().strip('"').strip("'")
                if k:
                    os.environ.setdefault(k, v)
    except Exception:
        return


def _telegram_operator_alert(message: str) -> None:
    """로컬 블랙박스 기록 + 콘솔 고가시성 경고 (팩토리 중단 없음)."""
    try:
        with open(TELEGRAM_ERROR_LOG, "a", encoding="utf-8") as f:
            f.write(message.rstrip() + "\n")
    except Exception:
        pass
    print("\n" + "*" * 72)
    print("⚠️  TELEGRAM REPORT NOT DELIVERED — CHECK telegram_error_log.txt  ⚠️")
    print("*" * 72)
    print(message)
    print("*" * 72 + "\n")


def send_telegram_report(text: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        _load_dotenv_optional()
        import telegram_env

        token = telegram_env.get_lab_token()
        chat_id = telegram_env.get_lab_chat_id()
        if not token or not chat_id:
            _telegram_operator_alert(
                f"[{ts}] ERROR: Telegram Token or Chat ID is missing. Message skipped."
            )
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": (text or "")[:4096]}, timeout=25)
    except Exception as e:
        _telegram_operator_alert(
            f"[{ts}] ERROR: Telegram send failed: {type(e).__name__}: {e}"
        )


def _open_market_db_ro() -> Optional[sqlite3.Connection]:
    p = market_db_read_path()
    if not os.path.exists(p):
        return None
    try:
        uri = f"file:{p.replace(chr(92), '/')}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        conn.execute("PRAGMA query_only=ON;")
        return conn
    except Exception:
        return None


def _list_price_tables(conn: sqlite3.Connection) -> list[str]:
    try:
        cur = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND (name LIKE 'KR_%' OR name LIKE 'US_%')
            ORDER BY name
            """
        )
        return [r[0] for r in cur.fetchall() if r and r[0]]
    except Exception:
        return []


def _read_ohlcv_table(conn: sqlite3.Connection, table: str) -> Optional[pd.DataFrame]:
    try:
        cutoff = (datetime.now() - timedelta(days=190)).strftime("%Y-%m-%d")
        q = (
            f'SELECT Date, Open, High, Low, Close, Volume FROM "{table}" '
            f"WHERE Date >= ? ORDER BY Date ASC"
        )
        df = pd.read_sql(q, conn, params=(cutoff,))
    except Exception:
        return None
    if df is None or len(df) < OOS_MIN_BARS:
        try:
            df = pd.read_sql(
                f'SELECT Date, Open, High, Low, Close, Volume FROM "{table}" ORDER BY Date ASC',
                conn,
            )
        except Exception:
            return None
        if df is None or len(df) < OOS_MIN_BARS:
            return None
    df = df.tail(OOS_MIN_BARS + 40).copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    for c in ("Open", "High", "Low", "Close", "Volume"):
        if c not in df.columns:
            return None
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
    if len(df) < OOS_MIN_BARS:
        return None
    return df.reset_index(drop=True)


def _prepare_eval_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    genetic_expr_builder.VARIABLES 표준 변수 집합을 실데이터로 동일 구성.
    (인큐베이터·라이브와 변수 정의가 1:1 이어야 OOS 가 공정하다.)
    """
    out = pd.DataFrame(
        {
            "open": df["Open"].astype(np.float64),
            "high": df["High"].astype(np.float64),
            "low": df["Low"].astype(np.float64),
            "close": df["Close"].astype(np.float64),
            "volume": df["Volume"].astype(np.float64),
        }
    )
    eps = 1e-9
    out["vol_ma5"] = out["volume"].rolling(5, min_periods=1).mean()
    out["vol_lag1"] = out["volume"].shift(1)
    out["ma5"] = out["close"].rolling(5, min_periods=1).mean()
    out["ma10"] = out["close"].rolling(10, min_periods=1).mean()
    out["ma20"] = out["close"].rolling(20, min_periods=1).mean()
    out["ret1"] = out["close"] / out["close"].shift(1) - 1.0
    out["body"] = (out["close"] - out["open"]) / (out["open"] + eps)
    out["hl_range"] = (out["high"] - out["low"]) / (out["close"] + eps)
    out["vol_ratio"] = out["volume"] / (out["vol_ma5"] + eps)
    return out


def _panel_baseline_drift(frames_by_key: dict[str, pd.DataFrame]) -> float:
    """실데이터 패널의 무조건(랜덤) 익일 수익 평균 = '시장 중력' 기준선."""
    allr: list[float] = []
    for raw in frames_by_key.values():
        c = pd.to_numeric(raw["Close"], errors="coerce").astype(np.float64)
        fwd = (c.shift(-1) / c - 1.0).to_numpy()
        fwd = fwd[np.isfinite(fwd)]
        if fwd.size:
            allr.extend(fwd.tolist())
    return float(np.mean(np.array(allr))) if allr else 0.0


def regime_hard_block_enabled() -> bool:
    """Hard Block SSOT — Architect 정책상 항상 False (OOS_REGIME_HARD_BLOCK env 무시)."""
    return bool(REGIME_HARD_BLOCK_ENABLED)


def _mdd_pct_from_returns(rets: Sequence[float]) -> float:
    if not rets:
        return 0.0
    arr = np.asarray(rets, dtype=np.float64)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return 0.0
    equity = np.cumprod(1.0 + arr)
    peak = np.maximum.accumulate(equity)
    dd = equity / np.maximum(peak, 1e-12) - 1.0
    return float(np.min(dd) * 100.0)


def legacy_regime_hard_block_reason(stress_audit: Optional[Mapping[str, Any]]) -> str:
    """
    레거시 BEAR/BLACK_SWAN Hard Block 이 Drop 했을 reason — audit 전용.
    REGIME_HARD_BLOCK_ENABLED=False 이므로 pass/승격에 사용하지 않는다.
    """
    audit = stress_audit if isinstance(stress_audit, dict) else {}
    bear = audit.get("BEAR")
    if not isinstance(bear, dict):
        return ""
    n_sig = int(bear.get("n_signals") or 0)
    if n_sig < 10:
        return ""
    try:
        excess = float(bear.get("excess_alpha") or 0.0)
    except (TypeError, ValueError):
        excess = 0.0
    try:
        mdd = float(bear.get("mdd_pct") or 0.0)
    except (TypeError, ValueError):
        mdd = 0.0
    if excess < DEPRECATED_BEAR_MIN_EXCESS_ALPHA:
        return "legacy_bear_excess_alpha_fail"
    if mdd < DEPRECATED_BEAR_MAX_MDD_PCT:
        return "legacy_bear_mdd_fail"
    return ""


def apply_regime_hard_block(stress_audit: Optional[Mapping[str, Any]]) -> tuple[bool, str]:
    """
    국면 stress Hard Block — Architect 정책상 무효(no-op).
    반환 (blocked, reason): blocked 는 항상 False.
    """
    if not regime_hard_block_enabled():
        return False, ""
    reason = legacy_regime_hard_block_reason(stress_audit)
    if reason:
        return True, reason
    return False, ""


def resolve_oos_promotion_pass(
    *,
    excess_alpha: float,
    oos_wr: float,
    n_sig: int,
    eval_failed: bool = False,
    stress_audit: Optional[Mapping[str, Any]] = None,
) -> tuple[bool, str]:
    """
    실데이터 OOS 승격 게이트 — 국면 stress 와 무관.
    eval_error / no_signals 는 호출부에서 선처리; 여기서는 수치 게이트만.
    """
    if eval_failed:
        return False, "eval_error"
    if n_sig <= 0:
        return False, "no_signals_on_real_panel"
    passed = (
        excess_alpha > PROMOTE_MIN_EXCESS_ALPHA
        and oos_wr > PROMOTE_MIN_WIN_RATE
        and n_sig >= PROMOTE_MIN_SIGNALS
    )
    if not passed:
        return False, "real_panel_threshold"
    blocked, block_reason = apply_regime_hard_block(stress_audit)
    if blocked:
        return False, block_reason or "regime_hard_block"
    return True, ""


def _load_synthetic_regime_frames(
    db_path: str = SYNTHETIC_DB,
    *,
    max_tickers: int = SYNTHETIC_REGIME_MAX_TICKERS,
) -> dict[str, dict[str, pd.DataFrame]]:
    """
    synthetic_ohlcv → 국면 버킷(BULL/BEAR/SIDEWAYS)별 ticker OHLCV 패널.
    Hard Block 아님 — Regime Tagging(Item 2) 입력용 audit.
    """
    out: dict[str, dict[str, pd.DataFrame]] = {k: {} for k in REGIME_STRESS_BUCKETS}
    if not os.path.isfile(db_path):
        return out
    try:
        with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False) as conn:
            raw = pd.read_sql(
                "SELECT ticker, date, open, high, low, close, volume, regime "
                "FROM synthetic_ohlcv ORDER BY ticker, date",
                conn,
            )
    except Exception:
        return out
    if raw.empty or "regime" not in raw.columns:
        return out

    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    raw = raw.dropna(subset=["date"])
    tickers = sorted(raw["ticker"].dropna().unique().tolist())[: max(1, int(max_tickers))]

    for ticker in tickers:
        sub = raw[raw["ticker"] == ticker].copy()
        if sub.empty:
            continue
        for bucket, regime_set in REGIME_STRESS_BUCKETS.items():
            blk = sub[sub["regime"].astype(str).str.upper().isin(regime_set)].copy()
            if len(blk) < SYNTHETIC_REGIME_MIN_BARS:
                continue
            blk = blk.sort_values("date")
            df = pd.DataFrame(
                {
                    "Date": blk["date"],
                    "Open": pd.to_numeric(blk["open"], errors="coerce"),
                    "High": pd.to_numeric(blk["high"], errors="coerce"),
                    "Low": pd.to_numeric(blk["low"], errors="coerce"),
                    "Close": pd.to_numeric(blk["close"], errors="coerce"),
                    "Volume": pd.to_numeric(blk["volume"], errors="coerce"),
                }
            )
            df = df.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
            if len(df) >= SYNTHETIC_REGIME_MIN_BARS:
                out[bucket][str(ticker)] = df.reset_index(drop=True)
    return out


def evaluate_synthetic_regime_stress_audit(
    expr: str,
    regime_frames: Optional[Mapping[str, Mapping[str, pd.DataFrame]]] = None,
) -> dict[str, Any]:
    """
    합성 DB 국면별 stress 채점 — audit-only (pass/승격 미연동).
    """
    frames = regime_frames if regime_frames is not None else _load_synthetic_regime_frames()
    audit: dict[str, Any] = {}
    for bucket in REGIME_STRESS_BUCKETS:
        panel = frames.get(bucket) if isinstance(frames, dict) else None
        if not isinstance(panel, dict) or not panel:
            audit[bucket] = {
                "n_signals": 0,
                "win_rate": None,
                "avg_return": None,
                "excess_alpha": None,
                "mdd_pct": None,
                "n_tickers": 0,
                "audit_only": True,
            }
            continue
        all_r: list[float] = []
        all_win: list[float] = []
        drift_r: list[float] = []
        for raw in panel.values():
            c = pd.to_numeric(raw["Close"], errors="coerce").astype(np.float64)
            fwd = (c.shift(-1) / c - 1.0).to_numpy()
            fwd = fwd[np.isfinite(fwd)]
            if fwd.size:
                drift_r.extend(fwd.tolist())
            ev = _prepare_eval_frame(raw)
            rv = _oos_forward_returns_at_signals(expr, ev)
            if rv is None or rv.size == 0:
                continue
            all_r.extend(rv.tolist())
            all_win.extend((rv > 0.0).astype(float).tolist())
        if not all_r:
            audit[bucket] = {
                "n_signals": 0,
                "win_rate": None,
                "avg_return": None,
                "excess_alpha": None,
                "mdd_pct": None,
                "n_tickers": len(panel),
                "audit_only": True,
            }
            continue
        baseline = float(np.mean(np.array(drift_r))) if drift_r else float(np.mean(np.array(all_r)))
        oos_wr = float(np.mean(np.array(all_win)))
        oos_ar = float(np.mean(np.array(all_r)))
        audit[bucket] = {
            "n_signals": int(len(all_r)),
            "win_rate": round(oos_wr, 6),
            "avg_return": round(oos_ar, 8),
            "excess_alpha": round(oos_ar - baseline, 8),
            "mdd_pct": round(_mdd_pct_from_returns(all_r), 4),
            "n_tickers": len(panel),
            "audit_only": True,
        }
    audit["_hard_block_disabled"] = not regime_hard_block_enabled()
    audit["_legacy_would_block"] = legacy_regime_hard_block_reason(audit)
    tag, tag_meta = classify_regime_specialization_tag(audit)
    audit["_regime_tag"] = tag
    audit["_regime_tag_meta"] = tag_meta
    return audit


def _float_metric(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        return f if np.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _bucket_regime_profile(
    stress_audit: Mapping[str, Any],
    bucket: str,
) -> dict[str, Any]:
    """국면 버킷(BULL/BEAR/SIDEWAYS) 강약 프로필 — Tagging 입력."""
    raw = stress_audit.get(bucket) if isinstance(stress_audit, dict) else None
    blk = raw if isinstance(raw, dict) else {}
    n_sig = int(blk.get("n_signals") or 0)
    excess = _float_metric(blk.get("excess_alpha"))
    wr = _float_metric(blk.get("win_rate"))
    avg_ret = _float_metric(blk.get("avg_return"))
    mdd = _float_metric(blk.get("mdd_pct"))

    base = {
        "bucket": bucket,
        "n_signals": n_sig,
        "excess_alpha": excess,
        "win_rate": wr,
        "avg_return": avg_ret,
        "mdd_pct": mdd,
    }
    if n_sig < REGIME_TAG_MIN_SIGNALS or excess is None or wr is None:
        return {**base, "profile": "insufficient"}

    b = str(bucket).upper()
    if b == "BULL":
        strong = excess >= BULL_STRONG_EXCESS_ALPHA and wr >= BULL_STRONG_MIN_WIN_RATE
        weak = excess <= REGIME_WEAK_EXCESS_ALPHA or wr <= REGIME_WEAK_MAX_WIN_RATE
    elif b == "BEAR":
        strong = (
            (excess >= BEAR_STRONG_EXCESS_ALPHA and wr >= BEAR_STRONG_MIN_WIN_RATE)
            or (
                avg_ret is not None
                and avg_ret >= BEAR_STRONG_MIN_AVG_RETURN
                and excess > REGIME_WEAK_EXCESS_ALPHA
            )
        )
        weak = excess <= REGIME_WEAK_EXCESS_ALPHA or wr <= REGIME_WEAK_MAX_WIN_RATE
    else:  # SIDEWAYS
        strong = (
            excess >= BULL_STRONG_EXCESS_ALPHA * 0.5
            and wr >= (BULL_STRONG_MIN_WIN_RATE - 0.02)
        )
        weak = excess <= REGIME_WEAK_EXCESS_ALPHA or wr <= REGIME_WEAK_MAX_WIN_RATE

    if strong and not weak:
        profile = "strong"
    elif weak and not strong:
        profile = "weak"
    elif strong and weak:
        profile = "strong" if excess >= 0.0 else "weak"
    else:
        profile = "neutral"
    return {**base, "profile": profile}


def classify_regime_specialization_tag(
    stress_audit: Optional[Mapping[str, Any]],
) -> tuple[str, dict[str, Any]]:
    """
    합성 국면별 stress → Regime Specialization Tag.

    BULL_ONLY  — 상승장 폭발 · 하락장 방어 약함
    BEAR_ONLY  — 상승장 약함 · 하락장 숏/방어 절대수익
    ALL_WEATHER — BULL·BEAR 모두 strong
    UNCLASSIFIED — 전문가 패턴 미확정(표본 부족·중립)
    """
    audit = stress_audit if isinstance(stress_audit, dict) else {}
    bull = _bucket_regime_profile(audit, "BULL")
    bear = _bucket_regime_profile(audit, "BEAR")
    side = _bucket_regime_profile(audit, "SIDEWAYS")
    bp, brp, sp = bull["profile"], bear["profile"], side["profile"]

    if bp == "strong" and brp == "strong":
        tag, reason = "ALL_WEATHER", "bull_and_bear_strong"
    elif bp == "strong" and brp in ("weak", "insufficient", "neutral"):
        tag, reason = "BULL_ONLY", "bull_strong_bear_not"
    elif brp == "strong" and bp in ("weak", "insufficient", "neutral"):
        tag, reason = "BEAR_ONLY", "bear_strong_bull_not"
    elif sp == "strong" and bp != "strong" and brp != "strong":
        tag, reason = "UNCLASSIFIED", "sideways_only_no_bull_bear_edge"
    else:
        tag, reason = "UNCLASSIFIED", "no_clear_specialist_pattern"

    meta = {
        "tag": tag,
        "reason": reason,
        "bull_profile": bp,
        "bear_profile": brp,
        "sideways_profile": sp,
        "bull": bull,
        "bear": bear,
        "sideways": side,
        "thresholds": {
            "min_signals": REGIME_TAG_MIN_SIGNALS,
            "bull_strong_excess": BULL_STRONG_EXCESS_ALPHA,
            "bull_strong_wr": BULL_STRONG_MIN_WIN_RATE,
            "bear_strong_excess": BEAR_STRONG_EXCESS_ALPHA,
            "bear_strong_wr": BEAR_STRONG_MIN_WIN_RATE,
            "bear_strong_avg_return": BEAR_STRONG_MIN_AVG_RETURN,
            "weak_excess": REGIME_WEAK_EXCESS_ALPHA,
            "weak_wr": REGIME_WEAK_MAX_WIN_RATE,
        },
    }
    if tag not in VALID_REGIME_TAGS:
        tag = "UNCLASSIFIED"
    return tag, meta


def normalize_regime_tag(tag: Any) -> str:
    """LIVE/MAB 소비용 SSOT 정규화."""
    t = str(tag or "UNCLASSIFIED").strip().upper()
    return t if t in VALID_REGIME_TAGS else "UNCLASSIFIED"


def attach_regime_tag_fields(rec: dict[str, Any], stress_audit: Mapping[str, Any]) -> None:
    """summary/promoted dict 에 regime_tag + meta in-place."""
    tag = stress_audit.get("_regime_tag") if isinstance(stress_audit, dict) else None
    meta = stress_audit.get("_regime_tag_meta") if isinstance(stress_audit, dict) else None
    if not tag:
        tag, meta = classify_regime_specialization_tag(stress_audit)
    rec["regime_tag"] = normalize_regime_tag(tag)
    rec["regime_tag_meta"] = meta if isinstance(meta, dict) else {}


def _eval_engine() -> str:
    try:
        import numexpr  # noqa: F401

        return "numexpr"
    except Exception:
        return "python"


def _oos_forward_returns_at_signals(expr: str, ev: pd.DataFrame) -> Optional[np.ndarray]:
    """단일 종목: 시그널 발생일 익일 수익률 벡터. pd.eval 실패 시 None (시그널 0건과 구분)."""
    eng = _eval_engine()
    local_base = {col: ev[col] for col in ev.columns}
    try:
        sig = pd.eval(expr, local_dict=local_base, engine=eng)
    except Exception:
        try:
            sig = pd.eval(expr, local_dict=local_base, engine="python")
        except Exception:
            return None
    sig = pd.Series(sig).fillna(False).astype(bool)
    fwd1 = ev["close"].shift(-1) / ev["close"] - 1.0
    m = np.asarray(sig, dtype=bool)
    r = np.asarray(fwd1, dtype=np.float64)
    valid = m & np.isfinite(r)
    return r[valid]


def _fdr_fallback_panel() -> dict[str, pd.DataFrame]:
    """DB에 테이블이 없을 때만 — 소수 대형주 6개월."""
    out: dict[str, pd.DataFrame] = {}
    try:
        import FinanceDataReader as fdr
    except Exception:
        return out
    end = datetime.now()
    start = end - timedelta(days=210)
    for code in ("005930", "000660", "035420", "051910", "006400", "035720"):
        try:
            d = fdr.DataReader(code, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
            if d is None or len(d) < 60:
                continue
            d = d.copy()
            if isinstance(d.columns, pd.MultiIndex):
                d.columns = [str(c[0]) if isinstance(c, tuple) else str(c) for c in d.columns]
            need = {"Open", "High", "Low", "Close", "Volume"}
            if not need.issubset(set(d.columns)):
                continue
            d = d.reset_index()
            date_col = "Date" if "Date" in d.columns else d.columns[0]
            d = d.rename(columns={date_col: "Date"})
            out[f"KR_{code}"] = d[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        except Exception:
            continue
    return out


def load_champions_from_hall_of_fame(
    path: str = HALL_OF_FAME_JSON,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    hall = data.get("hall_of_fame") if isinstance(data, dict) else None
    if not isinstance(hall, list):
        return []
    out: list[dict[str, Any]] = []
    for row in hall[:top_n]:
        if isinstance(row, dict) and str(row.get("expr", "")).strip():
            out.append(dict(row))
    return out


def run_oos_validation(
    champions: Sequence[Mapping[str, Any]] | None = None,
    top_n: int = 10,
    rng_seed: int = 42,
) -> dict[str, Any]:
    champs = list(champions) if champions is not None else load_champions_from_hall_of_fame(top_n=top_n)
    rng = random.Random(rng_seed)

    frames_by_key: dict[str, pd.DataFrame] = {}
    conn = _open_market_db_ro()
    tables: list[str] = []
    if conn is not None:
        try:
            tables = _list_price_tables(conn)
            rng.shuffle(tables)
            tables = tables[:MAX_TICKERS_SAMPLE]
            for t in tables:
                raw = _read_ohlcv_table(conn, t)
                if raw is not None:
                    frames_by_key[t] = raw
        finally:
            try:
                conn.close()
            except Exception:
                pass

    if len(frames_by_key) < 5:
        frames_by_key = _fdr_fallback_panel()

    baseline = _panel_baseline_drift(frames_by_key)

    synthetic_regime_frames = _load_synthetic_regime_frames()

    promoted: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []

    for c in champs:
        name = str(c.get("name", "?"))
        expr = str(c.get("expr", "")).strip()
        if not expr:
            continue
        all_r: list[float] = []
        all_win: list[float] = []
        eval_failed = False
        for _tbl, raw in frames_by_key.items():
            ev = _prepare_eval_frame(raw)
            rv = _oos_forward_returns_at_signals(expr, ev)
            if rv is None:
                eval_failed = True
                break
            if rv.size == 0:
                continue
            all_r.extend(rv.tolist())
            all_win.extend((rv > 0.0).astype(float).tolist())

        stress_audit = evaluate_synthetic_regime_stress_audit(
            expr, regime_frames=synthetic_regime_frames
        )

        if eval_failed:
            rec_fail = {
                "name": name,
                "expr": expr,
                "oos_win_rate": None,
                "oos_avg_return": None,
                "n_signals": 0,
                "pass": False,
                "reason": "eval_error",
                "regime_stress_audit": stress_audit,
                "regime_hard_block_applied": False,
                "legacy_regime_block_reason": legacy_regime_hard_block_reason(stress_audit),
            }
            attach_regime_tag_fields(rec_fail, stress_audit)
            summaries.append(rec_fail)
            continue

        if not all_r:
            rec_empty = {
                "name": name,
                "expr": expr,
                "oos_win_rate": None,
                "oos_avg_return": None,
                "n_signals": 0,
                "pass": False,
                "reason": "no_signals_on_real_panel",
                "regime_stress_audit": stress_audit,
                "regime_hard_block_applied": False,
                "legacy_regime_block_reason": legacy_regime_hard_block_reason(stress_audit),
            }
            attach_regime_tag_fields(rec_empty, stress_audit)
            summaries.append(rec_empty)
            continue

        oos_wr = float(np.mean(np.array(all_win)))
        oos_ar = float(np.mean(np.array(all_r)))
        excess_alpha = oos_ar - baseline
        n_sig = int(len(all_r))
        # [P2-1] 챔피언별 OOS Sharpe(per-signal) 기록 → 이후 다중검정(DSR) 보정에 사용.
        try:
            _arr = np.asarray(all_r, dtype=float)
            _sd = float(np.std(_arr, ddof=1)) if _arr.size >= 2 else 0.0
            oos_sharpe = float(np.mean(_arr) / _sd) if _sd > 1e-12 else 0.0
        except Exception:
            oos_sharpe = 0.0
        # [Mission 5] 합격 = 실데이터 베이스라인 대비 초과 알파 + 최소 승률 + 최소 표본.
        # 국면 stress(BEAR/BLACK_SWAN) 는 audit-only — Hard Block 미적용.
        passed, pass_reason = resolve_oos_promotion_pass(
            excess_alpha=excess_alpha,
            oos_wr=oos_wr,
            n_sig=n_sig,
            stress_audit=stress_audit,
        )
        rec = {
            "name": name,
            "expr": expr,
            "synthetic_win_rate": c.get("win_rate"),
            "synthetic_avg_return": c.get("avg_return"),
            "synthetic_sharpe": c.get("sharpe"),
            "oos_win_rate": round(oos_wr, 6),
            "oos_avg_return": round(oos_ar, 8),
            "oos_baseline_drift": round(baseline, 8),
            "oos_excess_alpha": round(excess_alpha, 8),
            "oos_sharpe": round(oos_sharpe, 6),
            "_oos_returns": all_r,  # DSR 보정용 임시(페이로드 직전 제거)
            "n_signals": n_sig,
            "n_tickers_used": len(frames_by_key),
            "pass": passed,
            "reason": pass_reason if not passed else "",
            "regime_stress_audit": stress_audit,
            "regime_hard_block_applied": False,
            "legacy_regime_block_reason": legacy_regime_hard_block_reason(stress_audit),
        }
        attach_regime_tag_fields(rec, stress_audit)
        summaries.append(rec)

    # [P2-1] 다중검정 보정(Deflated Sharpe) — 전 챔피언 OOS 수익률을 '시도(trials)'로 보고
    # 각 챔피언의 DSR(우연한 최대샤프 임계 초과확률)을 주석. OOS_DSR_MIN>0 일 때만 승격 게이트로
    # 작동(기본 0=보고만 → 라이브 무영향). 실패해도 기존 합격 로직 유지.
    try:
        from validation import walk_forward as _wf

        _series = [
            np.asarray(r.get("_oos_returns"), dtype=float)
            for r in summaries
            if r.get("_oos_returns") and len(r.get("_oos_returns")) >= 2
        ]
        n_trials = len(_series)
        if n_trials >= 2:
            _srs = [_wf.sharpe_ratio(s) for s in _series]
            _sr_var = float(np.var(np.asarray(_srs), ddof=1))
            for r in summaries:
                rr = r.get("_oos_returns")
                if not rr or len(rr) < 2:
                    r["oos_dsr"] = None
                    continue
                arr = np.asarray(rr, dtype=float)
                _sk, _ku = _wf._skew_kurt(arr)
                _d = _wf.deflated_sharpe_ratio(
                    _wf.sharpe_ratio(arr),
                    sr_variance_trials=_sr_var,
                    n_trials=n_trials,
                    n_samples=int(arr.size),
                    skew=_sk,
                    kurt=_ku,
                )
                r["oos_dsr"] = round(_d["dsr"], 6)
        try:
            _dsr_min = float(os.environ.get("OOS_DSR_MIN", "0") or "0")
        except (TypeError, ValueError):
            _dsr_min = 0.0
        if _dsr_min > 0:
            for r in summaries:
                if r.get("pass") and (
                    r.get("oos_dsr") is None or float(r.get("oos_dsr") or 0.0) < _dsr_min
                ):
                    r["pass"] = False
                    r["reason"] = f"dsr<{_dsr_min}"
    except Exception as _dsr_ex:
        print(f"⚠️ [DSR] 다중검정 보정 스킵(비치명적): {_dsr_ex}")

    # 승격 목록 재구성(DSR 게이트 반영) + 임시 필드(_oos_returns) 제거
    for r in summaries:
        r.pop("_oos_returns", None)
        if r.get("pass"):
            row = {k: v for k, v in r.items() if k != "pass"}
            row["validated_at"] = datetime.now(timezone.utc).isoformat()
            promoted.append(row)

    payload = {
        "validated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_source": "market_data.sqlite_ro" if os.path.exists(MARKET_DB) else "fdr_fallback",
        "n_tickers_panel": len(frames_by_key),
        "baseline_drift": round(baseline, 8),
        "regime_specialization_policy": {
            "hard_block_enabled": regime_hard_block_enabled(),
            "stress_audit_only": True,
            "tagging_enabled": True,
            "valid_tags": sorted(VALID_REGIME_TAGS),
            "buckets": list(REGIME_STRESS_BUCKETS.keys()),
            "tag_thresholds": {
                "min_signals": REGIME_TAG_MIN_SIGNALS,
                "bull_strong_excess": BULL_STRONG_EXCESS_ALPHA,
                "bull_strong_wr": BULL_STRONG_MIN_WIN_RATE,
                "bear_strong_excess": BEAR_STRONG_EXCESS_ALPHA,
                "bear_strong_wr": BEAR_STRONG_MIN_WIN_RATE,
                "bear_strong_avg_return": BEAR_STRONG_MIN_AVG_RETURN,
            },
            "note": "BEAR fail does not drop; regime_tag preserved on LIVE promotion",
        },
        "thresholds": {
            "min_win_rate": PROMOTE_MIN_WIN_RATE,
            "min_excess_alpha": PROMOTE_MIN_EXCESS_ALPHA,
            "min_signals": PROMOTE_MIN_SIGNALS,
        },
        "promoted": promoted,
        "all_results": summaries,
    }
    _atomic_write_json(VALIDATED_JSON, payload)
    return payload


def _format_telegram_top1(payload: dict[str, Any]) -> str:
    summ = payload.get("all_results") or []
    top = summ[0] if summ else {}
    name = str(top.get("name", "?"))
    wr = top.get("oos_win_rate")
    passed = bool(top.get("pass"))
    reason = str(top.get("reason") or "")
    ex = top.get("oos_excess_alpha")
    if reason == "eval_error":
        body = f"실전 OOS: 표현식 평가 실패(eval_error) — {name} → 불합격"
    elif wr is None:
        body = "실전 OOS: 시그널 없음 또는 데이터 부족 → 불합격"
    else:
        wr_pct = float(wr) * 100.0
        ex_pct = float(ex or 0.0) * 100.0
        verdict = "최종 합격" if passed else "최종 불합격"
        body = (
            f"가상 1등({name}) 실데이터: 승률 {wr_pct:.2f}% · "
            f"초과알파 {ex_pct:+.4f}% → [{verdict}]"
        )
    n_promo = len(payload.get("promoted") or [])
    base_pct = float(payload.get("baseline_drift", 0.0)) * 100.0
    tag_line = ""
    rtag = top.get("regime_tag")
    if rtag:
        tag_line = f"\n· 국면 태그: <b>{rtag}</b> ({(top.get('regime_tag_meta') or {}).get('reason', '')})"
    return (
        "🛡️ [실전 OOS 검증 완료]\n"
        f"{body}\n"
        f"· 시장 베이스라인: {base_pct:+.4f}% (이걸 이겨야 합격)\n"
        f"· 합격 승격 전략 수: {n_promo}\n"
        f"· 패널 종목 수: {payload.get('n_tickers_panel', 0)}"
        f"{tag_line}"
    )


def main() -> None:
    print("🛡️ Mutant OOS Validator — 실데이터 게이트…")
    try:
        out = run_oos_validation()
    except Exception as e:
        print(f"⚠️ OOS 검증 실패: {e}")
        _atomic_write_json(
            VALIDATED_JSON,
            {
                "validated_at": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "promoted": [],
            },
        )
        send_telegram_report(
            f"🛡️ [실전 OOS 검증 완료] 오류로 중단: {e}\n→ [최종 불합격/미실행]"
        )
        return
    print(f"✅ 저장: {VALIDATED_JSON} | 합격 {len(out.get('promoted') or [])}건")
    try:
        from mutant_pending_bridge import sync_validated_json_into_pending

        n_add, sync_msg = sync_validated_json_into_pending()
        print(f"[mutant_pending_bridge] {sync_msg} (신규 {n_add})")
    except Exception as sync_e:
        print(f"⚠️ [mutant_pending_bridge] PENDING 동기화 스킵: {sync_e}")

    # [Mission 5] 수동 승인 게이트 우회 — OOS 통과 유전자 수식을 INCUBATOR_TEMPLATES 에
    # 즉시 자동 병합하고, 최소 켈리(탐색 모드) 밴딧 밸브로만 실전 투입.
    auto_msg = ""
    try:
        from mutant_pending_bridge import auto_merge_validated_into_incubator

        n_auto, auto_msg = auto_merge_validated_into_incubator()
        print(f"[auto_promote] {auto_msg} (자동 병합 {n_auto})")
    except Exception as auto_e:
        print(f"⚠️ [auto_promote] 자동 병합 스킵: {auto_e}")

    msg = _format_telegram_top1(out)
    if auto_msg:
        msg += f"\n🚀 자동 승격(탐색 켈리): {auto_msg}"
    send_telegram_report(msg)


if __name__ == "__main__":
    main()
