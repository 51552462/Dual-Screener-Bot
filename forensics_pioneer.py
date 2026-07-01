"""
상한가 코호트 DNA(system_config 의 LIMIT_UP_DNA 블록) 패턴과 일치하는 종목 스캔
→ virtual_trade_history (sig_type=forensics_pioneer).
독립 위성 — main / supernova_hunter 미수정.
"""
from __future__ import annotations

import random
import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import pytz

try:
    import FinanceDataReader as fdr
except ImportError:
    fdr = None

try:
    import yfinance as yf
except ImportError:
    yf = None

from limit_up_forensics import (
    PATTERN_KEYS,
    compute_dna_window_flags,
    load_config,
    _fetch_listing_krx,
    _fetch_ohlcv_kr,
    _fetch_ohlcv_us,
    _pick_marcap_series,
    _sleep_stealth,
    _us_listing_merge,
)

import shadow_tracking
from dna_schema_constants import LIMIT_UP_COHORT_DNA_CONFIG_KEY

STRATEGY_NAME = "forensics_pioneer"
MAX_SCAN_KR = 220
MAX_SCAN_US = 220

# [P3-3] 가중 리프트 스코어 통과 임계치 (system_config LIMIT_UP_DNA_LIFT_THRESHOLD 로 override 가능)
DEFAULT_LIFT_PASS_THRESHOLD = 0.7


def _required_rules_from_dna(dna_block: Optional[Dict[str, Any]]) -> List[str]:
    """[레거시] 하위 호환 조회용으로만 유지 — 게이팅에는 더 이상 사용하지 않는다."""
    if not isinstance(dna_block, dict):
        return []
    rules = dna_block.get("pre_emptive_rule") or {}
    if not isinstance(rules, dict):
        return []
    out = [k for k in PATTERN_KEYS if rules.get(k)]
    return out


def _flags_match_required(flags: Dict[str, bool], required: List[str]) -> bool:
    """
    [DEPRECATED — P3-3] 10개 패턴 중 하나라도 틀리면 탈락시키는 가혹한 이진(AND) 게이트.
    `_weighted_lift_score` 기반 가중 스코어링으로 대체되었다. 하위 호환을 위해서만 유지.
    """
    if not required:
        return False
    return all(flags.get(k) for k in required)


def _pattern_weights_from_dna(dna_block: Optional[Dict[str, Any]]) -> Dict[str, float]:
    """
    [P3-3] 지역 DNA 블록에서 패턴별 확률 기반 가중치(Lift)를 읽어온다.

    신규 스키마(`pattern_lift_weights`)가 있으면 그대로 사용하고, 구버전 설정
    (`pre_emptive_rule` 합의 플래그만 존재)만 있으면 합의된 패턴에 균등 가중치를
    부여하는 방식으로 안전하게 폴백한다.
    """
    if not isinstance(dna_block, dict):
        return {}
    weights = dna_block.get("pattern_lift_weights")
    if isinstance(weights, dict) and weights:
        out: Dict[str, float] = {}
        for k in PATTERN_KEYS:
            try:
                out[k] = max(0.0, float(weights.get(k, 0.0) or 0.0))
            except (TypeError, ValueError):
                out[k] = 0.0
        if sum(out.values()) > 0:
            return out
    # 레거시 폴백: 가중치 정보가 없는 구버전 설정 → 합의 플래그만 균등 가중
    required = _required_rules_from_dna(dna_block)
    if not required:
        return {}
    w = 1.0 / len(required)
    return {k: w for k in required}


def _weighted_lift_score(flags: Dict[str, bool], weights: Dict[str, float]) -> float:
    """[P3-3] 라이브 후보의 패턴 플래그 × 패턴별 Lift 가중치 합산 (가중합 스코어, 0~1)."""
    if not flags or not weights:
        return 0.0
    return float(sum(weights.get(k, 0.0) for k in PATTERN_KEYS if flags.get(k)))


def _lift_pass_threshold(cfg: Dict[str, Any]) -> float:
    try:
        return float(cfg.get("LIMIT_UP_DNA_LIFT_THRESHOLD", DEFAULT_LIFT_PASS_THRESHOLD))
    except (TypeError, ValueError):
        return DEFAULT_LIFT_PASS_THRESHOLD


def _kr_scan_codes(max_codes: int) -> List[str]:
    if fdr is None:
        return []
    df, err = _fetch_listing_krx()
    if err or df.empty:
        return []
    df = df.copy()
    if "Marcap" in df.columns:
        df["_mc"] = pd.to_numeric(df["Marcap"], errors="coerce").fillna(0.0)
        df = df.nlargest(max_codes, "_mc")
    else:
        df = df.head(max_codes)
    col = "Code" if "Code" in df.columns else ("Symbol" if "Symbol" in df.columns else None)
    codes: List[str] = []
    if col:
        for c in df[col].tolist():
            s = str(c).strip().zfill(6)[-6:]
            if s.isdigit():
                codes.append(s)
    return list(dict.fromkeys(codes))


def _us_scan_symbols(max_syms: int) -> List[str]:
    base = _us_listing_merge()
    if base.empty or "Symbol" not in base.columns:
        return []
    base = base.copy()
    base["_mc"] = _pick_marcap_series(base)
    base["Symbol"] = base["Symbol"].astype(str).str.replace(".", "-", regex=False)
    base = base.sort_values("_mc", ascending=False).head(max_syms)
    return base["Symbol"].astype(str).tolist()


def _already_logged_today(
    cursor: sqlite3.Cursor, market: str, code: str, tz: pytz.BaseTzInfo
) -> bool:
    day = datetime.now(tz).strftime("%Y-%m-%d")
    cursor.execute(
        """
        SELECT 1 FROM virtual_trade_history
        WHERE market=? AND code=? AND sig_type LIKE ?
          AND logged_at LIKE ?
        LIMIT 1
        """,
        (market, str(code)[:32], f"%{STRATEGY_NAME}%", day + "%"),
    )
    return cursor.fetchone() is not None


def record_forensics_virtual_trade(
    market: str,
    code: str,
    name: str,
    entry_price: float,
    max_retries: int = 5,
) -> bool:
    """virtual_trade_history 단일 행 (장갑차)."""
    tz = pytz.timezone("Asia/Seoul") if market == "KR" else pytz.timezone("America/New_York")
    logged_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    for attempt in range(max_retries):
        try:
            conn = sqlite3.connect(shadow_tracking.DB_PATH, timeout=60)
            conn.execute("PRAGMA journal_mode=WAL;")
            cur = conn.cursor()
            shadow_tracking.init_shadow_tables(cur)

            if _already_logged_today(cur, market, code, tz):
                conn.close()
                return False

            shadow_tracking.insert_virtual_trade_row(
                cur,
                market,
                str(code),
                str(name)[:200],
                float(entry_price),
                STRATEGY_NAME,
                "LIMIT_UP_DNA_LIVE_SCAN",
                logged_at,
            )
            conn.commit()
            conn.close()
            return True
        except sqlite3.OperationalError:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(0.05, 0.2))
            else:
                return False
    return False


def run_forensics_pioneer(market: str) -> None:
    """
    market: 'KR' | 'US'
    상한가 코호트 DNA(설정 키 LIMIT_UP_DNA)의 해당 지역 합의 룰과 오늘 바 기준 DNA 플래그가 모두 일치하면 가상 매매 기록.
    """
    mkt = market.upper()
    print(f"🔭 [forensics_pioneer] {mkt} 장 내 부검 패턴 스캔...")
    try:
        cfg = load_config()
        limit_up_cohort_dna = cfg.get(LIMIT_UP_COHORT_DNA_CONFIG_KEY)
        if not isinstance(limit_up_cohort_dna, dict):
            print(f"⚠️ {LIMIT_UP_COHORT_DNA_CONFIG_KEY} (상한가 코호트 DNA) 없음 — 스킵")
            return

        region_block = limit_up_cohort_dna.get(mkt)
        weights = _pattern_weights_from_dna(region_block if isinstance(region_block, dict) else None)
        if not weights:
            print(f"⚠️ {mkt} pattern_lift_weights/pre_emptive_rule 비어 있음 또는 DNA 미합의 — 스킵")
            return
        pass_threshold = _lift_pass_threshold(cfg)

        hits: List[str] = []

        if mkt == "KR":
            codes = _kr_scan_codes(MAX_SCAN_KR)
            name_map: Dict[str, str] = {}
            try:
                live, _err_live = _fetch_listing_krx()
                if isinstance(live, pd.DataFrame) and not live.empty and "Code" in live.columns:
                    cc = live["Code"].astype(str).str.strip().str.zfill(6)
                    if "Name" in live.columns:
                        for a, b in zip(cc, live["Name"].astype(str)):
                            name_map[a[-6:]] = b
            except Exception:
                pass
            for code in codes:
                _sleep_stealth()
                ohlc = _fetch_ohlcv_kr(code)
                if ohlc is None or len(ohlc) < 15:
                    continue
                T_idx = len(ohlc) - 1
                flags = compute_dna_window_flags(ohlc, T_idx)
                if not flags or _weighted_lift_score(flags, weights) < pass_threshold:
                    continue
                name = name_map.get(code, str(code))
                ep = float(ohlc["Close"].iloc[-1])
                if record_forensics_virtual_trade("KR", code, name, ep):
                    hits.append(code)
        else:
            syms = _us_scan_symbols(MAX_SCAN_US)
            for sym in syms:
                _sleep_stealth()
                ohlc = _fetch_ohlcv_us(sym)
                if ohlc is None or len(ohlc) < 15:
                    continue
                T_idx = len(ohlc) - 1
                flags = compute_dna_window_flags(ohlc, T_idx)
                if not flags or _weighted_lift_score(flags, weights) < pass_threshold:
                    continue
                name = sym
                ep = float(ohlc["Close"].iloc[-1])
                if record_forensics_virtual_trade("US", sym, name, ep):
                    hits.append(sym)

        print(f"✅ forensics_pioneer {mkt}: 신규 가상매매 {len(hits)}건 — {hits[:12]}{'...' if len(hits) > 12 else ''}")

    except Exception as e:
        print(f"⚠️ forensics_pioneer 오류: {e}")


if __name__ == "__main__":
    import sys

    arg = sys.argv[1].upper() if len(sys.argv) > 1 else "KR"
    run_forensics_pioneer(arg)
