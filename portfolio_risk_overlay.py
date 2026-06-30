"""
[P1-1] 포트폴리오 상관관계 캡 (Correlation Overlay).

진입 관문(try_add_virtual_position)에서 신규 후보와 현재 OPEN 포지션들 간 '최근 60일 수익률
상관계수'를 계산해, 0.7 이상으로 과밀한 포지션이 이미 있으면 켈리 비중을 축소(기본 0.5배)하거나
진입을 거부하도록 신호를 돌려준다 — 집중 리스크(같은 테마 동시 폭락) 방어.

설계 원칙: market_data.sqlite 의 일봉(KR_<code>/US_<code>) 종가만 **읽기**로 사용(무DB락,
busy_timeout 통일). 데이터 부재/부족 시 '충돌 없음(neutral)' 으로 수렴 → 기존 진입·켈리 무영향.
"""
from __future__ import annotations

import os
import sqlite3
from typing import Dict, List, Optional, Tuple

import numpy as np

try:
    from market_db_paths import MARKET_DATA_DB_PATH as _DB_PATH
except Exception:  # pragma: no cover
    _DB_PATH = None

try:
    from low_ram_sqlite_pragmas import apply_busy_timeout
except Exception:  # pragma: no cover
    def apply_busy_timeout(conn) -> None:  # type: ignore
        try:
            conn.execute("PRAGMA busy_timeout=60000;")
        except Exception:
            pass


def corr_threshold() -> float:
    try:
        return float(os.environ.get("KR_CORR_CAP_THRESHOLD", "0.7"))
    except (TypeError, ValueError):
        return 0.7


def corr_kelly_mult() -> float:
    try:
        return float(os.environ.get("KR_CORR_KELLY_MULT", "0.5"))
    except (TypeError, ValueError):
        return 0.5


def target_vol(market: str) -> float:
    """시장별 목표 연환산 변동성(소수). 기본 KR/US 모두 0.15(15%)."""
    key = f"{str(market).upper()}_TARGET_VOL"
    try:
        return float(os.environ.get(key, os.environ.get("PORTFOLIO_TARGET_VOL", "0.15")))
    except (TypeError, ValueError):
        return 0.15


def vol_target_bounds() -> Tuple[float, float]:
    """변동성 타게팅 배수 하한/상한(전역 디그로싱/그로싱 클램프)."""
    try:
        lo = float(os.environ.get("VOL_TARGET_MULT_FLOOR", "0.5"))
    except (TypeError, ValueError):
        lo = 0.5
    try:
        hi = float(os.environ.get("VOL_TARGET_MULT_CAP", "1.5"))
    except (TypeError, ValueError):
        hi = 1.5
    return lo, hi


# (market, YYYY-MM-DD) → scalar 캐시 — 스캔 1회 내 반복 진입에서 재계산 회피.
_vt_cache: Dict[str, float] = {}


def _connect() -> Optional[sqlite3.Connection]:
    if not _DB_PATH:
        return None
    try:
        conn = sqlite3.connect(_DB_PATH, timeout=60)
    except Exception:
        return None
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass
    apply_busy_timeout(conn)
    return conn


def _returns_by_date(
    conn: sqlite3.Connection, market: str, code: str, lookback: int
) -> Optional[Dict[str, float]]:
    """KR_/US_ 일봉 종가 → {date: daily_return}. 데이터 부족/오염 시 None."""
    code_s = str(code).zfill(6) if str(market).upper() == "KR" else str(code)
    table = f"{market}_{code_s}"
    try:
        rows = conn.execute(
            f'SELECT Date, Close FROM "{table}" ORDER BY Date DESC LIMIT ?',
            (int(lookback) + 1,),
        ).fetchall()
    except Exception:
        return None
    if not rows or len(rows) < 12:
        return None
    rows = list(reversed(rows))  # 오름차순(과거→현재)
    out: Dict[str, float] = {}
    prev_close: Optional[float] = None
    prev_date: Optional[str] = None
    for d, c in rows:
        try:
            close = float(c)
        except (TypeError, ValueError):
            prev_close, prev_date = None, None
            continue
        if close <= 0:
            prev_close, prev_date = None, None
            continue
        if prev_close is not None and prev_close > 0 and prev_date is not None:
            out[str(d)] = (close - prev_close) / prev_close
        prev_close, prev_date = close, str(d)
    return out or None


def _pearson(a: np.ndarray, b: np.ndarray) -> Optional[float]:
    if a.size < 10 or b.size < 10:
        return None
    if not (np.all(np.isfinite(a)) and np.all(np.isfinite(b))):
        return None
    sa, sb = float(np.std(a)), float(np.std(b))
    if sa <= 1e-12 or sb <= 1e-12:
        return None
    try:
        r = float(np.corrcoef(a, b)[0, 1])
    except Exception:
        return None
    if not np.isfinite(r):
        return None
    return r


def portfolio_vol_target_scalar(
    conn: Optional[sqlite3.Connection],
    market: str,
    *,
    lookback: int = 60,
    min_overlap: int = 20,
    annualization: float = 252.0,
) -> Dict[str, object]:
    """[P1-2] 현재 OPEN 북의 실현 변동성으로 전역 켈리 배수 스케일링.

    scalar = clamp(target_vol / realized_vol, floor, cap). 변동성 급등기 자동 디그로싱.
    반환: {scalar, realized_vol, target_vol, n}. 포지션<2·데이터부족·오류 시 scalar=1.0(중립).
    DB는 KR_/US_ 일봉 **읽기 전용**. 같은 날짜 결과는 캐시.
    """
    from datetime import datetime as _dt

    tv = target_vol(market)
    result: Dict[str, object] = {
        "scalar": 1.0, "realized_vol": 0.0, "target_vol": tv, "n": 0,
    }
    try:
        today = _dt.now().strftime("%Y-%m-%d")
        ck = f"{str(market).upper()}|{today}"
        if ck in _vt_cache:
            result["scalar"] = _vt_cache[ck]
            return result

        own = False
        c = conn
        if c is None:
            c = _connect()
            own = True
        if c is None:
            return result
        try:
            rows = c.execute(
                "SELECT code, sim_kelly_invest FROM forward_trades "
                "WHERE market=? AND status='OPEN'",
                (market,),
            ).fetchall()
            positions = []
            for code, cap in rows or []:
                if not code:
                    continue
                try:
                    w = abs(float(cap or 0.0))
                except (TypeError, ValueError):
                    w = 0.0
                positions.append((str(code), w))
            # 가중치 0(명목 결측)은 동일가중 폴백
            if positions and all(w <= 0 for _, w in positions):
                positions = [(cc, 1.0) for cc, _ in positions]
            positions = [(cc, w) for cc, w in positions if w > 0]
            if len(positions) < 2:
                _vt_cache[ck] = 1.0
                return result

            ret_maps: Dict[str, Dict[str, float]] = {}
            for cc, _w in positions:
                rm = _returns_by_date(c, market, cc, lookback)
                if rm:
                    ret_maps[cc] = rm
            if len(ret_maps) < 2:
                _vt_cache[ck] = 1.0
                return result

            common = None
            for rm in ret_maps.values():
                ks = set(rm.keys())
                common = ks if common is None else (common & ks)
            common = sorted(common or [])
            if len(common) < int(min_overlap):
                _vt_cache[ck] = 1.0
                return result

            tot_w = sum(w for cc, w in positions if cc in ret_maps)
            if tot_w <= 0:
                _vt_cache[ck] = 1.0
                return result
            weights = {cc: (w / tot_w) for cc, w in positions if cc in ret_maps}

            port = np.zeros(len(common), dtype=np.float64)
            for cc, rm in ret_maps.items():
                wv = weights.get(cc, 0.0)
                if wv <= 0:
                    continue
                port += wv * np.array([rm[d] for d in common], dtype=np.float64)

            if port.size < 2:
                _vt_cache[ck] = 1.0
                return result
            realized_vol = float(np.std(port, ddof=1) * np.sqrt(annualization))
            result["realized_vol"] = round(realized_vol, 4)
            result["n"] = len(ret_maps)
            if realized_vol <= 1e-6:
                _vt_cache[ck] = 1.0
                return result

            lo, hi = vol_target_bounds()
            scalar = float(np.clip(tv / realized_vol, lo, hi))
            result["scalar"] = round(scalar, 4)
            _vt_cache[ck] = scalar
            return result
        finally:
            if own and c is not None:
                try:
                    c.close()
                except Exception:
                    pass
    except Exception:
        return result


def check_portfolio_correlation(
    conn: Optional[sqlite3.Connection],
    market: str,
    candidate_code: str,
    open_codes: List[str],
    *,
    lookback: int = 60,
    threshold: Optional[float] = None,
    min_overlap: int = 20,
) -> Dict[str, object]:
    """신규 후보 vs 기존 OPEN 포지션들의 60일 수익률 상관 검사.

    반환: {conflict(bool), max_corr(float), peer_code(str|None), peer_corr_n(int), reason(str)}.
    conflict=True 면 호출부가 켈리 축소(0.5배) 또는 거부를 결정. 예외/데이터부족 시 conflict=False.
    """
    thr = corr_threshold() if threshold is None else float(threshold)
    result: Dict[str, object] = {
        "conflict": False, "max_corr": 0.0, "peer_code": None,
        "peer_corr_n": 0, "reason": "",
    }
    try:
        peers = [str(x) for x in (open_codes or []) if x]
        # 후보 자기 자신은 제외(중복 보유는 상위에서 이미 차단)
        cand_norm = str(candidate_code).zfill(6) if str(market).upper() == "KR" else str(candidate_code)
        peers = [p for p in peers if (str(p).zfill(6) if str(market).upper() == "KR" else str(p)) != cand_norm]
        if not peers:
            return result

        own = False
        c = conn
        if c is None:
            c = _connect()
            own = True
        if c is None:
            return result
        try:
            cand_ret = _returns_by_date(c, market, candidate_code, lookback)
            if not cand_ret:
                result["reason"] = "후보 수익률 데이터 부족"
                return result

            max_corr = 0.0
            peer_hit: Optional[str] = None
            n_hi = 0
            for p in peers:
                peer_ret = _returns_by_date(c, market, p, lookback)
                if not peer_ret:
                    continue
                common = sorted(set(cand_ret.keys()) & set(peer_ret.keys()))
                if len(common) < int(min_overlap):
                    continue
                a = np.array([cand_ret[d] for d in common], dtype=np.float64)
                b = np.array([peer_ret[d] for d in common], dtype=np.float64)
                r = _pearson(a, b)
                if r is None:
                    continue
                if r >= thr:
                    n_hi += 1
                if r > max_corr:
                    max_corr = r
                    peer_hit = p
            result["max_corr"] = round(float(max_corr), 4)
            result["peer_corr_n"] = int(n_hi)
            if max_corr >= thr:
                result["conflict"] = True
                result["peer_code"] = peer_hit
                result["reason"] = (
                    f"기존 OPEN {peer_hit} 와 60일 수익률 상관 {max_corr:.2f}≥{thr:.2f}"
                )
            return result
        finally:
            if own and c is not None:
                try:
                    c.close()
                except Exception:
                    pass
    except Exception:
        return result
