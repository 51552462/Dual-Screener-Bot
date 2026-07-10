"""
[P1-1] 포트폴리오 상관관계 캡 (Correlation Overlay).
[P1-1b] 챔피언 로직 간 직교성 — 일일 PnL 시계열 Pearson 상관 (Correlation-based Kelly Sizing 1번).

진입 관문(try_add_virtual_position)에서:
  · 신규 후보 vs OPEN 포지션 60일 가격 수익률 상관 (집중 리스크) — [2번]
  · 동일 종목 중복 시그널 시 로직 간 일일 PnL 상관 (직교성 평가) — [1번]
  · Correlation Kelly 합동공격/페널티/거부 클램핑 — [3번]

설계 원칙: market_data.sqlite 일봉·forward_trades 장부 **읽기 전용**. 데이터 부족 시
neutral(충돌 없음/직교 가정) → 기존 진입·켈리 무영향.
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

logger = logging.getLogger(__name__)

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


# ── [P1-1b] 챔피언 로직 간 직교성 — 일일 PnL Pearson 상관 ─────────────────────


def logic_corr_high_threshold() -> float:
    """로직 간 상관이 이 값 이상이면 '비직교(중복 베팅)' Tail Risk."""
    try:
        return float(os.environ.get("LOGIC_CORR_HIGH_THRESHOLD", "0.7"))
    except (TypeError, ValueError):
        return 0.7


def logic_pnl_lookback_days() -> int:
    try:
        return int(os.environ.get("LOGIC_PNL_LOOKBACK_DAYS", "90"))
    except (TypeError, ValueError):
        return 90


def min_logic_pnl_overlap_days() -> int:
    try:
        return int(os.environ.get("MIN_LOGIC_PNL_OVERLAP_DAYS", "15"))
    except (TypeError, ValueError):
        return 15


def extract_logic_group_key(sig_type: object) -> str:
    """sig_type → 코어 그룹(챔피언 로직) 키. forward/shared.py 와 동일 규칙."""
    try:
        from meta_treasury_entry_guard import extract_core_group_name

        return extract_core_group_name(str(sig_type or ""))
    except Exception:
        clean = str(sig_type or "").replace("💀[기각/관찰용] ", "")
        clean = re.sub(r"^\[.*?\]\s*", "", clean)
        return clean.split(" [")[0].strip()


def _normalize_ledger_date(value: object) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().replace("T", " ").replace("/", "-")
    if not s:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else None


def _trade_event_date(exit_date: object, entry_date: object) -> Optional[str]:
    return _normalize_ledger_date(exit_date) or _normalize_ledger_date(entry_date)


def _trade_notional_weight(
    sim_kelly_invest: object,
    invest_amount: object,
    *,
    shadow_fallback: float = 1.0,
) -> float:
    """섀도우(명목 0) 건은 equal-weight 폴백으로 PnL 시계열에 포함."""
    vals: List[float] = []
    for raw in (sim_kelly_invest, invest_amount):
        try:
            v = abs(float(raw or 0.0))
        except (TypeError, ValueError):
            v = 0.0
        if v > 0:
            vals.append(v)
    if vals:
        return max(vals)
    return float(shadow_fallback)


def fetch_logic_daily_pnl_returns(
    conn: sqlite3.Connection,
    market: str,
    group_key: str,
    *,
    lookback: Optional[int] = None,
    min_overlap: Optional[int] = None,
) -> Dict[str, float]:
    """
    챔피언 로직(group_key)의 일일 PnL 수익률 시계열 — CLOSED·섀도우 장부 기준.

    각 청산일: daily_ret = Σ(pnl) / Σ(notional), pnl = notional × final_ret / 100.
    섀도우(sim_kelly=0)는 notional=1.0 equal-weight 폴백.
    """
    gk = str(group_key or "").strip()
    if not gk:
        return {}

    lb = int(lookback or logic_pnl_lookback_days())
    cutoff = (datetime.now() - timedelta(days=lb)).strftime("%Y-%m-%d")
    mk = str(market or "KR").upper()

    try:
        cur = conn.execute(
            """
            SELECT exit_date, entry_date, final_ret,
                   sim_kelly_invest, invest_amount, status
            FROM forward_trades
            WHERE UPPER(TRIM(market)) = ?
              AND status LIKE 'CLOSED%%'
              AND final_ret IS NOT NULL
              AND IFNULL(sig_type,'') LIKE ?
            ORDER BY rowid DESC
            LIMIT 800
            """,
            (mk, f"%{gk}%"),
        )
        rows = cur.fetchall()
    except Exception as ex:
        logger.debug("fetch_logic_daily_pnl_returns query skip: %s", ex)
        return {}

    daily_pnl: Dict[str, float] = {}
    daily_cap: Dict[str, float] = {}

    for exit_d, entry_d, final_ret, sk_inv, inv_amt, _status in rows or []:
        evt = _trade_event_date(exit_d, entry_d)
        if not evt or evt < cutoff:
            continue
        try:
            ret_pct = float(final_ret)
        except (TypeError, ValueError):
            continue
        if not np.isfinite(ret_pct):
            continue

        cap = _trade_notional_weight(sk_inv, inv_amt)
        pnl = cap * (ret_pct / 100.0)

        daily_pnl[evt] = daily_pnl.get(evt, 0.0) + pnl
        daily_cap[evt] = daily_cap.get(evt, 0.0) + cap

    out: Dict[str, float] = {}
    for d in sorted(daily_pnl.keys()):
        cap = daily_cap.get(d, 0.0)
        if cap <= 0:
            continue
        out[d] = float(daily_pnl[d] / cap)
    return out


def compute_aligned_pnl_correlation(
    series_a: Mapping[str, float],
    series_b: Mapping[str, float],
    *,
    min_overlap: Optional[int] = None,
) -> Dict[str, Any]:
    """두 로직 일일 PnL 수익률 시계열 Pearson 상관."""
    need = int(min_overlap or min_logic_pnl_overlap_days())
    result: Dict[str, Any] = {
        "correlation": None,
        "n_overlap": 0,
        "neutral": True,
        "orthogonal": True,
        "reason": "insufficient_data",
    }
    if not series_a or not series_b:
        return result

    common = sorted(set(series_a.keys()) & set(series_b.keys()))
    result["n_overlap"] = len(common)
    if len(common) < need:
        result["reason"] = f"overlap<{need}"
        return result

    a = np.array([float(series_a[d]) for d in common], dtype=np.float64)
    b = np.array([float(series_b[d]) for d in common], dtype=np.float64)
    r = _pearson(a, b)
    if r is None:
        result["reason"] = "pearson_failed"
        return result

    thr = logic_corr_high_threshold()
    result["correlation"] = round(float(r), 4)
    result["neutral"] = False
    result["orthogonal"] = float(r) < thr
    result["reason"] = "computed"
    return result


def evaluate_logic_pair_orthogonality(
    conn: sqlite3.Connection,
    market: str,
    group_a: str,
    group_b: str,
    *,
    lookback: Optional[int] = None,
) -> Dict[str, Any]:
    """두 챔피언 로직 간 직교성 평가 SSOT."""
    ga = str(group_a or "").strip()
    gb = str(group_b or "").strip()
    if not ga or not gb:
        return {
            "group_a": ga,
            "group_b": gb,
            "skipped": True,
            "reason": "empty_group",
            "orthogonal": True,
            "correlation": None,
        }
    if ga == gb:
        return {
            "group_a": ga,
            "group_b": gb,
            "skipped": True,
            "reason": "same_group",
            "orthogonal": False,
            "correlation": 1.0,
        }

    sa = fetch_logic_daily_pnl_returns(conn, market, ga, lookback=lookback)
    sb = fetch_logic_daily_pnl_returns(conn, market, gb, lookback=lookback)
    corr_detail = compute_aligned_pnl_correlation(sa, sb)

    return {
        "group_a": ga,
        "group_b": gb,
        "skipped": False,
        "series_a_days": len(sa),
        "series_b_days": len(sb),
        **corr_detail,
        "high_corr_threshold": logic_corr_high_threshold(),
    }


def evaluate_same_symbol_champion_convergence(
    conn: sqlite3.Connection,
    market: str,
    code: str,
    candidate_sig_type: object,
    existing_sig_type: object,
    *,
    extra_peer_groups: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    [1번] 동일 종목에 2개 이상 챔피언 로직이 수렴할 때 직교성 평가.

    무조건 배중 합산·1개 차단 대신, 장부 일일 PnL Pearson 상관으로
    로직 간 중복 베팅 위험을 정량화한다.
    """
    cand_g = extract_logic_group_key(candidate_sig_type)
    exist_g = extract_logic_group_key(existing_sig_type)
    code_s = str(code or "").strip()

    out: Dict[str, Any] = {
        "convergence_detected": False,
        "market": str(market or "").upper(),
        "code": code_s,
        "candidate_group": cand_g,
        "existing_group": exist_g,
        "logic_corr": None,
        "logic_corr_n": 0,
        "orthogonal": True,
        "pairwise": [],
        "max_logic_corr": 0.0,
        "reason": "",
    }

    if not cand_g or not exist_g:
        out["reason"] = "group_extract_failed"
        return out
    if cand_g == exist_g:
        out["reason"] = "same_logic_duplicate"
        return out

    out["convergence_detected"] = True
    pair = evaluate_logic_pair_orthogonality(conn, market, cand_g, exist_g)
    out["pairwise"] = [pair]
    out["logic_corr"] = pair.get("correlation")
    out["logic_corr_n"] = int(pair.get("n_overlap") or 0)
    out["orthogonal"] = bool(pair.get("orthogonal", True))
    if pair.get("correlation") is not None:
        out["max_logic_corr"] = float(pair["correlation"])

    peers = [str(g).strip() for g in (extra_peer_groups or []) if str(g).strip()]
    peers = [g for g in peers if g and g not in (cand_g, exist_g)]
    for pg in peers:
        pp = evaluate_logic_pair_orthogonality(conn, market, cand_g, pg)
        out["pairwise"].append(pp)
        rc = pp.get("correlation")
        if rc is not None and float(rc) > float(out["max_logic_corr"]):
            out["max_logic_corr"] = float(rc)
            out["logic_corr"] = float(rc)
            out["logic_corr_n"] = int(pp.get("n_overlap") or 0)

    thr = logic_corr_high_threshold()
    if out["max_logic_corr"] >= thr:
        out["orthogonal"] = False
        out["reason"] = (
            f"logic_corr={out['max_logic_corr']:.2f}>={thr:.2f} "
            f"({cand_g} vs {exist_g})"
        )
    elif pair.get("reason") == "insufficient_data" or pair.get("neutral"):
        out["reason"] = "neutral_insufficient_pnl_history"
        out["orthogonal"] = True
    else:
        out["reason"] = (
            f"orthogonal logic_corr={out['max_logic_corr']:.2f}<{thr:.2f}"
        )
    return out


def _normalize_market_code(market: str, code: object) -> str:
    mk = str(market or "KR").upper()
    raw = str(code or "").strip()
    if mk == "KR":
        return raw.zfill(6)
    return raw


def fetch_open_positions(
    conn: sqlite3.Connection,
    market: str,
) -> List[Dict[str, Any]]:
    """현재 LIVE OPEN 포지션 스냅샷 — concentration 평가 입력."""
    mk = str(market or "KR").upper()
    try:
        cur = conn.execute(
            """
            SELECT code, sig_type, sector, sim_kelly_invest, invest_amount, name
            FROM forward_trades
            WHERE UPPER(TRIM(market)) = ? AND status = 'OPEN'
            ORDER BY rowid DESC
            """,
            (mk,),
        )
        rows = cur.fetchall()
    except Exception as ex:
        logger.debug("fetch_open_positions skip: %s", ex)
        return []

    out: List[Dict[str, Any]] = []
    for code, sig_type, sector, sk_inv, inv_amt, nm in rows or []:
        if not code:
            continue
        code_n = _normalize_market_code(mk, code)
        try:
            w = abs(float(sk_inv or 0.0))
        except (TypeError, ValueError):
            w = 0.0
        if w <= 0:
            try:
                w = abs(float(inv_amt or 0.0))
            except (TypeError, ValueError):
                w = 0.0
        out.append(
            {
                "code": code_n,
                "sig_type": str(sig_type or ""),
                "sector": str(sector or ""),
                "notional": w,
                "name": str(nm or ""),
                "group_key": extract_logic_group_key(sig_type),
            }
        )
    return out


def _pairwise_return_correlation(
    cand_ret: Mapping[str, float],
    peer_ret: Mapping[str, float],
    *,
    min_overlap: int,
) -> Dict[str, Any]:
    common = sorted(set(cand_ret.keys()) & set(peer_ret.keys()))
    detail: Dict[str, Any] = {
        "n_overlap": len(common),
        "correlation": None,
        "computed": False,
    }
    if len(common) < int(min_overlap):
        detail["reason"] = f"overlap<{min_overlap}"
        return detail
    a = np.array([float(cand_ret[d]) for d in common], dtype=np.float64)
    b = np.array([float(peer_ret[d]) for d in common], dtype=np.float64)
    r = _pearson(a, b)
    if r is None:
        detail["reason"] = "pearson_failed"
        return detail
    detail["correlation"] = round(float(r), 4)
    detail["computed"] = True
    detail["reason"] = "computed"
    return detail


def evaluate_portfolio_concentration_risk(
    conn: Optional[sqlite3.Connection],
    market: str,
    candidate_code: str,
    *,
    open_codes: Optional[Sequence[str]] = None,
    open_positions: Optional[Sequence[Mapping[str, Any]]] = None,
    exclude_same_code: bool = True,
    lookback: int = 60,
    threshold: Optional[float] = None,
    min_overlap: int = 20,
) -> Dict[str, Any]:
    """
    [2번] 계좌 OPEN 종목군 vs 신규 진입 후보 — 최근 60일 가격 수익률 Pearson 상관.

    concentrated(max_corr ≥ threshold): 포트폴리오 쏠림·Tail Risk
    diversified(max_corr < threshold): 분산(Hedged) 상태
    데이터 부족 시 neutral → diversified 로 수렴(기존 진입 무영향).
    """
    thr = corr_threshold() if threshold is None else float(threshold)
    mk = str(market or "KR").upper()
    cand_norm = _normalize_market_code(mk, candidate_code)

    result: Dict[str, Any] = {
        "concentrated": False,
        "diversified": True,
        "neutral": True,
        "max_corr": 0.0,
        "peer_code": None,
        "peer_corr_n": 0,
        "pairwise": [],
        "open_count": 0,
        "peer_count": 0,
        "candidate_code": cand_norm,
        "lookback_days": int(lookback),
        "threshold": thr,
        "reason": "",
    }

    try:
        peers_raw: List[str] = []
        if open_codes:
            peers_raw = [str(x) for x in open_codes if x]
        elif open_positions:
            peers_raw = [
                str(p.get("code") or "")
                for p in open_positions
                if isinstance(p, Mapping) and p.get("code")
            ]
        peers_norm = [_normalize_market_code(mk, p) for p in peers_raw]
        if exclude_same_code:
            peers_norm = [p for p in peers_norm if p != cand_norm]
        peers_norm = sorted(set(peers_norm))
        result["open_count"] = len(peers_raw)
        result["peer_count"] = len(peers_norm)
        if not peers_norm:
            result["reason"] = "no_open_peers"
            return result

        own = False
        c = conn
        if c is None:
            c = _connect()
            own = True
        if c is None:
            result["reason"] = "market_db_unavailable"
            return result
        try:
            cand_ret = _returns_by_date(c, mk, candidate_code, lookback)
            if not cand_ret:
                result["reason"] = "candidate_return_data_missing"
                return result

            max_corr = 0.0
            peer_hit: Optional[str] = None
            n_hi = 0
            pairwise: List[Dict[str, Any]] = []
            any_computed = False

            for peer in peers_norm:
                peer_ret = _returns_by_date(c, mk, peer, lookback)
                if not peer_ret:
                    pairwise.append(
                        {
                            "peer_code": peer,
                            "correlation": None,
                            "n_overlap": 0,
                            "computed": False,
                            "reason": "peer_return_data_missing",
                        }
                    )
                    continue
                pdetail = _pairwise_return_correlation(
                    cand_ret, peer_ret, min_overlap=min_overlap
                )
                pdetail["peer_code"] = peer
                pairwise.append(pdetail)
                r = pdetail.get("correlation")
                if not pdetail.get("computed") or r is None:
                    continue
                any_computed = True
                rf = float(r)
                if rf >= thr:
                    n_hi += 1
                if rf > max_corr:
                    max_corr = rf
                    peer_hit = peer

            result["pairwise"] = pairwise
            result["max_corr"] = round(float(max_corr), 4)
            result["peer_corr_n"] = int(n_hi)
            result["peer_code"] = peer_hit

            if not any_computed:
                result["reason"] = "insufficient_overlap_or_price_data"
                return result

            result["neutral"] = False
            if max_corr >= thr:
                result["concentrated"] = True
                result["diversified"] = False
                result["reason"] = (
                    f"OPEN {peer_hit} 와 60일 수익률 상관 {max_corr:.2f}≥{thr:.2f}"
                )
            else:
                result["concentrated"] = False
                result["diversified"] = True
                result["reason"] = (
                    f"diversified max_corr={max_corr:.2f}<{thr:.2f}"
                )
            return result
        finally:
            if own and c is not None:
                try:
                    c.close()
                except Exception:
                    pass
    except Exception as ex:
        logger.debug("evaluate_portfolio_concentration_risk skip: %s", ex)
        result["reason"] = "evaluation_error"
        return result


def evaluate_champion_convergence_risk_profile(
    conn: sqlite3.Connection,
    market: str,
    code: str,
    candidate_sig_type: object,
    existing_sig_type: object,
    *,
    extra_peer_groups: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    [1번+2번] 동일 종목 챔피언 수렴 — 로직 직교성 + 계좌 Concentration 통합 프로필.
    """
    orth = evaluate_same_symbol_champion_convergence(
        conn,
        market,
        code,
        candidate_sig_type,
        existing_sig_type,
        extra_peer_groups=extra_peer_groups,
    )
    open_positions = fetch_open_positions(conn, market)
    concentration = evaluate_portfolio_concentration_risk(
        conn,
        market,
        code,
        open_positions=open_positions,
        exclude_same_code=True,
    )
    orth["concentration"] = concentration
    orth["portfolio_max_corr"] = concentration.get("max_corr")
    orth["portfolio_diversified"] = bool(concentration.get("diversified", True))
    orth["portfolio_concentrated"] = bool(concentration.get("concentrated", False))
    orth["open_position_count"] = len(open_positions)
    orth["concentration_peer_count"] = concentration.get("peer_count", 0)
    return orth


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

    [2번] evaluate_portfolio_concentration_risk 래퍼 — 하위 호환 conflict 키 유지.
    """
    conc = evaluate_portfolio_concentration_risk(
        conn,
        market,
        candidate_code,
        open_codes=open_codes,
        exclude_same_code=True,
        lookback=lookback,
        threshold=threshold,
        min_overlap=min_overlap,
    )
    return {
        "conflict": bool(conc.get("concentrated")),
        "max_corr": conc.get("max_corr", 0.0),
        "peer_code": conc.get("peer_code"),
        "peer_corr_n": conc.get("peer_corr_n", 0),
        "reason": conc.get("reason", ""),
        "concentration": conc,
    }


# ── [P1-1d] Correlation-based Kelly Sizing — 최종 비중 클램핑 [3번] ─────────────


def correlation_kelly_sizing_enabled() -> bool:
    raw = os.environ.get("ENABLE_CORRELATION_KELLY_SIZING", "1")
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def resolve_correlation_kelly_sizing(
    *,
    convergence_detected: bool = False,
    orthogonal: bool = True,
    logic_corr: Optional[float] = None,
    logic_corr_neutral: bool = True,
    portfolio_diversified: bool = True,
    portfolio_concentrated: bool = False,
    portfolio_neutral: bool = True,
    portfolio_max_corr: float = 0.0,
) -> Dict[str, Any]:
    """
    [3번] 1번(로직 직교) + 2번(Concentration) → Kelly 배수·진입 허용 판정.

    joint_attack: 직교 + 분산 → Kelly 100% (합동 공격 허용)
    penalty:      단일 Tail Risk → Kelly ×0.5
    reject:       로직·포트 동시 Tail Risk → 신규 챔피언 시그널 거부
    standard:     일반 진입 — concentration만 양호 시 100%
    """
    logic_thr = logic_corr_high_threshold()
    port_thr = corr_threshold()
    penalty_mult = corr_kelly_mult()

    logic_tail = False
    if convergence_detected:
        if not orthogonal and not logic_corr_neutral:
            logic_tail = True
        elif logic_corr is not None and float(logic_corr) >= logic_thr:
            logic_tail = True

    port_tail = bool(portfolio_concentrated)
    if not portfolio_neutral and not portfolio_diversified:
        if float(portfolio_max_corr) >= port_thr:
            port_tail = True

    out: Dict[str, Any] = {
        "action": "standard",
        "kelly_mult": 1.0,
        "reject_entry": False,
        "allow_convergence_entry": False,
        "logic_tail_risk": logic_tail,
        "portfolio_tail_risk": port_tail,
        "reason": "",
    }

    if convergence_detected:
        out["allow_convergence_entry"] = True
        if logic_tail and port_tail:
            out.update(
                {
                    "action": "reject",
                    "kelly_mult": 0.0,
                    "reject_entry": True,
                    "allow_convergence_entry": False,
                    "reason": (
                        f"dual_tail logic_ρ≥{logic_thr:.2f} & port_ρ≥{port_thr:.2f}"
                    ),
                }
            )
            return out
        if logic_tail or port_tail:
            parts = []
            if logic_tail:
                parts.append(
                    f"logic_ρ={float(logic_corr) if logic_corr is not None else 'high'}"
                )
            if port_tail:
                parts.append(f"port_ρ={float(portfolio_max_corr):.2f}")
            out.update(
                {
                    "action": "penalty",
                    "kelly_mult": penalty_mult,
                    "reason": f"tail_risk_penalty ({', '.join(parts)})",
                }
            )
            return out
        out.update(
            {
                "action": "joint_attack",
                "kelly_mult": 1.0,
                "reason": (
                    f"joint_attack logic_ρ={float(logic_corr) if logic_corr is not None else 'neutral'}"
                    f"<{logic_thr:.2f} port_ρ={float(portfolio_max_corr):.2f}<{port_thr:.2f}"
                ),
            }
        )
        return out

    if port_tail:
        out.update(
            {
                "action": "penalty",
                "kelly_mult": penalty_mult,
                "reason": (
                    f"concentration_penalty port_ρ={float(portfolio_max_corr):.2f}"
                    f"≥{port_thr:.2f}"
                ),
            }
        )
        return out

    out["reason"] = (
        f"standard port_ρ={float(portfolio_max_corr):.2f}<{port_thr:.2f}"
        if not portfolio_neutral
        else "standard neutral_or_diversified"
    )
    return out


def resolve_correlation_kelly_from_profile(
    profile: Mapping[str, Any],
) -> Dict[str, Any]:
    """evaluate_champion_convergence_risk_profile → Kelly sizing."""
    conc = profile.get("concentration") if isinstance(profile.get("concentration"), dict) else {}
    pairwise = profile.get("pairwise") or []
    pair0 = pairwise[0] if pairwise and isinstance(pairwise[0], dict) else {}
    sizing = resolve_correlation_kelly_sizing(
        convergence_detected=bool(profile.get("convergence_detected")),
        orthogonal=bool(profile.get("orthogonal", True)),
        logic_corr=profile.get("logic_corr"),
        logic_corr_neutral=bool(pair0.get("neutral", True)),
        portfolio_diversified=bool(profile.get("portfolio_diversified", True)),
        portfolio_concentrated=bool(profile.get("portfolio_concentrated", False)),
        portfolio_neutral=bool(conc.get("neutral", True)),
        portfolio_max_corr=float(
            profile.get("portfolio_max_corr") or conc.get("max_corr") or 0.0
        ),
    )
    sizing["profile"] = dict(profile)
    return sizing


def apply_mega_trend_correlation_forgiveness(
    sizing: Mapping[str, Any],
    *,
    sys_config: Optional[Mapping[str, Any]] = None,
    candidate_sector: object = None,
    open_sectors: Optional[Sequence[object]] = None,
    candidate_code: object = None,
) -> Dict[str, Any]:
    """
    [Mega-Trend 2번] 활성 MEGA_TREND_SECTOR — CorrKelly 페널티/거부 전면 Bypass.
    ROTATION_ADVANTAGE 자본 합산 승인 플래그 부여.

    수렴(동일 종목 중복) 시: 후보·기존 OPEN 모두 메가트렌드 섹터에 속할 때만 면죄.
    """
    prev = dict(sizing)
    if not evaluate_mega_trend_forgiveness_eligibility(
        sys_config,
        candidate_sector,
        open_sectors=open_sectors,
        candidate_code=candidate_code,
    ):
        return prev

    act = str(prev.get("action") or "standard")
    if act == "reject" and prev.get("same_logic_block"):
        return prev
    # 면죄 대상: penalty / reject(dual tail) / standard+concentration penalty
    if act not in ("penalty", "reject", "standard") and not prev.get("portfolio_tail_risk"):
        if act in ("joint_attack", "mega_trend_unlock"):
            return prev

    prev.update(
        {
            "action": "mega_trend_unlock",
            "kelly_mult": 1.0,
            "reject_entry": False,
            "allow_convergence_entry": True,
            "mega_trend_forgiveness": True,
            "rotation_advantage_approved": True,
            "reason": (
                f"mega_trend_forgiveness bypass "
                f"(was {act}: {prev.get('reason', '')})"
            ),
        }
    )
    return prev


def evaluate_mega_trend_forgiveness_eligibility(
    sys_config: Optional[Mapping[str, Any]],
    candidate_sector: object,
    *,
    open_sectors: Optional[Sequence[object]] = None,
    candidate_code: object = None,
) -> bool:
    """메가트렌드 CorrKelly 면죄부 자격 — 후보(및 수렴 시 기존 OPEN) 섹터 검증."""
    try:
        from mega_trend_ignition import (
            MEGA_TREND_CONFIG_KEY,
            is_mega_trend_sector,
            mega_trend_unlock_enabled,
            resolve_kr_code_sector,
        )
    except Exception:
        return False

    if not mega_trend_unlock_enabled():
        return False
    if not isinstance(sys_config, Mapping):
        return False

    block = sys_config.get(MEGA_TREND_CONFIG_KEY)
    if isinstance(block, Mapping) and block.get("correlation_forgiveness_revoked"):
        return False

    sec = candidate_sector
    if candidate_code and (not sec or str(sec).strip() in ("", "유망섹터 포착")):
        sec = resolve_kr_code_sector(candidate_code, sec)

    if not is_mega_trend_sector(sec, sys_config):
        return False

    if open_sectors:
        for osec in open_sectors:
            if not is_mega_trend_sector(osec, sys_config):
                return False

    return True


def resolve_convergence_open_sectors(
    conn: sqlite3.Connection,
    market: str,
    code: str,
    open_rows: Sequence[Sequence[Any]],
) -> List[str]:
    """수렴 게이트용 — 동일 종목 OPEN 행의 표준 섹터 목록."""
    try:
        from mega_trend_ignition import resolve_kr_code_sector
    except Exception:
        resolve_kr_code_sector = None  # type: ignore

    mk = str(market or "KR").upper()
    code_n = _normalize_market_code(mk, code)
    sectors: List[str] = []
    sector_by_code: Dict[str, str] = {}
    try:
        for row in fetch_open_positions(conn, mk):
            c = str(row.get("code") or "")
            if c:
                sector_by_code[c] = str(row.get("sector") or "")
    except Exception:
        pass

    for row in open_rows or []:
        if resolve_kr_code_sector is None:
            break
        row_code = code_n
        row_sec = sector_by_code.get(row_code, "")
        if not row_sec and len(row) > 0:
            row_sec = ""
        sectors.append(resolve_kr_code_sector(row_code, row_sec))
    return sectors


def format_mega_trend_sig_tag(sizing: Mapping[str, Any]) -> str:
    if sizing.get("mega_trend_forgiveness") or sizing.get("action") == "mega_trend_unlock":
        return " #MegaTrend언락"
    return ""


def evaluate_convergence_entry_gate(
    conn: sqlite3.Connection,
    market: str,
    code: str,
    candidate_sig_type: object,
    open_rows: Sequence[Sequence[Any]],
    *,
    sys_config: Optional[Mapping[str, Any]] = None,
    candidate_sector: object = None,
) -> Dict[str, Any]:
    """
    [3번] 동일 종목 OPEN 존재 시 — 챔피언 수렴 진입 게이트.

    동일 로직 중복 → 항상 거부.
    서로 다른 챔피언 → correlation Kelly sizing 으로 합동/페널티/거부.
    """
    cand_g = extract_logic_group_key(candidate_sig_type)
    result: Dict[str, Any] = {
        "allow_entry": True,
        "reject_entry": False,
        "same_logic_block": False,
        "kelly_mult": 1.0,
        "action": "standard",
        "reason": "",
        "profiles": [],
        "sizing": None,
    }
    if not open_rows:
        return result

    open_sectors = resolve_convergence_open_sectors(conn, market, code, open_rows)
    worst_mult = 1.0
    action = "standard"
    profiles: List[Dict[str, Any]] = []
    reject_reason = ""

    for row in open_rows:
        exist_sig = row[1] if len(row) > 1 else ""
        exist_g = extract_logic_group_key(exist_sig)
        if cand_g and exist_g and cand_g == exist_g:
            result.update(
                {
                    "allow_entry": False,
                    "reject_entry": True,
                    "same_logic_block": True,
                    "kelly_mult": 0.0,
                    "action": "reject",
                    "reason": f"same_logic_duplicate:{cand_g}",
                }
            )
            return result

        profile = evaluate_champion_convergence_risk_profile(
            conn,
            market,
            code,
            candidate_sig_type,
            exist_sig,
        )
        profiles.append(profile)
        sizing = resolve_correlation_kelly_from_profile(profile)
        sizing = apply_mega_trend_correlation_forgiveness(
            sizing,
            sys_config=sys_config,
            candidate_sector=candidate_sector,
            open_sectors=open_sectors,
            candidate_code=code,
        )
        if sizing.get("reject_entry"):
            reject_reason = str(sizing.get("reason") or "convergence_tail_reject")
            result.update(
                {
                    "allow_entry": False,
                    "reject_entry": True,
                    "kelly_mult": 0.0,
                    "action": "reject",
                    "reason": reject_reason,
                    "profiles": profiles,
                    "sizing": sizing,
                }
            )
            return result

        act = str(sizing.get("action") or "standard")
        mult = float(sizing.get("kelly_mult", 1.0) or 1.0)
        if act in ("joint_attack", "mega_trend_unlock"):
            action = "joint_attack" if act == "joint_attack" else "mega_trend_unlock"
            worst_mult = 1.0
        elif act == "penalty":
            if action not in ("joint_attack", "mega_trend_unlock"):
                action = "penalty"
            worst_mult = min(worst_mult, mult)

    result.update(
        {
            "allow_entry": True,
            "kelly_mult": worst_mult,
            "action": action,
            "reason": (
                profiles[-1].get("reason", "") if profiles else ""
            ),
            "profiles": profiles,
            "sizing": (
                resolve_correlation_kelly_from_profile(profiles[-1])
                if profiles
                else None
            ),
        }
    )
    if profiles and result.get("sizing"):
        result["sizing"] = apply_mega_trend_correlation_forgiveness(
            result["sizing"],
            sys_config=sys_config,
            candidate_sector=candidate_sector,
            open_sectors=open_sectors,
            candidate_code=code,
        )
        result["kelly_mult"] = float(result["sizing"].get("kelly_mult", worst_mult) or 1.0)
        result["action"] = str(result["sizing"].get("action") or action)
        if result["sizing"].get("mega_trend_forgiveness"):
            result["mega_trend_forgiveness"] = True
            result["rotation_advantage_approved"] = True
            result["reason"] = str(result["sizing"].get("reason") or result["reason"])
            action = result["action"]
    if result.get("action") == "penalty":
        result["reason"] = (
            f"convergence_penalty x{worst_mult:g} "
            f"({result['sizing'].get('reason', '') if result.get('sizing') else ''})"
        )
    elif result.get("action") in ("joint_attack", "mega_trend_unlock") and profiles:
        if result.get("action") == "mega_trend_unlock":
            result["reason"] = str(result["sizing"].get("reason", "") if result.get("sizing") else "")
        else:
            result["reason"] = str(
                resolve_correlation_kelly_from_profile(profiles[-1]).get("reason", "")
            )
    return result


def format_correlation_kelly_sig_tag(sizing: Mapping[str, Any]) -> str:
    """장부 sig_type 부착용 태그."""
    if sizing.get("mega_trend_forgiveness") or sizing.get("action") == "mega_trend_unlock":
        return ""
    act = str(sizing.get("action") or "standard")
    mult = float(sizing.get("kelly_mult", 1.0) or 1.0)
    if act == "joint_attack":
        return " #CorrKelly합동공격(x1)"
    if act == "penalty" and mult != 1.0:
        return f" #CorrKelly페널티(x{mult:g})"
    if act == "standard" and not sizing.get("portfolio_tail_risk"):
        port_r = sizing.get("profile", {}).get("portfolio_max_corr")
        if port_r is not None and float(port_r) > 0:
            return f" #CorrKelly분산(ρ{float(port_r):.2f})"
    return ""


def apply_entry_correlation_kelly_overlay(
    conn: sqlite3.Connection,
    market: str,
    code: str,
    sig_type: str,
    kelly_risk_pct: float,
    *,
    facts: Optional[Dict[str, Any]] = None,
    sys_config: Optional[Mapping[str, Any]] = None,
    candidate_sector: object = None,
) -> Tuple[float, str, Dict[str, Any]]:
    """
    [3번] try_add Kelly 단계 — 수렴 선반영 + 일반 진입 concentration 통합 클램핑.
    """
    if not correlation_kelly_sizing_enabled():
        return float(kelly_risk_pct), sig_type, {"action": "disabled"}

    facts_in = facts if isinstance(facts, dict) else {}
    out_kelly = float(kelly_risk_pct)
    pre_action = str(facts_in.get("_correlation_sizing_action") or "")
    pre_mult = float(facts_in.get("_correlation_kelly_mult", 1.0) or 1.0)
    sizing_detail: Dict[str, Any]

    if pre_action in ("joint_attack", "mega_trend_unlock") and facts_in.get("_convergence_profile"):
        sizing_detail = resolve_correlation_kelly_from_profile(
            facts_in["_convergence_profile"]
        )
        sizing_detail = apply_mega_trend_correlation_forgiveness(
            sizing_detail,
            sys_config=sys_config or facts_in.get("_sys_config"),
            candidate_sector=candidate_sector or facts_in.get("sector"),
            candidate_code=code,
        )
        if pre_action == "mega_trend_unlock" or sizing_detail.get("mega_trend_forgiveness"):
            sizing_detail["action"] = "mega_trend_unlock"
            sizing_detail["kelly_mult"] = 1.0
        elif pre_action == "penalty" and pre_mult != 1.0:
            sizing_detail["kelly_mult"] = pre_mult
            sizing_detail["action"] = "penalty"
            out_kelly *= pre_mult
        else:
            out_kelly *= float(sizing_detail.get("kelly_mult", 1.0) or 1.0)
    elif pre_action in ("joint_attack", "mega_trend_unlock"):
        sizing_detail = {
            "action": pre_action,
            "kelly_mult": 1.0,
            "reason": str(facts_in.get("_correlation_sizing_reason") or ""),
            "mega_trend_forgiveness": pre_action == "mega_trend_unlock",
        }
    elif pre_action == "penalty" and facts_in.get("_convergence_profile"):
        sizing_detail = resolve_correlation_kelly_from_profile(
            facts_in["_convergence_profile"]
        )
        sizing_detail["kelly_mult"] = pre_mult
        sizing_detail["action"] = "penalty"
        out_kelly *= pre_mult
    elif pre_mult != 1.0:
        sizing_detail = {
            "action": pre_action or "penalty",
            "kelly_mult": pre_mult,
            "reason": str(facts_in.get("_correlation_sizing_reason") or ""),
        }
        out_kelly *= pre_mult
    else:
        open_positions = fetch_open_positions(conn, market)
        conc = evaluate_portfolio_concentration_risk(
            conn,
            market,
            code,
            open_positions=open_positions,
            exclude_same_code=True,
        )
        sizing_detail = resolve_correlation_kelly_sizing(
            convergence_detected=False,
            portfolio_diversified=bool(conc.get("diversified", True)),
            portfolio_concentrated=bool(conc.get("concentrated", False)),
            portfolio_neutral=bool(conc.get("neutral", True)),
            portfolio_max_corr=float(conc.get("max_corr", 0.0) or 0.0),
        )
        sizing_detail["concentration"] = conc
        mult = float(sizing_detail.get("kelly_mult", 1.0) or 1.0)
        if mult != 1.0:
            out_kelly *= mult

    sector_for_mt = candidate_sector or facts_in.get("sector")
    sizing_detail = apply_mega_trend_correlation_forgiveness(
        sizing_detail,
        sys_config=sys_config or facts_in.get("_sys_config"),
        candidate_sector=sector_for_mt,
        candidate_code=code,
    )
    if sizing_detail.get("mega_trend_forgiveness"):
        out_kelly = float(kelly_risk_pct)
        facts_in["_mega_trend_forgiveness"] = True
        facts_in["_rotation_advantage_approved"] = True

    tag = format_correlation_kelly_sig_tag(sizing_detail)
    mt_tag = format_mega_trend_sig_tag(sizing_detail)
    if mt_tag and mt_tag not in sig_type:
        sig_type = sig_type + mt_tag
    if not tag and sizing_detail.get("concentration"):
        conc = sizing_detail["concentration"]
        if (
            sizing_detail.get("action") == "standard"
            and not conc.get("neutral")
            and conc.get("diversified")
        ):
            mx = float(conc.get("max_corr", 0.0) or 0.0)
            if mx > 0:
                tag = f" #CorrKelly분산(ρ{mx:.2f})"
    if tag and tag not in sig_type:
        sig_type = sig_type + tag

    return out_kelly, sig_type, sizing_detail
