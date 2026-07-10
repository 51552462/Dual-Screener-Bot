"""
[P0-1] 스마트 수급(외국인·기관 순매수) 시계열 영속화 + 진입 가산 팩터.

- 저장: market_data.sqlite 의 `kr_investor_flow(date, code, name, foreign_inst_krw, foreign_inst_vol)`.
  smart_money_tracker 가 매일 단일거래일 순매수 리더보드를 적재한다(스냅샷 → 시계열).
- 산출: 종목별 '최근 N일 수급 모멘텀(누적 순매수)' + '수급 다이버전스(주가 횡보 + 순매수 증가)' →
  0~KR_FLOW_BONUS_MAX 의 진입 가산점(bonus). try_add_virtual_position(KR) 에서 score 가산에 사용.

설계 원칙: **읽기 위주·무DB락(busy_timeout 통일)·전부 방어적**(데이터/테이블 부재 시 중립 0 으로 수렴,
기존 진입 로직 절대 훼손 금지).
"""
from __future__ import annotations

import math
import os
import sqlite3
from typing import Any, Dict, Optional, Sequence, Set

try:
    from market_db_paths import MARKET_DATA_DB_PATH as _DB_PATH
except Exception:  # pragma: no cover - 경로 모듈 부재 시에도 무해하게 동작
    _DB_PATH = None

try:
    from low_ram_sqlite_pragmas import apply_busy_timeout
except Exception:  # pragma: no cover
    def apply_busy_timeout(conn) -> None:  # type: ignore
        try:
            conn.execute("PRAGMA busy_timeout=60000;")
        except Exception:
            pass

FLOW_TABLE = "kr_investor_flow"

# 같은 스캔 프로세스(크론 1회) 내 반복 조회 캐시 — 짧은 수명이라 staleness 무해.
_score_cache: Dict[str, Dict[str, Any]] = {}


def _bonus_max() -> float:
    try:
        return max(0.0, float(os.environ.get("KR_FLOW_BONUS_MAX", "10")))
    except (TypeError, ValueError):
        return 10.0


def _db_path() -> Optional[str]:
    return _DB_PATH


def _connect() -> Optional[sqlite3.Connection]:
    p = _db_path()
    if not p:
        return None
    try:
        conn = sqlite3.connect(p, timeout=60)
    except Exception:
        return None
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass
    apply_busy_timeout(conn)
    return conn


def ensure_flow_table(conn: sqlite3.Connection) -> None:
    """테이블·인덱스 보장(존재 시 no-op). 호출부가 conn 을 소유."""
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS {FLOW_TABLE} (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            foreign_inst_krw REAL DEFAULT 0,
            foreign_inst_vol REAL DEFAULT 0,
            PRIMARY KEY (date, code)
        )"""
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{FLOW_TABLE}_code ON {FLOW_TABLE}(code)"
    )


def existing_flow_dates() -> Set[str]:
    """이미 적재된 날짜 집합(YYYY-MM-DD) — 백필 중복 방지용."""
    conn = _connect()
    if conn is None:
        return set()
    try:
        ensure_flow_table(conn)
        rows = conn.execute(f"SELECT DISTINCT date FROM {FLOW_TABLE}").fetchall()
        return {str(r[0]) for r in rows if r and r[0]}
    except Exception:
        return set()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def persist_daily_flow(date_norm: str, acc: Dict[str, Dict[str, Any]]) -> int:
    """단일거래일 순매수(외인+기관) 스냅샷을 시계열로 upsert. 반환: 기록 행수."""
    if not date_norm or not acc:
        return 0
    conn = _connect()
    if conn is None:
        return 0
    n = 0
    try:
        ensure_flow_table(conn)
        cur = conn.cursor()
        for code, slot in acc.items():
            if not isinstance(slot, dict):
                continue
            code_s = str(code).zfill(6)
            try:
                krw = float(slot.get("krw") or 0.0)
                vol = float(slot.get("vol") or 0.0)
            except (TypeError, ValueError):
                krw, vol = 0.0, 0.0
            name = str(slot.get("name") or "")
            cur.execute(
                f"INSERT OR REPLACE INTO {FLOW_TABLE}"
                f"(date, code, name, foreign_inst_krw, foreign_inst_vol) VALUES (?,?,?,?,?)",
                (date_norm, code_s, name, krw, vol),
            )
            n += 1
        conn.commit()
    except Exception as ex:
        print(f"⚠️ [수급 시계열] persist 실패: {ex}")
        n = 0
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return n


def get_flow_score(
    code: str,
    conn: Optional[sqlite3.Connection] = None,
    lookback_days: int = 5,
) -> Dict[str, Any]:
    """종목의 최근 수급 모멘텀/다이버전스 → 진입 가산 bonus(0~KR_FLOW_BONUS_MAX).

    반환 dict: found, momentum_krw(누적 순매수), days, price_change_pct, divergence, bonus.
    데이터/테이블 부재 시 모두 0 의 중립값(found=False)으로 수렴 — 진입 로직 무영향.
    conn 을 주면 재사용(진입 핫패스에서 중첩연결 회피), 없으면 단명 연결.
    """
    neutral: Dict[str, Any] = {
        "found": False, "momentum_krw": 0.0, "days": 0,
        "price_change_pct": 0.0, "divergence": 0.0, "bonus": 0.0,
    }
    try:
        code_s = str(code).zfill(6)
    except Exception:
        return neutral
    if code_s in _score_cache:
        return _score_cache[code_s]

    own = False
    c = conn
    try:
        if c is None:
            c = _connect()
            own = True
        if c is None:
            return neutral
        try:
            ensure_flow_table(c)
        except Exception:
            pass

        try:
            rows = c.execute(
                f"SELECT date, foreign_inst_krw FROM {FLOW_TABLE} "
                f"WHERE code=? ORDER BY date DESC LIMIT ?",
                (code_s, int(lookback_days)),
            ).fetchall()
        except Exception:
            rows = []
        if not rows:
            _score_cache[code_s] = neutral
            return neutral

        momentum = 0.0
        for r in rows:
            try:
                momentum += float(r[1] or 0.0)
            except (TypeError, ValueError):
                continue
        days = len(rows)

        # 같은 윈도우의 주가 변화율(횡보 여부) — KR_<code> 일봉 종가.
        price_change_pct = 0.0
        try:
            tbl = f"KR_{code_s}"
            px = c.execute(
                f'SELECT Close FROM "{tbl}" ORDER BY Date DESC LIMIT ?',
                (int(lookback_days) + 1,),
            ).fetchall()
            if px and len(px) >= 2:
                p_new = float(px[0][0])
                p_old = float(px[-1][0])
                if p_old > 0:
                    price_change_pct = (p_new - p_old) / p_old * 100.0
        except Exception:
            price_change_pct = 0.0

        # 모멘텀 정규화: 순매수 대금(원) 로그 스케일(0~1). 음수(순매도)면 0.
        mom_norm = 0.0
        if momentum > 0:
            mom_norm = max(0.0, min(1.0, math.log10(momentum + 1.0) / 10.0))
        # 횡보도: |주가변화| 작을수록 1 (5% 이상이면 0). 다이버전스 = 매집 강도 × 횡보도.
        flat = max(0.0, 1.0 - abs(price_change_pct) / 5.0)
        divergence = round(mom_norm * flat, 4)

        bonus = 0.0
        if momentum > 0:
            bonus = round(_bonus_max() * max(mom_norm * 0.6, divergence), 3)

        out = {
            "found": True,
            "momentum_krw": momentum,
            "days": days,
            "price_change_pct": round(price_change_pct, 2),
            "divergence": divergence,
            "bonus": bonus,
        }
        _score_cache[code_s] = out
        return out
    except Exception:
        return neutral
    finally:
        if own and c is not None:
            try:
                c.close()
            except Exception:
                pass


# ── [Mega-Trend 1번] 섹터 단위 기관/외인 순매수 Z-Score ─────────────────────────


_code_sector_map_cache: Optional[Dict[str, str]] = None


def _flow_z_window_days() -> int:
    try:
        return max(2, int(os.environ.get("MEGA_TREND_FLOW_WINDOW_DAYS", "5")))
    except (TypeError, ValueError):
        return 5


def _flow_z_history_days() -> int:
    try:
        return max(10, int(os.environ.get("MEGA_TREND_FLOW_HISTORY_DAYS", "60")))
    except (TypeError, ValueError):
        return 60


def _flow_z_min_threshold() -> float:
    try:
        return float(os.environ.get("MEGA_TREND_FLOW_Z_MIN", "2.0"))
    except (TypeError, ValueError):
        return 2.0


def build_kr_code_sector_map() -> Dict[str, str]:
    """FDR KOSPI/KOSDAQ 리스트 → 표준 섹터 버킷 (mega-trend·수급 집계 공용)."""
    global _code_sector_map_cache
    if _code_sector_map_cache is not None:
        return _code_sector_map_cache

    out: Dict[str, str] = {}
    try:
        import FinanceDataReader as fdr
        from sector_taxonomy import map_standard_sector

        for tag in ("KOSPI", "KOSDAQ"):
            try:
                lst = fdr.StockListing(tag)
            except Exception:
                continue
            if lst is None or lst.empty:
                continue
            code_col = "Code" if "Code" in lst.columns else "Symbol"
            sec_col = None
            for c in ("Industry", "업종", "Sector", "sector", "분류"):
                if c in lst.columns:
                    sec_col = c
                    break
            if sec_col is None:
                continue
            for _, row in lst.iterrows():
                code = str(row.get(code_col) or "").zfill(6)
                if not code or code == "000000":
                    continue
                raw = str(row.get(sec_col) or "").strip()
                out[code] = map_standard_sector(raw, market="KR")
    except Exception:
        pass

    _code_sector_map_cache = out
    return out


def aggregate_sector_flow_by_date(
    conn: Optional[sqlite3.Connection] = None,
    *,
    history_days: Optional[int] = None,
) -> Dict[str, Dict[str, float]]:
    """{date: {sector: foreign_inst_krw_sum}} — kr_investor_flow 집계."""
    hist = int(history_days or _flow_z_history_days())
    own = False
    c = conn
    if c is None:
        c = _connect()
        own = True
    if c is None:
        return {}

    cmap = build_kr_code_sector_map()
    try:
        ensure_flow_table(c)
        rows = c.execute(
            f"""
            SELECT date, code, foreign_inst_krw
            FROM {FLOW_TABLE}
            ORDER BY date DESC
            LIMIT ?
            """,
            (hist * 800,),
        ).fetchall()
    except Exception:
        rows = []
    finally:
        if own and c is not None:
            try:
                c.close()
            except Exception:
                pass

    by_date: Dict[str, Dict[str, float]] = {}
    seen_dates: Set[str] = set()
    for date_s, code, krw in rows or []:
        d = str(date_s or "")[:10]
        if not d:
            continue
        if d not in seen_dates and len(seen_dates) >= hist:
            continue
        seen_dates.add(d)
        sec = cmap.get(str(code).zfill(6), "미분류(원시)")
        try:
            v = float(krw or 0.0)
        except (TypeError, ValueError):
            v = 0.0
        bucket = by_date.setdefault(d, {})
        bucket[sec] = bucket.get(sec, 0.0) + v
    return by_date


def compute_sector_flow_zscore(
    sector: str,
    conn: Optional[sqlite3.Connection] = None,
    *,
    window_days: Optional[int] = None,
    history_days: Optional[int] = None,
) -> Dict[str, Any]:
    """
    섹터 누적 순매수(외인+기관) Z-Score — 최근 window일 합 vs 과거 window일 합 분포.
    """
    from sector_taxonomy import map_standard_sector

    sec = map_standard_sector(sector, market="KR")
    win = int(window_days or _flow_z_window_days())
    hist = int(history_days or _flow_z_history_days())
    neutral: Dict[str, Any] = {
        "sector": sec,
        "z_score": None,
        "window_krw": 0.0,
        "n_windows": 0,
        "neutral": True,
        "reason": "insufficient_data",
    }

    by_date = aggregate_sector_flow_by_date(conn, history_days=hist)
    dates = sorted(by_date.keys())
    if len(dates) < win + 5:
        return neutral

    def _window_sum(end_idx: int) -> float:
        start = max(0, end_idx - win + 1)
        total = 0.0
        for i in range(start, end_idx + 1):
            total += float(by_date[dates[i]].get(sec, 0.0))
        return total

    windows = [_window_sum(i) for i in range(win - 1, len(dates))]
    if len(windows) < 5:
        return neutral

    current = float(windows[-1])
    prior = windows[:-1]
    mean = sum(prior) / len(prior)
    var = sum((x - mean) ** 2 for x in prior) / max(1, len(prior) - 1)
    std = math.sqrt(var) if var > 1e-12 else 0.0
    if std <= 1e-6:
        return {
            **neutral,
            "window_krw": current,
            "n_windows": len(windows),
            "reason": "zero_std",
        }

    z = (current - mean) / std
    return {
        "sector": sec,
        "z_score": round(float(z), 4),
        "window_krw": round(current, 2),
        "window_mean_krw": round(mean, 2),
        "window_std_krw": round(std, 2),
        "n_windows": len(windows),
        "window_days": win,
        "neutral": False,
        "strong_inflow": float(z) >= _flow_z_min_threshold(),
        "reason": "computed",
    }


def compute_all_sector_flow_zscores(
    conn: Optional[sqlite3.Connection] = None,
    *,
    sectors: Optional[Sequence[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """전 섹터(또는 지정 목록) flow Z-Score 맵."""
    if sectors:
        targets = [str(s) for s in sectors]
    else:
        by_date = aggregate_sector_flow_by_date(conn)
        targets = sorted(
            {sec for day in by_date.values() for sec in day.keys() if sec}
        )
    out: Dict[str, Dict[str, Any]] = {}
    for sec in targets:
        out[sec] = compute_sector_flow_zscore(sec, conn=conn)
    return out

