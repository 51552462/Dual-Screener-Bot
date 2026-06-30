"""
[P1-3a] 펀더멘털(가치·퀄리티) 시계열 적재 + 진입 교차검증 팩터. **API 키 불필요.**

- 수집: pykrx `get_market_fundamental_by_ticker(date, market)` — 하루 시장당 1콜로 전 종목
  PER/PBR/EPS/BPS/DIV 확보(저비용). ROE 는 EPS/BPS 프록시.
- 저장: market_data.sqlite `kr_fundamentals(date, code, per, pbr, eps, bps, roe_proxy)`.
- 산출: 저평가(저PBR/적정PER) + 흑자/고ROE = 가산, 적자/거품 = 경계. 기술적 신호와 교차검증.

설계 원칙: pykrx 부재/실패·결측(0=N/A) 시 중립 0 → 기존 진입 무영향. 읽기 위주·busy_timeout 통일.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

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

try:
    from pykrx import stock as _krx
except Exception:  # pragma: no cover
    _krx = None

FUND_TABLE = "kr_fundamentals"

_score_cache: Dict[str, Dict[str, Any]] = {}


def _bonus_max() -> float:
    try:
        return max(0.0, float(os.environ.get("KR_FUND_BONUS_MAX", "5")))
    except (TypeError, ValueError):
        return 5.0


def _penalty_max() -> float:
    try:
        return max(0.0, float(os.environ.get("KR_FUND_PENALTY_MAX", "4")))
    except (TypeError, ValueError):
        return 4.0


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


def ensure_fund_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS {FUND_TABLE} (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            per REAL DEFAULT 0,
            pbr REAL DEFAULT 0,
            eps REAL DEFAULT 0,
            bps REAL DEFAULT 0,
            roe_proxy REAL DEFAULT 0,
            PRIMARY KEY (date, code)
        )"""
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{FUND_TABLE}_code ON {FUND_TABLE}(code)"
    )


def existing_fund_dates() -> Set[str]:
    conn = _connect()
    if conn is None:
        return set()
    try:
        ensure_fund_table(conn)
        rows = conn.execute(f"SELECT DISTINCT date FROM {FUND_TABLE}").fetchall()
        return {str(r[0]) for r in rows if r and r[0]}
    except Exception:
        return set()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _col(df, *needles: str) -> Optional[str]:
    for c in df.columns:
        s = str(c)
        if any(n == s or n in s for n in needles):
            return c
    return None


def _fetch_one_day(date_yyyymmdd: str) -> Dict[str, Dict[str, float]]:
    if _krx is None:
        return {}
    acc: Dict[str, Dict[str, float]] = {}
    for market in ("KOSPI", "KOSDAQ"):
        try:
            df = _krx.get_market_fundamental_by_ticker(date_yyyymmdd, market=market)
        except Exception:
            df = None
        if df is None or getattr(df, "empty", True):
            continue
        per_c = _col(df, "PER")
        pbr_c = _col(df, "PBR")
        eps_c = _col(df, "EPS")
        bps_c = _col(df, "BPS")
        for code, row in df.iterrows():
            def _g(c):
                if c is None:
                    return 0.0
                try:
                    return float(row.get(c, 0) or 0)
                except (TypeError, ValueError):
                    return 0.0
            per = _g(per_c); pbr = _g(pbr_c); eps = _g(eps_c); bps = _g(bps_c)
            roe = (eps / bps * 100.0) if bps and bps != 0 else 0.0
            acc[str(code).zfill(6)] = {
                "per": per, "pbr": pbr, "eps": eps, "bps": bps, "roe_proxy": roe,
            }
    return acc


def persist_daily_fundamentals(date_norm: str, acc: Dict[str, Dict[str, float]]) -> int:
    if not date_norm or not acc:
        return 0
    conn = _connect()
    if conn is None:
        return 0
    n = 0
    try:
        ensure_fund_table(conn)
        cur = conn.cursor()
        for code, slot in acc.items():
            cur.execute(
                f"INSERT OR REPLACE INTO {FUND_TABLE}"
                f"(date, code, per, pbr, eps, bps, roe_proxy) VALUES (?,?,?,?,?,?,?)",
                (
                    date_norm, str(code).zfill(6),
                    float(slot.get("per", 0.0) or 0.0),
                    float(slot.get("pbr", 0.0) or 0.0),
                    float(slot.get("eps", 0.0) or 0.0),
                    float(slot.get("bps", 0.0) or 0.0),
                    float(slot.get("roe_proxy", 0.0) or 0.0),
                ),
            )
            n += 1
        conn.commit()
    except Exception as ex:
        print(f"⚠️ [펀더멘털] persist 실패: {ex}")
        n = 0
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return n


def _recent_trade_dates(n: int = 2) -> List[str]:
    if _krx is not None:
        try:
            to = datetime.now().strftime("%Y%m%d")
            frm = (datetime.now() - timedelta(days=14)).strftime("%Y%m%d")
            days = _krx.get_previous_business_days(fromdate=frm, todate=to)
            if days:
                return [d.strftime("%Y%m%d") for d in days][-n:]
        except Exception:
            pass
    out: List[str] = []
    d = datetime.now()
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return list(reversed(out))


def backfill_recent(days: int = 2) -> int:
    """펀더멘털은 일변동이 작으므로 최근 1~2영업일만 스냅샷 갱신."""
    if _krx is None:
        return 0
    try:
        have = existing_fund_dates()
        total = 0
        for d in _recent_trade_dates(days):
            d_norm = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            if d_norm in have:
                continue
            acc = _fetch_one_day(d)
            if not acc:
                continue
            total += persist_daily_fundamentals(d_norm, acc)
        if total:
            print(f"📚 [펀더멘털] kr_fundamentals 적재/갱신: {total}행")
        return total
    except Exception as ex:
        print(f"⚠️ [펀더멘털] 백필 스킵(비치명적): {ex}")
        return 0


def get_fundamental_score(
    code: str, conn: Optional[sqlite3.Connection] = None
) -> Dict[str, Any]:
    """가치·퀄리티 → 진입 조정(net = value/quality bonus - 적자/거품 penalty).

    반환: found, per, pbr, eps, roe_proxy, value, quality, bonus, penalty, net.
    결측(0=N/A)·테이블 부재 시 중립 0.
    """
    neutral: Dict[str, Any] = {
        "found": False, "per": 0.0, "pbr": 0.0, "eps": 0.0, "roe_proxy": 0.0,
        "value": 0.0, "quality": 0.0, "bonus": 0.0, "penalty": 0.0, "net": 0.0,
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
            ensure_fund_table(c)
        except Exception:
            pass
        try:
            row = c.execute(
                f"SELECT per, pbr, eps, roe_proxy FROM {FUND_TABLE} "
                f"WHERE code=? ORDER BY date DESC LIMIT 1",
                (code_s,),
            ).fetchone()
        except Exception:
            row = None
        if not row:
            _score_cache[code_s] = neutral
            return neutral

        try:
            per = float(row[0] or 0.0); pbr = float(row[1] or 0.0)
            eps = float(row[2] or 0.0); roe = float(row[3] or 0.0)
        except (TypeError, ValueError):
            _score_cache[code_s] = neutral
            return neutral

        # 가치 점수(0~1): 저PBR + 적정PER
        value = 0.0
        if 0 < pbr <= 1.0:
            value += 0.6
        elif 1.0 < pbr <= 2.0:
            value += 0.3
        if 0 < per <= 10.0:
            value += 0.4
        elif 10.0 < per <= 20.0:
            value += 0.2
        value = min(1.0, value)

        # 퀄리티 점수(0~1): 흑자 + 고ROE(프록시)
        quality = 0.0
        if eps > 0:
            quality += 0.4
        if roe >= 15.0:
            quality += 0.6
        elif roe >= 8.0:
            quality += 0.3
        quality = min(1.0, quality)

        bonus = round(_bonus_max() * (value * 0.5 + quality * 0.5), 3)

        # 경계: 적자(EPS≤0, 단 EPS 데이터 존재 시) 또는 PBR 거품(>5)
        penalty = 0.0
        if eps < 0:
            penalty += _penalty_max() * 0.6
        if pbr > 5.0:
            penalty += _penalty_max() * 0.4
        penalty = round(min(_penalty_max(), penalty), 3)

        out = {
            "found": True, "per": round(per, 2), "pbr": round(pbr, 2),
            "eps": round(eps, 1), "roe_proxy": round(roe, 1),
            "value": round(value, 3), "quality": round(quality, 3),
            "bonus": bonus, "penalty": penalty, "net": round(bonus - penalty, 3),
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


def run() -> None:
    backfill_recent()


if __name__ == "__main__":
    run()
