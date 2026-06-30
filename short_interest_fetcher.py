"""
[P1-4] 공매도/대차잔고 시계열 적재 + 숏스퀴즈/크라우디드 숏 진입 팩터.

- 수집: pykrx `get_shorting_volume_by_ticker(date, market)`(공매도 거래비중) +
  `get_shorting_balance_by_ticker`(공매도 잔고비중) — 하루 시장당 1콜로 전 종목 확보(저비용).
- 저장: market_data.sqlite `kr_short_interest(date, code, short_ratio, short_balance_ratio)`.
- 산출: 종목별 최근 공매도 추세 →
    · 스퀴즈 후보(공매도 비중 높았다 급감 + 주가 반등) → 진입 가산(bonus, 상한가/슈퍼노바 시너지).
    · 크라우디드 숏(공매도 비중 급증) → 진입 경계(penalty 감점).

설계 원칙: pykrx 부재/실패 시 조용히 스킵, 데이터 없으면 중립 0 → 기존 진입 로직 무영향.
읽기 위주·busy_timeout 통일(무DB락). 일일 갱신은 smart_money_tracker 의 KR 일일 잡에 피기백.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

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

SHORT_TABLE = "kr_short_interest"

_score_cache: Dict[str, Dict[str, Any]] = {}


def _bonus_max() -> float:
    try:
        return max(0.0, float(os.environ.get("KR_SHORT_BONUS_MAX", "6")))
    except (TypeError, ValueError):
        return 6.0


def _penalty_max() -> float:
    try:
        return max(0.0, float(os.environ.get("KR_SHORT_PENALTY_MAX", "4")))
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


def ensure_short_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS {SHORT_TABLE} (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            short_ratio REAL DEFAULT 0,
            short_balance_ratio REAL DEFAULT 0,
            PRIMARY KEY (date, code)
        )"""
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{SHORT_TABLE}_code ON {SHORT_TABLE}(code)"
    )


def existing_short_dates() -> Set[str]:
    conn = _connect()
    if conn is None:
        return set()
    try:
        ensure_short_table(conn)
        rows = conn.execute(f"SELECT DISTINCT date FROM {SHORT_TABLE}").fetchall()
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
        if any(n in s for n in needles):
            return c
    return None


def _fetch_one_day(date_yyyymmdd: str) -> Dict[str, Dict[str, float]]:
    """단일거래일 전 종목 공매도 비중/잔고비중. {code: {ratio, bal_ratio}}."""
    if _krx is None:
        return {}
    acc: Dict[str, Dict[str, float]] = {}
    for market in ("KOSPI", "KOSDAQ"):
        # 1) 공매도 거래비중
        try:
            dv = _krx.get_shorting_volume_by_ticker(date_yyyymmdd, market)
        except Exception:
            dv = None
        if dv is not None and not getattr(dv, "empty", True):
            ratio_c = _col(dv, "비중")
            if ratio_c is not None:
                for code, row in dv.iterrows():
                    try:
                        r = float(row.get(ratio_c, 0) or 0)
                    except (TypeError, ValueError):
                        continue
                    acc.setdefault(str(code).zfill(6), {})["ratio"] = r
        # 2) 공매도 잔고비중(가용 시)
        try:
            bal = _krx.get_shorting_balance_by_ticker(date_yyyymmdd, market)
        except Exception:
            bal = None
        if bal is not None and not getattr(bal, "empty", True):
            bal_c = _col(bal, "비중")
            if bal_c is not None:
                for code, row in bal.iterrows():
                    try:
                        b = float(row.get(bal_c, 0) or 0)
                    except (TypeError, ValueError):
                        continue
                    acc.setdefault(str(code).zfill(6), {})["bal_ratio"] = b
    return acc


def persist_daily_short(date_norm: str, acc: Dict[str, Dict[str, float]]) -> int:
    if not date_norm or not acc:
        return 0
    conn = _connect()
    if conn is None:
        return 0
    n = 0
    try:
        ensure_short_table(conn)
        cur = conn.cursor()
        for code, slot in acc.items():
            cur.execute(
                f"INSERT OR REPLACE INTO {SHORT_TABLE}"
                f"(date, code, short_ratio, short_balance_ratio) VALUES (?,?,?,?)",
                (
                    date_norm,
                    str(code).zfill(6),
                    float(slot.get("ratio", 0.0) or 0.0),
                    float(slot.get("bal_ratio", 0.0) or 0.0),
                ),
            )
            n += 1
        conn.commit()
    except Exception as ex:
        print(f"⚠️ [공매도 시계열] persist 실패: {ex}")
        n = 0
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return n


def _recent_trade_dates(n: int = 6) -> List[str]:
    """최근 영업일(YYYYMMDD) — pykrx 영업일 캘린더, 실패 시 단순 평일 폴백."""
    if _krx is not None:
        try:
            to = datetime.now().strftime("%Y%m%d")
            frm = (datetime.now() - timedelta(days=20)).strftime("%Y%m%d")
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


def backfill_recent(days: int = 6) -> int:
    """최근 N영업일 중 미적재분만 공매도 시계열로 백필. 반환: 기록 행수."""
    if _krx is None:
        return 0
    try:
        have = existing_short_dates()
        total = 0
        for d in _recent_trade_dates(days):
            d_norm = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            if d_norm in have:
                continue
            acc = _fetch_one_day(d)
            if not acc:
                continue
            total += persist_daily_short(d_norm, acc)
        if total:
            print(f"🩳 [공매도 시계열] kr_short_interest 적재/갱신: {total}행")
        return total
    except Exception as ex:
        print(f"⚠️ [공매도 시계열] 백필 스킵(비치명적): {ex}")
        return 0


def get_short_score(
    code: str,
    conn: Optional[sqlite3.Connection] = None,
    lookback_days: int = 6,
) -> Dict[str, Any]:
    """공매도 추세 → 진입 조정(net = squeeze bonus - crowded penalty).

    반환: found, short_ratio(최근), delta_pp(최근-과거평균), price_change_pct,
    squeeze(bool), crowded(bool), bonus, penalty, net(=bonus-penalty).
    데이터/테이블 부재 시 전부 0 중립.
    """
    neutral: Dict[str, Any] = {
        "found": False, "short_ratio": 0.0, "delta_pp": 0.0,
        "price_change_pct": 0.0, "squeeze": False, "crowded": False,
        "bonus": 0.0, "penalty": 0.0, "net": 0.0,
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
            ensure_short_table(c)
        except Exception:
            pass
        try:
            rows = c.execute(
                f"SELECT date, short_ratio FROM {SHORT_TABLE} "
                f"WHERE code=? ORDER BY date DESC LIMIT ?",
                (code_s, int(lookback_days)),
            ).fetchall()
        except Exception:
            rows = []
        if not rows or len(rows) < 2:
            _score_cache[code_s] = neutral
            return neutral

        ratios = []
        for r in rows:
            try:
                ratios.append(float(r[1] or 0.0))
            except (TypeError, ValueError):
                ratios.append(0.0)
        recent = ratios[0]
        prior = ratios[1:]
        prior_avg = sum(prior) / len(prior) if prior else recent
        delta = recent - prior_avg  # %p (양수=공매도 증가)

        # 주가 변화율(반등 여부)
        price_change_pct = 0.0
        try:
            px = c.execute(
                f'SELECT Close FROM "KR_{code_s}" ORDER BY Date DESC LIMIT ?',
                (int(lookback_days) + 1,),
            ).fetchall()
            if px and len(px) >= 2:
                p_new = float(px[0][0]); p_old = float(px[-1][0])
                if p_old > 0:
                    price_change_pct = (p_new - p_old) / p_old * 100.0
        except Exception:
            price_change_pct = 0.0

        # 임계값(환경변수 조정 가능)
        try:
            squeeze_min = float(os.environ.get("KR_SHORT_SQUEEZE_MIN_RATIO", "3.0"))
            drop_pp = float(os.environ.get("KR_SHORT_DROP_PP", "1.0"))
            rise_pp = float(os.environ.get("KR_SHORT_RISE_PP", "1.5"))
        except (TypeError, ValueError):
            squeeze_min, drop_pp, rise_pp = 3.0, 1.0, 1.5

        bonus = 0.0
        penalty = 0.0
        squeeze = False
        crowded = False

        # 스퀴즈 후보: 공매도 비중이 높았다(과거평균≥min) + 급감(delta≤-drop) + 주가 반등(>0)
        if prior_avg >= squeeze_min and delta <= -drop_pp and price_change_pct > 0:
            squeeze = True
            mag = min(1.0, (abs(delta) / max(drop_pp, 1e-9)) / 3.0 + prior_avg / 20.0)
            bonus = round(_bonus_max() * max(0.0, min(1.0, mag)), 3)
        # 크라우디드 숏: 공매도 비중 급증(delta≥rise) — 하락 위험 경계
        elif delta >= rise_pp:
            crowded = True
            mag = min(1.0, delta / max(rise_pp * 3.0, 1e-9))
            penalty = round(_penalty_max() * mag, 3)

        out = {
            "found": True,
            "short_ratio": round(recent, 3),
            "delta_pp": round(delta, 3),
            "price_change_pct": round(price_change_pct, 2),
            "squeeze": squeeze,
            "crowded": crowded,
            "bonus": bonus,
            "penalty": penalty,
            "net": round(bonus - penalty, 3),
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
    """단독 실행 엔트리(`python -m short_interest_fetcher`) — 최근분 백필."""
    backfill_recent()


if __name__ == "__main__":
    run()
