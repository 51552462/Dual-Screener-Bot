"""
[P1-3b] OpenDART 공시 이벤트 적재 + 진입 교차검증 팩터. **OPENDART_API_KEY 있을 때만 활성화.**

- 수집: OpenDART `list.json`(전 종목 최근 공시) → 보고서명(report_nm) 키워드로 호재/악재 분류.
    · 호재(+): 자기주식취득·소각, 단일판매/공급계약, 흑자전환.
    · 악재(-): 유상증자, 전환사채/신주인수권부/교환사채(희석), 감자, 횡령·배임, 감사의견 비적정.
- 저장: market_data.sqlite `kr_dart_events(date, code, sign, report_nm)`.
- 산출: 최근 N일 공시 부호 합 → 진입 가산(호재)/경계(악재).

설계 원칙: 키 부재/요청 실패/레이트리밋 시 전부 no-op(이벤트 0 → 중립) → 기존 진입 무영향.
파일·DB 모두 방어적(읽기 위주, busy_timeout). 일일 적재는 KR 일일 잡에 피기백(키 있을 때만 동작).
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore

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

DART_TABLE = "kr_dart_events"
DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"

# 보고서명 키워드 → 부호(+호재 / -악재). 순서대로 첫 매칭 적용.
_POSITIVE_KEYS = (
    "자기주식취득", "자기주식 취득", "자기주식취득신탁", "자기주식소각", "자기주식 소각",
    "단일판매ㆍ공급계약", "단일판매·공급계약", "공급계약", "흑자전환",
)
_NEGATIVE_KEYS = (
    "유상증자", "전환사채", "신주인수권부사채", "교환사채", "무상감자", "감자결정",
    "횡령", "배임", "감사의견", "거래정지", "관리종목", "상장폐지",
)

_score_cache: Dict[str, Dict[str, Any]] = {}


def _api_key() -> str:
    return (os.environ.get("OPENDART_API_KEY") or "").strip()


def is_enabled() -> bool:
    return bool(_api_key()) and requests is not None


def _bonus_max() -> float:
    try:
        return max(0.0, float(os.environ.get("KR_DART_BONUS_MAX", "5")))
    except (TypeError, ValueError):
        return 5.0


def _penalty_max() -> float:
    try:
        return max(0.0, float(os.environ.get("KR_DART_PENALTY_MAX", "6")))
    except (TypeError, ValueError):
        return 6.0


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


def ensure_dart_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS {DART_TABLE} (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            sign INTEGER DEFAULT 0,
            report_nm TEXT,
            PRIMARY KEY (date, code, report_nm)
        )"""
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{DART_TABLE}_code ON {DART_TABLE}(code)"
    )


def _classify(report_nm: str) -> int:
    s = str(report_nm or "")
    for k in _POSITIVE_KEYS:
        if k in s:
            return 1
    for k in _NEGATIVE_KEYS:
        if k in s:
            return -1
    return 0


def _fetch_list(bgn_de: str, end_de: str, max_pages: int = 10) -> List[Dict[str, Any]]:
    """기간 내 전 종목 공시 목록(페이지네이션). 키/네트워크 실패 시 빈 리스트."""
    if not is_enabled():
        return []
    out: List[Dict[str, Any]] = []
    key = _api_key()
    for page in range(1, int(max_pages) + 1):
        try:
            r = requests.get(
                DART_LIST_URL,
                params={
                    "crtfc_key": key,
                    "bgn_de": bgn_de,
                    "end_de": end_de,
                    "page_no": page,
                    "page_count": 100,
                },
                timeout=20,
            )
            data = r.json()
        except Exception:
            break
        status = str(data.get("status", ""))
        if status != "000":
            # 013=조회데이터 없음, 그 외=키/한도 오류 → 조용히 종료
            break
        rows = data.get("list") or []
        out.extend(rows)
        try:
            total_page = int(data.get("total_page", 1))
        except (TypeError, ValueError):
            total_page = 1
        if page >= total_page:
            break
    return out


def backfill_recent(days: int = 3, max_pages: int = 20) -> int:
    """최근 N일 공시를 적재(호재/악재만 기록). 키 부재 시 0."""
    if not is_enabled():
        return 0
    conn = _connect()
    if conn is None:
        return 0
    n = 0
    try:
        ensure_dart_table(conn)
        end_de = datetime.now().strftime("%Y%m%d")
        bgn_de = (datetime.now() - timedelta(days=int(days))).strftime("%Y%m%d")
        rows = _fetch_list(bgn_de, end_de, max_pages=max_pages)
        cur = conn.cursor()
        for it in rows:
            code = str(it.get("stock_code") or "").strip()
            if not code or len(code) != 6:
                continue
            report_nm = str(it.get("report_nm") or "").strip()
            sign = _classify(report_nm)
            if sign == 0:
                continue
            rcept = str(it.get("rcept_dt") or "").strip()
            if len(rcept) == 8:
                d_norm = f"{rcept[:4]}-{rcept[4:6]}-{rcept[6:8]}"
            else:
                d_norm = end_de[:4] + "-" + end_de[4:6] + "-" + end_de[6:8]
            cur.execute(
                f"INSERT OR REPLACE INTO {DART_TABLE}(date, code, sign, report_nm) "
                f"VALUES (?,?,?,?)",
                (d_norm, code.zfill(6), int(sign), report_nm[:120]),
            )
            n += 1
        conn.commit()
        if n:
            print(f"📰 [DART] kr_dart_events 적재: {n}건 (기간 {bgn_de}~{end_de})")
    except Exception as ex:
        print(f"⚠️ [DART] 백필 스킵(비치명적): {ex}")
        n = 0
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return n


def get_dart_event_score(
    code: str,
    conn: Optional[sqlite3.Connection] = None,
    lookback_days: int = 10,
) -> Dict[str, Any]:
    """최근 공시 부호 합 → 진입 조정(net = 호재 bonus - 악재 penalty).

    반환: found, pos, neg, bonus, penalty, net, last_report.
    테이블/데이터 부재(키 미설정 포함) 시 중립 0.
    """
    neutral: Dict[str, Any] = {
        "found": False, "pos": 0, "neg": 0,
        "bonus": 0.0, "penalty": 0.0, "net": 0.0, "last_report": "",
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
            ensure_dart_table(c)
        except Exception:
            pass
        cutoff = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y-%m-%d")
        try:
            rows = c.execute(
                f"SELECT sign, report_nm FROM {DART_TABLE} "
                f"WHERE code=? AND date >= ? ORDER BY date DESC",
                (code_s, cutoff),
            ).fetchall()
        except Exception:
            rows = []
        if not rows:
            _score_cache[code_s] = neutral
            return neutral

        pos = sum(1 for r in rows if int(r[0] or 0) > 0)
        neg = sum(1 for r in rows if int(r[0] or 0) < 0)
        last_report = str(rows[0][1] or "") if rows else ""

        bonus = round(min(_bonus_max(), _bonus_max() * (min(pos, 3) / 3.0)), 3) if pos else 0.0
        penalty = round(min(_penalty_max(), _penalty_max() * (min(neg, 3) / 3.0)), 3) if neg else 0.0

        out = {
            "found": True, "pos": pos, "neg": neg,
            "bonus": bonus, "penalty": penalty, "net": round(bonus - penalty, 3),
            "last_report": last_report[:60],
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
    if not is_enabled():
        print("ℹ️ [DART] OPENDART_API_KEY 미설정 — 공시 수집 비활성(중립). .env 에 키 추가 시 자동 가동.")
        return
    backfill_recent()


if __name__ == "__main__":
    run()
