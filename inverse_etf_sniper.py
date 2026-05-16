"""
Inverse ETF Sniper — 테일 리스크 전용 슬리브 (롱 팩토리 미사용).

- 자금: TAIL_RISK_FUND_KR / TAIL_RISK_FUND_US 만 OCC(`update_config_value`)로 Reserve/Release.
- CENTRAL_TREASURY_* 는 읽지도 쓰지도 않는다.
- 장부: `forward_trades` 에 sig_type 에 `[INVERSE_ETF]` 마커가 포함된 행만 인버스로 간주.
- 킬 스위치: INVERSE_MODE_ACTIVE=False 인데 OPEN 인버스가 남아 있으면 전량 시장가 청산 후 테일로 반환.
- 진입: 테일 잔액의 30% 하드캡, OPEN 인버스가 1건이라도 있으면 신규 진입 금지.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Callable, Optional

import FinanceDataReader as fdr
import numpy as np
import pandas as pd
import pytz
import yfinance as yf

from auto_forward_tester import DB_PATH, init_forward_db
from config_manager import (
    ConfigConcurrencyError,
    get_config_value,
    load_system_config,
    update_config_value,
)

# ---------------------------------------------------------------------------
# 식별자 / 유니버스 (롱 팩토리 로직 미사용 — 스나이퍼 전용 상수)
# ---------------------------------------------------------------------------
INVERSE_SIG_MARKER = "[INVERSE_ETF]"
INVERSE_HARD_CAP_PCT = 0.30

INVERSE_CANDIDATES: list[dict[str, str]] = [
    {
        "market": "US",
        "code": "SQQQ",
        "name": "ProShares UltraPro Short QQQ",
        "hedge": "QQQ",
        "trigger_ret_5d": "-2.0",
    },
    {
        "market": "US",
        "code": "SOXS",
        "name": "Direxion Daily Semiconductor Bear 3X",
        "hedge": "SOXX",
        "trigger_ret_5d": "-2.0",
    },
    {
        "market": "KR",
        "code": "252670",
        "name": "KODEX 200선물인버스2X",
        "hedge": "069500",
        "trigger_ret_5d": "-1.5",
    },
]


def _tail_fund_key(market: str) -> str:
    return f"TAIL_RISK_FUND_{market.upper()}"


def _numeric_tail_balance(key: str) -> float:
    """KV 단독 키 → 없으면 병합 설정 뷰에서 조회."""
    v = get_config_value(key, None)
    if v is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0
    cfg = load_system_config()
    try:
        return float(cfg.get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _inverse_mode_active(cfg: dict[str, Any]) -> bool:
    return bool(cfg.get("INVERSE_MODE_ACTIVE", False))


def _fetch_hedge_5d_return_pct(market: str, hedge_code: str) -> Optional[float]:
    """언더라이어 최근 약 5거래일 누적 수익률(%). 실패 시 None."""
    try:
        end = datetime.now()
        start = end - pd.Timedelta(days=14)
        st = start.strftime("%Y-%m-%d")
        ed = end.strftime("%Y-%m-%d")
        if market.upper() == "KR":
            raw = fdr.DataReader(hedge_code, st, ed)
        else:
            raw = yf.download(hedge_code, start=st, end=ed, progress=False, auto_adjust=False)
        if raw is None or getattr(raw, "empty", True):
            return None
        if isinstance(raw.columns, pd.MultiIndex) and "Close" in raw.columns.get_level_values(0):
            c = raw["Close"].iloc[:, 0].dropna()
        else:
            c = raw["Close"].dropna()
        if len(c) < 2:
            return None
        tail = c.iloc[-5:] if len(c) >= 5 else c
        r = (float(tail.iloc[-1]) / float(tail.iloc[0]) - 1.0) * 100.0
        return float(r) if np.isfinite(r) else None
    except Exception:
        return None


def _fetch_last_close(market: str, code: str) -> Optional[float]:
    try:
        end = datetime.now()
        st = (end - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
        ed = end.strftime("%Y-%m-%d")
        if market.upper() == "KR":
            raw = fdr.DataReader(code, st, ed)
        else:
            raw = yf.download(code, start=st, end=ed, progress=False, auto_adjust=False)
        if raw is None or getattr(raw, "empty", True):
            return None
        if isinstance(raw.columns, pd.MultiIndex) and "Close" in raw.columns.get_level_values(0):
            s = raw["Close"].iloc[:, 0].dropna()
        else:
            s = raw["Close"].dropna()
        if s.empty:
            return None
        v = float(s.iloc[-1])
        return v if np.isfinite(v) else None
    except Exception:
        return None


class _InsufficientTail(RuntimeError):
    """테일 잔액 부족 — OCC modifier 에서만 사용."""


def _occ_modify_tail(key: str, modifier: Callable[[float], float]) -> float:
    def _wrap(old: Any) -> Any:
        cur = float(old) if old is not None else _numeric_tail_balance(key)
        return modifier(cur)

    return update_config_value(key, _wrap)


def reserve_tail_amount(market: str, amount: float) -> bool:
    """테일에서 amount 만큼 차감(OCC). 성공 여부."""
    if amount <= 0:
        return False
    key = _tail_fund_key(market)

    def _debit(cur: float) -> float:
        if cur + 1e-9 < amount:
            raise _InsufficientTail()
        return round(cur - amount, 2)

    try:
        _occ_modify_tail(key, _debit)
        return True
    except (_InsufficientTail, ConfigConcurrencyError):
        return False


def release_tail_amount(market: str, amount: float) -> None:
    """청산·킬스위치 회수분을 테일로 반환(OCC)."""
    if amount <= 0:
        return
    key = _tail_fund_key(market)

    def _credit(cur: float) -> float:
        return round(cur + float(amount), 2)

    try:
        _occ_modify_tail(key, _credit)
    except ConfigConcurrencyError:
        # 마지막 시도 실패 시 로그만 — 운영자 재시도
        print(f"🚨 [inverse_etf_sniper] release_tail_amount OCC 실패: {key} +{amount}")


def _inverse_open_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    cur = conn.execute(
        f"""
        SELECT * FROM forward_trades
        WHERE status = 'OPEN' AND IFNULL(sig_type,'') LIKE ?
        """,
        (f"%{INVERSE_SIG_MARKER}%",),
    )
    return list(cur.fetchall())


def _close_inverse_row_at_market(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    exit_price: float,
    exit_reason: str,
) -> float:
    """
    단일 인버스 OPEN 행을 시장가 청산 처리. 반환: 테일로 돌려줄 회수 금액(원금+손익 근사).
    """
    rid = int(row["id"])
    entry_price = float(row["entry_price"] or 0.0)
    invest_amount = float(row["invest_amount"] or 0.0)
    if entry_price <= 0 or exit_price <= 0:
        final_ret = 0.0
    else:
        final_ret = (exit_price - entry_price) / entry_price * 100.0
    status = "CLOSED_WIN" if final_ret >= 0 else "CLOSED_LOSS"
    tz_kr = pytz.timezone("Asia/Seoul")
    tz_us = pytz.timezone("America/New_York")
    mkt = str(row["market"] or "").upper()
    today_str = datetime.now(tz_us if mkt == "US" else tz_kr).strftime("%Y-%m-%d")
    recovered = invest_amount * (1.0 + final_ret / 100.0) if invest_amount > 0 else 0.0
    prev_high = float(row["max_high"] or entry_price or exit_price)
    prev_low = float(row["min_low"] or entry_price or exit_price)
    new_high = max(prev_high, exit_price)
    new_low = min(prev_low, exit_price)
    conn.execute(
        """
        UPDATE forward_trades
        SET status = ?, exit_date = ?, exit_reason = ?, final_ret = ?, max_high = ?, min_low = ?
        WHERE id = ?
        """,
        (status, today_str, exit_reason, round(final_ret, 4), new_high, new_low, rid),
    )
    return max(0.0, float(recovered))


def enforce_inverse_kill_switch(conn: sqlite3.Connection, cfg: dict[str, Any]) -> int:
    """
    INVERSE_MODE_ACTIVE=False 이고 OPEN 인버스가 있으면 전량 청산 + 테일 반환.
    반환: 청산한 건수.
    """
    if _inverse_mode_active(cfg):
        return 0
    rows = _inverse_open_rows(conn)
    if not rows:
        return 0
    n = 0
    for row in rows:
        mkt = str(row["market"] or "").upper()
        code = str(row["code"] or "").strip()
        px = _fetch_last_close(mkt, code)
        if px is None:
            ep = float(row["entry_price"] or 0.0)
            px = ep if ep > 0 else 0.01
        recovered = _close_inverse_row_at_market(
            conn,
            row,
            px,
            "KILL_SWITCH_INVERSE_MODE_OFF",
        )
        release_tail_amount(mkt, recovered)
        n += 1
    conn.commit()
    print(f"🛡️ [inverse_etf_sniper] 킬 스위치: 인버스 OPEN {n}건 시장가 청산 후 테일 반환.")
    return n


def _insert_inverse_forward_trade(
    conn: sqlite3.Connection,
    *,
    market: str,
    code: str,
    name: str,
    entry_price: float,
    invest_amount: float,
    shares: int,
) -> None:
    init_forward_db()
    tz_kr = pytz.timezone("Asia/Seoul")
    tz_us = pytz.timezone("America/New_York")
    mkt = market.upper()
    today_str = datetime.now(tz_us if mkt == "US" else tz_kr).strftime("%Y-%m-%d")
    sig_type = f"Dante_INVERSE_ETF_Sniper[V1]{INVERSE_SIG_MARKER}"
    sector = "InverseETF-TailSleeve"
    tier = "INVERSE"
    score = 0.0
    ep = float(entry_price)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO forward_trades
        (entry_date, market, code, name, sector, sig_type, tier, total_score,
         dyn_rs, dyn_cpv, dyn_tb, entry_price, v_cpv, v_yang, v_energy, v_rs,
         max_high, min_low, market_breadth, entry_breadth, entry_cos_score, entry_dtw_score,
         entry_atr, invest_amount, shares, sim_kelly_invest, entry_regime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?, ?, 1.0, 1.0, 0.0, 99.0, 0.0, ?, ?, ?, 'INVERSE_HEDGE')
        """,
        (
            today_str,
            mkt,
            code,
            name,
            sector,
            sig_type,
            tier,
            score,
            0.0,
            0.0,
            0.0,
            ep,
            ep,
            ep,
            round(invest_amount, 2),
            int(shares),
            round(invest_amount, 2),
        ),
    )


def run_inverse_etf_sniper_cycle() -> dict[str, Any]:
    """
    단일 사이클: 킬 스위치 → (모드 ON 시) 신규 스나이핑 1건까지.
    """
    init_forward_db()
    cfg = load_system_config()
    summary: dict[str, Any] = {"kill_closed": 0, "entered": None, "skipped": None}

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")

    summary["kill_closed"] = enforce_inverse_kill_switch(conn, cfg)
    cfg = load_system_config()

    if not _inverse_mode_active(cfg):
        summary["skipped"] = "INVERSE_MODE_ACTIVE=False (킬 스위치만 처리)"
        conn.close()
        return summary

    if _inverse_open_rows(conn):
        summary["skipped"] = "OPEN 인버스 1건 이상 존재 — 신규 진입 차단(상관 중복 방지)"
        conn.close()
        return summary

    for cand in INVERSE_CANDIDATES:
        mkt = cand["market"].upper()
        thr = float(cand["trigger_ret_5d"])
        r5 = _fetch_hedge_5d_return_pct(mkt, cand["hedge"])
        if r5 is None or r5 > thr:
            continue

        code = cand["code"]
        px = _fetch_last_close(mkt, code)
        if px is None or px <= 0:
            continue

        key = _tail_fund_key(mkt)
        tail_bal = _numeric_tail_balance(key)
        max_inv = max(0.0, tail_bal * INVERSE_HARD_CAP_PCT)
        if max_inv <= 0:
            summary["skipped"] = "테일 30% 캡: 잔액 0으로 진입 불가"
            break

        invest = round(min(max_inv, tail_bal), 2)
        shares = max(1, int(invest / px))
        invest = round(min(shares * px, max_inv), 2)
        if invest <= 0:
            continue

        if not reserve_tail_amount(mkt, invest):
            summary["skipped"] = "테일 30% 캡: Reserve OCC 실패(잔액·경합)"
            break

        try:
            _insert_inverse_forward_trade(
                conn,
                market=mkt,
                code=code,
                name=cand["name"],
                entry_price=px,
                invest_amount=invest,
                shares=shares,
            )
            conn.commit()
            summary["entered"] = {"market": mkt, "code": code, "invest": invest, "hedge_5d_ret": r5}
            print(f"🎯 [inverse_etf_sniper] 진입: {mkt} {code} invest={invest:,.0f} (테일 30% 캡, hedge5d={r5:.2f}%)")
            break
        except Exception as e:
            conn.rollback()
            release_tail_amount(mkt, invest)
            summary["skipped"] = f"INSERT 실패 후 Reserve 롤백: {e}"
            print(f"🚨 [inverse_etf_sniper] {summary['skipped']}")
            break

    conn.close()
    return summary


if __name__ == "__main__":
    # Windows spawn 호환: 프로세스 풀을 붙이지 않더라도 직접 실행 진입점은 가드 유지.
    out = run_inverse_etf_sniper_cycle()
    print(out)
