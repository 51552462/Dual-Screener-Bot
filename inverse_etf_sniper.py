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


# ---------------------------------------------------------------------------
# [Sector Pinpoint Inverse] US 섹터별 전용 인버스 ETF 매핑 (붕괴 섹터 정조준)
# ---------------------------------------------------------------------------
FADE_SIG_MARKER = "[TOXIC_FADE]"  # 톡식 역배팅 시그널 마커(INVERSE 마커와 병기)

US_SECTOR_INVERSE_MAP: dict[str, dict[str, str]] = {
    "SEMI":       {"code": "SOXS", "name": "Direxion Daily Semiconductor Bear 3X", "hedge": "SOXX"},
    "TECH":       {"code": "SQQQ", "name": "ProShares UltraPro Short QQQ", "hedge": "QQQ"},
    "FINANCIAL":  {"code": "FAZ",  "name": "Direxion Daily Financial Bear 3X", "hedge": "XLF"},
    "SMALLCAP":   {"code": "SRTY", "name": "ProShares UltraPro Short Russell2000", "hedge": "IWM"},
    "ENERGY":     {"code": "ERY",  "name": "Direxion Daily Energy Bear 2X", "hedge": "XLE"},
    "BROAD":      {"code": "SPXS", "name": "Direxion Daily S&P500 Bear 3X", "hedge": "SPY"},
}
# 섹터 문자열 → 매핑 키 (KR 표준 버킷·US 영문 모두 커버). 우선순위 순서대로 첫 매칭 승.
_US_SECTOR_KEYWORDS: list[tuple[tuple[str, ...], str]] = [
    (("반도체", "semi", "soxx", "chip"), "SEMI"),
    (("금융", "은행", "증권", "지주", "financ", "bank", "broker"), "FINANCIAL"),
    (("에너지", "정유", "화학", "energy", "oil", "gas"), "ENERGY"),
    (("러셀", "스몰", "small", "russell"), "SMALLCAP"),
    (("반도체/it", "테크", "tech", "software", "소프트", "nasdaq", "qqq", "데이터", " ai", "it"), "TECH"),
]
KR_DEFAULT_INVERSE: dict[str, str] = {"code": "252670", "name": "KODEX 200선물인버스2X", "hedge": "069500"}


def _sector_inverse_key(sector: str) -> str:
    s = str(sector or "").strip().lower()
    for keys, label in _US_SECTOR_KEYWORDS:
        if any(k in s for k in keys):
            return label
    return "BROAD"


def resolve_sector_inverse(market: str, sector: str) -> dict[str, str]:
    """섹터 문자열 → 전용 인버스 ETF 후보(dict: market, code, name, hedge, sector_key)."""
    mkt = str(market or "").upper()
    if mkt == "KR":
        d = dict(KR_DEFAULT_INVERSE)
        d.update({"market": "KR", "sector_key": "KR_BROAD"})
        return d
    key = _sector_inverse_key(sector)
    base = US_SECTOR_INVERSE_MAP.get(key, US_SECTOR_INVERSE_MAP["BROAD"])
    d = dict(base)
    d.update({"market": "US", "sector_key": key})
    return d


def find_collapsing_sector(market: str, *, lookback_days: int = 20, min_trades: int = 5) -> Optional[str]:
    """
    최근 청산 원장에서 평균수익이 가장 나쁜(붕괴 중인) 섹터 반환. RO 조회·실패 시 None.
    sector_rotation_store 의 '주도(최고)' 관점과 반대로, 여기선 '최악 폼' 섹터를 정조준한다.
    """
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        try:
            conn.execute("PRAGMA query_only=ON;")
            cutoff = (datetime.now() - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            df = pd.read_sql(
                """
                SELECT sector, final_ret FROM forward_trades
                WHERE UPPER(TRIM(market))=? AND status LIKE 'CLOSED%'
                  AND final_ret IS NOT NULL
                  AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
                  AND IFNULL(sig_type,'') NOT LIKE ?
                  AND COALESCE(NULLIF(TRIM(exit_date),''), entry_date) >= ?
                """,
                conn,
                params=(str(market).upper(), f"%{INVERSE_SIG_MARKER}%", cutoff),
            )
        finally:
            conn.close()
    except Exception:
        return None
    if df is None or df.empty or "sector" not in df.columns:
        return None
    df = df.dropna(subset=["sector"])
    if df.empty:
        return None
    g = df.groupby("sector")["final_ret"].agg(["count", "mean"])
    g = g[g["count"] >= int(min_trades)]
    if g.empty:
        return None
    return str(g.sort_values("mean").index[0])


def _sector_pinpoint_candidate(market: str) -> Optional[dict[str, str]]:
    """붕괴 섹터 → 전용 인버스 ETF 후보(INVERSE_CANDIDATES 호환 dict). US 전용, 없으면 None."""
    if str(market or "").upper() != "US":
        return None
    sector = find_collapsing_sector("US")
    if not sector:
        return None
    inv = resolve_sector_inverse("US", sector)
    return {
        "market": "US",
        "code": inv["code"],
        "name": f"{inv['name']} (붕괴섹터 {sector}→{inv['sector_key']})",
        "hedge": inv["hedge"],
        "trigger_ret_5d": "-2.0",
    }


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
    status: str = "OPEN",
    sig_type: Optional[str] = None,
    sector: str = "InverseETF-TailSleeve",
    entry_regime: str = "INVERSE_HEDGE",
) -> None:
    init_forward_db()
    tz_kr = pytz.timezone("Asia/Seoul")
    tz_us = pytz.timezone("America/New_York")
    mkt = market.upper()
    today_str = datetime.now(tz_us if mkt == "US" else tz_kr).strftime("%Y-%m-%d")
    if not sig_type:
        sig_type = f"Dante_INVERSE_ETF_Sniper[V1]{INVERSE_SIG_MARKER}"
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
         entry_atr, invest_amount, shares, sim_kelly_invest, entry_regime, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?, ?, 1.0, 1.0, 0.0, 99.0, 0.0, ?, ?, ?, ?, ?)
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
            entry_regime,
            status,
        ),
    )


def _insert_inverse_shadow(
    conn: sqlite3.Connection,
    *,
    market: str,
    code: str,
    name: str,
    entry_price: float,
    reason: str = "",
    sig_type: Optional[str] = None,
) -> None:
    """
    [Shadow Inverse Ledger] 실자본 0 가상거래 기록.
    테일 잔액 부족·30% 캡 거부 시에도 인버스 로직의 승률·표본을 계속 축적한다.
    status='OPEN_SHADOW', invest_amount=0, shares=0.
    """
    if not sig_type:
        sig_type = f"Dante_INVERSE_ETF_Sniper[V1][SHADOW]{INVERSE_SIG_MARKER}"
    _insert_inverse_forward_trade(
        conn,
        market=market,
        code=code,
        name=name,
        entry_price=entry_price,
        invest_amount=0.0,
        shares=0,
        status="OPEN_SHADOW",
        sig_type=sig_type,
        entry_regime="INVERSE_SHADOW",
    )
    if reason:
        print(f"👻 [inverse_etf_sniper] 섀도우 기록: {market} {code} (invest=0) — {reason}")


def run_inverse_etf_sniper_cycle() -> dict[str, Any]:
    """
    단일 사이클: 킬 스위치 → (모드 ON 시) 신규 스나이핑 1건까지.
    """
    init_forward_db()
    cfg = load_system_config()
    summary: dict[str, Any] = {"kill_closed": 0, "entered": None, "skipped": None, "shadow": None}

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

    # [Sector Pinpoint] US 붕괴 섹터 전용 인버스를 최우선 후보로 정조준 → 실패 시 기존 시장 인버스.
    candidates: list[dict[str, str]] = list(INVERSE_CANDIDATES)
    try:
        pinpoint = _sector_pinpoint_candidate("US")
        if pinpoint and not any(c["code"] == pinpoint["code"] for c in candidates):
            candidates.insert(0, pinpoint)
    except Exception as _pp_ex:
        print(f"⚠️ [inverse_etf_sniper] 섹터 정조준 스킵: {_pp_ex}")

    for cand in candidates:
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
            # [Shadow Inverse Ledger] 잔액 0 → return False 대신 가상거래로 표본 축적
            try:
                _insert_inverse_shadow(
                    conn, market=mkt, code=code, name=cand["name"], entry_price=px,
                    reason="테일 30% 캡: 잔액 0(실진입 거부)",
                )
                conn.commit()
                summary["shadow"] = {"market": mkt, "code": code, "entry_price": px, "reason": "tail_balance_zero"}
            except Exception as _sh_ex:
                conn.rollback()
                print(f"🚨 [inverse_etf_sniper] 섀도우 INSERT 실패: {_sh_ex}")
            summary["skipped"] = "테일 30% 캡: 잔액 0으로 실진입 불가 → 섀도우 기록"
            break

        invest = round(min(max_inv, tail_bal), 2)
        shares = max(1, int(invest / px))
        invest = round(min(shares * px, max_inv), 2)
        if invest <= 0:
            continue

        if not reserve_tail_amount(mkt, invest):
            # [Shadow Inverse Ledger] Reserve 거부 → 가상거래로 표본 축적
            try:
                _insert_inverse_shadow(
                    conn, market=mkt, code=code, name=cand["name"], entry_price=px,
                    reason="테일 30% 캡: Reserve OCC 실패(실진입 거부)",
                )
                conn.commit()
                summary["shadow"] = {"market": mkt, "code": code, "entry_price": px, "reason": "reserve_failed"}
            except Exception as _sh_ex:
                conn.rollback()
                print(f"🚨 [inverse_etf_sniper] 섀도우 INSERT 실패: {_sh_ex}")
            summary["skipped"] = "테일 30% 캡: Reserve OCC 실패(잔액·경합) → 섀도우 기록"
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


# ---------------------------------------------------------------------------
# [Toxic Alpha Fading Engine] 톡식 롱 시그널 → 섹터 인버스 역매매 브릿지
# ---------------------------------------------------------------------------
def match_toxic_fade_target(sig_type: Any, sys_config: Optional[dict[str, Any]] = None) -> bool:
    """
    스캔된 시그널이 TOXIC_FADE_TARGET(승률<30%·심각한 마이너스 평균수익) 그룹에 속하는지 판정.
    config["TOXIC_FADE_TARGETS"] 키(그룹키)가 sig_type 에 포함되면 True.
    핫 패스(스캐너)용 — 무거운 import 없이 부분문자열 매칭으로 가볍게 동작.
    """
    s = str(sig_type or "")
    if not s:
        return False
    # 인버스·페이드 자기 시그널은 절대 페이드 대상으로 보지 않음(피드백 루프 차단)
    if INVERSE_SIG_MARKER in s or FADE_SIG_MARKER in s:
        return False
    cfg = sys_config if isinstance(sys_config, dict) else load_system_config()
    targets = cfg.get("TOXIC_FADE_TARGETS") if isinstance(cfg, dict) else None
    if not isinstance(targets, dict) or not targets:
        return False
    su = s.upper()
    for key in targets.keys():
        k = str(key or "").strip().upper()
        if k and k in su:
            return True
    return False


def fade_long_to_inverse(
    market: str,
    sector: str,
    src_code: str,
    src_name: str,
    src_sig: str,
    ref_price: float,
    sys_config: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    톡식 롱 시그널을 '해당 종목 섹터의 인버스 ETF 매수'로 역전.
    테일 자금 가용 시 실진입(LIVE), 부족/캡이면 OPEN_SHADOW 로 표본만 축적.
    실거래 NAV·5-Factor 앙상블과 독립 — 테일 펀드 KV 만 사용. 모든 단계 방어적.
    """
    out: dict[str, Any] = {"ok": False, "summary": "", "mode": None, "code": None}
    try:
        cand = resolve_sector_inverse(market, sector)
        inv_mkt = cand["market"]
        code = cand["code"]
        name = cand["name"]
        out["code"] = code

        px = _fetch_last_close(inv_mkt, code) or 0.0
        if px <= 0:
            px = float(ref_price or 0.0) or 1.0
            price_known = False
        else:
            price_known = True

        tail_bal = _numeric_tail_balance(_tail_fund_key(inv_mkt))
        max_inv = max(0.0, tail_bal * INVERSE_HARD_CAP_PCT)
        fade_sig = (
            f"Dante_TOXIC_FADE[{cand.get('sector_key')}]{FADE_SIG_MARKER}{INVERSE_SIG_MARKER}"
            f" ◀{str(src_code)[:8]}/{str(src_sig)[:32]}"
        )

        init_forward_db()
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        try:
            if price_known and max_inv > 0:
                invest = round(min(max_inv, tail_bal), 2)
                shares = max(1, int(invest / px))
                invest = round(min(shares * px, max_inv), 2)
                if invest > 0 and reserve_tail_amount(inv_mkt, invest):
                    try:
                        _insert_inverse_forward_trade(
                            conn, market=inv_mkt, code=code, name=name, entry_price=px,
                            invest_amount=invest, shares=shares, status="OPEN",
                            sig_type=fade_sig, entry_regime="TOXIC_FADE",
                        )
                        conn.commit()
                        out.update(
                            ok=True, mode="LIVE",
                            summary=f"{inv_mkt} {code} 실진입 {invest:,.0f} (섹터 {sector}→{code})",
                        )
                        print(f"🔻 [toxic_fade] LIVE {inv_mkt} {code} invest={invest:,.0f} ◀{src_code}")
                        return out
                    except Exception:
                        conn.rollback()
                        release_tail_amount(inv_mkt, invest)
            # 자금 부족·캡·가격미상 → 섀도우 기록
            _insert_inverse_forward_trade(
                conn, market=inv_mkt, code=code, name=name, entry_price=px,
                invest_amount=0.0, shares=0, status="OPEN_SHADOW",
                sig_type=fade_sig, entry_regime="TOXIC_FADE_SHADOW",
            )
            conn.commit()
            out.update(
                ok=True, mode="SHADOW",
                summary=f"{inv_mkt} {code} 섀도우 기록(invest=0) (섹터 {sector}→{code})",
            )
            print(f"👻 [toxic_fade] SHADOW {inv_mkt} {code} ◀{src_code}")
            return out
        finally:
            conn.close()
    except Exception as e:
        out["summary"] = f"fade_error: {e}"
        return out


if __name__ == "__main__":
    # Windows spawn 호환: 프로세스 풀을 붙이지 않더라도 직접 실행 진입점은 가드 유지.
    out = run_inverse_etf_sniper_cycle()
    print(out)
