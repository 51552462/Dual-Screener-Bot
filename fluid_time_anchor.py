"""
Fluid Lookback Anchor — KR/US 휴장·지연 시 마지막 유효 거래일로 트래킹 유지.

live | carry_over | halt — bars_held 는 session_date 당 1회만 증가.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Literal, Optional

import pytz

from reports.report_timekeeper import (
    business_lag_days,
    kr_session_anchor_date,
    us_last_trading_session_date,
)

AnchorMode = Literal["live", "carry_over", "halt"]

_KR_TZ = pytz.timezone("Asia/Seoul")
_US_ET = pytz.timezone("America/New_York")


def _track_session_key(market: str) -> str:
    return f"FLUID_TRACK_SESSION_{str(market or 'KR').upper()}"


def _anchor_state_key(market: str) -> str:
    return f"FLUID_{str(market or 'KR').upper()}_ANCHOR_STATE"


@dataclass(frozen=True)
class FluidAnchorResult:
    market: str
    mode: AnchorMode
    session_date: str
    calendar_today: str
    latest_candle_date: str
    lag_business_days: int
    reason: str

    def should_increment_bars(self, sys_config: Dict[str, Any]) -> bool:
        """동일 session_date 에 대해 KST 하루 1회만 bars_held 증가."""
        key = _track_session_key(self.market)
        last = str((sys_config or {}).get(key) or "")
        return self.session_date > last

    def mark_tracked(self, sys_config: Dict[str, Any]) -> None:
        if isinstance(sys_config, dict):
            sys_config[_track_session_key(self.market)] = self.session_date


class FluidLookbackAnchor:
    """벤치마크 캔들 + 시장 달력으로 트래킹 앵커를 동적으로 결정."""

    def __init__(self, sys_config: Optional[Dict[str, Any]] = None) -> None:
        self.cfg = sys_config if isinstance(sys_config, dict) else {}

    @classmethod
    def resolve(
        cls,
        market: str,
        *,
        sys_config: Optional[Dict[str, Any]] = None,
        ref: Optional[datetime] = None,
    ) -> FluidAnchorResult:
        return cls(sys_config).resolve_market(market, ref=ref)

    def resolve_market(self, market: str, *, ref: Optional[datetime] = None) -> FluidAnchorResult:
        mk = str(market or "KR").upper()
        if mk == "KR":
            return self._resolve_kr_fluid(ref)
        return self._resolve_us_fluid(ref)

    def _lag_limits(self, market: str) -> tuple[int, int]:
        mk = str(market).upper()
        prefix = "FLUID_KR" if mk == "KR" else "FLUID_US"
        max_lag = int(self.cfg.get(f"{prefix}_MAX_CARRY_LAG_DAYS", 3) or 3)
        max_stale = int(self.cfg.get(f"{prefix}_MAX_STALE_BUSINESS_DAYS", 5) or 5)
        return max_lag, max_stale

    def _resolve_kr_fluid(self, ref: Optional[datetime]) -> FluidAnchorResult:
        tz = _KR_TZ
        now = ref.astimezone(tz) if ref and ref.tzinfo else datetime.now(tz)
        cal = now.strftime("%Y-%m-%d")
        max_lag, max_stale = self._lag_limits("KR")

        candle = cal
        try:
            from network_timeout import fdr_data_reader

            start = (now - timedelta(days=60)).strftime("%Y-%m-%d")
            idx = fdr_data_reader("069500", start)
            if idx is not None and not idx.empty:
                candle = idx.index[-1].strftime("%Y-%m-%d")
        except Exception:
            pass

        expected = kr_session_anchor_date(ref=now.date())
        expected_s = expected.strftime("%Y-%m-%d")
        lag = business_lag_days(candle, expected_s, market="KR")

        if lag > max_stale:
            return FluidAnchorResult(
                market="KR",
                mode="halt",
                session_date=candle,
                calendar_today=cal,
                latest_candle_date=candle,
                lag_business_days=lag,
                reason=f"stale_kospi>{max_stale}bd",
            )

        if candle == cal or candle == expected_s:
            return FluidAnchorResult(
                market="KR",
                mode="live",
                session_date=candle,
                calendar_today=cal,
                latest_candle_date=candle,
                lag_business_days=0,
                reason="kr_live",
            )

        cal_lag = business_lag_days(candle, cal, market="KR")
        if cal_lag <= max_lag:
            return FluidAnchorResult(
                market="KR",
                mode="carry_over",
                session_date=candle,
                calendar_today=cal,
                latest_candle_date=candle,
                lag_business_days=cal_lag,
                reason="kr_carry_over",
            )

        return FluidAnchorResult(
            market="KR",
            mode="halt",
            session_date=candle,
            calendar_today=cal,
            latest_candle_date=candle,
            lag_business_days=cal_lag,
            reason="kr_carry_lag_exceeded",
        )

    def _resolve_us_fluid(self, ref: Optional[datetime]) -> FluidAnchorResult:
        tz = _US_ET
        now = ref.astimezone(tz) if ref and ref.tzinfo else datetime.now(tz)
        cal = now.strftime("%Y-%m-%d")
        max_lag, max_stale = self._lag_limits("US")

        candle = cal
        try:
            from network_timeout import yf_download

            start = (now - timedelta(days=90)).strftime("%Y-%m-%d")
            idx = yf_download("SPY", start=start, progress=False)
            if idx is not None and not idx.empty:
                candle = idx.index[-1].strftime("%Y-%m-%d")
        except Exception:
            pass

        expected = us_last_trading_session_date(ref=now.astimezone(_KR_TZ))
        expected_s = expected.strftime("%Y-%m-%d")
        lag = business_lag_days(candle, expected_s, market="US")

        if lag > max_stale:
            return FluidAnchorResult(
                market="US",
                mode="halt",
                session_date=candle,
                calendar_today=cal,
                latest_candle_date=candle,
                lag_business_days=lag,
                reason=f"stale_spy>{max_stale}bd",
            )

        if candle == cal or candle == expected_s:
            return FluidAnchorResult(
                market="US",
                mode="live",
                session_date=candle,
                calendar_today=cal,
                latest_candle_date=candle,
                lag_business_days=0,
                reason="us_live",
            )

        cal_lag = business_lag_days(candle, cal, market="US")
        if cal_lag <= max_lag:
            return FluidAnchorResult(
                market="US",
                mode="carry_over",
                session_date=candle,
                calendar_today=cal,
                latest_candle_date=candle,
                lag_business_days=cal_lag,
                reason="us_carry_over",
            )

        return FluidAnchorResult(
            market="US",
            mode="halt",
            session_date=candle,
            calendar_today=cal,
            latest_candle_date=candle,
            lag_business_days=cal_lag,
            reason="carry_lag_exceeded",
        )


def persist_anchor_state(sys_config: Dict[str, Any], result: FluidAnchorResult) -> None:
    """FLUID_{KR|US}_ANCHOR_STATE — health·리포트용."""
    if not isinstance(sys_config, dict):
        return
    sys_config[_anchor_state_key(result.market)] = {
        "mode": result.mode,
        "session_date": result.session_date,
        "calendar_today": result.calendar_today,
        "latest_candle": result.latest_candle_date,
        "lag_bd": result.lag_business_days,
        "reason": result.reason,
        "at_kst": datetime.now(_KR_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    }


def load_spy_session_from_db() -> Optional[str]:
    """로컬 US_SPY 테이블 우선 (증분 OHLCV 직후)."""
    try:
        import sqlite3

        from market_db_paths import MARKET_DATA_DB_PATH

        if not os.path.isfile(MARKET_DATA_DB_PATH):
            return None
        conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=15)
        try:
            row = conn.execute(
                'SELECT Date FROM "US_SPY" ORDER BY Date DESC LIMIT 1'
            ).fetchone()
            if row and row[0]:
                return str(row[0])[:10]
        finally:
            conn.close()
    except Exception:
        pass
    return None


def load_kr_kospi_session_from_db() -> Optional[str]:
    """로컬 KR_KOSPI_IDX 테이블 우선."""
    try:
        import sqlite3

        from market_db_paths import MARKET_DATA_DB_PATH

        if not os.path.isfile(MARKET_DATA_DB_PATH):
            return None
        conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=15)
        try:
            row = conn.execute(
                'SELECT Date FROM "KR_KOSPI_IDX" ORDER BY Date DESC LIMIT 1'
            ).fetchone()
            if row and row[0]:
                return str(row[0])[:10]
        finally:
            conn.close()
    except Exception:
        pass
    return None


def _apply_db_fallback(
    res: FluidAnchorResult,
    db_candle: Optional[str],
    *,
    sys_config: Optional[Dict[str, Any]],
    market: str,
) -> FluidAnchorResult:
    if not db_candle:
        return res
    cfg = sys_config if isinstance(sys_config, dict) else {}
    max_lag, _ = FluidLookbackAnchor(cfg)._lag_limits(market)
    cal = res.calendar_today
    cal_lag = business_lag_days(db_candle, cal, market=market)
    if res.mode == "halt" and db_candle >= res.latest_candle_date and cal_lag <= max_lag:
        return FluidAnchorResult(
            market=market,
            mode="carry_over" if db_candle != cal else "live",
            session_date=db_candle,
            calendar_today=cal,
            latest_candle_date=db_candle,
            lag_business_days=cal_lag,
            reason=f"{market.lower()}_db_benchmark_fallback",
        )
    if res.mode in ("carry_over", "live") and db_candle > res.latest_candle_date:
        return FluidAnchorResult(
            market=market,
            mode="carry_over" if db_candle != cal else "live",
            session_date=db_candle,
            calendar_today=cal,
            latest_candle_date=db_candle,
            lag_business_days=cal_lag,
            reason=f"{market.lower()}_db_benchmark_upgrade",
        )
    return res


def resolve_us_with_db_fallback(sys_config: Optional[Dict[str, Any]] = None) -> FluidAnchorResult:
    """daily-us: DB SPY 날짜가 있으면 yfinance 대신 사용."""
    res = FluidLookbackAnchor.resolve("US", sys_config=sys_config)
    return _apply_db_fallback(res, load_spy_session_from_db(), sys_config=sys_config, market="US")


def resolve_kr_with_db_fallback(sys_config: Optional[Dict[str, Any]] = None) -> FluidAnchorResult:
    """daily-kr: DB KR_KOSPI_IDX 날짜 우선."""
    res = FluidLookbackAnchor.resolve("KR", sys_config=sys_config)
    return _apply_db_fallback(res, load_kr_kospi_session_from_db(), sys_config=sys_config, market="KR")


def resolve_market_with_db_fallback(
    market: str,
    sys_config: Optional[Dict[str, Any]] = None,
) -> FluidAnchorResult:
    mk = str(market or "KR").upper()
    if mk == "US":
        return resolve_us_with_db_fallback(sys_config)
    return resolve_kr_with_db_fallback(sys_config)


def finalize_fluid_track_session(sys_config: Dict[str, Any], anchor: Optional[FluidAnchorResult]) -> None:
    """track_daily_positions 종료 시 session watermark."""
    if anchor is None or not hasattr(anchor, "mark_tracked"):
        return
    anchor.mark_tracked(sys_config)
    try:
        from config_manager import save_system_config

        save_system_config(sys_config)
    except Exception:
        pass
