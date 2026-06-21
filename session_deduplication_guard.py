"""
Stale Session Gate — 동일 session_date 재스캔으로 forward_trades 과적합 오염 방지.

OHLCV 분석 대상 session_date 가 가상 장부 최근 entry 앵커와 같으면 신규 스캔만 중단.
오픈 포지션 트래킹(ledger track) 은 별도 경로로 유지.
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from fluid_time_anchor import FluidAnchorResult, resolve_market_with_db_fallback
from market_db_paths import MARKET_DATA_DB_PATH


def allow_session_rescan() -> bool:
    """수동 재스캔 — FACTORY_ALLOW_SESSION_RESCAN=1 또는 FORCE 와 동시 설정."""
    v = str(os.environ.get("FACTORY_ALLOW_SESSION_RESCAN", "")).strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    try:
        from market_session_gate import force_scan_outside_session

        return force_scan_outside_session()
    except Exception:
        return False


@dataclass(frozen=True)
class SessionDeduplicationDecision:
    market: str
    abort_scan: bool
    session_date: str
    last_entry_anchor: str
    reason: str
    anchor_mode: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SessionDeduplicationGuard:
    """forward_trades entry 앵커 vs fluid session_date 중복 스캔 차단."""

    def __init__(self, sys_config: Optional[Dict[str, Any]] = None) -> None:
        self.cfg = sys_config if isinstance(sys_config, dict) else None

    def resolve_session_date(self, market: str) -> FluidAnchorResult:
        try:
            if self.cfg is None:
                from config_manager import load_system_config

                self.cfg = load_system_config() or {}
        except Exception:
            self.cfg = self.cfg or {}
        return resolve_market_with_db_fallback(market, self.cfg)

    @staticmethod
    def get_last_entry_anchor(market: str, *, db_path: Optional[str] = None) -> str:
        """해당 시장 forward_trades 의 최신 entry_date (YYYY-MM-DD)."""
        mk = str(market or "KR").upper()
        path = db_path or MARKET_DATA_DB_PATH
        if not path or not os.path.isfile(path):
            return ""
        try:
            conn = sqlite3.connect(path, timeout=15)
            try:
                row = conn.execute(
                    """
                    SELECT MAX(substr(CAST(entry_date AS TEXT), 1, 10))
                    FROM forward_trades
                    WHERE UPPER(TRIM(COALESCE(market, ''))) = ?
                      AND entry_date IS NOT NULL
                      AND TRIM(CAST(entry_date AS TEXT)) != ''
                    """,
                    (mk,),
                ).fetchone()
                if row and row[0]:
                    return str(row[0])[:10]
            finally:
                conn.close()
        except sqlite3.Error:
            pass
        return ""

    def evaluate(
        self,
        market: str,
        *,
        anchor: Optional[FluidAnchorResult] = None,
        db_path: Optional[str] = None,
    ) -> SessionDeduplicationDecision:
        mk = str(market or "KR").upper()
        if allow_session_rescan():
            res = anchor or self.resolve_session_date(mk)
            return SessionDeduplicationDecision(
                market=mk,
                abort_scan=False,
                session_date=res.session_date,
                last_entry_anchor=self.get_last_entry_anchor(mk, db_path=db_path),
                reason="FORCE_RESCAN bypass",
                anchor_mode=res.mode,
            )

        res = anchor or self.resolve_session_date(mk)
        session = str(res.session_date or "")[:10]
        last_anchor = self.get_last_entry_anchor(mk, db_path=db_path)

        if not session:
            return SessionDeduplicationDecision(
                market=mk,
                abort_scan=False,
                session_date=session,
                last_entry_anchor=last_anchor,
                reason="no_session_date",
                anchor_mode=res.mode,
            )

        if not last_anchor:
            return SessionDeduplicationDecision(
                market=mk,
                abort_scan=False,
                session_date=session,
                last_entry_anchor="",
                reason="no_prior_entry_anchor",
                anchor_mode=res.mode,
            )

        if session == last_anchor:
            return SessionDeduplicationDecision(
                market=mk,
                abort_scan=True,
                session_date=session,
                last_entry_anchor=last_anchor,
                reason=(
                    f"stale_session_dedup: OHLCV session={session} "
                    f"== forward_trades last_entry={last_anchor}"
                ),
                anchor_mode=res.mode,
            )

        return SessionDeduplicationDecision(
            market=mk,
            abort_scan=False,
            session_date=session,
            last_entry_anchor=last_anchor,
            reason="session_advance_ok",
            anchor_mode=res.mode,
        )
