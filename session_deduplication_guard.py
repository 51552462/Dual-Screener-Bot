"""
Stale Session Gate — 동일 session_date 재스캔으로 forward_trades 과적합 오염 방지.

OHLCV 분석 대상 session_date 에 **유효 OPEN**(명목>0) 이 있을 때만 재스캔 차단.
funnel 스냅샷만 있고 장부 등재·유효 OPEN 이 없으면 재스캔 허용 (표본 기아 복구).
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
    open_count_session: int = 0
    funnel_slots_session: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SessionDeduplicationGuard:
    """forward_trades OPEN 앵커 · funnel 스냅샷 vs fluid session_date."""

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
    def _forward_trades_open_df(
        conn: sqlite3.Connection,
        market: str,
        *,
        session: str = "",
    ) -> "pd.DataFrame":
        import pandas as pd

        mk = str(market or "KR").upper()
        info = conn.execute("PRAGMA table_info(forward_trades)").fetchall()
        if not info:
            return pd.DataFrame()
        names = {str(r[1]) for r in info}
        cols = [
            c
            for c in (
                "market",
                "entry_date",
                "status",
                "shares",
                "current_qty",
                "sim_kelly_invest",
                "invest_amount",
                "entry_price",
                "sig_type",
            )
            if c in names
        ]
        if "status" not in cols or "entry_date" not in cols:
            return pd.DataFrame()
        col_sql = ", ".join(cols)
        if session and len(str(session)[:10]) == 10:
            sess = str(session)[:10]
            q = f"""
                SELECT {col_sql} FROM forward_trades
                WHERE UPPER(TRIM(COALESCE(market, ''))) = ?
                  AND UPPER(TRIM(COALESCE(status, ''))) = 'OPEN'
                  AND substr(CAST(entry_date AS TEXT), 1, 10) = ?
            """
            return pd.read_sql(q, conn, params=(mk, sess))
        q = f"""
            SELECT {col_sql} FROM forward_trades
            WHERE UPPER(TRIM(COALESCE(market, ''))) = ?
              AND UPPER(TRIM(COALESCE(status, ''))) = 'OPEN'
        """
        return pd.read_sql(q, conn, params=(mk,))

    @staticmethod
    def _load_open_rows(
        market: str,
        *,
        session: str = "",
        db_path: Optional[str] = None,
    ) -> "pd.DataFrame":
        import pandas as pd

        path = db_path or MARKET_DATA_DB_PATH
        if not path or not os.path.isfile(path):
            return pd.DataFrame()
        try:
            conn = sqlite3.connect(path, timeout=15)
            try:
                return SessionDeduplicationGuard._forward_trades_open_df(
                    conn, market, session=session
                )
            finally:
                conn.close()
        except (sqlite3.Error, Exception):
            return pd.DataFrame()

    @staticmethod
    def count_valid_open_on_session(
        market: str,
        session: str,
        *,
        db_path: Optional[str] = None,
    ) -> int:
        """유효 명목 OPEN — ghost(status-only) 행 제외."""
        from forward.forward_book_integrity import reporter_valid_holding_mask

        df = SessionDeduplicationGuard._load_open_rows(
            market, session=session, db_path=db_path
        )
        if df is None or df.empty:
            return 0
        return int(reporter_valid_holding_mask(df).sum())

    @staticmethod
    def get_last_valid_open_entry_anchor(market: str, *, db_path: Optional[str] = None) -> str:
        """유효 OPEN 의 최신 entry_date — ghost 행 무시."""
        import pandas as pd

        from forward.forward_book_integrity import reporter_valid_holding_mask

        mk = str(market or "KR").upper()
        path = db_path or MARKET_DATA_DB_PATH
        if not path or not os.path.isfile(path):
            return ""
        try:
            conn = sqlite3.connect(path, timeout=15)
            try:
                df = SessionDeduplicationGuard._forward_trades_open_df(conn, mk)
            finally:
                conn.close()
        except sqlite3.Error:
            return ""
        if df is None or df.empty:
            return ""
        valid = df[reporter_valid_holding_mask(df)]
        if valid.empty or "entry_date" not in valid.columns:
            return ""
        dates = valid["entry_date"].astype(str).str[:10]
        dates = dates[dates.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)]
        return str(dates.max())[:10] if not dates.empty else ""

    @staticmethod
    def get_last_open_entry_anchor(market: str, *, db_path: Optional[str] = None) -> str:
        """유효 OPEN 앵커 (레거시 이름 유지)."""
        return SessionDeduplicationGuard.get_last_valid_open_entry_anchor(
            market, db_path=db_path
        )

    @staticmethod
    def count_open_on_session(
        market: str,
        session: str,
        *,
        db_path: Optional[str] = None,
    ) -> int:
        mk = str(market or "KR").upper()
        sess = str(session or "")[:10]
        if len(sess) != 10:
            return 0
        path = db_path or MARKET_DATA_DB_PATH
        if not path or not os.path.isfile(path):
            return 0
        try:
            conn = sqlite3.connect(path, timeout=15)
            try:
                row = conn.execute(
                    """
                    SELECT COUNT(*) FROM forward_trades
                    WHERE UPPER(TRIM(COALESCE(market, ''))) = ?
                      AND UPPER(TRIM(COALESCE(status, ''))) = 'OPEN'
                      AND substr(CAST(entry_date AS TEXT), 1, 10) = ?
                    """,
                    (mk, sess),
                ).fetchone()
                return int(row[0] or 0) if row else 0
            finally:
                conn.close()
        except sqlite3.Error:
            return 0

    @staticmethod
    def funnel_slots_on_session(
        market: str,
        session: str,
        *,
        db_path: Optional[str] = None,
    ) -> int:
        """scan_funnel_snapshot 적재 횟수 — 당일 스캔 실제 수행 여부."""
        mk = str(market or "KR").upper()
        sess = str(session or "")[:10]
        if len(sess) != 10:
            return 0
        path = db_path or MARKET_DATA_DB_PATH
        if not path or not os.path.isfile(path):
            return 0
        try:
            conn = sqlite3.connect(path, timeout=10)
            try:
                row = conn.execute(
                    """
                    SELECT COUNT(*) FROM scan_funnel_snapshot
                    WHERE market = ? AND substr(ts, 1, 10) = ?
                    """,
                    (mk, sess),
                ).fetchone()
                return int(row[0] or 0) if row else 0
            finally:
                conn.close()
        except sqlite3.Error:
            return 0

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
                last_entry_anchor=self.get_last_open_entry_anchor(mk, db_path=db_path),
                reason="FORCE_RESCAN bypass",
                anchor_mode=res.mode,
            )

        res = anchor or self.resolve_session_date(mk)
        session = str(res.session_date or "")[:10]
        last_anchor = self.get_last_valid_open_entry_anchor(mk, db_path=db_path)
        n_open_sess = self.count_valid_open_on_session(mk, session, db_path=db_path)
        n_funnel = self.funnel_slots_on_session(mk, session, db_path=db_path)

        if not session:
            return SessionDeduplicationDecision(
                market=mk,
                abort_scan=False,
                session_date=session,
                last_entry_anchor=last_anchor,
                reason="no_session_date",
                anchor_mode=res.mode,
                open_count_session=n_open_sess,
                funnel_slots_session=n_funnel,
            )

        if not last_anchor and n_open_sess == 0 and n_funnel == 0:
            return SessionDeduplicationDecision(
                market=mk,
                abort_scan=False,
                session_date=session,
                last_entry_anchor="",
                reason="no_prior_entry_anchor",
                anchor_mode=res.mode,
                open_count_session=n_open_sess,
                funnel_slots_session=n_funnel,
            )

        if session != last_anchor and n_open_sess == 0:
            return SessionDeduplicationDecision(
                market=mk,
                abort_scan=False,
                session_date=session,
                last_entry_anchor=last_anchor,
                reason="session_advance_ok",
                anchor_mode=res.mode,
                open_count_session=n_open_sess,
                funnel_slots_session=n_funnel,
            )

        # 유효 OPEN 장부만 재스캔 차단 — funnel 스냅샷만으로는 차단하지 않음 (drought 복구)
        if n_open_sess > 0:
            return SessionDeduplicationDecision(
                market=mk,
                abort_scan=True,
                session_date=session,
                last_entry_anchor=last_anchor or session,
                reason=(
                    f"valid_open_dedup: session={session} "
                    f"valid_open={n_open_sess} funnel_slots={n_funnel}"
                ),
                anchor_mode=res.mode,
                open_count_session=n_open_sess,
                funnel_slots_session=n_funnel,
            )

        return SessionDeduplicationDecision(
            market=mk,
            abort_scan=False,
            session_date=session,
            last_entry_anchor=last_anchor,
            reason=(
                f"rescan_allowed: session={session} "
                f"valid_open=0 funnel_slots={n_funnel} last_anchor={last_anchor or '—'}"
            ),
            anchor_mode=res.mode,
            open_count_session=n_open_sess,
            funnel_slots_session=n_funnel,
        )
