"""
LS-GOAL-UX-01 — LONG/SHORT progress summary (display-only, read-only).

Track B goal numbers (MDD 5% / CAGR 12~25% / B0~B3) stay shared.
Blocked-reason buckets stay in short_funnel_report_bg (no recompute here).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

import pytz

from bitget.infra.clock import utc_datetime_str
from bitget.infra.data_paths import market_data_db_path
from bitget.infra.shared_db_connector import get_connection


def ls_split_enabled() -> bool:
    import os

    env = os.environ.get("POST_DEPLOY_OBS_LS_SPLIT_ENABLED")
    if env is not None and str(env).strip():
        return str(env).strip().lower() in ("1", "true", "yes", "on")
    try:
        from bitget.infra import config_manager as cm

        raw = cm.get_config_value("POST_DEPLOY_OBS_LS_SPLIT_ENABLED", None)
        if raw is not None:
            if isinstance(raw, bool):
                return raw
            return str(raw).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        pass
    try:
        from bitget.infra.memory_policy import POST_DEPLOY_OBS_LS_SPLIT_ENABLED

        return bool(POST_DEPLOY_OBS_LS_SPLIT_ENABLED)
    except Exception:
        return True


def _today_kst() -> str:
    return datetime.now(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d")


def _empty_side() -> Dict[str, Any]:
    return {
        "open_count": 0,
        "closed_today": 0,
        "closed_cum": 0,
        "win_cum": 0,
        "loss_cum": 0,
        "pnl_cum_usdt": None,
    }


def collect_ls_split_summary(
    *,
    forward_db_path: Optional[str] = None,
    short_funnel: Optional[Dict[str, Any]] = None,
    today_kst: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Read-only hydrate: bitget_forward_trades GROUP BY position_side × status.

    SHORT.blocked_today comes from short_funnel_report_bg totals (import only).
    LONG has no blocked_today key (LONG-FUNNEL-01 later).
    """
    path = forward_db_path or market_data_db_path()
    day = str(today_kst or _today_kst())[:10]
    long_s = _empty_side()
    short_s = _empty_side()
    err: Optional[str] = None
    # side → sum(sim_kelly_invest * final_ret/100) for CLOSED — ledger close formula
    pnl_acc: Dict[str, float] = {"LONG": 0.0, "SHORT": 0.0}
    pnl_n: Dict[str, int] = {"LONG": 0, "SHORT": 0}

    try:
        conn = get_connection(path, read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT UPPER(COALESCE(position_side, 'LONG')) AS side,
                       status,
                       COUNT(*) AS n,
                       SUM(
                         CASE
                           WHEN status LIKE 'CLOSED%'
                             AND IFNULL(sim_kelly_invest, 0) != 0
                             AND final_ret IS NOT NULL
                           THEN CAST(sim_kelly_invest AS REAL)
                                * CAST(final_ret AS REAL) / 100.0
                           ELSE NULL
                         END
                       ) AS pnl_usdt
                FROM bitget_forward_trades
                GROUP BY 1, 2
                """
            ).fetchall()
            for side, status, n, pnl in rows:
                side_u = str(side or "LONG").upper()
                if side_u not in ("LONG", "SHORT"):
                    side_u = "LONG"
                st = str(status or "").upper()
                n_i = int(n or 0)
                bucket = long_s if side_u == "LONG" else short_s
                if st.startswith("OPEN"):
                    bucket["open_count"] += n_i
                elif st.startswith("CLOSED"):
                    bucket["closed_cum"] += n_i
                    if st == "CLOSED_WIN":
                        bucket["win_cum"] += n_i
                    elif st == "CLOSED_LOSS":
                        bucket["loss_cum"] += n_i
                    if pnl is not None:
                        try:
                            pnl_acc[side_u] += float(pnl)
                            pnl_n[side_u] += 1
                        except (TypeError, ValueError):
                            pass

            today_rows = conn.execute(
                """
                SELECT UPPER(COALESCE(position_side, 'LONG')) AS side,
                       COUNT(*) AS n
                FROM bitget_forward_trades
                WHERE status LIKE 'CLOSED%'
                  AND substr(COALESCE(exit_date, ''), 1, 10) = ?
                GROUP BY 1
                """,
                (day,),
            ).fetchall()
            for side, n in today_rows:
                side_u = str(side or "LONG").upper()
                if side_u == "SHORT":
                    short_s["closed_today"] = int(n or 0)
                else:
                    long_s["closed_today"] = int(n or 0)
        finally:
            conn.close()
    except Exception as exc:
        err = f"forward_trades:{exc}"[:200]

    if pnl_n["LONG"] > 0:
        long_s["pnl_cum_usdt"] = round(pnl_acc["LONG"], 4)
    if pnl_n["SHORT"] > 0:
        short_s["pnl_cum_usdt"] = round(pnl_acc["SHORT"], 4)

    # SHORT blocked_today — import only (no bucket recompute)
    sf = short_funnel
    if sf is None:
        try:
            from bitget.observability.short_funnel_report_bg import (
                collect_short_funnel_report,
            )

            sf = collect_short_funnel_report(forward_db_path=path)
        except Exception:
            sf = {}
    blocked = 0
    try:
        blocked = int((sf or {}).get("blocked_short_total") or 0)
    except (TypeError, ValueError):
        blocked = 0
    short_s["blocked_today"] = blocked

    return {
        "checked_at": utc_datetime_str(),
        "date_kst": day,
        "enabled": True,
        "LONG": long_s,
        "SHORT": short_s,
        "footnote_spot": "현물(SPOT)은 구조상 숏 불가 · 숏은 선물만 (SPOT SHORT=0이 정상)",
        "detail_hint": "차단 상세 → digest 「숏(선물) 연습」퍼널 참고",
        "last_error": err,
    }


def format_ls_split_plain_line(summary: Optional[Dict[str, Any]]) -> str:
    """One-line kid summary: 롱 OPEN n · CLOSED W/L | 숏 OPEN n · CLOSED W/L."""
    if not summary:
        return ""
    lo = summary.get("LONG") or {}
    sh = summary.get("SHORT") or {}
    blocked = sh.get("blocked_today")
    blocked_txt = f" · 차단 {int(blocked)}" if blocked is not None else ""
    return (
        f"롱 OPEN {int(lo.get('open_count') or 0)} · "
        f"CLOSED {int(lo.get('win_cum') or 0)}W/{int(lo.get('loss_cum') or 0)}L"
        f"  |  "
        f"숏 OPEN {int(sh.get('open_count') or 0)} · "
        f"CLOSED {int(sh.get('win_cum') or 0)}W/{int(sh.get('loss_cum') or 0)}L"
        f"{blocked_txt}"
    )


def format_ls_split_html_block(summary: Optional[Dict[str, Any]]) -> str:
    """Telegram HTML — L/S 2-column progress (goals stay Track B shared)."""
    import html as _html

    def _e(v: Any) -> str:
        return _html.escape(str(v if v is not None else ""), quote=False)

    if not summary:
        return ""
    lo = summary.get("LONG") or {}
    sh = summary.get("SHORT") or {}
    lines = [
        "<b>롱 / 숏 진행</b> <i>(목표 MDD·연복리는 Track B 공유)</i>",
        (
            f"🟦 <b>롱</b> OPEN {_e(lo.get('open_count'))} · "
            f"오늘청산 {_e(lo.get('closed_today'))} · "
            f"누적 {_e(lo.get('closed_cum'))} "
            f"({_e(lo.get('win_cum'))}W/{_e(lo.get('loss_cum'))}L)"
        ),
        (
            f"🟧 <b>숏</b> OPEN {_e(sh.get('open_count'))} · "
            f"오늘청산 {_e(sh.get('closed_today'))} · "
            f"누적 {_e(sh.get('closed_cum'))} "
            f"({_e(sh.get('win_cum'))}W/{_e(sh.get('loss_cum'))}L) · "
            f"차단 {_e(sh.get('blocked_today'))}"
        ),
        f"<i>{_e(summary.get('detail_hint'))}</i>",
        f"<i>{_e(summary.get('footnote_spot'))}</i>",
    ]
    return "\n".join(lines)
