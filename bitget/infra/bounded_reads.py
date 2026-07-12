"""
Bitget Tier-2 bounded SQL reads SSOT — 4GB RAM peak DataFrame 방지.

`memory_bounds.forward_trades_bounded_sql` (OPEN 보존) 과 complement:
  - append-only `bitget_real_execution` leaderboard
  - ai_overseer 일일 감사 (column projection + LIMIT)
  - practitioner per-key samples

모든 LIMIT·컬럼 목록은 `memory_policy` 상수만 참조한다.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from bitget.infra.memory_policy import (
    FORWARD_BRAIN_TUNE_CLOSED_LIMIT,
    FORWARD_CLOSED_TRADES_LIMIT,
    FORWARD_DASHBOARD_CLOSED_LIMIT,
    FORWARD_IDENTITY_CLOSED_LIMIT,
    FORWARD_HEATMAP_OPEN_LIMIT,
    FORWARD_OPEN_MAX_SAFETY,
    OPEN_SAFETY_ALERT_MIN_INTERVAL_SEC,
    FORWARD_PIL_CLOSED_LIMIT,
    FORWARD_PRI_WEEK_CLOSED_LIMIT,
    PRI_FUNNEL_WEEK_LIMIT,
    PRI_FRICTION_WEEK_LIMIT,
    GENESIS_CLOSED_TRADES_LIMIT,
    GENESIS_ARM_SNAPSHOT_LIMIT,
    GENESIS_PENDING_BACKFILL_LIMIT,
    GENESIS_UNRESOLVED_PREDICTION_LIMIT,
    ELASTIC_VOL_OPEN_LIMIT,
    ELASTIC_VOL_CLOSED_LIMIT,
    EXIT_RATCHET_RUNNER_LIMIT,
    EXPLORATION_BUDGET_ROLLING_CLOSED_LIMIT,
    UNDERDOG_MINER_CLOSED_LIMIT,
    BLACKHOLE_HUNTER_CLOSED_LIMIT,
    TOXIC_GRAVEYARD_CLOSED_LIMIT,
    FORWARD_WEEKLY_TF_ROTATION_LIMIT,
    GRAND_REPORT_CLOSED_LIMIT,
    GRAND_REPORT_GENESIS_LIMIT,
    GRAND_REPORT_DEATHMATCH_CHAMPION_LIMIT,
    GRAND_REPORT_ELIMINATION_EVENT_LIMIT,
    GRAND_REPORT_STRATEGY_REGISTRY_LIMIT,
    GRAND_REPORT_REGISTRY_WINDOW_DETAIL_LIMIT,
    OVERSEER_DAILY_CLOSED_LIMIT,
    MACRO_DAILY_LOOKBACK_MAX_ROWS,
    REAL_EXECUTION_PRACTITIONER_SAMPLE_LIMIT,
    REAL_EXECUTION_READ_LIMIT,
    SHADOW_BLOCKED_READ_LIMIT,
    DATA_MINER_MFE_WINNERS_LIMIT,
    WS_OPEN_WATCH_SYMBOL_LIMIT,
    DATA_MINER_MFE_TRAINING_LIMIT,
    SUPERNOVA_CLUSTER_FORWARD_SYMBOL_LIMIT,
    SUPERNOVA_CLUSTER_MAX_TABLES,
    FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT,
    FORWARD_IDENTITY_BLANK_REPAIR_BATCH_LIMIT,
    TIME_MACHINE_MAX_TABLES,
)

REAL_EXECUTION_LEADERBOARD_COLUMNS: str = (
    "market_type, practitioner_key, exec_ok, is_dry_run, notional_usdt, "
    "realized_ret_pct, virtual_final_ret"
)

OVERSEER_CLOSED_COLUMNS: str = (
    "id, entry_date, exit_date, final_ret, position_side, sig_type, status"
)

# track_daily_positions — SELECT * 대신 명시 projection (스키마 drift 방지)
FORWARD_OPEN_TRACK_COLUMNS: str = (
    "id, entry_date, market_type, symbol, timeframe, sig_type, tier, total_score, "
    "dyn_rs, dyn_cpv, dyn_tb, v_cpv, v_yang, v_energy, v_rs, entry_price, position_side, "
    "entry_atr, entry_high, atr_sl_mult, leverage, sim_kelly_risk_pct, margin_used, "
    "sim_kelly_invest, quantity, max_high, min_low, bars_held, up_vol_sum, down_vol_sum, "
    "status, entry_breadth, funding_rate_last, funding_next_settle_ts, funding_accum_usdt_est, "
    "pyramid_adds, parent_trade_id, scaled_out_frac, realized_partial_ret, free_runner, flow_tags"
)

FORWARD_OPEN_FLOAT_PNL_COLUMNS: str = (
    "id, market_type, symbol, timeframe, entry_price, position_side, "
    "sim_kelly_invest, margin_used, leverage"
)

FORWARD_OPEN_RECON_COLUMNS: str = (
    "id, market_type, symbol, position_side, entry_price, max_high, min_low, "
    "bars_held, up_vol_sum, down_vol_sum, entry_breadth"
)

FORWARD_OPEN_PNL_PARITY_COLUMNS: str = (
    "id, symbol, timeframe, market_type, position_side, sig_type, "
    "entry_price, margin_used, sim_kelly_invest, status"
)

FORWARD_HEATMAP_OPEN_COLUMNS: str = (
    "symbol, market_type, position_side, timeframe, total_score, "
    "margin_used, sim_kelly_invest, leverage, entry_price"
)

FORWARD_DASHBOARD_CLOSED_COLUMNS: str = (
    "id, entry_date, exit_date, market_type, symbol, timeframe, sig_type, status, "
    "final_ret, sim_kelly_invest, margin_used, leverage, position_side, "
    "dyn_cpv, dyn_tb, v_energy, dyn_rs"
)

FORWARD_BRAIN_TUNE_COLUMNS: str = "final_ret, mfe, dyn_cpv, dyn_tb, v_energy"

FORWARD_PRI_OPEN_COLUMNS: str = (
    "entry_price, max_high, min_low, position_side"
)

FORWARD_PRI_CLOSED_COLUMNS: str = "final_ret, exit_date"

FORWARD_AUTOPILOT_CLOSED_COLUMNS: str = (
    "entry_date, exit_date, position_side, final_ret, sim_kelly_invest, timeframe, "
    "live_a_ret, cand_b_ret, champ_c_ret, entry_price, min_low, max_high"
)

FORWARD_GRAND_REPORT_CLOSED_COLUMNS: str = (
    "id, entry_date, exit_date, market_type, symbol, sig_type, status, "
    "final_ret, sim_kelly_invest, position_side, timeframe"
)

FORWARD_INCUBATOR_JUDGE_COLUMNS: str = "sig_type, final_ret"

FORWARD_WEEKLY_TF_COLUMNS: str = "entry_date, timeframe"

FORWARD_WEEKLY_FLOW_CLOSED_COLUMNS: str = (
    "exit_date, sig_type, final_ret, sim_kelly_invest"
)
FORWARD_WEEKLY_FLOW_TAG_COLUMNS: str = "flow_tags, final_ret"

FORWARD_IDENTITY_COLUMNS: str = (
    "id, market_type, symbol, status, entry_date, exit_date, "
    "final_ret, flow_tags, sig_type, timeframe"
)


def _identity_market_sql(market_type: str) -> tuple[str, tuple]:
    mk = str(market_type or "all").strip().lower()
    if mk == "futures":
        return "LOWER(IFNULL(market_type,'')) = 'futures'", ()
    if mk == "spot":
        return "LOWER(IFNULL(market_type,'')) = 'spot'", ()
    return "1=1", ()


def _clamp_limit(limit: int | None, *, default: int, floor: int, ceiling: int) -> int:
    n = int(limit if limit is not None else default)
    return max(floor, min(n, ceiling))


def real_execution_leaderboard_sql(
    *,
    market_type: str = "all",
    limit: int | None = None,
) -> tuple[str, list]:
    """Practitioner reality leaderboard — recent N executions only."""
    lim = _clamp_limit(limit, default=REAL_EXECUTION_READ_LIMIT, floor=100, ceiling=20_000)
    mkt = str(market_type).lower()
    if mkt in ("spot", "futures"):
        sql = f"""
            SELECT {REAL_EXECUTION_LEADERBOARD_COLUMNS}
            FROM bitget_real_execution
            WHERE market_type=?
            ORDER BY id DESC
            LIMIT ?
        """
        return sql, [mkt, lim]
    sql = f"""
        SELECT {REAL_EXECUTION_LEADERBOARD_COLUMNS}
        FROM bitget_real_execution
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, [lim]


def real_execution_practitioner_sample_sql(
    *,
    limit: int | None = None,
) -> tuple[str, int]:
    lim = _clamp_limit(
        limit,
        default=REAL_EXECUTION_PRACTITIONER_SAMPLE_LIMIT,
        floor=50,
        ceiling=2_000,
    )
    sql = """
        SELECT realized_ret_pct, notional_usdt
        FROM bitget_real_execution
        WHERE market_type=? AND practitioner_key=?
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, lim


def _kst_exit_date_union(kst_day: str) -> tuple[str, str]:
    """UTC date-only exit_date keys overlapping a KST calendar day (conservative union)."""
    cur = str(kst_day)[:10]
    prev = (datetime.strptime(cur, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    return prev, cur


def overseer_daily_closed_sql(
    *,
    today: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """KST audit day — exit_date UTC date-only storage → prev+cur union filter."""
    lim = _clamp_limit(limit, default=OVERSEER_DAILY_CLOSED_LIMIT, floor=50, ceiling=5_000)
    prev, cur = _kst_exit_date_union(today)
    sql = f"""
        SELECT {OVERSEER_CLOSED_COLUMNS}
        FROM bitget_forward_trades
        WHERE entry_date <= ?
          AND substr(COALESCE(exit_date, ''), 1, 10) IN (?, ?)
          AND status LIKE 'CLOSED%'
          AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (cur, prev, cur, lim)


def overseer_rnd_day_count_sql(*, today: str) -> tuple[str, tuple]:
    """R&D row count — single scalar COUNT (no LIMIT row fetch)."""
    day = str(today)[:10]
    sql = """
        SELECT COUNT(*) AS cnt FROM bitget_forward_trades
        WHERE entry_date=? AND sig_type LIKE '%[R&D_%'
    """
    return sql, (day,)


FORWARD_OPEN_INTEGRITY_COLUMNS: str = (
    "id, market_type, symbol, status, entry_date, quantity, "
    "sim_kelly_invest, margin_used, entry_price, sig_type"
)


def forward_open_integrity_open_sql(
    *,
    market_type: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """forward_book_integrity — non-CLOSED rows, projection + OPEN safety cap."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_OPEN_MAX_SAFETY,
        floor=1,
        ceiling=FORWARD_OPEN_MAX_SAFETY,
    )
    mkt = str(market_type).strip().lower()
    sql = f"""
        SELECT {FORWARD_OPEN_INTEGRITY_COLUMNS}
        FROM bitget_forward_trades
        WHERE LOWER(IFNULL(market_type,'')) = ?
          AND IFNULL(status,'') NOT LIKE 'CLOSED%'
        ORDER BY id ASC
        LIMIT ?
    """
    return sql, (mkt, lim)


def forward_integrity_closed_window_count_sql(
    *,
    market_type: str,
    since_date: str,
) -> tuple[str, tuple]:
    """Rolling CLOSED count for integrity stats — scalar COUNT, no row fetch."""
    mkt = str(market_type).strip().lower()
    since = str(since_date)[:10]
    sql = """
        SELECT COUNT(*) FROM bitget_forward_trades
        WHERE LOWER(IFNULL(market_type,'')) = ?
          AND status LIKE 'CLOSED%'
          AND IFNULL(exit_date, '') >= ?
    """
    return sql, (mkt, since)


def forward_open_track_sql(
    *,
    market_type: str | None = None,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """OPEN 포지션 track — column projection + safety cap."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_OPEN_MAX_SAFETY,
        floor=1,
        ceiling=FORWARD_OPEN_MAX_SAFETY,
    )
    if market_type is not None:
        sql = f"""
            SELECT {FORWARD_OPEN_TRACK_COLUMNS}
            FROM bitget_forward_trades
            WHERE market_type=? AND status='OPEN'
            ORDER BY id DESC
            LIMIT ?
        """
        return sql, (str(market_type).lower(), lim)
    sql = f"""
        SELECT {FORWARD_OPEN_TRACK_COLUMNS}
        FROM bitget_forward_trades
        WHERE status='OPEN'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)


def forward_open_float_pnl_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """글로벌 서킷 브레이커용 OPEN 손실 합산 — 최소 컬럼 + safety cap."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_OPEN_MAX_SAFETY,
        floor=1,
        ceiling=FORWARD_OPEN_MAX_SAFETY,
    )
    sql = f"""
        SELECT {FORWARD_OPEN_FLOAT_PNL_COLUMNS}
        FROM bitget_forward_trades
        WHERE status='OPEN'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)


def forward_open_recon_futures_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """OMS phantom OPEN reconcile — futures only, projection + safety cap."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_OPEN_MAX_SAFETY,
        floor=1,
        ceiling=FORWARD_OPEN_MAX_SAFETY,
    )
    sql = f"""
        SELECT {FORWARD_OPEN_RECON_COLUMNS}
        FROM bitget_forward_trades
        WHERE lower(market_type)='futures' AND status='OPEN'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)


def forward_open_count_sql(*, market_type: str | None = None) -> tuple[str, tuple]:
    if market_type is not None:
        return (
            "SELECT COUNT(*) FROM bitget_forward_trades WHERE market_type=? AND status='OPEN'",
            (str(market_type).lower(),),
        )
    return ("SELECT COUNT(*) FROM bitget_forward_trades WHERE status='OPEN'", ())


def forward_open_watch_symbols_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """
    Public WS universe — DISTINCT OPEN (symbol, market_type) for spread gates.
    Projection-only; ORDER BY id DESC so newest books win the channel budget.
    """
    lim = _clamp_limit(
        limit,
        default=WS_OPEN_WATCH_SYMBOL_LIMIT,
        floor=1,
        ceiling=WS_OPEN_WATCH_SYMBOL_LIMIT,
    )
    sql = """
        SELECT symbol, market_type
        FROM bitget_forward_trades
        WHERE status='OPEN'
        GROUP BY symbol, market_type
        ORDER BY MAX(id) DESC
        LIMIT ?
    """
    return sql, (lim,)


def forward_open_dup_id_sql() -> str:
    """Entry gate — same symbol/tf/market/side already OPEN (LIMIT 1)."""
    return (
        "SELECT id FROM bitget_forward_trades "
        "WHERE symbol=? AND timeframe=? AND market_type=? AND position_side=? AND status='OPEN' "
        "LIMIT 1"
    )


def forward_group_closed_pnl_sum_sql() -> str:
    """Entry sizing — group realized PnL scalar (sig_type LIKE core_group)."""
    return (
        "SELECT SUM((sim_kelly_invest * final_ret) / 100.0) "
        "FROM bitget_forward_trades WHERE status LIKE 'CLOSED%' AND sig_type LIKE ?"
    )


def forward_group_open_margin_sum_sql() -> str:
    """Entry sizing — group OPEN margin scalar (sig_type LIKE core_group)."""
    return (
        "SELECT SUM(margin_used) FROM bitget_forward_trades "
        "WHERE status='OPEN' AND sig_type LIKE ?"
    )


def forward_open_exposure_sum_sql(*, market_type: str) -> tuple[str, tuple]:
    """Pyramid / NAV — market OPEN notional exposure scalar."""
    return (
        "SELECT COALESCE(SUM(sim_kelly_invest),0) FROM bitget_forward_trades "
        "WHERE market_type=? AND status='OPEN'",
        (str(market_type).lower(),),
    )


def forward_open_gross_notional_sum_sql() -> tuple[str, tuple]:
    """Portfolio gross — all-market OPEN notional (spot+futures) scalar."""
    return (
        "SELECT COALESCE(SUM(sim_kelly_invest),0) FROM bitget_forward_trades "
        "WHERE status='OPEN'",
        (),
    )


def forward_open_concentration_book_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """OPEN book rows for BTC-proxy concentration gate (bounded)."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_OPEN_MAX_SAFETY,
        floor=1,
        ceiling=FORWARD_OPEN_MAX_SAFETY,
    )
    return (
        """
        SELECT symbol, market_type, position_side, sim_kelly_invest
        FROM bitget_forward_trades
        WHERE status='OPEN'
        ORDER BY id DESC
        LIMIT ?
        """,
        (lim,),
    )


def forward_weekly_market_pnl_sum_sql(*, market_type: str, since_date: str) -> tuple[str, tuple]:
    """Weekly report — market CLOSED PnL since date (scalar SUM)."""
    return (
        """
        SELECT SUM((sim_kelly_invest * final_ret) / 100.0)
        FROM bitget_forward_trades
        WHERE market_type=? AND exit_date >= ? AND status LIKE 'CLOSED%'
          AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
        """,
        (str(market_type).lower(), str(since_date)[:10]),
    )


def forward_zombie_zero_invest_ids_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """reporter_cleanup_zombie — zero qty/invest OPEN batch ids."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT,
        floor=10,
        ceiling=FORWARD_OPEN_MAX_SAFETY,
    )
    sql = """
        SELECT id FROM bitget_forward_trades
        WHERE (status = 'OPEN' OR UPPER(TRIM(IFNULL(status,''))) = 'ACTIVE')
          AND COALESCE(quantity,0) <= 0
          AND COALESCE(sim_kelly_invest,0) <= 0
          AND COALESCE(margin_used,0) <= 0
          AND IFNULL(sig_type,'') NOT LIKE '%OBSERVE_ONLY%'
        ORDER BY id ASC
        LIMIT ?
    """
    return sql, (lim,)


def forward_zombie_fact_close_ids_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """reporter_cleanup_zombie — exit fact present but status still OPEN batch ids."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT,
        floor=10,
        ceiling=FORWARD_OPEN_MAX_SAFETY,
    )
    sql = """
        SELECT id FROM bitget_forward_trades
        WHERE (status = 'OPEN' OR UPPER(TRIM(IFNULL(status,''))) = 'ACTIVE')
          AND COALESCE(quantity,0) <= 0
          AND (
            (exit_date IS NOT NULL AND TRIM(CAST(exit_date AS TEXT)) != '')
            OR final_ret IS NOT NULL
          )
        ORDER BY id ASC
        LIMIT ?
    """
    return sql, (lim,)


_OPEN_SAFETY_ALERT_MONO: float = 0.0


def warn_if_open_exceeds_safety(
    conn,
    *,
    market_type: str | None = None,
    max_open: int | None = None,
    alert: bool = True,
) -> int:
    """OPEN 건수가 safety cap 초과 시 ops gauge + logger + throttled CRITICAL.

    반환: OPEN count. 텔레그램은 ``OPEN_SAFETY_ALERT_MIN_INTERVAL_SEC`` 스로틀.
    """
    global _OPEN_SAFETY_ALERT_MONO
    cap = int(max_open if max_open is not None else FORWARD_OPEN_MAX_SAFETY)
    q, params = forward_open_count_sql(market_type=market_type)
    row = conn.execute(q, params).fetchone()
    n = int(row[0] or 0) if row else 0
    if n <= cap:
        return n

    mkt = str(market_type or "all")
    try:
        from bitget.infra.logging_setup import get_logger

        get_logger("bitget.forward.open_safety").warning(
            "OPEN book exceeds safety cap: count=%s max=%s market=%s",
            n,
            cap,
            mkt,
        )
    except Exception:
        pass

    try:
        from bitget.infra import ops_logger

        ops_logger.record_gauge_snapshot(
            "bitget.forward.open_safety",
            {
                "open_count": n,
                "max_safety": cap,
                "market_type": mkt,
                "warn": True,
            },
        )
    except Exception:
        pass

    if alert:
        import time as _time

        now = _time.monotonic()
        min_gap = float(OPEN_SAFETY_ALERT_MIN_INTERVAL_SEC)
        if now - _OPEN_SAFETY_ALERT_MONO >= min_gap:
            _OPEN_SAFETY_ALERT_MONO = now
            try:
                from bitget.governance.meta_alerts import send_meta_critical_alert

                send_meta_critical_alert(
                    "OPEN book safety cap",
                    f"open_count={n} max_safety={cap} market={mkt} — OOM precursor / book blow-up",
                    prefix="OPEN_SAFETY",
                )
            except Exception:
                pass
    return n


def forward_open_pnl_parity_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """PnL fingerprint — OPEN projection + safety cap (stable ORDER BY id ASC)."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_OPEN_MAX_SAFETY,
        floor=1,
        ceiling=FORWARD_OPEN_MAX_SAFETY,
    )
    sql = f"""
        SELECT {FORWARD_OPEN_PNL_PARITY_COLUMNS}
        FROM bitget_forward_trades
        WHERE status='OPEN'
        ORDER BY id ASC
        LIMIT ?
    """
    return sql, (lim,)


def forward_shadow_defended_match_sql() -> str:
    """shadow_performance_tracker — blocked→future CLOSED match (LIMIT 1)."""
    return """
        SELECT final_ret, leverage, position_side
        FROM bitget_forward_trades
        WHERE market_type=? AND symbol=? AND entry_date>=? AND status LIKE 'CLOSED%'
        ORDER BY id ASC
        LIMIT 1
    """


def forward_identity_blank_symbol_ids_sql(
    *,
    market_where: str = "1=1",
    limit: int | None = None,
) -> tuple[str, tuple]:
    """Identity repair — blank symbol rows batch (id, market_type, timeframe)."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_IDENTITY_BLANK_REPAIR_BATCH_LIMIT,
        floor=10,
        ceiling=FORWARD_OPEN_MAX_SAFETY,
    )
    sql = f"""
        SELECT id, market_type, timeframe FROM bitget_forward_trades
        WHERE ({market_where}) AND (symbol IS NULL OR TRIM(symbol) = '')
        ORDER BY id ASC
        LIMIT ?
    """
    return sql, (lim,)


def forward_heatmap_open_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    lim = _clamp_limit(limit, default=FORWARD_HEATMAP_OPEN_LIMIT, floor=50, ceiling=2_000)
    sql = f"""
        SELECT {FORWARD_HEATMAP_OPEN_COLUMNS}
        FROM bitget_forward_trades
        WHERE status='OPEN'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)


def forward_dashboard_closed_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    lim = _clamp_limit(
        limit,
        default=FORWARD_DASHBOARD_CLOSED_LIMIT,
        floor=200,
        ceiling=GRAND_REPORT_CLOSED_LIMIT,
    )
    sql = f"""
        SELECT {FORWARD_DASHBOARD_CLOSED_COLUMNS}
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)


def forward_pil_trades_sql(*, market_type: str) -> tuple[str, tuple]:
    """PIL — memory_bounds OPEN 전건 + CLOSED recent N (주식 forward 패턴)."""
    import memory_bounds

    return memory_bounds.forward_trades_bounded_sql(
        table="bitget_forward_trades",
        market_type=str(market_type).lower(),
        closed_limit=FORWARD_PIL_CLOSED_LIMIT,
    )


def forward_brain_tune_closed_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """init_forward_db 실시간 뇌수술 — 최근 CLOSED N건, 튜닝 컬럼만."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_BRAIN_TUNE_CLOSED_LIMIT,
        floor=30,
        ceiling=500,
    )
    sql = f"""
        SELECT {FORWARD_BRAIN_TUNE_COLUMNS}
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)


def forward_pri_open_metrics_sql(
    *,
    market_type: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """Weekly Shadow PRI / elastic vol — OPEN MFE/MAE live proxy, bounded."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_OPEN_MAX_SAFETY,
        floor=1,
        ceiling=FORWARD_OPEN_MAX_SAFETY,
    )
    sql = f"""
        SELECT {FORWARD_PRI_OPEN_COLUMNS}
        FROM bitget_forward_trades
        WHERE market_type=? AND status='OPEN'
          AND entry_price IS NOT NULL AND entry_price > 0
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (str(market_type).lower(), lim)


def elastic_vol_closed_rets_sql(
    *,
    market_type: str,
    since_date: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """elastic_threshold vol proxy — rolling CLOSED final_ret sample."""
    lim = _clamp_limit(
        limit,
        default=ELASTIC_VOL_CLOSED_LIMIT,
        floor=50,
        ceiling=10_000,
    )
    since = str(since_date)[:10]
    sql = """
        SELECT final_ret
        FROM bitget_forward_trades
        WHERE market_type=? AND status LIKE 'CLOSED%'
          AND substr(IFNULL(NULLIF(TRIM(exit_date),''), entry_date),1,10) >= ?
          AND final_ret IS NOT NULL
          AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (str(market_type).lower(), since, lim)


EXIT_RATCHET_RUNNER_COLUMNS: str = "mfe, final_ret, exit_type, bars_held"


def exit_ratchet_runner_trades_sql(
    *,
    cutoff: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """exit_ratchet_rl_bg — free_runner / scaled-out CLOSED sample (recent first)."""
    lim = _clamp_limit(
        limit,
        default=EXIT_RATCHET_RUNNER_LIMIT,
        floor=10,
        ceiling=5_000,
    )
    cut = str(cutoff)[:10]
    sql = f"""
        SELECT {EXIT_RATCHET_RUNNER_COLUMNS}
        FROM bitget_forward_trades
        WHERE (free_runner=1 OR scaled_out_frac > 0)
          AND status LIKE 'CLOSED%'
          AND final_ret IS NOT NULL AND mfe IS NOT NULL
          AND substr(IFNULL(exit_date, entry_date),1,10) >= ?
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (cut, lim)


def forward_pri_closed_week_sql(
    *,
    market_type: str,
    week_start: str,
    week_end: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """Weekly Shadow PRI — date-window CLOSED, bounded."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_PRI_WEEK_CLOSED_LIMIT,
        floor=100,
        ceiling=10_000,
    )
    ws, we = str(week_start)[:10], str(week_end)[:10]
    sql = f"""
        SELECT {FORWARD_PRI_CLOSED_COLUMNS}
        FROM bitget_forward_trades
        WHERE market_type=? AND status LIKE 'CLOSED%'
          AND substr(COALESCE(NULLIF(TRIM(exit_date), ''), ''), 1, 10) >= ?
          AND substr(COALESCE(NULLIF(TRIM(exit_date), ''), ''), 1, 10) <= ?
          AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (str(market_type).lower(), ws, we, lim)


PRI_FUNNEL_WEEK_COLUMNS: str = "ts, market, universe_size, survivors, pass_rate_pct"
PRI_FRICTION_WEEK_COLUMNS: str = "date, market, event_type"

GENESIS_LABEL_TRADE_COLUMNS: str = (
    "entry_date, exit_date, symbol, sig_type, status, final_ret, "
    "dyn_cpv, dyn_tb, v_energy, entry_breadth, flow_tags"
)
GENESIS_MARKET_ENERGY_COLUMNS: str = "entry_date, symbol, v_energy"
GENESIS_ARM_SNAPSHOT_COLUMNS: str = "trade_date, composite_score, mean_ret"


def pri_funnel_week_sql(
    *,
    market: str,
    week_start: str,
    week_end: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """Weekly Shadow PRI — scan_funnel_snapshot date window, bounded."""
    lim = _clamp_limit(
        limit,
        default=PRI_FUNNEL_WEEK_LIMIT,
        floor=1,
        ceiling=5_000,
    )
    ws, we = str(week_start)[:10], str(week_end)[:10]
    sql = f"""
        SELECT {PRI_FUNNEL_WEEK_COLUMNS}
        FROM scan_funnel_snapshot
        WHERE market=? AND substr(ts,1,10) >= ? AND substr(ts,1,10) <= ?
        ORDER BY ts ASC
        LIMIT ?
    """
    return sql, (str(market), ws, we, lim)


def pri_friction_week_sql(
    *,
    market: str,
    week_start: str,
    week_end: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """Weekly Shadow PRI — regime_friction_event date window, bounded."""
    lim = _clamp_limit(
        limit,
        default=PRI_FRICTION_WEEK_LIMIT,
        floor=1,
        ceiling=2_000,
    )
    ws, we = str(week_start)[:10], str(week_end)[:10]
    sql = f"""
        SELECT {PRI_FRICTION_WEEK_COLUMNS}
        FROM regime_friction_event
        WHERE market=? AND date >= ? AND date <= ?
        ORDER BY date ASC
        LIMIT ?
    """
    return sql, (str(market), ws, we, lim)


def genesis_closed_trades_sql(
    *,
    market_type: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """champion_genesis — recent CLOSED trades (DESC); caller reverses for chronology."""
    lim = _clamp_limit(
        limit,
        default=GENESIS_CLOSED_TRADES_LIMIT,
        floor=50,
        ceiling=20_000,
    )
    sql = f"""
        SELECT {GENESIS_LABEL_TRADE_COLUMNS}
        FROM bitget_forward_trades
        WHERE market_type = ? AND status LIKE 'CLOSED%'
        ORDER BY entry_date DESC
        LIMIT ?
    """
    return sql, (str(market_type).lower(), lim)


def genesis_market_energy_closed_sql(
    *,
    market_type: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """champion_genesis v_energy Z-score — lightweight recent CLOSED subset."""
    lim = _clamp_limit(
        limit,
        default=GENESIS_CLOSED_TRADES_LIMIT,
        floor=50,
        ceiling=20_000,
    )
    sql = f"""
        SELECT {GENESIS_MARKET_ENERGY_COLUMNS}
        FROM bitget_forward_trades
        WHERE market_type = ? AND status LIKE 'CLOSED%'
          AND entry_date IS NOT NULL
        ORDER BY entry_date DESC
        LIMIT ?
    """
    return sql, (str(market_type).lower(), lim)


def genesis_arm_snapshot_sql(
    *,
    market: str,
    label: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """champion_genesis — deathmatch_arm_snapshot composite_score tail (DESC)."""
    lim = _clamp_limit(
        limit,
        default=GENESIS_ARM_SNAPSHOT_LIMIT,
        floor=10,
        ceiling=5_000,
    )
    dm_mk = "FUT" if str(market).lower() in ("futures", "fut", "future") else "SPOT"
    lbl = str(label)
    sql = f"""
        SELECT {GENESIS_ARM_SNAPSHOT_COLUMNS}
        FROM deathmatch_arm_snapshot
        WHERE market = ? AND (label = ? OR arm_id = ?)
        ORDER BY trade_date DESC
        LIMIT ?
    """
    return sql, (dm_mk, lbl, lbl, lim)


def genesis_pending_champions_sql(
    *,
    market: str,
    cutoff: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """champion_genesis backfill — pending champion causal resolve batch."""
    lim = _clamp_limit(
        limit,
        default=GENESIS_PENDING_BACKFILL_LIMIT,
        floor=10,
        ceiling=1_000,
    )
    sql = """
        SELECT id, champion_label, crowned_date, confidence, decay_count
        FROM champion_precursor_genesis
        WHERE market = ? AND kind='champion' AND status='pending'
          AND crowned_date IS NOT NULL AND crowned_date <= ?
        ORDER BY crowned_date ASC
        LIMIT ?
    """
    return sql, (str(market).lower(), str(cutoff)[:10], lim)


def genesis_unresolved_predictions_sql(
    *,
    market: str,
    cutoff: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """champion_genesis backfill — unresolved prediction hit verification batch."""
    lim = _clamp_limit(
        limit,
        default=GENESIS_UNRESOLVED_PREDICTION_LIMIT,
        floor=10,
        ceiling=1_000,
    )
    sql = """
        SELECT id, matched_champion_label, matched_sector, predict_date
        FROM precursor_prediction_log
        WHERE market=? AND hit IS NULL AND predict_date <= ?
        ORDER BY predict_date ASC
        LIMIT ?
    """
    return sql, (str(market).lower(), str(cutoff)[:10], lim)


def forward_autopilot_analysis_closed_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """auto_pilot run_autonomous_analysis — Thompson/Kelly/엔진6/서킷용."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_CLOSED_TRADES_LIMIT,
        floor=50,
        ceiling=2_000,
    )
    sql = f"""
        SELECT {FORWARD_AUTOPILOT_CLOSED_COLUMNS}
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)


def forward_incubator_judge_closed_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    lim = _clamp_limit(
        limit,
        default=FORWARD_CLOSED_TRADES_LIMIT,
        floor=30,
        ceiling=2_000,
    )
    sql = f"""
        SELECT {FORWARD_INCUBATOR_JUDGE_COLUMNS}
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%' AND IFNULL(sig_type, '') LIKE '%INCUBATOR%'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)


def forward_weekly_tf_rotation_sql(
    *,
    market_type: str,
    since_date: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """auto_pilot 주간 Flow — TF 궤적 (entry_date 필터 + bounded)."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_WEEKLY_TF_ROTATION_LIMIT,
        floor=100,
        ceiling=5_000,
    )
    since = str(since_date)[:10]
    sql = f"""
        SELECT {FORWARD_WEEKLY_TF_COLUMNS}
        FROM bitget_forward_trades
        WHERE market_type=? AND entry_date >= ?
          AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
        ORDER BY entry_date ASC
        LIMIT ?
    """
    return sql, (str(market_type).lower(), since, lim)


def forward_weekly_flow_closed_sql(
    *,
    market_type: str,
    since_date: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """auto_pilot 주간 Flow — daily PnL / MVP 집계용 bounded CLOSED sample."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_WEEKLY_TF_ROTATION_LIMIT,
        floor=100,
        ceiling=5_000,
    )
    since = str(since_date)[:10]
    sql = f"""
        SELECT {FORWARD_WEEKLY_FLOW_CLOSED_COLUMNS}
        FROM bitget_forward_trades
        WHERE market_type=? AND exit_date >= ? AND status LIKE 'CLOSED%'
          AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
        ORDER BY exit_date DESC, id DESC
        LIMIT ?
    """
    return sql, (str(market_type).lower(), since, lim)


def forward_weekly_flow_tags_sql(
    *,
    since_date: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """bitget_pipelines weekly exec — flow_tags toxic tag stats (bounded)."""
    lim = _clamp_limit(
        limit,
        default=FORWARD_WEEKLY_TF_ROTATION_LIMIT,
        floor=100,
        ceiling=5_000,
    )
    since = str(since_date)[:10]
    sql = f"""
        SELECT {FORWARD_WEEKLY_FLOW_TAG_COLUMNS}
        FROM bitget_forward_trades
        WHERE exit_date >= ? AND status LIKE 'CLOSED%'
          AND IFNULL(flow_tags,'') != ''
          AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
        ORDER BY exit_date DESC, id DESC
        LIMIT ?
    """
    return sql, (since, lim)


EXPLORATION_BUDGET_ROLLING_COLUMNS: str = "sig_type, final_ret, sim_kelly_invest"


def forward_exploration_budget_closed_sql(
    *,
    since_date: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """exploration_budget 7d MAB — exit_date window + column projection + LIMIT."""
    lim = _clamp_limit(
        limit,
        default=EXPLORATION_BUDGET_ROLLING_CLOSED_LIMIT,
        floor=100,
        ceiling=EXPLORATION_BUDGET_ROLLING_CLOSED_LIMIT,
    )
    since = str(since_date)[:10]
    sql = f"""
        SELECT {EXPLORATION_BUDGET_ROLLING_COLUMNS}
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%' AND IFNULL(exit_date,'') >= ?
          AND final_ret IS NOT NULL
          AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
        ORDER BY exit_date DESC
        LIMIT ?
    """
    return sql, (since, lim)


UNDERDOG_MINER_COLUMNS: str = (
    "market_type, position_side, dyn_cpv, dyn_tb, v_energy, dyn_rs, final_ret, total_score"
)


def forward_underdog_miner_closed_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """underdog_miner — low-score high-return CLOSED DNA (bounded)."""
    lim = _clamp_limit(
        limit,
        default=UNDERDOG_MINER_CLOSED_LIMIT,
        floor=50,
        ceiling=UNDERDOG_MINER_CLOSED_LIMIT,
    )
    sql = f"""
        SELECT {UNDERDOG_MINER_COLUMNS}
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%'
          AND total_score <= 60
          AND final_ret >= 15.0
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)


BLACKHOLE_HUNTER_COLUMNS: str = "symbol, final_ret, dyn_cpv, dyn_tb, v_energy, dyn_rs"


def forward_blackhole_recent_closed_sql(
    *,
    since_date: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """blackhole_hunter — recent CLOSED toxic scan (exit_date window + LIMIT)."""
    lim = _clamp_limit(
        limit,
        default=BLACKHOLE_HUNTER_CLOSED_LIMIT,
        floor=100,
        ceiling=BLACKHOLE_HUNTER_CLOSED_LIMIT,
    )
    since = str(since_date)[:10]
    sql = f"""
        SELECT {BLACKHOLE_HUNTER_COLUMNS}
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%' AND IFNULL(exit_date,'') >= ?
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (since, lim)


TOXIC_GRAVEYARD_COLUMNS: str = (
    "market_type, dyn_cpv, dyn_tb, v_energy, dyn_rs, final_ret"
)


def forward_toxic_graveyard_closed_sql(
    *,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """toxic_graveyard_analyzer — CLOSED anti-pattern ML (column projection + LIMIT)."""
    lim = _clamp_limit(
        limit,
        default=TOXIC_GRAVEYARD_CLOSED_LIMIT,
        floor=100,
        ceiling=TOXIC_GRAVEYARD_CLOSED_LIMIT,
    )
    sql = f"""
        SELECT {TOXIC_GRAVEYARD_COLUMNS}
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%'
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)


DATA_MINER_MFE_WINNER_COLUMNS: str = (
    "id, entry_date, exit_date, market_type, symbol, timeframe, position_side, sig_type, "
    "dyn_cpv, dyn_tb, v_energy, dyn_rs, v_rs, mfe, final_ret"
)

DATA_MINER_MFE_TRAINING_COLUMNS: str = "market_type, symbol, timeframe, entry_date, mfe"


def forward_data_miner_mfe_winners_sql(
    *,
    timeframe: str,
    mfe_min: float,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """data_miner GMM — high-MFE CLOSED per timeframe (column projection + LIMIT)."""
    lim = _clamp_limit(
        limit,
        default=DATA_MINER_MFE_WINNERS_LIMIT,
        floor=50,
        ceiling=DATA_MINER_MFE_WINNERS_LIMIT,
    )
    sql = f"""
        SELECT {DATA_MINER_MFE_WINNER_COLUMNS}
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%'
          AND UPPER(timeframe)=?
          AND COALESCE(mfe, 0) >= ?
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (str(timeframe).upper(), float(mfe_min), lim)


def forward_data_miner_mfe_training_sql(
    *,
    timeframe: str,
    since_date: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """data_miner AST — recent MFE training rows (UTC since_date + LIMIT)."""
    lim = _clamp_limit(
        limit,
        default=DATA_MINER_MFE_TRAINING_LIMIT,
        floor=30,
        ceiling=DATA_MINER_MFE_TRAINING_LIMIT,
    )
    since = str(since_date)[:10]
    tf = str(timeframe).upper()
    sql = f"""
        SELECT {DATA_MINER_MFE_TRAINING_COLUMNS}
        FROM bitget_forward_trades
        WHERE status LIKE 'CLOSED%'
          AND UPPER(timeframe)=?
          AND IFNULL(entry_date, '') >= ?
          AND COALESCE(mfe, 0) > 0
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (tf, since, lim)


def forward_cluster_mining_symbols_sql(
    *,
    since_date: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """build_supernova_csv — recent forward_trades triples for OHLCV table resolution."""
    lim = _clamp_limit(
        limit,
        default=SUPERNOVA_CLUSTER_FORWARD_SYMBOL_LIMIT,
        floor=50,
        ceiling=SUPERNOVA_CLUSTER_FORWARD_SYMBOL_LIMIT,
    )
    since = str(since_date)[:10]
    sql = """
        SELECT market_type, symbol, timeframe
        FROM bitget_forward_trades
        WHERE IFNULL(entry_date, '') >= ?
           OR IFNULL(status, '') NOT LIKE 'CLOSED%'
        GROUP BY market_type, symbol, timeframe
        ORDER BY MAX(id) DESC
        LIMIT ?
    """
    return sql, (since, lim)


def sqlite_bitget_ohlcv_tables_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """Fallback BITGET OHLCV table names from sqlite_master (bounded)."""
    lim = _clamp_limit(
        limit,
        default=SUPERNOVA_CLUSTER_MAX_TABLES,
        floor=1,
        ceiling=SUPERNOVA_CLUSTER_MAX_TABLES,
    )
    sql = """
        SELECT name FROM sqlite_master
        WHERE type='table'
          AND name LIKE 'BITGET_%'
          AND name NOT LIKE '%__tmp%'
        ORDER BY name
        LIMIT ?
    """
    return sql, (lim,)


def sqlite_bitget_ohlcv_1d_tables_sql(
    *,
    limit: int | None = None,
    exclude_btc: bool = True,
) -> tuple[str, tuple]:
    """pump_forensics / forensics_pioneer — 1D OHLCV table name scan cap."""
    lim = _clamp_limit(
        limit,
        default=TIME_MACHINE_MAX_TABLES,
        floor=1,
        ceiling=TIME_MACHINE_MAX_TABLES,
    )
    btc_clause = "AND name NOT LIKE '%BTC_USDT%'" if exclude_btc else ""
    sql = f"""
        SELECT name FROM sqlite_master
        WHERE type='table'
          AND name LIKE 'BITGET_%_1D'
          AND name NOT LIKE '%__tmp%'
          {btc_clause}
        ORDER BY name
        LIMIT ?
    """
    return sql, (lim,)


def sqlite_bitget_ohlcv_tf_tables_sql(
    *,
    timeframe: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """supernova_hunter DNA batch — TF-suffix OHLCV table list cap."""
    lim = _clamp_limit(
        limit,
        default=SUPERNOVA_CLUSTER_MAX_TABLES,
        floor=1,
        ceiling=SUPERNOVA_CLUSTER_MAX_TABLES,
    )
    tf = str(timeframe).upper()
    sql = """
        SELECT name FROM sqlite_master
        WHERE type='table'
          AND name LIKE 'BITGET_%'
          AND name LIKE ?
          AND name NOT LIKE '%__tmp%'
        ORDER BY name
        LIMIT ?
    """
    return sql, (f"%_{tf}", lim)


def sqlite_bitget_scan_tables_sql(
    *,
    market_type: str,
    timeframe: str,
    limit: int | None = None,
    exclude_btc: bool = True,
) -> tuple[str, tuple]:
    """supernova_hunter live scan — market+TF table list cap."""
    lim = _clamp_limit(
        limit,
        default=SUPERNOVA_CLUSTER_MAX_TABLES,
        floor=1,
        ceiling=SUPERNOVA_CLUSTER_MAX_TABLES,
    )
    pref = "BITGET_SPOT_" if str(market_type).lower() == "spot" else "BITGET_FUT_"
    tf = str(timeframe).upper()
    btc_clause = "AND name NOT LIKE '%BTC_USDT%'" if exclude_btc else ""
    sql = f"""
        SELECT name FROM sqlite_master
        WHERE type='table'
          AND name LIKE ?
          AND name LIKE ?
          AND name NOT LIKE '%__tmp%'
          {btc_clause}
        ORDER BY name
        LIMIT ?
    """
    return sql, (f"{pref}%", f"%_{tf}", lim)


def forward_grand_report_closed_sql(
    *,
    market_type: str,
    start: str,
    end: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """weekend_grand_report — 기간 필터 + column projection."""
    lim = _clamp_limit(
        limit,
        default=GRAND_REPORT_CLOSED_LIMIT,
        floor=200,
        ceiling=GRAND_REPORT_CLOSED_LIMIT,
    )
    start_s, end_s = str(start)[:10], str(end)[:10]
    sql = f"""
        SELECT {FORWARD_GRAND_REPORT_CLOSED_COLUMNS}
        FROM bitget_forward_trades
        WHERE market_type=? AND status LIKE 'CLOSED%'
          AND IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'
          AND IFNULL(exit_date, '') >= ? AND IFNULL(exit_date, '') <= ?
        ORDER BY exit_date ASC
        LIMIT ?
    """
    return sql, (str(market_type).lower(), start_s, end_s + " 23:59:59", lim)


GRAND_REPORT_GENESIS_COLUMNS: str = (
    "market, champion_label, kind, status, realized_fwd_ret, crowned_date, resolved_at"
)

GRAND_REPORT_DEATHMATCH_CHAMPION_COLUMNS: str = (
    "market, champion_label, composite_score, win_rate"
)

GRAND_REPORT_ELIMINATION_COLUMNS: str = "market, arm_id, reason, event_date"

GRAND_REPORT_REGISTRY_COLUMNS: str = (
    "market, group_key, state, last_promoted_at, last_demoted_at, promote_reason, demote_reason"
)


def grand_report_genesis_sql(
    *,
    start: str,
    end: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """weekend_grand_report — champion_precursor_genesis period window."""
    lim = _clamp_limit(
        limit,
        default=GRAND_REPORT_GENESIS_LIMIT,
        floor=50,
        ceiling=GRAND_REPORT_GENESIS_LIMIT,
    )
    start_s, end_s = str(start)[:10], str(end)[:10]
    sql = f"""
        SELECT {GRAND_REPORT_GENESIS_COLUMNS}
        FROM champion_precursor_genesis
        WHERE IFNULL(resolved_at, '') >= ?
          AND IFNULL(resolved_at, '') <= ?
        ORDER BY resolved_at DESC
        LIMIT ?
    """
    return sql, (start_s, end_s + " 23:59:59", lim)


def grand_report_deathmatch_champion_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """weekend_grand_report — current deathmatch champions snapshot."""
    lim = _clamp_limit(
        limit,
        default=GRAND_REPORT_DEATHMATCH_CHAMPION_LIMIT,
        floor=1,
        ceiling=GRAND_REPORT_DEATHMATCH_CHAMPION_LIMIT,
    )
    sql = f"""
        SELECT {GRAND_REPORT_DEATHMATCH_CHAMPION_COLUMNS}
        FROM deathmatch_champion
        ORDER BY composite_score DESC
        LIMIT ?
    """
    return sql, (lim,)


def grand_report_elimination_events_sql(
    *,
    start: str,
    end: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """weekend_grand_report — deathmatch elimination events in period."""
    lim = _clamp_limit(
        limit,
        default=GRAND_REPORT_ELIMINATION_EVENT_LIMIT,
        floor=20,
        ceiling=GRAND_REPORT_ELIMINATION_EVENT_LIMIT,
    )
    start_s, end_s = str(start)[:10], str(end)[:10]
    sql = f"""
        SELECT {GRAND_REPORT_ELIMINATION_COLUMNS}
        FROM deathmatch_elimination_event
        WHERE IFNULL(event_date, '') >= ?
          AND IFNULL(event_date, '') <= ?
        ORDER BY event_date DESC
        LIMIT ?
    """
    return sql, (start_s, end_s + " 23:59:59", lim)


def grand_report_strategy_registry_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """weekend_grand_report — strategy lifecycle registry snapshot."""
    lim = _clamp_limit(
        limit,
        default=GRAND_REPORT_STRATEGY_REGISTRY_LIMIT,
        floor=50,
        ceiling=GRAND_REPORT_STRATEGY_REGISTRY_LIMIT,
    )
    sql = f"""
        SELECT {GRAND_REPORT_REGISTRY_COLUMNS}
        FROM strategy_registry
        ORDER BY IFNULL(updated_at, '') DESC, market ASC, group_key ASC
        LIMIT ?
    """
    return sql, (lim,)


def grand_report_registry_promoted_sql(
    *,
    start: str,
    end: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """weekend_grand_report — promoted rows in reporting window (detail cap)."""
    lim = _clamp_limit(
        limit,
        default=GRAND_REPORT_REGISTRY_WINDOW_DETAIL_LIMIT,
        floor=6,
        ceiling=GRAND_REPORT_REGISTRY_WINDOW_DETAIL_LIMIT,
    )
    start_s, end_s = str(start)[:10], str(end)[:10]
    sql = f"""
        SELECT {GRAND_REPORT_REGISTRY_COLUMNS}
        FROM strategy_registry
        WHERE IFNULL(last_promoted_at, '') >= ?
          AND IFNULL(last_promoted_at, '') <= ?
        ORDER BY last_promoted_at DESC
        LIMIT ?
    """
    return sql, (start_s, end_s + " 23:59:59", lim)


def grand_report_registry_demoted_sql(
    *,
    start: str,
    end: str,
    limit: int | None = None,
) -> tuple[str, tuple]:
    """weekend_grand_report — demoted rows in reporting window (detail cap)."""
    lim = _clamp_limit(
        limit,
        default=GRAND_REPORT_REGISTRY_WINDOW_DETAIL_LIMIT,
        floor=6,
        ceiling=GRAND_REPORT_REGISTRY_WINDOW_DETAIL_LIMIT,
    )
    start_s, end_s = str(start)[:10], str(end)[:10]
    sql = f"""
        SELECT {GRAND_REPORT_REGISTRY_COLUMNS}
        FROM strategy_registry
        WHERE IFNULL(last_demoted_at, '') >= ?
          AND IFNULL(last_demoted_at, '') <= ?
        ORDER BY last_demoted_at DESC
        LIMIT ?
    """
    return sql, (start_s, end_s + " 23:59:59", lim)


def forward_identity_trades_sql(
    *,
    market_type: str,
    rolling_cutoff: str,
    session_anchor: str,
    closed_limit: int | None = None,
) -> tuple[str, tuple]:
    """Identity diagnose/backfill — OPEN 전량 보존 + rolling-window CLOSED bounded."""
    lim = _clamp_limit(
        closed_limit,
        default=FORWARD_IDENTITY_CLOSED_LIMIT,
        floor=200,
        ceiling=FORWARD_IDENTITY_CLOSED_LIMIT,
    )
    mwhere, mparams = _identity_market_sql(market_type)
    ws, we = str(rolling_cutoff)[:10], str(session_anchor)[:10]
    entry_slice = "substr(COALESCE(NULLIF(TRIM(entry_date),''), ''), 1, 10)"
    sql = f"""
        SELECT {FORWARD_IDENTITY_COLUMNS}
        FROM bitget_forward_trades
        WHERE ({mwhere})
          AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
          AND (
            UPPER(TRIM(IFNULL(status,''))) IN ('OPEN','ACTIVE')
            OR (
              UPPER(IFNULL(status,'')) LIKE 'CLOSED%'
              AND {entry_slice} >= ?
              AND {entry_slice} <= ?
              AND id IN (
                  SELECT id FROM (
                      SELECT id FROM bitget_forward_trades
                      WHERE ({mwhere})
                        AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
                        AND UPPER(IFNULL(status,'')) LIKE 'CLOSED%'
                        AND {entry_slice} >= ?
                        AND {entry_slice} <= ?
                      ORDER BY id DESC
                      LIMIT ?
                  )
              )
            )
          )
        ORDER BY id DESC
    """
    params = mparams + (ws, we) + mparams + (ws, we, lim)
    return sql, params


# ---------------------------------------------------------------------------
# task_queue — claim_next column projection (SELECT * 금지)
# ---------------------------------------------------------------------------
TASK_QUEUE_CLAIM_COLUMNS: str = (
    "id, engine, mode, payload, priority, status, attempts, max_attempts, "
    "enqueued_at, available_at, picked_at, finished_at, worker, last_error"
)


def task_queue_claim_next_sql() -> str:
    """PENDING 픽업 1건 — explicit columns + LIMIT 1 (Tier-2 SSOT)."""
    return (
        f"SELECT {TASK_QUEUE_CLAIM_COLUMNS} FROM task_queue "
        "WHERE status='PENDING' AND available_at <= ? "
        "ORDER BY priority ASC, id ASC LIMIT 1"
    )


# ---------------------------------------------------------------------------
# alt_data.sqlite macro_daily — column projection (SELECT * 금지)
# ---------------------------------------------------------------------------
MACRO_DAILY_COLUMNS: str = (
    "date, btc_dominance, eth_btc_ratio, total_market_cap_usd, "
    "market_cap_change_24h, btc_price_usd, eth_price_usd"
)


def macro_daily_last_row_sql() -> str:
    """alt_data_miner — most recent macro row (LIMIT 1, projected columns)."""
    return f"SELECT {MACRO_DAILY_COLUMNS} FROM macro_daily ORDER BY date DESC LIMIT 1"


def macro_daily_lookback_sql(*, max_rows: int | None = None) -> tuple[str, tuple]:
    """macro_hydrate — recent macro rows (column projection + bounded LIMIT)."""
    lim = max(1, int(max_rows if max_rows is not None else MACRO_DAILY_LOOKBACK_MAX_ROWS))
    sql = f"SELECT {MACRO_DAILY_COLUMNS} FROM macro_daily ORDER BY date DESC LIMIT ?"
    return sql, (lim,)


SHADOW_BLOCKED_COLUMNS: str = (
    "id, market_type, symbol, reason, position_side, timeframe, entry_price, blocked_at"
)


def shadow_blocked_history_sql(*, limit: int | None = None) -> tuple[str, tuple]:
    """shadow_performance_tracker — recent blocked trades (column projection + LIMIT)."""
    lim = _clamp_limit(
        limit,
        default=SHADOW_BLOCKED_READ_LIMIT,
        floor=50,
        ceiling=SHADOW_BLOCKED_READ_LIMIT,
    )
    sql = f"""
        SELECT {SHADOW_BLOCKED_COLUMNS}
        FROM bitget_blocked_trade_history
        ORDER BY id DESC
        LIMIT ?
    """
    return sql, (lim,)
