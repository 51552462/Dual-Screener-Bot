"""Virtual position entry and exit engine."""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from bitget.infra.shared_db_connector import get_connection
from bitget.forward.gates import (
    _apply_thompson_kelly_multiplier,
    _calc_atr14,
    _calc_market_breadth,
    _extract_4d_dna_from_facts,
    _facts_cos_scalar_01,
    _is_blocked_by_anti_patterns,
    _load_bench_close,
    _load_hist,
    _table_name,
    _tf_weight,
    compute_evolved_alpha_bonus_score,
    evaluate_evolved_alpha_formula,
)
from bitget.forward.shared import (
    DB_PATH,
    DEFAULT_MAX_OPEN_POSITIONS,
    _cached_funding_snapshot,
    get_exploration_role_scaler,
    init_forward_db,
    load_system_config,
    save_system_config,
    send_telegram_msg,
)
from bitget.governance.meta_consumer import (
    apply_meta_kelly_merge,
    effective_max_position_pct,
    load_meta_state_resolved,
)

_DEFAULT_BITGET_MAX_OPEN_POSITIONS = DEFAULT_MAX_OPEN_POSITIONS


def _execute_retry(conn, sql, params, *, context="", max_retry=5):
    """`database is locked` 재시도(지수 백오프). 커밋은 호출자 책임(같은 트랜잭션 유지용)."""
    for attempt in range(max_retry):
        try:
            return conn.execute(sql, params)
        except sqlite3.OperationalError as e:
            em = str(e).lower()
            if "database is locked" in em and attempt < max_retry - 1:
                wait_s = 0.5 * (2 ** attempt)
                print(f"⏳ [DB LOCK 재시도] {context} #{attempt + 1}/{max_retry} wait={wait_s:.2f}s")
                time.sleep(wait_s)
                continue
            raise


def _execute_commit_retry(conn, sql, params, *, context="", max_retry=5):
    """`database is locked` 재시도 후 즉시 커밋.

    track_daily_positions 처럼 여러 포지션을 순회하며 쓰는 루프에서, 각 행마다
    바로 커밋해 writer 락 점유 시간을 한 문장(statement) 수준으로 최소화한다.
    (과거엔 루프 전체가 끝날 때까지 커밋을 미뤄 락을 길게 물고 있었고, 그 사이
    다른 프로세스의 scan INSERT 가 busy_timeout(60s)을 넘겨 `database is locked`
    로 전체 스캔 작업이 통째로 실패하는 원인이었다.)
    """
    _execute_retry(conn, sql, params, context=context, max_retry=max_retry)
    conn.commit()


def try_add_virtual_position(
    market_type,
    symbol,
    timeframe,
    sig_type,
    score,
    entry_price,
    facts,
    side="LONG",
    entry_high=0.0,
):
    init_forward_db()
    cfg = load_system_config()
    if str(cfg.get("GLOBAL_CIRCUIT_BREAKER", "OFF")).upper() == "ON":
        return False, "🚫 글로벌 서킷 브레이커 ON: 계좌 통합 동결 — 신규 진입 차단."
    tf = str(timeframe).upper()
    market_type = str(market_type).lower()
    symbol = str(symbol)
    position_side = str(side or "LONG").upper()
    if position_side not in ("LONG", "SHORT"):
        position_side = "LONG"
    if market_type == "spot" and position_side == "SHORT":
        return False, "현물(Spot) 시장 숏(Short) 진입 불가"
    entry_high_val = float(entry_high) if entry_high is not None else float(entry_price)

    score_bucket = int(float(score) // 10) * 10
    if score_bucket >= 100:
        score_bucket = 90
    tier_label = f"{score_bucket}점대"
    is_incubator_shadow = "[INCUBATOR_" in str(sig_type).upper()

    conn = get_connection(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "SELECT id FROM bitget_forward_trades WHERE symbol=? AND timeframe=? AND market_type=? AND position_side=? AND status='OPEN'",
        (symbol, tf, market_type, position_side),
    )
    if cur.fetchone():
        conn.close()
        return False, "중복 보유 중"

    try:
        _q = cfg.get("BITGET_MAX_OPEN_POSITIONS", _DEFAULT_BITGET_MAX_OPEN_POSITIONS)
        max_open_quota = max(1, int(float(_q)))
    except (TypeError, ValueError):
        max_open_quota = _DEFAULT_BITGET_MAX_OPEN_POSITIONS
    cur.execute("SELECT COUNT(*) FROM bitget_forward_trades WHERE status='OPEN'")
    _open_quota_n = cur.fetchone()[0] or 0
    if int(_open_quota_n) >= max_open_quota:
        conn.close()
        return False, "🚨 시장 쿼터 초과"

    blocked, sim = _is_blocked_by_anti_patterns(cfg, facts, threshold=0.85)
    if blocked:
        try:
            import bitget.shadow_tracking as bitget_shadow_tracking
            bitget_shadow_tracking.record_blocked_trade(
                symbol=symbol,
                reason=f"TOXIC_ANTI_PATTERN(sim={sim:.3f})",
                entry_price=float(entry_price),
                market_type=market_type,
                name=symbol,
                position_side=position_side,
                timeframe=tf,
            )
        except Exception:
            pass
        conn.close()
        return False, f"ANTI_PATTERNS 차단: 참사 DNA 유사도 {sim:.3f} >= 0.850"

    hist_df = _load_hist(conn, market_type, symbol, tf, limit=300)
    if hist_df is None or len(hist_df) < 60:
        conn.close()
        return False, "ATR 계산용 히스토리 부족"

    evaluate_df = hist_df.copy()
    for col in ("Open", "High", "Low", "Close", "Volume"):
        evaluate_df[col] = pd.to_numeric(evaluate_df[col], errors="coerce")
    evaluate_df = evaluate_df.dropna(subset=("Open", "High", "Low", "Close", "Volume"), how="any")
    alpha_bonus_score = compute_evolved_alpha_bonus_score(cfg, evaluate_df)

    max_alpha_cos = _facts_cos_scalar_01(facts or {}, score)
    max_alpha_cos_effective = min(1.0, float(max_alpha_cos) + float(alpha_bonus_score))

    dyn_cos_limit = float(cfg.get("DYNAMIC_ALPHA_LIMIT", cfg.get("DYNAMIC_SUPERNOVA_CUTOFF", 0.75)))
    dyn_dtw_limit = float(cfg.get("DYNAMIC_DTW_LIMIT", 2.5))
    raw_dtw = None if not facts else facts.get("dtw_score")
    if raw_dtw is None or (isinstance(raw_dtw, str) and raw_dtw.strip() == ""):
        dtw_ok = True
    else:
        try:
            fd = float(raw_dtw)
            dtw_ok = fd <= dyn_dtw_limit
        except (TypeError, ValueError):
            dtw_ok = True

    cutoff_passed = (max_alpha_cos_effective >= dyn_cos_limit) and dtw_ok

    sig_type_row = sig_type
    if cutoff_passed and alpha_bonus_score > 0:
        sig_type_row = f"{sig_type_row} [🧬알파 융합 AST]"

    if not is_incubator_shadow and not cutoff_passed:
        conn.close()
        return False, (
            f"시계열 게이트: AST 융합 Cos_eff={max_alpha_cos_effective:.3f} (기준≥{dyn_cos_limit}) 또는 "
            f"DTW 조건 불만족(DTW cutoff≤{dyn_dtw_limit})"
        )

    entry_atr = _calc_atr14(hist_df)
    atr_sl_mult = float(cfg.get("ATR_SL_MULT", 2.0))
    if position_side == "SHORT":
        stop_price = float(entry_price) + (atr_sl_mult * entry_atr)
        risk_distance = stop_price - float(entry_price)
    else:
        stop_price = float(entry_price) - (atr_sl_mult * entry_atr)
        risk_distance = float(entry_price) - stop_price
    if risk_distance <= 0:
        conn.close()
        return False, "리스크 거리 계산 실패"

    fixed_risk_pct = float(cfg.get("FIXED_RISK_PCT", 0.02))
    try:
        from bitget.governance.meta_consumer import load_meta_state_resolved, resolve_trading_kelly_base

        kelly_risk_pct = resolve_trading_kelly_base(cfg, load_meta_state_resolved())
    except Exception:
        kelly_risk_pct = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01))
    w_s1 = float(cfg.get("WEIGHT_S1", 1.0) or 1.0)
    w_s4 = float(cfg.get("WEIGHT_S4", 1.0) or 1.0)
    breadth_now = _calc_market_breadth(conn)
    # breadth 기반 미세 보정: 알트 확산이면 추세(S1), 쏠림이면 돌파/역추세(S4/S6/S7) 가중
    if breadth_now > 1.03:
        w_s1 *= 1.15
    elif breadth_now < 0.97:
        w_s1 *= 0.85
        w_s4 *= 1.15
    if "S1" in sig_type:
        kelly_risk_pct *= w_s1
    if "S4" in sig_type or "S6" in sig_type or "S7" in sig_type:
        kelly_risk_pct *= w_s4
    tf_weight = _tf_weight(tf, cfg)
    kelly_risk_pct *= tf_weight

    # 💡 [Namespace Thompson Kelly Sampler] auto_forward_tester 와 동일: [TF]_*_BETA_PARAMS 로 자본 동적 배분
    kelly_risk_pct = _apply_thompson_kelly_multiplier(cfg, tf, sig_type, float(kelly_risk_pct))
    sector = _coin_asset_group(symbol)
    predicted_sector = str(cfg.get("PREDICTED_NEXT_SECTOR", "UNKNOWN"))
    is_rotation_prebuy = (sector == predicted_sector)
    sys_config = cfg

    # 👇👇 [V105.0 자율 진화] 순환매 선취매 태깅 및 베팅 어드밴티지 코인 이식 👇👇
    if is_rotation_prebuy:
        sig_type_row += " #순환매_선취매"
        # 관제탑이 주말 데스매치를 통해 우위를 증명했다면 켈리 비중 2배 뻥튀기
        if sys_config.get("ROTATION_ADVANTAGE_ACTIVE", False):
            kelly_risk_pct *= 2.0 

    core_group = _extract_core_group(sig_type)
    _meta_state = load_meta_state_resolved()
    ns_prefix = _thompson_ns_prefix(tf, sig_type)
    kelly_risk_pct = apply_meta_kelly_merge(
        kelly_risk_pct,
        _meta_state,
        ns_prefix=ns_prefix,
        core_group_name=core_group,
        sys_config=sys_config,
        entry_facts=facts if isinstance(facts, dict) else {},
        sector_mapped=str(sector),
    )

    # [동적 탐험예산 — 7일 롤링 MAB] 최종 Kelly 비중에 챔피언(LIVE)/탐험
    # (OBSERVING·CANDIDATE) 역할 스케일러를 곱한다. 국면전환 방어 중이거나
    # 미분류(NEUTRAL) 그룹은 스케일러 1.0(무변경). 실패 시 항상 안전 폴백.
    _explore_scaler, _explore_role = get_exploration_role_scaler(cfg, core_group)
    kelly_risk_pct *= _explore_scaler

    account_size = float(cfg.get("ACCOUNT_SIZE_USDT", 100000))
    max_position_pct = float(effective_max_position_pct(cfg, _meta_state))

    cur.execute(
        "SELECT SUM((sim_kelly_invest * final_ret) / 100.0) FROM bitget_forward_trades WHERE status LIKE 'CLOSED%' AND sig_type LIKE ?",
        (f"%{core_group}%",),
    )
    realized_pnl = float(cur.fetchone()[0] or 0.0)
    group_current_seed = account_size + realized_pnl

    # Pre-scan slippage stage (seed vs 24h liquidity)
    if not is_incubator_shadow:
        from bitget.trading.slippage_guard import check_pre_scan_liquidity

        liq_ok, liq_reason = check_pre_scan_liquidity(
            group_current_seed,
            facts,
            cfg,
            symbol=symbol,
            market_type=market_type,
            is_incubator_shadow=False,
        )
        if not liq_ok:
            conn.close()
            return False, liq_reason

    cur.execute(
        "SELECT SUM(margin_used) FROM bitget_forward_trades WHERE status='OPEN' AND sig_type LIKE ?",
        (f"%{core_group}%",),
    )
    locked_cash = float(cur.fetchone()[0] or 0.0)
    available_cash = group_current_seed - locked_cash
    if available_cash <= 0:
        conn.close()
        return False, f"예수금 부족: [{core_group}] 가용 자산 없음"

    treasury_key = "TREASURY_SPOT_USDT" if market_type == "spot" else "TREASURY_FUTURES_USDT"
    treasury_balance = float(cfg.get(treasury_key, 100000.0))
    if treasury_balance <= 0:
        conn.close()
        return False, f"{treasury_key} 잔고 부족"

    leverage = 1.0 if market_type == "spot" else float(cfg.get("FUTURES_LEVERAGE", 3.0))
    max_invest_limit = min(group_current_seed * max_position_pct, available_cash, treasury_balance)

    raw_qty = float((group_current_seed * kelly_risk_pct) / risk_distance)
    raw_notional = raw_qty * float(entry_price)
    if market_type == "futures":
        raw_notional *= leverage
    margin_required = raw_notional if market_type == "spot" else raw_notional / max(leverage, 1e-9)

    if margin_required > max_invest_limit:
        margin_used = max_invest_limit
        sim_kelly_invest = margin_used if market_type == "spot" else margin_used * leverage
    else:
        margin_used = margin_required
        sim_kelly_invest = raw_notional

    if is_incubator_shadow:
        # 인큐베이터 섀도우 트레이딩: 국고 손실 원천 차단 (가상 기록만 유지)
        margin_used = 0.0
        sim_kelly_invest = 0.0

    quantity = sim_kelly_invest / float(entry_price) if float(entry_price) > 0 else 0.0
    if quantity <= 0 and not is_incubator_shadow:
        conn.close()
        return False, "수량 산출 실패"
    if is_incubator_shadow:
        quantity = 0.0

    fixed_qty = float((group_current_seed * fixed_risk_pct) / risk_distance)
    fixed_notional = fixed_qty * float(entry_price)
    if market_type == "futures":
        fixed_notional *= leverage
    _ = fixed_notional

    now = datetime.utcnow().strftime("%Y-%m-%d")
    entry_cos_score = float(max_alpha_cos_effective * 100.0)
    entry_dtw_score = float(facts.get("dtw_score", facts.get("entry_dtw_score", 0.0)) or 0.0)
    fr0 = 0.0
    fts0 = ""
    acc0 = 0.0
    if str(market_type).lower() == "futures":
        try:
            _s = fetch_funding_snapshot(symbol)
            if _s:
                fr0 = float(_s.get("funding_rate") or 0.0)
                fts0 = str(_s.get("next_funding_iso") or _s.get("next_funding_ts") or "").strip()
        except Exception:
            pass
    _execute_retry(
        conn,
        """
        INSERT INTO bitget_forward_trades
        (entry_date, market_type, symbol, timeframe, sig_type, tier, total_score, dyn_rs, dyn_cpv, dyn_tb,
         entry_price, position_side, entry_atr, entry_high, atr_sl_mult, stop_price, leverage, tf_weight, sim_kelly_risk_pct, margin_used,
         sim_kelly_invest, quantity, entry_cos_score, entry_dtw_score, v_cpv, v_yang, v_energy, v_rs, max_high, min_low, status,
         sim_stat_status, sim_tech_status, sim_breadth_status, entry_breadth, live_a_status, cand_b_status, champ_c_status,
         funding_rate_last, funding_next_settle_ts, funding_accum_usdt_est)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            now,
            market_type,
            symbol,
            tf,
            sig_type_row,
            tier_label,
            float(score),
            float(facts.get("dyn_rs", 0)),
            float(facts.get("dyn_cpv", 0)),
            float(facts.get("dyn_tb", 0)),
            float(entry_price),
            position_side,
            round(entry_atr, 6),
            entry_high_val,
            atr_sl_mult,
            float(stop_price),
            leverage,
            tf_weight,
            float(kelly_risk_pct),
            float(margin_used),
            float(sim_kelly_invest),
            float(quantity),
            entry_cos_score,
            entry_dtw_score,
            float(facts.get("v_cpv", 0)),
            float(facts.get("v_yang", 0)),
            float(facts.get("v_energy", 0)),
            float(facts.get("v_rs", 0)),
            float(entry_price),
            float(entry_price),
            "OPEN",
            "OPEN",
            "OPEN",
            "OPEN",
            float(breadth_now),
            "OPEN",
            "OPEN",
            "OPEN",
            fr0,
            fts0,
            acc0,
        ),
        context=f"신규진입 {symbol}",
    )
    satellite_tags = None
    try:
        import bitget.shadow_tracking as bitget_shadow_tracking
        satellite_tags = bitget_shadow_tracking.build_satellite_tags(cfg)
    except Exception:
        satellite_tags = None
    if satellite_tags is not None:
        try:
            import bitget.shadow_tracking as bitget_shadow_tracking
            logged_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            bitget_shadow_tracking.insert_virtual_trade_row(
                cur,
                market_type,
                symbol,
                symbol,
                float(entry_price),
                sig_type_row,
                str(satellite_tags),
                logged_at,
                position_side=position_side,
                timeframe=tf,
            )
        except Exception:
            pass
    conn.commit()
    # DB INSERT/COMMIT 성공 이후에만 국고 차감 저장 (자금 증발 방지)
    cfg[treasury_key] = max(0.0, treasury_balance - margin_used)
    save_system_config(cfg)
    conn.close()
    return True, f"편입 성공: {symbol} {tf} [{market_type}] kelly={kelly_risk_pct:.4f} tf_w={tf_weight:.2f} lev={leverage:.1f}"

def _get_latest_bar(conn, market_type, symbol, timeframe):
    df = _load_hist(conn, market_type, symbol, timeframe, limit=3)
    if df is None or len(df) < 2:
        return None
    last = df.iloc[-1]
    prev = df.iloc[-2]
    return {
        "close": float(last["Close"]),
        "open": float(last["Open"]),
        "high": float(last["High"]),
        "low": float(last["Low"]),
        "vol": float(last["Volume"]),
        "prev_close": float(prev["Close"]),
        "date": str(last["Date"].date()),
        "hist_df": _load_hist(conn, market_type, symbol, timeframe, limit=300),
    }

def _floating_pnl_usdt_open_row(conn, r) -> float:
    """OPEN 한 줄 기준 현재 평가 USDT 손익(주식 sim_kelly_invest·수익률 곱 패턴과 동일). 양수/음수 반환."""
    latest = _get_latest_bar(conn, r["market_type"], r["symbol"], r["timeframe"])
    if latest is None:
        return 0.0
    ep = float(r["entry_price"] or 0.0)
    if ep <= 0:
        return 0.0
    c = float(latest["close"])
    pos_side = str(r.get("position_side", "LONG")).upper()
    if pos_side == "SHORT":
        current_ret_pct = ((ep - c) / ep) * 100.0
    else:
        current_ret_pct = ((c - ep) / ep) * 100.0
    notion = float(r.get("sim_kelly_invest", 0.0) or 0.0)
    if notion <= 0.0:
        margin_used = float(r.get("margin_used", 0.0) or 0.0)
        lev = float(r.get("leverage", 1.0) or 1.0)
        mkt = str(r.get("market_type", "")).lower()
        if margin_used > 0 and mkt == "futures" and lev > 0:
            notion = margin_used * lev
        else:
            notion = margin_used
    return float(notion) * (current_ret_pct / 100.0)

def _aggregate_global_open_loss_usdt(conn) -> tuple[float, int]:
    """
    status=OPEN 인 전 종목 미실현 손익 중 손실분만 합산 (양수 포지션 PnL은 제외 → 주식 total_open_loss_amount 와 동일).
    반환: (total_open_loss_amount, open_count).
    """
    df_open = pd.read_sql(
        "SELECT * FROM bitget_forward_trades WHERE status='OPEN'",
        conn,
    )
    total_open_loss_amount = 0.0
    for _, row in df_open.iterrows():
        pnl = _floating_pnl_usdt_open_row(conn, row)
        if pnl < 0:
            total_open_loss_amount += pnl
    return total_open_loss_amount, len(df_open)

def _finalize_global_circuit_breaker_track(conn, cfg):
    """OPEN 전역 미실현 손실 기준 글로벌 서킷 ON + 커밋.(주식 track_daily_positions 패턴)"""
    base_seed = float(cfg.get("ACCOUNT_SIZE_USDT", 100000.0) or 0.0)
    total_open_loss_amount, n_open_global = _aggregate_global_open_loss_usdt(conn)
    conn.commit()
    if base_seed > 0:
        loss_ratio = total_open_loss_amount / base_seed
        if loss_ratio <= -0.05:
            latest_config = load_system_config()
            if str(latest_config.get("GLOBAL_CIRCUIT_BREAKER", "OFF")).upper() != "ON":
                latest_config["GLOBAL_CIRCUIT_BREAKER"] = "ON"
                latest_config["GLOBAL_CIRCUIT_BREAKER_TRIGGERED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                latest_config["GLOBAL_CIRCUIT_BREAKER_LAST_LOSS_RATIO"] = round(float(loss_ratio), 6)
                save_system_config(latest_config)
                send_telegram_msg(
                    f"🚨 <b>[GLOBAL CIRCUIT BREAKER — Bitget]</b>\n"
                    f"▪ 미실현 손실 합(OPEN만): <b>{total_open_loss_amount:,.2f} USDT</b>\n"
                    f"▪ 기준 시드(ACCOUNT_SIZE_USDT): <b>{base_seed:,.2f} USDT</b>\n"
                    f"▪ 손실/시드: <b>{loss_ratio * 100:.2f}%</b> (한계 ≤-5.0%)\n"
                    f"▪ 현재 OPEN 수: <b>{n_open_global}</b>\n"
                    f"조치: <code>GLOBAL_CIRCUIT_BREAKER=ON</code> — 신규 진입 전면 차단."
                )

def _days_since_entry_date(entry_date_val):
    try:
        if entry_date_val is None:
            return None
        s = str(entry_date_val).strip()[:10]
        if len(s) < 10:
            return None
        ed = datetime.strptime(s, "%Y-%m-%d").date()
        return (datetime.utcnow().date() - ed).days
    except Exception:
        return None

def _force_close_zombie_delist_or_halt(conn, r):
    """
    DB/캔들 단절이 14일+ 지속되면 상장폐지·장기 거래정지로 간주하고 좀비 포지션 강제 청산 + 국고 환입.
    """
    ret = -100.0
    exit_rsn = "상폐/거래정지 강제청산"
    exit_type = "DELIST_OR_HALT"
    exit_d = datetime.utcnow().strftime("%Y-%m-%d")
    ep = float(r.get("entry_price") or 0.0)
    new_max = float(r.get("max_high") or ep)
    new_min = float(r.get("min_low") or ep)
    new_bars = int(r.get("bars_held") or 0)
    new_up = float(r.get("up_vol_sum") or 0.0)
    new_down = float(r.get("down_vol_sum") or 0.0)
    eb = float(r.get("entry_breadth") or 1.0)
    flow_tags = "#상폐_거래정지_좀비해제"
    neg = float(ret)
    update_sql = """
        UPDATE bitget_forward_trades
        SET status='CLOSED_LOSS', exit_date=?, exit_reason=?, final_ret=?, mfe=?,
            max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?,
            exit_type=?,
            sim_stat_ret=?, sim_stat_status='CLOSED_LOSS',
            sim_tech_ret=?, sim_tech_status='CLOSED_LOSS',
            sim_breadth_ret=?, sim_breadth_status='CLOSED_LOSS',
            entry_breadth=?,
            live_a_ret=?, live_a_status='CLOSED_LOSS',
            cand_b_ret=?, cand_b_status='CLOSED_LOSS',
            champ_c_ret=?, champ_c_status='CLOSED_LOSS',
            flow_tags=?
        WHERE id=?
    """
    params = (
        exit_d,
        exit_rsn,
        ret,
        0.0,
        new_max,
        new_min,
        new_bars,
        new_up,
        new_down,
        exit_type,
        neg,
        neg,
        neg,
        eb,
        neg,
        neg,
        neg,
        flow_tags,
        int(r["id"]),
    )
    _execute_commit_retry(conn, update_sql, params, context=f"좀비청산 {r['symbol']}")

    treasury_key = "TREASURY_SPOT_USDT" if r["market_type"] == "spot" else "TREASURY_FUTURES_USDT"
    cur_cfg = load_system_config()
    before = float(cur_cfg.get(treasury_key, 0.0))
    margin_used = float(r.get("margin_used", 0.0) or 0.0)
    pnl = float(r.get("sim_kelly_invest", 0.0) or 0.0) * (ret / 100.0)
    cur_cfg[treasury_key] = max(0.0, before + margin_used + pnl)
    save_system_config(cur_cfg)

    send_telegram_msg(
        f"☠️ <b>[좀비 해제]</b> {str(r['market_type']).upper()} <code>{r['symbol']}</code> #{r['timeframe']}\n"
        f"▪ {exit_rsn} (진입 후 14일+ 데이터 단절)\n"
        f"▪ final_ret <b>{ret}%</b> · 국고 환입 반영 ({treasury_key})"
    )

def track_daily_positions(market_type):
    init_forward_db()
    conn = get_connection(DB_PATH)
    cfg = load_system_config()
    df_active = pd.read_sql(
        "SELECT * FROM bitget_forward_trades WHERE market_type=? AND status='OPEN'",
        conn,
        params=(str(market_type).lower(),),
    )
    if df_active.empty:
        print(f"\n🔍 [포워드 테스팅] {market_type} OPEN 0건 — 글로브 손실/서킷만 점검")
        try:
            _finalize_global_circuit_breaker_track(conn, cfg)
        finally:
            conn.close()
        return

    breadth_now = _calc_market_breadth(conn)
    # 주식 auto_forward_tester: breadth < 0.97 → MAE 손절·타임스탑 0.5배 비상 조임
    breadth_collapse_tightening = breadth_now < 0.97
    if breadth_collapse_tightening:
        print(
            f"🛡️ [포워드 Bitget] 시장 폭 붕괴 연동 (breadth={breadth_now:.3f} < 0.97): "
            f"기보유 MAE 손절선·타임스탑 0.5배 타이트닝"
        )

    print(f"\n🔍 [포워드 테스팅] {market_type} {len(df_active)}개 포지션 추적 중...")

    for _, r in df_active.iterrows():
        try:
            days_in_pos = _days_since_entry_date(r.get("entry_date"))
            latest = _get_latest_bar(conn, r["market_type"], r["symbol"], r["timeframe"])
            hist_df = latest.get("hist_df") if latest is not None else None
            data_insufficient = latest is None or hist_df is None or len(hist_df) < 20
            if data_insufficient:
                if days_in_pos is not None and days_in_pos >= 14:
                    _force_close_zombie_delist_or_halt(conn, r)
                continue

            c = latest["close"]
            o = latest["open"]
            h = latest["high"]
            l = latest["low"]
            v = latest["vol"]
            ep = float(r["entry_price"])
            if ep <= 0:
                continue
            pos_side = str(r.get("position_side", "LONG")).upper()
            if pos_side == "SHORT":
                current_ret_pct = ((ep - c) / ep) * 100.0
                low_ret_pct = ((ep - h) / ep) * 100.0   # SHORT: high = max loss
                high_ret_pct = ((ep - l) / ep) * 100.0  # SHORT: low = max profit
            else:
                current_ret_pct = ((c - ep) / ep) * 100.0
                low_ret_pct = ((l - ep) / ep) * 100.0
                high_ret_pct = ((h - ep) / ep) * 100.0

            new_max = max(float(r["max_high"]), h)
            new_min = min(float(r["min_low"]), l)
            new_bars = int(r["bars_held"]) + 1
            new_up_vol = float(r["up_vol_sum"]) + (v if c > o else 0.0)
            new_down_vol = float(r["down_vol_sum"]) + (v if c < o else 0.0)

            is_futures_row = str(r["market_type"]).lower() == "futures"
            snap = _cached_funding_snapshot(str(r["symbol"])) if is_futures_row else None
            notion_open = float(r.get("sim_kelly_invest", 0.0) or 0.0)
            if notion_open <= 0:
                mu_o = float(r.get("margin_used", 0.0) or 0.0)
                lev_o = float(r.get("leverage", 1.0) or 1.0)
                if mu_o > 0 and is_futures_row and lev_o > 0:
                    notion_open = mu_o * lev_o
                else:
                    notion_open = mu_o
            period_h_fund = float(cfg.get("FUNDING_INTERVAL_HOURS_DEFAULT", 8.0))
            tf_hours_fund_map = {"1H": 1.0, "2H": 2.0, "4H": 4.0, "1D": 24.0}
            tf_hours_fund = float(tf_hours_fund_map.get(str(r["timeframe"]).upper(), 4.0))
            rate_used = float(snap.get("funding_rate") or 0.0) if snap else float(r.get("funding_rate_last", 0) or 0)
            prev_accum = float(r.get("funding_accum_usdt_est", 0) or 0)
            accum_fund = prev_accum
            fts_store = str(r.get("funding_next_settle_ts", "") or "")
            if snap:
                fts_store = str(snap.get("next_funding_iso") or snap.get("next_funding_ts") or "").strip()
                rate_used = float(snap.get("funding_rate") or 0.0)
            fr_row = rate_used
            if is_futures_row and period_h_fund > 1e-9 and notion_open > 0:
                if pos_side == "LONG":
                    accum_fund = prev_accum - notion_open * rate_used * (tf_hours_fund / period_h_fund)
                else:
                    accum_fund = prev_accum + notion_open * rate_used * (tf_hours_fund / period_h_fund)

            hist = hist_df
            cur_atr = _calc_atr14(hist)
            # 주식 forward_trades와 동형: 백필 시 entry_atr을 DB에 박제 — 캔들마다 cur_atr로 손절선이 밀리는 고무줄 버그 방지
            entry_atr = r.get("entry_atr", 0.0)
            if entry_atr == 0.0 or pd.isna(entry_atr):
                entry_atr = float(cur_atr)
                _execute_commit_retry(
                    conn,
                    "UPDATE bitget_forward_trades SET entry_atr=? WHERE id=?",
                    (round(entry_atr, 6), int(r["id"])),
                    context=f"entry_atr백필 {r['symbol']}",
                )
            else:
                entry_atr = float(entry_atr)

            hist["ema10"] = hist["Close"].ewm(span=10, adjust=False).mean()
            hist["ema20"] = hist["Close"].ewm(span=20, adjust=False).mean()
            hist["ema34"] = hist["Close"].ewm(span=34, adjust=False).mean()
            hist["ema60"] = hist["Close"].ewm(span=60, adjust=False).mean()
            hist["ema75"] = hist["Close"].ewm(span=75, adjust=False).mean()
            hist["ema160"] = hist["Close"].ewm(span=160, adjust=False).mean()
            z_ema1 = hist["Close"].ewm(span=20, adjust=False).mean()
            z_ema2 = z_ema1.ewm(span=20, adjust=False).mean()
            cur_zlema = float((z_ema1 + (z_ema1 - z_ema2)).iloc[-1])
            ema10_now = float(hist["ema10"].iloc[-1])
            ema20_now = float(hist["ema20"].iloc[-1])
            ema10_prev = float(hist["ema10"].iloc[-2])
            ema20_prev = float(hist["ema20"].iloc[-2])
            is_tech_exit_long = (c < cur_zlema) or (ema10_now < ema20_now and ema10_prev >= ema20_prev)
            is_tech_exit_short = (c > cur_zlema) or (ema10_now > ema20_now and ema10_prev <= ema20_prev)
            is_tech_exit = is_tech_exit_short if pos_side == "SHORT" else is_tech_exit_long

            entry_breadth = float(r.get("entry_breadth", 1.0) or 1.0)
            breadth_delta = breadth_now - entry_breadth
            breadth_collapse = breadth_now < 0.95 and breadth_delta < -0.03

            ns_prefix = f"{str(r['timeframe']).upper()}_LIVE_PARAMS"
            live_params = cfg.get(ns_prefix, {"DYNAMIC_MAE_SL": -3.5, "DYNAMIC_MFE_TP": 10.0})
            dyn_mae_sl = float(live_params.get("DYNAMIC_MAE_SL", -3.5))
            dyn_mfe_tp = float(live_params.get("DYNAMIC_MFE_TP", 10.0))
            opt_time_stop = int(cfg.get(f"{str(r['timeframe']).upper()}_TIME_STOP", 10))
            # 주식 패턴: 시장폭 붕괴(<0.97) 또는 포지션 단위 폭 급변 → MAE·타임스탑 절반
            if breadth_collapse_tightening or breadth_collapse:
                dyn_mae_sl *= 0.5
                opt_time_stop = max(1, int(round(float(opt_time_stop) * 0.5)))
            opt_sl_atr = float(r["atr_sl_mult"] or cfg.get("ATR_SL_MULT", 2.0))
            if pos_side == "SHORT":
                sl_price = ep + (opt_sl_atr * entry_atr)
            else:
                sl_price = ep - (opt_sl_atr * entry_atr)

            do_exit = False
            exit_rsn = ""
            actual_exit_type = "HOLD"
            actual_exit_price = c

            # MFE/MAE 1순위
            if low_ret_pct <= dyn_mae_sl:
                do_exit = True
                exit_rsn = f"수학적 MAE 장중 이탈 칼손절 ({dyn_mae_sl:.2f}%)"
                actual_exit_type = "STAT_MAE"
                actual_exit_price = ep * (1.0 + dyn_mae_sl / 100.0)
            elif high_ret_pct >= dyn_mfe_tp:
                if c < l + (h - l) * 0.7:
                    do_exit = True
                    exit_rsn = f"수학적 MFE 장중 도달 익절 ({dyn_mfe_tp:.2f}%)"
                    actual_exit_type = "STAT_MFE"
                    actual_exit_price = ep * (1.0 + dyn_mfe_tp / 100.0)

            # ATR/TimeStop/TECH 2순위
            if not do_exit:
                tf_u = str(r["timeframe"]).upper()
                funding_stop_bars_map = {"1H": 24, "2H": 18, "4H": 12, "1D": 5}
                funding_stop_bars = int(funding_stop_bars_map.get(tf_u, 12))
                is_futures = is_futures_row
                bleed_max_profit = float(cfg.get("FUNDING_BLEED_MAX_ROE_PCT", 1.5))
                th_fb = float(cfg.get("FUNDING_BLEED_RATE_THRESHOLD", 0.0003))
                funding_bleed = False
                if is_futures and snap is not None and new_bars >= funding_stop_bars and current_ret_pct < bleed_max_profit:
                    rate_api = float(snap.get("funding_rate") or 0.0)
                    if pos_side == "LONG" and rate_api > th_fb:
                        funding_bleed = True
                    elif pos_side == "SHORT" and rate_api < -th_fb:
                        funding_bleed = True
                if funding_bleed:
                    do_exit = True
                    exit_rsn = (
                        f"펀딩비(API) 불리 출혈 방어 rate={float(snap.get('funding_rate') or 0):.6f} "
                        f"next={str(snap.get('next_funding_iso') or snap.get('next_funding_ts') or '').strip()} "
                        f"({tf_u} ≥{funding_stop_bars} bars)"
                    )
                    actual_exit_type = "FUNDING_BLEED_STOP"
                elif new_bars >= opt_time_stop and current_ret_pct < 3.0:
                    do_exit = True
                    exit_rsn = f"타임스탑 ({opt_time_stop} bars)"
                    actual_exit_type = "TIME_STOP"
                elif breadth_collapse and current_ret_pct < 1.5:
                    do_exit = True
                    exit_rsn = f"시장폭 붕괴 청산 (entry {entry_breadth:.3f} -> now {breadth_now:.3f})"
                    actual_exit_type = "BREADTH_EXIT"
                elif (h >= sl_price if pos_side == "SHORT" else l <= sl_price):
                    do_exit = True
                    exit_rsn = f"ATR {opt_sl_atr:.2f}배 장중 방어 손절"
                    actual_exit_type = "ATR_STOP"
                    actual_exit_price = sl_price
                elif is_tech_exit:
                    do_exit = True
                    exit_rsn = "기술적 추세 이탈 (ZLEMA/EMA10-20 데드)"
                    actual_exit_type = "TECH_EXIT"

            # SHORT 전용 즉시청산/트레일링 익절 (Pine Script)
            if not do_exit and pos_side == "SHORT":
                ema34_now = float(hist["ema34"].iloc[-1])
                ema60_now = float(hist["ema60"].iloc[-1])
                ema75_now = float(hist["ema75"].iloc[-1])
                ema160_now = float(hist["ema160"].iloc[-1])
                ema34_prev = float(hist["ema34"].iloc[-2]) if len(hist) >= 2 else ema34_now
                ema60_prev = float(hist["ema60"].iloc[-2]) if len(hist) >= 2 else ema60_now
                ema75_prev = float(hist["ema75"].iloc[-2]) if len(hist) >= 2 else ema75_now
                ema160_prev = float(hist["ema160"].iloc[-2]) if len(hist) >= 2 else ema160_now

                cross_up_ema160 = (latest["prev_close"] <= ema160_prev) and (c > ema160_now)
                entry_high = float(r.get("entry_high", 0.0) or 0.0)
                break_entry_high = entry_high > 0 and c > entry_high

                if cross_up_ema160 or break_entry_high:
                    do_exit = True
                    actual_exit_type = "SHORT_PINE_STOP"
                    if cross_up_ema160 and break_entry_high:
                        actual_exit_price = max(ema160_now, entry_high)
                        exit_rsn = "숏 즉시손절: EMA160 상향돌파 + entry_high 돌파 (Pine)"
                    elif cross_up_ema160:
                        actual_exit_price = ema160_now
                        exit_rsn = "숏 즉시손절: EMA160 상향돌파 (Pine)"
                    else:
                        actual_exit_price = entry_high
                        exit_rsn = "숏 즉시손절: entry_high 돌파 (Pine)"
                else:
                    cross_up_ema34 = (latest["prev_close"] <= ema34_prev) and (c > ema34_now)
                    cross_up_ema60 = (latest["prev_close"] <= ema60_prev) and (c > ema60_now)
                    cross_up_ema75 = (latest["prev_close"] <= ema75_prev) and (c > ema75_now)
                    if cross_up_ema34:
                        do_exit = True
                        actual_exit_type = "SHORT_TRAIL_TP_34"
                        exit_rsn = "숏 트레일링 익절: EMA34 상향 돌파 (Pine)"
                    elif cross_up_ema60:
                        do_exit = True
                        actual_exit_type = "SHORT_TRAIL_TP_60"
                        exit_rsn = "숏 트레일링 익절: EMA60 상향 돌파 (Pine)"
                    elif cross_up_ema75:
                        do_exit = True
                        actual_exit_type = "SHORT_TRAIL_TP_75"
                        exit_rsn = "숏 트레일링 익절: EMA75 상향 돌파 (Pine)"

            # 🚨 [코인 생태계 특화] 레버리지 감안 강제 청산 (ROE -100% 이하 도달 시)
            leverage = float(r.get("leverage", 1.0) or 1.0)
            if leverage > 0 and (current_ret_pct * leverage) <= -100.0:
                do_exit, exit_rsn, actual_exit_type = True, f"레버리지({leverage}x) 한도 초과 강제청산", "LIQUIDATION"
                actual_exit_price = ep * (1.0 + (100.0 / leverage / 100.0)) if pos_side == "SHORT" else ep * (1.0 - (100.0 / leverage / 100.0))

            # 장기 좀비 청소
            if not do_exit and new_bars >= max(20, opt_time_stop * 2):
                do_exit = True
                exit_rsn = "장기 거래 정체 포지션 강제 청소"
                actual_exit_type = "ZOMBIE_FORCE_CLOSE"
                actual_exit_price = ep

            # 평행우주 기록
            sim_stat_ret = dyn_mae_sl if low_ret_pct <= dyn_mae_sl else (dyn_mfe_tp if high_ret_pct >= dyn_mfe_tp else current_ret_pct)
            sim_tech_ret = dyn_mae_sl if low_ret_pct <= dyn_mae_sl else (current_ret_pct if not is_tech_exit else current_ret_pct)
            sim_breadth_ret = current_ret_pct
            sim_stat_status = "CLOSED_LOSS" if low_ret_pct <= dyn_mae_sl else ("CLOSED_WIN" if high_ret_pct >= dyn_mfe_tp else "OPEN")
            sim_tech_status = "CLOSED_LOSS" if low_ret_pct <= dyn_mae_sl else ("CLOSED_WIN" if is_tech_exit else "OPEN")
            sim_breadth_status = "CLOSED_LOSS" if breadth_collapse and current_ret_pct < 0 else ("CLOSED_WIN" if breadth_now > 1.05 and current_ret_pct > 0 else "OPEN")

            live_a_ret = sim_tech_ret
            cand_b_ret = sim_stat_ret
            champ_c_ret = current_ret_pct
            live_a_status = sim_tech_status
            cand_b_status = sim_stat_status
            champ_c_status = "CLOSED_WIN" if do_exit and current_ret_pct > 0 else ("CLOSED_LOSS" if do_exit else "OPEN")

            if do_exit:
                if pos_side == "SHORT":
                    ret = round(((ep - actual_exit_price) / ep) * 100.0, 2)
                else:
                    ret = round(((actual_exit_price - ep) / ep) * 100.0, 2)
                if pos_side == "SHORT":
                    mfe = round(((ep - new_min) / ep) * 100.0, 2)
                else:
                    mfe = round(((new_max - ep) / ep) * 100.0, 2)
                tags = []
                if mfe >= 15.0 and new_bars <= 10:
                    tags.append("#빠른슈팅_완벽")
                elif mfe >= 8.0:
                    tags.append("#지연슈팅_수명연장")
                elif mfe < 3.0:
                    tags.append("#슈팅실패_조기소멸")
                vol_ratio = new_up_vol / (new_down_vol + 1.0)
                if vol_ratio >= 1.5:
                    tags.append("#건전한조정_매집우위")
                elif vol_ratio < 0.8:
                    tags.append("#음봉대량거래_세력이탈")

                # 🧟 [핵심 추가] 언더독(0~60점대) 전용 정밀 부검 꼬리표 부착
                if float(r.get("total_score", 100)) <= 60.0:
                    _rs = float(r.get("dyn_rs", 0) or r.get("v_rs", 0))
                    _eng = float(r.get("v_energy", 0) or 0)
                    _cpv = float(r.get("dyn_cpv", 0) or r.get("v_cpv", 0))

                    if ret > 0 or mfe >= 10.0:  # 수익으로 마감했거나 장중 10% 이상 대시세를 준 경우
                        if _rs < 0:
                            tags.append("#저득점_역배열_반등성공")
                        elif _rs > 30:
                            tags.append("#저득점_이격과다_추가폭발")

                        if _eng > 15.0:
                            tags.append("#저득점_수급깡패_성공")
                    else:  # 손실 마감 (참사주)
                        if _cpv > 0.75:
                            tags.append("#저득점_윗꼬리_참사")
                        elif vol_ratio < 0.6:
                            tags.append("#저득점_투매_수급붕괴")

                flow_tags = " ".join(tags)

                update_sql = """
                    UPDATE bitget_forward_trades
                    SET status=?, exit_date=?, exit_reason=?, final_ret=?, mfe=?, max_high=?, min_low=?, bars_held=?,
                        up_vol_sum=?, down_vol_sum=?, exit_type=?, sim_stat_ret=?, sim_stat_status=?, sim_tech_ret=?, sim_tech_status=?,
                        sim_breadth_ret=?, sim_breadth_status=?, entry_breadth=?, live_a_ret=?, live_a_status=?, cand_b_ret=?, cand_b_status=?, champ_c_ret=?, champ_c_status=?, flow_tags=?
                    WHERE id=?
                """
                update_params = (
                    "CLOSED_WIN" if ret > 0 else "CLOSED_LOSS",
                    datetime.utcnow().strftime("%Y-%m-%d"),
                    exit_rsn,
                    ret,
                    mfe,
                    new_max,
                    new_min,
                    new_bars,
                    new_up_vol,
                    new_down_vol,
                    actual_exit_type,
                    sim_stat_ret,
                    sim_stat_status,
                    sim_tech_ret,
                    sim_tech_status,
                    sim_breadth_ret,
                    sim_breadth_status,
                    float(entry_breadth),
                    live_a_ret,
                    live_a_status,
                    cand_b_ret,
                    cand_b_status,
                    champ_c_ret,
                    champ_c_status,
                    flow_tags,
                    int(r["id"]),
                )

                _execute_commit_retry(conn, update_sql, update_params, context=f"청산 {r['symbol']}")

                # 국고 환입
                treasury_key = "TREASURY_SPOT_USDT" if r["market_type"] == "spot" else "TREASURY_FUTURES_USDT"
                cur_cfg = load_system_config()
                before = float(cur_cfg.get(treasury_key, 0.0))
                margin_used = float(r.get("margin_used", 0.0) or 0.0)
                raw_pnl = float(r.get("sim_kelly_invest", 0.0) or 0.0) * (ret / 100.0)
                # 💡 [레버리지 강제청산 방어] 잃을 수 있는 최대 금액은 투입한 증거금(margin_used)으로 철저히 제한
                pnl = max(-margin_used, raw_pnl)
                cur_cfg[treasury_key] = max(0.0, before + margin_used + pnl)
                save_system_config(cur_cfg)

                icon = "🔥스마트청산" if ret > 0 else "🛡️방어손절"
                send_telegram_msg(
                    f"🤖 [{str(r['market_type']).upper()} 관제탑] {icon}: {r['symbol']} ({r['sig_type']} | {round(float(r['total_score']),1)}점)\n"
                    f"▪️ 수익: {ret}%\n▪️ 사유: {exit_rsn}\n▪️ 태그: {flow_tags}"
                )
            else:
                update_sql = """
                    UPDATE bitget_forward_trades
                    SET max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?,
                        sim_stat_ret=?, sim_stat_status=?, sim_tech_ret=?, sim_tech_status=?,
                        sim_breadth_ret=?, sim_breadth_status=?, entry_breadth=?,
                        live_a_ret=?, live_a_status=?, cand_b_ret=?, cand_b_status=?, champ_c_ret=?, champ_c_status=?,
                        funding_rate_last=?, funding_next_settle_ts=?, funding_accum_usdt_est=?
                    WHERE id=?
                """
                update_params = (
                    new_max,
                    new_min,
                    new_bars,
                    new_up_vol,
                    new_down_vol,
                    sim_stat_ret,
                    sim_stat_status,
                    sim_tech_ret,
                    sim_tech_status,
                    sim_breadth_ret,
                    sim_breadth_status,
                    float(entry_breadth),
                    live_a_ret,
                    live_a_status,
                    cand_b_ret,
                    cand_b_status,
                    champ_c_ret,
                    champ_c_status,
                    fr_row,
                    fts_store,
                    accum_fund,
                    int(r["id"]),
                )

                _execute_commit_retry(conn, update_sql, update_params, context=f"추적갱신 {r['symbol']}")
        except Exception as e:
            try:
                print(f"🚨 [청산 추적 에러] {r['symbol']}: {e}")
            except Exception:
                print(f"🚨 [청산 추적 에러] unknown_symbol: {e}")
            continue

    try:
        _finalize_global_circuit_breaker_track(conn, cfg)
    finally:
        conn.close()

