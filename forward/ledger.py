"""Forward ledger — track_daily_positions, virtual entries."""
import logging

from forward.shared import *  # noqa: F403
from reports.forward_report_scalar import (
    ohlcv_last_floats,
    prepare_forward_trades_df,
    row_scalar,
    safe_float_cast,
)
from network_timeout import fdr_data_reader, yf_download

logger = logging.getLogger(__name__)

# ── 스마트 서킷 브레이커(유체 방어) 상수 ────────────────────────────────────
#   트립: OPEN 미실현 손실/시드 ≤ −5%. 자율 해제: 손실 회복(>−2%) 또는 3거래일 쿨다운.
CB_TRIP_LOSS_RATIO = -0.05
CB_RELEASE_LOSS_RATIO = -0.02
CB_COOLDOWN_TRADING_DAYS = 3


def _update_global_circuit_breaker(market, loss_ratio, open_loss_amount, base_seed):
    """
    [유체 방어 #1] 전역 서킷 브레이커 트립 + 자율 해제(Sticky-ON 고착 방지).

    - OFF 상태: loss_ratio ≤ −5% 면 ON 트립(발동 시각·날짜·시장 기록).
    - ON  상태: (a) 트립 시장의 OPEN 손실이 −2% 이내로 회복 OR (b) 3거래일 쿨다운 경과 시 OFF.

    방어적: config(KV) 1회 load/save, 모든 단계 try/except. 메인 SQLite 원장 커넥션과
    무관(track 종료 후 호출)하며, 5-Factor 앙상블 상태와도 별도 저장소라 락 경합 없음.
    """
    from datetime import datetime
    import numpy as _np

    try:
        cfg = load_system_config()
    except Exception:
        return
    state = str(cfg.get("GLOBAL_CIRCUIT_BREAKER", "OFF")).upper()
    today = datetime.now().strftime("%Y-%m-%d")

    # ── OFF → 트립 평가 ──────────────────────────────────────────────────
    if state != "ON":
        if loss_ratio <= CB_TRIP_LOSS_RATIO:
            cfg["GLOBAL_CIRCUIT_BREAKER"] = "ON"
            cfg["GLOBAL_CIRCUIT_BREAKER_TRIGGER_DATE"] = today
            cfg["GLOBAL_CIRCUIT_BREAKER_TRIGGER_MARKET"] = str(market or "")
            cfg["GLOBAL_CIRCUIT_BREAKER_TRIGGERED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            cfg["GLOBAL_CIRCUIT_BREAKER_LAST_LOSS_RATIO"] = round(float(loss_ratio), 6)
            try:
                save_system_config(cfg)
            except Exception:
                return
            try:
                send_telegram_msg(
                    f"🚨 <b>[GLOBAL CIRCUIT BREAKER 발동]</b>\n"
                    f"시장: {market}\n"
                    f"당일 보유 손실 합계: {open_loss_amount:,.0f}\n"
                    f"기준 시드: {base_seed:,.0f}\n"
                    f"손실률: {loss_ratio*100:.2f}% (한계 ≤ −5.0%)\n"
                    f"조치: 신규 진입 전면 차단 · 회복(>−2%)·3거래일 쿨다운 시 자율 해제"
                )
            except Exception:
                pass
        return

    # ── ON → 자율 해제 평가 ──────────────────────────────────────────────
    trig_market = str(cfg.get("GLOBAL_CIRCUIT_BREAKER_TRIGGER_MARKET") or "")
    # 회복 경로: 트립 시장이 재확인할 때만(다른 시장의 깨끗한 장부로 오해제 방지). 미상이면 허용.
    can_confirm_recovery = (not trig_market) or (str(market or "") == trig_market)
    recovered = bool(can_confirm_recovery and float(loss_ratio) > CB_RELEASE_LOSS_RATIO)

    cooled = False
    trig_date = str(cfg.get("GLOBAL_CIRCUIT_BREAKER_TRIGGER_DATE") or "")
    if trig_date:
        try:
            td = int(_np.busday_count(trig_date, today))
            cooled = td >= CB_COOLDOWN_TRADING_DAYS
        except Exception:
            cooled = False

    if recovered or cooled:
        reason = "OPEN 손실 −2% 이내 회복" if recovered else f"{CB_COOLDOWN_TRADING_DAYS}거래일 쿨다운 경과"
        cfg["GLOBAL_CIRCUIT_BREAKER"] = "OFF"
        cfg["GLOBAL_CIRCUIT_BREAKER_RELEASED_AT"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        cfg["GLOBAL_CIRCUIT_BREAKER_RELEASE_REASON"] = reason
        try:
            save_system_config(cfg)
        except Exception:
            return
        try:
            send_telegram_msg(
                f"🟢 <b>[GLOBAL CIRCUIT BREAKER 자율 해제]</b>\n"
                f"평가 시장: {market}\n"
                f"사유: {reason}\n"
                f"현재 손실률: {loss_ratio*100:.2f}%\n"
                f"조치: 신규 진입 재개(유체 방어 — Sticky-ON 고착 방지)"
            )
        except Exception:
            pass


def _maybe_pyramid_add(conn, r, market, code, cur_price, sys_config, regime, edge_score):
    """
    [M4 자가 증식] 프리러너 엣지 폭발 시 유휴 NAV(미사용 자본)로 불타기 추가매수를
    가상 장부에 자동 편입. 순수 판정은 exit_dynamics.pyramid_decision 사용.
    """
    try:
        import exit_dynamics as _xd
        from live_nav_manager import live_nav
    except Exception:
        return False

    ep_now = safe_float_cast(cur_price, 0.0)
    if ep_now <= 0:
        return False

    # 유휴 현금 = Live NAV − 현재 시장 OPEN 노출액 합
    try:
        nav = float(live_nav(market))
    except Exception:
        nav = 0.0
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(sim_kelly_invest),0) FROM forward_trades WHERE market=? AND status='OPEN'",
            (market,),
        ).fetchone()
        open_exposure = float((row or (0.0,))[0] or 0.0)
    except Exception:
        open_exposure = 0.0
    idle_cash = max(0.0, nav - open_exposure)

    decision = _xd.pyramid_decision(
        edge_score=float(edge_score),
        regime=regime,
        idle_cash=idle_cash,
        nav=nav,
        pyramid_adds_done=int(row_scalar(r, 'pyramid_adds', 0.0)),
        free_runner=True,
    )
    if not decision.get("do"):
        return False

    add_notional = float(decision["add_notional"])
    parent_id = int(r.get('id') or 0)
    tz = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
    entry_date = datetime.now(tz).strftime('%Y-%m-%d')
    base_sig = str(r.get('sig_type') or '')
    pyr_sig = (base_sig + " [PYRAMID]").strip()
    risk_pct = row_scalar(r, 'sim_kelly_risk_pct', 0.02)
    entry_atr = row_scalar(r, 'entry_atr', 0.0)
    v_energy = row_scalar(r, 'v_energy', 0.0)

    try:
        conn.execute(
            """
            INSERT INTO forward_trades
                (entry_date, market, code, name, sector, sig_type, tier, total_score,
                 entry_price, v_energy, status, max_high, min_low, bars_held,
                 sim_kelly_invest, sim_kelly_risk_pct, entry_atr, entry_regime,
                 free_runner, parent_trade_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                entry_date, market, str(code), str(r.get('name') or code),
                str(r.get('sector') or '유망섹터'), pyr_sig, str(r.get('tier') or 'PYRAMID'),
                row_scalar(r, 'total_score', 0.0), ep_now, v_energy, 'OPEN',
                ep_now, ep_now, 0, add_notional, risk_pct, entry_atr, str(regime),
                0, parent_id,
            ),
        )
        conn.execute(
            "UPDATE forward_trades SET pyramid_adds=? WHERE id=?",
            (int(row_scalar(r, 'pyramid_adds', 0.0)) + 1, parent_id),
        )
        logger.info(
            "pyramid add: parent=%s code=%s notional=%.0f edge=%.2f regime=%s",
            parent_id, code, add_notional, edge_score, regime,
        )
        return True
    except Exception as ex:
        logger.warning("pyramid insert fail parent=%s: %s", parent_id, ex)
        return False


# ==========================================
# 2. 매일 종가 흐름 추적 및 청산 엔진 (DB 기반)
# ==========================================
def track_daily_positions(market):
    init_forward_db()
    # 💡 [V25.0] 긴 작업 시 다른 스레드가 대기할 수 있도록 60초 타임아웃 적용
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    
    # 현재 보유 중인 종목만 불러오기
    df_active = pd.read_sql("SELECT * FROM forward_trades WHERE market=? AND status='OPEN'", conn, params=(market,))
    df_active = prepare_forward_trades_df(df_active, context="track_daily_positions")
    if df_active.empty:
        conn.close()
        return

    print(f"\n🔍 [포워드 테스팅] {market} 시장 {len(df_active)}개 종목 추적 중...")
    sys_config = load_system_config()
    base_seed = sys_config.get("ACCOUNT_SIZE", 20000000)
    total_open_loss_amount = 0.0
    
    # 👇👇 [V102.3] 휴장·지연 방어 — KR/US Fluid Lookback Anchor 👇👇
    tz_mkt = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
    today_mkt_str = datetime.now(tz_mkt).strftime('%Y-%m-%d')
    fluid_anchor = None
    _mk = str(market).upper()

    try:
        from fluid_time_anchor import persist_anchor_state, resolve_market_with_db_fallback

        fluid_anchor = resolve_market_with_db_fallback(_mk, sys_config)
        persist_anchor_state(sys_config, fluid_anchor)
        try:
            save_system_config(sys_config)
        except Exception:
            pass
        if fluid_anchor.mode == 'halt':
            print(
                f"💤 [{_mk}] fluid halt ({fluid_anchor.reason}): "
                f"candle={fluid_anchor.latest_candle_date} cal={fluid_anchor.calendar_today} "
                f"lag={fluid_anchor.lag_business_days}bd"
            )
            conn.close()
            return
        if fluid_anchor.mode == 'carry_over':
            print(
                f"🌊 [{_mk}] fluid carry-over session={fluid_anchor.session_date} "
                f"(candle={fluid_anchor.latest_candle_date} cal={today_mkt_str})"
            )
    except Exception as _fa_ex:
        print(f"⚠️ [{_mk}] fluid anchor fallback: {_fa_ex}")
        fluid_anchor = None
    
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    idx_ticker = '069500' if market == 'KR' else 'SPY'
    
    try:
        idx_df = (
            fdr_data_reader(idx_ticker, start_date)
            if market == 'KR'
            else yf_download(idx_ticker, start=start_date, progress=False)
        )
        
        latest_candle_date = idx_df.index[-1].strftime('%Y-%m-%d')
        idx_close = idx_df['Close'] if market == 'KR' else idx_df['Close'].squeeze()
    except Exception as e: 
        print(f"⚠️ 벤치마크 로드 에러: {e}")
        idx_close = pd.Series(dtype=float)
    # 👆👆 [패치 완료] 👆👆

    cur_breadth_mkt = get_cached_market_breadth()
    breadth_collapse = cur_breadth_mkt < 0.97
    if breadth_collapse:
        print(
            f"🛡️ [포워드] 시장 폭 붕괴 연동 (breadth={cur_breadth_mkt:.3f} < 0.97): "
            f"기보유 청산 — MAE 손절·타임스탑 0.5배 비상 조임"
        )

    # [초월적 비대칭 청산] 국면 1회 로드(행별 재로딩 방지) + 동적 청산 수식 모듈
    try:
        _meta_regime = str(load_meta_state_resolved().get("META_REGIME_KEY") or "UNKNOWN").upper()
    except Exception:
        _meta_regime = "UNKNOWN"
    try:
        import exit_dynamics as _xdyn
        _ratchet_state = _xdyn.load_ratchet_state(sys_config)
    except Exception:
        _xdyn = None
        _ratchet_state = None

    for _, r in df_active.iterrows():
        try:
            code = r['code']
            ep = safe_float_cast(r.get('entry_price'), 0.0)
            if not np.isfinite(ep) or ep <= 0:
                logger.warning(
                    "track_daily_positions invalid entry_price market=%s id=%s code=%s raw=%r",
                    market,
                    r.get('id'),
                    code,
                    r.get('entry_price'),
                )
                continue
            _sig_raw = str(r.get('sig_type') or '')
            _is_observe_only = 'OBSERVE_ONLY' in _sig_raw

            if market == 'US':
                import time, random
                time.sleep(random.uniform(0.3, 0.7)) # 무호흡 연사로 인한 IP 차단 완벽 방어
            try:
                df = (
                    fdr_data_reader(code, start_date)
                    if market == 'KR'
                    else yf_download(code, start=start_date, progress=False)
                )
            except TimeoutError as te:
                logger.warning(
                    "track_daily_positions OHLCV timeout market=%s id=%s code=%s: %s",
                    market,
                    r.get('id'),
                    code,
                    te,
                )
                continue

            # 💡 [픽스 1] yfinance MultiIndex 에러 완벽 대응 (미국장 0승 0패 마비 해결)
            if market == 'US' and isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
                
            if df.empty or len(df) < 20: 
                # 💡 [픽스 2] 거래정지 좀비 종목 무한 누적 방지 (30일 경과 시 강제 사형)
                try:
                    entry_dt = datetime.strptime(r['entry_date'][:10], '%Y-%m-%d')
                    if (datetime.now() - entry_dt).days > 30:
                        conn.execute("UPDATE forward_trades SET status='CLOSED_LOSS', final_ret=-15.0, exit_reason='장기 거래정지/상폐 강제청산' WHERE id=?", (r['id'],))
                except: pass
                continue
                
            c, o, h, l, v = ohlcv_last_floats(df)
            if not all(np.isfinite(x) for x in (c, o, h, l, v)):
                logger.warning(
                    "track_daily_positions bad OHLCV market=%s id=%s code=%s",
                    market,
                    r.get('id'),
                    code,
                )
                continue
            
            # 장중 수익률 3총사: 이후 모든 판독 로직보다 먼저 계산해 NameError 방지
            current_ret_pct = ((c - ep) / ep) * 100        # 종가 기준 수익률
            low_ret_pct = ((l - ep) / ep) * 100            # 장중 최저 수익률 (손절 터치 감시용)
            high_ret_pct = ((h - ep) / ep) * 100           # 장중 최고 수익률 (익절 터치 감시용)

            # 계좌 통합 서킷 브레이커용 당일 보유 손실(실투자 OPEN만 — 관측 행 제외)
            if not _is_observe_only:
                position_notional = row_scalar(r, 'sim_kelly_invest', 0.0)
                if position_notional <= 0:
                    fallback_notional = row_scalar(r, 'invest_amount', 0.0)
                    position_notional = (
                        fallback_notional if fallback_notional > 0 else ep
                    )
                position_pnl = position_notional * (current_ret_pct / 100.0)
                if position_pnl < 0:
                    total_open_loss_amount += position_pnl

            new_max = max(row_scalar(r, 'max_high', ep), h)
            new_min = min(row_scalar(r, 'min_low', ep), l)
            _inc_bars = True
            if fluid_anchor is not None and not fluid_anchor.should_increment_bars(sys_config):
                _inc_bars = False
            new_bars = int(row_scalar(r, 'bars_held', 0.0)) + (1 if _inc_bars else 0)
            new_up_vol = row_scalar(r, 'up_vol_sum', 0.0) + (v if c > o else 0)
            new_down_vol = row_scalar(r, 'down_vol_sum', 0.0) + (v if c < o else 0)

            # =================================================================
            # 👑 [3차원 청산 최적화 엔진 가동] MFE/MAE, ATR, Time Stop 연산
            # =================================================================
            # 1. 14일 ATR(변동성) 실시간 연산
            df['prev_c'] = df['Close'].shift(1)
            df['tr'] = np.maximum(df['High'] - df['Low'], np.maximum(abs(df['High'] - df['prev_c']), abs(df['Low'] - df['prev_c'])))
            df['atr'] = df['tr'].ewm(span=14, adjust=False).mean()
            cur_atr = safe_float_cast(df['atr'].iloc[-1], 0.0)
            
            # 진입 시점의 ATR이 DB에 없다면 현재 ATR로 팩트 보정 후 저장
            entry_atr = safe_float_cast(r.get('entry_atr'), 0.0)
            if entry_atr == 0.0 or pd.isna(entry_atr):
                entry_atr = cur_atr
                conn.execute("UPDATE forward_trades SET entry_atr=? WHERE id=?", (entry_atr, r['id']))

            # 2. 기술적(TECH) 지표 연산 (기존 ZLEMA 및 단기데드)
            df['ema10'] = df['Close'].ewm(span=10, adjust=False).mean()
            df['ema20'] = df['Close'].ewm(span=20, adjust=False).mean()
            z_ema1 = df['Close'].ewm(span=20, adjust=False).mean()
            z_ema2 = z_ema1.ewm(span=20, adjust=False).mean()
            cur_zlema = safe_float_cast((z_ema1 + (z_ema1 - z_ema2)).iloc[-1], 0.0)
            
            is_tech_exit = (c < cur_zlema) or (
                safe_float_cast(df['ema10'].iloc[-1], 0.0) < safe_float_cast(df['ema20'].iloc[-1], 0.0)
                and safe_float_cast(df['ema10'].iloc[-2], 0.0) >= safe_float_cast(df['ema20'].iloc[-2], 0.0)
            )

            # 3. 🎯 관제탑 네임스페이스 매핑 및 JSON 지시사항 수신
            sys_config = load_system_config()
            active_mode = sys_config.get("ACTIVE_EXIT_MODE", "HYBRID")
            
            # 👇👇 [수정] 초신성(SUPERNOVA) 전용 독립 네임스페이스 분기 추가 👇👇
            ns_prefix = f"{market}_MASTER_S1" # 기본값
            
            if "SUPERNOVA" in _sig_raw:
                # 초신성은 오리지널과 완전히 분리된 전용 파라미터 방을 사용합니다.
                ns_prefix = f"{market}_SUPERNOVA_MASTER"
            elif "KR_BOWL" in _sig_raw or _is_observe_only:
                # 밥그릇 관측·표본: 롱 마스터 S1 청산 규칙(가상 청산만, 실주문 없음)
                ns_prefix = f"{market}_MASTER_S1"
            else:
                # 기존 오리지널 로직 분류 유지
                if "S4" in _sig_raw:
                    ns_prefix = f"{market}_MASTER_S4"
                if "눌림" in _sig_raw:
                    ns_prefix = f"{market}_NULRIM_S4" if "S4" in _sig_raw else f"{market}_NULRIM_S1"
                if "5선" in _sig_raw:
                    ns_prefix = f"{market}_5EMA_S1"
            # 👆👆 [수정 끝] 👆👆
            
            opt_time_stop = sys_config.get(f"{ns_prefix}_TIME_STOP", 10)
            opt_sl_atr    = sys_config.get(f"{ns_prefix}_ATR_SL", 2.0)
            if breadth_collapse:
                opt_time_stop = max(1, int(round(safe_float_cast(opt_time_stop, 10.0) * 0.5)))
            opt_sl_atr = safe_float_cast(opt_sl_atr, 2.0)
            
            # 수학적 손절가(SL) 산출: 진입가 - (관제탑 승수 * 진입변동성)
            sl_price = ep - (opt_sl_atr * entry_atr)

            # 4. ⚔️ 청산 아레나: MFE/MAE 및 관제탑 모드에 따른 수학적 사형 집행
            do_exit, exit_rsn, actual_exit_type = False, "", "HOLD"
            
            # 💡 [V51.0 핵심] 내 전략(Namespace) 방에 할당된 독립 파라미터 뇌(Brain) 꺼내오기
            ns_live_params = sys_config.get(f"{ns_prefix}_LIVE_PARAMS", sys_config)
            
            # 💡 [보강] 종목의 출신 성분(STANDARD vs SUPERNOVA)에 맞는 파라미터 팩 로드
            is_sn = "[SUPERNOVA]" in r['sig_type']
            prefix = ns_prefix # 기본값 (KR_MASTER_S1 등)

            abc_sets = {
                'live_a': ns_live_params,
                'cand_b': sys_config.get(f"{prefix}_CANDIDATE_PARAMS", {}),
                'champ_c': sys_config.get(f"{prefix}_CHAMPION_PARAMS", {})
            }

            # 모든 평행우주(A, B, C)에 대해 장중 저가(Low) 기준으로 손절 여부 판독
            for key, params in abc_sets.items():
                if not params: continue
                sl_limit = safe_float_cast(params.get("DYNAMIC_MAE_SL", -3.5), -3.5)
                if breadth_collapse:
                    sl_limit *= 0.5
                
                # 장중 저가가 손절선을 건드렸다면 해당 평행우주는 'CLOSED_LOSS'
                if low_ret_pct <= sl_limit:
                    conn.execute(f"UPDATE forward_trades SET {key}_ret=?, {key}_status=? WHERE id=?", (sl_limit, "CLOSED_LOSS", r['id']))
                else:
                    conn.execute(f"UPDATE forward_trades SET {key}_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # [V17.0 청산 평행우주 대결 (STAT vs TECH)]
            # 💡 [팩트] 관제탑이 내 전략방(ns_prefix) 맞춤형으로 깎아둔 실전 한계점 로드
            dyn_mae_sl = safe_float_cast(ns_live_params.get("DYNAMIC_MAE_SL", -3.5), -3.5)
            if breadth_collapse:
                dyn_mae_sl *= 0.5
            dyn_mfe_tp = safe_float_cast(ns_live_params.get("DYNAMIC_MFE_TP", 10.0), 10.0)
            od_hurdle = safe_float_cast(sys_config.get("DYNAMIC_OD_HURDLE", 20.0), 20.0)
            is_overdrive_on = row_scalar(r, 'v_energy', 0.0) >= od_hurdle
            if is_overdrive_on:
                dyn_mfe_tp *= 1.10

            _ace_evo = None
            try:
                from evolution.ace_exit_bridge import ace_exit_overrides

                _ace_evo = ace_exit_overrides(r, market, sys_config)
                if _ace_evo.active:
                    dyn_mfe_tp = safe_float_cast(dyn_mfe_tp, 10.0) + safe_float_cast(
                        _ace_evo.mfe_tp_relax_pct, 0.0
                    )
            except Exception:
                _ace_evo = None

            if r.get('sim_stat_status', 'OPEN') == 'OPEN':
                if low_ret_pct <= dyn_mae_sl: # 장중 손절 터치
                    conn.execute("UPDATE forward_trades SET sim_stat_ret=?, sim_stat_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                elif high_ret_pct >= dyn_mfe_tp: # 장중 익절 터치
                    if c >= l + (h - l) * 0.7:
                        conn.execute("UPDATE forward_trades SET sim_stat_ret=? WHERE id=?", (current_ret_pct, r['id']))
                    else:
                        conn.execute("UPDATE forward_trades SET sim_stat_ret=?, sim_stat_status='CLOSED_WIN' WHERE id=?", (dyn_mfe_tp, r['id']))
                else:
                    conn.execute("UPDATE forward_trades SET sim_stat_ret=? WHERE id=?", (current_ret_pct, r['id']))

            if r.get('sim_tech_status', 'OPEN') == 'OPEN':
                if low_ret_pct <= dyn_mae_sl:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=?, sim_tech_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                elif is_tech_exit:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=?, sim_tech_status='CLOSED_WIN' WHERE id=?", (current_ret_pct, r['id']))
                else:
                    conn.execute("UPDATE forward_trades SET sim_tech_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # [V24.0 시장 폭 필터링 실험 존]
            if r.get('sim_breadth_status', 'OPEN') == 'OPEN':
                e_breadth = r.get('entry_breadth', 1.0)
                if pd.isna(e_breadth): e_breadth = 1.0
                
                if e_breadth < 0.97:
                    conn.execute("UPDATE forward_trades SET sim_breadth_status='FILTERED_OUT' WHERE id=?", (r['id'],))
                else:
                    if low_ret_pct <= dyn_mae_sl:
                        conn.execute("UPDATE forward_trades SET sim_breadth_ret=?, sim_breadth_status='CLOSED_LOSS' WHERE id=?", (dyn_mae_sl, r['id']))
                    elif high_ret_pct >= dyn_mfe_tp:
                        if c >= l + (h - l) * 0.7:
                            conn.execute("UPDATE forward_trades SET sim_breadth_ret=? WHERE id=?", (current_ret_pct, r['id']))
                        else:
                            conn.execute("UPDATE forward_trades SET sim_breadth_ret=?, sim_breadth_status='CLOSED_WIN' WHERE id=?", (dyn_mfe_tp, r['id']))
                    else:
                        conn.execute("UPDATE forward_trades SET sim_breadth_ret=? WHERE id=?", (current_ret_pct, r['id']))

            # 1순위: MFE/MAE 절대 한계점 도달 시 무조건 청산 
            actual_exit_price = c # 기본 청산가는 종가로 세팅
            
            # 💡 [핵심 교정] 종가가 아닌 '저가(l)'와 '고가(h)'로 실전과 똑같이 슬리피지 청산
            _scaled_done = row_scalar(r, 'scaled_out_frac', 0.0)
            _is_free_runner = row_scalar(r, 'free_runner', 0.0) >= 1
            _realized_partial = row_scalar(r, 'realized_partial_ret', 0.0)
            if low_ret_pct <= dyn_mae_sl:
                do_exit, exit_rsn, actual_exit_type = True, f"수학적 MAE 장중 이탈 칼손절 ({dyn_mae_sl}%)", "STAT_MAE"
                actual_exit_price = ep * (1 + (dyn_mae_sl / 100.0)) # 손절선에서 털린 가격
            elif high_ret_pct >= dyn_mfe_tp and _scaled_done < 1e-6:
                # [M1] 고정 상한 캡 폐기 — 1차 목표가 도달 시 유동 비율(F_out)만 부분 실현,
                # 나머지는 캡 없는 '프리러너'로 전환하여 우측 꼬리를 끝까지 추적한다.
                _vol_pct = (cur_atr / ep * 100.0) if (ep > 0 and np.isfinite(cur_atr)) else 5.0
                _edge_pre = (current_ret_pct / max(1, int(new_bars))) * (row_scalar(r, 'v_energy', 1.0) / 10.0)
                if _xdyn is not None:
                    f_out = _xdyn.fluid_scale_out_fraction(_meta_regime, _vol_pct, _edge_pre)
                else:
                    f_out = 0.5
                if f_out >= 0.999:
                    # 완전 방어 국면 — 전량 실현(단, 종가가 아닌 TP 지정가 체결)
                    do_exit, exit_rsn, actual_exit_type = True, f"유동 전량익절 (방어국면 F_out={f_out:.0%})", "STAT_MFE_FULL"
                    actual_exit_price = ep * (1 + (dyn_mfe_tp / 100.0))
                else:
                    # 부분 실현분(F_out)은 TP 지정가에서 체결로 적립, 잔여는 러너로 계속 보유
                    _partial_locked = round(f_out * dyn_mfe_tp, 4)
                    conn.execute(
                        "UPDATE forward_trades SET scaled_out_frac=?, realized_partial_ret=?, free_runner=1, max_high=? WHERE id=?",
                        (round(f_out, 4), _partial_locked, new_max, r['id']),
                    )
                    _scaled_done = f_out
                    _is_free_runner = True
                    _realized_partial = _partial_locked
            
            # [M2] 프리러너 볼록 트레일링 래칫 — 부분익절 후 잔여 물량을 MaxHigh×(1-κ)로 끝까지 추적.
            if not do_exit and _is_free_runner and _xdyn is not None:
                _run_ret = ((new_max - ep) / ep) * 100.0
                _kappa = _xdyn.convex_ratchet_kappa(_run_ret, _ratchet_state)
                _trail_px = _xdyn.trail_stop_price(new_max, _kappa)
                if l <= _trail_px:
                    # [거래량 확증 게이트] 트레일 하향 돌파라도 '거래량 없는 페이크 하락'이면 청산 유예.
                    #   · DB 무접근 — 이미 로드된 v·up/down_vol_sum·new_bars 재사용(O(1), 추가 쿼리 0).
                    #   · 양봉/도지(c>=o)는 추세 이탈로 보지 않고 즉시 유예(홀드).
                    #   · 음봉(c<o)일 때만 ICR/RVOL 평가 → 하나라도 충족이면 확증 청산, 둘 다 미달이면 유예.
                    #   · 1순위 MAE/MFE·2순위 타임스탑·κ RL 엔진은 일절 불변(P1b 내부에서만 분기).
                    if c < o:
                        _icr = v / max(1.0, row_scalar(r, 'up_vol_sum', 1.0))
                        _avg_vol = (
                            row_scalar(r, 'up_vol_sum', 0.0) + row_scalar(r, 'down_vol_sum', 0.0)
                        ) / max(1, int(new_bars))
                        _rvol = v / max(1.0, _avg_vol)
                        _vol_confirmed = (_icr >= 0.25) or (_rvol >= 1.5)
                    else:
                        # 양봉·도지에서 트레일 터치 = 거래량 동반 투매로 보기 어려움 → 유예
                        _icr = _rvol = 0.0
                        _vol_confirmed = False

                    if _vol_confirmed:
                        do_exit, exit_rsn, actual_exit_type = (
                            True,
                            f"프리러너 볼록 트레일 청산 (κ={_kappa:.3f} · 고점 {_run_ret:.0f}% "
                            f"· ICR {_icr:.2f}/RVOL {_rvol:.2f})",
                            "RUNNER_TRAIL",
                        )
                        actual_exit_price = _trail_px
                    else:
                        # 거래량 없는 페이크 하락 — 청산 유예, 러너 보존(RL 표본은 자연 연장돼 자가학습).
                        try:
                            _prev_ft = r.get('flow_tags')
                            _prev_ft = (
                                '' if _prev_ft is None
                                or (isinstance(_prev_ft, float) and pd.isna(_prev_ft))
                                else str(_prev_ft)
                            )
                            _gtag = "#RUNNER_유예_거래량페이크"
                            if _gtag not in _prev_ft:
                                conn.execute(
                                    "UPDATE forward_trades SET flow_tags=? WHERE id=?",
                                    ((f"{_prev_ft} {_gtag}").strip(), r['id']),
                                )
                        except Exception:
                            pass
                        print(
                            f"⏸️ [RUNNER 유예] {code} 거래량 페이크 하락 "
                            f"(l={l:.2f}≤trail={_trail_px:.2f} · ICR {_icr:.2f}/RVOL {_rvol:.2f} 미달) — 러너 유지"
                        )

            # [M4] 엣지 폭발(수급강도×수익속도) 시 유휴 NAV 끌어와 불타기 자가 증식.
            if not do_exit and _is_free_runner and _xdyn is not None and not _is_observe_only:
                try:
                    _edge_now = (current_ret_pct / max(1, int(new_bars))) * (
                        row_scalar(r, 'v_energy', 1.0) / 10.0
                    )
                    _maybe_pyramid_add(conn, r, market, code, c, sys_config, _meta_regime, _edge_now)
                except Exception as _pyr_ex:
                    logger.warning("pyramid hook skip id=%s: %s", r.get('id'), _pyr_ex)

            # RL 프록시(Q-Value 근사): 2순위 타임스탑 직전에 홀딩 엣지가 높으면 opt_time_stop만 +2일 연장(1순위 MAE/MFE 불변)
            try:
                _ots = int(round(safe_float_cast(opt_time_stop, 10.0)))
            except (TypeError, ValueError):
                _ots = 10
            opt_time_stop_effective = max(1, _ots)
            holding_edge_score = (current_ret_pct / max(1, int(new_bars))) * (
                row_scalar(r, 'v_energy', 1.0) / 10.0
            )
            if holding_edge_score > 1.5:
                opt_time_stop_effective = opt_time_stop_effective + 2

            if _ace_evo is not None and _ace_evo.active:
                opt_time_stop_effective = max(
                    opt_time_stop_effective,
                    int(round(opt_time_stop_effective * safe_float_cast(_ace_evo.time_stop_mult, 1.0)))
                    + int(_ace_evo.min_hold_bars_extra),
                )

            # 2순위: 한계점 내부에서 움직일 경우, 국면 모드에 따른 추세/시간 청산
            if not do_exit:
                if active_mode == "TECH":
                    if is_tech_exit: 
                        do_exit, exit_rsn, actual_exit_type = True, "기술적 추세 이탈 (ZLEMA/데드)", "TECH"
                elif active_mode == "STAT":
                    if new_bars >= opt_time_stop_effective and current_ret_pct < 3.0:
                        do_exit, exit_rsn, actual_exit_type = True, f"통계적 유통기한 만료 ({opt_time_stop_effective}일)", "STAT_TIME"
                    elif l <= sl_price: # 💡 c <= sl_price 가 아니라 장중 저가 l 로 변경
                        do_exit, exit_rsn, actual_exit_type = True, f"ATR {opt_sl_atr}배 장중 방어 손절", "STAT_ATR"
                        actual_exit_price = sl_price
                else: # HYBRID
                    if new_bars >= opt_time_stop_effective and current_ret_pct < 3.0:
                        do_exit, exit_rsn, actual_exit_type = True, f"하이브리드 타임스탑 ({opt_time_stop_effective}일)", "HYBRID_TIME"
                    elif l <= sl_price: # 💡 c <= sl_price 가 아니라 장중 저가 l 로 변경
                        do_exit, exit_rsn, actual_exit_type = True, f"ATR {opt_sl_atr}배 장중 방어 손절", "HYBRID_ATR"
                        actual_exit_price = sl_price
                    elif is_tech_exit: 
                        do_exit, exit_rsn, actual_exit_type = True, "하이브리드 추세 이탈 익절", "HYBRID_TECH"

            # 3순위: 장기 거래정지/좀비 종목 강제 청소 (유통기한 2배 초과 시 원금 회수 가정)
            if not do_exit and new_bars >= opt_time_stop_effective * 2:
                do_exit, exit_rsn, actual_exit_type = True, "장기 거래정지/좀비종목 강제청소", "ZOMBIE_FORCE_CLOSE"
                actual_exit_price = ep


            # 5. DB 업데이트 실행 (청산 시)
            if do_exit:
                # 💡 [핵심] 최종 수익률(ret)은 희망회로 종가(c)가 아니라 '실제 증권사가 던진 가격(actual_exit_price)' 기반으로 계산
                ret = round(((actual_exit_price - ep) / ep) * 100, 2)
                # [M1] 부분익절된 포지션은 '부분 실현분 + 잔여 러너 실현분'을 비대칭 합산.
                if _scaled_done > 1e-6 and actual_exit_type != "STAT_MFE_FULL" and _xdyn is not None:
                    ret = round(_xdyn.blend_final_return(_realized_partial, _scaled_done, ret), 2)
                mfe = round(((new_max - ep) / ep) * 100, 2)
                
                tags = []
                if _ace_evo is not None and _ace_evo.active and _ace_evo.flow_tag:
                    tags.append(f"#{_ace_evo.flow_tag}")
                    tags.append("#에이스진화_보유연장")
                if mfe >= 7.0 and new_bars <= 8: tags.append("#빠른슈팅_완벽")
                elif mfe >= 7.0 and new_bars > 8: tags.append("#지연슈팅_수명연장")
                elif mfe < 3.0: tags.append("#슈팅실패_조기소멸")
                
                vol_ratio = new_up_vol / (new_down_vol + 1)
                if vol_ratio >= 1.5: tags.append("#건전한조정_매집우위")
                elif vol_ratio < 0.8: tags.append("#음봉대량거래_세력이탈")

                # 👇👇 [추가] 오리지널과 초신성의 흐름(Flow) 오토 추적 분리 👇👇
                if "SUPERNOVA" in r['sig_type']:
                    # 초신성 전용 광기/투매 추적 로직 (스케일이 다름)
                    if mfe >= 20.0: tags.append("#초신성_광기폭발_성공")
                    elif mfe >= 10.0: tags.append("#초신성_1차슈팅_완료")
                    elif mfe < 3.0: tags.append("#가짜초신성_수급불발")
                    
                    if vol_ratio >= 2.0: tags.append("#미친매수세_잔류")
                    elif vol_ratio < 0.6: tags.append("#세력_엑시트_투매출회")
                else:
                    # 기존 오리지널 로직 유지
                    if mfe >= 7.0 and new_bars <= 8: tags.append("#빠른슈팅_완벽")
                    elif mfe >= 7.0 and new_bars > 8: tags.append("#지연슈팅_수명연장")
                    elif mfe < 3.0: tags.append("#슈팅실패_조기소멸")
                    
                    if vol_ratio >= 1.5: tags.append("#건전한조정_매집우위")
                    elif vol_ratio < 0.8: tags.append("#음봉대량거래_세력이탈")
                # 👆👆 [추가 끝] 👆👆
                
                # 🧟 [핵심 추가] 언더독(0~60점대) 전용 정밀 부검 꼬리표 부착
                if row_scalar(r, 'total_score', 100.0) <= 60.0:
                    _rs = row_scalar(r, 'dyn_rs', 0.0) or row_scalar(r, 'v_rs', 0.0)
                    _eng = row_scalar(r, 'v_energy', 0.0)
                    _cpv = row_scalar(r, 'dyn_cpv', 0.0) or row_scalar(r, 'v_cpv', 0.0)

                    if ret > 0 or mfe >= 10.0: # 수익으로 마감했거나 장중 10% 이상 대시세를 준 경우
                        if _rs < 0: tags.append("#저득점_역배열_반등성공")
                        elif _rs > 30: tags.append("#저득점_이격과다_추가폭발")
                        
                        if _eng > 15.0: tags.append("#저득점_수급깡패_성공")
                    else: # 손실 마감 (참사주)
                        if _cpv > 0.75: tags.append("#저득점_윗꼬리_참사")
                        elif vol_ratio < 0.6: tags.append("#저득점_투매_수급붕괴")

                flow_tags = " ".join(tags)
                tz_exit = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
                exit_date = datetime.now(tz_exit).strftime('%Y-%m-%d')
                
                # 💡 관제탑이 피드백을 위해 수집할 exit_type 완벽 로깅
                conn.execute('''
                    UPDATE forward_trades 
                    SET status=?, exit_date=?, exit_reason=?, flow_tags=?, final_ret=?, mfe=?, max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?, exit_type=?
                    WHERE id=?
                ''', ('CLOSED_WIN' if ret > 0 else 'CLOSED_LOSS', exit_date, exit_rsn, flow_tags, ret, mfe, new_max, new_min, new_bars, new_up_vol, new_down_vol, actual_exit_type, r['id']))

                # [Live NAV 동기화] 청산 실현손익을 treasury_state.json NAV 에 즉시 복리 반영.
                # net_pnl = NAV × 그 거래 켈리(sim_kelly_risk_pct) × (ret/100). 실패해도 장부는 계속.
                try:
                    from live_nav_manager import record_closure
                    _kelly = row_scalar(r, 'sim_kelly_risk_pct', 0.0)
                    if _kelly and _kelly > 1.0:
                        _kelly = _kelly / 100.0
                    record_closure(
                        market,
                        final_ret_pct=float(ret),
                        kelly_pct=(_kelly if _kelly and _kelly > 0 else None),
                        exit_date=exit_date,
                    )
                except Exception:
                    pass

                # [초월적 진화 M3] 밴딧 베이지안 갱신 — 청산 1건마다 승/패로 Beta(α,β) 업데이트.
                # 승격 템플릿의 sig_type 일 때만 동작(아니면 무 I/O).
                try:
                    from template_bandit import update_bandit_for_closure
                    update_bandit_for_closure(row_scalar(r, 'sig_type', ''), won=(float(ret) > 0))
                except Exception:
                    pass

                icon = "🔥스마트청산" if ret > 0 else "🛡️방어손절"
                if _is_observe_only:
                    icon = "👁️관측청산" if ret > 0 else "👁️관측손절"
                # 💡 [V15.1 픽스] 시그널 타입(sig_type) 명시 및 점수 소수점 첫째 자리 정리
                send_telegram_msg(
                    f"🤖 [{market} 관제탑 제어] {icon}: {r['name']} "
                    f"({r['sig_type']} | {round(row_scalar(r, 'total_score', 0.0), 1)}점)\n"
                    f"▪️ 수익: {ret}%"
                    + (" · <i>가상장부(OBSERVE_ONLY)</i>" if _is_observe_only else "")
                    + f"\n▪️ 모드: {active_mode}\n▪️ 사유: {exit_rsn}\n▪️ 태그: {flow_tags}"
                )
            else:
                # DB 업데이트 (유지)
                conn.execute('''
                    UPDATE forward_trades 
                    SET max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?
                    WHERE id=?
                ''', (new_max, new_min, new_bars, new_up_vol, new_down_vol, r['id']))
                
        except Exception as e:
            logger.warning(
                "track_daily_positions skip market=%s id=%s code=%s: %s",
                market,
                r.get('id'),
                r.get('code'),
                e,
                exc_info=True,
            )
            continue

    conn.commit()
    if fluid_anchor is not None:
        try:
            from fluid_time_anchor import finalize_fluid_track_session

            finalize_fluid_track_session(sys_config, fluid_anchor)
        except Exception:
            fluid_anchor.mark_tracked(sys_config)
            try:
                save_system_config(sys_config)
            except Exception:
                pass
    conn.close()

    # 블랙스완 붕괴 감지 → 서킷 트립 / 회복·쿨다운 → 자율 해제 (스마트 서킷, 유체 방어 #1)
    if base_seed > 0:
        loss_ratio = total_open_loss_amount / safe_float_cast(base_seed, 20000000.0)
        _update_global_circuit_breaker(market, loss_ratio, total_open_loss_amount, base_seed)
