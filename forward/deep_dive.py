"""Forward reporting — deep dive, comprehensive daily, practitioner."""
from forward.shared import *  # noqa: F403
import forward.shared as _forward_shared
from forward_market_guard import enforce_market_frame

# import * skips leading-underscore names (PEP). Bind via module alias — survives star-import.
_DEEP_DIVE_PRIVATE_NAMES = (
    "_open_market_db_ro",
    "_normalize_trade_market",
    "_reporter_cleanup_zombie_forward_trades",
    "_reporter_valid_holding_mask",
    "_reporter_deploy_fleet_mask",
    "_daily_report_trades_for_market",
    "_strategy_colosseum_brief",
    "_shadow_performance_brief",
    "_tier80_sync_effective_and_report_line",
    "_parse_mkt_group_key",
    "_exit_date_on_calendar",
    "_format_exit_reason_display",
    "_safe_final_ret_pct",
    "_win_loss_flat_counts",
    "_spillover_fallback_enabled",
    "_format_forward_ledger_error_html",
)
for _priv in _DEEP_DIVE_PRIVATE_NAMES:
    globals()[_priv] = getattr(_forward_shared, _priv)


def _verify_deep_dive_private_bindings() -> None:
    missing = [n for n in _DEEP_DIVE_PRIVATE_NAMES if not callable(globals().get(n))]
    if missing:
        raise ImportError(
            f"forward.deep_dive: private reporter bindings missing: {missing}"
        )


_verify_deep_dive_private_bindings()


def send_comprehensive_daily_report(
    *,
    ctx=None,
    refresh_sentiment: bool = True,
    refresh_sector_spillover: bool = True,
    refresh_meta_governor: bool = True,
    apply_deathmatch_allocation: bool = True,
    cleanup_zombie_trades: bool = True,
):
    """[V104.1] 국가별 9분할 정밀 리포트 — DailyReportContext 시계 SSOT 필수."""
    from reports.daily_report_context import DailyReportContext
    from reports.forward_report_tier import filter_death_combo_df, filter_tier_80_df

    if ctx is None:
        ctx = DailyReportContext.build()
    elif not isinstance(ctx, DailyReportContext):
        raise TypeError("ctx must be DailyReportContext")
    if refresh_meta_governor:
        try:
            from meta_state_store import rebuild_meta_state

            _meta_heal = rebuild_meta_state(force=False, refresh_regime=True)
            print(f"🛰️ [일일 통합 리포트] MetaGovernor·REGIME 동기 치유: {_meta_heal}")
        except Exception as _mg_e:
            print(f"⚠️ [일일 통합 리포트] MetaGovernor 치유 실패(리포트는 계속): {_mg_e}")

    if refresh_sector_spillover:
        try:
            from sector_spillover_refresh import refresh_sector_spillover_state

            _sec_out = refresh_sector_spillover_state(save=True)
            print(f"🔄 [일일 통합 리포트] 섹터·스필오버 선행 갱신: {_sec_out}")
        except Exception as _sec_e:
            print(f"⚠️ [일일 통합 리포트] 섹터·스필오버 갱신 실패(리포트는 계속): {_sec_e}")

    if refresh_sentiment:
        try:
            from sentiment_miner import run_sentiment_mining

            _sent_out = run_sentiment_mining()
            print(f"🧠 [일일 통합 리포트] 센티먼트 선행 갱신: {_sent_out}")
        except Exception as _sent_e:
            print(f"⚠️ [일일 통합 리포트] 센티먼트 선행 갱신 실패(리포트는 계속): {_sent_e}")

    try:
        from doomsday_bridge import sync_doomsday_to_system_config

        _dd_sync = sync_doomsday_to_system_config(
            alert_on_escalation=False,
            run_inverse_cycle=True,
        )
        print(f"🛰️ [일일 통합 리포트] 둠스데이 브릿지: {_dd_sync}")
    except Exception as _ddb_e:
        print(f"⚠️ [일일 통합 리포트] 둠스데이 브릿지 스킵: {_ddb_e}")

    tz_kr = pytz.timezone('Asia/Seoul')
    today_str = ctx.calendar_today_kst
    _report_time_header = ctx.global_header_html()
    sys_config = load_system_config()

    if cleanup_zombie_trades:
        try:
            _nz = _reporter_cleanup_zombie_forward_trades()
            if _nz:
                print(f"🧹 [일일 통합 리포트] 좀비 OPEN 정리: {_nz}건")
        except Exception as _ez:
            print(f"⚠️ [일일 통합 리포트] 좀비 정리 스킵: {_ez}")

    from reports.report_collectors import (
        _df_long_only,
        build_market_report_opening,
    )
    from evolution_digest import build_global_evolution_digest_html
    from satellite_intel_brief import (
        build_satellite_intel_for_report,
        build_strategy_insight_html,
        collect_satellite_intel_metrics,
    )

    _sat_metrics = collect_satellite_intel_metrics(sys_config)
    smart_money_count = _sat_metrics["smart_money_count"]
    toxic_count = _sat_metrics["toxic_rule_count"]
    blackhole_count = _sat_metrics["blackhole_count"]

    sentiment_fresh_warn = False
    if refresh_sentiment:
        try:
            from news_data_paths import assert_sentiment_fresh_for_report, today_kst_str

            if not assert_sentiment_fresh_for_report():
                sentiment_fresh_warn = True
                print(
                    f"⚠️ [일일 통합 리포트] 당일({today_kst_str()}) 센티먼트 미확인 — "
                    "리포트에 데이터 없음/스냅샷 날짜로 표시"
                )
        except Exception as _sent_chk_e:
            sentiment_fresh_warn = True
            print(f"⚠️ [일일 통합 리포트] 센티먼트 검증 스킵: {_sent_chk_e}")

    satellite_brief_kr = build_satellite_intel_for_report(
        sys_config, market="KR", sentiment_fresh_warn=sentiment_fresh_warn
    )
    satellite_brief_us = build_satellite_intel_for_report(
        sys_config, market="US", sentiment_fresh_warn=sentiment_fresh_warn
    )
    strategy_insight = build_strategy_insight_html(sys_config)

    ranking_brief = ""
    try:
        ranking_brief = _strategy_colosseum_brief(colosseum_db_path_for_report())
    except Exception as _rank_e:
        print(f"⚠️ [일일 통합 리포트] 콜로세움 브리핑 스킵: {_rank_e}")

    shadow_brief = ""
    try:
        shadow_brief = _shadow_performance_brief(sys_config)
    except Exception as _sh_e:
        print(f"⚠️ [일일 통합 리포트] 그림자 장부 브리핑 스킵: {_sh_e}")

    def _assemble_satellite_tail() -> str:
        tail = strategy_insight
        if ranking_brief:
            tail += ranking_brief
        if shadow_brief:
            tail += shadow_brief
        tail += "--------------------------------------\n"
        return tail

    _satellite_tail = _assemble_satellite_tail()
    satellite_brief_kr += _satellite_tail
    satellite_brief_us += _satellite_tail

    base_seed = sys_config.get("ACCOUNT_SIZE", 20000000)
    try:
        meta_state_daily = load_meta_state_resolved()
    except Exception:
        meta_state_daily = {}

    for market in ['KR', 'US']:
        market_icon = "🇰🇷" if market == 'KR' else "🇺🇸"

        try:
            conn = _open_market_db_ro()

            # [사전 데이터 로드] — Timekeeper 윈도우 [rolling_cutoff, session_anchor]
            mkt_slice = ctx.load_market_slice(
                conn,
                market,
                df_long_only_fn=_df_long_only,
                normalize_market_fn=_daily_report_trades_for_market,
                valid_open_mask_fn=_reporter_valid_holding_mask,
            )
            df_real = mkt_slice.df_real
            df_closed = mkt_slice.df_closed
            df_open = mkt_slice.df_open
            _win_hdr = ctx.market_window_header_html(
                market,
                n_real=len(df_real),
                n_closed=mkt_slice.n_closed_window,
                n_open=mkt_slice.n_open_valid,
            )
            
            # ---------------------------------------------------------
            # 📑 결과지 1: 거시 국면 & 국고 현황 (ReportStateBinder)
            # ---------------------------------------------------------
            block_mt = build_macro_treasury_block(
                meta=meta_state_daily,
                sys_config=sys_config,
                df_closed_real=df_closed,
                treasury_config_key=f"CENTRAL_TREASURY_{market}",
                ledger_zero_invest_fallback=400000.0,
            )
            try:
                _unified_open = build_market_report_opening(market, sys_config, conn=conn)
            except Exception as _uo_ex:
                _unified_open = f"<i>⚠️ [0/0b] 통합 헤더 스킵: {_uo_ex}</i>\n"
            lead_in = ""
            if market == "KR":
                lead_in = (
                    _report_time_header
                    + _unified_open
                    + "━━━━━━━━━━━━━━━━━━━━\n"
                    + f"📢 <b>[일일 통합 성과 리포트]</b>\n"
                    + satellite_brief_kr
                )
            elif market == "US":
                lead_in = _unified_open + satellite_brief_us
            msg1 = format_macro_treasury_section_html(
                block_mt,
                display_label=market,
                market_icon=market_icon,
                today_str=today_str,
                lead_in_html=lead_in,
                currency_suffix="원",
                amount_decimals=0,
            )
            try:
                from mutant_pending_bridge import pending_rd_telegram_fragment

                msg1 += pending_rd_telegram_fragment(sys_config)
            except Exception:
                pass
            send_telegram_msg(msg1); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 2: 생존자 리더보드 (프로듀스 101)
            # ---------------------------------------------------------
            import re
            def get_core_group(sig):
                # 💡 모든 [태그]를 완벽히 제거하여 순수 로직명만 추출 (파편화 방지)
                clean_sig = re.sub(r'\[.*?\]', '', str(sig)).strip()
                return clean_sig if clean_sig else str(sig).replace('[', '').replace(']', '').strip()

            msg2 = f"{market_icon} <b>[2/9] 로직별 복리 생존 리더보드</b>\n"
            msg2 += _win_hdr
            if not df_real.empty:
                df_all_copy = df_real.copy()
                df_all_copy['group'] = df_all_copy['sig_type'].apply(get_core_group)
                leaderboard = []
                for group in df_all_copy['group'].unique():
                    g_df = df_all_copy[df_all_copy['group'] == group]
                    g_closed = g_df[g_df['status'].str.contains('CLOSED', na=False)]
                    # 💡 과거 에러 데이터(투입금 0원)를 기본 40만원(2%)으로 보정하여 복리 누락 방어
                    valid_invest = g_closed['sim_kelly_invest'].replace(0, 400000)
                    pnl = (valid_invest * g_closed['final_ret'] / 100.0).sum()
                    wr = (len(g_closed[g_closed['final_ret'] > 0]) / len(g_closed)) * 100 if len(g_closed) > 0 else 0
                    total_closed = len(g_closed)
                    pf = (
                        g_closed[g_closed['final_ret'] > 0]['final_ret'].sum()
                        / (abs(g_closed[g_closed['final_ret'] <= 0]['final_ret'].sum()) + 0.1)
                    ) if total_closed > 0 else 0
                    leaderboard.append({
                        'g': group,
                        'bal': base_seed + pnl,
                        'wr': wr,
                        'op': int(_reporter_valid_holding_mask(g_df).sum()),
                        'tot': total_closed,
                        'pf': pf,
                    })
                
                leaderboard = sorted(leaderboard, key=lambda x: x['bal'], reverse=True)
                for i, e in enumerate(leaderboard[:15]):
                    m = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "🏃"
                    if e['bal'] < base_seed * 0.8: m = "📉"
                    if e['bal'] < base_seed * 0.5: m = "💀"
                    msg2 += f"{m} <b>{e['g']}</b>: {e['bal']:,.0f}원\n"
                    msg2 += f"   ↳ 승률 {e['wr']:.0f}% (PF {e['pf']:.2f}) | 누적 {e['tot']}전 | 현재 {e['op']}종목 보유\n"
            else:
                _tk_m = ctx.timekeeper_for(market)
                msg2 += (
                    f" ↳ 매매 데이터 없음 (윈도우 {_tk_m.rolling_cutoff}~{_tk_m.session_anchor} · "
                    f"표본 0 · lag {ctx.lag_for(market)})\n"
                )
            send_telegram_msg(msg2); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 3: 통합 계좌 대결 (켈리 vs 고정) — CapitalDeathmatchAnalyzer
            # ---------------------------------------------------------
            dm_analyzer = CapitalDeathmatchAnalyzer(
                reference_capital=float(base_seed),
                zero_invest_fallback=400000.0,
            )
            dm_block = dm_analyzer.analyze(df_closed)
            msg3 = DeathmatchNarrativeBuilder.to_telegram_html(
                market_icon=market_icon,
                block=dm_block,
                subtitle="(정규직 로직 한정)",
            )
            send_telegram_msg(msg3); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 4: 포트폴리오 다중화 (VIP 편대 + 투입>0 기준으로 집계·한도 경고 통일)
            # ---------------------------------------------------------
            _unified = _reporter_deploy_fleet_mask(df_open, market)
            n_vip_fleet = int(_unified.sum())
            n_legacy_open = int(len(df_open) - n_vip_fleet)

            if not df_open.empty and "sim_kelly_invest" in df_open.columns:
                _sk_open = pd.to_numeric(df_open["sim_kelly_invest"], errors="coerce").fillna(0.0)
            else:
                _sk_open = pd.Series(0.0, index=df_open.index)
            _sig_open = df_open["sig_type"].astype(str)
            trend_mask = _sig_open.str.contains("🔥주도주", na=False) & _unified
            recon_mask = _sig_open.str.contains("🛡️차기섹터", na=False) & _unified
            trend_fleet = int(trend_mask.sum())
            recon_fleet = int(recon_mask.sum())
            trend_invest = scalar_float(_sk_open[trend_mask].sum())
            recon_invest = scalar_float(_sk_open[recon_mask].sum())
            total_invest = trend_invest + recon_invest
            if total_invest > 0:
                trend_weight = (trend_invest / total_invest) * 100.0
                recon_weight = (recon_invest / total_invest) * 100.0
            else:
                trend_weight = 0.0
                recon_weight = 0.0

            msg4 = f"{market_icon} <b>[4/9] 섹터 포트폴리오 다중화 현황</b>\n"
            if n_vip_fleet > 20:
                msg4 += (
                    "🚨 <b>[시스템 경고]</b> VIP 편대(주도/차기 트랙 + 투입금>0) 기준 보유가 시장 한도(20기)를 초과했습니다. "
                    "과거 레거시·표기 불일치 데이터를 점검하십시오.\n\n"
                )
            if n_legacy_open > 0:
                msg4 += (
                    f"📎 <b>포지션 팩트:</b> 현재 유효 VIP 편대 <b>{n_vip_fleet}기</b> "
                    f"(기타 레거시 OPEN <b>{n_legacy_open}기</b> 별도 보관 중 — 투입 집계·한도 경고는 VIP 편대만 반영)\n\n"
                )
            msg4 += f"🎯 <b>투입 자본 시너지 팩트 체크</b> <i>(VIP 트랙 + 투입금 양수)</i>\n"
            msg4 += (
                f" ▪️ 🔥주도주 편대: {trend_fleet}기 "
                f"(투입금: {trend_invest:,.0f}원 | 비중: {trend_weight:.1f}%)\n"
            )
            msg4 += (
                f" ▪️ 🛡️차기섹터 정찰: {recon_fleet}기 "
                f"(투입금: {recon_invest:,.0f}원 | 비중: {recon_weight:.1f}%)\n\n"
            )
            msg4 += "🗣️ <b>[관제탑 동적 시선]</b>\n"
            if total_invest == 0:
                msg4 += "현재 시장에 투입된 자본이 없습니다. 완벽한 현금 관망 상태입니다.\n"
            elif trend_weight >= 70.0:
                msg4 += (
                    f"전체 투자금의 {trend_weight:.1f}%가 주도 섹터에 강력하게 집중(Synergy)되어 있습니다. "
                    "추세 추종 극대화 모드입니다.\n"
                )
            elif recon_weight >= 70.0:
                msg4 += (
                    f"기존 주도주의 수명이 꺾였다고 판단, 자본의 {recon_weight:.1f}%를 "
                    "차기 섹터 발굴(정찰)에 선제적으로 투입 중입니다.\n"
                )
            else:
                msg4 += "주도 테마 추종과 차기 섹터 발굴에 자본을 균형 있게 배분하여 리스크를 헷징하고 있습니다.\n"
            send_telegram_msg(msg4); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 5: 티어 및 데스콤보 검증
            # ---------------------------------------------------------
            msg5 = f"{market_icon} <b>[5/9] 티어 및 데스콤보 검증</b>\n"
            msg5 += _win_hdr
            t1_df = filter_tier_80_df(df_closed)
            dc_df = filter_death_combo_df(df_closed, market=market)
            try:
                msg5 += _tier80_sync_effective_and_report_line(market, t1_df, sys_config)
            except Exception as _te:
                print(f"⚠️ [5/9] tier_effective 동기화/표시 예외: {_te}")
            if not dc_df.empty: msg5 += f"💀 데스콤보 승률: {(len(dc_df[dc_df['final_ret']>0])/len(dc_df))*100:.1f}% (필터 작동 중)\n"
            if t1_df.empty and dc_df.empty:
                if df_closed.empty:
                    msg5 += (
                        f" ↳ 검증 표본 부족 (윈도우 내 청산 0 · lag {ctx.lag_for(market)})\n"
                    )
                else:
                    msg5 += (
                        f" ↳ 80점대·데스콤보 0건 (청산 {len(df_closed)}건 윈도우 내)\n"
                    )
            send_telegram_msg(msg5); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 6: 4차원 DNA 정밀 부검 (DailyReportContext + 3단 Fallback)
            # ---------------------------------------------------------
            from forward.dna_autopsy import build_dna_autopsy_slice, format_dna_autopsy_section

            n_closed_mkt = int(len(df_closed))
            dna_slice = build_dna_autopsy_slice(
                ctx, market, df_closed, sys_config=sys_config
            )
            msg6 = f"{market_icon} <b>[6/9] 대박주/참사주 4차원 DNA 부검</b>\n"
            msg6 += _win_hdr
            msg6 += format_dna_autopsy_section(
                dna_slice,
                ctx=ctx,
                n_real=len(df_real),
                n_open=mkt_slice.n_open_valid,
                sys_config=sys_config,
                meta=meta_state_daily,
            )
            send_telegram_msg(msg6); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 7: 섹터 순환매 궤적 및 스필오버 (Timekeeper · Junk Hard Block)
            # ---------------------------------------------------------
            from forward.rotation_report_section import build_rotation_spillover_section

            msg7 = build_rotation_spillover_section(
                ctx,
                market,
                mkt_slice,
                sys_config=sys_config,
                market_icon=market_icon,
            )
            send_telegram_msg(msg7); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 8: 메타 최적화 및 반감기 (MetaGovernor 레지스트리 SSOT)
            # ---------------------------------------------------------
            try:
                lc_block = build_lifecycle_report_block(
                    meta=meta_state_daily,
                    sys_config=sys_config,
                    now=datetime.now(tz_kr),
                )
                msg8 = format_lifecycle_section_html(
                    lc_block,
                    market_icon=market_icon,
                    today_str=today_str,
                )
            except Exception as ex:
                msg8 = (
                    f"{market_icon} <b>[8/9] 메타 최적화 및 알파 반감기</b>\n"
                    f"⚠️ 생애주기 스냅샷 생성 실패: {html_escape(str(ex), quote=False)}\n"
                )
            send_telegram_msg(msg8); time.sleep(1)

            # ---------------------------------------------------------
            # 📑 결과지 9: 시스템 데스매치 결산 (DailyReportContext · Tier DM)
            # ---------------------------------------------------------
            from forward.deathmatch_report_section import build_deathmatch_section

            msg9 = build_deathmatch_section(
                ctx,
                market,
                df_closed,
                mkt_slice,
                sys_config=sys_config,
                meta=meta_state_daily,
                market_icon=market_icon,
                apply_deathmatch_allocation=apply_deathmatch_allocation,
            )
            send_telegram_msg(msg9); time.sleep(1)

            conn.close()
        except Exception as e:
            send_telegram_msg(
                _format_forward_ledger_error_html(f"{market} 일일 통합 리포트", e)
            )

    try:
        _delta_global = build_global_evolution_digest_html(meta_state_daily)
        if _delta_global:
            send_telegram_msg(_delta_global)
            time.sleep(1)
    except Exception as _delta_ex:
        send_telegram_msg(
            f"<i>⚠️ [Δ] 진화·튜닝 스킵: {html_escape(str(_delta_ex)[:72], quote=False)}</i>"
        )

def send_group_practitioner_reports(
    ctx=None,
    *,
    cleanup_zombie_trades: bool = True,
    markets: tuple[str, ...] | list[str] | None = None,
    **kwargs,
):
    """PIL — 활성 시그널 그룹별 실무자 리포트(Post-Mortem·Vitality·LLM) + 메타 페널티."""
    from practitioner_intelligence import (
        build_practitioner_brief,
        format_practitioner_brief_html,
        parse_group_from_sig,
    )
    from practitioner_penalty_bridge import apply_pil_vitality_penalties
    from reports.practitioner_report_context import (
        PractitionerReportContext,
        format_practitioner_fail_card,
    )

    if ctx is None:
        ctx = PractitionerReportContext.build()

    sys_config = load_system_config()
    base_seed = sys_config.get("ACCOUNT_SIZE", 20000000)
    pil_header = ctx.global_timekeeper_header_html()

    if cleanup_zombie_trades:
        try:
            _nz = _reporter_cleanup_zombie_forward_trades()
            if _nz:
                print(f"🧹 [실무자 리포트] 좀비 OPEN 정리: {_nz}건")
        except Exception as _ez:
            print(f"⚠️ [실무자 리포트] 좀비 정리 스킵: {_ez}")

    try:
        meta_state = load_meta_state_resolved()
    except Exception:
        meta_state = {}

    briefs = []
    n_fail = 0
    try:
        read_path = ctx.db_read_path
        uri = read_path.replace("\\", "/")
        conn = sqlite3.connect(f"file:{uri}?mode=ro", uri=True, timeout=60)
        df_all = pd.read_sql(
            "SELECT * FROM forward_trades WHERE IFNULL(sig_type, '') NOT LIKE '%INCUBATOR%'",
            conn,
        )
        conn.close()

        if df_all.empty:
            return

        df_all = df_all.copy()
        df_all["market"] = df_all.apply(
            lambda r: _normalize_trade_market(r.get("code"), r.get("market")),
            axis=1,
        )
        df_all["group"] = df_all["sig_type"].apply(parse_group_from_sig)
        df_all["mkt_group"] = df_all["market"].astype(str) + "_" + df_all["group"].astype(str)

        _exit_cal = (
            df_all["exit_date"].map(_exit_date_on_calendar)
            if "exit_date" in df_all.columns
            else pd.Series("", index=df_all.index)
        )
        df_all["_exit_cal"] = _exit_cal
        df_all["_pil_active"] = df_all.apply(
            lambda r: ctx.is_row_active(
                str(r.get("market", "")),
                r.get("status"),
                str(r.get("_exit_cal", "")),
            ),
            axis=1,
        )
        active_groups = sorted(
            g
            for g, sub in df_all.groupby("mkt_group", dropna=True)
            if str(g).strip() and sub["_pil_active"].any()
        )
        market_allow = None
        if markets:
            market_allow = {
                str(m).strip().upper()
                for m in markets
                if str(m).strip()
            }
            if not market_allow:
                market_allow = None
        # 호환성: 과거 호출부가 market="KR" 단일 인자를 넘겨도 동작
        single_market = str(kwargs.get("market", "")).strip().upper()
        if single_market:
            market_allow = (
                {single_market}
                if market_allow is None
                else (market_allow | {single_market})
            )
        print(
            f"📋 [PIL] 활성 그룹 {len(active_groups)}개 "
            f"(KR앵커 {ctx.tk_kr.session_anchor} · US앵커 {ctx.tk_us.session_anchor})"
        )

        for mkt_group in active_groups:
            market, group = _parse_mkt_group_key(mkt_group)
            if market_allow is not None and market not in market_allow:
                continue
            g_all = df_all[
                (df_all["mkt_group"] == mkt_group)
                & (df_all["market"].astype(str).str.upper() == market)
            ].copy()
            if market == "KR":
                g_all = g_all[g_all["code"].astype(str).str.match(r"^\d{5,6}$", na=False)]
            else:
                g_all = g_all[~g_all["code"].astype(str).str.match(r"^\d{5,6}$", na=False)]

            if g_all.empty:
                continue

            market_icon = "🇰🇷" if market == "KR" else "🇺🇸"
            sample_sig = str(g_all["sig_type"].iloc[0] if len(g_all) else group)
            session_anchor = ctx.session_anchor_str(market)

            try:
                g_closed = g_all[
                    g_all["status"].astype(str).str.contains("CLOSED", na=False)
                ].copy()
                if "exit_date" in g_closed.columns:
                    g_closed["_exit_day"] = g_closed["exit_date"].map(_exit_date_on_calendar)
                    g_today_closed = g_closed[
                        g_closed["_exit_day"] == session_anchor
                    ].copy()
                else:
                    g_today_closed = g_closed.iloc[0:0].copy()

                valid_open = _reporter_valid_holding_mask(g_all)
                stale_banner = ctx.staleness_banner_html(
                    market, live_row_count=len(g_today_closed)
                )
                brief = build_practitioner_brief(
                    market=market,
                    group_key=group,
                    sample_sig=sample_sig,
                    g_all=g_all,
                    g_closed=g_closed,
                    g_today_closed=g_today_closed,
                    sys_config=sys_config,
                    meta=meta_state,
                    base_seed=float(base_seed),
                    market_icon=market_icon,
                    mkt_today_str=session_anchor,
                    session_anchor=session_anchor,
                    timekeeper_header=pil_header,
                    staleness_banner=stale_banner,
                    valid_open_mask=valid_open,
                    format_exit_reason=_format_exit_reason_display,
                    safe_ret_fn=_safe_final_ret_pct,
                    win_loss_fn=_win_loss_flat_counts,
                )
                briefs.append(brief)
                send_telegram_msg(format_practitioner_brief_html(brief))
                time.sleep(3.5)
            except Exception as ex:
                n_fail += 1
                print(f"⚠️ [PIL] {mkt_group} 실패: {ex}")
                send_telegram_msg(
                    format_practitioner_fail_card(
                        market=market,
                        group_key=group,
                        sample_sig=sample_sig,
                        error=ex,
                        ctx=ctx,
                    )
                )
                time.sleep(1.0)

        if briefs:
            pen = apply_pil_vitality_penalties(briefs, sys_config)
            print(f"🛡️ [PIL] 메타 페널티: {pen}")
        if n_fail:
            print(f"⚠️ [PIL] 그룹 Fail-safe {n_fail}건 (다른 그룹은 정상 송출)")
    except Exception as e:
        send_telegram_msg(
            _format_forward_ledger_error_html("실무자 리포트 전역", e)
        )
# ==========================================
# 4. [방향성 5,6,7번] 퀀트 딥 다이브 분석 엔진 (특징 추출 및 티어별 성적표)
# ==========================================
def _deep_dive_cross_market_isolation_footer(df: pd.DataFrame, market: str) -> str:
    """
    tier 절대값으로 KR/US를 직접 비교하지 않도록 텔레그램 해석 가이드 + 동일 시장 내 total_score Z-구간 요약.
    """
    lines = [
        "\n◽ <b>[KR/US 격리 · tier 해석]</b>",
        "• 동일 표기의 <b>tier</b>(예: 40점대)도 <b>시장(market)별 산출 경로</b>가 다릅니다. "
        "<b>KR과 US를 tier 절대값으로 직접 비교하지 마십시오.</b>",
    ]
    ts = pd.to_numeric(df.get("total_score"), errors="coerce").dropna()
    if len(ts) >= 10:
        mu = scalar_float(ts.mean())
        sd = scalar_float(ts.std(ddof=0), 1e-9) or 1e-9
        z = (ts - mu) / sd
        lo = int((z < -0.5).sum())
        mid = int(((z >= -0.5) & (z <= 0.5)).sum())
        hi = int((z > 0.5).sum())
        lines.append(
            f"• <b>{market}장·본 윈도우 내부 total_score</b>: μ={mu:.1f} σ={sd:.1f} "
            f"→ Z≤-0.5: <b>{lo}</b>건 | -0.5~0.5: <b>{mid}</b>건 | Z&gt;0.5: <b>{hi}</b>건 "
            f"<i>(시장 간 비교 시 각 시장별로 동일 절차의 Z를 따로 산출한 뒤 해석)</i>"
        )
    return "\n".join(lines) + "\n"


def run_deep_dive_analysis(market='KR'):
    """
    미래 데이터(포워드 테스팅)를 기반으로 내 시스템의 과최적화를 검증하고,
    대박/참사 종목의 DNA와 티어별 진짜 승률을 텔레그램으로 보고합니다.
    """
    try:
        kr_tz = pytz.timezone("Asia/Seoul")
        now_kst = datetime.now(kr_tz)
        _cfg_dd = load_system_config()
        try:
            _rd = int(_cfg_dd.get("FORWARD_DEEP_DIVE_EXIT_WINDOW_DAYS", 90))
        except (TypeError, ValueError):
            _rd = 90
        rolling_days = _rd if _rd in (90, 180) else 90

        read_path = report_db_read_path()
        read_src = report_read_source_label(read_path)

        conn = _open_market_db_ro()
        try:
            wm = query_latest_closed_trade_date(conn, market)
            _rs: str = "MAIN" if read_src == "MAIN" else "SNAPSHOT"
            tk = ReportTimekeeper.for_market(
                market,
                rolling_days=rolling_days,
                ref_kst=now_kst,
                db_watermark_exit=wm,
                read_source=_rs,  # type: ignore[arg-type]
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM forward_trades WHERE market=? AND status LIKE 'CLOSED%'",
                (market,),
            )
            n_all_closed = int((cur.fetchone() or (0,))[0] or 0)
            df = pd.read_sql(
                """
                SELECT * FROM forward_trades
                WHERE market=? AND status LIKE 'CLOSED%'
                  AND substr(
                        IFNULL(
                            NULLIF(TRIM(CAST(exit_date AS TEXT)), ''),
                            NULLIF(TRIM(CAST(entry_date AS TEXT)), '')
                        ),
                        1,
                        10
                  ) >= ?
                  AND substr(
                        IFNULL(
                            NULLIF(TRIM(CAST(exit_date AS TEXT)), ''),
                            NULLIF(TRIM(CAST(entry_date AS TEXT)), '')
                        ),
                        1,
                        10
                  ) <= ?
                  AND IFNULL(sig_type,'') NOT LIKE '%INCUBATOR%'
                ORDER BY exit_date DESC
                """,
                conn,
                params=(market, tk.rolling_cutoff, tk.session_anchor),
            )
            df_live_raw, df_hist_raw, df_champion_raw, dual_meta = load_dual_track_frames(
                conn,
                market,
                timekeeper=tk,
            )
        finally:
            conn.close()

        today_str = tk.calendar_today_kst
        anchor_biz = tk.session_anchor
        cutoff_rolling = tk.rolling_cutoff
        cutoff_rot_60 = (now_kst.date() - timedelta(days=60)).strftime("%Y-%m-%d")
        staleness = evaluate_staleness(tk, live_row_count=dual_meta.live_row_count)
        persist_staleness_to_config(
            tk, staleness, save_fn=save_system_config, load_fn=load_system_config
        )

        n_rolling = len(df)
        if n_rolling < 10:
            skip_msg = (
                f"⚠️ <b>[{market}장 포워드 딥 다이브]</b>\n"
                f"표본 부족 (현재 <b>{n_rolling}</b>건 / 최소 10건)으로 딥다이브 생략 "
                f"(롤링 {rolling_days}일·전체 청산 {n_all_closed}건, KST {today_str})."
            )
            print(skip_msg.replace("<b>", "").replace("</b>", ""))
            send_telegram_msg(skip_msg)
            return

        df = prepare_forward_trades_df(df, context=f"deep_dive:{market}")
        df = enforce_market_frame(df, market, context=f"deep_dive:{market}")
        df_live = prepare_forward_trades_df(
            df_live_raw, context=f"deep_dive_live:{market}"
        )
        df_live = enforce_market_frame(df_live, market, context=f"deep_dive_live:{market}")
        df_hist = prepare_forward_trades_df(
            df_hist_raw, context=f"deep_dive_hist:{market}"
        )
        df_hist = enforce_market_frame(df_hist, market, context=f"deep_dive_hist:{market}")
        df_champion = prepare_forward_trades_df(
            df_champion_raw, context=f"deep_dive_champion:{market}"
        )
        df_champion = enforce_market_frame(
            df_champion, market, context=f"deep_dive_champion:{market}"
        )
        df["Win"] = np.where(df["final_ret"] > 0, 1, 0)
        m_roll = len(df)
        report_msg = (
            f"🔬 [{market}장 포워드 테스팅 딥 다이브 분석]\n"
            f"(최근 {rolling_days}일 청산 {m_roll}건 / 전체 {n_all_closed}건)\n"
            f"📎 <i>{tk.header_watermark_line()}</i>\n"
            f"🟢 LIVE <b>{dual_meta.live_row_count}</b> · "
            f"🏛️ HIST <b>{dual_meta.hist_row_count}</b> · "
            f"Staleness <b>{staleness.grade}</b>\n\n"
        )
        if staleness.banner_html:
            report_msg += staleness.banner_html + "\n\n"
        if staleness.fail_safe_html:
            report_msg += staleness.fail_safe_html

        try:
            meta_dd = load_meta_state_resolved()
        except Exception:
            meta_dd = {}

        rfa_dd = ReportFeatureAnalyzer(sys_config=_cfg_dd, meta=meta_dd)

        # ---------------------------------------------------------
        # 🌍 Universal DNA — 전역 1회 (버킷 루프 전)
        # ---------------------------------------------------------
        uni_block = build_universal_dna_block(df, analyzer=rfa_dd)
        report_msg += format_universal_dna_html(
            uni_block,
            market=market,
            rolling_days=rolling_days,
            today_str=today_str,
        )

        # ---------------------------------------------------------
        # 👑 점수대별 Micro-DNA — pd.cut 벡터 버킷 + ReportFeatureAnalyzer 동적 DNA
        # ---------------------------------------------------------
        dive = ForwardScoreBucketDeepDive(
            sys_config=_cfg_dd, meta=meta_dd, analyzer=rfa_dd
        )
        dual_blocks = build_dual_track_bucket_blocks(df_live, df_hist, dive)
        live_champion_blocks = dive.build_bucket_blocks(df_live)
        hist_champion_blocks = dive.build_bucket_blocks(df_hist)
        if staleness.allow_micro_dna:
            report_msg += format_dual_track_micro_dna_html(
                dual_blocks,
                staleness_banner="",
                anchor_day=anchor_biz,
                anchor_label=tk.anchor_label,
                meta_line=(
                    f"{market}장 · LIVE/HIST 분리 · 롤링 {rolling_days}일 "
                    f"({dual_meta.rolling_cutoff}~{anchor_biz})"
                ),
            )

        prep_df = ForwardScoreBucketDeepDive.assign_score_buckets(df)
        for bucket_label, t_df in prep_df.dropna(subset=["_score_bucket"]).groupby("_score_bucket", observed=True, sort=True):
            if len(t_df) < 5:
                continue
            try:
                t_int = int(str(bucket_label).replace("점대", "").strip())
            except ValueError:
                continue
            winners = t_df[
                pd.to_numeric(col_series(t_df, "final_ret"), errors="coerce") > 5.0
            ]
            if t_int <= 50 and len(winners) >= 3:
                _dna_cols = ("dyn_cpv", "dyn_tb", "v_energy", "dyn_rs")
                if all(c in winners.columns for c in _dna_cols):
                    ud_name = f"{market}_UNDERDOG_{t_int}점"
                    try:
                        deep_cfg = load_system_config()
                        inc_map = deep_cfg.get("INCUBATOR_TEMPLATES", {})
                        if not isinstance(inc_map, dict):
                            inc_map = {}
                        else:
                            inc_map = dict(inc_map)
                        inc_map[ud_name] = {
                            "cpv": round(series_mean(winners, "dyn_cpv"), 4),
                            "tb": round(series_mean(winners, "dyn_tb"), 4),
                            "bbe": round(series_mean(winners, "v_energy"), 4),
                            "rs": round(series_mean(winners, "dyn_rs"), 4),
                            "cos_cutoff": 0.75,
                            "created_at": datetime.now().strftime("%Y-%m-%d"),
                            "status": "INCUBATING",
                        }
                        deep_cfg["INCUBATOR_TEMPLATES"] = inc_map
                        save_system_config(deep_cfg)
                        tier_lbl = f"{t_int}점대"
                        report_msg += f"🧬 [자율 진화] {tier_lbl} 대박주 DNA가 인큐베이터({ud_name})에 신규 등재되었습니다.\n"
                    except Exception as _e:
                        report_msg += f"⚠️ 인큐베이터 DNA 주입 실패({ud_name}): {_e}\n"
        report_msg += "\n"

        if staleness.allow_tier_champion:
            report_msg += format_dual_track_tier_champion_summary_html(
                live_champion_blocks,
                hist_champion_blocks,
                market=market,
                rolling_days=rolling_days,
                session_anchor=anchor_biz,
                calendar_today_kst=today_str,
                db_watermark=tk.db_watermark_exit,
                anchor_label=tk.anchor_label,
                read_source=tk.read_source,
                staleness_grade=staleness.grade,
            )

        tag_snap = build_flow_tag_snapshot(
            df,
            timekeeper=tk,
            staleness=staleness,
            sys_config=_cfg_dd,
            persist_toxic=True,
            save_config_fn=save_system_config,
            load_config_fn=load_system_config,
        )
        report_msg += format_flow_tag_report_html(
            tag_snap,
            timekeeper=tk,
            staleness=staleness,
            rolling_days=rolling_days,
        )

        report_msg += _deep_dive_cross_market_isolation_footer(df, market)

        # ---------------------------------------------------------
        # 👑 엔진 7: [V28.0 한미 주도 섹터 스필오버(Spillover) 시차 분석]
        # ---------------------------------------------------------
        if market == "KR":
            try:
                from spillover_v28_report import build_v28_spillover_section

                report_msg += build_v28_spillover_section(
                    open_db_ro=_open_market_db_ro,
                    tk_kr=tk,
                    ref_kst=now_kst,
                    load_system_config=load_system_config,
                    save_system_config=save_system_config,
                    spillover_fallback_enabled=_spillover_fallback_enabled,
                )
            except Exception as e:
                report_msg += f"⚠️ 스필오버 분석 에러: {e}\n"

        # 👑 엔진 8: [V29.0 주도 섹터 순환매(Rotation) 수명 및 전이 추적]
        # ---------------------------------------------------------
        report_msg += f"\n🔄 <b>[V29.0 {market}장 주도 섹터 순환매 자금 추적]</b>\n"
        try:
            conn_rt = _open_market_db_ro()
            try:
                rot_df = pd.read_sql(
                    "SELECT entry_date, sector FROM forward_trades WHERE market=? AND entry_date >= ? ORDER BY entry_date ASC",
                    conn_rt,
                    params=(market, cutoff_rot_60),
                )
            finally:
                conn_rt.close()

            def map_standard_sector(s):
                s_str = str(s).lower()
                if any(k in s_str for k in ["반도체", "it", "ai", "소프트웨어", "모바일", "테크", "데이터"]): return "반도체/IT"
                if any(k in s_str for k in ["바이오", "헬스", "의료", "제약"]): return "바이오/헬스케어"
                if any(k in s_str for k in ["배터리", "2차전지", "화학", "에너지", "정유"]): return "에너지/화학"
                if any(k in s_str for k in ["금융", "은행", "증권", "지주", "투자"]): return "금융/지주"
                if any(k in s_str for k in ["기계", "조선", "방산", "산업재", "로봇", "전력"]): return "산업재/기계"
                if any(k in s_str for k in ["소비", "유통", "식품", "화장품", "엔터", "미디어"]): return "소비재/엔터"
                return "기타/혼합"

            rot_df['sector'] = rot_df['sector'].apply(map_standard_sector)

            if not rot_df.empty:
                # 일자별 대장 섹터 추출
                # 💡 [픽스] 가짜 섹터 배제
                def get_real_sector_deep(x):
                    valid_s = [str(s) for s in x if '유망' not in str(s) and '포착' not in str(s)]
                    return pd.Series(valid_s).mode()[0] if valid_s else None
                    
                daily_dom = rot_df.groupby('entry_date')['sector'].agg(get_real_sector_deep).dropna()
                
                streaks = {}      # 섹터별 머무는 기간(수명)
                transitions = {}  # A -> B 로의 자금 이동 횟수
                
                current_sec = None
                current_streak = 0
                
                # 순환매 체인(Markov Chain) 연산
                for date, sec in daily_dom.items():
                    if sec == current_sec:
                        current_streak += 1
                    else:
                        if current_sec is not None:
                            # 수명 기록
                            if current_sec not in streaks: streaks[current_sec] = []
                            streaks[current_sec].append(current_streak)
                            
                            # 자금 이동 궤적 기록 (A ➔ B)
                            trans_key = f"{current_sec[:15]} ➔ {sec[:15]}"
                            transitions[trans_key] = transitions.get(trans_key, 0) + 1
                        
                        current_sec = sec
                        current_streak = 1
                
                # 마지막 진행 중인 파동 기록
                if current_sec is not None:
                    if current_sec not in streaks: streaks[current_sec] = []
                    streaks[current_sec].append(current_streak)

                # 1. 섹터별 체류 수명 리포팅
                report_msg += "▪️ <b>섹터별 자금 체류 시간 (수명):</b>\n"
                for sec, lengths in streaks.items():
                    avg_len = sum(lengths) / len(lengths)
                    max_len = max(lengths)
                    report_msg += f" - {sec[:15]}: 평균 {avg_len:.1f}일 (최장 {max_len}일)\n"
                    
                # 2. 자금 이동 궤적 리포팅
                report_msg += "\n▪️ <b>가장 빈번한 자금 이동 경로 (최근 60일, KST 진입일 기준):</b>\n"
                sorted_trans = sorted(transitions.items(), key=lambda x: x[1], reverse=True)[:3]
                if sorted_trans:
                    for path, count in sorted_trans:
                        report_msg += f" - {path} ({count}회 관측)\n"
                else:
                    report_msg += " - 아직 뚜렷한 전이 패턴이 형성되지 않았습니다.\n"
                    
                if current_sec and sorted_trans:
                    # "A ➔ B" 형태에서 B(다음 섹터) 추출
                    top_transition = sorted_trans[0][0]
                    if "➔" in top_transition:
                        next_sec = top_transition.split("➔")[1].strip()
                    else:
                        next_sec = "다음 섹터"
                    report_msg += f"💡 <b>관제탑 동적 통찰:</b> 현재 주도 섹터인 [{current_sec}]의 수명이 다해갈 경우, 과거 패턴상 자금 이동 확률이 가장 높은 [{next_sec}] 섹터의 선취매를 준비하십시오.\n"
                else:
                    report_msg += "💡 <b>관제탑 동적 통찰:</b> 아직 뚜렷한 섹터 전이 패턴이 확보되지 않아 관망을 권장합니다.\n"
            else:
                report_msg += (
                    f"⚠️ 표본 부족 (최근 60일 {market}장 진입 행 0건)으로 순환매 추적 딥다이브 생략.\n"
                )
        except Exception as e:
            report_msg += f"⚠️ 순환매 추적 에러: {e}\n"
            
        # ---------------------------------------------------------
        # 👑 엔진 9: [V39.0 자금 관리 시뮬레이션: 고정 리스크 vs 켈리 리스크]
        # ---------------------------------------------------------
        if 'invest_amount' in df.columns and 'sim_kelly_invest' in df.columns:
            report_msg += f"\n⚖️ <b>[자금 관리 평행우주 대결 — 최근 {rolling_days}일 청산(KST) 기준 실현 손익]</b>\n"
            
            # 💡 [버그 픽스] 과거 투입금 0원 데이터 보정 (기본 40만원)
            valid_invest_fixed = (
                pd.to_numeric(col_series(df, "invest_amount"), errors="coerce")
                .replace(0, 400000)
                .fillna(400000)
            )
            valid_invest_kelly = (
                pd.to_numeric(col_series(df, "sim_kelly_invest"), errors="coerce")
                .replace(0, 400000)
                .fillna(400000)
            )
            fr_dd = pd.to_numeric(col_series(df, "final_ret"), errors="coerce").fillna(0.0)

            total_fixed_profit = scalar_float((valid_invest_fixed * (fr_dd / 100)).sum())
            total_kelly_profit = scalar_float((valid_invest_kelly * (fr_dd / 100)).sum())

            report_msg += f"▪️ 고정 2% 베팅 누적 손익: <b>{total_fixed_profit:,.0f}원</b>\n"
            report_msg += f"▪️ 국면형 켈리 누적 손익: <b>{total_kelly_profit:,.0f}원</b>\n"
            
            if total_kelly_profit > total_fixed_profit:
                if total_kelly_profit > 0:
                    report_msg += "🏆 <b>[켈리 승리]</b> 상승장에서 비중을 싣고 하락장에서 방어한 동적 켈리 전략이 자본 증식에 유리했습니다.\n"
                else:
                    report_msg += "🛡️ <b>[켈리 선방]</b> 두 전략 모두 손실이나, 동적 켈리가 하락장에서 비중을 줄여 계좌 타격을 더 잘 방어했습니다.\n"
            else:
                if total_fixed_profit > 0:
                    report_msg += "🏆 <b>[고정 리스크 승리]</b> 휩소 장세로 인해 켈리 베팅이 엇박자를 내어, 고정 비중 투자가 더 유리했습니다.\n"
                else:
                    report_msg += "🛡️ <b>[고정 리스크 선방]</b> 두 전략 모두 손실이나, 고정 비중이 켈리의 과도한 리스크 베팅보다 타격이 적었습니다.\n"

        # 💡 [핵심 교정] 엔진 9번의 텍스트가 모두 report_msg에 담긴 후 최종 발송하도록 순서 교정
        send_telegram_msg(report_msg)
        print(f"✅ [{market}] 딥 다이브 분석 리포트 발송 완료.")
        
    except Exception as e:
        err_msg = _format_forward_ledger_error_html("딥 다이브 분석 중 에러 발생", e)
        print(err_msg)
        send_telegram_msg(err_msg)
