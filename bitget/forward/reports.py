"""Daily reports and deep dive."""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from bitget.forward.execution_bridge import (
    build_practitioner_reality_leaderboard,
    sync_real_leaderboard_with_virtual,
)
from bitget.forward.gates import _extract_core_group
from bitget.forward.mutant import (
    _auto_tune_brain_from_closed_df,
    _calculate_metrics,
    _coin_asset_group,
    _pf,
)
from bitget.forward.deathmatch_report_section import build_deathmatch_section
from bitget.forward.dna_autopsy import build_dna_autopsy_slice, format_dna_autopsy_section
from bitget.forward.forward_book_integrity import (
    compute_open_book_stats,
    format_open_book_integrity_html,
)
from bitget.forward.rotation_report_section import build_rotation_spillover_section
from bitget.forward.shared import DB_PATH, init_forward_db, load_system_config, save_system_config, send_telegram_msg
from bitget.infra.shared_db_connector import get_connection
from bitget.governance.meta_consumer import load_meta_state_resolved
from bitget.reports.bitget_report_context import BitgetReportContext
from reports.forward_report_scalar import (
    col_series,
    prepare_forward_trades_df,
    row_scalar,
    scalar_float,
)
from reports.report_state_binder import build_macro_treasury_block, format_macro_treasury_section_html


def _norm_market_type(market_type: str) -> str:
    """DB SSOT: ledger inserts lowercase spot/futures (bug #2)."""
    return str(market_type or "spot").strip().lower()


def send_group_practitioner_reports():
    """PIL — Bitget PRACT_01~30 (spot/futures) Post-Mortem · Vitality · LLM · ZOMBIE 페널티."""
    from bitget.forward.practitioner_bitget_adapter import send_bitget_practitioner_reports_pil

    init_forward_db()
    sync_real_leaderboard_with_virtual()
    cfg = load_system_config()
    seed = float(cfg.get("BITGET_ACCOUNT_SIZE_USDT", 10000) or 10000)
    try:
        from bitget.governance.meta_consumer import load_meta_state_resolved
    except Exception:
        load_meta_state_resolved = lambda: {}

    try:
        out = send_bitget_practitioner_reports_pil(
            db_path=DB_PATH,
            send_telegram_msg=send_telegram_msg,
            load_system_config=load_system_config,
            load_meta_state_resolved=load_meta_state_resolved,
            base_seed_usdt=seed,
        )
        print(f"🧠 [Bitget PIL] 실무자 리포트: {out}")
    except Exception as ex:
        send_telegram_msg(f"⚠️ Bitget PIL 실무자 리포트 에러: {ex}")

def send_comprehensive_daily_report():
    init_forward_db()
    cfg = load_system_config()
    meta = load_meta_state_resolved() or {}
    ctx = BitgetReportContext.build()
    conn = get_connection(DB_PATH)

    for market_type in ["spot", "futures"]:
        mkt = _norm_market_type(market_type)
        m_icon = "🟢" if mkt == "spot" else "🟠"
        df_all = pd.read_sql(
            "SELECT * FROM bitget_forward_trades WHERE market_type=?",
            conn,
            params=(mkt,),
        )
        # 표본 0건이어도 주식 daily_report 패리티대로 "0건" 리포트를 발송한다 —
        # 여기서 continue 하면 스캔이 아직 진입 기록을 쌓지 못한 시장은 텔레그램에
        # 아무 알림도 뜨지 않아(원인불명 침묵) 장애 감지가 불가능해진다.
        # 아래 슬라이스/집계 함수들은 모두 빈 DataFrame을 안전하게 처리한다.
        df_all = prepare_forward_trades_df(df_all, context=f"bitget_comprehensive:{mkt}")
        mkt_slice = ctx.slice_for_market(df_all, market_type)
        df_closed = mkt_slice.df_closed
        df_open = mkt_slice.df_open
        tk = ctx.timekeeper_for(market_type)
        book_stats = compute_open_book_stats(
            df_all,
            market_type=market_type,
            session_anchor=tk.session_anchor,
        )
        integrity_html = format_open_book_integrity_html(book_stats)

        treasury_key = "TREASURY_SPOT_USDT" if mkt == "spot" else "TREASURY_FUTURES_USDT"
        treasury = float(cfg.get(treasury_key, 0.0))
        regime = cfg.get("CURRENT_REGIME_KEY", "UNKNOWN")
        kelly = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01)) * 100.0
        b_status = str(cfg.get("CRYPTO_BREADTH_STATUS", "NEUTRAL"))
        w1 = float(cfg.get("WEIGHT_S1", 1.0))
        w4 = float(cfg.get("WEIGHT_S4", 1.0))

        msg1 = f"{m_icon} <b>[1/9] {mkt.upper()} 국면/국고 현황</b>\n"
        msg1 += ctx.market_window_header_html(
            market_type,
            n_real=len(mkt_slice.df_real),
            n_closed=mkt_slice.n_closed_window,
            n_open=mkt_slice.n_open_valid,
        )
        if integrity_html:
            msg1 += integrity_html
        msg1 += f"📅 {datetime.utcnow().strftime('%Y-%m-%d')} UTC | 국면: <b>{regime}</b>\n"
        msg1 += f"🏦 잔여 국고: <b>{treasury:,.2f} USDT</b>\n"
        msg1 += f"⚖️ 동적 켈리: {kelly:.2f}%\n"
        msg1 += f"🌊 Breadth: {b_status} | base_w1={w1:.2f}, base_w4={w4:.2f}\n"
        send_telegram_msg(msg1)
        time.sleep(1.0)

        msg2 = f"{m_icon} <b>[2/9] 로직별 복리 리더보드</b>\n"
        groups = {}
        for _, r in df_all.iterrows():
            g = _extract_core_group(r.get("sig_type", "UNKNOWN"))
            groups.setdefault(g, {"closed": [], "open": 0})
            if str(r.get("status", "")).startswith("OPEN"):
                groups[g]["open"] += 1
            else:
                groups[g]["closed"].append(float(r.get("final_ret", 0.0) or 0.0))
        board = []
        base_seed = float(cfg.get("ACCOUNT_SIZE_USDT", 100000))
        for g, v in groups.items():
            s = pd.Series(v["closed"], dtype=float)
            pnl = ((s / 100.0) * base_seed * 0.01).sum()
            bal = base_seed + pnl
            wr = float((s > 0).mean() * 100.0) if len(s) else 0.0
            board.append((g, bal, wr, v["open"]))
        board.sort(key=lambda x: x[1], reverse=True)
        if not board:
            msg2 += "표본 부족 — 아직 로직별 진입 기록이 없습니다.\n"
        for i, (g, bal, wr, op) in enumerate(board[:7]):
            medal = "🥇" if i == 0 else ("🥈" if i == 1 else ("🥉" if i == 2 else "🏃"))
            msg2 += f"{medal} <b>{g}</b>: {bal:,.2f} USDT (승률 {wr:.1f}% / OPEN {op})\n"
        send_telegram_msg(msg2)
        time.sleep(1.0)

        msg3 = f"{m_icon} <b>[3/9] 자금관리 진검승부</b>\n"
        if not df_closed.empty:
            kelly_pnl = float((df_closed["sim_kelly_invest"] * df_closed["final_ret"] / 100.0).sum())
            fixed_pnl = float((df_closed["margin_used"] * df_closed["final_ret"] / 100.0).sum())
        else:
            kelly_pnl = 0.0
            fixed_pnl = 0.0
        msg3 += f"💰 누적 켈리 손익: <b>{kelly_pnl:+,.2f} USDT</b>\n"
        msg3 += f"🛡️ 누적 고정 손익: {fixed_pnl:+,.2f} USDT\n"
        msg3 += f"🏁 우위: {'동적 켈리' if kelly_pnl > fixed_pnl else '고정 리스크'}\n"
        send_telegram_msg(msg3)
        time.sleep(1.0)

        msg4 = f"{m_icon} <b>[4/9] 자산군 포트폴리오 다중화</b>\n"
        if not df_closed.empty:
            ag_counts = {}
            for _, r in df_closed.iterrows():
                ag = _coin_asset_group(str(r.get("symbol", "")))
                ag_counts[ag] = ag_counts.get(ag, 0) + 1
            total = sum(ag_counts.values()) or 1
            for ag, cnt in sorted(ag_counts.items(), key=lambda x: -x[1])[:6]:
                msg4 += f"▪️ {ag}: {cnt}건 ({cnt / total * 100:.1f}%)\n"
        else:
            msg4 += "표본 부족\n"
        send_telegram_msg(msg4)
        time.sleep(1.0)

        msg5 = f"{m_icon} <b>[5/9] 티어 및 데스콤보 검증</b>\n"
        if not df_closed.empty:
            t1 = df_closed[df_closed["total_score"] >= 80]
            if not t1.empty:
                msg5 += f"💎 1티어 승률: {(t1['final_ret'] > 0).mean()*100:.1f}% | PF {_pf(t1['final_ret']):.2f}\n"
            msg5 += f"⚙️ 전체 PF: {_pf(df_closed['final_ret']):.2f}\n"
            for tf in ["1D", "4H", "2H", "1H"]:
                sub = df_closed[df_closed["timeframe"].astype(str).str.upper() == tf]
                if sub.empty:
                    continue
                st = sub[sub["sig_type"].astype(str).str.contains("STANDARD", na=False)]
                sn = sub[sub["sig_type"].astype(str).str.contains("SUPERNOVA", na=False)]
                st_pf = _pf(st["final_ret"]) if not st.empty else 0.0
                sn_pf = _pf(sn["final_ret"]) if not sn.empty else 0.0
                winner = "SUPERNOVA" if sn_pf > st_pf else "STANDARD"
                msg5 += f"▪️ {tf}: STD {st_pf:.2f} vs SN {sn_pf:.2f} → <b>{winner}</b>\n"
        else:
            msg5 += "표본 부족\n"
        send_telegram_msg(msg5)
        time.sleep(1.0)

        dna_slice = build_dna_autopsy_slice(ctx, market_type, df_closed, sys_config=cfg)
        msg6 = f"{m_icon} <b>[6/9] 대박주/참사주 4차원 DNA 부검</b>\n"
        msg6 += format_dna_autopsy_section(
            dna_slice,
            ctx=ctx,
            market_type=market_type,
            n_real=len(mkt_slice.df_real),
            n_open=mkt_slice.n_open_valid,
            sys_config=cfg,
            meta=meta,
        )
        send_telegram_msg(msg6)
        time.sleep(1.0)

        msg7 = build_rotation_spillover_section(
            ctx, market_type, mkt_slice, sys_config=cfg, market_icon=m_icon
        )
        send_telegram_msg(msg7)
        time.sleep(1.0)

        msg8 = f"{m_icon} <b>[8/9] 메타 최적화 및 알파 반감기</b>\n"
        health = meta.get("META_STRATEGY_HEALTH") if isinstance(meta, dict) else None
        if isinstance(health, dict) and health:
            for arm, h in list(health.items())[:5]:
                if isinstance(h, dict):
                    msg8 += f"▪️ {arm}: score={h.get('score', '—')} decay={h.get('half_life_days', '—')}\n"
        else:
            msg8 += "<i>메타 헬스 데이터 없음 — governance/meta_consumer 경로 확인</i>\n"
        send_telegram_msg(msg8)
        time.sleep(1.0)

        msg9 = build_deathmatch_section(
            ctx,
            market_type,
            df_closed,
            mkt_slice,
            sys_config=cfg,
            meta=meta,
            market_icon=m_icon,
            apply_deathmatch_allocation=True,
        )
        send_telegram_msg(msg9)
        time.sleep(1.0)

    conn.close()

    # [동적 탐험예산 — 7일 롤링 MAB] 챔피언/탐험 자본배분 현황 패널.
    # 하루 1회 여기서 갱신(무거운 DB 롤링 집계) → 이후 Kelly 사이징 핫패스는
    # system_config 에 저장된 상태만 읽어 스케일러를 곱한다(추가 I/O 없음).
    try:
        from bitget.governance.exploration_budget import (
            format_exploration_budget_panel_html,
            refresh_exploration_budget_state,
        )

        _budget_state = refresh_exploration_budget_state()
        send_telegram_msg(format_exploration_budget_panel_html(_budget_state))
    except Exception as e:
        send_telegram_msg(f"⚠️ [자본 배분] exploration_budget 갱신 실패: {e}")

    # 일일 종합 리포트와 실무자 30인 개별 리포트를 연동 실행
    try:
        send_group_practitioner_reports()
    except Exception as e:
        send_telegram_msg(f"⚠️ practitioner report error: {e}")

def run_deep_dive_analysis(market_type="spot"):
    """
    미래 데이터(포워드 테스팅)를 기반으로 내 시스템의 과최적화를 검증하고,
    대박/참사 종목의 DNA와 티어별 진짜 승률을 텔레그램으로 보고합니다.
    """
    mkt = _norm_market_type(market_type)
    try:
        init_forward_db()
        cfg = load_system_config()
        meta = load_meta_state_resolved() or {}
        ctx = BitgetReportContext.build()

        conn = get_connection(DB_PATH)
        df_all = pd.read_sql(
            "SELECT * FROM bitget_forward_trades WHERE market_type=?",
            conn,
            params=(mkt,),
        )
        conn.close()

        if df_all.empty:
            skip_msg = (
                f"⚠️ <b>[{mkt.upper()} Bitget 딥 다이브]</b>\n"
                "표본 부족 (현재 <b>0</b>건 / 최소 10건)으로 딥다이브 생략."
            )
            print(skip_msg.replace("<b>", "").replace("</b>", ""))
            send_telegram_msg(skip_msg)
            return

        df_all = prepare_forward_trades_df(df_all, context=f"bitget_deep_dive:{mkt}")
        mkt_slice = ctx.slice_for_market(df_all, mkt)
        df = mkt_slice.df_closed.copy()
        tk = ctx.timekeeper_for(mkt)

        n_closed = len(df)
        if n_closed < 10:
            skip_msg = (
                f"⚠️ <b>[{mkt.upper()} Bitget 딥 다이브]</b>\n"
                f"표본 부족 (롤링 윈도우 <b>{n_closed}</b>건 / 최소 10건)으로 딥다이브 생략.\n"
                f"📎 <i>{tk.rolling_cutoff}~{tk.session_anchor} · lag {ctx.lag_for(mkt)}d</i>"
            )
            print(skip_msg.replace("<b>", "").replace("</b>", ""))
            send_telegram_msg(skip_msg)
            return

        df["Win"] = np.where(col_series(df, "final_ret") > 0, 1, 0)
        report_msg = (
            f"🔬 [{mkt.upper()}장 포워드 테스팅 딥 다이브 분석]\n"
            f"(롤링 {ctx.window_days}일 청산 {n_closed}건)\n"
            f"📎 <i>{tk.rolling_cutoff}~{tk.session_anchor}</i>\n\n"
        )

        for t in range(10, 100, 10):
            tier_label = f"{t}점대"
            t_df = df[df["tier"] == tier_label].copy()
            if len(t_df) < 5:
                continue
            report_msg += f"📌 <b>[{tier_label} 구간 심층 분석]</b>\n"
            t_wr, t_pf = _calculate_metrics(t_df, "final_ret")
            report_msg += f"▪️ 성적: 승률 {t_wr:.1f}% | PF {t_pf:.2f}\n\n"

        dna_slice = build_dna_autopsy_slice(ctx, mkt, df, sys_config=cfg)
        report_msg += "🌍 <b>[4차원 DNA 정밀 부검 — dna_autopsy SSOT]</b>\n"
        report_msg += format_dna_autopsy_section(
            dna_slice,
            ctx=ctx,
            market_type=mkt,
            n_real=len(mkt_slice.df_real),
            n_open=mkt_slice.n_open_valid,
            sys_config=cfg,
            meta=meta,
        )
        report_msg += "\n"

        report_msg += "🏷️ [세부 흐름 태그별 승률 기여도]\n"
        tag_stats = {}
        for _, row in df.iterrows():
            if pd.isna(row.get("flow_tags")):
                continue
            for tag in str(row.get("flow_tags")).split():
                tag_stats.setdefault(tag, {"win": 0, "total": 0})
                tag_stats[tag]["total"] += 1
                if int(row.get("Win", 0)) == 1:
                    tag_stats[tag]["win"] += 1
        for tag, stats in sorted(tag_stats.items(), key=lambda x: x[1]["total"], reverse=True)[:5]:
            if stats["total"] >= 3:
                tag_win_rate = round((stats["win"] / stats["total"]) * 100, 1)
                report_msg += f" ▪️ {tag}: 승률 {tag_win_rate}% (출현 {stats['total']}회)\n"

        if "margin_used" in df.columns and "sim_kelly_invest" in df.columns:
            report_msg += "\n⚖️ <b>[V39.0 자금 관리 평행우주 대결 (누적 실현 손익)]</b>\n"
            df["fixed_profit"] = pd.to_numeric(df["margin_used"], errors="coerce").fillna(0.0) * (
                pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) / 100.0
            )
            total_fixed_profit = scalar_float(df["fixed_profit"].sum())
            df["kelly_profit"] = pd.to_numeric(df["sim_kelly_invest"], errors="coerce").fillna(0.0) * (
                pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) / 100.0
            )
            total_kelly_profit = scalar_float(df["kelly_profit"].sum())
            report_msg += f"▪️ 고정 2% 베팅 누적 손익: <b>{total_fixed_profit:,.2f}USDT</b>\n"
            report_msg += f"▪️ 국면형 켈리 누적 손익: <b>{total_kelly_profit:,.2f}USDT</b>\n"
            if total_kelly_profit > total_fixed_profit:
                report_msg += "🏆 <b>결론: 동적 켈리가 승리했습니다.</b> 상승장에서 비중을 싣고 하락장에서 방어한 전략이 누적 자본 증식에 훨씬 유리함을 데이터로 증명했습니다.\n"
            else:
                report_msg += "🛡️ <b>결론: 고정 리스크가 유리했습니다.</b> 켈리 베팅이 과도한 리스크를 지거나 휩소에 당했습니다. 켈리 승수를 하향 조정해야 합니다.\n"

        # ANTI_PATTERNS 누적: -10% 이하 참사주의 4D DNA를 면역 메모리로 저장
        fatal_df = df[pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) <= -10.0].copy()
        if not fatal_df.empty:
            anti_patterns = cfg.get("ANTI_PATTERNS", [])
            if not isinstance(anti_patterns, list):
                anti_patterns = []
            for _, row in fatal_df.iterrows():
                anti_patterns.append(
                    {
                        "market_type": mkt,
                        "symbol": str(row.get("symbol", "")),
                        "sig_type": str(row.get("sig_type", "")),
                        "dyn_cpv": row_scalar(row, "dyn_cpv"),
                        "dyn_tb": row_scalar(row, "dyn_tb"),
                        "v_energy": row_scalar(row, "v_energy"),
                        "dyn_rs": row_scalar(row, "dyn_rs"),
                        "final_ret": row_scalar(row, "final_ret"),
                        "recorded_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
            # 폭주 방지: 최신 500개만 유지
            cfg["ANTI_PATTERNS"] = anti_patterns[-500:]
            report_msg += f"\n🧬 [ANTI_PATTERNS] 참사주 DNA {len(fatal_df)}건 누적 저장 완료\n"

        # 딥다이브 결과를 실제 설정값으로 반영하는 자율 뇌수술(Brain Surgery)
        cfg, tune_msgs = _auto_tune_brain_from_closed_df(cfg, df)
        save_system_config(cfg)
        if tune_msgs:
            report_msg += "\n🧠 [자율 튜닝 적용]\n"
            for m in tune_msgs:
                report_msg += f"▪️ {m}\n"

        # [동적 탐험예산] 참고용 스냅샷(하루 1회 갱신은 comprehensive_report 에서 수행).
        try:
            from bitget.governance.exploration_budget import (
                format_exploration_budget_panel_html,
            )

            report_msg += "\n" + format_exploration_budget_panel_html()
        except Exception:
            pass

        send_telegram_msg(report_msg)
        print(f"✅ [{mkt}] 딥 다이브 분석 리포트 발송 완료.")
    except Exception as e:
        from html import escape as html_escape

        err_msg = (
            f"🚨 <b>[포워드 장부 에러]</b> 딥 다이브 분석 중 에러 발생:\n"
            f"<code>{html_escape(str(e), quote=False)}</code>"
        )
        print(err_msg)
        send_telegram_msg(err_msg)

