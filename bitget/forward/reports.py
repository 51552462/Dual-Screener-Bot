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
from bitget.forward.shared import DB_PATH, init_forward_db, load_system_config, save_system_config, send_telegram_msg
from meta_governor_consumer import load_meta_state_resolved
from reports.forward_report_scalar import (
    col_series,
    prepare_forward_trades_df,
    row_scalar,
    scalar_float,
)
from reports.report_state_binder import build_macro_treasury_block, format_macro_treasury_section_html

def send_group_practitioner_reports():
    """PIL — Bitget PRACT_01~30 (spot/futures) Post-Mortem · Vitality · LLM · ZOMBIE 페널티."""
    from bitget.forward.practitioner_bitget_adapter import send_bitget_practitioner_reports_pil

    init_forward_db()
    sync_real_leaderboard_with_virtual()
    cfg = load_system_config()
    seed = float(cfg.get("BITGET_ACCOUNT_SIZE_USDT", 10000) or 10000)
    try:
        from meta_governor_consumer import load_meta_state_resolved
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
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")

    for market_type in ["spot", "futures"]:
        m_icon = "🟢" if market_type == "spot" else "🟠"
        df_all = pd.read_sql(
            "SELECT * FROM bitget_forward_trades WHERE market_type=?",
            conn,
            params=(market_type,),
        )
        if df_all.empty:
            continue
        df_closed = df_all[df_all["status"].str.contains("CLOSED", na=False)].copy()
        df_open = df_all[df_all["status"] == "OPEN"].copy()

        treasury_key = "TREASURY_SPOT_USDT" if market_type == "spot" else "TREASURY_FUTURES_USDT"
        treasury = float(cfg.get(treasury_key, 0.0))
        regime = cfg.get("CURRENT_REGIME_KEY", "UNKNOWN")
        kelly = float(cfg.get("DYNAMIC_KELLY_RISK", 0.01)) * 100.0
        b_status = str(cfg.get("CRYPTO_BREADTH_STATUS", "NEUTRAL"))
        w1 = float(cfg.get("WEIGHT_S1", 1.0))
        w4 = float(cfg.get("WEIGHT_S4", 1.0))

        # [1/6] 거시+국고
        msg1 = f"{m_icon} <b>[1/6] {market_type.upper()} 국면/국고 현황</b>\n"
        msg1 += f"📅 {datetime.utcnow().strftime('%Y-%m-%d')} | 국면: <b>{regime}</b>\n"
        msg1 += f"🏦 잔여 국고: <b>{treasury:,.2f} USDT</b>\n"
        msg1 += f"⚖️ 동적 켈리: {kelly:.2f}%\n"
        msg1 += f"🌊 Breadth: {b_status} | base_w1={w1:.2f}, base_w4={w4:.2f}\n"
        send_telegram_msg(msg1)
        time.sleep(1.0)

        # [2/6] 리더보드
        msg2 = f"{m_icon} <b>[2/6] 로직별 복리 리더보드</b>\n"
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
        for i, (g, bal, wr, op) in enumerate(board[:7]):
            medal = "🥇" if i == 0 else ("🥈" if i == 1 else ("🥉" if i == 2 else "🏃"))
            msg2 += f"{medal} <b>{g}</b>: {bal:,.2f} USDT (승률 {wr:.1f}% / OPEN {op})\n"
        send_telegram_msg(msg2)
        time.sleep(1.0)

        # [3/6] 자금관리 결투
        msg3 = f"{m_icon} <b>[3/6] 자금관리 진검승부</b>\n"
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

        # [4/6] 티어/데스콤보
        msg4 = f"{m_icon} <b>[4/6] 티어/필터 검증</b>\n"
        if not df_closed.empty:
            t1 = df_closed[df_closed["total_score"] >= 80]
            if not t1.empty:
                msg4 += f"💎 1티어 승률: {(t1['final_ret'] > 0).mean()*100:.1f}% | PF {_pf(t1['final_ret']):.2f}\n"
            msg4 += f"⚙️ 전체 PF: {_pf(df_closed['final_ret']):.2f}\n"
        else:
            msg4 += "표본 부족\n"
        send_telegram_msg(msg4)
        time.sleep(1.0)

        # [5/6] TF별 데스매치
        msg5 = f"{m_icon} <b>[5/6] TF별 데스매치</b>\n"
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
        send_telegram_msg(msg5)
        time.sleep(1.0)

        # [6/6] 오픈포지션 스냅샷
        msg6 = f"{m_icon} <b>[6/6] 오픈 포지션 스냅샷</b>\n"
        msg6 += f"📌 OPEN 개수: {len(df_open)}\n"
        if not df_open.empty:
            top = df_open.sort_values("total_score", ascending=False).head(5)
            for _, r in top.iterrows():
                msg6 += f" - {r['symbol']} [{r['timeframe']}] {float(r['total_score']):.1f}점 / {r['sig_type']}\n"
        send_telegram_msg(msg6)
        time.sleep(1.0)

    conn.close()
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
    try:
        init_forward_db()
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL;")
        df = pd.read_sql(
            "SELECT * FROM bitget_forward_trades WHERE market_type=? AND status LIKE 'CLOSED%'",
            conn,
            params=(market_type,),
        )
        conn.close()

        n_closed = len(df)
        if n_closed < 10:
            skip_msg = (
                f"⚠️ <b>[{str(market_type).upper()} Bitget 딥 다이브]</b>\n"
                f"표본 부족 (현재 <b>{n_closed}</b>건 / 최소 10건)으로 딥다이브 생략."
            )
            print(skip_msg.replace("<b>", "").replace("</b>", ""))
            send_telegram_msg(skip_msg)
            return

        cfg = load_system_config()
        df = prepare_forward_trades_df(df, context=f"bitget_deep_dive:{market_type}")

        df["Win"] = np.where(col_series(df, "final_ret") > 0, 1, 0)
        report_msg = f"🔬 [{str(market_type).upper()}장 포워드 테스팅 딥 다이브 분석]\n(총 {len(df)}개 실전 검증 데이터 기반)\n\n"

        for t in range(10, 100, 10):
            tier_label = f"{t}점대"
            t_df = df[df["tier"] == tier_label].copy()
            if len(t_df) < 5:
                continue
            report_msg += f"📌 <b>[{tier_label} 구간 심층 분석]</b>\n"
            t_wr, t_pf = _calculate_metrics(t_df, "final_ret")
            report_msg += f"▪️ 성적: 승률 {t_wr:.1f}% | PF {t_pf:.2f}\n"

            winners = t_df[pd.to_numeric(t_df["final_ret"], errors="coerce").fillna(0.0) > 5.0]
            sideways = t_df[
                (pd.to_numeric(t_df["final_ret"], errors="coerce").fillna(0.0) >= -3.0)
                & (pd.to_numeric(t_df["final_ret"], errors="coerce").fillna(0.0) <= 5.0)
            ]
            losers = t_df[pd.to_numeric(t_df["final_ret"], errors="coerce").fillna(0.0) < -3.0]

            def get_dna(sub_df):
                if len(sub_df) == 0:
                    return f"표본 부족 (현재 0건)으로 {tier_label} DNA 생략"
                rs = pd.to_numeric(col_series(sub_df, "dyn_rs"), errors="coerce").dropna()
                cpv = pd.to_numeric(col_series(sub_df, "dyn_cpv"), errors="coerce").dropna()
                eng = pd.to_numeric(col_series(sub_df, "v_energy"), errors="coerce").dropna()
                if rs.empty or cpv.empty or eng.empty:
                    return f"표본 부족 (현재 {len(sub_df)}건, DNA 컬럼 결측)으로 {tier_label} DNA 생략"
                return (
                    f"RS:{(10 - scalar_float(rs.mean())) * 11.1:.1f}% | "
                    f"CPV:{(10 - scalar_float(cpv.mean())) * 11.1:.1f}% | "
                    f"ENG:{scalar_float(eng.mean()):.1f}"
                )

            report_msg += f" ✅ 대박 DNA: {get_dna(winners)}\n"
            report_msg += f" ↔️ 횡보 DNA: {get_dna(sideways)}\n"
            report_msg += f" 💀 참사 DNA: {get_dna(losers)}\n"
            if len(winners) > 0 and len(losers) > 0:
                w_eng = pd.to_numeric(winners.get("v_energy", pd.Series(dtype=float)), errors="coerce").dropna()
                l_eng = pd.to_numeric(losers.get("v_energy", pd.Series(dtype=float)), errors="coerce").dropna()
                if not w_eng.empty and not l_eng.empty and w_eng.mean() > l_eng.mean() + 1.0:
                    report_msg += f" 💡 통찰: {tier_label}는 에너지가 높을 때만 날아갑니다. 에너지 낮은 종목은 거르십시오.\n"
            report_msg += "\n"

        report_msg += "🌍 [전체 티어 통합: 유니버설(Universal) DNA 분석]\n"
        all_winners = df[pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) > 5.0]
        all_sideways = df[
            (pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) >= -3.0)
            & (pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) <= 5.0)
        ]
        all_losers = df[pd.to_numeric(df["final_ret"], errors="coerce").fillna(0.0) < -3.0]
        if len(all_winners) >= 5 and len(all_losers) >= 5:
            aw_rs = pd.to_numeric(all_winners.get("dyn_rs", pd.Series(dtype=float)), errors="coerce").dropna()
            aw_eng = pd.to_numeric(all_winners.get("v_energy", pd.Series(dtype=float)), errors="coerce").dropna()
            as_cpv = pd.to_numeric(all_sideways.get("dyn_cpv", pd.Series(dtype=float)), errors="coerce").dropna()
            al_cpv = pd.to_numeric(all_losers.get("dyn_cpv", pd.Series(dtype=float)), errors="coerce").dropna()
            al_tb = pd.to_numeric(all_losers.get("dyn_tb", pd.Series(dtype=float)), errors="coerce").dropna()
            if not aw_rs.empty and not aw_eng.empty:
                report_msg += f"✅ [전체 대박주 {len(all_winners)}개 절대 공통점]\n"
                report_msg += f" ↳ 평균 RS: 상위 {(10-aw_rs.mean())*11.1:.1f}% | 평균 에너지: {aw_eng.mean():.1f}\n"
            if not as_cpv.empty:
                report_msg += f"↔️ [전체 횡보주 {len(all_sideways)}개 절대 공통점]\n"
                report_msg += f" ↳ 평균 캔들지배력(CPV): 상위 {(10-as_cpv.mean())*11.1:.1f}% (애매한 매도세가 횡보를 유발함)\n"
            if not al_cpv.empty and not al_tb.empty:
                report_msg += f"💀 [전체 참사주 {len(all_losers)}개 절대 공통점]\n"
                report_msg += f" ↳ 평균 캔들지배력(CPV): 상위 {(10-al_cpv.mean())*11.1:.1f}% | 찐양봉 빈도 하위 {(al_tb.mean())*11.1:.1f}%\n"
                report_msg += f"💡 <b>[관제탑 최종 결론]</b>\n"
                if aw_rs.mean() < al_cpv.mean():
                    report_msg += "현재 시장은 점수와 무관하게 철저히 '상대강도(RS)'가 주도하는 추세장입니다.\n"
                else:
                    report_msg += "현재 시장은 악성 윗꼬리(CPV)에 한 번 걸리면 무조건 계좌가 녹아내리는 변동성 장세입니다.\n"
        else:
            report_msg += (
                f"⚠️ 표본 부족 (대박 {len(all_winners)}건 · 참사 {len(all_losers)}건, "
                "각 5건 이상 필요)으로 Universal DNA 딥다이브 생략.\n"
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
                        "market_type": str(market_type).lower(),
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

        send_telegram_msg(report_msg)
        print(f"✅ [{market_type}] 딥 다이브 분석 리포트 발송 완료.")
    except Exception as e:
        from html import escape as html_escape

        err_msg = (
            f"🚨 <b>[포워드 장부 에러]</b> 딥 다이브 분석 중 에러 발생:\n"
            f"<code>{html_escape(str(e), quote=False)}</code>"
        )
        print(err_msg)
        send_telegram_msg(err_msg)

