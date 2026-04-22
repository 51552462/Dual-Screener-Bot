# auto_forward_tester.py
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import yfinance as yf
import os, time, requests
from datetime import datetime, timedelta
import pytz
import sqlite3

TELEGRAM_TOKEN = "7988939051:AAG4FqMzzz12vd7Crzt8DVPWiL3fMHM8tEc"
TELEGRAM_CHAT_ID = "6838834566"

# 💡 [방향성 2번] 전문적인 DB 시스템 (CSV 폐기)
DB_PATH = os.path.join(os.path.expanduser('~'), 'dante_bots', 'Dual-Screener-Bot', 'market_data.sqlite')

def send_telegram_msg(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    except: pass

def init_forward_db():
    """장부 테이블 생성 (섹터 및 동적 백분위 점수 추가)"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS forward_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT,
            market TEXT,
            code TEXT,
            name TEXT,
            sector TEXT,           -- 💡 [방향성 3번] 주도 섹터 추적용
            sig_type TEXT,
            tier TEXT,
            total_score REAL,
            dyn_rs REAL,           -- 💡 [방향성 1,4번] 상대평가 백분위 기록
            dyn_cpv REAL,
            dyn_tb REAL,
            is_tenbagger INTEGER,  -- 텐배거 조건 충족 여부 (0/1)
            is_top_dna INTEGER,    -- 최상위 DNA 일치 여부
            is_worst_dna INTEGER,  -- 최악의 DNA 일치 여부
            is_death_combo INTEGER,-- 데스콤보 발생 여부
            entry_price REAL,
            v_cpv REAL,
            v_yang REAL,
            v_rs REAL,
            v_energy REAL,         -- 응축 에너지 (BB) 👈 추가
            marcap_eok REAL,       -- 시가총액(억원) 👈 추가
            score_marcap REAL,     -- 시총 체급 점수 👈 추가
            freq_count INTEGER,    -- 과거 매매 빈도 👈 추가
            max_high REAL,
            min_low REAL,
            bars_held INTEGER DEFAULT 0,
            up_vol_sum REAL DEFAULT 0,
            down_vol_sum REAL DEFAULT 0,
            status TEXT DEFAULT 'OPEN',
            exit_date TEXT,
            exit_reason TEXT,
            flow_tags TEXT,
            final_ret REAL,
            mfe REAL
        )
    ''')
    conn.commit()
    conn.close()

# ==========================================
# 1. 신규 종목 편입 엔진 (검색기에서 호출)
# ==========================================
def try_add_virtual_position(market, code, name, sig_type, score, ep, facts, sector="유망섹터"):
    init_forward_db()
    code_str = str(code).zfill(6) if market == 'KR' else str(code)
    
    if score < 40: tier, limit = '10~30점대', 15
    elif score < 70: tier, limit = '40~70점대', 15
    else: tier, limit = '70~100점대', 30

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 중복 체크
    cursor.execute("SELECT id FROM forward_trades WHERE code=? AND status='OPEN'", (code_str,))
    if cursor.fetchone():
        conn.close()
        return False, "중복 보유 중"
        
    # 쿼터 체크
    cursor.execute("SELECT COUNT(*) FROM forward_trades WHERE tier=? AND status='OPEN'", (tier,))
    current_tier_count = cursor.fetchone()[0]
    if current_tier_count >= limit:
        conn.close()
        return False, f"쿼터 초과 ({tier} 꽉 참)"

    tz = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
    today_str = datetime.now(tz).strftime('%Y-%m-%d')

    # 💡 모든 팩트(절대수치 + 백분위 + 섹터) 완벽 박제
    cursor.execute('''
        INSERT INTO forward_trades 
        (entry_date, market, code, name, sector, sig_type, tier, total_score, dyn_rs, dyn_cpv, dyn_tb, entry_price, v_cpv, v_yang, v_energy, v_rs, max_high, min_low)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        today_str, market, code_str, name, sector, sig_type, tier, score,
        facts.get('dyn_rs', 0), facts.get('dyn_cpv', 0), facts.get('dyn_tb', 0), ep,
        facts.get('v_cpv', 0), facts.get('v_yang', 0), facts.get('v_energy', 0), facts.get('v_rs', 0),
        ep, ep
    ))
    conn.commit()
    conn.close()
    
    return True, f"{tier} 편입 성공 (현재 {current_tier_count+1}/{limit})"

# ==========================================
# 2. 매일 종가 흐름 추적 및 청산 엔진 (DB 기반)
# ==========================================
def track_daily_positions(market):
    init_forward_db()
    conn = sqlite3.connect(DB_PATH)
    
    # 현재 보유 중인 종목만 불러오기
    df_active = pd.read_sql("SELECT * FROM forward_trades WHERE market=? AND status='OPEN'", conn, params=(market,))
    if df_active.empty:
        conn.close()
        return

    print(f"\n🔍 [포워드 테스팅] {market} 시장 {len(df_active)}개 종목 추적 중...")
    
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    idx_ticker = '069500' if market == 'KR' else 'SPY'
    try:
        idx_df = fdr.DataReader(idx_ticker, start_date) if market == 'KR' else yf.download(idx_ticker, start=start_date, progress=False)
        idx_close = idx_df['Close'] if market == 'KR' else idx_df['Close'].squeeze()
    except: idx_close = pd.Series(dtype=float)

    for _, r in df_active.iterrows():
        code = r['code']
        ep = r['entry_price']
        
        try:
            df = fdr.DataReader(code, start_date) if market == 'KR' else yf.download(code, start=start_date, progress=False)
            if df.empty or len(df) < 20: continue
                
            c, o, h, l, v = float(df['Close'].iloc[-1]), float(df['Open'].iloc[-1]), float(df['High'].iloc[-1]), float(df['Low'].iloc[-1]), float(df['Volume'].iloc[-1])
            
            new_max = max(r['max_high'], h)
            new_min = min(r['min_low'], l)
            new_bars = r['bars_held'] + 1
            new_up_vol = r['up_vol_sum'] + (v if c > o else 0)
            new_down_vol = r['down_vol_sum'] + (v if c < o else 0)

            # 지표 계산
            df['ema10'] = df['Close'].ewm(span=10, adjust=False).mean()
            df['ema20'] = df['Close'].ewm(span=20, adjust=False).mean()
            z_ema1 = df['Close'].ewm(span=20, adjust=False).mean()
            z_ema2 = z_ema1.ewm(span=20, adjust=False).mean()
            cur_zlema = float((z_ema1 + (z_ema1 - z_ema2)).iloc[-1])

            do_exit, exit_rsn = False, ""
            
            if c < cur_zlema: do_exit, exit_rsn = True, "ZLEMA 이탈"
            elif float(df['ema10'].iloc[-1]) < float(df['ema20'].iloc[-1]) and float(df['ema10'].iloc[-2]) >= float(df['ema20'].iloc[-2]):
                do_exit, exit_rsn = True, "단기데드"
            elif new_bars >= 10 and c <= ep:
                do_exit, exit_rsn = True, "10일 횡보 타임컷"

            if do_exit:
                ret = round(((c - ep) / ep) * 100, 2)
                mfe = round(((new_max - ep) / ep) * 100, 2)
                
                # 💡 [방향성 4번] 태깅 시스템 (과최적화 분석용)
                tags = []
                if mfe >= 7.0 and new_bars <= 8: tags.append("#빠른슈팅_완벽")
                elif mfe >= 7.0 and new_bars > 8: tags.append("#지연슈팅_수명연장")
                elif mfe < 3.0: tags.append("#슈팅실패_조기소멸")
                
                vol_ratio = new_up_vol / (new_down_vol + 1)
                if vol_ratio >= 1.5: tags.append("#건전한조정_매집우위")
                elif vol_ratio < 0.8: tags.append("#음봉대량거래_세력이탈")
                
                try:
                    idx_ret = ((idx_close.iloc[-1] - idx_close.loc[r['entry_date']:].iloc[0]) / idx_close.loc[r['entry_date']:].iloc[0]) * 100
                    if ret > float(idx_ret): tags.append("#RS_방어성공")
                    else: tags.append("#주도력_상실")
                except: pass
                
                flow_tags = " ".join(tags)
                exit_date = datetime.now().strftime('%Y-%m-%d')
                
                # DB 업데이트 (청산)
                conn.execute('''
                    UPDATE forward_trades 
                    SET status=?, exit_date=?, exit_reason=?, flow_tags=?, final_ret=?, mfe=?, max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?
                    WHERE id=?
                ''', ('CLOSED_WIN' if ret > 0 else 'CLOSED_LOSS', exit_date, exit_rsn, flow_tags, ret, mfe, new_max, new_min, new_bars, new_up_vol, new_down_vol, r['id']))
                
                icon = "🔥익절" if ret > 0 else "💀손절"
                send_telegram_msg(f"🤖 [{market}] 청산: {icon} {r['name']} ({r['total_score']}점)\n▪️ 수익: {ret}%\n▪️ 태그: {flow_tags}\n▪️ 사유: {exit_rsn}")
            else:
                # DB 업데이트 (유지)
                conn.execute('''
                    UPDATE forward_trades 
                    SET max_high=?, min_low=?, bars_held=?, up_vol_sum=?, down_vol_sum=?
                    WHERE id=?
                ''', (new_max, new_min, new_bars, new_up_vol, new_down_vol, r['id']))
                
        except Exception as e: pass

    conn.commit()
    conn.close()

# ==========================================
# 3. 매일 16:00 일일 종합 리포트 텔레그램 (섹터 흐름 추가)
# ==========================================
def send_daily_summary_report():
    tz_kr = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(tz_kr).strftime('%Y-%m-%d')
    
    report_msg = f"📊 [포워드 테스팅 일일 종합 리포트]\n📅 {today_str} 기준\n\n"

    try:
        conn = sqlite3.connect(DB_PATH)
        
        for market in ['KR', 'US']:
            # 승률 계산
            cursor = conn.execute("SELECT COUNT(*), SUM(CASE WHEN final_ret > 0 THEN 1 ELSE 0 END) FROM forward_trades WHERE market=? AND status LIKE 'CLOSED%'", (market,))
            total_closed, wins = cursor.fetchone()
            total_closed = total_closed or 0
            wins = wins or 0
            win_rate = round((wins / total_closed) * 100, 1) if total_closed > 0 else 0.0
            
            report_msg += f"📈 [{market}장 누적 실전 승률]: {win_rate}% (총 {total_closed}건 검증)\n"
            
            # 보유 쿼터 현황
            cursor = conn.execute("SELECT tier, COUNT(*) FROM forward_trades WHERE market=? AND status='OPEN' GROUP BY tier", (market,))
            tier_counts = {row[0]: row[1] for row in cursor.fetchall()}
            total_active = sum(tier_counts.values())
            
            report_msg += f"📦 [현재 포트폴리오: 총 {total_active}종목]\n"
            report_msg += f" - 70~100점: {tier_counts.get('70~100점대', 0)}/30\n"
            report_msg += f" - 40~69점: {tier_counts.get('40~70점대', 0)}/15\n"
            report_msg += f" - 10~39점: {tier_counts.get('10~30점대', 0)}/15\n\n"
        
        # 💡 [방향성 3번] 최근 7일 주도 섹터(돈이 몰리는 곳) 추출
        report_msg += "🔥 [최근 7일 알고리즘 주도 섹터 TOP 3]\n"
        query = "SELECT sector, COUNT(*) as cnt FROM forward_trades WHERE entry_date >= date('now', '-7 days') GROUP BY sector ORDER BY cnt DESC LIMIT 3"
        for row in conn.execute(query).fetchall():
            report_msg += f" 🎯 {row[0]} ({row[1]}개 포착)\n"
            
        conn.close()
    except Exception as e:
        report_msg += f"\n에러 발생: {e}"

    report_msg += "\n💡 (모든 지표와 백분위 데이터는 DB에 완벽히 박제 중입니다.)"
    send_telegram_msg(report_msg)
    print(f"✅ 16:00 일일 종합 리포트 텔레그램 발송 완료.")
    
    # ==========================================
# 4. [방향성 5,6,7번] 퀀트 딥 다이브 분석 엔진 (특징 추출 및 티어별 성적표)
# ==========================================
def run_deep_dive_analysis(market='KR'):
    """
    미래 데이터(포워드 테스팅)를 기반으로 내 시스템의 과최적화를 검증하고,
    대박/참사 종목의 DNA와 티어별 진짜 승률을 텔레그램으로 보고합니다.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # 이미 청산(결과가 확정된) 종목들만 불러옵니다.
        df = pd.read_sql(f"SELECT * FROM forward_trades WHERE market='{market}' AND status LIKE 'CLOSED%'", conn)
        conn.close()
        
        if len(df) < 10:
            print(f"⚠️ [{market}] 아직 통계를 낼 만큼 청산된 데이터가 충분하지 않습니다. (최소 10개 필요)")
            return

        df['Win'] = np.where(df['final_ret'] > 0, 1, 0)
        
        report_msg = f"🔬 [{market}장 포워드 테스팅 딥 다이브 분석]\n(총 {len(df)}개 실전 검증 데이터 기반)\n\n"

        # ---------------------------------------------------------
        # [방향성 6번 해결] 티어별(10~30, 40~70, 70~100) 실전 성적표 추적
        # ---------------------------------------------------------
        report_msg += "📊 [티어별 실전 성적표]\n"
        tier_grp = df.groupby('tier').agg(
            매매건수=('id', 'count'),
            승률=('Win', lambda x: round(x.mean() * 100, 1)),
            평균수익=('final_ret', lambda x: round(x[x > 0].mean(), 2) if len(x[x > 0]) > 0 else 0),
            평균손실=('final_ret', lambda x: round(x[x <= 0].mean(), 2) if len(x[x <= 0]) > 0 else 0)
        ).reset_index()
        
        for _, r in tier_grp.iterrows():
            report_msg += f"🏅 {r['tier']} (총 {r['매매건수']}건)\n"
            report_msg += f" ↳ 실전 승률: {r['승률']}% | 평균 익절 +{r['평균수익']}% / 평균 손절 {r['평균손실']}%\n"
        report_msg += "\n"

        # ---------------------------------------------------------
        # [방향성 5, 6번 해결] 어떤 지표가 진짜 도움이 되었나? (과최적화 솎아내기)
        # ---------------------------------------------------------
        winners = df[df['final_ret'] > 5.0]  # 대박 종목 기준 (5% 이상 수익)
        losers = df[df['final_ret'] < -3.0]  # 참사 종목 기준 (-3% 이하 손실)
        
        report_msg += "🧬 [대박 vs 참사 종목 DNA(특징) 대조]\n"
        if len(winners) > 0 and len(losers) > 0:
            # 절대 수치가 아닌 '동적 백분위(dyn_)'를 대조하여 지표의 유효성 검증
            w_rs, l_rs = winners['dyn_rs'].mean(), losers['dyn_rs'].mean()
            w_cpv, l_cpv = winners['dyn_cpv'].mean(), losers['dyn_cpv'].mean()
            
            report_msg += f"📈 대박 종목 평균: RS 상위 {(10-w_rs)*11.1:.1f}% | 찐양봉 상위 {(10-winners['dyn_tb'].mean())*11.1:.1f}%\n"
            report_msg += f"💀 참사 종목 평균: RS 상위 {(10-l_rs)*11.1:.1f}% | 찐양봉 상위 {(10-losers['dyn_tb'].mean())*11.1:.1f}%\n"
            
            # 피드백 자동 도출 (어떤 지표가 도움이 되고 무용지물인지)
            if w_rs > l_rs + 1.0:
                report_msg += "💡 결론: 상대강도(RS)가 높을수록 실전 수익률에 매우 큰 도움이 됨. (RS 가중치 신뢰도 높음)\n"
            else:
                report_msg += "💡 결론: RS는 실전 결과와 큰 연관이 없음. (다른 지표 가중치를 올려야 함)\n"
        else:
            report_msg += "데이터가 더 누적되어야 DNA 대조가 가능합니다.\n"
        report_msg += "\n"

        # ---------------------------------------------------------
        # [방향성 6번 해결] 세부 특징(태그)별 승률 기여도 추적
        # ---------------------------------------------------------
        report_msg += "🏷️ [세부 흐름 태그별 승률 기여도]\n"
        tag_stats = {}
        for _, row in df.iterrows():
            if pd.isna(row['flow_tags']): continue
            for tag in str(row['flow_tags']).split():
                if tag not in tag_stats: tag_stats[tag] = {'win': 0, 'total': 0}
                tag_stats[tag]['total'] += 1
                if row['Win'] == 1: tag_stats[tag]['win'] += 1
                
        for tag, stats in sorted(tag_stats.items(), key=lambda x: x[1]['total'], reverse=True)[:5]:
            if stats['total'] >= 3: # 최소 3번 이상 등장한 태그만
                tag_win_rate = round((stats['win'] / stats['total']) * 100, 1)
                report_msg += f" ▪️ {tag}: 승률 {tag_win_rate}% (출현 {stats['total']}회)\n"

        send_telegram_msg(report_msg)
        print(f"✅ [{market}] 딥 다이브 분석 리포트 발송 완료.")
        
    except Exception as e:
        print(f"⚠️ 딥 다이브 분석 중 에러: {e}")

if __name__ == "__main__":
    # 나중에 데이터가 충분히 쌓이면 주말에 한 번씩 이 파일을 직접 실행해서 분석 리포트를 받아봅니다.
    # run_deep_dive_analysis('KR')
    # run_deep_dive_analysis('US')
    pass
