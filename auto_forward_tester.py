# auto_forward_tester.py
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import yfinance as yf
import pandas_ta as ta
import os, time, requests
from datetime import datetime
import pytz

TELEGRAM_TOKEN = "7988939051:AAG4FqMzzz12vd7Crzt8DVPWiL3fMHM8tEc" # 본캐 토큰
TELEGRAM_CHAT_ID = "6838834566"

# 💡 한국/미국 독립 DB 설정
DB_FILES = {
    'KR': {'active': 'Live_Active_KR.csv', 'completed': 'Live_Completed_KR.csv'},
    'US': {'active': 'Live_Active_US.csv', 'completed': 'Live_Completed_US.csv'}
}

def send_telegram_msg(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    except: pass

# ==========================================
# 1. 쿼터 통제 및 신규 종목 편입 엔진 (검색기에서 호출됨)
# ==========================================
def try_add_virtual_position(market, code, name, sig_type, score, ep, facts):
    active_file = DB_FILES[market]['active']
    
    # 1. 기존 장부 로드
    if os.path.exists(active_file):
        df = pd.read_csv(active_file)
    else:
        df = pd.DataFrame(columns=['Entry_Date', 'Market', 'Code', 'Name', 'Signal', 'Score', 'Tier', 'Entry_Price', 
                                   'CPV', 'TB', 'BBE', 'RS_Entry', 'Max_High', 'Min_Low', 'Bars_Held', 
                                   'Up_Vol_Sum', 'Down_Vol_Sum'])

    code_str = str(code).zfill(6) if market == 'KR' else str(code)

    # 2. 중복 포지션 차단 (이미 들고 있으면 스킵)
    if not df.empty and code_str in df['Code'].astype(str).values:
        return False, "중복 보유 중"

    # 3. 점수 티어 분류 및 쿼터(할당량) 체크
    if score < 40: tier, limit = '10~30점대', 15
    elif score < 70: tier, limit = '40~70점대', 15
    else: tier, limit = '70~100점대', 30

    current_tier_count = len(df[df['Tier'] == tier])
    if current_tier_count >= limit:
        return False, f"쿼터 초과 ({tier} 꽉 참)"

    # 4. 장부에 신규 편입
    tz = pytz.timezone('Asia/Seoul') if market == 'KR' else pytz.timezone('America/New_York')
    new_pos = {
        'Entry_Date': datetime.now(tz).strftime('%Y-%m-%d'),
        'Market': market, 'Code': code_str, 'Name': name, 'Signal': sig_type,
        'Score': score, 'Tier': tier, 'Entry_Price': ep,
        'CPV': facts.get('v_cpv', 0), 'TB': facts.get('v_yang', 0), 
        'BBE': facts.get('v_energy', 0), 'RS_Entry': facts.get('v_rs', 0),
        'Max_High': ep, 'Min_Low': ep, 'Bars_Held': 0,
        'Up_Vol_Sum': 0.0, 'Down_Vol_Sum': 0.0
    }
    
    df = pd.concat([df, pd.DataFrame([new_pos])], ignore_index=True)
    df.to_csv(active_file, index=False, encoding='utf-8-sig')
    return True, f"{tier} 편입 성공 (현재 {current_tier_count+1}/{limit})"

# ==========================================
# 2. 매일 종가 흐름 추적 및 청산/태깅 엔진
# ==========================================
def track_daily_positions(market):
    active_file = DB_FILES[market]['active']
    comp_file = DB_FILES[market]['completed']
    
    if not os.path.exists(active_file): return
    df_active = pd.read_csv(active_file)
    if df_active.empty: return

    print(f"\n🔍 [포워드 테스팅] {market} 시장 {len(df_active)}개 종목 추적 및 채점 중...")
    
    keep_list = []
    completed_list = []
    
    # 벤치마크 지수 로드 (RS 흐름 비교용)
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
    idx_ticker = '069500' if market == 'KR' else 'SPY' # 임의로 KODEX/SPY 기준
    try:
        idx_df = fdr.DataReader(idx_ticker, start_date) if market == 'KR' else yf.download(idx_ticker, start=start_date, progress=False)
        idx_close = idx_df['Close'] if market == 'KR' else idx_df['Close'].squeeze()
    except: idx_close = pd.Series(dtype=float)

    for _, r in df_active.iterrows():
        code = str(r['Code']).zfill(6) if market == 'KR' else str(r['Code'])
        ep = float(r['Entry_Price'])
        
        try:
            df = fdr.DataReader(code, start_date) if market == 'KR' else yf.download(code, start=start_date, progress=False)
            if df.empty or len(df) < 20: 
                keep_list.append(r)
                continue
                
            c, o, h, l, v = df['Close'].iloc[-1], df['Open'].iloc[-1], df['High'].iloc[-1], df['Low'].iloc[-1], df['Volume'].iloc[-1]
            c = float(c) # yf 시리즈 방어
            
            # 흐름 1: 생애주기 업데이트
            r['Max_High'] = max(r['Max_High'], float(h))
            r['Min_Low'] = min(r['Min_Low'], float(l))
            r['Bars_Held'] += 1
            
            # 흐름 2: 거래량 프로파일 누적 (양봉 거래량 vs 음봉 거래량)
            if c > o: r['Up_Vol_Sum'] += float(v)
            elif c < o: r['Down_Vol_Sum'] += float(v)

            # 지표 계산 (청산 룰)
            df['ema10'] = df['Close'].ewm(span=10, adjust=False).mean()
            df['ema20'] = df['Close'].ewm(span=20, adjust=False).mean()
            z_ema1 = ta.ema(df['Close'].squeeze(), length=20)
            z_ema2 = ta.ema(z_ema1, length=20)
            cur_zlema = float((z_ema1 + (z_ema1 - z_ema2)).iloc[-1])

            do_exit, exit_rsn = False, ""
            
            # 청산 룰 검사
            if c < cur_zlema: do_exit, exit_rsn = True, "ZLEMA 이탈"
            elif float(df['ema10'].iloc[-1]) < float(df['ema20'].iloc[-1]) and float(df['ema10'].iloc[-2]) >= float(df['ema20'].iloc[-2]):
                do_exit, exit_rsn = True, "단기데드"
            elif r['Bars_Held'] >= 10 and c <= ep:
                do_exit, exit_rsn = True, "10일 횡보 타임컷"

            if do_exit:
                ret = ((c - ep) / ep) * 100
                mfe = ((r['Max_High'] - ep) / ep) * 100
                
                # --------------------------------------------------
                # 💡 [핵심] 흐름 채점 및 자동 태깅 (Auto-Tagging)
                # --------------------------------------------------
                tags = []
                
                # 1. 생애주기 채점
                if mfe >= 7.0 and r['Bars_Held'] <= 8: tags.append("#빠른슈팅_완벽")
                elif mfe >= 7.0 and r['Bars_Held'] > 8: tags.append("#지연슈팅_수명연장")
                elif mfe < 3.0: tags.append("#슈팅실패_조기소멸")
                
                # 2. 거래량 프로파일 채점
                vol_ratio = r['Up_Vol_Sum'] / (r['Down_Vol_Sum'] + 1)
                if vol_ratio >= 1.5: tags.append("#건전한조정_매집우위")
                elif vol_ratio < 0.8: tags.append("#음봉대량거래_세력이탈")
                
                # 3. RS 방어력 채점
                try:
                    idx_ret = ((idx_close.iloc[-1] - idx_close.loc[r['Entry_Date']:].iloc[0]) / idx_close.loc[r['Entry_Date']:].iloc[0]) * 100
                    if ret > float(idx_ret): tags.append("#RS_방어성공")
                    else: tags.append("#주도력_상실")
                except: pass

                r['Exit_Date'] = datetime.now().strftime('%Y-%m-%d')
                r['Final_Ret'] = round(ret, 2)
                r['MFE'] = round(mfe, 2)
                r['Exit_Reason'] = exit_rsn
                r['Flow_Tags'] = " ".join(tags)
                
                completed_list.append(r)
            else:
                keep_list.append(r)
                
        except Exception as e:
            keep_list.append(r)

    # 장부 업데이트
    pd.DataFrame(keep_list).to_csv(active_file, index=False, encoding='utf-8-sig')
    
    if completed_list:
        comp_df = pd.DataFrame(completed_list)
        if os.path.exists(comp_file):
            comp_df.to_csv(comp_file, mode='a', header=False, index=False, encoding='utf-8-sig')
        else:
            comp_df.to_csv(comp_file, index=False, encoding='utf-8-sig')

        # 텔레그램 브리핑
        msg = f"🤖 [{market}] 포워드 테스팅 청산 브리핑\n\n"
        for _, r in comp_df.iterrows():
            icon = "🔥익절" if r['Final_Ret'] > 0 else "💀손절"
            msg += f"{icon} {r['Name']} ({r['Score']}점 / {r['Tier']})\n"
            msg += f"▪️ 최종수익: {r['Final_Ret']}%\n"
            msg += f"▪️ 흐름태그: {r['Flow_Tags']}\n"
            msg += f"▪️ 사유: {r['Exit_Reason']} ({r['Bars_Held']}일)\n\n"
            
        send_telegram_msg(msg)
# ==========================================
# 3. 매일 16:00 일일 종합 리포트 텔레그램 발송 엔진
# ==========================================
def send_daily_summary_report():
    tz_kr = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(tz_kr).strftime('%Y-%m-%d')
    
    report_msg = f"📊 [포워드 테스팅 일일 종합 리포트]\n📅 {today_str} 장 마감 기준\n\n"

    for market in ['KR', 'US']:
        active_file = DB_FILES[market]['active']
        comp_file = DB_FILES[market]['completed']
        
        # 1. 누적 승률 계산
        total_trades, win_rate = 0, 0.0
        today_closed = pd.DataFrame()
        if os.path.exists(comp_file):
            comp_df = pd.read_csv(comp_file)
            total_trades = len(comp_df)
            if total_trades > 0:
                wins = len(comp_df[comp_df['Final_Ret'] > 0])
                win_rate = round((wins / total_trades) * 100, 1)
            # 오늘 청산된 종목만 필터링
            today_closed = comp_df[comp_df['Exit_Date'] == today_str]

        report_msg += f"📈 [{market}장 누적 실전 승률]: {win_rate}% (총 {total_trades}건 검증완료)\n"
        
        # 2. 오늘 청산된 종목 브리핑 (흐름 태그 포함)
        if not today_closed.empty:
            report_msg += f"✅ [오늘의 청산 내역]\n"
            for _, r in today_closed.iterrows():
                icon = "🔥익절" if r['Final_Ret'] > 0 else "💀손절"
                report_msg += f"{icon} {r['Name']} ({r['Score']}점)\n"
                report_msg += f" ↳ 수익률: {r['Final_Ret']}% (최대 {r['MFE']}%)\n"
                report_msg += f" ↳ 팩트체크: {r['Flow_Tags']}\n"
        else:
            report_msg += "✅ [오늘의 청산 내역] 없음\n"

        # 3. 현재 보유(추적) 중인 포트폴리오 쿼터 현황
        tier_counts = {'70~100점대': 0, '40~70점대': 0, '10~30점대': 0}
        total_active = 0
        if os.path.exists(active_file):
            act_df = pd.read_csv(active_file)
            total_active = len(act_df)
            if not act_df.empty:
                counts = act_df['Tier'].value_counts().to_dict()
                for k in tier_counts.keys():
                    tier_counts[k] = counts.get(k, 0)
        
        report_msg += f"📦 [현재 추적 중인 포트폴리오: 총 {total_active}종목]\n"
        report_msg += f" - 70~100점 쿼터: {tier_counts['70~100점대']}/30개\n"
        report_msg += f" - 40~69점 쿼터: {tier_counts['40~70점대']}/15개\n"
        report_msg += f" - 10~39점 쿼터: {tier_counts['10~30점대']}/15개\n\n"
        report_msg += "━"*25 + "\n\n"

    report_msg += "💡 (자세한 세부 지표 수치는 서버의 CSV 파일에 안전하게 누적 기록 중입니다.)"
    
    send_telegram_msg(report_msg)
    print(f"✅ 16:00 일일 종합 리포트 텔레그램 발송 완료.")
