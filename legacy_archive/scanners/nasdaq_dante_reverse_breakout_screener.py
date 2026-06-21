# Dante_US_Reverse_Breakout_1D_AI_Pro.py
import os, re, time, threading, concurrent.futures
from datetime import datetime, timedelta
import pytz
import numpy as np, pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import requests
import warnings, urllib3
import logging

# 리포트: gemini_report_cache 파사드 (import 시 google.generativeai 비로드)
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)

# 💡 1. 듀얼 텔레그램 봇 세팅 (본캐용 / 홍보용 분리) — .env → telegram_env
import telegram_env

TELEGRAM_TOKEN_MAIN = telegram_env.get_equity_us_main_token()
TELEGRAM_TOKEN_PROMO = telegram_env.get_equity_us_promo_token()
TELEGRAM_CHAT_ID = telegram_env.get_equity_us_factory_chat_id()
SEND_TELEGRAM = bool(TELEGRAM_TOKEN_MAIN and TELEGRAM_CHAT_ID)

from telegram_message_queue import (
    enqueue_telegram,
    start_telegram_queue_daemons,
    wait_telegram_queue_drained,
)

start_telegram_queue_daemons(
    TELEGRAM_TOKEN_MAIN,
    TELEGRAM_TOKEN_PROMO or TELEGRAM_TOKEN_MAIN,
    TELEGRAM_CHAT_ID,
    SEND_TELEGRAM,
)

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_US_P_1D')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 120
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9._-]', '_', s)

# 💡 2. 본캐 팩트 리포트 (해시태그 파싱 오류 제거) — Gemini 캐시·다중키: gemini_report_cache
def generate_ai_report(code: str, company_name: str):
    from gemini_report_cache import get_report_provider

    return get_report_provider().generate("stock", code=code, company_name=company_name)

def get_us_ticker_list():
    try:
        df = pd.concat([fdr.StockListing('NASDAQ'), fdr.StockListing('NYSE'), fdr.StockListing('AMEX')])
        df = df[df['Symbol'].str.isalpha()] 
        df['Symbol'] = df['Symbol'].str.replace('.', '-', regex=False)
        return df[['Symbol', 'Name']].drop_duplicates(subset=['Symbol']).dropna()
    except: return pd.DataFrame()

def calculate_trust_score(c, e60, signal_arr):
    score = 5 
    lowest_60 = np.min(c[-60:])
    runup_ratio = (c[-1] / lowest_60) - 1
    if runup_ratio > 0.50: score -= 4     
    elif runup_ratio > 0.30: score -= 2   
    lookback = min(100, len(c))
    for i in range(len(c) - lookback, len(c) - 1):
        if signal_arr[i]:
            valid = True
            entry_price = c[i]
            for j in range(i + 1, len(c)):
                if c[j] < e60[j] or c[j] >= entry_price * 1.15:
                    valid = False
                    break
            if valid: score += 2 
    return max(1, min(10, score)) 

def compute_inverse_1d(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    df = df.loc[:, ~df.columns.duplicated()].copy()
    _cl = np.squeeze(np.asarray(df['Close']))
    if getattr(_cl, 'ndim', 0) != 1:
        _cl = np.ravel(_cl)
    df['Close'] = pd.Series(_cl, index=df.index)

    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    df['AvgVol3'] = df['Volume'].shift(1).rolling(3, min_periods=1).mean()
    
    c = np.squeeze(np.asarray(df['Close']))
    o = np.squeeze(np.asarray(df['Open']))
    h = np.squeeze(np.asarray(df['High']))
    l = np.squeeze(np.asarray(df['Low']))
    v = np.squeeze(np.asarray(df['Volume']))
    av3 = df['AvgVol3'].values
    ema60, ema112, ema224, ema448 = df['EMA60'].values, df['EMA112'].values, df['EMA224'].values, df['EMA448'].values

    # 💡 미국장은 5_000_000 / 3.0 으로 완벽 유지
    moneyOk = (c * v) >= 5_000_000 
    priceOk = c >= 3.0
    
    condBearAlign = (ema112 < ema224) & (ema224 < ema448)
    condHold112 = c > ema112

    condCrossEvent = np.zeros(len(c), dtype=bool)
    for i in range(1, 9):
        shifted_c = np.roll(c, i)
        shifted_c[:i] = np.inf 
        shifted_ema112 = np.roll(ema112, i)
        condCrossEvent |= (shifted_c < shifted_ema112)

    isAccBull = c > o
    rng = h - l
    with np.errstate(divide='ignore', invalid='ignore'):
        closePos = np.where(rng > 0, (c - l) / rng, 0)
        
    valMa20 = pd.Series(c*v).rolling(20, min_periods=1).mean().values
    isAccCandle = isAccBull & ((c*v) >= (1.6 * valMa20)) & (closePos >= 0.68)
    condHasAcc = pd.Series(isAccCandle).rolling(window=20, min_periods=1).sum().values > 0

    with np.errstate(invalid='ignore'):
        condVolSpike = v >= (np.nan_to_num(av3, nan=1.0) * 3)

    signalBase = priceOk & moneyOk & condBearAlign & condHold112 & condCrossEvent & condHasAcc & condVolSpike & (c > o)
    if not signalBase[-1]: return False, "", df, {}

    condBullAlign = (ema112 > ema224) & (ema224 > ema448)
    
    # ⭐️ 15% 상승 실패 시 누적 로직 보존 ⭐️
    p_counts = np.zeros(len(c), dtype=int)
    current_p_count = 0
    wait_idx = -1

    for i in range(len(c)):
        if condBullAlign[i]: 
            current_p_count = 0
            wait_idx = -1

        if wait_idx != -1:
            if i <= wait_idx + 3:
                if h[i] >= c[wait_idx] * 1.15: 
                    current_p_count = 0
                    wait_idx = -1
            if i == wait_idx + 3 and wait_idx != -1:
                wait_idx = -1

        if signalBase[i]:
            current_p_count += 1
            wait_idx = i
            
        p_counts[i] = current_p_count

    sig_type = "P (연속)" if p_counts[-1] > 1 else "P (신규)"
    trust_score = calculate_trust_score(c, ema60, signalBase)
    
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "p_count": int(p_counts[-1])}

# 💡 매일 로테이션되는 5가지 프리미엄 차트 테마
def get_daily_theme():
    theme_idx = datetime.now().day % 5
    themes = [
        {'bg': '#0B0E14', 'grid': '#1A202C', 'text': '#FFFFFF', 'up': '#F6465D', 'down': '#0ECB81'}, # 0: Binance Premium
        {'bg': '#FFFFFF', 'grid': '#F0F0F0', 'text': '#131722', 'up': '#E0294A', 'down': '#2EBD85'}, # 1: Institutional White
        {'bg': '#131722', 'grid': '#2A2E39', 'text': '#D1D4DC', 'up': '#26A69A', 'down': '#EF5350'}, # 2: TradingView Classic
        {'bg': '#000000', 'grid': '#111111', 'text': '#00FFA3', 'up': '#00FFA3', 'down': '#FF3366'}, # 3: Cyberpunk Terminal
        {'bg': '#F8F9FA', 'grid': '#E9ECEF', 'text': '#212529', 'up': '#FF4757', 'down': '#2ED573'}  # 4: Modern Light
    ]
    return themes[theme_idx]

chart_lock = threading.Lock()
def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, show_volume=False, is_promo=False) -> str:
    with chart_lock:
        try:
            plt.rcParams['font.family'] = 'NanumGothic'
            plt.rcParams['axes.unicode_minus'] = False
            
            timestamp_ms = int(time.time() * 1000)
            vol_suffix = "promo" if is_promo else ("wVol" if show_volume else "noVol")
            path = os.path.join(CHART_FOLDER, f"{rank:03d}_{sanitize_filename(code)}_{timestamp_ms}_{vol_suffix}.png")
            
            df_cut = df.iloc[-DISPLAY_BARS:].copy()
            df_cut.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)
            if df_cut.empty or len(df_cut) < 5: return None

            c, o, h, l = df_cut['Close'].iloc[-1], df_cut['Open'].iloc[-1], df_cut['High'].iloc[-1], df_cut['Low'].iloc[-1]
            v = int(df_cut['Volume'].iloc[-1])
            prev_c = df_cut['Close'].iloc[-2] if len(df_cut) > 1 else c
            diff = c - prev_c
            diff_pct = (diff / prev_c) * 100 if prev_c != 0 else 0
            
            sign = "▲" if diff > 0 else ("▼" if diff < 0 else "-")
            
            # 💡 홍보용 vs 본캐용 분기
            if is_promo:
                theme = get_daily_theme()
                bg_color, grid_color, text_main = theme['bg'], theme['grid'], theme['text']
                color_up, color_down = theme['up'], theme['down']
                text_sub = text_main
                custom_figsize = (9, 9) 
            else:
                bg_color, grid_color, text_main, text_sub = '#131722', '#2A2E39', '#FFFFFF', '#8A91A5'
                color_up, color_down = '#FF3B69', '#00B4D8'
                custom_figsize = (11, 6.5) if show_volume else (9, 9)
            
            color_diff = color_up if diff > 0 else (color_down if diff < 0 else text_sub)

            signal_marker = pd.Series(np.nan, index=df_cut.index)
            y_offset = (df_cut['High'].max() - df_cut['Low'].min()) * 0.04 
            signal_marker.iloc[-1] = df_cut['Low'].iloc[-1] - y_offset
            ap = mpf.make_addplot(signal_marker, type='scatter', markersize=400 if is_promo else 300, marker='^', color='#FFD700', alpha=1.0)

            mc = mpf.make_marketcolors(up=color_up, down=color_down, edge='inherit', wick='inherit', volume='inherit')
            s = mpf.make_mpf_style(marketcolors=mc, facecolor=bg_color, edgecolor=bg_color, figcolor=bg_color, gridcolor=grid_color, gridstyle='--', y_on_right=True, rc={'font.family': plt.rcParams['font.family'], 'text.color': text_main, 'axes.labelcolor': text_sub, 'xtick.color': text_sub, 'ytick.color': text_sub})
            
            plt.close('all')
            fig, axes = mpf.plot(df_cut, type="candle", volume=show_volume, addplot=ap, style=s, figsize=custom_figsize, tight_layout=False, returnfig=True)

            title_y, sub_y = (0.94, 0.90) if not show_volume or is_promo else (0.93, 0.88)
            fig.subplots_adjust(top=0.85, bottom=0.1, left=0.05, right=0.92)
            
            fig.text(0.05, title_y, f"{code} | {name}", fontsize=24 if is_promo else 22, fontweight='bold', color=text_main, ha='left')
            
            right_text1 = f"{sign} {abs(diff_pct):.2f}%" if is_promo else f"Close: ${c:,.2f} ({sign} ${abs(diff):,.2f}, {sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, title_y, right_text1, fontsize=22 if is_promo else 18, fontweight='bold', color=color_diff, ha='right')

            if not is_promo:
                right_text2 = f"Vol: {v:,}  | O: ${o:,.2f}  H: ${h:,.2f}  L: ${l:,.2f}"
                fig.text(0.95, sub_y, right_text2, fontsize=12, color=text_sub, ha='right')
                
            fig.text(0.05, 0.03, "Proprietary Algorithmic Signal", fontsize=10, color=text_sub, ha='left', style='italic')

            fig.savefig(path, dpi=250 if is_promo else 200, bbox_inches='tight', facecolor=bg_color)
            plt.close(fig)
            return path
        except Exception as e:
            print(f"\n❌ [{name}] 차트 에러: {e}")
            return None

def scan_market_1d():
    stock_list = get_us_ticker_list()
    if stock_list.empty: return
    t0 = time.time()
    print(f"\n🇺🇸 [일봉 전용] 미국장 2번(역매공파) 스캔 시작! (안정화 패치 완료)")
    
    # 💡 당일 중복 발송 차단 로직
    ny_tz = pytz.timezone('America/New_York')
    today_str = datetime.now(ny_tz).strftime('%Y-%m-%d')
    log_file = os.path.join(TOP_FOLDER, "sent_log_us_p.txt")
    
    sent_today = set()
    if os.path.exists(log_file):
        try:
            with open(log_file, "r") as f:
                lines = f.read().splitlines()
                if lines and lines[0] == today_str:
                    sent_today = set(lines[1:])
        except Exception as e:
            logger.error(f"비치명적 에러 발생: {e}", exc_info=True)

    ticker_to_info = {row['Symbol']: {'code': row['Symbol'], 'name': row['Name']} for _, row in stock_list.iterrows()}
    tickers = list(ticker_to_info.keys())
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0, 'fetch_failed': 0}
    start_date = (datetime.now(ny_tz) - timedelta(days=3 * 365)).strftime('%Y-%m-%d')
    end_date = datetime.now(ny_tz).strftime('%Y-%m-%d')

    from market_data_fetcher import fetch_market_data

    def _fetch_one_us(tk: str):
        return fetch_market_data(tk, "US", start_date, end_date)

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
        fetched = list(ex.map(_fetch_one_us, tickers))

    for tk, df_ticker in zip(tickers, fetched):
        tracker['scanned'] += 1
        info = ticker_to_info.get(tk)
        if not info:
            continue
        name, code = info['name'], info['code']

        try:
            if df_ticker is None or df_ticker.empty:
                tracker['fetch_failed'] += 1
                continue

            df_ticker = df_ticker.loc[:, ~df_ticker.columns.duplicated()].copy()
            df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
            df_ticker = df_ticker[~df_ticker.index.duplicated(keep='last')]

            # 거래정지·단일가(Static Quote) — 최근 3일 동일 종가 + 거래량 극소 시 매집 착시 방지 (미국장)
            if df_ticker is not None and not df_ticker.empty and len(df_ticker) >= 3:
                try:
                    tail_3 = df_ticker.tail(3)
                    if "Close" in tail_3.columns and "Volume" in tail_3.columns:
                        t3 = tail_3[["Close", "Volume"]].dropna()
                        if len(t3) >= 3 and int(t3["Close"].nunique()) == 1 and float(t3["Volume"].sum()) < 50000:
                            continue
                except Exception:
                    pass

            if len(df_ticker) >= 500:
                tracker['analyzed'] += 1
                hit, sig_type, df, dbg = compute_inverse_1d(df_ticker)
                    
                if hit:
                    if code in sent_today:
                        hit = False 
                    else:
                        tracker['hits'] += 1
                        hit_rank = tracker['hits']
                        sent_today.add(code) 
                        try:
                            with open(log_file, "w") as f:
                                f.write(today_str + "\n")
                                for s_code in sent_today: f.write(s_code + "\n")
                        except Exception as e:
                            logger.error(f"비치명적 에러 발생: {e}", exc_info=True)
                            
                if hit:
                    # 💡 본캐용 및 홍보용 차트 생성
                    main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True, is_promo=False)
                    threads_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False, is_promo=True)
                        
                    if main_chart_path and threads_chart_path:
                        ai_main, _ = generate_ai_report(code, name)
                        try:
                            sector_info = ai_main.split('\n')[0].replace('1. 섹터:', '').strip()
                        except Exception:
                            sector_info = "유망 섹터 포착"
                            
                        # 1️⃣ 본캐용 캡션 (유료방용 - 기존 멘트 단 한 줄도 건드리지 않음)
                        main_caption = (
                            f"🎯 [{dbg.get('sig_type', '')}]\n"
                            f"🎯 추천: {dbg.get('recommend', '스윙, 중장기 / 종가배팅')}\n\n"
                            f"🏢 {name} ({code})\n"
                            f"💰 현재가: ${dbg.get('last_close', 0):,.2f}\n\n"
                            f"📉 [매수/손절 전략]\n"
                            f"- 양봉 길이만큼 분할매수\n"
                            f"- 마지막 분할매수에서 -5% 손절 or 진입 양봉 시가 이탈시 손절\n\n"
                            f"⭐ 알고리즘 신뢰도: {dbg.get('score', 10)} / 10점\n\n"
                            f"💡 [AI 비즈니스 요약]\n"
                            f"{ai_main}\n\n"
                            f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요.\n\n"
                            f"⚠️ [면책 조항]\n"
                            f"본 정보는 알고리즘에 의한 기술적 분석일 뿐, 특정 종목에 대한 매수/매도 권유가 아닙니다. 투자의 최종 판단과 책임은 투자자 본인에게 있습니다."
                        )
                        enqueue_telegram(
                            "MAIN",
                            main_chart_path,
                            main_caption,
                            enabled=SEND_TELEGRAM,
                        )

                        try:
                            import auto_forward_tester as aft
                            entry_facts = {
                                'v_rs': 0, 'v_cpv': 0, 'v_yang': 0, 'v_energy': 0,
                                'marcap_eok': 0, 'score_marcap': 0, 'freq_count': 0,
                                'dyn_rs': 0, 'dyn_cpv': 0, 'dyn_tb': 0,
                                'is_tenbagger': 0, 'is_top_dna': 0, 'is_worst_dna': 0, 'is_death_combo': 0
                            }
                            scaled_score = float(dbg.get('score', 0) or 0) * 10
                            ep_us = float(dbg.get('last_close', 0) or 0)
                            success, fwd_msg = aft.try_add_virtual_position(
                                market='US', code=code, name=name,
                                sig_type=f"[STANDARD] {dbg.get('sig_type', 'NASDAQ_INVERSE')}",
                                ep=ep_us, facts=entry_facts, sector=sector_info,
                                score=scaled_score,
                                trade_source="STANDARD"
                            )
                            print(f"   ↳ [미국장 오리지널 장부 기록]: {fwd_msg}")
                        except Exception as e:
                            print(f"   ↳ [포워드 장부 에러]: {e}")

                        # 2️⃣ 홍보용 캡션 (쓸데없는 멘트 다 빼고 초심플 압축)
                        # ⭐️ 멘트 싹 날리고 [차트+종목+섹터+현재가]만! (미국장이므로 $ 유지)
                        promo_caption = (
                            f"📈 [알고리즘 차트 포착]\n\n"
                            f"🏢 종목: {name} ({code})\n"
                            f"🏷️ 섹터: {sector_info}\n"
                            f"💰 현재가: ${dbg.get('last_close', 0):,.2f}"
                        )
                        enqueue_telegram(
                            "PROMO",
                            threads_chart_path,
                            promo_caption,
                            enabled=SEND_TELEGRAM,
                        )

                        print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 (총 2개) 전송 대기열 추가 완료!")
            else:
                tracker['fetch_failed'] += 1
        except Exception as e:
            pass
                
        if tracker['scanned'] % 500 == 0 or tracker['scanned'] == len(tickers):
            print(f"   진행중... {tracker['scanned']}/{len(tickers)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")

    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        wait_telegram_queue_drained(("MAIN", "PROMO"), timeout_sec=7200.0)

    elapsed = time.time() - t0
    print(f"\n✅ [미국장 2번 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {elapsed/60:.1f}분\n")
    if SEND_TELEGRAM:
        if tracker["hits"] > 0:
            from telegram_html_delivery import post_telegram_message

            post_telegram_message(
                url=f"https://api.telegram.org/bot{TELEGRAM_TOKEN_MAIN}/sendMessage",
                chat_id=str(TELEGRAM_CHAT_ID),
                text=(
                    f"🇺🇸 <b>[US 역매공파]</b>\n"
                    f"스캔 완료 — <b>{tracker['hits']}건</b> 포착\n"
                    f"<i>분석 {tracker['analyzed']}종 · 소요 {elapsed/60:.1f}분</i>"
                ),
                parse_mode="HTML",
                timeout=15.0,
            )
        else:
            from scanner_funnel import notify_equity_scan_zero_hits

            notify_equity_scan_zero_hits(
                market="US",
                label="US 역매공파",
                scanned=int(tracker.get("scanned", 0)),
                analyzed=int(tracker.get("analyzed", 0)),
                elapsed_sec=elapsed,
                token_main=TELEGRAM_TOKEN_MAIN,
                chat_id=TELEGRAM_CHAT_ID,
                send_enabled=True,
            )

def run_scheduler():
    ny_tz = pytz.timezone('America/New_York')
    print("🕒 [2번 미국장 검색기] 10:00 / 12:00 / 16:00 대기 중...")
    while True:
        now_ny = datetime.now(ny_tz)
        if (now_ny.hour == 10 and now_ny.minute == 0) or (now_ny.hour == 12 and now_ny.minute == 0) or (now_ny.hour == 14 and now_ny.minute == 0):
            print(f"🚀 [2번 미국장 스캔 시작] {now_ny.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)
        try:
            import ops_logger
            ops_logger.record_heartbeat("scanner.nasdaq_reverse")
        except Exception:
            pass

if __name__ == "__main__":
    scan_market_1d()
