# Dante_KRX_Bowl_1D_AI_Pro.py
import os, re, time, threading, queue, concurrent.futures
from datetime import datetime, timedelta
import pytz
import numpy as np, pandas as pd
import mplfinance as mpf
import matplotlib; matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
import warnings, urllib3
from bs4 import BeautifulSoup
from io import StringIO
import FinanceDataReader as fdr
import matplotlib.font_manager as fm

from google import genai
from google.genai import types
from dotenv import load_dotenv

# ==========================================
# 🔑 .env 안전 파일 방식 적용
# ==========================================
load_dotenv() 
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("🚨 API 키를 찾을 수 없습니다! .env 파일을 확인해 주세요.")

client = genai.Client(api_key=GEMINI_API_KEY)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

TELEGRAM_TOKEN    = "7764404352:AAE9ZlpIPusEFd1qGk1VDWJE5cjtTogm4Pw"
TELEGRAM_CHAT_ID  = "6838834566"
SEND_TELEGRAM     = True
telegram_queue = queue.Queue()

# ⭐️ 추가할 2줄: 당일 중복 발송 방지용 전역 기억 장치 ⭐️
sent_today = set()
last_run_date = ""

TOP_FOLDER   = os.path.join(os.path.expanduser('~'), 'Desktop', 'Dante_Pro_System')
CHART_FOLDER = os.path.join(TOP_FOLDER, 'charts')
DISPLAY_BARS = 150
os.makedirs(CHART_FOLDER, exist_ok=True)

def sanitize_filename(s: str) -> str: return re.sub(r'[^A-Za-z0-9가-힣._-]', '_', s)

# ⭐️ AI 에러 원인 추적기 (last_error 버그 픽스) ⭐️
def generate_kr_ai_report(code: str, company_name: str) -> str:
    sector = "정보 없음"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    fn_summary, naver_summary = [], []

    try:
        res_naver = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers=headers, timeout=5, verify=False)
        if res_naver.status_code == 200:
            soup = BeautifulSoup(res_naver.text, 'html.parser')
            tag = soup.select_one('h4.h_sub.sub_tit7 a')
            if tag: sector = tag.text.strip()
            summary_tags = soup.select('.summary_info p')
            if summary_tags: naver_summary = [t.text.strip() for t in summary_tags if t.text.strip()]
    except: pass

    try:
        res_fn = requests.get(f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?gicode=A{code}", headers=headers, timeout=5, verify=False)
        if res_fn.status_code == 200:
            tags = BeautifulSoup(res_fn.text, 'html.parser').select('ul#bizSummaryContent > li')
            if tags: fn_summary = [t.text.strip() for t in tags if t.text.strip()]
    except: pass

    target_summary = fn_summary if fn_summary else naver_summary
    performance = "실적 데이터 일시적 수집 지연"
    outlook = "추가 전망 데이터가 요약본에 포함되지 않은 종목입니다."

    if len(target_summary) >= 2:
        performance = target_summary[1].replace("동사는", f"[{company_name}]은(는)")
    if len(target_summary) >= 3:
        outlook = target_summary[2].replace("동사는", f"[{company_name}]은(는)")
    elif len(target_summary) == 1:
        performance = target_summary[0].replace("동사는", f"[{company_name}]은(는)")

    return (
        f"💡 [기업 핵심 팩트]\n"
        f"📌 주요 섹터/테마: {sector}\n\n"
        f"📈 [최근 실적 및 비즈니스 현황]\n"
        f"✔️ {performance}\n\n"
        f"🔭 [향후 모멘텀 및 전망]\n"
        f"✔️ {outlook}"
    )

def get_krx_list_kind():
    try:
        df_ks = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt", verify=False, timeout=10).text), header=0)[0]
        df_ks['Market'] = 'KOSPI'
        df_kq = pd.read_html(StringIO(requests.get("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt", verify=False, timeout=10).text), header=0)[0]
        df_kq['Market'] = 'KOSDAQ'
        df = pd.concat([df_ks, df_kq])
        df['Code'] = df['종목코드'].astype(str).str.zfill(6)
        df = df.rename(columns={'회사명': 'Name'})
        return df[~df['Name'].str.contains('스팩|ETN|ETF|우$|홀딩스|리츠', regex=True)][['Code', 'Name', 'Market']].dropna()
    except: return pd.DataFrame()

def telegram_sender_daemon():
    while True:
        item = telegram_queue.get()
        if item is None: break
        img_path, caption = item
        
        safe_caption = caption[:1000] + "\n...(글자수 제한으로 요약됨)" if len(caption) > 1000 else caption
        
        if SEND_TELEGRAM:
            is_success = False
            for attempt in range(3):
                try:
                    with open(img_path, 'rb') as f:
                        res = requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto", 
                            params={"chat_id": TELEGRAM_CHAT_ID, "caption": safe_caption}, 
                            files={"photo": f}, 
                            timeout=60, 
                            verify=False
                        )
                    if res.status_code == 200: 
                        print(f"\n✅ 텔레그램 전송 성공: {img_path}")
                        is_success = True
                        break
                    elif res.status_code == 429: 
                        time.sleep(3)
                except requests.exceptions.ReadTimeout:
                    print(f"\n⚠️ 텔레그램 서버 응답 지연 (중복 방지를 위해 패스합니다.)")
                    break
                except: 
                    time.sleep(2)
            time.sleep(1.5)
        telegram_queue.task_done()

threading.Thread(target=telegram_sender_daemon, daemon=True).start()

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
                    valid = False; break
            if valid: score += 2 
    return max(1, min(10, score)) 

def compute_bobgeureut(df_raw: pd.DataFrame):
    if df_raw is None or len(df_raw) < 500: return False, "", df_raw, {}
    df = df_raw.copy()
    for n in [10, 20, 30, 60, 112, 224, 448]:
        df[f'EMA{n}'] = df['Close'].ewm(span=n, adjust=False, min_periods=0).mean()
    c, o, h, l, v = df['Close'].values, df['Open'].values, df['High'].values, df['Low'].values, df['Volume'].values
    
    ma20 = pd.Series(c).rolling(20, min_periods=1).mean().values
    stddev = pd.Series(c).rolling(20, min_periods=1).std(ddof=1).values
    bbUpper = ma20 + (stddev * 2)

    tenkan = (pd.Series(h).rolling(9, min_periods=1).max() + pd.Series(l).rolling(9, min_periods=1).min()) / 2
    kijun = (pd.Series(h).rolling(26, min_periods=1).max() + pd.Series(l).rolling(26, min_periods=1).min()) / 2
    spanA = (tenkan + kijun) / 2
    spanB = (pd.Series(h).rolling(52, min_periods=1).max() + pd.Series(l).rolling(52, min_periods=1).min()) / 2
    senkou1 = np.roll(spanA, 25); senkou1[:25] = np.nan
    senkou2 = np.roll(spanB, 25); senkou2[:25] = np.nan
    cloudTop = np.fmax(senkou1, senkou2)

    mean120 = pd.Series(c).rolling(120, min_periods=1).mean().values
    std120 = pd.Series(c).rolling(120, min_periods=1).std(ddof=1).values
    mean120_s = np.roll(mean120, 5); mean120_s[:5] = np.nan
    std120_s = np.roll(std120, 5); std120_s[:5] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'): condBox6m = (std120_s / mean120_s) < 0.20

    mean60 = pd.Series(c).rolling(60, min_periods=1).mean().values
    std60 = pd.Series(c).rolling(60, min_periods=1).std(ddof=1).values
    mean60_s = np.roll(mean60, 5); mean60_s[:5] = np.nan
    std60_s = np.roll(std60, 5); std60_s[:5] = np.nan
    with np.errstate(divide='ignore', invalid='ignore'): condBox3m = (std60_s / mean60_s) < 0.20

    isCat2 = condBox6m
    isCat1 = (~condBox6m) & condBox3m
    hasBox = isCat1 | isCat2

    ema10, ema20, ema30, ema60, ema112, ema224 = df['EMA10'].values, df['EMA20'].values, df['EMA30'].values, df['EMA60'].values, df['EMA112'].values, df['EMA224'].values
    condPrice = c >= 1000
    isBullish = c > o
    prev_c = np.roll(c, 1); prev_c[0] = np.inf
    prev_ema224 = np.roll(ema224, 1); prev_ema224[0] = 0
    condEma = (c > ema224) & (prev_c < prev_ema224 * 1.05)
    condCloud = (c > cloudTop) & (~np.isnan(cloudTop))
    condBb = c >= bbUpper * 0.98
    volAvg = pd.Series(v).rolling(20, min_periods=1).mean().values
    condVol = v > volAvg * 2.0
    condNotOverheated = c <= ema224 * 1.15
    
    v_1 = np.roll(v, 1); v_1[0] = 0
    v_2 = np.roll(v, 2); v_2[:2] = 0
    v_3 = np.roll(v, 3); v_3[:3] = 0
    avgVol3 = (v_1 + v_2 + v_3) / 3
    condVolSpike = v >= (avgVol3 * 5)

    signalBase = condPrice & isBullish & condEma & condCloud & condBb & condVol & condNotOverheated & hasBox & condVolSpike
    if not signalBase[-1]: return False, "", df, {}

    signalCat2 = signalBase & isCat2
    signalCat1 = signalBase & isCat1
    isAligned = (ema10 > ema20) & (ema20 > ema30) & (ema30 > ema60) & (ema60 > ema112) & (ema112 > ema224)

    if (signalCat2 & isAligned)[-1] or (signalCat1 & isAligned)[-1]: sig_type = "B (J 강조)"
    else: sig_type = "B (일반)"

    # ⭐️ 밥그릇 Cat2(바닥권) 타점 기준: 3봉 내 15% 상승 실패 시 누적, 성공 시 리셋 로직 ⭐️
    cat2_counts = np.zeros(len(c), dtype=int)
    current_cat2_count = 0
    wait_idx = -1

    for i in range(len(c)):
        if wait_idx != -1:
            # 타점 발생 후 3봉 이내에 고가가 15% 이상 상승했는지 체크
            if i <= wait_idx + 3:
                if h[i] >= c[wait_idx] * 1.15: # 15% 달성 시 리셋 (시세 분출 완료)
                    current_cat2_count = 0
                    wait_idx = -1
            # 3봉이 지났는데도 15% 도달을 못했으면 누적 유지 (세력의 가격 통제 및 매집 지속)
            if i == wait_idx + 3 and wait_idx != -1:
                wait_idx = -1

        # Cat2 타점 발생 시 카운트 올리고 대기열에 등록
        if signalCat2[i]:
            current_cat2_count += 1
            wait_idx = i
            
        cat2_counts[i] = current_cat2_count

    trust_score = calculate_trust_score(c, ema60, signalBase)

    # 💡 cat2_count(누적 횟수)를 텔레그램으로 넘겨줌
    return True, sig_type, df, {"sig_type": sig_type, "last_close": float(c[-1]), "score": trust_score, "cat2_count": int(cat2_counts[-1])}

chart_lock = threading.Lock()

def save_chart(df: pd.DataFrame, code: str, name: str, rank: int, dbg: dict, show_volume=False) -> str:
    with chart_lock:
        try:
            plt.rcParams['font.family'] = 'NanumGothic'
            plt.rcParams['axes.unicode_minus'] = False
            
            timestamp_ms = int(time.time() * 1000)
            vol_suffix = "wVol" if show_volume else "noVol"
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
            
            bg_color, grid_color, text_main, text_sub = '#131722', '#2A2E39', '#FFFFFF', '#8A91A5'
            color_up, color_down = '#FF3B69', '#00B4D8'
            color_diff = color_up if diff > 0 else (color_down if diff < 0 else text_sub)

            signal_marker = pd.Series(np.nan, index=df_cut.index)
            y_offset = (df_cut['High'].max() - df_cut['Low'].min()) * 0.04 
            signal_marker.iloc[-1] = df_cut['Low'].iloc[-1] - y_offset
            ap = mpf.make_addplot(signal_marker, type='scatter', markersize=300, marker='^', color='#FFD700', alpha=1.0)

            mc = mpf.make_marketcolors(up=color_up, down=color_down, edge='inherit', wick='inherit', volume='inherit')
            s = mpf.make_mpf_style(marketcolors=mc, facecolor=bg_color, edgecolor=bg_color, figcolor=bg_color, gridcolor=grid_color, gridstyle='--', y_on_right=True, rc={'font.family': plt.rcParams['font.family'], 'text.color': text_main, 'axes.labelcolor': text_sub, 'xtick.color': text_sub, 'ytick.color': text_sub})
            
            plt.close('all')
            fig, axes = mpf.plot(df_cut, type="candle", volume=show_volume, addplot=ap, style=s, figsize=(11, 6.5), tight_layout=False, returnfig=True)

            fig.subplots_adjust(top=0.85, bottom=0.1, left=0.05, right=0.92)
            fig.text(0.05, 0.93, f"{code} | {name}", fontsize=22, fontweight='bold', color=text_main, ha='left')
            fig.text(0.05, 0.88, "1D / KRX", fontsize=12, color=text_sub, ha='left')

            right_text1 = f"Close: {c:,.0f} ({sign} {abs(diff):,.0f}, {sign} {abs(diff_pct):.2f}%)"
            fig.text(0.95, 0.93, right_text1, fontsize=18, fontweight='bold', color=color_diff, ha='right')

            right_text2 = f"Vol: {v:,}  |  O: {o:,.0f}  H: {h:,.0f}  L: {l:,.0f}"
            fig.text(0.95, 0.88, right_text2, fontsize=12, color=text_sub, ha='right')
            fig.text(0.05, 0.03, "Proprietary Algorithmic Signal", fontsize=10, color=text_sub, ha='left', style='italic')

            fig.savefig(path, dpi=200, bbox_inches='tight', facecolor=bg_color)
            plt.close(fig)
            return path
        except Exception as e:
            print(f"\n❌ [{name}] 차트 에러: {e}")
            return None
def scan_market_1d():
    global sent_today, last_run_date
    kr_tz = pytz.timezone('Asia/Seoul')
    today_str = datetime.now(kr_tz).strftime('%Y-%m-%d')
    
    if today_str != last_run_date:
        sent_today.clear()
        last_run_date = today_str

    stock_list = get_krx_list_kind()
    if stock_list.empty: return

    print(f"\n⚡ [일봉 전용] 한국장 4번(밥그릇) 스캔 시작! (당일 중복 차단 🛡️)")
    t0 = time.time()
    tracker = {'scanned': 0, 'analyzed': 0, 'hits': 0}
    console_lock = threading.Lock()

    start_date = (datetime.now() - timedelta(days=3*365)).strftime('%Y-%m-%d')
    
    def worker(row_tuple):
        _, row = row_tuple
        name, code = row["Name"], row["Code"]
        df_raw = None
        is_valid = False
        hit, sig_type, df, dbg = False, "", None, {}
        
        try:
            df_raw = fdr.DataReader(code, start_date)
            if df_raw is not None and not df_raw.empty:
                df_raw = df_raw[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                
            is_valid = (df_raw is not None and not df_raw.empty and len(df_raw) >= 500)
            if is_valid: 
                hit, sig_type, df, dbg = compute_bobgeureut(df_raw)
        except Exception:
            pass

        hit_rank = 0
        with console_lock:
            tracker['scanned'] += 1
            if is_valid: tracker['analyzed'] += 1 
            if tracker['scanned'] % 100 == 0 or tracker['scanned'] == len(stock_list):
                print(f"   진행중... {tracker['scanned']}/{len(stock_list)} (정상분석: {tracker['analyzed']}개, 포착: {tracker['hits']}개)")
            if hit:
                tracker['hits'] += 1
                hit_rank = tracker['hits']
                
        if hit:
            main_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=True)
            if main_chart_path:
                ai_fact_check = generate_kr_ai_report(code, name)
                cat2_count = dbg.get('cat2_count', 0)
                
                if cat2_count >= 3:
                    intro_title = "🌟 [응축된 에너지의 폭발 임계점 도달]"
                    intro_desc = "바닥 구간에서 지속적인 자금 유입이 누적되며 에너지가 한계치까지 꽉 차오른 상태입니다. 조만간 방향성이 결정될 시 강한 시세 분출이 일어날 수 있는 폭발적 잠재력을 품고 있으므로, 지금부터는 아주 주의 깊게 흐름을 관찰해야 할 최적의 타이밍입니다."
                elif "J 강조" in dbg.get('sig_type', ""):
                    intro_title = "✨ [본격적인 가치 회복의 서막]"
                    intro_desc = "오랜 기다림 끝에 기업의 내재 가치가 빛을 발하기 시작하는 결정적 순간입니다. 시장의 흐름과 함께 안정적인 우상향을 기대하며 발걸음을 맞춰보세요."
                else:
                    intro_title = "🌱 [흙 속의 진주, 비상을 위한 준비]"
                    intro_desc = "당장의 화려함보다는 묵묵히 내실을 다져온 기업입니다. 조급한 매매보다는 '관심종목'에 조용히 담아두고, 기업의 진정한 가치가 시장에서 인정받는 과정을 여유롭게 지켜보시길 권해드립니다."

                main_caption = (
                    f"🏢 {name} ({code})\n"
                    f"💰 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
                    f"{intro_title}\n"
                    f"{intro_desc}\n\n"
                    f"⚖️ [건강한 매매를 위한 가이드]\n"
                    f"• 여유로운 접근: 현재가부터 천천히 모아가며 마음의 여유를 가지세요.\n"
                    f"• 원칙 대응: 약속된 지지라인(-5%) 이탈 시에는 기계적으로 대응하여 소중한 자산을 보호합니다.\n\n"
                    f"💡 [AI 비즈니스 요약]\n"
                    f"{ai_fact_check}\n\n"
                    f"💬 기업에 대해 더 깊이 알고 싶다면 채팅창에 '/질문 내용'을 입력해 보세요."
                )
                telegram_queue.put((main_chart_path, main_caption))

                threads_chart_path = save_chart(df, code, name, hit_rank, dbg, show_volume=False)
                if threads_chart_path:
                    threads_caption = (
                        f"🏢 종목명: {name} ({code})\n"
                        f"💰 현재가: {dbg.get('last_close', 0):,.0f}원\n\n"
                        f"💡 시장의 주목을 받기 전, 알고리즘에 포착된 차트 분석입니다. 투자의 참고 자료로 활용해 보세요!"
                    )
                    telegram_queue.put((threads_chart_path, threads_caption))
                
                print(f"\n✅ [{name}] 본캐 1개 + 홍보용 1개 전송 대기열 추가 완료 (바닥 매집 누적: {cat2_count}회)")

    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        list(executor.map(worker, list(stock_list.iterrows())))
        
    if tracker['hits'] > 0:
        print("\n⏳ 텔레그램 결과지 전송 중입니다. 잠시만 대기해 주세요...")
        telegram_queue.join()
        
    print(f"\n✅ [한국장 4번(밥그릇) 스캔 완료] 포착: {tracker['hits']}개 | 소요시간: {(time.time() - t0)/60:.1f}분\n")
    
def run_scheduler():
    kr_tz = pytz.timezone('Asia/Seoul')
    print("🕒 [4번 검색기] 10:30 / 13:00 대기 중...")
    while True:
        now_kr = datetime.now(kr_tz)
        if (now_kr.hour == 10 and now_kr.minute == 30) or (now_kr.hour == 13 and now_kr.minute == 0):
            print(f"🚀 [4번 스캔 시작] {now_kr.strftime('%Y-%m-%d %H:%M:%S')}")
            scan_market_1d()
            time.sleep(60) 
        else: time.sleep(10)

if __name__ == "__main__":
    run_scheduler()  # 💡 스케줄러를 잠시 끄고
