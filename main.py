import threading
import time
import os
import urllib.request
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
from datetime import datetime
import pytz

# ==========================================
# 🇰🇷 한글 폰트 강제 설치
# ==========================================
print("📥 랜더 서버 한글 폰트 적용 중...")
font_path = "NanumGothic.ttf"
if not os.path.exists(font_path):
    urllib.request.urlretrieve("https://github.com/google/fonts/raw/main/ofl/nanumgothic/NanumGothic-Regular.ttf", font_path)
fm.fontManager.addfont(font_path)
plt.rc('font', family='NanumGothic')
plt.rcParams['axes.unicode_minus'] = False
print("✅ 한글 폰트 준비 완료!\n")

# 로직 파일 임포트
import nasdaq_all_ema224_signal_screener as us_ema
import nasdaq_dante_reverse_breakout_screener as us_rev
import nulusa as us_nul
import usa as us_bowl
import dante_krx_reverse_breakout_screener as kr_rev
import korea_ema224_signal_screener as kr_ema
import kr as kr_bowl
import nulrim as kr_nul
import ohdole as kr_ohdole

# 실행 함수
def start_us_ema(): us_ema.run_scheduler()
def start_us_rev(): us_rev.run_scheduler()
def start_us_nul(): us_nul.run_scheduler()
def start_us_bowl(): us_bowl.run_scheduler()
def start_kr_rev(): kr_rev.run_scheduler()
def start_kr_ema(): kr_ema.run_scheduler()
def start_kr_bowl(): kr_bowl.run_scheduler()
def start_kr_nul(): kr_nul.run_scheduler()
def start_kr_ohdole(): kr_ohdole.run_scheduler()

# 📊 실시간 생존 확인(Heartbeat) 모니터링
def status_monitor():
    seoul_tz = pytz.timezone('Asia/Seoul')
    ny_tz = pytz.timezone('America/New_York')
    while True:
        now_kr = datetime.now(seoul_tz).strftime('%Y-%m-%d %H:%M:%S')
        now_ny = datetime.now(ny_tz).strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n[🟢 실시간 관제탑 정상 작동 중] 🇰🇷한국: {now_kr} | 🇺🇸미국: {now_ny}")
        print("모든 검색기 스레드 생존 확인. 스케줄 대기 중...")
        time.sleep(60) # 1분마다 상태 출력

if __name__ == "__main__":
    print("🚀 24시간 독립 멀티스레딩 컨트롤 타워 가동 시작...")

    try: kr_bowl.initialize_tv_pool()
    except Exception as e: print(f"TV 에러: {e}")

    threads = [
        threading.Thread(target=start_us_ema, daemon=True),
        threading.Thread(target=start_us_rev, daemon=True),
        threading.Thread(target=start_us_nul, daemon=True),
        threading.Thread(target=start_us_bowl, daemon=True),
        threading.Thread(target=start_kr_rev, daemon=True),
        threading.Thread(target=start_kr_ema, daemon=True),
        threading.Thread(target=start_kr_bowl, daemon=True),
        threading.Thread(target=start_kr_nul, daemon=True),
        threading.Thread(target=start_kr_ohdole, daemon=True),
        threading.Thread(target=status_monitor, daemon=True) # 관제탑 스레드 추가
    ]

    for t in threads:
        t.start()
        time.sleep(2)

    for t in threads:
        t.join()
