import sys
import threading
import time
import os
import urllib.request
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
from datetime import datetime
import pytz
import functools

# 💡 터미널 한글 & 이모지 깨짐 방지
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# 🕵️ 스마트 에러 추적기 (속도 지장 0%)
# ==========================================
class SmartErrorTracker:
    def __init__(self):
        self.errors = []
        self.lock = threading.Lock()
        self.original_stderr = sys.stderr

    def write(self, text):
        self.original_stderr.write(text) 
        
        if text.strip() and ("Exception" in text or "Traceback" in text or "Error" in text):
            with self.lock:
                now = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%H:%M:%S')
                short_err = text.strip().split('\n')[-1][:100]
                self.errors.append(f"[{now}] {short_err}")

    def flush(self):
        self.original_stderr.flush()

    def get_and_clear(self):
        with self.lock:
            errs = list(self.errors)
            self.errors.clear()
            return errs

error_tracker = SmartErrorTracker()
sys.stderr = error_tracker 

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

# ==========================================
# 로직 파일 임포트
# ==========================================
import nasdaq_all_ema224_signal_screener as us_ema
import nasdaq_dante_reverse_breakout_screener as us_rev
import nulusa as us_nul
import usa as us_bowl
import dante_krx_reverse_breakout_screener as kr_rev
import master as kr_master
import kr as kr_bowl
import nulrim as kr_nul
import ohdole as kr_ohdole

# ==========================================
# 🛑 주말 자동 휴장 스마트 차단기
# ==========================================
def skip_weekend_kr(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        kr_tz = pytz.timezone('Asia/Seoul')
        if datetime.now(kr_tz).weekday() in [5, 6]:
            print(f"💤 [🇰🇷 한국장 주말 휴장] 스캔을 건너뜁니다.")
            return
        return func(*args, **kwargs)
    return wrapper

def skip_weekend_us(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ny_tz = pytz.timezone('America/New_York')
        if datetime.now(ny_tz).weekday() in [5, 6]:
            print(f"💤 [🇺🇸 미국장 주말 휴장] 스캔을 건너뜁니다.")
            return
        return func(*args, **kwargs)
    return wrapper

# ⭐️ 어떤 함수 이름이든 알아서 찾아서 패치하는 무적 로직
def apply_weekend_patch(module, is_us):
    patcher = skip_weekend_us if is_us else skip_weekend_kr
    if hasattr(module, 'scan_market_1d'):
        module.scan_market_1d = patcher(module.scan_market_1d)
    elif hasattr(module, 'scan_market'):
        module.scan_market = patcher(module.scan_market)

apply_weekend_patch(us_ema, True)
apply_weekend_patch(us_rev, True)
apply_weekend_patch(us_nul, True)
apply_weekend_patch(us_bowl, True)

apply_weekend_patch(kr_rev, False)
apply_weekend_patch(kr_master, False)
apply_weekend_patch(kr_bowl, False)
apply_weekend_patch(kr_nul, False)
apply_weekend_patch(kr_ohdole, False)

# ==========================================
# 📊 실시간 생존 확인 및 종합 보고서
# ==========================================
def status_monitor(threads_dict):
    seoul_tz = pytz.timezone('Asia/Seoul')
    ny_tz = pytz.timezone('America/New_York')

    print("\n📡 [스마트 관제탑] 실시간 모니터링 및 자동 에러 감지 시스템 가동!\n")

    while True:
        now_kr = datetime.now(seoul_tz)
        now_ny = datetime.now(ny_tz)

        if now_kr.minute == 59 and now_kr.second >= 50:
            print("\n" + "━"*65)
            print(f"📊 [관제탑 1시간 종합 보고서] 🇰🇷 {now_kr.strftime('%H:%M')} 기준 마감")
            print("━"*65)

            dead_bots = []
            for name, t in threads_dict.items():
                if not t.is_alive():
                    dead_bots.append(name)

            if not dead_bots:
                print("🟢 [스레드 상태] 9개 검색기 모두 100% 정상 작동 중! (사망 없음)")
            else:
                print(f"🔴 [스레드 상태] 🚨 경고! 사망한 봇 발견: {', '.join(dead_bots)}")

            recent_errors = error_tracker.get_and_clear()
            if not recent_errors:
                print("🟢 [시스템 건강] 지난 1시간 동안 충돌이나 에러 없이 완벽하게 스캔했습니다.")
            else:
                print(f"🟡 [시스템 건강] 지난 1시간 동안 {len(recent_errors)}건의 경미한 에러/지연이 있었습니다:")
                for err in recent_errors[-5:]: 
                    print(f"   ↳ {err}")

            print("━"*65 + "\n")
            time.sleep(15) 

        elif now_kr.minute % 10 == 0 and now_kr.second < 2:
            print(f"📡 [관제탑 핑] 🇰🇷 {now_kr.strftime('%H:%M:%S')} | 🇺🇸 {now_ny.strftime('%H:%M:%S')} (모든 시스템 감시 중...)")
            time.sleep(2)

        time.sleep(1)

# ==========================================
# 🚀 메인 실행부
# ==========================================
if __name__ == "__main__":
    print("🚀 24시간 독립 멀티스레딩 컨트롤 타워 가동 시작...")

    bot_targets = {
        "🇺🇸 1. US EMA": us_ema.run_scheduler,
        "🇺🇸 2. US 역매공파": us_rev.run_scheduler,
        "🇺🇸 3. US 눌림목": us_nul.run_scheduler,
        "🇺🇸 4. US 밥그릇": us_bowl.run_scheduler,
        "🇰🇷 5. KR 역매공파": kr_rev.run_scheduler,
        "🇰🇷 6. KR 마스터": kr_master.run_scheduler,
        "🇰🇷 7. KR 밥그릇": kr_bowl.run_scheduler,
        "🇰🇷 8. KR 눌림목": kr_nul.run_scheduler,
        "🇰🇷 9. KR 오돌이": kr_ohdole.run_scheduler
    }

    active_threads = {}

    for name, target_func in bot_targets.items():
        t = threading.Thread(target=target_func, daemon=True, name=name)
        t.start()
        active_threads[name] = t
        time.sleep(1)

    monitor_thread = threading.Thread(target=status_monitor, args=(active_threads,), daemon=True, name="관제탑")
    monitor_thread.start()

    for t in active_threads.values():
        t.join()
