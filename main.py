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
# 🕵️ 스마트 에러 추적기
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
# 👑 퀀트 팩토리 9대 모듈 완벽 임포트
# ==========================================
try:
    import us_master as us_master
except ImportError as e:
    print(f"⚠️ us_master 임포트 실패(해당 봇만 비활성): {e}")
    us_master = None
try:
    import nasdaq_dante_reverse_breakout_screener as us_rev
except ImportError as e:
    print(f"⚠️ nasdaq_dante_reverse_breakout_screener 임포트 실패(해당 봇만 비활성): {e}")
    us_rev = None
try:
    import nulusa as us_nul
except ImportError as e:
    print(f"⚠️ nulusa 임포트 실패(해당 봇만 비활성): {e}")
    us_nul = None
try:
    import usa as us_bowl
except ImportError as e:
    print(f"⚠️ usa 임포트 실패(해당 봇만 비활성): {e}")
    us_bowl = None
try:
    import us_5ema as us_5ema
except ImportError as e:
    print(f"⚠️ us_5ema 임포트 실패(해당 봇만 비활성): {e}")
    us_5ema = None
try:
    import dante_krx_reverse_breakout_screener as kr_rev
except ImportError as e:
    print(f"⚠️ dante_krx_reverse_breakout_screener 임포트 실패(해당 봇만 비활성): {e}")
    kr_rev = None
try:
    import master as kr_master
except ImportError as e:
    print(f"⚠️ master 임포트 실패(해당 봇만 비활성): {e}")
    kr_master = None
try:
    import kr as kr_bowl
except ImportError as e:
    print(f"⚠️ kr 임포트 실패(해당 봇만 비활성): {e}")
    kr_bowl = None
try:
    import nulrim as kr_nul
except ImportError as e:
    print(f"⚠️ nulrim 임포트 실패(해당 봇만 비활성): {e}")
    kr_nul = None
try:
    import ema5 as kr_5ema
except ImportError as e:
    print(f"⚠️ ema5 임포트 실패(해당 봇만 비활성): {e}")
    kr_5ema = None

# 👇👇 [핵심 추가] 중앙 통제 시스템 코어 엔진 임포트
import data_updater
import auto_forward_tester
import system_auto_pilot
import supernova_hunter  # 💡 [추가] 4번째 코어 엔진 임포트!
import ai_overseer
import ai_secretary

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

def apply_weekend_patch(module, is_us):
    patcher = skip_weekend_us if is_us else skip_weekend_kr
    if hasattr(module, 'scan_market_1d'):
        module.scan_market_1d = patcher(module.scan_market_1d)
    elif hasattr(module, 'scan_market'):
        module.scan_market = patcher(module.scan_market)

if us_master is not None:
    apply_weekend_patch(us_master, True)
if us_rev is not None:
    apply_weekend_patch(us_rev, True)
if us_nul is not None:
    apply_weekend_patch(us_nul, True)
if us_bowl is not None:
    apply_weekend_patch(us_bowl, True)
if us_5ema is not None:
    apply_weekend_patch(us_5ema, True)

if kr_rev is not None:
    apply_weekend_patch(kr_rev, False)
if kr_master is not None:
    apply_weekend_patch(kr_master, False)
if kr_bowl is not None:
    apply_weekend_patch(kr_bowl, False)
if kr_nul is not None:
    apply_weekend_patch(kr_nul, False)
if kr_5ema is not None:
    apply_weekend_patch(kr_5ema, False)

# ==========================================
# 👑 [신규] 아침 07:00 DB 자동 업데이트 스케줄러
# ==========================================
def run_db_updater_scheduler():
    tz = pytz.timezone('Asia/Seoul')
    print("💠 [데이터 파이프라인] 매일 07:00 DB 갱신 대기 중...")
    while True:
        now = datetime.now(tz)
        # 매일 아침 7시 정각에 최신 차트 데이터 DB로 긁어오기
        if now.hour == 7 and now.minute == 0:
            print("🚀 [데이터 파이프라인] 아침 DB 자동 갱신 가동!")
            try: data_updater.run_daily_db_update()
            except Exception as e: print(f"DB 업데이트 에러: {e}")
            time.sleep(65)
        time.sleep(30)

# ==========================================
# 📊 실시간 생존 확인 및 종합 보고서 (모니터)
# ==========================================
def status_monitor(threads_dict):
    seoul_tz = pytz.timezone('Asia/Seoul')
    print("\n📡 [스마트 관제탑] 실시간 모니터링 시스템 가동!\n")

    while True:
        now_kr = datetime.now(seoul_tz)
        if now_kr.minute == 59 and now_kr.second >= 50:
            print("\n" + "━"*65)
            print(f"📊 [관제탑 1시간 종합 보고서] 🇰🇷 {now_kr.strftime('%H:%M')} 기준 마감")
            print("━"*65)

            dead_bots = [name for name, t in threads_dict.items() if not t.is_alive()]

            if not dead_bots:
                print("🟢 [스레드 상태] 퀀트 팩토리 코어 엔진 100% 정상 작동 중!")
            else:
                print(f"🔴 [스레드 상태] 🚨 경고! 사망한 봇 발견: {', '.join(dead_bots)}")

            recent_errors = error_tracker.get_and_clear()
            if not recent_errors:
                print("🟢 [시스템 건강] 에러 없이 완벽 구동 중.")
            else:
                print(f"🟡 [시스템 건강] {len(recent_errors)}건의 경미한 에러 감지:")
                for err in recent_errors[-5:]: print(f"   ↳ {err}")

            print("━"*65 + "\n")
            time.sleep(15) 

        time.sleep(1)

# ==========================================
# 🚀 메인 실행부 (컨트롤 타워)
# ==========================================
if __name__ == "__main__":
    print("🚀 24시간 독립 멀티스레딩 컨트롤 타워 가동 시작...")

    bot_targets = {}
    _scanner_bots = [
        ("🇺🇸 1. US 마스터", us_master),
        ("🇺🇸 2. US 역매공파", us_rev),
        ("🇺🇸 3. US 눌림목", us_nul),
        ("🇺🇸 4. US 밥그릇", us_bowl),
        ("🇺🇸 10. US 5일선", us_5ema),
        ("🇰🇷 5. KR 역매공파", kr_rev),
        ("🇰🇷 6. KR 마스터", kr_master),
        ("🇰🇷 7. KR 밥그릇", kr_bowl),
        ("🇰🇷 8. KR 눌림목", kr_nul),
        ("🇰🇷 9. KR 5일선", kr_5ema),
    ]
    for _label, _mod in _scanner_bots:
        if _mod is not None and hasattr(_mod, "run_scheduler"):
            bot_targets[_label] = _mod.run_scheduler
    bot_targets.update({
        # 2. 👑 자율 운영 코어 엔진 (절대 멈추면 안 됨)
        "💠 [엔진] DB 자동 갱신": run_db_updater_scheduler,
        "💠 [엔진] 장부 관리기": auto_forward_tester.run_daily_scheduler,
        "💠 [엔진] 2주 자율 관제탑": system_auto_pilot.system_main_loop,
        "💠 [엔진] 초신성 역추적기": supernova_hunter.run_scheduler,  # 💡 [추가] 매주 월요일 17시 자동 실행 장착!
        "💠 [엔진] AI 최고 감시자": ai_overseer.overseer_loop,
        "💠 [엔진] 텔레그램 AI 비서": ai_secretary.run_secretary
    })

    active_threads = {}

    # 스레드 13개 동시 구동
    for name, target_func in bot_targets.items():
        t = threading.Thread(target=target_func, daemon=True, name=name)
        t.start()
        active_threads[name] = t
        time.sleep(0.5)

    # 관제 모니터 구동
    monitor_thread = threading.Thread(target=status_monitor, args=(active_threads,), daemon=True, name="관제탑_모니터")
    monitor_thread.start()

    # 메인 프로세스가 꺼지지 않도록 영원히 대기
    for t in active_threads.values():
        t.join()
