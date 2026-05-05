import sys
import threading
import time
import os
from datetime import datetime
import pytz

# 💡 터미널 한글 깨짐 방지
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
# 👑 코인 시스템 모듈 임포트 (주말 차단기 없음)
# ==========================================
import bitget_mtf_data_updater as updater
import bitget_master_scanner as scanner
import bitget_forward_tester as forward_tester
import bitget_supernova_hunter as sniper
import bitget_data_miner as miner

# ==========================================
# 📊 실시간 생존 확인 및 종합 보고서 (모니터)
# ==========================================
def status_monitor(threads_dict):
    seoul_tz = pytz.timezone('Asia/Seoul')
    print("\n📡 [코인 관제탑] 24/7 실시간 모니터링 시스템 가동!\n")

    while True:
        now_kr = datetime.now(seoul_tz)
        if now_kr.minute == 59 and now_kr.second >= 50:
            print("\n" + "━"*65)
            print(f"📊 [코인 관제탑 1시간 종합 보고서] 🪙 {now_kr.strftime('%H:%M')} 기준 마감")
            print("━"*65)

            dead_bots = [name for name, t in threads_dict.items() if not t.is_alive()]

            if not dead_bots:
                print("🟢 [스레드 상태] 퀀트 코어 엔진 100% 정상 작동 중!")
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
    print("🚀 비트겟(Bitget) 24/7 멀티 타임프레임 컨트롤 타워 가동 시작...")

    bot_targets = {
        "💠 [엔진] MTF 데이터 업데이터": updater.run_mtf_update, # 스케줄러 내장 가정
        "🪙 [스캔] 마스터 스캐너": scanner.run_scheduler,
        "💠 [엔진] 가상 장부 & 관제탑": forward_tester.run_daily_scheduler,
        "🦅 [스캔] 초신성 실시간 스나이퍼": sniper.run_live_sniper_scheduler,
        "🧪 [마이닝] K-Means 데이터 마이너": miner.run_cluster_mining # 1회성 또는 스케줄러
    }

    active_threads = {}

    for name, target_func in bot_targets.items():
        # 함수가 존재하는지 안전하게 체크 후 실행
        if callable(target_func):
            t = threading.Thread(target=target_func, daemon=True, name=name)
            t.start()
            active_threads[name] = t
            time.sleep(0.5)
        else:
            print(f"⚠️ {name} 실행 실패: 해당 함수를 찾을 수 없습니다.")

    # 관제 모니터 구동
    monitor_thread = threading.Thread(target=status_monitor, args=(active_threads,), daemon=True, name="관제탑_모니터")
    monitor_thread.start()

    # 메인 프로세스가 꺼지지 않도록 영원히 대기
    for t in active_threads.values():
        t.join()