import sys
import threading
import time
import os
import importlib
import subprocess
from datetime import datetime
import pytz
from bitget_logger import setup_logging, get_logger
from bitget.schedule_lock import acquire as schedule_acquire

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
setup_logging()
logger = get_logger("bitget.main")

# ==========================================
# 👑 코인 시스템 모듈 임포트 (주말 차단기 없음)
# ==========================================
def _safe_import(module_name):
    try:
        return importlib.import_module(module_name)
    except Exception as e:
        print(f"⚠️ 모듈 로드 실패: {module_name} -> {e}")
        return None


updater = _safe_import("bitget_mtf_data_updater")
scanner = _safe_import("bitget_master_scanner")
forward_tester = _safe_import("bitget_forward_tester")
sniper = _safe_import("bitget_supernova_hunter")
miner = _safe_import("bitget_data_miner")
auto_pilot = _safe_import("bitget_auto_pilot")
underdog_miner = _safe_import("bitget_underdog_miner")
pump_forensics = _safe_import("bitget_pump_forensics")
forensics_pioneer = _safe_import("bitget_forensics_pioneer")
sentiment_miner = _safe_import("bitget_sentiment_miner")
alt_data_miner = _safe_import("bitget_alt_data_miner")
shadow_perf = _safe_import("bitget_shadow_performance_tracker")
blackhole_hunter = _safe_import("bitget_blackhole_hunter")
synthetic_lab = _safe_import("bitget_synthetic_data_generator")
time_machine = _safe_import("bitget_time_machine_backtester")
disk_manager = _safe_import("bitget_disk_manager")


def _periodic_runner(target_func, interval_sec, name):
    while True:
        try:
            if schedule_acquire(f"main::{name}", max(30, int(interval_sec) - 3)):
                target_func()
        except Exception as e:
            print(f"⚠️ {name} 주기 실행 실패: {e}")
        time.sleep(max(5, int(interval_sec)))


def _dashboard_runner():
    root = os.path.dirname(os.path.abspath(__file__))
    while True:
        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "streamlit",
                    "run",
                    "bitget_dashboard.py",
                    "--server.port=8501",
                    "--server.headless=true",
                ],
                cwd=root,
                check=False,
            )
        except Exception as e:
            print(f"⚠️ bitget_dashboard 실행 실패: {e}")
        time.sleep(5)


def _heatmap_runner():
    root = os.path.dirname(os.path.abspath(__file__))
    while True:
        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "streamlit",
                    "run",
                    "bitget_heatmap_dashboard.py",
                    "--server.port=8502",
                    "--server.headless=true",
                ],
                cwd=root,
                check=False,
            )
        except Exception as e:
            print(f"⚠️ bitget_heatmap_dashboard 실행 실패: {e}")
        time.sleep(5)

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
    logger.info("bitget main starting")

    bot_targets = {
        "💠 [엔진] MTF 데이터 업데이터": getattr(updater, "run_mtf_update", None),
        "🪙 [스캔] 마스터 스캐너": getattr(scanner, "run_mtf_scheduler", None),
        "💠 [엔진] 가상 장부 & 관제탑": getattr(auto_pilot, "system_main_loop", None),
        "🦅 [스캔] 초신성 실시간 스나이퍼": getattr(sniper, "run_live_sniper_scheduler", None),
        "🧪 [마이닝] K-Means 데이터 마이너": getattr(miner, "run_cluster_mining", None),  # 1회성 또는 스케줄러
        "📊 [대시보드] Bitget 관제탑": _dashboard_runner,
        "🔥 [대시보드] Bitget 히트맵": _heatmap_runner,
        "🧹 [인프라] 디스크 매니저": getattr(disk_manager, "run_daily_cleanup_loop", None),
        "🧟 [위성] 언더독 마이너(6h)": (lambda: _periodic_runner(getattr(underdog_miner, "run_underdog_mining", lambda: None), 21600, "언더독 마이너")),
        "🚀 [위성] 펌프 부검소(4h)": (lambda: _periodic_runner(getattr(pump_forensics, "run_pump_forensics", lambda: None), 14400, "펌프 부검소")),
        "🔭 [위성] 선취매 파이오니어(3h)": (lambda: _periodic_runner(getattr(forensics_pioneer, "run_forensics_pioneer", lambda: None), 10800, "선취매 파이오니어")),
        "🧠 [위성] 센티먼트 마이너(2h)": (lambda: _periodic_runner(getattr(sentiment_miner, "run_sentiment_mining", lambda: None), 7200, "센티먼트 마이너")),
        "📡 [위성] 대체데이터 마이너(2h)": (lambda: _periodic_runner(getattr(alt_data_miner, "run_alternative_data_mining", lambda: None), 7200, "대체데이터 마이너")),
        "🛡️ [위성] 그림자 성능 추적(6h)": (lambda: _periodic_runner(getattr(shadow_perf, "run_shadow_performance_evaluation", lambda: None), 21600, "그림자 성능 추적")),
        "🕳️ [위성] 블랙홀 헌터(3h)": (lambda: _periodic_runner(getattr(blackhole_hunter, "scan_blackhole_targets", lambda: None), 10800, "블랙홀 헌터")),
        "🧪 [위성] 합성 스트레스 연구소(12h)": (lambda: _periodic_runner(getattr(synthetic_lab, "stress_test_mutants", lambda: None), 43200, "합성 스트레스 연구소")),
        "⏳ [위성] 타임머신 백테스트(24h)": (lambda: _periodic_runner(getattr(time_machine, "run_time_machine_backtest", lambda: None), 86400, "타임머신 백테스트")),
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
