import sys
import threading
import time
import os
import importlib
import subprocess
from datetime import datetime
import pytz
from bitget.infra.logging_setup import setup_logging, get_logger
from bitget.infra.data_paths import dashboard_port, heatmap_port

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


updater = _safe_import("bitget.mtf_data_updater")
scanner = _safe_import("bitget.master_scanner")
sniper = _safe_import("bitget.supernova_hunter")
auto_pilot = _safe_import("bitget.auto_pilot")
disk_manager = _safe_import("bitget.disk_manager")


def _dashboard_runner():
    pkg = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(pkg)
    dash = os.path.join(pkg, "dashboard.py")
    while True:
        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "streamlit",
                    "run",
                    dash,
                    f"--server.port={dashboard_port()}",
                    "--server.headless=true",
                ],
                cwd=project_root,
                check=False,
            )
        except Exception as e:
            print(f"⚠️ bitget dashboard 실행 실패: {e}")
        time.sleep(5)


def _heatmap_runner():
    pkg = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(pkg)
    heat = os.path.join(pkg, "heatmap_dashboard.py")
    while True:
        try:
            subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "streamlit",
                    "run",
                    heat,
                    f"--server.port={heatmap_port()}",
                    "--server.headless=true",
                ],
                cwd=project_root,
                check=False,
            )
        except Exception as e:
            print(f"⚠️ bitget heatmap_dashboard 실행 실패: {e}")
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
    import warnings

    warnings.warn(
        "bitget.main is DEPRECATED for production. Use systemd dante-bitget-* + "
        "bitget/deploy/bitget.sh (cron SSOT). See bitget/RUNBOOK.md",
        DeprecationWarning,
        stacklevel=1,
    )
    print(
        "[DEPRECATED] bitget.main — production uses dante-bitget-factory + bitget.sh cron. "
        "See bitget/RUNBOOK.md"
    )
    logger.warning("deprecated entry: use pipeline SSOT (bitget/RUNBOOK.md)")
    try:
        from bitget.infra import ops_logger

        ops_logger.record_heartbeat("bitget.main", extra={"event": "startup"})
    except Exception:
        pass

    bot_targets = {
        "💠 [엔진] MTF 데이터 업데이터": getattr(updater, "run_mtf_update", None),
        "🪙 [스캔] 마스터 스캐너": getattr(scanner, "run_mtf_scheduler", None),
        "💠 [엔진] 가상 장부 & 관제탑": getattr(auto_pilot, "system_main_loop", None),
        "🦅 [스캔] 초신성 실시간 스나이퍼": getattr(sniper, "run_live_sniper_scheduler", None),
        "📊 [대시보드] Bitget 관제탑": _dashboard_runner,
        "🔥 [대시보드] Bitget 히트맵": _heatmap_runner,
        "🧹 [인프라] 디스크 매니저": getattr(disk_manager, "run_daily_cleanup_loop", None),
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
