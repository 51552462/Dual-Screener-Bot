import threading
import time

# 🇺🇸 미국장 로직 파일들
import nasdaq_all_ema224_signal_screener as us_ema
import nasdaq_dante_reverse_breakout_screener as us_rev
import nulusa as us_nul
import usa as us_bowl

# 🇰🇷 한국장 로직 파일들
import dante_krx_reverse_breakout_screener as kr_rev
import korea_ema224_signal_screener as kr_ema
import kr as kr_bowl
import nulrim as kr_nul
import ohdole as kr_ohdole

# 각 파일의 자체 스케줄러를 실행하는 함수들
def start_us_ema(): us_ema.run_scheduler()
def start_us_rev(): us_rev.run_scheduler()
def start_us_nul(): us_nul.run_scheduler()
def start_us_bowl(): us_bowl.run_scheduler()

def start_kr_rev(): kr_rev.run_scheduler()
def start_kr_ema(): kr_ema.run_scheduler()
def start_kr_bowl(): kr_bowl.run_scheduler()
def start_kr_nul(): kr_nul.run_scheduler()
def start_kr_ohdole(): kr_ohdole.run_scheduler()

if __name__ == "__main__":
    print("🚀 24시간 독립 멀티스레딩 컨트롤 타워 가동 시작...")

    # 한국장 밥그릇(kr.py)에 필요한 트레이딩뷰 세팅 1회 먼저 실행
    try:
        kr_bowl.initialize_tv_pool()
    except Exception as e:
        print(f"트레이딩뷰 초기화 에러 (무시하고 진행): {e}")

    # 9개의 검색기를 각각 독립된 스레드(차선)로 할당
    threads = [
        threading.Thread(target=start_us_ema, daemon=True),
        threading.Thread(target=start_us_rev, daemon=True),
        threading.Thread(target=start_us_nul, daemon=True),
        threading.Thread(target=start_us_bowl, daemon=True),
        
        threading.Thread(target=start_kr_rev, daemon=True),
        threading.Thread(target=start_kr_ema, daemon=True),
        threading.Thread(target=start_kr_bowl, daemon=True),
        threading.Thread(target=start_kr_nul, daemon=True),
        threading.Thread(target=start_kr_ohdole, daemon=True)
    ]

    # 각 차선 동시 출발!
    for t in threads:
        t.start()
        # 💡 시스템이 너무 한 번에 몰려서 뻗지 않도록 2초씩만 여유를 두고 출발시킵니다.
        time.sleep(2)

    # 서버가 꺼지지 않고 계속 스레드들을 지켜보게 만듭니다.
    for t in threads:
        t.join()