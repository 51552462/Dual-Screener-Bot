import os
import random
import subprocess
import sys
import time


ROOT = os.path.dirname(os.path.abspath(__file__))
START_END_MSG = "🚨 [긴급 강제 보고] 전체 시스템 점검 및 리포트를 생성합니다..."


def _run_script(script_name):
    script_path = os.path.join(ROOT, script_name)
    if not os.path.exists(script_path):
        print(f"⚠️ 파일 없음: {script_name}")
        return
    try:
        print(f"▶ 실행: {script_name}")
        subprocess.run([sys.executable, script_path], cwd=ROOT, check=False)
    except Exception as e:
        print(f"⚠️ 실행 실패 ({script_name}): {e}")


def run_manual_report_trigger():
    print(START_END_MSG)

    sequence = [
        "regime_meta_analyzer.py",
        "shadow_performance_tracker.py",
        "auto_forward_tester.py",
        "ai_overseer.py",
    ]

    for i, script in enumerate(sequence):
        _run_script(script)
        if i < len(sequence) - 1:
            delay = random.uniform(3.0, 5.0)
            print(f"⏳ API 과부하 방지 대기: {delay:.1f}초")
            time.sleep(delay)

    print(START_END_MSG)


if __name__ == "__main__":
    run_manual_report_trigger()
