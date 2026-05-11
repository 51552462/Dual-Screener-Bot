import os
import subprocess
import time
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))


def launch_factory():
    print("🚀 [Dante Quant Factory] 마스터 런처 가동 시작...")
    print(
        " ↳ 🔬 글로벌 부검 위성(limit_up_forensics / forensics_pioneer)은 "
        "main.py → system_auto_pilot 타임테이블에서 구동됩니다."
    )

    processes = []

    # AI 보조 모듈 사전 점검 (에러가 나도 팩토리 전체 중단 금지)
    try:
        import ai_overseer  # noqa: F401
        print(" ↳ ✅ ai_overseer 임포트 점검 통과")
    except Exception as e:
        print(f" ↳ ⚠️ ai_overseer 점검 실패(무시): {e}")

    try:
        import auto_forward_tester  # noqa: F401
        print(" ↳ ✅ auto_forward_tester 임포트 점검 통과")
    except Exception as e:
        print(f" ↳ ⚠️ auto_forward_tester 점검 실패(무시): {e}")

    # 1. 메인 엔진 및 스케줄러 (백그라운드 실행)
    # (모든 위성은 main.py 안의 auto_pilot이 시간에 맞춰 알아서 돌려줍니다)
    print(" ↳ ⚙️ 1. 메인 신경망 (스나이퍼 및 오토파일럿) 점화...")
    try:
        p_main = subprocess.Popen([sys.executable, "main.py"], cwd=ROOT)
        processes.append(p_main)
    except Exception as e:
        print(f" ↳ ❌ main.py 점화 실패: {e}")

    # 2. 관제탑 대시보드 (Port 8501)
    print(" ↳ 📊 2. 관제탑 대시보드 웹 서버 점화...")
    try:
        p_dash = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                "dashboard.py",
                "--server.port=8501",
                "--server.headless=true",
            ],
            cwd=ROOT,
        )
        processes.append(p_dash)
    except Exception as e:
        print(f" ↳ ❌ dashboard.py 점화 실패: {e}")

    # 3. 섹터 히트맵 대시보드 (Port 8502)
    print(" ↳ 🔥 3. 섹터 히트맵 웹 서버 점화...")
    try:
        p_heat = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                "heatmap_dashboard.py",
                "--server.port=8502",
                "--server.headless=true",
            ],
            cwd=ROOT,
        )
        processes.append(p_heat)
    except Exception as e:
        print(f" ↳ ❌ heatmap_dashboard.py 점화 실패: {e}")

    print("\n✅ [가동 완료] 모든 시스템과 위성이 정상 궤도에 진입했습니다.")
    print("💡 이제 이 창을 켜두시면 팩토리는 영구 가동됩니다. (종료 시 Ctrl+C)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 팩토리 가동 중단 명령 수신. 모든 프로세스를 안전하게 종료합니다...")
        for p in processes:
            p.terminate()
        print("✅ 팩토리 셧다운 완료.")


if __name__ == "__main__":
    launch_factory()
