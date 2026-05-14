import os
import random
import time


ROOT = os.path.dirname(os.path.abspath(__file__))
START_END_MSG = "🚨 [긴급 강제 보고] 전체 시스템 점검 및 리포트를 생성합니다..."


def _pause():
    delay = random.uniform(3.0, 5.0)
    print(f"⏳ API 과부하 방지 대기: {delay:.1f}초")
    time.sleep(delay)


def run_manual_report_trigger():
    print(START_END_MSG)
    try:
        # 1) 현재 장세 판독
        print("▶ 실행: regime_meta_analyzer.analyze_market_regime()")
        import regime_meta_analyzer as rma

        rma.analyze_market_regime()
    except Exception as e:
        print(f"⚠️ regime_meta_analyzer 실패: {e}")
    _pause()

    try:
        # 2) 그림자 장부 방어력/기여도 평가
        print("▶ 실행: shadow_performance_tracker.run_shadow_performance_evaluation()")
        import shadow_performance_tracker as spt

        spt.run_shadow_performance_evaluation()
    except Exception as e:
        print(f"⚠️ shadow_performance_tracker 실패: {e}")
    _pause()

    try:
        # 3) 실무자 전투 랭킹 및 리포트 발송 (스케줄러 루프가 아닌 1회 함수만)
        print("▶ 실행: auto_forward_tester 보고 함수 1회 호출")
        import auto_forward_tester as aft

        aft.send_comprehensive_daily_report()
        aft.send_group_practitioner_reports()
    except Exception as e:
        print(f"⚠️ auto_forward_tester 보고 실패: {e}")
    _pause()

    try:
        # 4) AI 최종 브리핑 1회 발송 (루프 금지)
        print("▶ 실행: ai_overseer.run_ai_auditor()")
        import ai_overseer as aio

        aio.run_ai_auditor()
    except Exception as e:
        print(f"⚠️ ai_overseer 브리핑 실패: {e}")

    print(START_END_MSG)


if __name__ == "__main__":
    run_manual_report_trigger()
