import random
import time

START_END_MSG = "🚨 [Bitget 긴급 강제 보고] 수동 점검 및 리포트 트리거를 실행합니다..."


def _pause():
    delay = random.uniform(2.5, 4.5)
    print(f"⏳ API/DB 과부하 방지 대기: {delay:.1f}초")
    time.sleep(delay)


def run_manual_report_trigger():
    print(START_END_MSG)

    try:
        print("▶ 실행: bitget_auto_pilot.run_autonomous_analysis()")
        import bitget.auto_pilot as bitget_auto_pilot as bap

        if hasattr(bap, "run_autonomous_analysis"):
            bap.run_autonomous_analysis()
    except Exception as e:
        print(f"⚠️ bitget_auto_pilot 자율분석 실패: {e}")
    _pause()

    try:
        print("▶ 실행: bitget_auto_pilot.send_weekly_flow_master_report()")
        import bitget.auto_pilot as bitget_auto_pilot as bap

        if hasattr(bap, "send_weekly_flow_master_report"):
            bap.send_weekly_flow_master_report()
    except Exception as e:
        print(f"⚠️ bitget_auto_pilot 주간 보고 실패: {e}")
    _pause()

    try:
        print("▶ 실행: bitget_forward_tester 보고 함수 1회 호출")
        import bitget.forward_tester as bitget_forward_tester as bft

        if hasattr(bft, "send_comprehensive_daily_report"):
            bft.send_comprehensive_daily_report()
        if hasattr(bft, "send_group_practitioner_reports"):
            bft.send_group_practitioner_reports()
    except Exception as e:
        print(f"⚠️ bitget_forward_tester 보고 실패: {e}")

    print(START_END_MSG)


if __name__ == "__main__":
    run_manual_report_trigger()
