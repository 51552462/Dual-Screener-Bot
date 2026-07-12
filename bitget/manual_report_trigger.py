import random
import time

from bitget.infra.logging_setup import get_logger, log_exception

START_END_MSG = "🚨 [Bitget 긴급 강제 보고] 수동 점검 및 리포트 트리거를 실행합니다..."
logger = get_logger("bitget.manual_report_trigger")


def _pause():
    delay = random.uniform(2.5, 4.5)
    logger.info("throttle pause %.1fs (API/DB load guard)", delay)
    time.sleep(delay)


def run_manual_report_trigger():
    logger.info("%s", START_END_MSG)

    try:
        logger.info("run: bitget.auto_pilot.run_autonomous_analysis()")
        import bitget.auto_pilot as bap

        if hasattr(bap, "run_autonomous_analysis"):
            bap.run_autonomous_analysis()
    except Exception as e:
        log_exception(logger, "bitget_auto_pilot autonomous analysis failed: %s", e)
    _pause()

    try:
        logger.info("run: bitget.auto_pilot.send_weekly_flow_master_report()")
        import bitget.auto_pilot as bap

        if hasattr(bap, "send_weekly_flow_master_report"):
            bap.send_weekly_flow_master_report()
    except Exception as e:
        log_exception(logger, "bitget_auto_pilot weekly report failed: %s", e)
    _pause()

    try:
        logger.info("run: bitget.forward_tester report helpers")
        import bitget.forward_tester as bft

        if hasattr(bft, "send_comprehensive_daily_report"):
            bft.send_comprehensive_daily_report()
        if hasattr(bft, "send_group_practitioner_reports"):
            bft.send_group_practitioner_reports()
    except Exception as e:
        log_exception(logger, "bitget_forward_tester report failed: %s", e)

    logger.info("%s", START_END_MSG)


if __name__ == "__main__":
    run_manual_report_trigger()
