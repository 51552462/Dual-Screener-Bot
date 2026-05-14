import os
from bitget_logger import setup_logging, get_logger
import bitget_sentinel

ROOT = os.path.dirname(os.path.abspath(__file__))
setup_logging()
logger = get_logger("bitget.factory_launcher")


def launch_factory():
    print("🚀 [Bitget Quant Factory] Sentinel 런처 가동 시작...")
    logger.info("launching via sentinel watchdog")
    bitget_sentinel.run_sentinel()


if __name__ == "__main__":
    launch_factory()
