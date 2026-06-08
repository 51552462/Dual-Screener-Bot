import os
from bitget.infra.logging_setup import setup_logging, get_logger
import bitget.sentinel as bitget_sentinel

ROOT = os.path.dirname(os.path.abspath(__file__))
setup_logging()
logger = get_logger("bitget.factory_launcher")


def launch_factory():
    import warnings

    warnings.warn(
        "factory_launcher/sentinel is DEPRECATED for production. "
        "Use dante-bitget-factory + dante-bitget-dashboard systemd units.",
        DeprecationWarning,
        stacklevel=2,
    )
    print("[DEPRECATED] factory_launcher — use systemd dante-bitget-* (bitget/RUNBOOK.md)")
    logger.warning("deprecated sentinel launcher — use systemd")
    bitget_sentinel.run_sentinel()


if __name__ == "__main__":
    launch_factory()
