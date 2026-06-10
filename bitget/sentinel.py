import os
import subprocess
import sys
import time

import requests

from bitget.env import bitget_telegram_chat_id, bitget_telegram_token
from bitget.infra.data_paths import dashboard_port, heatmap_port
from bitget.infra.logging_setup import get_logger, setup_logging

BITGET_PKG = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BITGET_PKG)
logger = get_logger("bitget.sentinel")


def _send_telegram_alert(text):
    token = bitget_telegram_token()
    chat_id = bitget_telegram_chat_id()
    if not token or not chat_id:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=8,
        )
    except Exception:
        pass


def _spawn(name):
    if name == "dashboard":
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                os.path.join(BITGET_PKG, "dashboard.py"),
                f"--server.port={dashboard_port()}",
                "--server.headless=true",
            ],
            cwd=PROJECT_ROOT,
        )
    if name == "heatmap":
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                os.path.join(BITGET_PKG, "heatmap_dashboard.py"),
                f"--server.port={heatmap_port()}",
                "--server.headless=true",
            ],
            cwd=PROJECT_ROOT,
        )
    raise ValueError(f"unknown process name: {name}")


def run_sentinel():
    setup_logging()
    logger.info("sentinel started")
    # Production daemon: dante-bitget-factory → bitget_auto_pilot (not bitget.main)
    names = ["dashboard", "heatmap"]
    procs = {}
    for name in names:
        try:
            procs[name] = _spawn(name)
            logger.info("spawned %s pid=%s", name, procs[name].pid)
        except Exception as e:
            logger.exception("initial spawn failed: %s", name)
            _send_telegram_alert(f"[Bitget Sentinel] {name} launch failed: {e}")
    while True:
        for name in names:
            p = procs.get(name)
            if p is None or p.poll() is not None:
                code = None if p is None else p.returncode
                msg = f"[Bitget Sentinel] {name} died (exit={code}), restarting now."
                logger.warning(msg)
                _send_telegram_alert(msg)
                try:
                    procs[name] = _spawn(name)
                    logger.info("restarted %s pid=%s", name, procs[name].pid)
                except Exception as e:
                    logger.exception("restart failed: %s", name)
                    _send_telegram_alert(f"[Bitget Sentinel] {name} restart failed: {e}")
        time.sleep(5)


if __name__ == "__main__":
    run_sentinel()
