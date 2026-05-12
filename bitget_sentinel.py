import os
import subprocess
import sys
import time

import requests

from bitget_env import bitget_telegram_chat_id, bitget_telegram_token
from bitget_logger import get_logger, setup_logging


ROOT = os.path.dirname(os.path.abspath(__file__))
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
    if name == "main":
        return subprocess.Popen([sys.executable, "bitget_main.py"], cwd=ROOT)
    if name == "dashboard":
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                "bitget_dashboard.py",
                "--server.port=8501",
                "--server.headless=true",
            ],
            cwd=ROOT,
        )
    if name == "heatmap":
        return subprocess.Popen(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                "bitget_heatmap_dashboard.py",
                "--server.port=8502",
                "--server.headless=true",
            ],
            cwd=ROOT,
        )
    raise ValueError(f"unknown process name: {name}")


def run_sentinel():
    setup_logging()
    logger.info("sentinel started")
    names = ["main", "dashboard", "heatmap"]
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
