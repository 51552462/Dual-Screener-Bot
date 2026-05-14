#!/usr/bin/env bash
# =============================================================================
# 배포 성공 알림 — 기존 telegram_message_queue.enqueue_telegram 경로만 사용
# (매매 코어 미수정). update_factory.sh 맨 끝에서 호출.
#
#   INSTALL_ROOT=/path/to/repo sudo ./deploy/ubuntu/post_update_notify.sh
#   (보통 update_factory.sh 가 INSTALL_ROOT 를 넘김)
#
# TELEGRAM_TOKEN_MAIN / TELEGRAM_CHAT_ID 는 INSTALL_ROOT/.env 에 있어야 함.
# =============================================================================
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
export INSTALL_ROOT
DEPLOY_USER="${DEPLOY_USER:-ubuntu}"

# root 가 호출해도 큐 DB 경로(~ubuntu/...)가 맞도록 동일 유저로 Python 실행
sudo -E -u "$DEPLOY_USER" bash <<EOS
set -euo pipefail
cd "\${INSTALL_ROOT}"
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source ./.env
  set +a
fi
export PYTHONPATH="\${INSTALL_ROOT}\${PYTHONPATH:+:\${PYTHONPATH}}"
exec "\${INSTALL_ROOT}/.venv/bin/python" - <<'PY'
import os
import subprocess
import sys

root = os.environ.get("INSTALL_ROOT", "").strip()
if not root:
    print("post_update_notify: INSTALL_ROOT 비어 있음", file=sys.stderr)
    sys.exit(1)
os.chdir(root)

def _ia(unit: str) -> str:
    try:
        r = subprocess.run(
            ["systemctl", "is-active", unit],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        return (r.stdout or "").strip() or "unknown"
    except Exception:
        return "unknown"

bits = [
    f"dante-factory={_ia('dante-factory.service')}",
    f"dante-dashboard={_ia('dante-dashboard.service')}",
    f"dante-async={_ia('dante-async.service')}",
    f"dante-snapshot.timer={_ia('dante-snapshot.timer')}",
    f"dante-watchdog.timer={_ia('dante-watchdog.timer')}",
]
line = ", ".join(bits)
msg = (
    "✅ [Dual-Screener 팩토리] V2 시스템 업데이트 및 전체 서비스 재기동이 성공적으로 완료되었습니다.\n"
    f"(is-active: {line})"
)

from telegram_message_queue import enqueue_telegram

tok = (os.environ.get("TELEGRAM_TOKEN_MAIN") or "").strip()
cid = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
enabled = bool(tok and cid)
rid = enqueue_telegram("MAIN", None, msg, enabled=enabled)
print(f"post_update_notify: enqueue id={rid} enabled={enabled}")
PY
EOS
