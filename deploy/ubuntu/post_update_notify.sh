#!/usr/bin/env bash
# =============================================================================
# 배포 성공 알림 — 기존 telegram_message_queue.enqueue_telegram 경로만 사용
# (매매 코어 미수정). update_factory.sh 맨 끝에서 호출.
#
#   INSTALL_ROOT=/path/to/repo sudo ./deploy/ubuntu/post_update_notify.sh
#   (보통 update_factory.sh 가 INSTALL_ROOT 를 넘김)
#
# TELEGRAM 자격 증명은 telegram_env(.env) 통일.
# =============================================================================
set -eu -o pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
export INSTALL_ROOT
DEPLOY_USER="${DEPLOY_USER:-ubuntu}"

# root 가 호출해도 큐 DB 경로(~ubuntu/...)가 맞도록 동일 유저로 Python 실행
sudo -E -u "$DEPLOY_USER" bash <<EOS
set -eu -o pipefail
cd "\${INSTALL_ROOT}"
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source ./.env
  set +a
fi
export PYTHONPATH="\${INSTALL_ROOT}\${PYTHONPATH:+:\${PYTHONPATH}}"
PYBIN="\${INSTALL_ROOT}/venv/bin/python"
if [[ ! -x "\$PYBIN" ]]; then
  PYBIN="\${INSTALL_ROOT}/.venv/bin/python"
fi
exec "\$PYBIN" - <<'PY'
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
    f"dante-backup.timer={_ia('dante-backup.timer')}",
]
line = ", ".join(bits)
msg = (
    "✅ [Dual-Screener 팩토리] 데이터 100% 보존 · venv 엔진 교체 및 전체 서비스 재기동 완료.\n"
    f"(is-active: {line})"
)

from telegram_message_queue import enqueue_telegram
from telegram_env import (
    get_equity_kr_factory_chat_id,
    get_equity_kr_main_token,
    get_equity_us_factory_chat_id,
    get_equity_us_main_token,
    get_factory_chat_id,
    get_main_token,
)

tok = get_main_token() or get_equity_us_main_token() or get_equity_kr_main_token()
cid = get_factory_chat_id() or get_equity_us_factory_chat_id() or get_equity_kr_factory_chat_id()
enabled = bool(tok and cid)
rid = enqueue_telegram("MAIN", None, msg, enabled=enabled)
print(f"post_update_notify: enqueue id={rid} enabled={enabled}")
PY
EOS
