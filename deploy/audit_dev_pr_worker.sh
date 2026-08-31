#!/usr/bin/env bash
# Secret-safe audit for the isolated Cursor draft-PR worker.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROLE="${1:-}"
RUN_USER="${AUTONOMY_RUN_USER:-ubuntu}"

if [[ "${ROLE}" != "stock" && "${ROLE}" != "bitget" ]]; then
  echo "usage: $0 stock|bitget" >&2
  exit 2
fi

USER_HOME="$(getent passwd "${RUN_USER}" 2>/dev/null | cut -d: -f6)"
AI_PATH="${USER_HOME}/.local/bin:${USER_HOME}/.cursor/bin:/usr/local/bin:/usr/bin:/bin"
failures=0

check() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "[OK] ${label}"
  else
    echo "[FAIL] ${label}"
    failures=$((failures + 1))
  fi
}

controller_clean() {
  [[ -z "$(sudo -u "${RUN_USER}" git -C "${ROOT}" status --porcelain 2>/dev/null)" ]]
}

controller_main() {
  [[ "$(sudo -u "${RUN_USER}" git -C "${ROOT}" branch --show-current 2>/dev/null)" == "main" ]]
}

controller_synced() {
  local head remote
  head="$(sudo -u "${RUN_USER}" git -C "${ROOT}" rev-parse HEAD 2>/dev/null)" || return 1
  remote="$(sudo -u "${RUN_USER}" git -C "${ROOT}" rev-parse refs/remotes/origin/main 2>/dev/null)" || return 1
  [[ "${head}" == "${remote}" ]]
}

check "merged PR worker" test -f "${ROOT}/dev_autonomy/pr_worker.py"
check "controller worktree clean" controller_clean
check "controller on main" controller_main
check "controller matches origin/main" controller_synced
check "service unit installed" test -f /etc/systemd/system/quant-dev-pr-worker@.service
check "timer enabled (${ROLE})" systemctl is-enabled "quant-dev-pr-worker@${ROLE}.timer"
check "timer active (${ROLE})" systemctl is-active "quant-dev-pr-worker@${ROLE}.timer"
check "Claude authenticated" sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${AI_PATH}" claude auth status
check "Cursor authenticated" sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${AI_PATH}" agent status
check "GitHub authenticated" sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${AI_PATH}" gh auth status
check "PR envelope readable" sudo -u "${RUN_USER}" test -r /etc/quant-dev-autonomy/envelope.json
check "PR envelope active" sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PYTHONPATH="${ROOT}" \
  "${ROOT}/venv/bin/python" -m dev_autonomy.pr_worker \
  --envelope /etc/quant-dev-autonomy/envelope.json \
  --runtime-root "/var/lib/quant-dev-pr-worker/${ROLE}" \
  --role "${ROLE}" --check-envelope-only --json

if [[ "${failures}" -eq 0 ]]; then
  echo "[READY] ${ROLE} Cursor draft-PR worker"
else
  echo "[NOT READY] ${failures} check(s) failed"
fi
exit "${failures}"
