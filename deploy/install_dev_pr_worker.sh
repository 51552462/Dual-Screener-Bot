#!/usr/bin/env bash
# Install the isolated Cursor draft-PR worker.  Disabled until all auth and
# explicit envelope capabilities pass.
set -euo pipefail

DEFAULT_INSTALL_ROOT="/home/ubuntu/dante_bots/Dual-Screener-Bot"
INSTALL_ROOT="${INSTALL_ROOT:-${DEFAULT_INSTALL_ROOT}}"
RUN_USER="${AUTONOMY_RUN_USER:-ubuntu}"
ROLE=""
INSTALL_GH=0
ENABLE_TIMER=0
START_NOW=0

usage() {
  cat <<'EOF'
Usage: sudo INSTALL_ROOT=/path/to/repo deploy/install_dev_pr_worker.sh --role stock|bitget [options]

Options:
  --install-github-cli  Install Ubuntu's gh package when it is missing.
  --enable-timer        Enable only after AI/GitHub auth and envelope checks.
  --start-now           Process at most one eligible job now (requires --enable-timer).

The worker may push an isolated branch and open a Draft PR. It cannot merge,
deploy, use SSH, change live risk, or modify the controller working tree.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --role)
      ROLE="${2:-}"
      shift 2
      ;;
    --install-github-cli)
      INSTALL_GH=1
      shift
      ;;
    --enable-timer)
      ENABLE_TIMER=1
      shift
      ;;
    --start-now)
      START_NOW=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      exit 2
      ;;
  esac
done

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "run with sudo" >&2
  exit 2
fi
if [[ "${ROLE}" != "stock" && "${ROLE}" != "bitget" ]]; then
  echo "--role must be stock or bitget" >&2
  exit 2
fi
if [[ "${START_NOW}" -eq 1 && "${ENABLE_TIMER}" -ne 1 ]]; then
  echo "--start-now requires --enable-timer" >&2
  exit 2
fi
if [[ ! -d "${INSTALL_ROOT}/.git" || ! -f "${INSTALL_ROOT}/dev_autonomy/pr_worker.py" ]]; then
  echo "merged repository not found at ${INSTALL_ROOT}" >&2
  exit 2
fi
if [[ ! -x "${INSTALL_ROOT}/venv/bin/python" ]]; then
  echo "venv python missing: ${INSTALL_ROOT}/venv/bin/python" >&2
  exit 2
fi
if ! id "${RUN_USER}" >/dev/null 2>&1; then
  echo "run user missing: ${RUN_USER}" >&2
  exit 2
fi

USER_HOME="$(getent passwd "${RUN_USER}" | cut -d: -f6)"
if [[ -z "${USER_HOME}" || "${INSTALL_ROOT}${USER_HOME}" == *"&"* || "${INSTALL_ROOT}${USER_HOME}" == *"|"* ]]; then
  echo "unsupported install/user path" >&2
  exit 2
fi

if [[ "${INSTALL_GH}" -eq 1 ]] && ! command -v gh >/dev/null 2>&1; then
  apt-get update
  apt-get install -y gh
fi

install -d -m 0700 -o "${RUN_USER}" -g "${RUN_USER}" "/var/lib/quant-dev-pr-worker/${ROLE}"
install -d -m 0750 -o root -g "${RUN_USER}" /etc/quant-dev-autonomy
if [[ -f /etc/quant-dev-autonomy/envelope.json ]]; then
  chown root:"${RUN_USER}" /etc/quant-dev-autonomy/envelope.json
  chmod 0640 /etc/quant-dev-autonomy/envelope.json
fi
chmod 0755 "${INSTALL_ROOT}/deploy/entrypoints/run_dev_pr_worker_service.sh"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT
sed \
  -e "s|@@INSTALL_ROOT@@|${INSTALL_ROOT}|g" \
  -e "s|@@RUN_USER@@|${RUN_USER}|g" \
  -e "s|@@USER_HOME@@|${USER_HOME}|g" \
  "${INSTALL_ROOT}/deploy/systemd/quant-dev-pr-worker@.service.in" \
  >"${TMP_DIR}/quant-dev-pr-worker@.service"
install -m 0644 "${TMP_DIR}/quant-dev-pr-worker@.service" /etc/systemd/system/quant-dev-pr-worker@.service
install -m 0644 "${INSTALL_ROOT}/deploy/systemd/quant-dev-pr-worker@.timer" \
  /etc/systemd/system/quant-dev-pr-worker@.timer
systemctl daemon-reload

if [[ "${ENABLE_TIMER}" -eq 1 ]]; then
  test -r /etc/quant-dev-autonomy/envelope.json
  sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${USER_HOME}/.local/bin:${USER_HOME}/.cursor/bin:/usr/local/bin:/usr/bin:/bin" agent status >/dev/null
  sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${USER_HOME}/.local/bin:${USER_HOME}/.cursor/bin:/usr/local/bin:/usr/bin:/bin" claude auth status >/dev/null
  sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${USER_HOME}/.local/bin:${USER_HOME}/.cursor/bin:/usr/local/bin:/usr/bin:/bin" gh auth status >/dev/null
  sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PYTHONPATH="${INSTALL_ROOT}" \
    "${INSTALL_ROOT}/venv/bin/python" -m dev_autonomy.pr_worker \
      --envelope /etc/quant-dev-autonomy/envelope.json \
      --runtime-root "/var/lib/quant-dev-pr-worker/${ROLE}" \
      --role "${ROLE}" --check-envelope-only --json
  systemctl enable --now "quant-dev-pr-worker@${ROLE}.timer"
  echo "[install] enabled quant-dev-pr-worker@${ROLE}.timer"
else
  echo "[install] timer unchanged; authenticate GitHub and install an explicit PR envelope first"
fi

if [[ "${START_NOW}" -eq 1 ]]; then
  systemctl start "quant-dev-pr-worker@${ROLE}.service"
  echo "[install] one PR-worker cycle completed"
fi

echo "[install] next: sudo bash ${INSTALL_ROOT}/deploy/audit_dev_pr_worker.sh ${ROLE}"

