#!/usr/bin/env bash
# Install the role-isolated autonomy service/timer. Secrets remain in existing
# .env files or the AI CLIs' own user credential stores.
set -euo pipefail

DEFAULT_INSTALL_ROOT="/home/ubuntu/dante_bots/Dual-Screener-Bot"
INSTALL_ROOT="${INSTALL_ROOT:-${DEFAULT_INSTALL_ROOT}}"
RUN_USER="${AUTONOMY_RUN_USER:-ubuntu}"
ROLE=""
INSTALL_AI_CLIS=0
ENABLE_TIMER=0
START_NOW=0

usage() {
  cat <<'EOF'
Usage: sudo INSTALL_ROOT=/path/to/repo deploy/install_dev_autonomy.sh --role stock|bitget [options]

Options:
  --install-ai-clis  Install official stable Claude Code and Cursor Agent CLIs for RUN_USER.
  --enable-timer     Enable the role timer after authentication and dry-run checks.
  --start-now        Run one live control cycle now (requires --enable-timer).

This script never creates or prints Telegram, Claude, Cursor, exchange, or GitHub secrets.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --role)
      ROLE="${2:-}"
      shift 2
      ;;
    --install-ai-clis)
      INSTALL_AI_CLIS=1
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
      usage >&2
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
if [[ ! -d "${INSTALL_ROOT}/.git" || ! -f "${INSTALL_ROOT}/dev_autonomy/weekday_runner.py" ]]; then
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

install -d -m 0700 -o "${RUN_USER}" -g "${RUN_USER}" "/var/lib/quant-dev-autonomy/${ROLE}"
install -d -m 0750 -o root -g "${RUN_USER}" /etc/quant-dev-autonomy
if [[ -f /etc/quant-dev-autonomy/envelope.json ]]; then
  chown root:"${RUN_USER}" /etc/quant-dev-autonomy/envelope.json
  chmod 0640 /etc/quant-dev-autonomy/envelope.json
fi
chmod 600 "${INSTALL_ROOT}/.env" 2>/dev/null || true
chmod 600 "${INSTALL_ROOT}/bitget/.env" 2>/dev/null || true
chmod 0755 "${INSTALL_ROOT}/deploy/entrypoints/run_dev_autonomy_service.sh"

if [[ "${INSTALL_AI_CLIS}" -eq 1 ]]; then
  if ! sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${USER_HOME}/.local/bin:/usr/local/bin:/usr/bin:/bin" \
    bash -lc 'command -v claude >/dev/null 2>&1'; then
    sudo -u "${RUN_USER}" env HOME="${USER_HOME}" bash -lc \
      'curl -fsSL https://claude.ai/install.sh | bash -s stable'
  fi
  if ! sudo -u "${RUN_USER}" env HOME="${USER_HOME}" PATH="${USER_HOME}/.local/bin:/usr/local/bin:/usr/bin:/bin" \
    bash -lc 'command -v agent >/dev/null 2>&1'; then
    sudo -u "${RUN_USER}" env HOME="${USER_HOME}" bash -lc \
      'curl https://cursor.com/install -fsS | bash'
  fi
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT
SERVICE_SRC="${INSTALL_ROOT}/deploy/systemd/quant-dev-autonomy@.service.in"
TIMER_SRC="${INSTALL_ROOT}/deploy/systemd/quant-dev-autonomy@.timer"

sed \
  -e "s|@@INSTALL_ROOT@@|${INSTALL_ROOT}|g" \
  -e "s|@@RUN_USER@@|${RUN_USER}|g" \
  -e "s|@@USER_HOME@@|${USER_HOME}|g" \
  "${SERVICE_SRC}" >"${TMP_DIR}/quant-dev-autonomy@.service"
install -m 0644 "${TMP_DIR}/quant-dev-autonomy@.service" /etc/systemd/system/quant-dev-autonomy@.service
install -m 0644 "${TIMER_SRC}" /etc/systemd/system/quant-dev-autonomy@.timer
systemctl daemon-reload

echo "[install] dry-run role=${ROLE}"
sudo -u "${RUN_USER}" env \
  HOME="${USER_HOME}" \
  PATH="${USER_HOME}/.local/bin:${USER_HOME}/.cursor/bin:/usr/local/bin:/usr/bin:/bin" \
  AUTONOMY_PYTHON="${INSTALL_ROOT}/venv/bin/python" \
  AUTONOMY_RUNTIME_ROOT="/var/lib/quant-dev-autonomy" \
  bash "${INSTALL_ROOT}/deploy/entrypoints/run_dev_autonomy_service.sh" "${ROLE}" --dry-run

if [[ "${ENABLE_TIMER}" -eq 1 ]]; then
  systemctl enable --now "quant-dev-autonomy@${ROLE}.timer"
  echo "[install] enabled quant-dev-autonomy@${ROLE}.timer"
else
  echo "[install] timer state unchanged; use --enable-timer after AI authentication"
fi

if [[ "${START_NOW}" -eq 1 ]]; then
  systemctl start "quant-dev-autonomy@${ROLE}.service"
  echo "[install] live control cycle completed"
fi

systemctl --no-pager --full status "quant-dev-autonomy@${ROLE}.timer" 2>/dev/null || true
echo "[install] next: bash ${INSTALL_ROOT}/deploy/audit_dev_autonomy.sh ${ROLE}"
