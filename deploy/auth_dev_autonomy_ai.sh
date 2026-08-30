#!/usr/bin/env bash
# Interactive login helper. Run as the same non-root user as the systemd unit.
set -euo pipefail

if [[ "${EUID:-0}" -eq 0 ]]; then
  echo "Do not authenticate AI CLIs as root." >&2
  echo "Run: sudo -u ubuntu -H bash deploy/auth_dev_autonomy_ai.sh" >&2
  exit 2
fi

export PATH="${HOME}/.local/bin:${HOME}/.cursor/bin:/usr/local/bin:/usr/bin:/bin"

if ! command -v claude >/dev/null 2>&1; then
  echo "Claude Code is not installed." >&2
  exit 2
fi
if ! command -v agent >/dev/null 2>&1; then
  echo "Cursor Agent CLI is not installed." >&2
  exit 2
fi

if claude auth status >/dev/null 2>&1; then
  echo "[OK] Claude authenticated"
else
  echo "[LOGIN] Claude — open the displayed URL on your laptop"
  claude auth login
fi

if agent status >/dev/null 2>&1; then
  echo "[OK] Cursor authenticated"
else
  echo "[LOGIN] Cursor — open the displayed URL on your laptop"
  NO_OPEN_BROWSER=1 agent login
fi

claude auth status >/dev/null 2>&1 && echo "[OK] Claude authentication verified"
agent status >/dev/null 2>&1 && echo "[OK] Cursor authentication verified"

