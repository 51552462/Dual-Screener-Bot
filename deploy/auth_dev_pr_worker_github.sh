#!/usr/bin/env bash
# One-time GitHub CLI browser login for the non-root service user.
set -euo pipefail

if [[ "${EUID:-0}" -eq 0 ]]; then
  echo "Do not authenticate GitHub CLI as root." >&2
  echo "Run: sudo -u ubuntu -H bash deploy/auth_dev_pr_worker_github.sh" >&2
  exit 2
fi
if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI is not installed." >&2
  exit 2
fi

if gh auth status >/dev/null 2>&1; then
  echo "[OK] GitHub CLI authenticated"
else
  echo "[LOGIN] GitHub — open the displayed URL on your laptop"
  gh auth login --web --git-protocol https
fi

gh auth setup-git
gh auth status >/dev/null 2>&1 && echo "[OK] GitHub Git credentials verified"

