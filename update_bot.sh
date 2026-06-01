#!/usr/bin/env bash
# =============================================================================
# Dual-Screener-Bot — 원터치 서버 업데이트
#   저장소 루트에서: sudo bash update_bot.sh
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_ROOT"

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "root 로 실행하세요: sudo bash update_bot.sh" >&2
  exit 1
fi

echo "▶ [1/6] CRLF 정리 (*.sh, deploy/*.sh)..."
sed -i 's/\r$//' *.sh deploy/*.sh 2>/dev/null || true

echo "▶ [2/6] GitHub 최신 코드 (main)..."
git pull origin main

echo "▶ [3/6] Python __pycache__ 정리..."
find . -name '__pycache__' -exec rm -rf {} +

echo "▶ [4/6] 구형 유닛 중지 (dante-main, dante-streamlit)..."
systemctl stop dante-main dante-streamlit 2>/dev/null || true

echo "▶ [5/6] 데몬·스케줄러 재기동..."
systemctl daemon-reload && systemctl restart dante-factory dante-async cron

echo -e "\n✅ 100% 모든 시스템이 최신 버전으로 정상 가동되었습니다! 실시간 로그를 보려면 'sudo journalctl -u dante-factory -f' 를 입력하세요."
