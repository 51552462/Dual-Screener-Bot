#!/usr/bin/env bash
# =============================================================================
# Bot-2(코인 전용)에서 주식 북극성 cron 제거.
#   sudo bash bitget/deploy/uninstall_stock_north_star_cron.sh
#
# 주식 SSOT: 주식 VPS의 factory-kr / director-digest 만.
# 코인 일보: bitget.sh --post-deploy-obs-digest (20:00 KST).
# =============================================================================
set -eu -o pipefail

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "root(sudo)로 실행하세요." >&2
  exit 1
fi

DEST="${BITGET_DIRECTOR_CRON_PATH:-/etc/cron.d/dual-screener-director-digest}"

if [[ ! -f "$DEST" ]]; then
  echo "✓ already absent: $DEST"
  exit 0
fi

if grep -q 'factory.sh --north-star-digest' "$DEST" 2>/dev/null; then
  rm -f "$DEST"
  echo "✓ removed stock north-star cron: $DEST"
else
  echo "⚠ $DEST exists but has no north-star-digest — leave untouched"
  exit 0
fi

if command -v systemctl >/dev/null 2>&1; then
  systemctl reload cron 2>/dev/null || systemctl reload crond 2>/dev/null || service cron reload 2>/dev/null || true
fi

echo "  Coin digest remains: /etc/cron.d/dual-screener-bitget → --post-deploy-obs-digest"
echo "  Verify: grep north-star /etc/cron.d/* || echo 'no stock north-star cron'"
