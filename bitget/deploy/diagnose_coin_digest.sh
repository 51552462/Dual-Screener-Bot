#!/usr/bin/env bash
# =============================================================================
# 코인 북극성(POST_DEPLOY_OBS) 왜 안 오는지 진단 + 선택적 즉시 발송
#   cd INSTALL_ROOT && bash bitget/deploy/diagnose_coin_digest.sh
#   bash bitget/deploy/diagnose_coin_digest.sh --send    # 텔레그램 지금 1회
#   bash bitget/deploy/diagnose_coin_digest.sh --dry-run
# =============================================================================
set -eu -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BITGET_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-$(cd "${BITGET_ROOT}/.." && pwd)}"
cd "$INSTALL_ROOT"

SEND=0
DRY=0
for a in "$@"; do
  case "$a" in
    --send) SEND=1 ;;
    --dry-run) DRY=1 ;;
  esac
done

pass() { echo "  ✓ $*"; }
warn() { echo "  ⚠ $*"; }
fail() { echo "  ✗ $*"; }

echo "=== 코인 북극성(POST_DEPLOY_OBS) 진단 ==="
echo "INSTALL_ROOT=$INSTALL_ROOT"
echo "time: $(date -u '+%Y-%m-%d %H:%M:%S UTC') / KST $(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S')"
echo ""

echo "[1] cron — 코인 일보 줄 (매일 UTC 11:00 = KST 20:00)"
CRON_BITGET="${BITGET_CRON_PATH:-/etc/cron.d/dual-screener-bitget}"
if [[ ! -f "$CRON_BITGET" ]]; then
  fail "없음: $CRON_BITGET"
  echo "    → sudo INSTALL_ROOT=$INSTALL_ROOT bash bitget/deploy/install_bitget_cron.sh"
elif grep -q 'bitget.sh --post-deploy-obs-digest' "$CRON_BITGET"; then
  pass "post-deploy-obs-digest 등록됨"
  grep 'post-deploy-obs-digest' "$CRON_BITGET" | sed 's/^/    /'
else
  fail "cron에 post-deploy-obs-digest 없음 → 코인 북극성이 자동으로 안 감"
  echo "    → sudo INSTALL_ROOT=$INSTALL_ROOT bash bitget/deploy/install_bitget_cron.sh"
fi
echo ""

echo "[2] 방해 요소 — 주식 북극성 cron (있으면 코인 채팅을 오염)"
if [[ -f /etc/cron.d/dual-screener-director-digest ]] \
  && grep -q 'north-star-digest' /etc/cron.d/dual-screener-director-digest 2>/dev/null; then
  fail "주식 north-star cron 이 코인 서버에 있음"
  echo "    → sudo bash bitget/deploy/uninstall_stock_north_star_cron.sh"
else
  pass "주식 north-star director-digest 없음"
fi
echo ""

echo "[3] REPORT_BOT (코인 일보 전송 채널)"
PY="${INSTALL_ROOT}/venv/bin/python"
[[ -x "$PY" ]] || PY="${INSTALL_ROOT}/.venv/bin/python"
if [[ -f "${INSTALL_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${INSTALL_ROOT}/.env"
  set +a
fi
if [[ -f "${BITGET_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${BITGET_ROOT}/.env"
  set +a
fi
if [[ -x "$PY" ]]; then
  "$PY" -c "
import telegram_env as t
rt, rc = t.get_report_token(), t.get_report_chat_id()
print('  token:', 'set' if rt else 'MISSING')
print('  chat:', 'set' if rc else 'MISSING')
" 2>/dev/null || warn "telegram_env 조회 실패"
else
  warn "python/venv 없음"
fi
if [[ "${POST_DEPLOY_OBS_DIGEST_ENABLED:-}" == "0" || "${POST_DEPLOY_OBS_DIGEST_ENABLED:-}" == "false" ]]; then
  fail "POST_DEPLOY_OBS_DIGEST_ENABLED=${POST_DEPLOY_OBS_DIGEST_ENABLED} → 발송 스킵"
fi
echo ""

echo "[4] 최근 실행 로그 (bitget_post_deploy_obs_*.log)"
LOG_DIR="${BITGET_LOG_DIR:-${BITGET_ROOT}/logs}"
shopt -s nullglob
logs=("$LOG_DIR"/bitget_post_deploy_obs_*.log)
if ((${#logs[@]} == 0)); then
  fail "로그 0개 — cron이 한 번도 안 돌았거나 LOG_DIR 다름 ($LOG_DIR)"
else
  newest="$(ls -t "${logs[@]}" | head -1)"
  pass "최신: $newest"
  echo "    mtime: $(date -r "$newest" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || stat -c %y "$newest" 2>/dev/null || true)"
  echo "    --- tail ---"
  tail -n 25 "$newest" | sed 's/^/    /'
fi
echo ""

echo "[5] 스케줄 안내"
echo "  코인 북극성 = 매일 20:00 KST 만 (주간/월간 텔레그램 없음)"
echo "  주식 북극성 = 주식 VPS 19:30 — 코인 서버에서 돌리면 안 됨"
echo ""

if [[ "$DRY" -eq 1 ]]; then
  echo "[6] dry-run (전송 없음)"
  bash "${SCRIPT_DIR}/bitget.sh" --post-deploy-obs-digest --dry-run
elif [[ "$SEND" -eq 1 ]]; then
  echo "[6] 지금 텔레그램 1회 발송"
  bash "${SCRIPT_DIR}/bitget.sh" --post-deploy-obs-digest
  echo "  → 텔레그램에 「📊 코인 북극성 · Bitget」이 왔는지 확인"
else
  echo "[6] 미실행. 지금 보내려면:"
  echo "    bash bitget/deploy/diagnose_coin_digest.sh --send"
  echo "  미리보기만:"
  echo "    bash bitget/deploy/diagnose_coin_digest.sh --dry-run"
fi
