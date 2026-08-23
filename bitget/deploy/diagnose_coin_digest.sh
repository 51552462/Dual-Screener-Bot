#!/usr/bin/env bash
# =============================================================================
# 코인 북극성(POST_DEPLOY_OBS) 왜 안 오는지 진단 + 선택적 즉시 발송
#   cd INSTALL_ROOT && bash bitget/deploy/diagnose_coin_digest.sh
#   bash bitget/deploy/diagnose_coin_digest.sh --send    # 텔레그램 지금 1회 + 로그/결과 표시
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

echo "[3] 봇 채널 — REPORT(북극성) vs BITGET(AI감시관) ※ 채팅이 다르면 '안 온 것'처럼 보임"
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
rt, rc = (t.get_report_token() or '').strip(), (t.get_report_chat_id() or '').strip()
bt, bc = (t.get_bitget_bot_token() or '').strip(), (t.get_bitget_chat_id() or '').strip()
print('  REPORT_BOT token:', 'set' if rt else 'MISSING', ' chat_tail:', (rc[-4:] if len(rc)>=4 else rc or 'MISSING'))
print('  BITGET_BOT token:', 'set' if bt else 'MISSING', ' chat_tail:', (bc[-4:] if len(bc)>=4 else bc or 'MISSING'))
if rc and bc:
    print('  same chat?:', 'YES' if rc == bc else 'NO ← AI감시관 채팅만 보면 코인 북극성 안 보임')
elif not rc:
    print('  ✗ REPORT_BOT_CHAT_ID missing — 코인 북극성 발송 불가')
" 2>/dev/null || warn "telegram_env 조회 실패"
else
  warn "python/venv 없음"
fi
if [[ "${POST_DEPLOY_OBS_DIGEST_ENABLED:-}" == "0" || "${POST_DEPLOY_OBS_DIGEST_ENABLED:-}" == "false" ]]; then
  fail "POST_DEPLOY_OBS_DIGEST_ENABLED=${POST_DEPLOY_OBS_DIGEST_ENABLED} → 발송 스킵"
fi
echo ""

LOG_DIR="${BITGET_LOG_DIR:-${BITGET_ROOT}/logs}"
# data path override (VPS often /var/lib/quant-bitget/logs)
if [[ -x "$PY" ]]; then
  _ld="$("$PY" -c 'from bitget.infra.data_paths import logs_dir; print(logs_dir())' 2>/dev/null || true)"
  [[ -n "${_ld:-}" ]] && LOG_DIR="$_ld"
fi
# VPS 관측 경로 (env 미로드 시)
if [[ ! -d "$LOG_DIR" && -d /var/lib/quant-bitget/logs ]]; then
  LOG_DIR=/var/lib/quant-bitget/logs
fi

echo "[4] 최근 실행 로그 ($LOG_DIR/bitget_post_deploy_obs_*.log)"
shopt -s nullglob
logs=("$LOG_DIR"/bitget_post_deploy_obs_*.log)
if ((${#logs[@]} == 0)); then
  fail "로그 0개 — cron이 한 번도 안 돌았거나 LOG_DIR 다름 ($LOG_DIR)"
else
  newest="$(ls -t "${logs[@]}" | head -1)"
  pass "최신: $newest"
  echo "    mtime: $(date -r "$newest" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || stat -c %y "$newest" 2>/dev/null || true)"
  echo "    --- tail ---"
  tail -n 40 "$newest" | sed 's/^/    /'
fi
echo ""

echo "[5] 스케줄 안내"
echo "  코인 북극성 = 매일 20:00 KST 만 · REPORT_BOT 채팅"
echo "  AI 감시관 = BITGET_BOT 채팅 (다를 수 있음)"
echo "  주식 북극성 = 주식 VPS 19:30 — 코인 서버 금지"
echo ""

if [[ "$DRY" -eq 1 ]]; then
  echo "[6] dry-run (전송 없음 · 터미널에 JSON 출력)"
  set +e
  bash "${SCRIPT_DIR}/bitget.sh" --post-deploy-obs-digest --dry-run
  rc=$?
  set -e
  echo "  exit=$rc"
elif [[ "$SEND" -eq 1 ]]; then
  echo "[6] 지금 텔레그램 1회 발송 (결과는 로그+아래 요약)"
  before_newest=""
  shopt -s nullglob
  _b=("$LOG_DIR"/bitget_post_deploy_obs_*.log)
  ((${#_b[@]} > 0)) && before_newest="$(ls -t "${_b[@]}" | head -1)"
  set +e
  bash "${SCRIPT_DIR}/bitget.sh" --post-deploy-obs-digest
  rc=$?
  set -e
  echo "  bitget.sh exit=$rc"
  sleep 1
  shopt -s nullglob
  _a=("$LOG_DIR"/bitget_post_deploy_obs_*.log)
  after_newest="$(ls -t "${_a[@]}" 2>/dev/null | head -1 || true)"
  echo "  log: ${after_newest:-none}"
  if [[ -n "${after_newest:-}" ]]; then
    echo "  --- 이번 실행 로그 ---"
    tail -n 60 "$after_newest" | sed 's/^/    /'
    if grep -q '"sent": true' "$after_newest" 2>/dev/null \
      || grep -q '"sent":true' "$after_newest" 2>/dev/null; then
      pass "JSON sent=true — REPORT_BOT 채팅을 확인 (AI감시관 채팅과 다를 수 있음)"
    elif grep -qiE 'send HTTP|send failed|send skipped|REPORT_BOT send failed|"sent": false' "$after_newest" 2>/dev/null; then
      fail "발송 실패 흔적 — 위 로그의 HTTP/error 줄 확인"
    elif [[ "$rc" -ne 0 ]]; then
      fail "exit=$rc — 로그에 traceback/disabled 있는지 확인"
    else
      warn "sent=true 문자열 없음 — 로그 전체 확인: less $after_newest"
    fi
  fi
  if [[ "$rc" -ne 0 ]]; then
    exit "$rc"
  fi
else
  echo "[6] 미실행. 지금 보내려면:"
  echo "    bash bitget/deploy/diagnose_coin_digest.sh --send"
  echo "  미리보기만:"
  echo "    bash bitget/deploy/diagnose_coin_digest.sh --dry-run"
fi
