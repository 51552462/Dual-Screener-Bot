#!/usr/bin/env bash
# =============================================================================
# 팩토리 통합 업데이트 (B: systemd 전용)
#   저장소 루트에서: sudo ./update_factory.sh
#
# - git pull 이전: INSTALL_ROOT 및 DB_STORAGE_PATH 의 *.sqlite → /var/backups/dante-pre-update/<ts>/
# - git pull (ubuntu)
# - deploy_quant_factory.sh (유닛 반영, journald, daemon-reload, enable)
# - 코어·대시보드·비동기 텔레그램 재시작 + 타이머 재시작
#
# 환경 변수:
#   INSTALL_ROOT — git·코드 루트 (기본: 이 스크립트가 있는 디렉터리)
#   DEPLOY_USER  — git pull 수행 유저 (기본: ubuntu)
# =============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-$REPO_ROOT}"
export INSTALL_ROOT
DEPLOY_USER="${DEPLOY_USER:-ubuntu}"

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "root 로 실행하세요: sudo ./update_factory.sh" >&2
  exit 1
fi

if [[ ! -d "$INSTALL_ROOT" ]]; then
  echo "INSTALL_ROOT 없음: $INSTALL_ROOT" >&2
  exit 1
fi

# [1/6] git pull 전: 모든 주요 *.sqlite 를 타임스탬프 디렉터리에 백업 (데이터 유실 방지)
_dante_pre_update_sqlite_backup() {
  local stamp dest f base extra_dir
  stamp="$(date -u +%Y%m%d_%H%M%S_utc)"
  dest="/var/backups/dante-pre-update/${stamp}"
  mkdir -p "$dest"

  _sqlite_copy_one() {
    local src="$1" out="$2"
    if command -v sqlite3 &>/dev/null; then
      sqlite3 "$src" ".backup '$out'" 2>/dev/null || cp -a -- "$src" "$out"
    else
      cp -a -- "$src" "$out"
    fi
  }

  if [[ -d "$INSTALL_ROOT" ]]; then
    shopt -s nullglob
    for f in "$INSTALL_ROOT"/*.sqlite; do
      [[ -f "$f" ]] || continue
      base="$(basename "$f")"
      _sqlite_copy_one "$f" "$dest/$base"
    done
    shopt -u nullglob
  fi

  extra_dir=""
  if [[ -f "$INSTALL_ROOT/.env" ]] && command -v python3 &>/dev/null; then
    extra_dir="$(INSTALL_ROOT="$INSTALL_ROOT" python3 -c "
import os
p = os.path.join(os.environ.get('INSTALL_ROOT', '.'), '.env')
if not os.path.isfile(p):
    raise SystemExit
for line in open(p, encoding='utf-8', errors='ignore'):
    s = line.strip()
    if not s or s.startswith('#') or '=' not in s:
        continue
    k, _, v = s.partition('=')
    if k.strip() == 'DB_STORAGE_PATH':
        v = v.strip().strip('\"').strip(\"'\")
        print(os.path.expanduser(v))
        break
" 2>/dev/null || true)"
  fi
  if [[ -n "${extra_dir// }" && -d "$extra_dir" ]]; then
    local abs_extra
    abs_extra="$(cd "$extra_dir" && pwd)"
    local abs_root=""
    if [[ -d "$INSTALL_ROOT" ]]; then
      abs_root="$(cd "$INSTALL_ROOT" && pwd)"
    fi
    if [[ "$abs_extra" != "$abs_root" ]]; then
      shopt -s nullglob
      for f in "$extra_dir"/*.sqlite; do
        [[ -f "$f" ]] || continue
        base="dataroot__$(basename "$f")"
        _sqlite_copy_one "$f" "$dest/$base"
      done
      shopt -u nullglob
    fi
  fi
  echo "  pre-update sqlite backup → $dest"
}

echo "[1/6] pre-update SQLite 백업 → /var/backups/dante-pre-update/"
_dante_pre_update_sqlite_backup

echo "[2/6] git pull ($DEPLOY_USER) → $INSTALL_ROOT"
if [[ -d "$INSTALL_ROOT/.git" ]]; then
  sudo -u "$DEPLOY_USER" git -C "$INSTALL_ROOT" pull --ff-only
else
  echo "  (경고) .git 없음 — pull 생략"
fi

echo "[3/6] systemd 유닛 재배포 → $INSTALL_ROOT"
sudo INSTALL_ROOT="$INSTALL_ROOT" "$REPO_ROOT/deploy_quant_factory.sh"

echo "[4/6] 장기 서비스 재시작 (코어·대시보드·텔레그램 비동기)"
systemctl restart dante-factory.service dante-dashboard.service dante-async.service

echo "[5/6] 타이머 재시작 (스냅샷·워치독·DR백업 스케줄 반영)"
systemctl restart dante-snapshot.timer dante-watchdog.timer dante-backup.timer || true

echo ""
echo "=== is-active ==="
systemctl is-active dante-factory.service dante-dashboard.service dante-async.service || true
echo ""
echo "=== 심장·로그 한눈에 (Ctrl+C 종료) ==="
echo "sudo journalctl -u dante-factory -u dante-dashboard -u dante-async -u dante-watchdog -f"
echo ""
echo "update_factory 완료."

echo "[6/6] 배포 완료 텔레그램 알림"
if [[ -f "${REPO_ROOT}/deploy/ubuntu/post_update_notify.sh" ]]; then
  INSTALL_ROOT="$INSTALL_ROOT" bash "${REPO_ROOT}/deploy/ubuntu/post_update_notify.sh" || echo "  (경고) post_update_notify 실패 — 로그 확인" >&2
else
  echo "  (경고) post_update_notify.sh 없음" >&2
fi
