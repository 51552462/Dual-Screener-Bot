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

# [1/6] git pull 전: *.sqlite + 핵심 파생 상태 파일을 타임스탬프 디렉터리에 백업
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

  _plain_copy_if_exists() {
    local src="$1" out="$2"
    if [[ -f "$src" ]]; then
      cp -a -- "$src" "$out"
      echo "  copied artifact: $(basename "$out")"
    fi
  }

  _backup_sqlite_named_from_dir() {
    local dir="$1" name="$2" prefix="${3:-}"
    [[ -d "$dir" ]] || return 0
    local src="${dir}/${name}"
    [[ -f "$src" ]] || return 0
    if [[ -n "$prefix" ]]; then
      _sqlite_copy_one "$src" "${dest}/${prefix}${name}"
    else
      _sqlite_copy_one "$src" "${dest}/${name}"
    fi
    echo "  copied sqlite: ${prefix}${name}"
  }

  _backup_artifacts_from_dir() {
    local dir="$1" prefix="${2:-}"
    [[ -d "$dir" ]] || return 0
    # MetaGovernor SSOT (config_kv) — git clean 내성
    _backup_sqlite_named_from_dir "$dir" "system_config.sqlite" "$prefix"
    local rel
    for rel in \
      Supernova_Flow_Tracking_Master.csv \
      meta_governor_state.json \
      validated_live_mutants.json \
      system_config.json \
      news_data.sqlite; do
      if [[ -f "${dir}/${rel}" ]]; then
        if [[ -n "$prefix" ]]; then
          _plain_copy_if_exists "${dir}/${rel}" "${dest}/${prefix}${rel}"
        else
          _plain_copy_if_exists "${dir}/${rel}" "${dest}/${rel}"
        fi
      fi
    done
  }

  if [[ -d "$INSTALL_ROOT" ]]; then
    shopt -s nullglob
    # market_data.sqlite 는 *.sqlite glob + data root 백업으로 이중 커버
    for f in "$INSTALL_ROOT"/*.sqlite; do
      [[ -f "$f" ]] || continue
      base="$(basename "$f")"
      _sqlite_copy_one "$f" "$dest/$base"
    done
    shopt -u nullglob
    _backup_artifacts_from_dir "$INSTALL_ROOT"
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
      _backup_artifacts_from_dir "$extra_dir" "dataroot__"
    fi
  fi
  # meta_state_log JSON 덤프 (git clean 내성 — MetaGovernor SSOT)
  if command -v python3 &>/dev/null; then
    INSTALL_ROOT="$INSTALL_ROOT" python3 -c "
import json, os, sqlite3
root = os.environ.get('INSTALL_ROOT', '.')
paths = []
for base in (root, os.path.join(root, 'data')):
    p = os.path.join(base, 'market_data.sqlite')
    if os.path.isfile(p):
        paths.append(p)
try:
    from market_db_paths import MARKET_DATA_DB_PATH
    if os.path.isfile(MARKET_DATA_DB_PATH):
        paths.append(MARKET_DATA_DB_PATH)
except Exception:
    pass
seen = set()
for db in paths:
    if db in seen:
        continue
    seen.add(db)
    try:
        conn = sqlite3.connect(db, timeout=15)
        cur = conn.execute(
            \"SELECT name FROM sqlite_master WHERE type='table' AND name='meta_state_log'\"
        )
        if not cur.fetchone():
            conn.close()
            continue
        rows = conn.execute('SELECT * FROM meta_state_log ORDER BY rowid DESC LIMIT 5').fetchall()
        cols = [d[0] for d in conn.execute('PRAGMA table_info(meta_state_log)').fetchall()]
        conn.close()
        out = os.path.join('${dest}', 'meta_state_log_dump.json')
        payload = [dict(zip(cols, r)) for r in rows]
        with open(out, 'w', encoding='utf-8') as f:
            json.dump({'db': db, 'rows': payload}, f, ensure_ascii=False, indent=2, default=str)
        print('  meta_state_log dump:', out)
    except Exception as e:
        print('  meta_state_log dump skip:', e)
" 2>/dev/null || true
  fi

  echo "  pre-update backup → $dest (sqlite + artifacts)"
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
