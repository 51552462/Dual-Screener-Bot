#!/usr/bin/env bash
# =============================================================================
# 팩토리 통합 업데이트 (B: systemd 전용)
#   저장소 루트에서: sudo ./update_factory.sh
#
# Zero Data Loss:
#   - git pull 이전: *.sqlite / system_config.json 등 → /var/backups/dante-pre-update/
#   - DB·JSON 삭제·DROP·init_db 금지 — 스키마는 ALTER TABLE 마이그레이션만
#   - 구버전(.venv) 파이썬 프로세스만 안전 종료 후 venv 엔진으로 재기동
#
#   git pull (ubuntu) → deploy_quant_factory.sh → 엔진 교체 → 서비스 재시작
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

# shellcheck source=deploy/dante_venv.sh
source "${REPO_ROOT}/deploy/dante_venv.sh"

if [[ "${EUID:-0}" -ne 0 ]]; then
  echo "root 로 실행하세요: sudo ./update_factory.sh" >&2
  exit 1
fi

if [[ ! -d "$INSTALL_ROOT" ]]; then
  echo "INSTALL_ROOT 없음: $INSTALL_ROOT" >&2
  exit 1
fi

DANTE_PY="$(dante_resolve_python "$INSTALL_ROOT" || true)"
if [[ -z "${DANTE_PY:-}" ]]; then
  echo "가상환경 없음: ${INSTALL_ROOT}/venv 또는 .venv — 엔진 교체 중단" >&2
  exit 1
fi
echo "  venv python: $DANTE_PY"

# [1/7] git pull 전: *.sqlite + 핵심 파생 상태 파일을 타임스탬프 디렉터리에 백업
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

  echo "  pre-update backup → $dest (sqlite + artifacts, zero deletion)"
}

# 구버전(.venv) 인터프리터로 INSTALL_ROOT 아래에서 돌아가는 잔존 프로세스만 종료 (DB/JSON untouched)
_dante_stop_stale_factory_processes() {
  local root="$1"
  local user="${2:-ubuntu}"
  local root_canon old_py
  root_canon="$(cd "$root" && pwd -P)"
  old_py="${root_canon}/.venv/bin/python"

  if ! command -v pgrep &>/dev/null; then
    echo "  (경고) pgrep 없음 — systemd stop 만 수행" >&2
    return 0
  fi

  local -a pids=()
  local pid cmd
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    cmd="$(tr '\0' ' ' <"/proc/${pid}/cmdline" 2>/dev/null || true)"
    [[ "$cmd" == *"${root_canon}"* ]] || continue
    if [[ "$cmd" == *"${old_py}"* ]] \
      || [[ "$cmd" == *"factory_launcher.py"* ]] \
      || [[ "$cmd" == *"async_telegram_daemon.py"* ]] \
      || [[ "$cmd" == *"main.py"* && "$cmd" == *"${root_canon}"* ]]; then
      pids+=("$pid")
    fi
  done < <(pgrep -u "$user" -f "${root_canon}" 2>/dev/null || true)

  if [[ "${#pids[@]}" -eq 0 ]]; then
    echo "  잔존 구버전/팩토리 프로세스 없음"
    return 0
  fi

  echo "  SIGTERM → stale PIDs: ${pids[*]}"
  for pid in "${pids[@]}"; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  sleep 3
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      echo "  SIGKILL (잔존): $pid"
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done
}

# 기존 SQLite에 ADD COLUMN 마이그레이션만 적용 (DROP/DELETE 없음)
_dante_apply_schema_migrations() {
  echo "  schema guard (ALTER TABLE only, no DROP)..."
  sudo -E -u "$DEPLOY_USER" env INSTALL_ROOT="$INSTALL_ROOT" PYTHONPATH="$INSTALL_ROOT" \
    "$DANTE_PY" - <<'PY' || echo "  (경고) schema migration 일부 스킵" >&2
import glob
import os
import sqlite3

import sqlite_schema_guard

root = os.environ.get("INSTALL_ROOT", ".")
paths = set()
for pattern in (os.path.join(root, "*.sqlite"), os.path.join(root, "data", "*.sqlite")):
    for p in glob.glob(pattern):
        if os.path.isfile(p):
            paths.add(p)
try:
    from factory_data_paths import factory_data_dir
    dr = factory_data_dir()
    for p in glob.glob(os.path.join(dr, "*.sqlite")):
        if os.path.isfile(p):
            paths.add(p)
except Exception:
    pass

for db in sorted(paths):
    try:
        conn = sqlite3.connect(db, timeout=30)
        for table in sqlite_schema_guard.KNOWN_COLUMN_MIGRATIONS:
            sqlite_schema_guard.apply_column_migrations(conn, table)
        conn.commit()
        conn.close()
        print(f"    migrated: {db}")
    except Exception as e:
        print(f"    skip {db}: {e}")
PY
}

echo "[1/7] pre-update SQLite 백업 → /var/backups/dante-pre-update/"
_dante_pre_update_sqlite_backup

echo "[2/7] git pull ($DEPLOY_USER) → $INSTALL_ROOT"
if [[ -d "$INSTALL_ROOT/.git" ]]; then
  sudo -u "$DEPLOY_USER" git -C "$INSTALL_ROOT" pull --ff-only
else
  echo "  (경고) .git 없음 — pull 생략"
fi

echo "[3/7] systemd 유닛 재배포 (ExecStart → venv) → $INSTALL_ROOT"
sudo INSTALL_ROOT="$INSTALL_ROOT" "$REPO_ROOT/deploy_quant_factory.sh"

echo "[4/7] 장기 서비스 graceful stop (데이터 파일 untouched)"
systemctl stop dante-factory.service dante-dashboard.service dante-async.service 2>/dev/null || true
sleep 2

echo "[5/7] 구버전(.venv) 잔존 프로세스 안전 종료 + 스키마 마이그레이션"
_dante_stop_stale_factory_processes "$INSTALL_ROOT" "$DEPLOY_USER"
_dante_apply_schema_migrations

echo "[6/7] 최신 venv 엔진으로 서비스 재기동"
systemctl daemon-reload
systemctl restart dante-factory.service dante-dashboard.service dante-async.service

echo "[7/7] 타이머 재시작 (스냅샷·워치독·DR백업 스케줄 반영)"
systemctl restart dante-snapshot.timer dante-watchdog.timer dante-backup.timer || true

echo ""
echo "=== is-active ==="
systemctl is-active dante-factory.service dante-dashboard.service dante-async.service || true
echo ""
echo "=== 심장·로그 한눈에 (Ctrl+C 종료) ==="
echo "sudo journalctl -u dante-factory -u dante-dashboard -u dante-async -u dante-watchdog -f"
echo ""
echo "update_factory 완료 — 데이터 100% 보존, 엔진 venv 교체."

if [[ -f "${REPO_ROOT}/deploy/ubuntu/post_update_notify.sh" ]]; then
  INSTALL_ROOT="$INSTALL_ROOT" bash "${REPO_ROOT}/deploy/ubuntu/post_update_notify.sh" || echo "  (경고) post_update_notify 실패 — 로그 확인" >&2
else
  echo "  (경고) post_update_notify.sh 없음" >&2
fi
