#!/usr/bin/env bash
# =============================================================================
# Dante DR — 핵심 SQLite·JSON 백업 (tar.gz)
#
# 대상(기본 DATA_ROOT 기준):
#   - market_data.sqlite
#   - ops_events.sqlite
#   - system_config.json
#
# SQLite 는 WAL 환경에서도 일관 복사를 위해 sqlite3 ".backup" 사용.
#
# 환경 변수:
#   DATA_ROOT        — 데이터·설정 파일 루트 (기본: INSTALL_ROOT)
#   INSTALL_ROOT      — 팩토리 코드 루트 (기본: /home/ubuntu/dante_bots/Dual-Screener-Bot)
#   BACKUP_DEST_DIR   — tar.gz 저장 디렉터리 (기본: /var/backups/dante-dr)
#   UPLOAD_MODE       — "none" | "s3" | "rsync" (기본: none)
#   S3_URI_PREFIX       — 예: s3://my-bucket/dante-dr/
#   RSYNC_TARGET        — 예: user@backup:/path/
#   RETENTION_DAYS      — BACKUP_DEST_DIR 내 오래된 백업 삭제 (기본 21일)
#
# crontab 예 (매일 03:15):
#   15 3 * * * INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot /home/ubuntu/dante_bots/Dual-Screener-Bot/deploy/ubuntu/backup_sqlite.sh >>/var/log/dante-sqlite-backup.log 2>&1
# =============================================================================
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
DATA_ROOT="${DATA_ROOT:-$INSTALL_ROOT}"
BACKUP_DEST_DIR="${BACKUP_DEST_DIR:-/var/backups/dante-dr}"
UPLOAD_MODE="${UPLOAD_MODE:-none}"
RETENTION_DAYS="${RETENTION_DAYS:-21}"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
WORKDIR="${TMPDIR:-/tmp}/dante-sqlite-bak-${STAMP}-$$"
mkdir -p "${WORKDIR}/staging"
cleanup() { rm -rf "${WORKDIR}"; }
trap cleanup EXIT

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

_sqlite_backup() {
  local src="$1"
  local dest="$2"
  if [[ ! -f "$src" ]]; then
    log "skip (없음): $src"
    return 0
  fi
  if command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "$src" ".backup '${dest}'"
    log "sqlite backup: $(basename "$src")"
  else
    log "WARN: sqlite3 없음 — 파일 복사로 대체: $(basename "$src")"
    cp -a "$src" "$dest"
  fi
}

_copy_if_exists() {
  local src="$1"
  local dest="$2"
  if [[ -f "$src" ]]; then
    cp -a "$src" "$dest"
    log "copied: $(basename "$src")"
  else
    log "skip (없음): $src"
  fi
}

_sqlite_backup "${DATA_ROOT}/market_data.sqlite" "${WORKDIR}/staging/market_data.sqlite"
_sqlite_backup "${DATA_ROOT}/ops_events.sqlite" "${WORKDIR}/staging/ops_events.sqlite"
_copy_if_exists "${DATA_ROOT}/system_config.json" "${WORKDIR}/staging/system_config.json"

ARCHIVE_NAME="dante-sqlite-${STAMP}.tar.gz"
ARCHIVE_PATH="${WORKDIR}/${ARCHIVE_NAME}"
tar -C "${WORKDIR}/staging" -czf "${ARCHIVE_PATH}" .
log "archive: ${ARCHIVE_PATH} ($(du -h "${ARCHIVE_PATH}" | cut -f1))"

mkdir -p "${BACKUP_DEST_DIR}"
install -m 0640 "${ARCHIVE_PATH}" "${BACKUP_DEST_DIR}/${ARCHIVE_NAME}"
log "saved: ${BACKUP_DEST_DIR}/${ARCHIVE_NAME}"

find "${BACKUP_DEST_DIR}" -maxdepth 1 -type f \( -name 'dante-sqlite-*.tar.gz' -o -name 'dante-dr-*.tar.gz' \) -mtime "+${RETENTION_DAYS}" -delete 2>/dev/null || true
log "retention: 생성 후 ${RETENTION_DAYS}일(기본 21) 초과 .tar.gz 삭제"

case "${UPLOAD_MODE}" in
  s3)
    if [[ -z "${S3_URI_PREFIX:-}" ]]; then
      log "ERROR: UPLOAD_MODE=s3 인데 S3_URI_PREFIX 가 비어 있습니다."
      exit 1
    fi
    command -v aws >/dev/null 2>&1 || { log "ERROR: aws CLI 없음"; exit 1; }
    aws s3 cp "${BACKUP_DEST_DIR}/${ARCHIVE_NAME}" "${S3_URI_PREFIX%/}/${ARCHIVE_NAME}"
    log "uploaded: ${S3_URI_PREFIX%/}/${ARCHIVE_NAME}"
    ;;
  rsync)
    if [[ -z "${RSYNC_TARGET:-}" ]]; then
      log "ERROR: UPLOAD_MODE=rsync 인데 RSYNC_TARGET 이 비어 있습니다."
      exit 1
    fi
    rsync -av "${BACKUP_DEST_DIR}/${ARCHIVE_NAME}" "${RSYNC_TARGET%/}/"
    log "rsync ok -> ${RSYNC_TARGET%/}/${ARCHIVE_NAME}"
    ;;
  none) log "UPLOAD_MODE=none — 로컬 백업만 완료" ;;
  *)
    log "ERROR: 알 수 없는 UPLOAD_MODE=${UPLOAD_MODE}"
    exit 1
    ;;
esac

log "done."
