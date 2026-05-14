#!/usr/bin/env bash
# =============================================================================
# Dante DR 백업 — 핵심 자산 압축 후 S3 또는 rsync 대상으로 전송
#   SQLite 는 실행 중일 수 있으므로 sqlite3 ".backup" 으로 일관 복사본을 만든 뒤 tar 한다.
#
# 사용 전 환경 변수(또는 아래 기본값) 설정:
#   DATA_ROOT          — 팩토리 데이터 루트 (기본: INSTALL_ROOT 와 동일)
#   BACKUP_MODE        — "s3" | "rsync" | "local" (기본: s3)
#   S3_BUCKET          — 예: s3://my-bucket/dante-dr/
#   RSYNC_TARGET       — 예: user@backup-host:/backups/dante/
#   LOCAL_ARCHIVE_DIR  — BACKUP_MODE=local 일 때 .tar.gz 저장 디렉터리
#   RETENTION_DAYS     — LOCAL 만: 오래된 아카이브 삭제 (기본 21)
#
# crontab 예 (매일 새벽 3시):
#   0 3 * * * INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot S3_BUCKET=s3://my-bucket/prefix/ /home/ubuntu/dante_bots/Dual-Screener-Bot/backup_to_cloud.sh >>/var/log/dante-backup.log 2>&1
# =============================================================================
set -euo pipefail

INSTALL_ROOT="${INSTALL_ROOT:-/home/ubuntu/dante_bots/Dual-Screener-Bot}"
DATA_ROOT="${DATA_ROOT:-$INSTALL_ROOT}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
WORKDIR="${TMPDIR:-/tmp}/dante-dr-${STAMP}-$$"
BACKUP_MODE="${BACKUP_MODE:-s3}"
RETENTION_DAYS="${RETENTION_DAYS:-21}"

mkdir -p "${WORKDIR}/staging"
cleanup() { rm -rf "${WORKDIR}"; }
trap cleanup EXIT

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

_sqlite_backup() {
  local src="$1"
  local dest="$2"
  if [[ ! -f "$src" ]]; then
    log "skip (missing): $src"
    return 0
  fi
  if ! command -v sqlite3 >/dev/null 2>&1; then
    log "WARN: sqlite3 없음 — 원본 파일 복사로 대체: $src"
    cp -a "$src" "$dest"
    return 0
  fi
  sqlite3 "$src" ".backup '${dest}'"
  log "sqlite backup ok: $(basename "$src")"
}

_copy_plain() {
  local src="$1"
  local dest="$2"
  if [[ -f "$src" ]]; then
    cp -a "$src" "$dest"
    log "copied: $(basename "$src")"
  else
    log "skip (missing): $src"
  fi
}

# --- 핵심 자산 (경로는 DATA_ROOT 기준) ---
_sqlite_backup "${DATA_ROOT}/market_data.sqlite" "${WORKDIR}/staging/market_data.sqlite"
_sqlite_backup "${DATA_ROOT}/ops_events.sqlite" "${WORKDIR}/staging/ops_events.sqlite"
# 레거리/별칭 파일이 있으면 함께
if [[ -f "${DATA_ROOT}/ops_health.sqlite" ]]; then
  _sqlite_backup "${DATA_ROOT}/ops_health.sqlite" "${WORKDIR}/staging/ops_health.sqlite"
fi
_sqlite_backup "${DATA_ROOT}/message_queue.sqlite" "${WORKDIR}/staging/message_queue.sqlite"
_sqlite_backup "${DATA_ROOT}/system_config.sqlite" "${WORKDIR}/staging/system_config.sqlite"

_copy_plain "${DATA_ROOT}/system_config.json" "${WORKDIR}/staging/system_config.json"
# .env 는 기본 제외(유출 위험). 필요 시 별도 KMS·암호화 파이프라인으로만 백업할 것.

ARCHIVE_NAME="dante-dr-${STAMP}.tar.gz"
ARCHIVE_PATH="${WORKDIR}/${ARCHIVE_NAME}"
tar -C "${WORKDIR}/staging" -czf "${ARCHIVE_PATH}" .
log "archive: ${ARCHIVE_PATH} ($(du -h "${ARCHIVE_PATH}" | cut -f1))"

case "${BACKUP_MODE}" in
  s3)
    if [[ -z "${S3_BUCKET:-}" ]]; then
      log "ERROR: BACKUP_MODE=s3 인데 S3_BUCKET 이 비어 있습니다."
      exit 1
    fi
    if ! command -v aws >/dev/null 2>&1; then
      log "ERROR: aws CLI 가 없습니다."
      exit 1
    fi
    aws s3 cp "${ARCHIVE_PATH}" "${S3_BUCKET%/}/${ARCHIVE_NAME}"
    log "uploaded -> ${S3_BUCKET%/}/${ARCHIVE_NAME}"
    ;;
  rsync)
    if [[ -z "${RSYNC_TARGET:-}" ]]; then
      log "ERROR: BACKUP_MODE=rsync 인데 RSYNC_TARGET 이 비어 있습니다."
      exit 1
    fi
    rsync -av --progress "${ARCHIVE_PATH}" "${RSYNC_TARGET%/}/"
    log "rsync ok -> ${RSYNC_TARGET%/}/${ARCHIVE_NAME}"
    ;;
  local)
    LOCAL_ARCHIVE_DIR="${LOCAL_ARCHIVE_DIR:-/var/backups/dante}"
    mkdir -p "${LOCAL_ARCHIVE_DIR}"
    cp -a "${ARCHIVE_PATH}" "${LOCAL_ARCHIVE_DIR}/${ARCHIVE_NAME}"
    log "local copy -> ${LOCAL_ARCHIVE_DIR}/${ARCHIVE_NAME}"
    find "${LOCAL_ARCHIVE_DIR}" -maxdepth 1 -name 'dante-dr-*.tar.gz' -mtime "+${RETENTION_DAYS}" -delete 2>/dev/null || true
    log "retention: deleted archives older than ${RETENTION_DAYS} days (if any)"
    ;;
  *)
    log "ERROR: unknown BACKUP_MODE=${BACKUP_MODE}"
    exit 1
    ;;
esac

log "done."
