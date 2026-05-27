# 데이터 복구 & 환경(ENV) 감사 (DATA_RECOVERY_AND_ENVIRONMENT_CHECK)

**작성일:** 2026-05-27  
**상태:** 서버 `/var/backups/dante-dr/` 에서 백업 파일이 존재하지 않아 복구 실패. `forward_trades` 가 0건으로 리셋됨.  
**목표:** (1) 남아있을지 모르는 백업을 마지막으로 수색 → (2) 없으면 “Day 1 모드”로 즉시 전환 → (3) 매일 자동 실행이 실제로 cron/systemd에 잡혀있는지 최종 점검.

---

## 0) 전제 확인(서버에서 1분 내 점검)

### 0.1 DB_STORAGE_PATH 와 forward_trades 0건 확정
```bash
INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot
DATA_ROOT=$(DB_STORAGE_PATH="${DB_STORAGE_PATH:-}" python3 -c 'from factory_data_paths import factory_data_dir; print(factory_data_dir())')
DB="$DATA_ROOT/market_data.sqlite"

echo "DATA_ROOT=$DATA_ROOT"
ls -lh "$DB" || true

sqlite3 "$DB" "SELECT COUNT(*) AS forward_trades_rows FROM forward_trades;" || true
sqlite3 "$DB" "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'KR_%' LIMIT 20;" || true
```

> `forward_trades_rows=0` 이고, `KR_*/US_*` 같은 OHLCV 테이블은 남아있다면: Day 1 전환 후 `--scan-*` 들이 새 forward_trades 를 자연 축적할 수 있습니다.

---

## 1) 로컬/서버 대체 백업본 수색 (P0)

### 1.1 이번 로컬 자동 수색 결과(파일명 기반)
- 이 프로젝트 폴더 내부에서: `market_data*.sqlite`, `*.sqlite`, `*.sqlite3`, `dante-sqlite-*.tar.gz` 후보를 **0건**으로 확인.
- 로컬 윈도우(`C:\\Users\\GoodLife` 하위)에서도 `market_data*.sqlite`, `market_data_snapshot.sqlite` 등 **0건**으로 확인.

> 결론: “자동 스캔 결과” 기준으로는 복원 후보(연속 데이터가 들어있는 sqlite)가 확인되지 않았습니다.

### 1.2 서버에서 “남아있을 가능성이 있는” 백업 위치 즉시 점검
서버에서 아래 순서대로 실행하세요.

```bash
ls -lah /var/backups/dante-dr/ || true
ls -lah /var/backups/ | tail -n 50 || true
ls -lah /tmp/ | rg -i "dante|restore|sqlite" || true
ls -lah "$HOME" | rg -i "dante|restore|sqlite" || true
```

### 1.3 (선택) “backup 아카이브 tar.gz” 흔적 찾기
```bash
ls -lah /var/backups/ | rg -i "dante.*tar\\.gz|dante-dr.*tar\\.gz" || true
```

### 1.4 복원 후보 발견 시 “안전 복원” 커맨드(스키마/스냅샷/파생 자산 포함)
백업 파일(예: `market_data.sqlite` 또는 `dante-sqlite-*.tar.gz`)을 하나라도 찾으면 아래 절차로만 복원하세요.

#### A) sqlite 파일이 바로 있는 경우(추천)
1) 후보 DB에서 `forward_trades` 건수가 충분한지 확인
```bash
DB_CAND=/path/to/market_data.sqlite
sqlite3 "$DB_CAND" "SELECT COUNT(*) FROM forward_trades;"
```

2) 서비스 중지 → 안전 백업 → 후보를 `DB_STORAGE_PATH` 로 이관(덮어쓰기)
```bash
set -euo pipefail

INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot
export DB_STORAGE_PATH="${DB_STORAGE_PATH:-/var/lib/dante-quant-factory/data}"

DATA_ROOT=$(DB_STORAGE_PATH="${DB_STORAGE_PATH}" python3 -c 'from factory_data_paths import factory_data_dir; print(factory_data_dir())')
TARGET_MAIN="$DATA_ROOT/market_data.sqlite"
TARGET_SNAP="$DATA_ROOT/market_data_snapshot.sqlite"

sudo systemctl stop dante-main dante-factory dante-dashboard dante-async 2>/dev/null || true

# 현재 운영 DB 보존(덮어쓰기 전)
sudo cp -a "$TARGET_MAIN" "${TARGET_MAIN}.bak.$(date -u +%Y%m%dT%H%M%SZ)" 2>/dev/null || true
sudo cp -a "$TARGET_SNAP" "${TARGET_SNAP}.bak.$(date -u +%Y%m%dT%H%M%SZ)" 2>/dev/null || true

# 후보 DB 반영
sudo cp -a "$DB_CAND" "$TARGET_MAIN"

# 스냅샷이 함께 존재하는 형태라면 같이 교체(없으면 snapshot timer가 다음 주기에 main을 백업)
if [ -f "${DB_CAND/_main/_snapshot}" ]; then
  sudo cp -a "${DB_CAND/_main/_snapshot}" "$TARGET_SNAP" || true
fi

sudo chown -R ubuntu:ubuntu "$DATA_ROOT"

# 스키마 검증 + 파생 CSV/MetaGovernor 재생성(행 데이터는 유지)
cd "$INSTALL_ROOT"
python3 -c 'from sqlite_schema_guard import ensure_market_db_core_schema; ensure_market_db_core_schema(heal=True, heal_snapshot=True)'
python3 -m factory_artifact_guard --force-csv --force-meta || true

sudo systemctl start dante-main dante-factory dante-dashboard dante-async 2>/dev/null || true
```

#### B) tar.gz 백업만 있는 경우
```bash
TAR=/tmp/dante-sqlite-YYYYMMDDTHHMMSSZ.tar.gz
INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot
export DB_STORAGE_PATH="${DB_STORAGE_PATH:-/var/lib/dante-quant-factory/data}"

sudo systemctl stop dante-main dante-factory dante-dashboard dante-async 2>/dev/null || true
mkdir -p /tmp/dante-restore && tar -xzf "$TAR" -C /tmp/dante-restore

DATA_ROOT=$(DB_STORAGE_PATH="${DB_STORAGE_PATH}" python3 -c 'from factory_data_paths import factory_data_dir; print(factory_data_dir())')
sudo cp -a /tmp/dante-restore/market_data.sqlite "$DATA_ROOT/market_data.sqlite"
sudo cp -a /tmp/dante-restore/market_data_snapshot.sqlite "$DATA_ROOT/market_data_snapshot.sqlite" 2>/dev/null || true

sudo chown -R ubuntu:ubuntu "$DATA_ROOT"
cd "$INSTALL_ROOT"
python3 -m factory_artifact_guard --force-csv --force-meta || true
sudo systemctl start dante-main dante-factory dante-dashboard dante-async 2>/dev/null || true
```

---

## 2) 과거 데이터 영구 유실 시나리오 확정 (Day 1 선언) (P0)

아래 조건 중 하나라도 만족하면 Day 1 모드로 확정합니다.

1. `/var/backups/dante-dr/` 에서 `dante-sqlite-*.tar.gz` 를 찾지 못함
2. 임의 복원 후보 파일을 찾아도 `forward_trades` 가 10건 이상(또는 과거 CLOSED 다수)이라는 조건을 만족 못함
3. 복원 시도 자체가 계속 실패/지연(운영 안정성이 더 중요)

이 경우 “어설픈 복구 시도”를 중단하고 **오늘부터 새 데이터 누적** 모드로 들어갑니다.

---

## 3) 최종 Day 1 우분투 시동 명령어(덮어쓰기 금지 전제)

아래는 “오늘부터 자동 누적”을 위한 **최종 시동 커맨드(권장: 한 번에 복붙)** 입니다.

> 변수: `INSTALL_ROOT` 와 `DB_STORAGE_PATH` 는 실제 서버 환경에 맞게 조정하세요.

```bash
set -euo pipefail

INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot
export DB_STORAGE_PATH="${DB_STORAGE_PATH:-/var/lib/dante-quant-factory/data}"

# 1) DB 스키마/파생 자산(Flow CSV, MetaGovernor) Self-heal
cd "$INSTALL_ROOT"
sudo mkdir -p "$DB_STORAGE_PATH"
sudo chown -R ubuntu:ubuntu "$DB_STORAGE_PATH"

sudo systemctl stop dante-main dante-factory dante-dashboard dante-async 2>/dev/null || true

# (1a) 파생 CSV/MetaGovernor 재생성 + forward_trades 스키마 heal
python3 -m factory_artifact_guard || true
python3 -c 'from sqlite_schema_guard import ensure_market_db_core_schema; print(ensure_market_db_core_schema())'

# 2) 즉시 seed (오늘 남은 일정에 맞춰 우선 KR 먼저 권장)
#    forward_trades 가 0이면 deep_dive/track이 “표본 부족”으로 degrade 될 수 있으므로 scan으로 새 행을 먼저 쌓습니다.
./factory.sh --scan-kr

# 3) 자동 실행(스케줄러) 재설치/갱신: cron 파일
sudo cp "$INSTALL_ROOT/deploy/factory.crontab.example" /etc/cron.d/dual-screener-factory
sudo chmod 644 /etc/cron.d/dual-screener-factory

# (서버 경로가 예시와 다르면) FACTORY= 값만 sed로 맞추세요.
sudo sed -i "s|^FACTORY=.*|FACTORY=${INSTALL_ROOT}|" /etc/cron.d/dual-screener-factory

sudo systemctl daemon-reload

# 4) systemd timer 확인/재시작(스냅샷/워치독/백업)
sudo systemctl enable --now dante-snapshot.timer dante-watchdog.timer dante-backup.timer 2>/dev/null || true

# 5) 서비스 복구
sudo systemctl start dante-main dante-factory dante-dashboard dante-async 2>/dev/null || true

echo "Day 1 started. Next: cron will run daily-kr and scans on schedule."
```

### 덮어쓰기(리셋) 사고를 “다시는” 막기 위한 운영 규칙(필수)
- `DB_STORAGE_PATH` 는 **코드 저장소와 분리된 절대 경로**로 고정(`INSTALL_ROOT/.env` 에 명시).
- 운영 DB 파일(`market_data.sqlite` 등)은 레포/동기화 대상에 넣지 않는다(로컬 업로드 금지).
- 서버에서는 **`scp/rsync --delete` 를 DB_STORAGE_PATH에 절대 사용하지 않는다.**

---

## 4) 무중단 자동화(Cron/Systemd) 구동 상태 점검 (P0)

### 4.1 systemd timer 상태 확인(스냅샷/워치독/백업)
```bash
sudo systemctl list-timers dante-snapshot.timer dante-watchdog.timer dante-backup.timer --no-pager
```

### 4.2 factory.sh --daily-kr 가 “실제 cron으로” 잡혀있는지
```bash
sudo cat /etc/cron.d/dual-screener-factory | rg "daily-kr"
sudo cat /etc/cron.d/dual-screener-factory | rg "scan-kr"
```

cron 반영 확인(서버 배포 환경에 따라 `cron` 또는 `crond` 이름이 다를 수 있음):
```bash
sudo systemctl status cron 2>/dev/null || sudo systemctl status crond 2>/dev/null || true
sudo systemctl reload cron 2>/dev/null || sudo systemctl reload crond 2>/dev/null || true
```

### 4.3 충돌 방지: main.py(데몬)가 factory.sh를 직접 호출하지 않는지
```bash
sudo journalctl -u dante-main -n 200 --no-pager | rg "factory\\.sh --daily|daily-kr|daily-us" || true
```

> 프로젝트 런타임 설계상 factory SSOT는 `factory.sh` + cron이며, `auto_forward_tester.run_daily_scheduler()` 는 “DISABLED” 상태입니다.

### 4.4 프로젝트 기준 “기대 스케줄”(참고)
리포지토리의 `deploy/factory.crontab.example` 에서:
- `--scan-kr` : 평일 KST 오전/중간 3회
- `--daily-kr` : 평일 KST 16:35 1회
또한 systemd timer(`deploy_quant_factory.sh`)는:
- `dante-snapshot.timer`
- `dante-watchdog.timer`
- `dante-backup.timer`
만 설치/운영합니다. 즉, `--daily-kr` 는 **systemd timer가 아니라 cron**으로 돌립니다.

---

## 5) 최종 결론(운영 지침)

1. 이 프로젝트/로컬(윈도우) 기준으로는 sqlite 백업 후보 파일을 찾지 못했습니다.
2. 따라서 백업이 100% 날아간 전제로 **Day 1 모드** 시동 커맨드를 제공합니다(3장).
3. 이후에는 cron이 `--scan-*`/`--daily-*` 를 계속 돌려 forward_trades 는 더 이상 0으로 되돌아가지 않게 됩니다.
4. 가장 중요한 “영구 자동화”의 본질은: **DB_STORAGE_PATH를 절대 경로로 고정하고, 레포/로컬 동기화가 그 경로를 덮어쓰지 않게 하는 것** 입니다.

