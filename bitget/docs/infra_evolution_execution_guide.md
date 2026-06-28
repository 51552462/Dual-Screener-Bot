# 🏛️ Institutional Single-Server Evolution — 구축 결과 & 실행 가이드

> 목적: 단일 서버 내 주식/코인 스케줄 충돌 + SQLite `database is locked` 데드락 근절.
> 전략: **(0) 무결성 백업 → (1) DAL 통합 → (2) 무중단 우선순위 큐 → (3) 서킷 브레이커.**
> 상태: 4개 미션 구현 완료 + 스모크 테스트 통과 (`ALL_SMOKE_OK`).

---

## 📦 산출물

| Mission | 파일 | 역할 |
|---|---|---|
| 0 | `bitget/scripts/institutional_db_backup.py` | Online Backup API + `integrity_check` + tar.gz + `RESTORE_GUIDE.md` 자동 생성 |
| 1 | `bitget/infra/shared_db_connector.py` | 전 시스템 표준 SQLite 커넥터(DAL) — Traffic Rule 강제 |
| 2 | `bitget/infra/task_orchestrator.py` | `task_queue.sqlite` 기반 무중단 우선순위 큐 |
| 3 | `bitget/watchdog.py` (+`bitget/infra/runtime.py` 연결) | 스캔 잡 서킷 브레이커 (Anti-Zombie) |

---

## Mission 0 — 기관급 무결성 백업

- `*.sqlite/.sqlite3/.db` 를 **헤더 magic** 까지 확인해 자동 식별 (`-wal/-shm/.tmp` 제외).
- 단순 복사가 아닌 **SQLite Online Backup API**(`Connection.backup()`) → 라이브 writer 가 돌아도 일관 스냅샷.
- 복제 직후 **`PRAGMA integrity_check` + `quick_check`** 자동 수행, 결과를 `manifest.json` 에 기록.
- 타임스탬프 tar.gz(`dual_screener_db_backup_<ts>.tar.gz`) 압축 + **`RESTORE_GUIDE.md` 자동 생성**.
- 무결성 실패가 1건이라도 있으면 **종료코드 2** (운영 스크립트가 감지).

```bash
# 전체 백업 (레포 루트 + bitget 데이터 디렉터리 자동 스캔)
python -m bitget.scripts.institutional_db_backup
# 압축 없이 staging 만 / 특정 루트 지정
python -m bitget.scripts.institutional_db_backup --no-compress --root /home/ubuntu/dante_bots/Dual-Screener-Bot
```

## Mission 1 — Global Data Access Layer (DAL)

`shared_db_connector.get_connection()` / `connect()` 가 모든 커넥션에 **Traffic Rule** 강제:

- `timeout = 60s` (하한 강제 — 60 미만 불가)
- `PRAGMA journal_mode = WAL`
- `PRAGMA synchronous = NORMAL`
- `PRAGMA busy_timeout = 60000`

```python
from bitget.infra.shared_db_connector import connect
with connect(db_path) as conn:               # 자동 commit/rollback/close
    conn.execute("INSERT ...")
with connect(db_path, read_only=True) as conn:  # mode=ro + query_only (writer 비차단)
    rows = conn.execute("SELECT ...").fetchall()
```

> ✅ 검증됨: `journal_mode=wal, synchronous=1(NORMAL), busy_timeout=60000`.
> 점진적 마이그레이션: 기존 `sqlite3.connect(...)` 들을 `get_connection(...)` 으로 치환하면
> 모듈별 7s~120s 들쭉날쭉 타임아웃 → 60s 통일.

## Mission 2 — 무중단 우선순위 큐

- 외부 의존성 없음(내장 `sqlite3`). 위치: `task_queue.sqlite` (env `TASK_QUEUE_DB_PATH` override).
- 겹치는 일정은 **Drop 하지 않고 PENDING 대기 → 순차 실행** (증발 없음).
- `claim_next()` 는 **`BEGIN EXCLUSIVE TRANSACTION`** 으로 다중 프로세스 동시 픽업(Race) 차단.
- **타임존 기반 권력 이양** (`primary_engine_now`):
  - KST 09:00~15:30 → **KR** Priority 1
  - ET 09:30~16:00 → **US** Priority 1
  - 그 외 / 주말 → **BITGET** Priority 1
- 픽업 순간 우선순위를 재평가 → 세션이 바뀌면 자동 후순위로 강등.
- 후순위 강제 동시 실행 시 `apply_cpu_throttle()` = **`os.nice(10)`** 로 CPU 스로틀 → 서버 다운 방지.

```python
from bitget.infra import task_orchestrator as to
to.enqueue("BITGET", "scan_spot_dante")        # dedupe: 동일 PENDING/RUNNING 이면 skip
to.drain(executor=my_runner)                   # PENDING 없을 때까지 순차 처리(+throttle)
```

> ✅ 검증됨: KR/US/주말 권력 이양, enqueue dedupe, 원자적 claim, fail→재시도/FAILED.

## Mission 3 — Watchdog 서킷 브레이커 (Anti-Zombie)

- 스캔 잡(`scan_*`)의 **연속 실패 횟수**를 `scan_circuit_breaker.json` 에 기록.
- **3회**(env `BITGET_SCAN_CB_THRESHOLD`) 연속 실패 → 회로 **OPEN** → 해당 차수 **폐기(차단)**.
- `runtime.dispatch_bitget_mode()` 가 스캔 실행 전 `is_circuit_open()` 확인 → OPEN 이면
  무거운 작업 없이 즉시 반환(좀비 5시간 스캔 + 알람 폭탄 차단).
- 성공 시 `record_job_success()` 가 회로를 닫고 카운터 초기화.
- OPEN 후 `BITGET_SCAN_CB_RESET_SEC`(기본 3600s) 경과 시 **half-open** 1회 탐침 허용 → 영구 마비 방지.

> ✅ 검증됨: 3회 실패 → OPEN, 차단, 성공 시 CLOSED 복귀.

---

## 🛠️ 시스템 재시작 순서 (System Owner Runbook)

> ⚠️ 순서를 지켜라. **반드시 데몬 정지 → 백업 → 정리 → 클린 부트.**

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot

# ── 1. 모든 데몬·타이머 정지 (DB writer 전면 차단) ──
sudo systemctl stop 'dante-bitget-*'      # ws / daemon / async / snapshot / watchdog
sudo systemctl stop 'dante-factory-*'     # 주식 측 (있다면)
crontab -l   # cron 잡이 도는 중이면 install 스크립트로 잠시 제거하거나 주석 처리

# ── 2. 기관급 무결성 백업 (Zero-Data-Loss) ──
python -m bitget.scripts.institutional_db_backup
#   → backups/db/dual_screener_db_backup_<ts>.tar.gz + RESTORE_GUIDE.md 생성
#   → 종료코드 0 = 전 DB integrity ok / 2 = 무결성 실패 (이 경우 멈추고 점검)

# ── 3. 기존 락 파일 + WAL 찌꺼기 정리 ──
#   (데몬이 모두 정지된 상태에서만! 살아있는 writer 가 있으면 절대 삭제 금지)
rm -f .factory_runtime.lock
find . -name '.bitget_runtime.lock' -o -name '.bitget_data_refresh.lock' | xargs -r rm -f
#   WAL/SHM 잔여물(정상 종료 시 자동 정리되지만 안전하게):
find . -name '*.sqlite-wal' -o -name '*.sqlite-shm' | xargs -r rm -f

# ── 4. 클린 부트 ──
sudo systemctl start 'dante-bitget-*'
# (cron 재설치)
sudo INSTALL_ROOT=$PWD bash bitget/deploy/install_bitget_cron.sh

# ── 5. 헬스 확인 ──
python -m bitget.pipelines.runner --mode health
python -m bitget.pipelines.runner --mode watchdog
```

### 롤백
장애 시 백업 tar.gz 안의 `RESTORE_GUIDE.md` 절차를 따른다(데몬 정지 → 해제 → DB 교체 → `integrity_check` → 재기동).

---

## 📌 후속 통합 권장 (점진 적용)

1. **DAL 전면 치환**: `forward/ledger.py`, `master_scanner.py`, `mtf_data_updater.py` 등 hot-path writer 의
   `sqlite3.connect(...)` → `shared_db_connector.get_connection(...)`. (가장 락 경합 잦은 곳 우선)
2. **큐 어댑터**: cron/`bitget.sh` 가 직접 `dispatch_bitget_mode` 하는 대신 `task_orchestrator.enqueue()` →
   단일 워커가 `drain()` 하도록 전환하면, 주식/코인 동시각 충돌이 "스킵"이 아니라 "대기 후 실행"으로 바뀐다.
3. **data_refresh 락 통합 검토**: 현재 `data_refresh` 만 별도 락이라 scan/track 과 동시 write →
   큐 도입 후에는 동일 워커 직렬화로 자연 해소.
