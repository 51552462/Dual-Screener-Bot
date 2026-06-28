# 🏛️ Bitget 인프라 진화 — 진행 내역 & 다음 단계 (핸드오프)

> 최종 갱신: 2026-06-28
> 목적: `database is locked` 근본 해결을 위한 단일 서버 진화 작업의 **전체 경과**와
> **남은 선택지**를 한 문서에 정리한다. (신규 합류자/재개 시 이 문서부터 읽으면 됨)

---

## 0. 한 줄 요약

- **원인**: 크로스 시스템(주식↔코인) 시간대 충돌이 아니라, **Bitget 코인 스택 내부**의
  다중 writer(데이터 갱신·24/7 WS·스냅샷·`track_positions`)가 같은
  `bitget_market_data.sqlite` 에 **제각각 타임아웃 + WAL 미적용**으로 접속해 생긴 내부 경합.
- **해결**: ① 무결성 백업 → ② 전역 DAL(연결 규칙 통일) → ③ 무중단 우선순위 큐 → ④ 서킷 브레이커.
- **현재 상태**: Mission 0~3 구현·검증 완료. DAL 치환 **P0~P3 완료**.
  cron→큐 어댑터는 **Phase b-1(인프라) 완료**, 실제 cron 전환(b-2/b-3)은 운영자 결정 대기.

---

## 1. 진단 단계 (코드 수정 전 검토)

문서: `docs/cron_timezone_db_lock_review.md`

- 한국/미국 퀀트 크론·시간대: **건전·격리됨**. 코인과 분 단위 충돌 회피 설계 확인.
- 코인(Bitget) 크론: 주식과 분 단위 충돌은 피하나 **내부 경합** 존재.
- `database is locked` 의 5대 내부 원인:
  1. 다중 writer 동시 접속(data_refresh / WS 데몬 / snapshot / track_positions).
  2. 모듈마다 `busy_timeout` 불일치(7s~120s, 일부 미설정).
  3. 일부 writer 의 WAL 미적용.
  4. `track_positions` 등 장기 트랜잭션이 양보하지 않음.
  5. 락 경합 시 "스킵(증발)"되어 차수 누락 발생.

---

## 2. Mission 0~3 — 핵심 아키텍처 (구현 완료)

### Mission 0 — 기관급 무결성 백업
- 파일: `scripts/institutional_db_backup.py`
- 모든 SQLite 자동 탐색 → `sqlite3.Connection.backup()` **온라인 백업** →
  `PRAGMA integrity_check` → `tar.gz`(타임스탬프) 아카이브 + `RESTORE_GUIDE.md` 생성.

### Mission 1 — 전역 데이터 접근 계층(DAL)
- 파일: `infra/shared_db_connector.py`
- `get_connection()` 단일 경로로 **busy_timeout=60s · WAL · synchronous=NORMAL** 강제.
- 읽기 전용은 `read_only=True` → `mode=ro` URI + `query_only=ON` 자동.
- 트래픽 규칙 상수(`DEFAULT_TIMEOUT_SEC=60`, `BUSY_TIMEOUT_MS=60000`)는 전 시스템 일관성의 핵심.

### Mission 2 — 무중단 우선순위 큐 (Zero-Dependency)
- 파일: `infra/task_orchestrator.py`
- 내장 `sqlite3` 만으로 `task_queue.sqlite` 구축.
- 충돌 시 Drop 대신 **PENDING 대기 → 단일 워커 순차 실행**.
- `BEGIN EXCLUSIVE TRANSACTION` 으로 다중 프로세스 동시 픽업(Race) 차단.
- **타임존 권력 이양**: KST 09:00~15:30→KR, ET 09:30~16:00→US, 그 외/주말→BITGET (Priority 1).
- 후순위 강제 동시 실행 시 `os.nice(10)` CPU 스로틀.
- 상태머신: `PENDING → RUNNING → DONE | FAILED` (실패 시 backoff 재시도).

### Mission 3 — 스캔 서킷 브레이커 (Anti-Zombie)
- 파일: `watchdog.py`(+ `infra/runtime.py` 연동)
- 스캔 잡 연속 실패 누적 → 3회 이상이면 회로 OPEN → 좀비 재시도 차단(`scan_circuit_breaker.json`).
- `runtime.dispatch_bitget_mode` 에서 락 획득 전 `is_circuit_open()` 검사,
  완료 후 `record_job_success/failure` 반영.

---

## 3. (a) DAL 전면 치환 — **P0~P3 완료**

모든 직접 `sqlite3.connect` + 수동 PRAGMA 를 `get_connection()` 으로 치환.
(쿼리·스키마 불변, 연결 옵션만 통일 → 행동 변화 없음)

| 단계 | 대상 | 처리 |
|---|---|---|
| 🔴 P0 | `forward/ledger.py`, `master_scanner.py`, `mtf_data_updater.py` | 핫 writer/reader 통일 (락 경합 최대 진원지) |
| 🟠 P1 | `forward/shared.py`, `forward/reports.py`, `forward/execution_bridge.py`, `forward/forward_trade_identity.py`, `forward/forward_book_integrity.py`, `forward/practitioner_bitget_adapter.py`, `shadow_tracking.py`, `shadow_performance_tracker.py`, `trading/reconciliation.py` | 포워드 북·트레이딩 writer/reader |
| 🟡 P2 | `supernova_hunter.py`, `data_miner.py`, `alt_data_miner.py`, `sentiment_miner.py`, `pump_forensics.py`, `underdog_miner.py`, `forensics_pioneer.py`, `blackhole_hunter.py`, `auto_pilot.py`, `system_auto_pilot.py`, `ai_overseer.py` | 보조 엔진·마이너 (리더는 `read_only=True`) |
| 🟢 P3 | `heatmap_dashboard.py`, `dashboard.py`, `toxic_graveyard_analyzer.py`, `reports/bitget_report_context.py`, `validation/pnl_parity.py`, `validation/load_test.py`, `time_machine_backtester.py` | 읽기 전용 → `read_only=True` |

**의도적으로 raw `sqlite3.connect` 유지(치환 대상 아님)**
- `shared_db_connector.py`(DAL 본체), `infra/task_orchestrator.py`(BEGIN EXCLUSIVE 큐),
  `scripts/institutional_db_backup.py`·`infra/snapshot_service.py`(`backup()` API), `tests/*`.

**아직 통일 안 한 경합 무관 인프라(선택)**
- `infra/ops_logger.py`, `infra/config_manager.py`, `infra/artifact_guard.py`, `watchdog.py:71`
- 각자 **전용 DB**(ops_events / config / artifact)를 쓰고 `ops_logger` 는 `apply_oom_safe_pragmas`
  저RAM 튜닝 + 재시도 로직이 얽혀 있음 → 시장 DB 경합과 무관해 보류(원하면 별도 단계).

---

## 4. (b) cron→큐 어댑터 — **Phase b-1(인프라) 완료**

cron → `bitget.sh <flag>` → 인라인 실행(충돌 시 SKIP/증발) 구조를
**enqueue → 단일 워커 순차 실행(충돌 시 PENDING 대기)** 으로 전환하는 어댑터.

| 산출물 | 경로 | 역할 |
|---|---|---|
| enqueue 진입점 | `pipelines/runner.py --enqueue` | 적재만 하고 즉시 종료(코인=BITGET 엔진, `_engine_for_mode`) |
| 워커 데몬 | `pipelines/queue_worker.py` (신규) | 큐 drain → `dispatch_bitget_mode`. `FAIL` 만 큐 재시도 |
| 셸 패스스루 | `deploy/bitget.sh --enqueue` | cron 라인에서 enqueue 전달 |
| systemd 유닛 | `deploy/systemd/dante-bitget-queue-worker.service.in` | `Restart=always` 단일 인스턴스 |
| 엔트리포인트 | `deploy/entrypoints/run_bitget_queue_worker.sh` | venv/.env 로드 후 워커 실행 |
| 배포 등록 | `deploy/deploy_bitget_factory.sh` | 활성화 목록에 워커 추가 |
| cron 토글 | `deploy/generate_bitget_crontab.py --use-queue` | 스캔 라인만 `--enqueue` 생성(**기본 off**) |

> **기본 cron 은 inline 유지** → 커밋된 `bitget.crontab.example` 불변(drift 0), 즉시 롤백 가능.
> ops(track/reconcile/data-refresh 등 경량)는 큐 모드에서도 inline 직접 실행 유지.

---

## 5. 검증 요약

- DAL: 대상 파일 `sqlite3.connect` 잔여 0건, `py_compile` 전부 통과, 패키지 정상 순서 import OK.
  - (참고) `reports/bitget_report_context.py` 단독 최상위 import 시 드러나는 `forward↔reports`
    순환참조는 **기존 구조 이슈**(이번 변경 무관). 정상 로드 순서에서는 문제없음.
- 큐: enqueue→drain 왕복 `{'PENDING':1}`→`{'DONE':1}`, dedupe 차단, 주말 BITGET priority=1,
  워커 종료코드 0 스모크 통과.
- cron 생성기: 기본 `--check` drift 없음 / `--use-queue` 는 스캔만 `--enqueue`, ops inline 유지.
- 환경 메모: Windows 콘솔 출력은 `PYTHONIOENCODING=utf-8` 로 실행(em dash `cp949` 표시 이슈 회피).

---

## 6. 운영 전환 런북 (b-2 카나리 → b-3 전체 / 롤백)

```bash
# 0) 배포 후 워커 기동 (shadow: cron 은 아직 inline)
sudo bash bitget/deploy/deploy_bitget_factory.sh
sudo systemctl start dante-bitget-queue-worker
journalctl -u dante-bitget-queue-worker -f       # "queue worker started" 확인

# 1) 수동 왕복 검증 (b-1)
bitget/deploy/bitget.sh --enqueue --scan-spot-supernova
python -c "from bitget.infra.task_orchestrator import queue_stats; print(queue_stats())"
#   → {'PENDING':1} → 잠시 후 {'DONE':1}

# 2) 카나리 (b-2): scan-spot 라인 1개만 손으로 `--enqueue` 추가 후 설치, 24h 관찰
sudo INSTALL_ROOT=<root> bash bitget/deploy/install_bitget_cron.sh

# 3) 전체 전환 (b-3): SSOT 에서 전 스캔 라인 일괄 enqueue 생성
python bitget/deploy/generate_bitget_crontab.py --use-queue --install-root <root>
sudo INSTALL_ROOT=<root> bash bitget/deploy/install_bitget_cron.sh

# 롤백 (즉시): inline 재생성 + 워커 정지
python bitget/deploy/generate_bitget_crontab.py --install-root <root>   # use-queue 없음
sudo INSTALL_ROOT=<root> bash bitget/deploy/install_bitget_cron.sh
sudo systemctl stop dante-bitget-queue-worker
```

모니터링: `queue_stats()` PENDING 적체 알람 / 워커 `Restart=always` + watchdog 하트비트 /
후순위 작업 `os.nice(10)`(top NI 컬럼) 확인.

---

## 7. 큐 SPOF 안전망 — **Option C/D 완료 (2026-06-28)**

단일 워커(SPOF)를 2중으로 방어. 별도 데몬 추가 없이 **기존 워치독 타이머
(`dante-bitget-watchdog.timer`, `*/5`)** 가 매 5분 능동 점검한다.

### Mission 1 — 큐 적체(Backlog) 능동 알람 (Option C)
- `infra/task_orchestrator.py`: `backlog_stats()` / `oldest_pending_age_sec()` 추가.
  - `backlog_stats() → {pending, running, done, failed, oldest_pending_age_sec}`.
- `watchdog._monitor_queue_safety()`: `PENDING ≥ 임계(기본 3)` **또는**
  `가장 오래된 PENDING 대기 ≥ 임계(기본 15분)` 이면
  `🚨 [CRITICAL: Queue Backlog Alert - 작업 적체 발생]` 텔레그램 발송.

### Mission 2 — 워커 하트비트 생존 감시 (Option D)
- `pipelines/queue_worker.py`: drain 루프가 **매 반복(유휴 포함) + 매 작업 직후 + 작업 착수 시**
  `.queue_worker_heartbeat`(큐 DB 와 같은 디렉터리)에 타임스탬프/PID/상태를 원자적 기록.
  - `task_orchestrator`: `touch_worker_heartbeat()` / `read_worker_heartbeat()` /
    `worker_heartbeat_age_sec()` + `drain(on_tick=...)` 진행 콜백.
- `watchdog._monitor_queue_safety()`: 하트비트가 `stale(기본 10분)` 이상이고 **처리 대기/진행
  작업이 있을 때만** `🚨 [CRITICAL: Queue Worker Dead/Hung - 워커 무응답]` 발송 후 재시작 안내.

### 오탐 방지 설계 (인라인 단계 안전)
- 하트비트 파일이 **없으면**(워커 미가동) → 경보 안 함 → 인라인 운영(b-1 이전)에서 무알람.
- 워커 사망 경보는 `PENDING>0 또는 RUNNING>0` 일 때만 → 빈 큐/롤백 중 정상 정지에 오탐 없음.
- 카테고리별 텔레그램 **쿨다운 분리**(`queue_backlog`/`queue_worker`/`watchdog`).

### 검증 (스모크 통과)
- PENDING 3건 → `backlog_stats` pending=3 + oldest age 산출.
- 하트비트 없음→`None`, touch 후 `~0s`.
- 워치독 모니터가 적체/워커사망 CRITICAL 2건 발송 경로 진입(자격증명 없으면 no-op).
- 빈 큐 + 하트비트 부재 → 무알람(인라인 안전) 확인.

### 환경 변수
| 키 | 기본값 | 의미 |
|---|---|---|
| `BITGET_QUEUE_MONITOR_ENABLED` | `1` | 큐 모니터 on/off |
| `BITGET_QUEUE_BACKLOG_PENDING_THRESHOLD` | `3` | PENDING 경보 임계(개수) |
| `BITGET_QUEUE_BACKLOG_AGE_SEC` | `900` | 가장 오래된 PENDING 대기 경보 임계(초) |
| `BITGET_QUEUE_WORKER_STALE_SEC` | `600` | 워커 하트비트 stale 판정(초) |
| `BITGET_QUEUE_WORKER_HEARTBEAT_PATH` | data_dir/`.queue_worker_heartbeat` | 하트비트 파일 경로 override |

> ⚠️ 운영 메모: 워커가 처리하는 **단일 작업의 최대 소요시간 < `WORKER_STALE_SEC`** 가 되도록
> 임계를 잡을 것(긴 스캔이 stale 임계를 넘으면 오탐). 무거운 `data_refresh` 등 ops 는 큐에
> 넣지 않고 inline 유지하므로 일반적으로 10분이면 충분.

---

## 8. 다음 단계 — 선택지

| # | 작업 | 내용 | 위험도 | 비고 |
|---|---|---|---|---|
| A | **(b-3) 전체 전환 코드 확정** | `bitget.crontab.example` 을 `--use-queue` 로 재생성·커밋 | 중 | 현재는 inline 유지 중. 커밋 = 배포 시 큐 전환 |
| B | **경합 무관 인프라 DAL 통일** | `ops_logger`/`config_manager`/`artifact_guard`/`watchdog` 를 OOM 프라그마 보존하며 DAL 화 | 낮~중 | 시장 DB 경합과 무관, 일관성 목적 |
| ~~C~~ | ~~큐 적체 알람 연동~~ | ✅ 완료 (위 §7 Mission 1) | — | — |
| ~~D~~ | ~~워커 헬스/하트비트~~ | ✅ 완료 (위 §7 Mission 2) | — | — |
| E | **여기서 마무리** | 운영자가 서버에서 b-2/b-3 수동 진행 | — | 코드 측 준비 완료 |

> 권장: 안전망(C/D) 완료됨 → **카나리(b-2) 관찰** → **A(b-3 확정)** 순.
> B 는 독립적이라 언제든 별도 진행 가능.

---

## 9. 관련 문서

- `docs/cron_timezone_db_lock_review.md` — 최초 진단(크론/시간대/락 원인).
- `docs/infra_evolution_execution_guide.md` — Mission 0~3 산출물 + 시스템 재시작 런북.
- `docs/infra_next_steps_a_b_plan.md` — (a)DAL/(b)큐 전환 상세 계획 + 구현 완료 현황 + 전환 런북.
- (본 문서) `docs/infra_evolution_progress_and_next.md` — 전체 경과 핸드오프 + 다음 단계 선택지.
