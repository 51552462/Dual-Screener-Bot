# 🧭 다음 단계 실행 계획 — (a) DAL 전면 치환 · (b) cron→큐 어댑터 전환

> 전제: Mission 0~3(백업/DAL/큐/서킷 브레이커) 구현·검증 완료.
> 본 문서는 두 후속 작업의 **범위·순서·리스크·검증 기준**을 정의한다.
> 원칙: **데이터 무결성 우선**, 운영 영향 최소화(비침투·점진 적용), 각 단계는 독립 롤백 가능.

---

## (a) DAL 전면 치환 — 모든 SQLite 접속을 `shared_db_connector` 로 통일

### 목표
모듈마다 제각각인 `sqlite3.connect(timeout=7~120)` + 개별 PRAGMA 를
`shared_db_connector.get_connection() / connect()` 단일 경로로 통일하여
**busy_timeout 60s · WAL · synchronous=NORMAL** 을 전 시스템에 강제한다.
→ `database is locked` 의 근본 원인(타임아웃 불일치 + WAL 미적용 writer) 제거.

### 현황 (치환 대상 ≈ 코어 모듈 40여 파일, 60+ 호출부)
`sqlite3.connect` 직접 호출이 남아 있는 핵심 파일(스모크/테스트/신규 모듈 제외):

| 우선순위 | 파일 | 현재 설정 | 비고 |
|---|---|---|---|
| 🔴 P0 (hot writer) | `forward/ledger.py` | `timeout=60` + WAL, **busy_timeout 없음** | `track_positions` 데드락 진원지 |
| 🔴 P0 | `master_scanner.py` | `timeout=20` / `30` | 스캔 writer/reader |
| 🔴 P0 | `mtf_data_updater.py` | `timeout=60` + `busy_timeout=7000`(7s) | `data_refresh` 벌크 OHLCV writer |
| 🟠 P1 | `shadow_tracking.py` / `shadow_performance_tracker.py` | `timeout=60`/`30` | shadow 차수 writer |
| 🟠 P1 | `trading/reconciliation.py` | `timeout=120` + `busy_timeout=15000` | reconcile writer |
| 🟠 P1 | `forward/shared.py`, `forward/reports.py`, `forward/execution_bridge.py`, `forward/forward_trade_identity.py`, `forward/forward_book_integrity.py`, `forward/practitioner_bitget_adapter.py` | `timeout=30~60` | 포워드 북 writer/reader |
| 🟡 P2 | `supernova_hunter.py`, `data_miner.py`, `alt_data_miner.py`, `sentiment_miner.py`, `pump_forensics.py`, `underdog_miner.py`, `forensics_pioneer.py`, `blackhole_hunter.py`, `auto_pilot.py`, `system_auto_pilot.py`, `ai_overseer.py` | 혼재 | 보조 엔진/마이너 |
| 🟢 P3 (read-only) | `heatmap_dashboard.py`, `dashboard.py`, `toxic_graveyard_analyzer.py`, `reports/bitget_report_context.py`, `validation/*` | `mode=ro` 등 | 읽기 전용 → `read_only=True` 로 치환 |
| ⚪ 유지 | `infra/ops_logger.py`, `infra/config_manager.py`, `infra/artifact_guard.py`, `infra/snapshot_service.py` | 자체 WAL/ro 처리 | 이미 안전 — 후순위/선택적 |

> `snapshot_service.py` 의 `src.backup(dst)` 는 Online Backup API 라 그대로 두되,
> 소스 연결의 busy_timeout 만 60s 로 맞추는 정도면 충분.

### 치환 패턴 (Before → After)
```python
# Before
conn = sqlite3.connect(DB_PATH, timeout=20)
conn.execute("PRAGMA journal_mode=WAL;")

# After (writer)
from bitget.infra.shared_db_connector import get_connection
conn = get_connection(DB_PATH)          # WAL/synchronous/busy_timeout 자동

# After (읽기 전용)
conn = get_connection(DB_PATH, read_only=True)

# After (컨텍스트 매니저 — 권장)
from bitget.infra.shared_db_connector import connect
with connect(DB_PATH) as conn:
    conn.execute("INSERT ...")          # 자동 commit/rollback/close
```
주의:
- `pd.read_sql(sql, conn)` 는 그대로 동작(커넥션만 교체).
- `BEGIN EXCLUSIVE` 등 수동 트랜잭션이 필요한 곳은 `get_connection(..., isolation_level=None)`.
- `check_same_thread=False` 가 필요한 멀티스레드 사용처는 동일 인자 전달.

### 실행 순서 (단계별, 각 단계 후 헬스 체크)
1. **P0 3종(ledger/master_scanner/mtf_data_updater)** 먼저 치환 → 락 경합 80% 이상 차지.
2. P1 포워드/트레이딩 → P2 보조 엔진 → P3 read-only.
3. 각 단계: `python -m py_compile <파일>` → `--mode health` → 해당 모드 1회 `--dry-run`/실주행.

### 리스크 & 가드
- **行동 변화 없음**(연결 옵션만 변경) — 쿼리·스키마 불변.
- 위험: `isolation_level` 기본값 차이로 암묵적 트랜잭션 타이밍이 바뀔 수 있음
  → 명시적 `commit()` 사용처는 그대로, 컨텍스트 매니저로 옮길 때만 자동 commit 적용.
- 롤백: 파일 단위 git revert (각 파일 독립적).

### 완료 기준 (DoD)
- [ ] P0/P1 파일의 `sqlite3.connect` 직접 호출 0건 (grep)
- [ ] 모든 writer 연결의 `PRAGMA busy_timeout` = 60000 (런타임 점검 스크립트)
- [ ] 24~48h 운영 중 `database is locked` 0건

---

## (b) cron → 큐 어댑터 전환 — "스킵" 을 "대기 후 실행" 으로

### 목표
현재 cron → `bitget.sh` → `runner.py` → `dispatch_bitget_mode` 직접 실행 구조에서는
주식/코인 동시각 충돌 시 `yield_to_factory` 로 **증발(Skip)** 한다.
이를 `task_orchestrator` 큐로 우회시켜 **Drop 대신 PENDING 대기 → 단일 워커가 순차 실행**하도록 바꾼다.

### 아키텍처 (After)
```
cron ──► bitget.sh --enqueue <mode>     # 즉시 enqueue 후 종료 (무거운 작업 X)
                     │
                     ▼
            task_queue.sqlite  (PENDING, priority = 권력이양 규칙)
                     │
        systemd ──►  단일 워커 (drain loop)
                     │  claim_next() [BEGIN EXCLUSIVE]
                     │  ├─ 주인 엔진? → 정상 실행
                     │  └─ 후순위?    → os.nice(10) 스로틀 후 실행
                     ▼
            dispatch_bitget_mode(mode, pipeline)   # 기존 파이프라인 재사용
```

### 구현 항목
1. **Enqueue 진입점** — `runner.py` 에 `--enqueue` 플래그 추가:
   ```python
   # bitget/pipelines/runner.py (신규 분기)
   if args.enqueue:
       from bitget.infra.task_orchestrator import enqueue
       engine = _engine_for_mode(args.mode)         # scan_spot_* → BITGET 등
       enqueue(engine, args.mode)                   # dedupe 기본 ON
       return 0
   ```
2. **모드→엔진 매핑 헬퍼** — `bitget_scan_schedule.scan_mode_market()` 활용
   (`scan_spot_*`/`scan_futures_*` → `BITGET`). 주식 측 모드는 `factory_scan_schedule` 의
   `scan_mode_market()` 로 KR/US 판정.
3. **워커 데몬** — `bitget/pipelines/queue_worker.py` 신규:
   ```python
   from bitget.infra.task_orchestrator import drain, Task
   from bitget.infra.runtime import dispatch_bitget_mode
   from bitget.pipelines.bitget_pipelines import get_pipeline

   def _executor(task: Task) -> None:
       report = dispatch_bitget_mode(task.mode, get_pipeline(task.mode))
       if not report.all_critical_ok and not report.skipped_lock:
           raise RuntimeError(report.status_label)   # 큐 fail() → 재시도/FAILED

   def main():
       while True:
           if drain(_executor, max_tasks=50) == 0:
               time.sleep(POLL_SEC)
   ```
4. **systemd 유닛** — `dante-bitget-queue-worker.service` (단일 인스턴스, `Restart=always`).
   기존 24/7 스캔 데몬과 **공존**하되, cron 스캔은 enqueue 로만 진입.
5. **cron 재생성** — `generate_bitget_crontab.py` 의 `_scan_command` 을
   `bitget.sh --enqueue <flag>` 형태로 변경(SSOT 한 곳만 수정 → 전 슬롯 반영).

### 전환 전략 (점진/안전)
- **Phase b-1 (shadow)**: 큐 워커를 enqueue 없이 띄워 두고, 기존 cron 은 그대로.
  수동으로 `enqueue()` 테스트 → claim/실행/완료 흐름만 검증.
- **Phase b-2 (canary)**: `scan_spot_*` 1종만 cron→enqueue 로 전환, 24h 관찰.
- **Phase b-3 (full)**: 전 스캔 모드 enqueue 전환. ops(track/reconcile/watchdog)는
  경량이라 cron 직접 유지 가능(선택).

### 큐 도입의 부수 효과
- `data_refresh` 별도 락 문제 자연 해소: 워커가 단일 직렬이라 scan↔data_refresh 동시 write 소멸.
- `yield_to_factory` 스킵 → "대기" 로 바뀌어 **차수 누락 0**.
- 서킷 브레이커(Mission 3)와 결합: FAILED 차수는 큐에서 영구 폐기 → 좀비 루프 차단.

### 리스크 & 가드
- 워커 단일 장애점 → `Restart=always` + heartbeat watchdog 로 감시.
- 큐 적체 모니터링: `task_orchestrator.queue_stats()` 를 ops 알람에 노출.
- 중복 실행 방지: `claim_next()` 의 `BEGIN EXCLUSIVE` 로 보장(검증 완료).
- 롤백: cron 을 기존 `bitget.sh <flag>`(직접 실행)으로 재생성하면 즉시 원복.

### 완료 기준 (DoD)
- [ ] cron 스캔 라인이 전부 `--enqueue` 경유
- [ ] 워커가 권력 이양 우선순위대로 처리(후순위 `os.nice` 확인)
- [ ] 충돌 시 SKIP 로그 0건 / PENDING 대기 후 DONE 으로 전환 확인
- [ ] 큐 적체(PENDING) 알람 임계 설정

---

## 권장 진행 순서
1. **(a) P0 3종 치환** — 가장 빠른 데드락 감소 효과, 위험 최소.
2. **(b) Phase b-1~b-2** — 큐 워커 도입 + 카나리.
3. (a) P1~P3 잔여 치환 → (b) Phase b-3 전체 전환.

> 각 단계는 독립적이라 (a)만 먼저 끝내고 운영 안정화 후 (b)로 넘어가도 무방하다.

---

## ✅ 구현 완료 현황 (2026-06-28)

### (a) DAL 전면 치환 — **P0~P3 완료**
- P0(ledger/master_scanner/mtf_data_updater), P1(forward/trading), P2(보조 엔진·마이너),
  P3(대시보드·리포트·validation) + `time_machine_backtester.py` 의 시장 DB 접속이
  전부 `shared_db_connector.get_connection()` 단일 경로로 통일됨.
- 잔여 raw `sqlite3.connect` 는 **설계상 유지**(DAL 본체, 큐, 백업/스냅샷 backup API, 테스트)와
  **경합 무관 전용 DB 인프라**(ops_logger/config_manager/artifact_guard — 별도 단계 선택).

### (b) cron→큐 어댑터 — **Phase b-1(인프라) 완료, b-2/b-3 운영자 전환 대기**
구현된 산출물:
| 산출물 | 경로 | 역할 |
|---|---|---|
| enqueue 진입점 | `pipelines/runner.py --enqueue` | 모드를 큐에 적재만 하고 즉시 종료(코인=BITGET 엔진) |
| 워커 데몬 | `pipelines/queue_worker.py` | 단일 직렬 drain → `dispatch_bitget_mode` 실행. FAIL 만 큐 재시도 |
| 셸 패스스루 | `deploy/bitget.sh --enqueue` | cron 라인에서 enqueue 전달 |
| systemd 유닛 | `deploy/systemd/dante-bitget-queue-worker.service.in` | `Restart=always` 단일 인스턴스 |
| 엔트리포인트 | `deploy/entrypoints/run_bitget_queue_worker.sh` | venv/.env 로드 후 워커 실행 |
| cron 토글 | `deploy/generate_bitget_crontab.py --use-queue` | 스캔 라인만 `--enqueue` 로 생성(기본 off) |

> 검증: enqueue→drain 왕복(`PENDING→DONE`), dedupe, 권력 이양 priority, 종료코드 0 스모크 통과.
> 기본 cron 은 **inline 유지**(committed `bitget.crontab.example` 불변) — 전환은 아래 절차로 명시적 수행.

### 운영 전환 런북 (b-2 카나리 → b-3 전체)
```bash
# 0) 코드 배포 후 워커 유닛 설치/기동 (shadow: cron 은 아직 inline)
sudo bash bitget/deploy/deploy_bitget_factory.sh
sudo systemctl start dante-bitget-queue-worker
journalctl -u dante-bitget-queue-worker -f      # "queue worker started" 확인

# 1) 수동 왕복 검증 (b-1)
bitget/deploy/bitget.sh --enqueue --scan-spot-supernova
python -c "from bitget.infra.task_orchestrator import queue_stats; print(queue_stats())"
#   → {'PENDING': 1} 잠시 후 워커가 처리하여 {'DONE': 1}

# 2) 카나리 (b-2): bitget.crontab.example 의 scan-spot 라인 1개만 손으로
#    `bitget.sh --scan-spot-supernova` → `bitget.sh --enqueue --scan-spot-supernova` 로 수정 후 설치.
sudo INSTALL_ROOT=<root> bash bitget/deploy/install_bitget_cron.sh
#    24h 관찰: SKIP 로그 0건 / PENDING 대기 후 DONE 전환 / queue_stats 적체 없음.

# 3) 전체 전환 (b-3): SSOT 에서 전 스캔 라인 일괄 enqueue 생성.
python bitget/deploy/generate_bitget_crontab.py --use-queue --install-root <root>
sudo INSTALL_ROOT=<root> bash bitget/deploy/install_bitget_cron.sh

# 롤백 (즉시): inline 로 재생성 후 재설치, 워커는 정지.
python bitget/deploy/generate_bitget_crontab.py --install-root <root>   # use-queue 없음 = inline
sudo INSTALL_ROOT=<root> bash bitget/deploy/install_bitget_cron.sh
sudo systemctl stop dante-bitget-queue-worker
```

### 모니터링 포인트
- `task_orchestrator.queue_stats()` 의 `PENDING` 적체 → ops 알람 임계 설정 권장.
- 워커 단일 장애점은 `Restart=always` + 기존 watchdog 하트비트로 감시.
- 후순위(비-주인 세션) 작업은 `os.nice(10)` 스로틀(Linux) 후 실행 — `top` 의 NI 컬럼으로 확인.
