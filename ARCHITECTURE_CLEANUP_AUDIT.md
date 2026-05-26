# ARCHITECTURE_CLEANUP_AUDIT

읽기 전용(READ-ONLY) 전사 감사 결과입니다. 코드/파일 삭제나 수정 없이 AST/정적 참조 기반으로 분석했습니다.  
스캔 기준 파일 수: Python `262`개 (`__pycache__`, venv 제외).

---

## 1. 전사적 의존성 지도 (Dependency Map) 요약

### 1.1 최상위 진입점 트리

`factory.sh`
- `--scan-kr|--scan-us|--daily-kr|--daily-us|--daily|--weekly` 파싱
- 최종 실행: `python system_auto_pilot.py --mode <MODE>`

`system_auto_pilot.py` (`run_factory_cli`)
- `factory_pipelines.get_pipeline(mode)`
- `factory_runtime.dispatch_factory_mode(...)`
- `factory_runtime.factory_exit_code(report)`

`factory_runtime.py`
- `factory_job_lock(mode)` (flock 기반 실행 락)
- `run_step(StepSpec)` 순차 실행
- `notify_factory_run` (SKIPPED_LOCK/PARTIAL_FAIL 전송)

`factory_pipelines.py` (SSOT)
- `scan_kr`: artifact_guard -> kr_cross_market_hydrate -> supernova_kr -> kr_bowl
- `scan_us`: artifact_guard -> us_health_gate/repair -> us_incremental -> supernova_us -> us_bowl -> publish
- `daily_audit_kr`: meta_sync -> guard -> sentiment -> (US prereq chain) -> track_kr -> deep_dive_kr -> doomsday -> PIL -> comprehensive -> ai_overseer
- `daily_audit_us`: meta_sync -> guard -> sentiment -> us health/update -> track_us -> deep_dive_us -> doomsday -> PIL -> comprehensive -> ai_overseer
- `weekly_master`: artifact_guard -> weekly_flow_master

보조 엔트리 (24x7)
- `main.py`: 다수 스캐너/봇 스레드 + `system_auto_pilot.system_main_loop()` + overseer/secretary 루프
- `factory_launcher.py`: systemd용 `main.py` 부트스트랩

### 1.2 Core vs Utility 분류

Core 모듈 (척추)
- 오케스트레이션: `factory.sh`, `system_auto_pilot.py`, `factory_runtime.py`, `factory_pipelines.py`, `main.py`
- 포워드 트레이딩/리포트: `forward/shared.py`, `forward/ledger.py`, `forward/deep_dive.py`, `daily_report_context.py`, `report_timekeeper.py`, `practitioner_*`
- 정책/가드: `meta_state_store.py`, `meta_governor_consumer.py`, `doomsday_bridge.py`, `factory_artifact_guard.py`
- 리포트 섹션: `forward/deathmatch_report_section.py`, `forward/rotation_report_section.py`, `forward/dna_autopsy.py`

Utility 모듈 (보조)
- 환경/경로/로깅: `telegram_env.py`, `market_db_paths.py`, `ops_logger.py`, `config_manager.py`, `system_config_atomic.py`
- 통신/큐: `telegram_message_queue.py`, `async_telegram_daemon.py`
- 운영 스크립트/배포: `scripts/*`, `deploy/*`
- 공용 방어 유틸: `network_timeout.py`, `low_ram_sqlite_pragmas.py`, `sqlite_schema_guard.py`

---

## 2. 좀비 파일 및 Dead Code 후보군

주의: 본 프로젝트는 `subprocess` 실행, 동적 import, try/except import가 많아 100% 단정은 불가합니다. 아래는 정적 근거 기반 후보입니다.

### 2.1 Orphan Modules 후보 (정적 참조 약함)

- `krx_equity_universe.py` (테스트 참조 위주)
- `heatmap_dashboard.py` (bitget 변형은 사용 흔적 있으나 root 파일 참조 약함)
- `scripts/migrate_bitget_package.py`
- `scripts/split_forward_tester.py`
- `scripts/repair_forward_trades_numeric_corruption.py`

분류 근거
- 메인 파이프라인(`factory_*`, `system_auto_pilot`, `main`)에서 직접 import/호출 흔적 미약
- 실행형 스크립트일 수 있으므로 "즉시 삭제 대상"이 아니라 "격리 우선" 후보

### 2.2 Dead Code 후보 (심볼 단독 정의/사용 흔적 없음)

고신뢰(정적 검색 기준)
- `ai_overseer.gather_daily_system_facts`
- `ai_overseer.safe_generate_content`
- `ai_overseer._gemini_raw_fallback_response`

중신뢰(모듈 자체가 레거시 경로)
- `main.py`에서 try-import만 하고 실경로에서 비활성/중복된 스캐너 래퍼들
- root/bitget 양쪽에 존재하는 호환 shim 파일들(예: `bitget_*` 래퍼)

---

## 3. 신/구 모듈 기능 중복 (Conflict & Duplication)

### 3.1 신규 컨텍스트 vs 레거시 경로

| 신규(의도) | 레거시/중복 | 진단 |
|---|---|---|
| `DailyReportContext` + `ReportTimekeeper` | 과거 now()-N일 직접 컷오프 | 일부 모듈에서 이중 컷오프 흔적이 남았고, `window_pre_sliced` 플래그로 완화 중 |
| `forward/*` 패키지 분리 | `auto_forward_tester.py`/호환 facade | facade 유지 자체는 필요하지만 import* 기반 의존 누락 리스크가 반복됨 |
| `factory_pipelines.py` SSOT | `main.py` 내부 자체 스케줄/봇 스레드 | 운영 모드에 따라 동일 기능이 이중 트리거될 수 있음 |

### 3.2 병목/이중 처리 구간

1) 동일 daily 파이프라인 내 중복 정리
- `send_group_practitioner_reports`와 `send_comprehensive_daily_report` 둘 다 `_reporter_cleanup_zombie_forward_trades()` 호출
- 결과: 같은 회차에서 DB 정리 작업 중복

2) Overseer 이중 실행 가능성
- `system_main_loop` 스케줄 경로 + `factory daily_*` 마지막 optional step 모두 `ai_overseer.run_ai_auditor()` 호출 가능

3) 주간 리포트 이중 발화 가능성
- daemon 스케줄 + cron `--weekly` 동시 설정 시 중복 전송 리스크

4) 데이터 취득 중복
- `track_daily_positions`/`deep_dive`/일부 스캐너가 각자 유사 OHLCV fallback을 독립 수행
- 같은 심볼 데이터에 대해 파이프라인 내 재조회가 발생

---

## 4. 안전한 리팩토링 및 격리(Quarantine) 로드맵 역제안

### 4.1 원칙
- 삭제 금지, 3단계 격리 -> 관찰 -> 제거
- 기능 단위 토글 + 로그 계측 + 회귀테스트 우선

### 4.2 단계별 계획

1단계: 인벤토리/태깅 (1주)
- 각 파일에 `role: core|utility|legacy-candidate` 메타 표 작성
- 엔트리포인트(`factory.sh`, `system_auto_pilot.py`, `main.py`)에서 실제 로딩 파일 목록 자동 덤프

2단계: 격리 폴더 도입 (2주)
- `legacy_archive/` 신설
- orphan 후보를 즉시 이동하지 않고, 먼저 shim 생성:
  - 기존 파일은 얇은 wrapper로 남기고 내부에서 `legacy_archive` 경로를 호출
- 운영 1~2주 관찰 후 wrapper hit=0이면 이동

3단계: 중복 파이프라인 통합 (2~4주)
- 리포트 정리 루틴(`_reporter_cleanup_zombie_forward_trades`) 단일 위치로 승격
- Overseer 실행 권한을 단일 orchestrator로 제한 (cron or daemon one-owner)
- weekly 발화도 one-owner로 고정

4단계: 아키텍처 압축 (목표 262 -> 80~120)
- 도메인 패키지 재편:
  - `orchestrator/` (factory_runtime, pipelines, cli)
  - `reporting/` (context, sections, formatter)
  - `datafeed/` (fdr/yf/http wrappers)
  - `strategy/` (scanner, signals)
  - `ops/` (telemetry, locks, guards)
- bitget/equity 호환 래퍼는 package `__init__` 재-export로 대체
- root에 산재한 실행 스크립트는 `entrypoints/`로 수렴

### 4.3 정량 KPI
- `SKIPPED_LOCK` 발생률 (주별)
- daily 파이프라인 wall-clock
- 동일 심볼 중복 fetch 횟수
- ai_overseer 일일 실행 횟수(1회 목표)
- legacy wrapper hit count (0 도달 시 격리/삭제 후보)

### 4.4 즉시 실행 가능한 안전 조치 (코드 변경 없는 운영안)
- cron/systemd owner 분리표 확정 (동일 기능 이중 실행 금지)
- `factory.sh --daily-*`와 daemon 16:30/weekly 타임슬롯 겹침 제거
- 런북에 "one-owner matrix" 추가

---

## 부록: 감사 신뢰도

- High: 엔트리포인트/파이프라인 트리, 중복 트리거(코드에 명시)
- Medium: orphan 후보/unused 심볼 (동적 import·subprocess 때문에 과소/과대 가능)
- Low: 수동 실행 스크립트의 실제 현업 사용 여부 (운영 로그 교차검증 필요)
