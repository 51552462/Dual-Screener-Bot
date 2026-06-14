# 01 — 주식 팩토리 ↔ Bitget 아키텍처 매핑 및 진단 보고서

> **작성일:** 2026-06-14  
> **범위:** Dual-Screener-Bot 루트(주식) **읽기 전용 스캔** + `bitget/` **진단**  
> **목적:** 주식 시스템 아키텍처를 Bitget에 1:1 이식하기 위한 1단계 분석·승인용 문서  
> **코드 변경:** 없음 (본 문서만 생성)

---

## 0. Executive Summary

주식 팩토리는 **「쉘 → 런타임 → 파이프라인 SSOT → systemd 데몬 + cron」** 4계층으로 완전히 맞물려 있다.  
Bitget은 Phase 0–7 구현으로 **동일 골격의 70~80%가 이미 존재**하지만, 아래 5가지 구조적 단절 때문에 **프로덕션에서 “부품은 있는데 엔진이 한 대에 세 개”** 상태다.

| # | 병목 | 영향 |
|---|------|------|
| 1 | **진입점 3중화** (`main.py` / `auto_pilot.system_main_loop` / `bitget_auto_pilot`) | heartbeat·스케줄·파이프라인 이중 실행 |
| 2 | **파이프라인 prelude 누락** (`meta_governor_sync`, 정식 `artifact_guard`) | Meta/Config/Kelly 분열, 리포트·거래 SSOT 붕괴 |
| 3 | **루트 모듈 의존** (`meta_governor_consumer`, `practitioner_*`) + Bitget DB 미연동 | 코인이 주식 Meta DB를 읽거나, sync 없이 UNKNOWN 유지 |
| 4 | **Config 이중 읽기** (SQLite KV vs `bitget_system_config.json`) | Governor·Kelly·Regime 표시 불일치 |
| 5 | **운영 env 레거시** (`BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget.main` 등) | Watchdog 오탐·재시작 루프 |

**결론:** Bitget을 “새로 짜는” 것이 아니라, **이미 있는 `pipelines/` + `infra/` SSOT를 단일 진실로 고정**하고, 주식 `factory_pipelines`의 **prelude·guard·meta sync 패턴을 Bitget 전용으로 이식**하면 정상화 가능하다.

---

## 1. 주식 시스템 아키텍처 (참조 스캔)

### 1.1 4계층 구조

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 1 — Deploy / Entry                                        │
│   factory.sh          → one-shot cron wrapper                     │
│   deploy/entrypoints/ → run_factory_daemon.sh, run_main_*.sh   │
│   deploy/systemd/     → dante-factory, dante-async, timers       │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 2 — Runtime (격리·락·exit code)                           │
│   factory_runtime.py  → StepSpec, flock, PARTIAL_FAIL, telegram │
└────────────────────────────┬────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 3 — Pipeline SSOT (mode → 순차 Step)                      │
│   factory_pipelines.py                                          │
│   Modes: scan_kr | scan_us | daily_audit_* | daily_audit | weekly│
└────────────────────────────┬────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4 — Domain Modules + 24/7 Daemon                          │
│   system_auto_pilot.py --daemon  (위성·유지보수, 리포트 X)       │
│   system_auto_pilot.py --mode    (factory.sh가 호출)             │
│   auto_forward_tester + forward/  (가상매매·리포트)               │
│   scanners, meta_governor, ops_logger, watchdog, config_manager │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 주식 핵심 파일 역할

| 파일/폴더 | 역할 |
|-----------|------|
| `factory.sh` | venv·`.env`·`PYTHONPATH=ROOT` 로드 후 `system_auto_pilot.py --mode` 실행 |
| `factory_runtime.py` | Step 격리 실행, `.factory_runtime.lock`, cron-safe exit code |
| `factory_pipelines.py` | mode별 Step 순서 SSOT. **모든 daily/scan prelude에 `meta_governor_sync` + `factory_artifact_guard`** |
| `system_auto_pilot.py` | `--daemon`: 위성 스케줄·인버스 스나이퍼·ops snapshot. `--mode`: 파이프라인 1회 |
| `factory_data_paths.py` | `DB_STORAGE_PATH` → `market_data.sqlite` 등 경로 SSOT |
| `config_manager.py` | `system_config.sqlite` KV (JSON은 bootstrap만) |
| `ops_logger.py` | `ops_events.sqlite` — log / gauge / **heartbeat.tick** |
| `watchdog.py` | heartbeat stale → Telegram → `systemctl restart dante-factory` |
| `auto_forward_tester.py` + `forward/` | track, deep_dive, comprehensive report, practitioner |
| `factory_artifact_guard.py` | DB·스키마·MetaGovernor cycle 보장 |
| `meta_state_store.py` | `rebuild_meta_state`, `ensure_config_regime_aligned` |
| `async_telegram_daemon.py` | `message_queue.sqlite` 비동기 발송 |
| `deploy/systemd/dante-*` | factory / async / dashboard / snapshot.timer / watchdog.timer |

### 1.3 주식 파이프라인 모드 (SSOT)

| Mode | 용도 | 핵심 Step (요약) |
|------|------|------------------|
| `scan_kr` | KR 장중 스캔 | guard → meta_sync → session_gate → supernova → nulrim/ema5/master |
| `scan_us` | US 장중 스캔 | guard → meta_sync → US health → supernova → nulrim/ema5 → cross_market publish |
| `daily_audit_kr` | KR 일일 감사 | prelude(meta+guard+sentiment+hydrate) → track → deep_dive → PIL → comprehensive → overseer |
| `daily_audit_us` | US 일일 감사 | 동일 (US track) |
| `daily_audit` | 수동 통합 | KR+US 연쇄 |
| `weekly_master` | 주간 Flow | weekly_flow_master report |

### 1.4 주식 24/7 자동화 (systemd + cron)

- **systemd 상시:** `dante-factory` (`system_auto_pilot.py --daemon`), `dante-async`, `dante-dashboard`, snapshot/watchdog timer
- **cron:** `factory.sh --scan-kr/us`, `--daily-kr/us`, `--weekly` (장 시간 연동)
- **원칙:** 일일 comprehensive 리포트는 **cron → factory.sh** SSOT. 데몬은 위성만 (`DUAL_EXECUTION_FIX`)

---

## 2. Bitget 현황 스캔 (진단)

### 2.1 Bitget 4계층 — 이미 존재하는 대응물

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 1 — bitget/deploy/bitget.sh, deploy_bitget_factory.sh      │
│ Layer 2 — bitget/infra/runtime.py                                │
│ Layer 3 — bitget/pipelines/bitget_pipelines.py                   │
│ Layer 4 — bitget/pipelines/bitget_auto_pilot.py (--daemon)       │
│           bitget/pipelines/runner.py (--mode)                      │
│           bitget/forward/*, bitget/trading/*, scanners, oms       │
└─────────────────────────────────────────────────────────────────┘
```

Bitget은 **주식과 동일한 패턴으로 설계**되어 있으며, `bitget/infra/data_paths.py`로 **데이터·DB·로그 경로가 주식과 격리**되어 있다.

### 2.2 Bitget 파이프라인 모드 (현재 SSOT)

| Mode | 용도 |
|------|------|
| `scan_all` / `scan_spot` / `scan_futures` | 데이터 갱신 + 스캔 + track |
| `track_positions` | 가상 포지션 추적 |
| `daily_audit` | sentiment → track → deep_dive → report → overseer → reconcile |
| `weekly_evolution` | `auto_pilot.run_autonomous_analysis` |
| `reconcile` / `data_refresh` / `gap_heal` / `snapshot` | OMS·데이터·CQRS |
| `health` / `watchdog` / validation modes | 인프라·Phase 7 cutover |

### 2.3 Bitget systemd (RUNBOOK 기준)

| 유닛 | 역할 |
|------|------|
| `dante-bitget-factory` | `python -m bitget.pipelines.bitget_auto_pilot --daemon` |
| `dante-bitget-ws` | WebSocket (주식에 없음 — 코인 전용) |
| `dante-bitget-async` | Bitget Telegram 큐 |
| `dante-bitget-dashboard` / `heatmap` | Streamlit :8511 / :8512 |
| `dante-bitget-watchdog.timer` | 5분 heartbeat |
| `dante-bitget-snapshot.timer` | 5분 DB snapshot |

---

## 3. 핵심 파일 매핑 테이블

**범례:** ✅ 대응 완료 · ⚠️ 부분 이식/불완전 · ❌ 누락 · 🔄 레거시(제거 대상) · ➖ 해당 없음(의도적)

### 3.1 인프라·오케스트레이션

| 주식 (루트) | Bitget 대응 | 상태 | 비고 |
|-------------|-------------|------|------|
| `factory.sh` | `bitget/deploy/bitget.sh` | ✅ | `BITGET_ROOT`·`bitget/.env` 추가 로드 |
| `factory_runtime.py` | `bitget/infra/runtime.py` | ✅ | `BITGET_MODES`, lock 경로 분리 |
| `factory_pipelines.py` | `bitget/pipelines/bitget_pipelines.py` | ⚠️ | prelude·meta sync·PIL step 누락 |
| `system_auto_pilot.py --mode` | `bitget/pipelines/runner.py` | ✅ | `python -m bitget.pipelines.runner --mode` |
| `system_auto_pilot.py --daemon` | `bitget/pipelines/bitget_auto_pilot.py` | ✅ | **프로덕션 SSOT** |
| `factory_data_paths.py` | `bitget/infra/data_paths.py` | ✅ | `BITGET_DB_STORAGE_PATH` 전용 |
| `config_manager.py` | `bitget/infra/config_manager.py` | ⚠️ | SQLite 있으나 JSON 직접 읽기 잔존 |
| `ops_logger.py` | `bitget/infra/ops_logger.py` | ✅ | `bitget_ops_events.sqlite` |
| `watchdog.py` | `bitget/watchdog.py` | ⚠️ | 코드 정상, **운영 env 레거시** 이슈 |
| `async_telegram_daemon.py` | `bitget/async_telegram_daemon.py` | ✅ | 큐 DB path 패치 후 루트 모듈 재사용 |
| `deploy/systemd/dante-factory` | `dante-bitget-factory.service.in` | ✅ | entrypoint: `run_bitget_daemon.sh` |
| `deploy/systemd/dante-async` | `dante-bitget-async.service.in` | ✅ | |
| `deploy/systemd/dante-dashboard` | `dante-bitget-dashboard` + `heatmap` | ✅ | 포트 8511/8512 |
| `deploy/systemd/dante-snapshot.timer` | `dante-bitget-snapshot.timer` | ✅ | |
| `deploy/systemd/dante-watchdog.timer` | `dante-bitget-watchdog.timer` | ✅ | |
| `FACTORY_FULL_OPS_MANUAL.md` | `bitget/RUNBOOK.md` | ✅ | |
| — | `bitget/deploy/bitget.crontab.example` | ✅ | 24/7 UTC cron (주식 장시간 cron과 다름) |
| — | `bitget/data/ws_*.py`, `dante-bitget-ws` | ➖ | 코인 전용 추가 계층 |

### 3.2 거버넌스·Meta·Guard

| 주식 (루트) | Bitget 대응 | 상태 | 비고 |
|-------------|-------------|------|------|
| `factory_artifact_guard.py` | `bitget_pipelines._step_artifact_guard` | ⚠️ | DB 존재·테이블 1개만 검사. 스키마·Meta cycle 없음 |
| `meta_state_store.py` | — | ❌ | Bitget 전용 meta store 없음 |
| `factory_pipelines._step_meta_governor_sync` | — | ❌ | **daily_audit·scan prelude에 없음** |
| `meta_governor.py` / `meta_governor_consumer.py` | 루트 import만 (`forward/ledger.py` 등) | ⚠️ | **주식 `market_data.sqlite` 읽기** — Bitget DB와 분리 안 됨 |
| `market_session_gate.py` | — | ➖ | 코인 24/7 — 의도적 생략 가능 |
| `cross_market_ssot.py` | — | ➖ | KR/US 연동 없음 — 의도적 |

### 3.3 포워드·리포트·스캔

| 주식 (루트) | Bitget 대응 | 상태 | 비고 |
|-------------|-------------|------|------|
| `auto_forward_tester.py` | `bitget/forward_tester.py` → `bitget/forward/_core.py` | ✅ | Phase 3 물리 분할 완료 |
| `forward/` (deep_dive, ledger, shared…) | `bitget/forward/` | ⚠️ | practitioner·meta 연동 미완 |
| `supernova_hunter.py` | `bitget/supernova_hunter.py` | ✅ | spot/futures |
| `legacy_archive/scanners/*` | `bitget/master_scanner.py`, `signal_engines.py` | ✅ | 코인 MTF 엔진 |
| `scanner_funnel.py` | `bitget/pipelines/scanner_hooks.py` | ✅ | hook-only 위임 |
| `sentiment_miner.py` | `bitget/sentiment_miner.py` | ✅ | |
| `ai_overseer.py` | `bitget/ai_overseer.py` | ⚠️ | config JSON 직결 가능 (Governor 리포트 불일치) |
| `practitioner_intelligence` + PIL step | `bitget/forward/practitioner_bitget_adapter.py` | ⚠️ | **파이프라인에 미연결** |
| `weekly_flow_report.py` | `bitget/auto_pilot.send_weekly_flow_master_report` | ⚠️ | `bitget_auto_pilot`에서만 호출 |

### 3.4 실행·OMS (코인 전용)

| 주식 | Bitget | 상태 |
|------|--------|------|
| (실계좌 주식 브로커 없음 — 가상매매 중심) | `bitget/oms.py` → `bitget/trading/*` | ✅ |
| — | `bitget/executor.py` | ✅ |
| — | `bitget/data/gap_healer.py`, `ws_supervisor.py` | ✅ |

### 3.5 레거시·충돌 진입점 (제거/고정 대상)

| 파일 | 상태 | 문제 |
|------|------|------|
| `bitget/main.py` | 🔄 DEPRECATED | `auto_pilot.system_main_loop` 스레드 기동 — pipeline SSOT와 **이중 오케스트레이션** |
| `bitget/auto_pilot.py` `system_main_loop()` | 🔄 레거시 | 인라인 daily/위성/OMS — `bitget_auto_pilot`과 **로직 중복** |
| `bitget/system_auto_pilot.py` | 🔄 DEPRECATED 표기 | `auto_pilot` 복제본, import 혼란 |
| `bitget/factory_launcher.py` + `sentinel.py` | 🔄 DEPRECATED | dashboard spawn만; systemd와 역할 겹침 |
| `bitget/job_cli.py` | 🔄 shim | `runner.py`로 redirect — 유지 가능 |

---

## 4. 작동하지 않는 핵심 원인 분석

### 4.1 [P0] 오케스트레이션 SSOT 미준수 — “엔진 3개”

프로덕션 RUNBOOK은 **`dante-bitget-factory` → `bitget_auto_pilot`** 을 SSOT로 명시한다.  
그러나 레거시 경로가 여전히 살아 있다.

| 진입점 | heartbeat component | daily_audit 실행 경로 | scan/track 실행 |
|--------|---------------------|----------------------|-----------------|
| `bitget_auto_pilot` (SSOT) | `bitget_auto_pilot` | `dispatch_bitget_mode("daily_audit")` | cron `bitget.sh` |
| `main.py` (deprecated) | (과거 `bitget.main` 1회) | `auto_pilot.system_main_loop` 인라인 | 인라인 스레드 |
| `auto_pilot.system_main_loop` | 없음/불규칙 | 자체 UTC 00:00 로직 | `run_mtf_scheduler` 스레드 |

**증상:** Watchdog가 `bitget.main` stale 알림, 파이프라인 lock과 인라인 루프 경합, 동일 satellite 2회 기동 가능.

**근거 파일:**
- `bitget/main.py:150-158` — DEPRECATED 경고, `auto_pilot.system_main_loop` 스레드
- `bitget/pipelines/bitget_auto_pilot.py:175-177` — cron SSOT 명시
- `bitget/bug_analysis_report_01.md` — Watchdog `component=bitget.main` 시나리오 A/B

---

### 4.2 [P0] Meta Governor / Config 동기화 파이프라인 부재

주식은 **모든 scan·daily prelude**에 `_step_meta_governor_sync()`가 있다 (`factory_pipelines.py:417-424`).  
Bitget `daily_audit`은 다음으로 시작한다 (`bitget_pipelines.py:259-271`):

```
config_bootstrap → artifact_guard(최소) → sentiment → track → deep_dive → report → ai_overseer → reconcile
```

**누락:**
- `meta_governor_sync` / `rebuild_meta_state`
- `ensure_config_regime_aligned`
- 주식 수준 `factory_artifact_guard` (MetaGovernor cycle, 스키마 완전성)

**동시에** Bitget 코드는 루트 `meta_governor_consumer`를 import한다:
- `bitget/forward/ledger.py`
- `bitget/forward/reports.py`
- `bitget/trading/execution_safety.py`
- `bitget/auto_pilot.py`

이 모듈은 **주식 `market_data.sqlite` / `system_config.sqlite`** 를 읽는다. Bitget cron만 돌면 Meta가 갱신되지 않거나, **주식 Meta와 Bitget Config가 영구 분열**한다.

**관측 증상 (기존 감사):** `meta_regime_key=BULL` vs `config_regime_key=UNKNOWN`, `effective_kelly_risk=1.00%`, Governor timestamp 고착 — `GOVERNOR_SYNC_FATAL_REPORT.md` 참조.

---

### 4.3 [P1] Config 이중 SSOT (SQLite KV vs JSON)

주식은 `config_manager.py`가 **SQLite가 SSOT**, JSON은 bootstrap만.  
Bitget도 `bitget/infra/config_manager.py`가 동일 패턴이나, 다수 모듈이 여전히:

- `bitget/forward/shared.py` — `CONFIG_PATH` JSON fallback
- `bitget/auto_pilot.py` — `config_hub` / JSON path
- `bitget/ai_overseer.py` — JSON 직결 가능

**결과:** Governor가 SQLite에 쓴 regime이 JSON·리포트·스캐너에 반영되지 않음.

---

### 4.4 [P1] Artifact Guard 축소 구현

Bitget `_step_artifact_guard` (`bitget_pipelines.py:54-68`):
- 파일 존재 + 테이블 1개만 확인

주식 `factory_artifact_guard.ensure_factory_artifacts()`:
- 필수 테이블·스키마·MetaGovernor·market DB 무결성

**결과:** DB가 “있지만 빈 껍데기”여도 파이프라인이 진행 → track/deep_dive/report 연쇄 실패 또는 빈 리포트.

---

### 4.5 [P1] Practitioner / PIL 파이프라인 미연결

주식 `daily_audit_*`는 `_step_pil_practitioner_reports`를 포함한다.  
Bitget은 `practitioner_bitget_adapter.py`가 존재하나 **`bitget_pipelines._pipeline_daily_audit`에 step 없음**.

**결과:** PRACT_01~30 실무자 리포트 미발송 (기존 버그 리포트 #3).

---

### 4.6 [P2] PYTHONPATH 공유로 인한 “조용한” 루트 의존

`bitget.sh`는 `PYTHONPATH=ROOT`(repo 루트)를 설정한다.  
의도는 `meta_governor_consumer`, `practitioner_intelligence`, `low_ram_sqlite_pragmas` 등 **읽기 전용 공유 유틸** 재사용.

**부작용:** Bitget이 루트 모듈을 import할 때 **어느 DB를 쓰는지** 모듈마다 다름 → 통합 테스트 없이 production 장애.

---

### 4.7 [P2] 검증·Cutover 플래그와 운영 갭

Phase 7 (`validation/`, `BITGET_PIPELINE_SSOT=1`, `cutover_check`)는 코드상 완료.  
운영 서버가 여전히 `main.py` / 구 env / 수동 프로세스를 쓰면 **cutover 조건 미충족**.

---

## 5. 주식 → Bitget 1:1 이식 원칙 (설계 가이드)

| 주식 원칙 | Bitget 이식 규칙 |
|-----------|------------------|
| 루트 파일 수정 금지 | 루트 모듈은 **import만**; Bitget 전용 래퍼는 `bitget/infra/` 또는 `bitget/governance/`에 생성 |
| 데이터 격리 | `BITGET_DB_STORAGE_PATH`, `bitget_market_data.sqlite`, `bitget_system_config.sqlite` |
| 단일 파이프라인 SSOT | one-shot = `bitget.sh` → `runner` → `bitget_pipelines`; 24/7 = `bitget_auto_pilot` only |
| 단일 config SSOT | 모든 읽기/쓰기 → `bitget.infra.config_manager` (JSON 직접 I/O 금지) |
| prelude 패턴 | scan·daily 앞에 **bitget_meta_sync + bitget_artifact_guard** (주식과 동일 순서) |
| 데몬은 위성만 | daily comprehensive / scan-all은 **cron** (이중 실행 방지) |

---

## 6. 단계별 수정·생성 계획 (승인 후 실행)

> 각 Phase 시작 전 `bitget/docs/0N_*.md` 상세 설계 문서를 먼저 작성한다 (Documentation First).

### Phase 1 — 진입점·운영 SSOT 고정 (즉시 효과)

**목표:** 프로덕션에서 `bitget_auto_pilot` + `bitget.sh` only.

| 작업 | 생성/수정 (bitget/ only) |
|------|-------------------------|
| 레거시 진입점 정리 | `main.py`, `factory_launcher.py`, `sentinel.py` — 실행 시 hard fail 또는 thin redirect |
| `auto_pilot.system_main_loop` | `bitget_auto_pilot` 위임으로 축소; 중복 satellite 제거 |
| 운영 env 템플릿 | `deploy/bitget_resource_limits.env.example`에 watchdog component 고정 |
| RUNBOOK 보강 | cutover 체크리스트, `grep BITGET_WATCHDOG` 검증 명령 |

**완료 기준:** Watchdog stale 0건, `heartbeat.tick` component = `bitget_auto_pilot` only.

---

### Phase 2 — 파이프라인 Prelude 이식 (주식 `factory_pipelines` parity)

**목표:** `daily_audit`·`scan_*` prelude를 주식과 동일 패턴으로.

| 작업 | 상세 |
|------|------|
| `bitget/infra/artifact_guard.py` **신규** | 주식 `factory_artifact_guard` 포팅 — Bitget DB·스키마·필수 테이블 |
| `bitget/governance/meta_sync.py` **신규** | Bitget 전용 meta rebuild + config regime align (BTC/ETH regime, funding, Kelly) |
| `bitget_pipelines.py` 수정 | `_with_daily_audit_prelude`, `_with_scan_prelude` — `meta_sync` → `artifact_guard` → … |
| `scanner_hooks` / `master_scanner` | scan 전 `load_meta_state` hydrate (주식 `cross_market` 대신 coin regime) |

**완료 기준:** `bitget.sh --daily-audit` 시 regime·Kelly가 config SQLite에 기록되고 리포트에 일치 표시.

---

### Phase 3 — Config·Meta 읽기 경로 단일화

| 작업 | 상세 |
|------|------|
| `forward/shared.py`, `ai_overseer.py`, `auto_pilot.py` | JSON 직접 read 제거 → `config_manager` only |
| `bitget/governance/meta_consumer.py` **신규** | 루트 `meta_governor_consumer` 래핑 또는 Bitget DB 전용 resolver |
| Governor timestamp freshness | 주식 `is_meta_state_degraded` age 검사 패턴 Bitget에 적용 |

---

### Phase 4 — 리포트·PIL·Forward 완결

| 작업 | 상세 |
|------|------|
| `bitget_pipelines` | `_step_pil_bitget_reports` 추가 (spot/futures) |
| `forward/reports.py` | deep_dive SQL 바인딩 수정 (bug #2) |
| `practitioner_bitget_adapter.py` | pipeline hook + telegram 경로 검증 |

---

### Phase 5 — 데몬·Cron·systemd 최종 정렬

| 작업 | 상세 |
|------|------|
| `bitget_auto_pilot` | satellite만 유지; scan/track은 cron 전용 재확인 |
| `bitget.crontab.example` | 서버 실제 crontab diff 자동화 스크립트 |
| systemd units | `After=` / `Wants=` dante-bitget-ws → factory 순서 |
| `deploy_bitget_factory.sh` | env 배포 시 `BITGET_WATCHDOG_HEARTBEAT_COMPONENT` 강제 |

---

### Phase 6 — 검증·Cutover·회귀 테스트

| 작업 | 상세 |
|------|------|
| `bitget.sh --cutover-check` | Phase 1–5 후 PASS |
| `validation/runner.py` | baseline 재기록 |
| pytest | `tests/test_trading_phase5.py`, pipeline integration test 추가 |

---

### Phase 7 — 문서·운영 마감

- `bitget/docs/02_phase1_entrypoint_consolidation.md` … 단계별 상세
- Ubuntu 서버 실측 체크리스트 (health, heartbeat, daily_audit 로그)

---

## 7. 승인 전 서버에서 즉시 확인 가능한 진단 명령

```bash
# 1) 어떤 진입점이 살아 있는지
systemctl status dante-bitget-factory dante-bitget-ws dante-bitget-async
ps aux | grep -E 'bitget\.(main|auto_pilot|pipelines)'

# 2) Watchdog env
grep BITGET_WATCHDOG /path/to/.env /path/to/bitget/.env

# 3) heartbeat component
sqlite3 /path/to/bitget_ops_events.sqlite \
  "SELECT component, MAX(ts_utc) FROM ops_events WHERE event='heartbeat.tick' GROUP BY component;"

# 4) Config regime 분열
sqlite3 /path/to/bitget_system_config.sqlite \
  "SELECT key, substr(value_json,1,80) FROM config_kv WHERE key IN ('CURRENT_REGIME_KEY','REGIME_ANALYSIS','DYNAMIC_KELLY_RISK');"

# 5) 파이프라인 SSOT health
cd /path/to/Dual-Screener-Bot && ./bitget/deploy/bitget.sh --health
./bitget/deploy/bitget.sh --cutover-check
```

---

## 8. 참조한 주식 파일 목록 (읽기 전용)

- `factory.sh`, `factory_runtime.py`, `factory_pipelines.py`
- `system_auto_pilot.py` (daemon 루프·`--mode` CLI)
- `factory_data_paths.py`, `config_manager.py`, `ops_logger.py`, `watchdog.py`
- `factory_artifact_guard.py`, `meta_state_store.py` (grep·구조 참조)
- `forward/shared.py`, `forward/deep_dive.py`
- `deploy/systemd/dante-*.service.in`, `deploy/entrypoints/run_factory_daemon.sh`
- `FACTORY_FULL_OPS_MANUAL.md`, `async_telegram_daemon.py`

## 9. 참조한 Bitget 파일·기존 문서

- `bitget/pipelines/*`, `bitget/infra/*`, `bitget/forward/*`
- `bitget/deploy/*`, `bitget/RUNBOOK.md`
- `bitget/bug_analysis_report_01.md`, `bitget/GOVERNOR_SYNC_FATAL_REPORT.md`
- `bitget/docs/README.md`, `implementation_phase_*.md`

---

## 10. 다음 액션 (사용자 승인 대기)

1. 본 문서 검토·승인
2. 승인 시 **Phase 1** 상세 설계 문서 (`02_phase1_entrypoint_consolidation.md`) 작성
3. Phase별 승인 후 코드 수정 ( **`bitget/` only** )

---

*본 보고서는 코드 변경 없이 아키텍처 스캔만 수행하였으며, 루트 주식 파일은 단 한 글자도 수정하지 않았다.*
