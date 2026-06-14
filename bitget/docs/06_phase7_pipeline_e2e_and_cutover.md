# 06 — Phase 7 실행 보고서 (파이프라인 E2E·아키텍처 검증·Cutover 강화)

> **작성일:** 2026-06-14  
> **선행 작업:** `05_phase6_bugfix_and_validation.md`  
> **수정 범위:** `bitget/` only

---

## 0. Executive Summary

| 항목 | 결과 |
|------|------|
| **아키텍처 불변식 검사** (`validation/architecture_checks.py`) | ✅ 신규 |
| **cutover_check 강화** (Phase 1~6 SSOT 포함) | ✅ |
| **파이프라인 E2E pytest** | ✅ `test_phase7_pipeline_e2e.py` |
| **PIL adapter market_type 정규화** | ✅ Phase 6 reports와 동일 패턴 |

Phase 7은 **운영 cutover 전 자동으로 검증 가능한 아키텍처 체크리스트**와 **runner·dispatch E2E 테스트**를 추가했다.

---

## 1. `validation/architecture_checks.py` (신규)

Phase 1~6에서 확립한 SSOT를 코드로 검증한다.

| 검사 | 내용 |
|------|------|
| `legacy_entrypoints` | `main`, `factory_launcher`, `sentinel` → `SystemExit(2)`; `system_auto_pilot.system_main_loop` → `RuntimeError` |
| `pipeline_structure` | `daily_audit` prelude + PIL/deep_dive body; `scan_spot` meta prelude |
| `satellite_config_hub` | 14개 위성 모듈에 `bitget_system_config.json` + `open(` 정적 스캔 0건 |
| `config_meta_alignment` | `CURRENT_REGIME_KEY` vs `META_REGIME_KEY`, degraded 여부 |
| `watchdog_component` | `BITGET_WATCHDOG_HEARTBEAT_COMPONENT` ≠ `bitget.main` |

```python
from bitget.validation.architecture_checks import run_architecture_checks
report = run_architecture_checks()
# report["passed"], report["failed"], report["checks"]
```

---

## 2. Cutover 연동

`check_cutover_readiness()`에 `architecture_ok` 체크 추가.

**`BITGET_PIPELINE_SSOT=1` 일 때** cutover `passed` 조건:

```
pipeline_ssot_env
∧ parallel_run_ready (48h)
∧ no_legacy_main_process
∧ async_telegram
∧ architecture_ok   ← Phase 7 신규
```

`bitget.sh --cutover-check` 출력 예:

```
[cutover] passed=False message=...
[cutover] checks={..., 'architecture_ok': True/False}
[cutover] architecture_passed=... failed=[...]
```

---

## 3. 파이프라인 E2E 테스트

`bitget/tests/test_phase7_pipeline_e2e.py`

| 테스트 | 검증 |
|--------|------|
| `TestPipelineStructure` | `daily_audit` / `scan_spot` step 이름·순서 |
| `TestDispatchRuntime` | `dispatch_bitget_mode` 순차 실행·`dry_run` 스킵 |
| `TestRunnerCli` | `runner --mode health --dry-run`; `cutover_check` 모드 |
| `TestArchitectureChecks` | 레거시 차단·구조·위성 config·cutover architecture 키 |

### 실행

```bash
python -m unittest bitget.tests.test_phase7_pipeline_e2e -v
python -m unittest bitget.tests.test_phase6_integration bitget.tests.test_phase7_pipeline_e2e -v
```

---

## 4. PIL adapter — market_type 정규화

`practitioner_bitget_adapter.py` SQL `params`에 `mkt = str(market_type).strip().lower()` 적용 (reports bug #2와 동일 SSOT).

---

## 5. Phase 1~7 통합 아키텍처

```
진입
  cron/수동  → bitget.sh → pipelines.runner
  24/7       → bitget_auto_pilot --daemon
  legacy     → main / launcher / sentinel / system_auto_pilot BLOCKED

파이프라인
  scan_*     → meta_sync_scan → artifact_guard → config_bootstrap → scan
  daily      → meta_sync → artifact_guard → config → sentiment → track
               → deep_dive → PIL → comprehensive → overseer → reconcile

데이터·설정
  config     → config_hub → SQLite KV
  meta       → governance/meta_sync + meta_consumer
  forward DB → data_paths.market_data_db_path()

검증 (Phase 7)
  cutover    → validation/cutover + architecture_checks
  pytest     → test_phase6_integration + test_phase7_pipeline_e2e
```

---

## 6. 서버 Cutover 절차 (권장)

```bash
# 1) 아키텍처만 빠르게
python -c "from bitget.validation.architecture_checks import run_architecture_checks as r; print(r())"

# 2) 전체 cutover 리포트
./bitget/deploy/bitget.sh --cutover-check

# 3) 48h parallel run 후
export BITGET_PIPELINE_SSOT=1
export BITGET_ASYNC_TELEGRAM=1
export BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot
./bitget/deploy/bitget.sh --cutover-check   # passed=True 기대

# 4) daily_audit 실측 (regime·Kelly 리포트 일치)
./bitget/deploy/bitget.sh --daily-audit
```

---

## 7. 잔여 항목 (Phase 8+ 후보)

| 항목 | 비고 |
|------|------|
| `daily_audit` full mock E2E (텔레그램·exchange 없이) | step fn patch |
| 서버 regime·Kelly 실측 자동화 | ops cron + architecture_checks 알림 |
| `BITGET_PIPELINE_SSOT=1` 프로덕션 cutover | 48h parallel 완료 후 |

---

## 8. 변경 파일

| 파일 | 변경 |
|------|------|
| `validation/architecture_checks.py` | 신규 |
| `validation/cutover.py` | architecture_ok 연동 |
| `validation/runner.py` | cutover 출력 보강 |
| `forward/practitioner_bitget_adapter.py` | market_type lowercase |
| `tests/test_phase7_pipeline_e2e.py` | 신규 |
| `docs/06_phase7_pipeline_e2e_and_cutover.md` | 본 문서 |
| `docs/README.md` | 인덱스 갱신 |

---

## 9. 격리 확인

- **미수정:** 루트 `factory_pipelines.py`, `forward/`, `system_auto_pilot.py`, `deploy/systemd/dante-*`
- **수정:** `bitget/` 하위만
