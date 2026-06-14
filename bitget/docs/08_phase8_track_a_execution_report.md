# 08 — Phase 8 Track A 실행 보고서 (Mock E2E + Regime/Kelly 감사)

> **작성일:** 2026-06-14  
> **선행:** `07_phase8_feasibility_review.md` (Track A 승인)  
> **수정 범위:** `bitget/` only (루트 주식 파일 **미수정**)  
> **환경:** 로컬 Windows (개발) → GitHub Push → Ubuntu (프로덕션) CI/CD

---

## 0. Executive Summary

| 항목 | 결과 |
|------|------|
| Mock E2E 파이프라인 테스트 | ✅ `test_phase8_track_a_mock_e2e.py` (11 tests) |
| Regime/Meta/Kelly 감사 모듈 | ✅ `validation/regime_audit.py` |
| architecture_checks 연동 | ✅ `regime_kelly_audit` 체크 추가 |
| CI unittest (Phase 6~8) | ✅ **26 tests OK** |

**Track A 목표 달성:** 거래소 API·실시장 DB 없이 로컬에서 파이프라인 **논리 흐름·critical/optional 실패 전파·Regime/Kelly SSOT 분리**를 검증할 수 있다.

---

## 1. Mock E2E 파이프라인 테스트

### 1.1 설계 원칙

| 원칙 | 구현 |
|------|------|
| API/DB 불필요 | `run_step` mock 또는 mirror no-op pipeline |
| SSOT step 이름 유지 | `get_pipeline(mode)`에서 추출한 이름·critical 플래그 그대로 |
| CI 자동 실행 | `python -m unittest bitget.tests.test_phase8_track_a_mock_e2e` |
| 주식 루트 미접촉 | `bitget/` only |

### 1.2 테스트 시나리오

| 테스트 | 검증 내용 |
|--------|-----------|
| `test_mirror_pipeline_full_order` | `daily_audit` 13 step 순서·prelude·PIL 포함 |
| `test_critical_failure_marks_fail_but_continues` | critical 실패 시 `FAIL` + exit 1, 후속 step 기록 (factory_runtime 패턴) |
| `test_optional_failure_is_partial` | optional 실패 → `PARTIAL_FAIL`, critical 계속 |
| `test_daily_audit_dispatch_visits_all_ssot_steps_without_bodies` | `run_step` mock으로 body 미실행·이름 전체 방문 |
| `test_scan_spot_mirror_order` | scan prelude 3종 + track_spot |
| `test_scan_all_includes_shadow_eval` | scan_all shadow_eval 존재 |
| `test_runner_daily_audit_all_steps_mocked_exit_zero` | runner CLI `--mode daily_audit` exit 0 |

### 1.3 검증된 `daily_audit` SSOT 순서

```
meta_governor_sync → artifact_guard → config_bootstrap → sentiment_mining
→ doomsday_radar → track_spot → track_futures
→ deep_dive_spot → deep_dive_futures → pil_practitioner_reports
→ comprehensive_report → ai_overseer → reconcile
```

### 1.4 발견·문서화한 런타임 특성

`dispatch_bitget_mode`는 critical step 실패 시 **중단하지 않고** 모든 step을 실행한 뒤 `status_label=FAIL`을 반환한다.  
이는 주식 `factory_runtime`과 동일한 **“전 step 기록 후 판정”** 패턴이며, Track A 테스트가 이를 명시적으로 고정했다.

---

## 2. Regime / Meta / Kelly 감사 (`regime_audit.py`)

### 2.1 목적

코인 Bitget 시스템의 Regime·Kelly가 **주식 `system_config.sqlite`와 완전 분리**되어 `bitget_system_config.sqlite` + Bitget meta KV/JSON에서 읽히는지 감사.

### 2.2 감사 항목

| 영역 | 검사 |
|------|------|
| **격리 (isolation)** | `config_db` = `bitget_system_config.sqlite`, 경로에 `bitget` 포함 |
| **Regime** | `CURRENT_REGIME_KEY`, `REGIME_ANALYSIS`, `META_REGIME_KEY` 정합 |
| **Meta 상태** | `is_config_regime_misaligned`, `is_bitget_meta_degraded` |
| **Kelly** | `DYNAMIC_KELLY_RISK`, `resolve_trading_kelly_base` (meta cap/floor 반영), hard max 25% |

### 2.3 API

```python
from bitget.validation.regime_audit import run_regime_kelly_audit

report = run_regime_kelly_audit()
# report["passed"], report["regime"], report["kelly"], report["isolation"]
```

### 2.4 architecture_checks 연동

`run_architecture_checks()`에 `regime_kelly_audit` 체크 추가.  
기존 `check_config_meta_alignment()`는 `regime_audit` 위임으로 단순화 (상세 audit JSON 포함).

---

## 3. CI 실행 방법 (로컬 · GitHub Actions 공통)

```powershell
# Windows (로컬 Cursor)
$env:PYTHONPATH = "C:\path\to\Dual-Screener-Bot"
python -m unittest bitget.tests.test_phase8_track_a_mock_e2e -v
```

```bash
# Ubuntu / CI
export PYTHONPATH=/home/ubuntu/Dual-Screener-Bot
python -m unittest \
  bitget.tests.test_phase6_integration \
  bitget.tests.test_phase7_pipeline_e2e \
  bitget.tests.test_phase8_track_a_mock_e2e -v
```

**권장 CI gate:** 26 tests OK + `run_regime_kelly_audit()["passed"]` (서버 pull 후)

---

## 4. 변경 파일

| 파일 | 변경 |
|------|------|
| `validation/regime_audit.py` | **신규** — Regime/Kelly 감사 SSOT |
| `validation/architecture_checks.py` | `regime_kelly_audit` 연동 |
| `tests/test_phase8_track_a_mock_e2e.py` | **신규** — Mock E2E 11 tests |
| `docs/08_phase8_track_a_execution_report.md` | 본 문서 |
| `docs/README.md` | 인덱스 갱신 |

---

## 5. Track B/C를 위해 준비된 것

| Track | Track A에서 완료된 준비 |
|-------|-------------------------|
| **B (서버 데이터)** | 파이프라인 step 계약 고정 → `data_refresh` 후 regression 안전 |
| **B** | regime_audit → pull 후 `run_regime_kelly_audit()` 로 regime·Kelly 일치 즉시 확인 |
| **C (Cutover)** | architecture_checks + regime_audit가 cutover `architecture_ok`에 포함 |
| **C** | mock E2E가 깨지면 prelude/body 순서 drift를 push 전에 차단 |

---

## 6. Ubuntu 서버에서 할 일 (Track B/C 체크리스트)

```bash
# 1) Pull 후 CI 동일 테스트
git pull
python -m unittest discover -s bitget/tests -p "test_*.py" -v

# 2) Regime/Kelly 실측
python -c "from bitget.validation.regime_audit import run_regime_kelly_audit as r; import json; print(json.dumps(r(), indent=2, default=str))"

# 3) 데이터 보강 (Track B)
./bitget/deploy/bitget.sh --data-refresh   # 또는 runner --mode data_refresh
./bitget/deploy/bitget.sh --scan-all --skip-telegram  # bitget.sh에 플래그 없으면 runner 직접

# 4) Cutover (Track C, 48h parallel 후)
export BITGET_PIPELINE_SSOT=1
export BITGET_ASYNC_TELEGRAM=1
export BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot
./bitget/deploy/bitget.sh --cutover-check
```

---

## 7. 아키텍트 브리핑

### 로컬에서 검증된 것

1. **파이프라인 topology** — prelude → body → reconcile 순서가 코드 SSOT와 일치  
2. **실패 semantics** — critical vs optional, exit code, status_label  
3. **Config/Meta 격리** — Bitget SQLite only, equity DB 미참조  
4. **Kelly 병합** — meta `kelly_cap`이 `resolve_trading_kelly_base`에 반영됨 (unit test)

### 로컬에서 아직 검증하지 않은 것 (Track B/C)

- 실시장 OHLCV 500+ symbols (`load_test`)  
- forward closed ≥10 (`deep_dive` 실리포트)  
- Telegram·Gemini·Bitget API live 호출  
- systemd 48h parallel · `BITGET_PIPELINE_SSOT=1`

### 권장 Git 워크플로

```
로컬 Track A (완료) → git push → Ubuntu git pull
→ unittest 26 OK → regime_audit PASS → data_refresh (B) → cutover (C)
```

---

## 8. 격리 확인

- **미수정:** `factory_pipelines.py`, `forward/`, `system_auto_pilot.py`, `deploy/systemd/dante-*` (주식)  
- **수정:** `bitget/validation/*`, `bitget/tests/*`, `bitget/docs/*` only

---

## 9. 다음 단계

| 우선순위 | 작업 | 트랙 |
|----------|------|------|
| 1 | **git push** → 서버 pull + unittest | B 준비 |
| 2 | 서버 `data_refresh` + `record_baseline` | B |
| 3 | `daily_audit --skip-telegram` 실측 | B |
| 4 | 48h parallel + SSOT cutover | C |
