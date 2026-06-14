# 07 — Phase 8 가능성 검토 (현재 환경 기준)

> **작성일:** 2026-06-14  
> **목적:** Phase 7 이후 **지금 이 환경(Windows dev + 로컬 DB)** 에서 Phase 8을 어떤 방향으로 갈 수 있는지 검토  
> **성격:** 계획·승인용 문서 (코드 변경 없음)  
> **선행:** `06_phase7_pipeline_e2e_and_cutover.md`

---

## 0. Executive Summary

Phase 8은 **한 가지 작업이 아니라 3개 트랙**으로 나뉜다. 지금 환경에서 **동시에 전부** 진행할 수는 없다.

| 트랙 | 환경 | 지금 가능? | 핵심 목표 |
|------|------|------------|-----------|
| **A — Dev/CI** | Windows + `PYTHONPATH` | ✅ **즉시** | `daily_audit` mock E2E, step 계약 테스트, regime/Kelly 자동 감사 |
| **B — 데이터 보강** | Windows + Bitget API + 네트워크 | ⚠️ **부분** | `data_refresh` → scan → baseline → 제한적 `daily_audit` |
| **C — 프로덕션 Cutover** | Ubuntu + systemd + 48h parallel | ❌ **서버 필수** | `BITGET_PIPELINE_SSOT=1`, cron·watchdog 실운영 전환 |

**결론:** Phase 8은 **Track A를 먼저 완료**하고, Track B는 API·DB가 준비되면, Track C는 Ubuntu 서버 승인 후 진행하는 것이 현실적이다.

---

## 1. 현재 환경 스냅샷 (실측)

검토 시점 환경: **Windows 10**, workspace `Dual-Screener-Bot\bitget`, Python unittest·architecture_checks 실행 가능.

### 1.1 데이터·경로

| 항목 | 값 |
|------|-----|
| `bitget_data_dir()` | `...\Dual-Screener-Bot\bitget` |
| `bitget_market_data.sqlite` | **존재** |
| `bitget_system_config.sqlite` | **존재** |
| `bitget_forward_trades` | **0건** (closed/open 모두 0) |
| OHLCV 테이블 (`BITGET_%`) | **4개** (load_test 기준 symbol 0) |

### 1.2 검증 상태

| 검사 | 결과 | 비고 |
|------|------|------|
| `run_architecture_checks()` | **PASS** | legacy·prelude·config_hub·meta 정합 OK |
| `CURRENT_REGIME_KEY` / `META_REGIME_KEY` | **CHOP / CHOP** | misaligned·degraded 없음 |
| `run_load_test(min_symbols=1)` | **FAIL** | symbol_count=0 (서버급 DB 아님) |
| `check_cutover_readiness()` | **FAIL** | `pipeline_ssot_env=0`, parallel 미시작, `async_telegram=0` |
| `architecture_ok` | **True** | cutover env만 미충족 |

### 1.3 테스트 현황 (Phase 6~7)

```bash
# 실행 확인됨 (15 tests OK)
python -m unittest bitget.tests.test_phase6_integration bitget.tests.test_phase7_pipeline_e2e -v
```

---

## 2. 환경별 제약 (Windows dev vs Ubuntu prod)

| 제약 | Windows (지금) | Ubuntu (서버) |
|------|----------------|---------------|
| `bitget.sh` | bash 필요 → **WSL/Git Bash** 또는 `python -m bitget.pipelines.runner` 직접 호출 | ✅ 표준 |
| `fcntl` job lock | **비활성** (`runtime.py` win32 스킵) | ✅ cron 중복 실행 방지 |
| `pgrep bitget.main` | **항상 false** (legacy 프로세스 감지 불가) | ✅ cutover 감시 가능 |
| systemd `dante-bitget-*` | ❌ | ✅ 24/7 SSOT |
| WebSocket `dante-bitget-ws` | ❌ (미설치) | scan/track 실시간 품질 |
| `BITGET_DB_STORAGE_PATH` 분리 | 미설정 시 `bitget/` 패키지 디렉터리 사용 | 서버는 별도 data root 권장 |
| 루트 `PYTHONPATH` | **필수** (`practitioner_intelligence`, `llm_gemini_core`, `reports/*`, `telegram_message_queue`) | `bitget.sh`가 자동 설정 |

---

## 3. Phase 8 후보 작업 — 가능성 평가

### 3.1 종합 매트릭스

| # | 후보 작업 | 지금 (Win) | 서버 (Ubuntu) | 선행 조건 |
|---|-----------|------------|---------------|-----------|
| 1 | `daily_audit` **mock E2E** (step patch) | ✅ 권장 | ✅ | 없음 |
| 2 | step별 **계약 테스트** (temp SQLite) | ✅ 권장 | ✅ | 없음 |
| 3 | **regime·Kelly 자동 감사** (architecture 확장) | ✅ 가능 | ✅ | config/meta DB |
| 4 | `daily_audit` **실행** (전체) | ⚠️ 부분 | ✅ | forward 데이터·API·텔레그램 정책 |
| 5 | `deep_dive` / comprehensive **실측** | ❌ 표본 0 | ⚠️ closed≥10 필요 | 포워드 체결 이력 |
| 6 | **PIL** 전체 발송 | ⚠️ import만 | ⚠️ | `practitioner_intelligence` + trade rows |
| 7 | `data_refresh` / `gap_heal` | ⚠️ API 키 | ✅ | Bitget API + ccxt |
| 8 | `scan_all` + signal parity | ⚠️ 빈 DB | ✅ | OHLCV 500+ symbols |
| 9 | `record_baseline` / `--validate` | ❌ sent_log 없음 | ✅ | scan 운영 후 |
| 10 | `load_test` PASS | ❌ symbol 0 | ✅ | 서버 DB |
| 11 | `start_parallel` 48h | ❌ | ✅ | systemd + cron 병렬 |
| 12 | **`BITGET_PIPELINE_SSOT=1` cutover** | ❌ | ✅ | 48h + env 4종 |
| 13 | 24/7 `bitget_auto_pilot --daemon` | ⚠️ 가능하나 WS 없음 | ✅ | `dante-bitget-ws` 권장 |
| 14 | 실거래·OMS 검증 | ⚠️ dry_run만 안전 | ⚠️ | `ENABLE_REAL_EXECUTION` 확인 필수 |

---

## 4. `daily_audit` step별 세부 검토

파이프라인 순서 (SSOT):

```
meta_governor_sync → artifact_guard → config_bootstrap → sentiment_mining
→ doomsday_radar → track_spot → track_futures
→ deep_dive_spot → deep_dive_futures → pil_practitioner_reports
→ comprehensive_report → ai_overseer → reconcile
```

| Step | 외부 의존 | 로컬(지금) | 비고 |
|------|-----------|------------|------|
| `meta_governor_sync` | config DB, `detect_coin_regime` | ✅ 실행 가능 | 실측 시 CHOP 동기 OK |
| `artifact_guard` | market DB 스키마 | ✅ | 빈 DB도 heal |
| `config_bootstrap` | JSON bootstrap | ✅ | |
| `sentiment_mining` | **HTTP** (FGI, Cointelegraph) | ⚠️ 네트워크 | 실패해도 critical=False |
| `doomsday_radar` | macro 모듈 | ⚠️ | 네트워크·설정 의존 |
| `track_spot/futures` | OHLCV 테이블, OPEN 포지션 | ⚠️ | OPEN 0이면 조기 종료만 |
| `deep_dive_*` | closed trades **≥10** | ❌ skip | 현재 0건 → 표본 부족 메시지 |
| `pil_practitioner_reports` | 루트 `practitioner_intelligence` | ⚠️ | PYTHONPATH + 빈 DF 시 무발송 |
| `comprehensive_report` | forward rows + **Telegram** | ⚠️ | `--skip-telegram` 권장 |
| `ai_overseer` | **Gemini API** (선택), Telegram | ⚠️ | API 없으면 fallback 감사 |
| `reconcile` | OMS / exchange (선택) | ⚠️ | API 없으면 제한적 |

**지금 환경에서 `daily_audit` 전체를 돌리면:** prelude는 통과하지만, 리포트·딥다이브·PIL은 **빈 데이터 스킵**이 대부분이며, 텔레그램·LLM 설정에 따라 **부수 발송** 위험이 있다.

**안전한 로컬 실행 예 (코드 경로만 검증):**

```powershell
$env:PYTHONPATH = "C:\Users\GoodLife\Desktop\quant\Dual-Screener-Bot"
python -m bitget.pipelines.runner --mode daily_audit --skip-telegram --dry-run
# dry-run: step 미실행, step 이름만 로그
```

실제 body 실행 시:

```powershell
python -m bitget.pipelines.runner --mode daily_audit --skip-telegram
```

---

## 5. 권장 Phase 8 로드맵 (3트랙)

### Track A — Dev/CI (지금 시작, `bitget/` only)

**목표:** 서버 없이 **파이프라인 계약·회귀**를 고정한다.

| 순서 | 작업 | 산출물 | 완료 기준 |
|------|------|--------|-----------|
| A1 | `daily_audit` mock E2E harness | `tests/test_phase8_daily_audit_e2e.py` | 13 step 순서·prelude 호출·critical 실패 전파 검증 |
| A2 | step fn 단위 smoke (temp DB seed) | 동일 또는 분리 테스트 | `meta_sync`·`artifact_guard`·`reports._norm_market_type` 회귀 |
| A3 | `check_regime_kelly_audit()` | `validation/regime_audit.py` | config vs meta vs Kelly 필드 JSON 리포트 |
| A4 | cutover JSON 리포트 | `runner --mode cutover_check` 출력 고정 | CI에서 `architecture_ok=True` assert |

**예상 공수:** 1~2일 (코드+문서, 서버 불필요)  
**리스크:** 낮음 (mock 위주, Telegram·exchange 미호출)

---

### Track B — 데이터 보강·제한 실측 (API·네트워크 필요)

**목표:** 로컬 또는 스테이징 DB를 **서버에 가깝게** 만든 뒤, 일부 실측을 한다.

| 순서 | 작업 | 선행 | 완료 기준 |
|------|------|------|-----------|
| B1 | `.env` Bitget API 키 확인 | `BITGET_ACCESS_KEY` 등 | `data_refresh` 오류 없음 |
| B2 | `python -m bitget.pipelines.runner --mode data_refresh` | B1 | `BITGET_%` 테이블·symbol 수 증가 |
| B3 | `gap_heal` (선택) | B2 | 누락 봉 보정 |
| B4 | `scan_all --skip-telegram` | B2 | `sent_log_bitget_master.txt` 생성 |
| B5 | `record_baseline` → `validate` | B4 | parity PASS (또는 baseline 갱신 합의) |
| B6 | 포워드 OPEN/closed 시드 축적 | scan/track 운영 | deep_dive 표본 ≥10 |
| B7 | `daily_audit --skip-telegram` | B6 | regime·Kelly 리포트 본문에 CHOP 일치 |

**예상 공수:** 2~5일 (API rate limit·데이터 축적 포함)  
**리스크:** 중간 — 텔레그램 off 필수, 실거래 플래그 재확인

**지금 로컬 DB 상태로는 B6·B7이 사실상 불가** (forward 0건). B1~B4는 API만 있으면 진행 가능.

---

### Track C — Ubuntu 프로덕션 Cutover (서버 전용)

**목표:** Phase 7 `cutover_check` **passed=True** 후 레거시 완전 퇴출.

| 순서 | 작업 | 환경 변수 / 유닛 |
|------|------|------------------|
| C1 | `deploy_bitget_factory.sh` 설치 | `INSTALL_ROOT`, `BITGET_DB_STORAGE_PATH` |
| C2 | `dante-bitget-ws` → factory → async 기동 | systemd |
| C3 | `./bitget.sh --start-parallel` | 48h window |
| C4 | 매일 `--validate` 또는 `--validate-all` | baseline 유지 |
| C5 | env 설정 | `BITGET_PIPELINE_SSOT=1`, `BITGET_ASYNC_TELEGRAM=1`, `BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot` |
| C6 | `--cutover-check` **passed=True** | architecture_ok 포함 |
| C7 | `daily_audit` cron 실측 | regime·Kelly·리포트 일치 운영 확인 |
| C8 | legacy 프로세스 중단 확인 | `pgrep bitget.main` 없음 |

**예상 공수:** 48h parallel + 운영 감시 (캘린더 2~3일)  
**리스크:** 높음 — 잘못된 cutover 시 이중 실행·텔레그램 폭주

**지금 Windows 환경에서는 C1~C8 전부 불가** (systemd·parallel·프로덕션 DB 전제).

---

## 6. Track 선택 가이드

```
                    ┌─────────────────────────────────────┐
                    │  Phase 8 목표가 무엇인가?            │
                    └─────────────────┬───────────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          ▼                           ▼                           ▼
   "코드 회귀·테스트"          "로컬에서 리포트 맛보기"      "실서버 cutover"
          │                           │                           │
          ▼                           ▼                           ▼
      Track A ✅                  Track B ⚠️                  Track C ❌
   (지금 즉시)              (API+데이터 필요)            (Ubuntu 필수)
```

| 사용자 의도 | 권장 트랙 |
|-------------|-----------|
| 문서 승인 후 코드만 안전하게 추가 | **A** |
| deep_dive·PIL이 실제로 도는지 보고 싶다 | **B** (데이터부터) |
| 프로덕션 SSOT 전환 | **C** (서버 작업) |

---

## 7. 지금 환경에서 **하지 말아야 할 것**

| 행동 | 이유 |
|------|------|
| `BITGET_PIPELINE_SSOT=1` 만 설정하고 cutover 완료로 간주 | parallel 48h·async_telegram·서버 프로세스 미검증 |
| `--skip-telegram` 없이 `daily_audit` 반복 실행 | 빈 리포트·에러 알림이 Telegram으로 갈 수 있음 |
| `ENABLE_REAL_EXECUTION` 확인 없이 scan/executor 테스트 | dry_run이 아니면 주문 시도 가능 |
| 루트 `forward/`·`factory_pipelines.py` 수정으로 PIL 우회 | 격리 원칙 위반 |
| Windows flock 부재를 “cron 안전”으로 가정 | 동시 runner 2개 실행 시 DB 경합 가능 |

---

## 8. Phase 8 승인 시 권장 문서·코드 순서

사용자 규칙(**문서 선행 → 승인 → 코드**)에 따른 제안:

1. **본 문서 승인** — 트랙 A/B/C 중 어디까지 할지 선택  
2. Track A 승인 시 → `08_phase8_track_a_execution_report.md` + 테스트·`regime_audit` 구현  
3. Track B 승인 시 → API·데이터 보강 실행 보고서 (별도)  
4. Track C 승인 시 → Ubuntu RUNBOOK 체크리스트 실측 기록 (서버)

---

## 9. Phase 8 완료 정의 (트랙별)

| 트랙 | 완료 정의 |
|------|-----------|
| **A** | mock E2E green + regime audit 함수 + CI unittest 20건+ |
| **B** | load_test PASS + forward closed≥10 + `daily_audit --skip-telegram` prelude~report 무크래시 |
| **C** | `cutover_check passed=True` 24h 유지 + cron `daily_audit` regime=meta 일치 로그 |

---

## 10. 잔여 의존성 (Phase 8에서 다루지 않음)

| 항목 | 처리 시점 |
|------|-----------|
| 루트 `practitioner_intelligence` PYTHONPATH | 설계 유지 (read-only import) |
| `sentiment_miner` → `news_data.sqlite` 경로 SSOT | 선택적 Phase 9 (아직 `BASE_DIR` 하드코딩) |
| Windows용 `bitget.ps1` wrapper | 필요 시 별도 — 현재 `runner` 직접 호출로 대체 가능 |

---

## 11. 참고 명령 (현재 Windows)

```powershell
# PYTHONPATH (세션마다)
$env:PYTHONPATH = "C:\Users\GoodLife\Desktop\quant\Dual-Screener-Bot"

# 아키텍처만
python -c "from bitget.validation.architecture_checks import run_architecture_checks as r; print(r()['passed'], r()['failed'])"

# cutover (정보성)
python -m bitget.pipelines.runner --mode cutover_check --skip-telegram

# daily_audit 드라이런 (step 미실행)
python -m bitget.pipelines.runner --mode daily_audit --dry-run --skip-telegram

# unittest
python -m unittest discover -s bitget/tests -p "test_*.py" -v
```

Ubuntu 서버에서는 동일 작업을 `./bitget/deploy/bitget.sh` 로 수행한다.

---

## 12. 격리 확인

- 본 문서는 **계획만** 포함 — 코드 변경 없음  
- Phase 8 구현 시에도 **`bitget/` only** 원칙 유지  
- 주식 루트 모듈은 **read-only import** (`practitioner_intelligence`, `llm_gemini_core` 등)

---

## 13. 다음 액션 (승인 대기)

| 우선순위 | 제안 | 트랙 |
|----------|------|------|
| **1** | Track A — `daily_audit` mock E2E + `regime_audit` | A |
| 2 | Track B — `data_refresh` 후 symbol/load_test 보강 | B |
| 3 | Track C — Ubuntu 48h parallel + SSOT cutover | C |

**Track A 승인 시** `08_phase8_track_a_*.md` 실행 보고서와 함께 코드 작업을 시작한다.
