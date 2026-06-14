# 05 — Phase 6 실행 보고서 (버그 수정·검증·정리)

> **작성일:** 2026-06-14  
> **선행 작업:** `04_phase5_satellite_config.md`  
> **수정 범위:** `bitget/` only (루트 주식 파일 **미수정**)

---

## 0. Executive Summary

| 항목 | 결과 |
|------|------|
| **bug #2** — `forward/reports.py` deep_dive SQL 바인딩 | ✅ `market_type` lowercase 정규화 |
| **`data_miner.py` 중복 모듈 블록** | ✅ 단일 파일로 병합 |
| **회귀 테스트** | ✅ `tests/test_phase6_integration.py` 추가 |
| **shadow_tracking DB_PATH** | ✅ 이미 `market_data_db_path()` 사용 중 (변경 없음) |

Phase 6은 **남아 있던 진단 항목(버그 #2, data_miner 구조)** 을 마무리하고, 파이프라인·config·DB 스키마에 대한 **최소 회귀 테스트**를 추가했다.

---

## 1. bug #2 — deep_dive / comprehensive report SQL 바인딩

### 1.1 원인

`bitget_forward_trades.market_type` 컬럼은 `ledger.py`에서 **항상 lowercase** (`spot` / `futures`)로 INSERT된다.

```python
# ledger.py (기존 SSOT)
market_type = str(market_type).lower()
params=(str(market_type).lower(),)  # track_daily_positions
```

반면 `forward/reports.py`는 `params=(market_type,)` 로 **대소문자 그대로** 바인딩했다.  
호출부(`bitget_pipelines`, `auto_pilot`)는 소문자를 쓰지만, 레거시·수동 호출(`"SPOT"`, `"FUTURES"`) 시 **0건 조회 → 표본 부족 스킵** 또는 빈 리포트가 발생할 수 있었다.

### 1.2 수정

`bitget/forward/reports.py`에 `_norm_market_type()` 헬퍼 추가 후, 아래 경로에 적용:

| 함수 | 변경 |
|------|------|
| `send_comprehensive_daily_report()` | `params=(mkt,)` + treasury 키 분기 |
| `run_deep_dive_analysis()` | SQL 바인딩·메시지·ANTI_PATTERNS 저장 시 `mkt` 사용 |

```python
def _norm_market_type(market_type: str) -> str:
    return str(market_type or "spot").strip().lower()
```

**효과:** `"SPOT"` / `"Futures"` 등 혼합 입력도 DB SSOT와 일치하는 조회가 보장된다.

---

## 2. `data_miner.py` 중복 블록 정리

### 2.1 Before

파일 하단(404행~)에 **두 번째 import 블록**이 이어 붙어 있었다.

- 블록 A (1~403): GMM DNA + AST alpha evolution
- 블록 B (404~523): KMeans cluster mining (`build_supernova_csv`, `run_cluster_mining`)
- `if __name__ == "__main__"` **2개** — 실행 진입점 충돌

### 2.2 After

단일 모듈로 병합:

```
import (GMM + KMeans + config_hub + data_paths + supernova_hunter)
  → mine_bitget_dna_templates / evolve_bitget_ast_formulas
  → build_supernova_csv / run_cluster_mining
  → run_bitget_data_miner()  # GMM + AST + cluster (cluster 실패 시 skip)
```

- `save_config_atomic` 단일 저장 경로
- `run_bitget_data_miner()` 마지막에 `run_cluster_mining()` 호출 (예외 시 경고만 출력)

---

## 3. 회귀 테스트 (`tests/test_phase6_integration.py`)

| 테스트 클래스 | 검증 내용 |
|---------------|-----------|
| `TestReportsMarketTypeBinding` | `_norm_market_type` + `"SPOT"` 입력 시 spot CLOSED만 deep_dive에 포함 |
| `TestConfigHubRoundtrip` | `save_config_atomic` → `load_config` SQLite roundtrip |
| `TestArtifactGuardSchema` | `init_forward_db` 후 `verify_bitget_market_db_schema` OK |

### 3.1 실행 방법 (Ubuntu / dev)

```bash
cd /path/to/Dual-Screener-Bot
python -m unittest bitget.tests.test_phase6_integration -v
```

---

## 4. 서버 검증 체크리스트 (cutover)

Phase 1~6 통합 후 서버에서 권장하는 확인 순서:

```bash
# 1) SSOT readiness
./bitget/deploy/bitget.sh --cutover-check

# 2) 단위 테스트
python -m unittest discover -s bitget/tests -p "test_*.py" -v

# 3) daily_audit dry-run (텔레그램 발송 주의 — 스테이징 권장)
./bitget/deploy/bitget.sh --daily-audit

# 4) watchdog heartbeat 컴포넌트 확인
# BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot
```

---

## 5. Phase 1~6 통합 아키텍처 (현재)

```
cron/수동     → bitget.sh → pipelines.runner → bitget_pipelines.py
24/7 daemon   → bitget_auto_pilot --daemon
prelude       → meta_sync → artifact_guard → config_bootstrap → …
config        → config_hub → config_manager → SQLite KV
meta/kelly    → governance/meta_sync + meta_consumer
forward DB    → data_paths.market_data_db_path()
reports       → reports._norm_market_type (spot/futures SSOT)
legacy        → main / launcher / sentinel BLOCKED
```

---

## 6. 잔여 항목 (Phase 7+ 후보)

| 항목 | 우선순위 | 비고 |
|------|----------|------|
| E2E pipeline pytest (runner `--mode` mock) | P2 | cron 환경 변수·텔레그램 mock 필요 |
| `practitioner_bitget_adapter` 대소문자 방어 | P3 | 루프가 이미 `("spot","futures")` 고정 |
| 서버 실측 `daily_audit` regime·Kelly 일치 검증 | P1 | 운영 승인 후 |
| 루트 `practitioner_intelligence` PYTHONPATH 의존 | 설계 | 주식 read-only import 유지 |

---

## 7. 변경 파일 목록

| 파일 | 변경 유형 |
|------|-----------|
| `bitget/forward/reports.py` | bug #2 fix |
| `bitget/data_miner.py` | 중복 블록 제거·병합 |
| `bitget/tests/test_phase6_integration.py` | 신규 |
| `bitget/docs/05_phase6_bugfix_and_validation.md` | 본 문서 |
| `bitget/docs/README.md` | 인덱스 갱신 |

---

## 8. 격리 확인

- **미수정 (주식):** `factory_pipelines.py`, `forward/`, `system_auto_pilot.py`, `deploy/systemd/dante-*` 등
- **수정:** `bitget/` 하위만
