# IMPORT_ERROR_AUDIT — 리포트 렌더링 NameError 전수 감사

**감사 일시:** 2026-05-27  
**증상:** 텔레그램 일일 리포트 송출 중 `NameError` 다발 → 실무자(PIL)·KR/US 9분할 리포트 중단  
**범위:** `forward/` 패키지 분리 이후 리포트·딥다이브·실무자 경로 (`scripts/split_forward_tester.py` 리팩토링 영향)

---

## Executive Summary

| 항목 | 결론 |
|------|------|
| **직접 원인** | `forward/deep_dive.py` 상단의 `from forward.shared import *` 가 **언더스코어(`_`)로 시작하는 심볼을 가져오지 않음** (PEP 448 / `__all__` 미정의 시 기본 동작) |
| **에러로 보고된 2건** | 두 함수 모두 **`forward/shared.py`에 정의**되어 있으나, `deep_dive.py` 네임스페이스에 **바인딩되지 않음** |
| **영향 파일** | **`forward/deep_dive.py` 단일 파일** (P0). `forward/ledger.py` 등 다른 `import *` 소비자는 해당 private 심볼 미사용 |
| **오진 가능성** | `mutant_oos_validator.py`의 `_open_market_db_ro`는 **별도 로컬 정의** — 이번 리포트 장애와 무관 |
| **권장 P0 픽스** | `deep_dive.py`에 `forward.shared`로부터 **명시적 named import 블록** 추가 (아래 §1.3) |

---

## 1. 누락된 Import 추적 및 픽스 계획 (P0)

### 1.1 심볼 정의 위치 (정확한 소스)

| 심볼 | 정의 모듈 | 라인(대략) | 역할 |
|------|-----------|------------|------|
| `_normalize_trade_market` | `forward/shared.py` | L871 | 종목코드·market 컬럼 → `KR`/`US` 정규화 |
| `_open_market_db_ro` | `forward/shared.py` | L74 | 리포트·딥다이브용 **메인 DB** read-only URI 연결 (`report_db_read_path()`) |
| `_reporter_cleanup_zombie_forward_trades` | `forward/shared.py` | L980 | OPEN 좀비 행 정리 |
| `_reporter_valid_holding_mask` | `forward/shared.py` | L839 | 유효 보유(OPEN·수량>0) 마스크 |
| `_reporter_deploy_fleet_mask` | `forward/shared.py` | L931 | VIP 편대(주도/차기) 마스크 |
| `_daily_report_trades_for_market` | `forward/shared.py` | L964 | 9분할 `load_market_slice`용 시장 필터 |
| `_strategy_colosseum_brief` | `forward/shared.py` | L197 | 콜로세움 브리핑 HTML |
| `_shadow_performance_brief` | `forward/shared.py` | L542 | 그림자 장부 브리핑 |
| `_tier80_sync_effective_and_report_line` | `forward/shared.py` | L1098 | [5/9] 티어80 동기·표시 |
| `_parse_mkt_group_key` | `forward/shared.py` | L887 | PIL `mkt_group` → (market, group) |
| `_exit_date_on_calendar` | `forward/shared.py` | L921 | 청산일 캘린더 문자열 |
| `_format_exit_reason_display` | `forward/shared.py` | L857 | PIL 청산 사유 포맷 |
| `_safe_final_ret_pct` | `forward/shared.py` | L901 | PIL 수익률 안전 캐스트 |
| `_win_loss_flat_counts` | `forward/shared.py` | L912 | PIL 승/패/무 카운트 |
| `_spillover_fallback_enabled` | `forward/shared.py` | L1162 | V28 스필오버 fallback 플래그 |
| `_format_forward_ledger_error_html` | `forward/shared.py` | L665 | `run_deep_dive_analysis` 예외 텔레그램 HTML |

**참고 — 사용자가 언급한 대안 경로:**

- `market_db_paths.py`: `report_db_read_path()`, `report_read_source_label()` 만 제공. **`_open_market_db_ro`는 없음** (내부에서 `report_db_read_path()`를 쓰는 래퍼는 `shared.py`에만 존재).
- `forward_market_guard.py`: `enforce_market_frame` 등 — `_normalize_trade_market` 없음.
- `mutant_oos_validator.py` L98: **동명이지만 별도 구현** (OOS 검증 전용).

### 1.2 근본 메커니즘 — “Import 누락”이 아니라 “Star Import 블라인드 스팟”

`forward/deep_dive.py` L2:

```python
from forward.shared import *  # noqa: F403
```

Python 규칙 (`forward/shared.py`에 `__all__` 없음):

- `import *` 는 **이름이 `_`로 시작하지 않는** 공개 심볼만 가져옴.
- 따라서 `load_system_config`, `send_telegram_msg`, `pd`, `pytz` 등은 정상.
- **`_open_market_db_ro`, `_normalize_trade_market` 등 private 헬퍼는 절대 바인딩되지 않음** → 첫 호출 시 `NameError`.

리팩토링 전 `auto_forward_tester.py` 단일 파일에서는 동일 모듈 내 정의·호출이라 문제가 없었음.  
`scripts/split_forward_tester.py`가 생성한 split 모듈이 **기존 모놀리식의 `import *` 패턴을 그대로 복제**하면서 회귀 발생.

### 1.3 에러 ↔ 호출 경로 매핑

| 사용자 보고 | 함수 | `deep_dive.py` 호출 위치 | 트리거 함수 |
|-------------|------|-------------------------|-------------|
| 실무자 리포트 전역 에러 | `_normalize_trade_market` | L495–497 | `send_group_practitioner_reports` |
| KR/US 리포트 에러 | `_open_market_db_ro` | L146, L650, L854, L868 | `send_comprehensive_daily_report`, `run_deep_dive_analysis` |

동일 파일에서 **추가로 터질 수 있는** (아직 해당 경로까지 실행 안 됐을 수 있음) 심볼:

- `send_comprehensive_daily_report`: L66, L113, L119, L153–154, L239, L277, L345
- `send_group_practitioner_reports`: L468, L503, L527, L556, L576–578
- `run_deep_dive_analysis`: L859, L993

### 1.4 P0 픽스 계획 (권장 diff — `forward/deep_dive.py`)

**파일 최상단** (`from forward.shared import *` **아래**에 명시 import 추가):

```python
from forward.shared import (  # reporter / DB private helpers — NOT exported by import *
    _open_market_db_ro,
    _normalize_trade_market,
    _reporter_cleanup_zombie_forward_trades,
    _reporter_valid_holding_mask,
    _reporter_deploy_fleet_mask,
    _daily_report_trades_for_market,
    _strategy_colosseum_brief,
    _shadow_performance_brief,
    _tier80_sync_effective_and_report_line,
    _parse_mkt_group_key,
    _exit_date_on_calendar,
    _format_exit_reason_display,
    _safe_final_ret_pct,
    _win_loss_flat_counts,
    _spillover_fallback_enabled,
    _format_forward_ledger_error_html,
)
```

**대안 (중기):** `forward/reporter_db.py` 등으로 private 헬퍼만 분리 → `shared.py`·`deep_dive.py` 양쪽에서 import (§2 참고).

**검증 명령 (로컬):**

```bash
python -c "from forward.deep_dive import send_comprehensive_daily_report, send_group_practitioner_reports; print('import ok')"
python -m pyflakes forward/deep_dive.py
# 또는
ruff check forward/deep_dive.py --select=F821
```

### 1.5 리팩토링 연쇄 Import 전수 스캔 — 추가 픽스 목록

#### A. `forward/deep_dive.py` — **P0 (NameError 확정)**

위 §1.3 전체 심볼 블록.

#### B. `forward/ledger.py` — **이번 NameError 해당 없음**

- `from forward.shared import *` 사용.
- `safe_float_cast` 등은 **`forward_report_scalar`에서 명시 import** (L5–10) — 올바른 패턴.
- `_reporter_*` / `_open_market_db_ro` 미참조.

#### C. `forward/rotation_report_section.py` — **정상 (지역 import)**

- `is_rotation_eligible_sector` → `rotation_sector_filter` (L12–16).
- `_load_daily_series` → `sector_rotation_store` 함수 내부 import (L165–169).

#### D. `forward/deathmatch_report_section.py`, `forward/dna_autopsy.py` — **정상**

- `DailyReportContext` 등 명시 import만 사용, `shared` private 미참조.

#### E. `practitioner_*.py` — **이번 장애 직접 원인 아님**

- `practitioner_report_context.py`: DB 연결을 **인라인** `sqlite3.connect` + `report_db_read_path()` 사용 (L44–48).
- `_normalize_trade_market` 미사용 — **호출부가 `deep_dive.send_group_practitioner_reports`**.

| 심볼 | 스캔 결과 |
|------|-----------|
| `safe_float_cast` | `forward/ledger.py` ✅ 명시 import / `forward/shared.py` ✅ `forward_report_scalar` 경유 |
| `is_rotation_eligible_sector` | `rotation_report_section.py`, `sector_rotation_store.py` ✅ 명시 import |
| `col_series`, `scalar_float`, `series_mean` | `deep_dive`는 `import *`로 **public** 이름만 필요 — `forward_report_scalar`가 shared를 통해 노출됨 ✅ |

#### F. `auto_forward_tester.py` / `forward/__init__.py` — **간접 영향**

- Facade가 `deep_dive`를 re-export. **`deep_dive` 수정으로 해결** (facade 자체 변경 불필요).

#### G. `bitget/forward_tester.py` — **별도 코드베이스**

- 여전히 **모놀리식** 리포트 구현 가능. equity cron은 `factory_pipelines.py` → `auto_forward_tester` 경로이므로 **split `forward/` 수정이 실제 운영 SSOT**.

---

## 2. 순환 참조(Circular Import) 위험 점검

### 2.1 현재 의존 그래프 (요약)

```
forward/deep_dive.py
  → forward/shared.py  (explicit private imports 제안)
  → market_db_paths, forward_report_scalar, report_*, …
  ✗ shared.py 는 deep_dive / ledger 를 import 하지 않음

forward/ledger.py → forward/shared.py (star only)

forward/deathmatch_report_section.py → daily_report_context (only)
forward/rotation_report_section.py → daily_report_context, rotation_sector_filter
```

**결론:** `deep_dive.py` 최상단에 `from forward.shared import (_open_market_db_ro, …)` 추가 시 **순환 참조 위험 낮음**.

### 2.2 Import 위치 설계 가이드

| 전략 | 적용 | 순환 위험 | 비고 |
|------|------|-----------|------|
| **A. 모듈 최상단 explicit import** (권장 P0) | `deep_dive` ← `shared` private | 낮음 | 정적 분석·IDE가 잡기 쉬움 |
| **B. 함수 내부 lazy import** | 무거운 optional 경로 | 중간 | 이미 `build_rotation_spillover_section` 등에서 사용 중 — **필수 아님** |
| **C. `shared.__all__`에 `_` 심볼 나열** | star로 private 노출 | 낮음 | API 오염, `# noqa` 남발 — 비권장 |
| **D. `forward/reporter_internal.py` 신설** | DB+reporter 헬퍼만 | 낮음 | shared 2100+ 라인 감소, **중기 리팩토링** |

### 2.3 주의할 잠재 순환 (향후)

- `forward/shared.py`가 커지며 **상호 import**를 추가하지 말 것 (예: `deep_dive`를 shared에서 호출).
- `daily_report_context`가 `forward.shared`를 끌어오기 시작하면 `deep_dive` ↔ `context` 루프 가능 — 현재는 **없음**.

### 2.4 `import *` 정책 (아키텍처)

- **신규 코드:** `import *` 금지, public API는 `forward/__init__.py` `__all__` + explicit re-export.
- **레거시 `deep_dive`:** star는 public만 유지하고, **private는 항상 named import 블록** (이번 패턴을 표준으로).

---

## 3. [역제안] 정적 분석(Linting) 안전장치 도입

### 3.1 문제 정의

`NameError`는 **첫 프로덕션/크론 실행**까지 드러나지 않음. 리팩토링·split 시 회귀 비용이 큼.  
목표: **커밋/배포 전에 F821(정의되지 않은 이름) 100% 차단**.

### 3.2 권장 스택 (경량 → 강력)

| 도구 | 역할 | 설정 예시 |
|------|------|-----------|
| **[Ruff](https://docs.astral.sh/ruff/)** | F821 `undefined-name`, F403/F405 `import *` | `pyproject.toml` |
| **pyflakes** | 동일 F821 (Ruff 미사용 시) | CI one-liner |
| **pre-commit** | 커밋 훅 | `.pre-commit-config.yaml` |
| **mypy** (선택) | 타입·일부 이름 | `ignore_missing_imports` 단계적 |

`requirements-dev.txt` (신규 권장):

```text
ruff>=0.4.0
pre-commit>=3.0.0
```

`pyproject.toml` (프로젝트 루트 신규):

```toml
[tool.ruff]
target-version = "py311"
line-length = 120
src = ["."]

[tool.ruff.lint]
select = [
  "F821",  # undefined name — 이번 버그 클래스
  "F822",  # undefined export
  "F823",  # undefined local
  "F401",  # unused import
  "F403",  # import * (경고: star 남용 탐지)
  "F405",  # star에서 undefined name 가능성
]
ignore = ["E501"]  # 필요 시

[tool.ruff.lint.per-file-ignores]
"forward/deep_dive.py" = []  # 픽스 후 F821 0건 목표
"auto_forward_tester.py" = ["F403", "F405"]  # 단기 facade 예외 가능
```

### 3.3 CI / 로컬 파이프라인 (복붙 가능)

**로컬·PR 게이트:**

```bash
ruff check forward/ auto_forward_tester.py factory_pipelines.py manual_report_trigger.py --select=F821,F822,F823
python -m compileall -q forward
```

**pre-commit (`.pre-commit-config.yaml`):**

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.4.4
    hooks:
      - id: ruff
        args: [--select, F821,F822,F823,F403,F405]
      - id: ruff-format
```

**factory.sh / cron 전 실행 (선택):**

```bash
ruff check forward/deep_dive.py --select=F821 || { echo "F821: abort daily report"; exit 1; }
```

### 3.4 `import *` 완화 정책

1. **단기:** `deep_dive` private explicit import (§1.4).  
2. **중기:** `forward/shared.py`에서 `__all__` 정의 — **public만** 나열; private는 절대 포함하지 않음.  
3. **장기:** Facade `auto_forward_tester.py`만 얇게 유지, 구현 모듈은 전부 explicit import.

### 3.5 회귀 테스트 (런타임 보완)

정적 분석에 더해 **드라이런 import 테스트** 1개 추가 권장:

```python
# tests/test_forward_imports.py
def test_deep_dive_private_symbols_bound():
    import forward.deep_dive as dd
    for name in (
        "_open_market_db_ro",
        "_normalize_trade_market",
        "_reporter_cleanup_zombie_forward_trades",
    ):
        assert hasattr(dd, name), name
```

DB/텔레그램 없이 **모듈 로드·심볼 바인딩**만 검증.

### 3.6 기대 효과

| Before | After |
|--------|-------|
| split 후 첫 cron에서 PIL/KR/US 전멸 중단 | PR/커밋 단계에서 F821 실패 |
| `import *` + private 호출 조합 재발 | F403/F405 + explicit import 표준 |
| 수동 코드 리뷰에 의존 | `ruff` + `compileall` 자동 게이트 |

---

## 4. 실행 체크리스트 (P0 → P1)

- [ ] **P0** `forward/deep_dive.py` §1.4 explicit import 적용  
- [ ] **P0** `ruff check forward/deep_dive.py --select=F821` 통과  
- [ ] **P0** `python -c "from forward.deep_dive import send_comprehensive_daily_report"` 통과  
- [ ] **P1** `pyproject.toml` + `requirements-dev.txt` + pre-commit 도입  
- [ ] **P1** `tests/test_forward_imports.py` 추가  
- [ ] **P2** `forward/reporter_internal.py` 분리 검토 (shared 비대화 해소)

---

## 5. 부록 — 스캔에 사용한 파일 목록

| 파일 | 상태 |
|------|------|
| `forward/deep_dive.py` | ❌ P0 NameError 다수 예상 |
| `forward/shared.py` | ✅ 정의 원본 |
| `forward/ledger.py` | ✅ |
| `forward/rotation_report_section.py` | ✅ |
| `forward/deathmatch_report_section.py` | ✅ |
| `forward/dna_autopsy.py` | ✅ |
| `practitioner_report_context.py` | ✅ (호출자 버그) |
| `rotation_sector_filter.py` | ✅ `is_rotation_eligible_sector` 정의 |
| `forward_report_scalar.py` | ✅ `safe_float_cast` 정의 |
| `market_db_paths.py` | ✅ ( `_open_market_db_ro` 없음 ) |
| `mutant_oos_validator.py` | ⚠️ 동명 함수 별도 정의 |
| `auto_forward_tester.py` | 간접 — deep_dive 수정으로 해결 |

---

*본 문서는 코드베이스 정적 grep·AST 수준 교차검증 및 Python `import *` 규칙 분석을 기반으로 작성되었습니다. P0 패치 적용 후 `manual_report_trigger.py` 또는 `factory_pipelines` 드라이런으로 텔레그램 송출 E2E 재검증을 권장합니다.*
