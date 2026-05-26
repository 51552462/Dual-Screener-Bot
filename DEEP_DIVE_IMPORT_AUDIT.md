# DEEP_DIVE_IMPORT_AUDIT — `deep_dive_kr` NameError 재발 감사

**감사 일시:** 2026-05-27  
**증상:** `factory.sh --daily-kr` → `deep_dive_kr` Critical Fail  
**에러:**
1. `run_deep_dive_analysis` — `NameError: name '_open_market_db_ro' is not defined` (서버 traceback: **line 650**)
2. 예외 처리 경로 — `NameError: name '_format_forward_ledger_error_html' is not defined`

---

## Executive Summary

| 항목 | 결론 |
|------|------|
| **로컬 코드 (커밋 e7d3549 이후)** | `forward/deep_dive.py`에 named import **이미 존재**했음. 현재 트리에서는 F821 재현 불가 |
| **서버 line 650** | 수정 **이전** 트리와 일치 (`run_deep_dive_analysis` 본문 `conn = _open_market_db_ro()` 위치). **+18줄** import 블록이 서버에 없음 → **배포/풀 누락 또는 구버전 `.pyc` 실행** 가능성 최우선 |
| **이번 P0 강화** | `import forward.shared as _forward_shared` + `getattr` 바인딩 + **모듈 import 시점** `_verify_deep_dive_private_bindings()` |
| **Ruff 미차단 이유** | pre-commit **미설치**, CI **부재**, 커밋 시 `ruff` **자동 실행 없음** — 수동 1회만 실행 |
| **재발 방지** | `.pre-commit-config.yaml` + `scripts/install_pre_commit_hooks.*` — **커밋 시 F821 강제** |

---

## 1. 잔여 NameError 완벽 픽스 (P0)

### 1.1 `forward/deep_dive.py` 전수 스캔 (shared private 호출)

| 심볼 | 용도 | 호출 예 |
|------|------|---------|
| `_open_market_db_ro` | RO DB 연결 | `send_comprehensive_daily_report`, `run_deep_dive_analysis`, V28/V29 |
| `_normalize_trade_market` | KR/US 정규화 | `send_group_practitioner_reports` |
| `_reporter_cleanup_zombie_forward_trades` | 좀비 OPEN 정리 | 일일·PIL 리포트 |
| `_reporter_valid_holding_mask` | 유효 보유 마스크 | 9분할·PIL |
| `_reporter_deploy_fleet_mask` | VIP 편대 마스크 | [4/9] |
| `_daily_report_trades_for_market` | `load_market_slice` 정규화 | 9분할 |
| `_strategy_colosseum_brief` | 콜로세움 tail | 일일 리포트 |
| `_shadow_performance_brief` | 그림자 장부 tail | 일일 리포트 |
| `_tier80_sync_effective_and_report_line` | [5/9] tier80 | 9분할 |
| `_parse_mkt_group_key` | PIL 그룹 키 | PIL |
| `_exit_date_on_calendar` | 청산일 캘린더 | PIL |
| `_format_exit_reason_display` | PIL 청산 사유 | PIL brief |
| `_safe_final_ret_pct` | PIL 수익률 | PIL brief |
| `_win_loss_flat_counts` | PIL 승패무 | PIL brief |
| `_spillover_fallback_enabled` | V28 fallback | `run_deep_dive_analysis` KR |
| `_format_forward_ledger_error_html` | 딥다이브 except HTML | `run_deep_dive_analysis` L1011 부근 |

**추가 누락:** 없음 (위 16개가 `forward/shared.py` private reporter·DB 전부).

로컬 정의(스캔 제외): `_assemble_satellite_tail`, `_deep_dive_cross_market_isolation_footer`, 예외 변수 `_ez`, `_mg_e` 등.

### 1.2 적용한 P0 패치 (현재 `forward/deep_dive.py` 상단)

**문제:** `from forward.shared import (...)` 만으로는 이론상 충분하나, 서버가 구버전이면 그대로 NameError.  
**해결:** 모듈 속성 직접 참조로 **star-import 규칙과 무관하게** 바인딩.

```python
from forward.shared import *  # noqa: F403
import forward.shared as _forward_shared

_DEEP_DIVE_PRIVATE_NAMES = (
    "_open_market_db_ro",
    "_normalize_trade_market",
    # ... (16개 전체, 소스와 동기화)
)
for _priv in _DEEP_DIVE_PRIVATE_NAMES:
    globals()[_priv] = getattr(_forward_shared, _priv)

def _verify_deep_dive_private_bindings() -> None:
    missing = [n for n in _DEEP_DIVE_PRIVATE_NAMES if not callable(globals().get(n))]
    if missing:
        raise ImportError(f"forward.deep_dive: private reporter bindings missing: {missing}")

_verify_deep_dive_private_bindings()
```

**효과:**
- `import forward.deep_dive` 만으로 즉시 실패 (크론 전에 발견).
- `run_deep_dive_analysis`의 `__globals__`에 16개 심볼 **항상** 존재.
- 서버에 **이 파일만** 최신이면 line 650 NameError 소거.

### 1.3 서버 배포 체크리스트 (필수)

```bash
cd /path/to/Dual-Screener-Bot
git fetch origin && git log -1 --oneline   # e7d3549 이후 커밋 포함 확인
git pull origin main
python3 -c "import forward.deep_dive as d; d._verify_deep_dive_private_bindings(); print('OK')"
# 구 .pyc 제거 (선택)
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null
factory.sh --daily-kr
```

**line 번호 대조:**

| 버전 | `run_deep_dive_analysis` def | `conn = _open_market_db_ro()` |
|------|-------------------------------|-------------------------------|
| 구버전 (서버 traceback) | ~632 | **~650** |
| e7d3549 (named import) | ~650 | ~668 |
| **본 패치 후** | ~662 | ~680 |

서버가 **650에서 conn** 이면 아직 구버전 실행 중.

### 1.4 호출 경로

```
factory.sh --daily-kr
  → factory_pipelines._step_deep_dive_kr()
  → auto_forward_tester.run_deep_dive_analysis("KR")
  → forward.deep_dive.run_deep_dive_analysis  (함수 __globals__ = deep_dive 모듈)
```

`bitget/forward_tester.py`의 동명 함수는 **equity cron과 무관**.

---

## 2. Linter(Ruff) 직무 유기 원인 분석 (P0)

### 2.1 왜 F821이 커밋 전에 차단되지 않았나

| # | 원인 | 설명 |
|---|------|------|
| 1 | **pre-commit 미설치** | `pyproject.toml`·`requirements-dev.txt`만 추가. **`.pre-commit-config.yaml` 없음**, `pre-commit install` **미실행** |
| 2 | **CI 파이프라인 부재** | GitHub Actions 등에서 `ruff check` **없음** |
| 3 | **수동 ruff 1회만** | 구현 당시 `ruff check forward/deep_dive.py --select=F821` **통과 후 푸시** — 이후 서버는 **구버전**이면 린터와 무관하게 실패 |
| 4 | **F821의 한계 (부가)** | `import *`만 있고 private를 **호출만** 할 때, 일부 도구/버전은 F405만 내고 F821을 내지 않을 수 있음. **named import 추가 후**에는 F821이 잡히지 않는 상태가 정상 |

### 2.2 자백 (프로세스)

- 커밋 훅 없이 `git push origin main` 완료.
- **“Ruff가 프로덕션을 막는다”**고 보고했으나, 실제로는 **로컬 수동 1회 + 문서상의 pyproject**에 그쳤고 **강제 게이트는 없었음**.
- 서버 NameError는 **린터 실패가 아니라 배포/버전 불일치** 가능성이 크나, **훅 부재**로 동일 클래스 버그가 다시 커밋될 수 있는 구조는 맞음.

### 2.3 수정 후 기대 동작

- `pre-commit install` 후: `forward/deep_dive.py`에 F821 있으면 **`git commit` 거부**.
- `always_run: true` 훅으로 staged 파일과 무관하게 **매 커밋** `forward/deep_dive.py` 검사.

---

## 3. 재발 방지 — 강제 검증 파이프라인 (역제안 및 설치)

### 3.1 추가 파일

| 파일 | 역할 |
|------|------|
| `.pre-commit-config.yaml` | 매 커밋 `ruff check forward/deep_dive.py --select=F821,F822,F823` + `tests/test_forward_imports.py` |
| `scripts/install_pre_commit_hooks.sh` | Linux/macOS 서버·개발기 설치 |
| `scripts/install_pre_commit_hooks.ps1` | Windows 설치 |

### 3.2 설치 (로컬·서버 개발 계정)

**Windows:**
```powershell
.\scripts\install_pre_commit_hooks.ps1
```

**Linux (서버):**
```bash
chmod +x scripts/install_pre_commit_hooks.sh
./scripts/install_pre_commit_hooks.sh
```

**수동:**
```bash
pip install -r requirements-dev.txt
pre-commit install
pre-commit run --all-files
```

### 3.3 커밋 전 필수 명령 (문서화)

```bash
ruff check forward/deep_dive.py --select=F821,F822,F823
python -m pytest tests/test_forward_imports.py -q
python -c "import forward.deep_dive as d; d._verify_deep_dive_private_bindings()"
```

### 3.4 권장 후속 (P2)

- GitHub Actions: `ruff check forward/` on push to `main`
- `factory.sh` 시작 시: `python -c "import forward.deep_dive; ..."` **1줄 프리플라이트**
- 장기: `forward/reporter_internal.py`로 private 분리 후 `import *` 제거

---

## 4. 검증 기록

```text
ruff check forward/deep_dive.py --select=F821,F822,F823  → All checks passed
pytest tests/test_forward_imports.py                   → 18 passed
import forward.deep_dive + _verify_deep_dive_private_bindings() → OK
```

---

*본 문서는 서버 traceback line 번호·로컬 소스 diff·Ruff/pre-commit 구성을 대조해 작성되었습니다.*
