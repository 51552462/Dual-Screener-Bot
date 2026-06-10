# Bitget 버그 픽스 변경 로그 #01

> 작성일: 2026-06-10  
> 기준 문서: `bitget/bug_analysis_report_01.md`  
> 대상: 딥 다이브 SQL 바인딩 / Practitioner 어댑터 / Watchdog 레거시 정리

---

## 개요

분석 보고서에서 식별한 3가지 치명적 에러와 레거시 혼선 요소를 코드에 반영했다.

| # | 에러 | 상태 |
|---|------|------|
| 1 | 포워드 딥 다이브 SQL 바인딩 누락 | ✅ 수정 |
| 2 | `practitioner_bitget_adapter` 모듈 누락 | ✅ 수정 |
| 3 | Watchdog `bitget.main` 유령 heartbeat | ✅ 레거시 제거 |

---

## 1. 포워드 딥 다이브 SQL 바인딩

### 파일: `bitget/forward/reports.py`

#### 변경 1 — `_calculate_metrics` import 추가 (16행 부근)

**Before:**

```python
from bitget.forward.mutant import _auto_tune_brain_from_closed_df, _coin_asset_group, _pf
```

**After:**

```python
from bitget.forward.mutant import (
    _auto_tune_brain_from_closed_df,
    _calculate_metrics,
    _coin_asset_group,
    _pf,
)
```

**이유:** `run_deep_dive_analysis()` 207행에서 `_calculate_metrics()`를 호출하지만, 파일 내 정의·import가 없어 표본 ≥ 10건 환경에서 `NameError` 발생 예상. 구현은 `bitget/forward/mutant.py:22`에 존재.

---

#### 변경 2 — `pd.read_sql()` params 추가 (179–183행)

**Before:**

```python
df = pd.read_sql(
    "SELECT * FROM bitget_forward_trades WHERE market_type=? AND status LIKE 'CLOSED%'",
    conn,
)
```

**After:**

```python
df = pd.read_sql(
    "SELECT * FROM bitget_forward_trades WHERE market_type=? AND status LIKE 'CLOSED%'",
    conn,
    params=(market_type,),
)
```

**이유:** SQL placeholder `?` 1개에 바인딩 0개 전달 → `Incorrect number of bindings supplied` 에러. 동일 파일 60–63행의 정상 패턴과 일치시킴.

---

## 2. Practitioner 어댑터 모듈 경로 복원

### 신규 파일: `bitget/forward/practitioner_bitget_adapter.py`

- **출처:** `legacy_archive/practitioner_bitget_adapter.py`
- **내용:** `send_bitget_practitioner_reports_pil()` — Bitget spot/futures × PRACT_01~30 PIL 리포트 + 메타 페널티 배치
- **의존:** `practitioner_intelligence`, `practitioner_penalty_bridge` (repo root, PYTHONPATH)

`legacy_archive/`는 PYTHONPATH 밖이라 import 불가. 운영 패키지 `bitget.forward` 내부로 복원.

---

### 파일: `bitget/forward/reports.py`

#### 변경 — import 경로 수정 (29행)

**Before:**

```python
from practitioner_bitget_adapter import send_bitget_practitioner_reports_pil
```

**After:**

```python
from bitget.forward.practitioner_bitget_adapter import send_bitget_practitioner_reports_pil
```

**호출 체인:**

```
daily_audit pipeline
  └─ send_comprehensive_daily_report()
       └─ send_group_practitioner_reports()  [166행]
            └─ send_bitget_practitioner_reports_pil(...)  [41행]
```

---

## 3. Watchdog 재발 방지 — 레거시 heartbeat 제거

운영 `.env`는 사용자가 `BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot`으로 이미 수정함.  
추가로 DB에 `bitget.main` heartbeat를 **새로 기록하는 레거시 경로**를 제거.

---

### 파일: `bitget/main.py`

#### 변경 — startup heartbeat 삭제 (161–166행)

**Before:**

```python
try:
    from bitget.infra import ops_logger

    ops_logger.record_heartbeat("bitget.main", extra={"event": "startup"})
except Exception:
    pass
```

**After:** 해당 블록 전체 삭제.

**이유:** deprecated `python -m bitget.main` 실행 시 `bitget.main` component heartbeat 1회 기록 → watchdog multi-component/fallback 설정 시 stale row 잔존 가능.

---

### 파일: `bitget/sentinel.py`

#### 변경 1 — `_spawn("main")` 분기 삭제 (33–37행)

**Before:**

```python
if name == "main":
    return subprocess.Popen(
        [sys.executable, "-m", "bitget.main"],
        cwd=PROJECT_ROOT,
    )
```

**After:** 분기 삭제. `_spawn()`은 `dashboard`, `heatmap`만 처리.

---

#### 변경 2 — 감시 프로세스 목록에서 `main` 제거 (70행)

**Before:**

```python
names = ["main", "dashboard", "heatmap"]
```

**After:**

```python
# Production daemon: dante-bitget-factory → bitget_auto_pilot (not bitget.main)
names = ["dashboard", "heatmap"]
```

**이유:** `factory_launcher` → `sentinel` dev 경로에서 `bitget.main` 자동 재시작 제거. prod 24/7 daemon SSOT는 `dante-bitget-factory` → `bitget.pipelines.bitget_auto_pilot --daemon`.

---

## 변경 파일 목록

| 파일 | 변경 유형 |
|------|-----------|
| `bitget/forward/reports.py` | 수정 (import 2건, SQL params 1건) |
| `bitget/forward/practitioner_bitget_adapter.py` | **신규** (legacy_archive에서 복원) |
| `bitget/main.py` | 수정 (heartbeat 블록 삭제) |
| `bitget/sentinel.py` | 수정 (main spawn 제거) |

---

## 파이프라인 정상화 (아키텍처)

```
dante-bitget-factory
  └─ bitget_auto_pilot --daemon
       └─ heartbeat.tick (component=bitget_auto_pilot, 60s)

dante-bitget-watchdog (timer)
  └─ BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot
       └─ bitget.main 유령 row 신규 생성 경로 차단

daily_audit (cron)
  ├─ run_deep_dive_analysis("spot"|"futures")   ← SQL params 수정
  └─ send_comprehensive_daily_report()
       └─ send_group_practitioner_reports()     ← adapter 경로 수정
```

---

## 배포 후 검증

```bash
# 1. 딥 다이브 + practitioner import smoke test
python -c "
from bitget.forward.reports import run_deep_dive_analysis, send_group_practitioner_reports
from bitget.forward.practitioner_bitget_adapter import send_bitget_practitioner_reports_pil
print('imports OK')
"

# 2. daily_audit 파이프라인 (텔레그램 생략)
bitget/deploy/bitget.sh --daily-audit --skip-telegram

# 3. heartbeat DB 확인
sqlite3 /path/to/bitget_ops_events.sqlite \
  "SELECT ts_utc, component FROM ops_events WHERE event='heartbeat.tick' ORDER BY id DESC LIMIT 10;"
# → bitget_auto_pilot 최신, bitget.main 신규 row 없음
```

---

## 미수정 / 후속 (선택)

| 항목 | 비고 |
|------|------|
| `legacy_archive/practitioner_bitget_adapter.py` | 원본 유지. 운영 SSOT는 `bitget/forward/` |
| `deploy/bitget.sh:50` help 텍스트 | `--daemon` 설명이 아직 `factory_launcher` 참조 (문서 혼선, 기능 무관) |
| `tests/test_forward_imports.py` | practitioner adapter import smoke test 추가 권장 |
