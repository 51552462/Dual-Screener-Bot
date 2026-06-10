# Bitget 버그 분석 보고서 #01

> 작성일: 2026-06-10  
> 대상 에러 3건 — Watchdog 유령 heartbeat / 포워드 딥 다이브 SQL 바인딩 / Practitioner 어댑터 누락

---

## 1. Watchdog 유령 심장박동 에러

### 에러 로그

```
[BITGET WATCHDOG] heartbeat stale 3 times (threshold 3) component=bitget.main
```

### 발생 위치

| 파일 | 줄 | 역할 |
|------|-----|------|
| `bitget/watchdog.py` | 226–233 | stale 판정 후 Telegram/재시작 알림 메시지 생성 (`component={matched_comp or label}`) |
| `bitget/watchdog.py` | 57–64 | `_resolve_watchdog_components()` — env에서 감시 대상 component 목록 결정 |
| `bitget/watchdog.py` | 96–117 | `_latest_heartbeat_ts()` — 감시 목록 중 **가장 최신** heartbeat의 component 반환 |
| `bitget/pipelines/bitget_auto_pilot.py` | 20, 29, 166–168 | 데몬 heartbeat 기록 (`HEARTBEAT_COMPONENT = "bitget_auto_pilot"`) |
| `bitget/main.py` | 164 | **레거시** startup 시 `record_heartbeat("bitget.main")` 1회 기록 |

### 추적 결과 — heartbeat 이름을 잘못 보내는가?

**아니오.** `bitget_auto_pilot.py`는 올바른 이름을 사용한다.

```python
# bitget/pipelines/bitget_auto_pilot.py:20
HEARTBEAT_COMPONENT = "bitget_auto_pilot"

# bitget/pipelines/bitget_auto_pilot.py:29
ops_logger.record_heartbeat(HEARTBEAT_COMPONENT)

# bitget/pipelines/bitget_auto_pilot.py:166-168 (daemon 시작 시)
ops_logger.record_heartbeat(
    HEARTBEAT_COMPONENT,
    extra={"event": "daemon_start", "orchestrator": "pipeline"},
)
```

60초 간격 `_heartbeat_loop` 스레드도 동일한 `HEARTBEAT_COMPONENT`를 사용한다. **송신 측 코드는 정상.**

### 추적 결과 — watchdog가 하드코딩/ env 미반영인가?

**하드코딩 아님.** watchdog 기본값은 이미 `bitget_auto_pilot`이다.

```python
# bitget/watchdog.py:29
DEFAULT_HEARTBEAT_COMPONENT = "bitget_auto_pilot"

# bitget/watchdog.py:62-64
raw = (os.environ.get("BITGET_WATCHDOG_HEARTBEAT_COMPONENT") or DEFAULT_HEARTBEAT_COMPONENT).strip()
parts = tuple(dict.fromkeys(p.strip() for p in raw.split(",") if p.strip()))
return parts if parts else (DEFAULT_HEARTBEAT_COMPONENT,)
```

env 로딩 경로도 정상이다.

- `deploy/systemd/dante-bitget-watchdog.service.in:10-11` — `EnvironmentFile=-@@INSTALL_ROOT@@/.env`, `EnvironmentFile=-@@INSTALL_ROOT@@/bitget/.env`
- `deploy/bitget.sh:15-27` — 동일 `.env` 파일을 `source` 후 `python -m bitget.pipelines.runner --mode watchdog` 실행

레포 내 예시 설정도 `bitget_auto_pilot`으로 통일되어 있다.

- `deploy/bitget_resource_limits.env.example:10` → `BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot`
- `docs/implementation_phase_0_1_2.md:113` → 동일

**코드/레포 기본값에는 `bitget.main`이 없다.**

### 정확한 원인

알림 메시지의 `component=bitget.main`은 `matched_comp` 변수에서 온다 (`watchdog.py:228`).  
이 값은 **DB에서 실제로 조회된 heartbeat의 component 이름**이다.

즉, watchdog는 현재 **감시 목록에 `bitget.main`이 포함**되어 있고, DB에 **`bitget.main` component의 (오래된) heartbeat.tick**이 존재하며, 그 timestamp가 stale 임계(기본 600초)를 초과했다.

가능한 시나리오 (우선순위 순):

#### 시나리오 A — 배포 `.env`가 구버전 값 유지 (가장 유력)

운영 서버의 `.env` 또는 `bitget/.env`에 아직 다음과 같은 **구버전 설정**이 남아 있을 가능성:

```bash
BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget.main
```

cutover 이전에는 `main.py`가 heartbeat 주체였고, env가 갱신되지 않으면 watchdog는 계속 `bitget.main`만 감시한다.  
`bitget_auto_pilot` 데몬은 `bitget_auto_pilot` 이름으로 heartbeat를 쏘지만, **watchdog는 그 이름을 보지 않는다.**

#### 시나리오 B — 쉼표 구분 fallback 목록에 `bitget.main` 포함

```bash
BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot,bitget.main
```

`dante-bitget-factory`가 죽어 `bitget_auto_pilot` heartbeat가 DB에 없으면, 과거 `main.py` startup 시 남긴 `bitget.main` heartbeat만 남는다.  
multi-component 모드에서는 **존재하는 heartbeat 중 최신**을 선택하므로, 유일한 후보인 stale `bitget.main`이 `matched_comp`로 표시된다.

#### 시나리오 C — 레거시 `main.py` / `sentinel.py` 잔존 실행

- `bitget/main.py:164` — deprecated이지만 실행 시 `bitget.main` heartbeat 1회 기록
- `bitget/sentinel.py:35` — 여전히 `python -m bitget.main` spawn 코드 존재 (prod systemd 경로와 별개)

이것만으로는 60초 주기 stale을 설명하기 어렵지만, DB에 `bitget.main` row를 남기는 **부가 요인**이 될 수 있다.

#### 시나리오 D — ops DB 경로 불일치 (보조 점검)

writer(`ops_logger.OPS_EVENTS_DB_PATH`)와 reader(`watchdog._ops_db_path()`) 모두 `bitget/infra/data_paths.py`의 `ops_events_db_path()`를 기본으로 사용하며, env override 키도 대칭적이다.  
**코드상 경로 불일치 가능성은 낮으나**, 운영 env에서 `BITGET_OPS_EVENTS_DB` / `DANTE_BITGET_OPS_EVENTS_DB`가 writer/reader에 다르게 적용되면 auto_pilot heartbeat를 못 볼 수 있다.

### 수정 계획

#### 즉시 (운영 env / systemd)

1. **운영 `.env` 확인 및 수정**

   ```bash
   grep BITGET_WATCHDOG_HEARTBEAT_COMPONENT /path/to/.env /path/to/bitget/.env
   ```

   다음으로 **단일 값** 설정 (fallback에 `bitget.main` 제거):

   ```bash
   BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot
   ```

2. **watchdog timer 재로드**

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl restart dante-bitget-watchdog.timer
   ```

3. **DB에서 heartbeat 실태 확인**

   ```sql
   SELECT ts_utc, component, event
   FROM ops_events
   WHERE event = 'heartbeat.tick'
   ORDER BY id DESC
   LIMIT 20;
   ```

   - `bitget_auto_pilot` row가 60초 이내로 갱신되는지 확인
   - `bitget.main`만 있고 최신 timestamp가 수 시간/일 전이면 시나리오 A 또는 B 확정

4. **factory 데몬 가동 확인**

   ```bash
   systemctl status dante-bitget-factory
   journalctl -u dante-bitget-factory -n 50
   ```

   `run_bitget_daemon.sh:35` → `python -m bitget.pipelines.bitget_auto_pilot --daemon` 이 실제로 실행 중이어야 한다.

#### 코드 개선 (선택, 재발 방지)

| 파일 | 변경 |
|------|------|
| `bitget/watchdog.py` | startup 시 `components`에 deprecated 이름(`bitget.main`)이 있으면 stderr 경고 출력 |
| `bitget/main.py:164` | heartbeat 기록 제거 또는 `bitget_auto_pilot`으로 통일 (deprecated 경로 혼선 방지) |
| `bitget/sentinel.py:35` | `bitget.main` spawn 제거 또는 deprecated 명시 |
| `deploy/bitget.sh:50` | help 텍스트 `--daemon` 설명을 `factory_launcher` → `bitget_auto_pilot`으로 수정 (문서 혼선 제거) |

---

## 2. 포워드 장부 딥 다이브 SQL 바인딩 에러

### 에러 로그

```
Execution failed on sql 'SELECT * FROM bitget_forward_trades WHERE market_type=? AND status LIKE 'CLOSED%'':
Incorrect number of bindings supplied. The current statement uses 1, and there are 0 supplied.
```

### 발생 위치

| 파일 | 줄 | 내용 |
|------|-----|------|
| **`bitget/forward/reports.py`** | **179–182** | `run_deep_dive_analysis()` — **`params` 인자 누락 (버그)** |
| `bitget/forward/reports.py` | 60–63 | 동일 파일 내 **정상 예시** — `params=(market_type,)` 전달 |
| `bitget/pipelines/bitget_pipelines.py` | 110–118 | daily_audit 파이프라인에서 `run_deep_dive_analysis("spot")`, `("futures")` 호출 |
| `bitget/auto_pilot.py` | 655–656 | 자율 분석 경로에서도 동일 함수 호출 |

### 정확한 원인

`run_deep_dive_analysis()` 함수 시그니처는 `market_type` 파라미터를 받지만, SQL 실행 시 바인딩을 넘기지 않는다.

**문제 코드 (`bitget/forward/reports.py:179-182`):**

```python
df = pd.read_sql(
    "SELECT * FROM bitget_forward_trades WHERE market_type=? AND status LIKE 'CLOSED%'",
    conn,
)  # ← params=(market_type,) 누락
```

SQL에 `market_type=?` placeholder가 **1개** 있는데, pandas/sqlite3에 전달된 바인딩은 **0개**이므로 에러가 발생한다.

**같은 파일의 정상 패턴 (`bitget/forward/reports.py:60-63`):**

```python
df_all = pd.read_sql(
    "SELECT * FROM bitget_forward_trades WHERE market_type=?",
    conn,
    params=(market_type,),  # ← 올바름
)
```

**다른 모듈의 동일 패턴 (참고 — 정상):**

- `bitget/system_auto_pilot.py:340-341` — `params=(market_type, cutoff)`
- `bitget/auto_pilot.py:707-708` — `params=(market_type, cutoff)`

### 수정 계획

#### 필수 수정 (1줄)

**파일:** `bitget/forward/reports.py`  
**줄:** 179–182

```python
df = pd.read_sql(
    "SELECT * FROM bitget_forward_trades WHERE market_type=? AND status LIKE 'CLOSED%'",
    conn,
    params=(market_type,),
)
```

#### 검증

```bash
cd /path/to/Dual-Screener-Bot
python -c "
from bitget.forward.reports import run_deep_dive_analysis
run_deep_dive_analysis('spot')
run_deep_dive_analysis('futures')
print('OK')
"
```

또는 daily_audit 파이프라인 dry-run:

```bash
bitget/deploy/bitget.sh --daily-audit --skip-telegram
```

#### 부가 발견 (본 에러와 무관, 후속 점검 권장)

`run_deep_dive_analysis()` 내부(207행)에서 `_calculate_metrics()`를 호출하지만, **`reports.py`에 해당 함수 정의가 없다.**  
SQL 바인딩 수정 후 표본 ≥ 10건 환경에서는 `NameError: _calculate_metrics`가 발생할 수 있다.  
`_calculate_metrics`를 `bitget/forward/mutant.py` 또는 다른 모듈에서 import하거나, 파일 내 helper로 추가해야 한다.

---

## 3. Practitioner 어댑터 모듈 누락 에러

### 에러 로그

```
practitioner report error: No module named 'practitioner_bitget_adapter'
```

### 발생 위치

| 파일 | 줄 | 내용 |
|------|-----|------|
| **`bitget/forward/reports.py`** | **29** | `from practitioner_bitget_adapter import send_bitget_practitioner_reports_pil` |
| `bitget/forward/reports.py` | 27–50 | `send_group_practitioner_reports()` — PIL 실무자 리포트 진입점 |
| `bitget/forward/reports.py` | 164–168 | `send_comprehensive_daily_report()` 종료 시 practitioner 리포트 연동 호출 |
| **`legacy_archive/practitioner_bitget_adapter.py`** | **전체** | **실제 구현 파일 위치** (PYTHONPATH 밖) |

### 호출 체인

```
daily_audit pipeline
  └─ bitget/pipelines/bitget_pipelines.py (daily report step)
       └─ bitget/forward/reports.py :: send_comprehensive_daily_report()
            └─ send_group_practitioner_reports()  [line 166]
                 └─ from practitioner_bitget_adapter import ...  [line 29]  ← ImportError
                      └─ send_bitget_practitioner_reports_pil(...)  [line 41]
```

`bitget.sh` / cron의 `--daily-audit` 실행 시, 일일 종합 리포트(6/6) 발송 후 practitioner 30인 개별 리포트를 연동 실행하는 구조다.

### 정확한 원인

1. **import 경로:** `practitioner_bitget_adapter`는 **repo root** (`Dual-Screener-Bot/`)에 있어야 import된다.
   - `deploy/bitget.sh:13` — `PYTHONPATH="${ROOT}..."` (repo root가 sys.path에 포함)
   - `practitioner_intelligence.py`, `practitioner_penalty_bridge.py` 등 의존 모듈은 repo root에 존재

2. **파일 위치:** `practitioner_bitget_adapter.py`는 **`legacy_archive/`로 이동**되어 있다.
   - `legacy_archive/practitioner_bitget_adapter.py` — 190행, `send_bitget_practitioner_reports_pil()` 구현 포함
   - repo root 및 `bitget/` 하위에는 **동명 파일 없음**

3. **import 미갱신:** `bitget/forward/reports.py:29`의 import는 cutover/아카이브 이후 **갱신되지 않았다.**
   - 레포 전체 grep 결과, `practitioner_bitget_adapter` 참조는 **`reports.py` 1곳뿐`**
   - 테스트 파일(`tests/test_practitioner_*.py`)도 이 adapter를 직접 import하지 않음 → CI에서 누락이 탐지되지 않음

### 수정 계획

#### 권장: adapter를 repo root로 복원

Bitget PIL 리포트는 아직 active 기능이므로, archive가 아닌 **운영 경로**에 두는 것이 맞다.

```bash
# repo root에서
git mv legacy_archive/practitioner_bitget_adapter.py ./practitioner_bitget_adapter.py
```

또는 파일 복사 후 archive 쪽은 deprecated 주석만 남긴다.

**의존성 확인** — adapter가 import하는 모듈은 모두 repo root에 존재:

- `practitioner_intelligence`
- `practitioner_penalty_bridge`

#### 대안: bitget 패키지 내부로 이동 (구조 정리 시)

```python
# bitget/forward/practitioner_bitget_adapter.py 생성 후
# bitget/forward/reports.py:29
from bitget.forward.practitioner_bitget_adapter import send_bitget_practitioner_reports_pil
```

패키지 상대 import로 경로를 명시하면 PYTHONPATH 의존을 줄일 수 있다.

#### 검증

```bash
python -c "
from bitget.forward.reports import send_group_practitioner_reports
send_group_practitioner_reports()
print('OK')
"
```

#### 재발 방지

- `tests/test_forward_imports.py` 또는 신규 test에 `send_group_practitioner_reports` import smoke test 추가
- daily_audit 파이프라인 integration test에서 practitioner step mock/실행 확인

---

## 요약表

| # | 발생 위치 | 원인 | 수정 |
|---|-----------|------|------|
| 1 | `watchdog.py:228` (알림), env/DB | **코드 버그 아님.** 운영 env가 `bitget.main`을 감시하거나, auto_pilot heartbeat 부재로 stale `bitget.main`만 남음 | `.env` → `BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot`, factory 가동·DB heartbeat 확인 |
| 2 | **`forward/reports.py:179-182`** | `pd.read_sql()`에 `params=(market_type,)` **누락** | `params=(market_type,)` 추가 |
| 3 | **`forward/reports.py:29`** | `practitioner_bitget_adapter.py`가 **`legacy_archive/`로 이동**, import 미갱신 | repo root 또는 `bitget.forward` 패키지로 모듈 복원 + import 정리 |

---

## 우선순위별 작업 순서

1. **[P0 — 1줄]** `forward/reports.py:182` SQL `params` 추가 → 딥 다이브 즉시 복구
2. **[P0 — env]** 운영 `.env`의 `BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot` 확인 → watchdog false restart 중단
3. **[P1 — 파일]** `practitioner_bitget_adapter.py` repo root 복원 → daily report practitioner step 복구
4. **[P2 — 후속]** `_calculate_metrics` 미정의, `main.py` legacy heartbeat, `sentinel.py` spawn 정리
