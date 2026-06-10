# [GOVERNOR SYNC FATAL AUDIT] 메타 거버너 ↔ system_config 동기화 단절 보고서

> 작성일: 2026-06-10  
> 방법: 소스코드 정밀 추적 (코드 수정 없음)  
> 증상 기준: `meta_regime_key=BULL`, `config_regime_key=UNKNOWN`, `effective_kelly_risk=1.00%`, `META_GOVERNOR_LAST_RUN_AT=2026-06-08` (리포트일 2026-06-10)

---

## Executive Summary

관측된 모순은 **단일 버그가 아니라 3개 저장소·2개 읽기 경로·1개 신선도 검사 허점**이 겹친 **동기화 아키텍처 붕괴**다.

| 계층 | SSOT (뇌) | 실행·리포트 (손발) |
|------|-----------|-------------------|
| Meta 국면 | `market_data.sqlite` → `meta_state_log` / `config_kv.META_GOVERNOR_STATE` / `meta_governor_state.json` | — |
| Config 국면·Kelly | `system_config.sqlite` `config_kv` (공식) | `system_config.json` (레거시 미러·**여전히 다수 모듈이 직접 읽음**) |
| 감사관 표시 | `load_meta_state_resolved()` → **Meta DB/JSON 체인** | `ai_overseer.load_config()` → **JSON 파일 직결** |

**핵심 결론**

1. **Governor 타임스탬프 6/8 고착**: `is_meta_state_degraded()` / `_meta_needs_rebuild()`는 `META_GOVERNOR_LAST_RUN_AT` **내용의 나이**를 검사하지 않는다. `BULL` + `status=OK` + 타임스탬프 **필드 존재**만으로 “건강” 판정 → **2일 전 스냅샷이 영구 신선으로 간주**될 수 있다.
2. **BULL vs UNKNOWN 분열**: Meta는 unified loader(`market_data.sqlite` 우선)에서 `BULL`, Config는 `REGIME_ANALYSIS.regime_key` / `CURRENT_REGIME_KEY`가 `UNKNOWN`인 채 유지. `sync_config_regime_from_meta()` 실패 시 **warning·`except: pass`로 상위 파이프라인은 계속 진행**.
3. **Kelly 1.00%**: `DYNAMIC_KELLY_RISK` 기본값 **`0.01`** (다수 모듈 하드코딩). UNKNOWN config + Meta 연동 실패·scanner hydrate skip 시 **거래 경로에 1%가 그대로 잔류**.

---

## Target 1: MetaGovernor Sync 파이프라인 마비 원인

### 1.1 진입점 — `meta_governor_sync.py`는 존재하지 않음

별도 파일 없음. SSOT는 **`factory_pipelines.py`** 의 `_step_meta_governor_sync()` (21–57행).

```python
# factory_pipelines.py:21-32
def _step_meta_governor_sync() -> None:
    from meta_state_store import rebuild_meta_state, ensure_config_regime_aligned, ...
    out = rebuild_meta_state(force=False, refresh_regime=True)
    align = ensure_config_regime_aligned()
    ...
```

호출 위치:

- `factory_pipelines.py:417-424` — `_with_daily_audit_prelude()` **맨 앞** (`daily_audit_kr` / `daily_audit_us` / `daily_audit` 공통)
- `factory_pipelines.py:483-484` — 스캔 파이프라인 `meta_governor_sync_scan`

**Bitget 격리 파이프라인에는 이 step이 없음**

- `bitget/pipelines/bitget_pipelines.py:259-271` — `daily_audit` = sentiment → track → deep_dive → report → **bitget `ai_overseer`** → reconcile
- **`meta_governor_sync` / `factory_artifact_guard` / `rebuild_meta_state` 미포함**

→ Bitget-only cron이면 주식 팩토리 Meta sync가 **구조적으로 생략**될 수 있음 (주식+Bitget 통합 배포면 주식 `daily_audit` cron에 의존).

### 1.2 `rebuild_meta_state()` 내부 체인 (`meta_state_store.py:438-532`)

| 단계 | 함수 | 역할 | 실패 시 |
|------|------|------|---------|
| 1 | `regime_meta_analyzer.analyze_market_regime()` | yfinance 지수 → `REGIME_ANALYSIS` | `result["regime"]="failed"` (로그+알림, **예외 없음**) |
| 2 | `factory_artifact_guard.ensure_meta_governor_state()` | Governor cycle | `result["meta"]="failed"` |
| 3 | `ensure_config_regime_aligned(meta_after, force=True)` | Meta→`config_kv` 국면 | `config_regime_sync.error` dict만 기록 |
| 4 | `invalidate_meta_state_cache()` | 캐시 무효화 | `except: pass` |

**Governor가 실제로 `META_GOVERNOR_LAST_RUN_AT`를 갱신하는 유일한 확정 지점**

- `meta_governor.py:1246-1248` — `MetaGovernor._finalize_meta_headers()`  
  `META_GOVERNOR_LAST_RUN_AT = datetime.now(timezone.utc).isoformat()`

이는 `factory_artifact_guard._run_meta_governor_cycle()` → `MetaGovernor.run_governor_cycle()` 경로에서만 호출됨 (`factory_artifact_guard.py:105-142`).

### 1.3 Governor 재실행이 **스킵**되는 구조적 이유 (6/8 고착의 직접 원인)

`factory_artifact_guard._meta_needs_rebuild()` (`factory_artifact_guard.py:71-102`):

1. `is_meta_state_degraded(state)` → **False** 조건:
   - `META_REGIME_KEY`가 `BULL` 등 유효값
   - `META_GOVERNOR_LAST_RUN_STATUS`가 `NEVER`/`ERROR` 아님
   - `META_GOVERNOR_LAST_RUN_AT` **키가 존재** (값이 48시간 전이어도 무관)
2. JSON 파일 **mtime** 기준 `FACTORY_META_MAX_AGE_HOURS`(기본 24h)만 추가 검사

`is_meta_state_degraded()` (`meta_state_store.py:260-285`):

- **`META_GOVERNOR_LAST_RUN_AT`의 시각적 신선도(age) 검사 없음**
- `BULL` + confidence > 0 + notes 존재 → degraded **아님**

→ **2026-06-08에 마지막으로 성공한 Governor 스냅샷이 2026-06-10에도 “정상”으로 분류**되어 `ensure_meta_governor_state()`가 `{"meta":"ok","meta_status":"fresh"}`로 **조기 반환** (`factory_artifact_guard.py:230-231`) 가능.

### 1.4 Meta `BULL`이 Config에 **쓰이지 않는** 경로

**쓰기 SSOT**: `sync_config_regime_from_meta()` (`meta_state_store.py:151-257`)

- `set_config_value("REGIME_ANALYSIS", ra_out)` — SQLite `config_kv`
- `set_config_value("CURRENT_REGIME_KEY", rk_meta)`
- **`system_config.json` 자동 갱신 없음** (`config_manager.set_config_value` 326-366행: SQLite만)

**Silent Fail 지점**

| 위치 | 파일:행 | 동작 |
|------|---------|------|
| A | `meta_state_store.py:429-432` | `save_meta_governor_state_unified()` 내 `sync_config_regime_from_meta()` 실패 → `logger.warning`만, 저장 계속 |
| B | `meta_state_store.py:500-501, 508-509` | `rebuild_meta_state()` 내 sync 예외 → `config_regime_sync.error` dict, **raise 없음** |
| C | `overseer_audit_binder.py:401-421` | `build_overseer_audit_dossier()` 내 `ensure_config_regime_aligned(force=True)` 실패 → **전체 `except: pass`** |
| D | `scanner_regime_ssot.py:27-32` | `ensure_config_regime_aligned()` 실패 → `logger.debug`만 |

SQLite lock/권한/OCC 충돌 시 15회 재시도 후 raise (`meta_state_store.py:213-228`)하지만, **상위 A/B/C/D가 예외를 삼킴**.

### 1.5 `regime_meta_analyzer`와 Governor의 **이중 국면 채널**

`regime_meta_analyzer.analyze_market_regime()` (`regime_meta_analyzer.py:315-374`):

- `REGIME_ANALYSIS.regime_key` + `CURRENT_REGIME_KEY`를 **별도** 산출 (yfinance 콜로세움)
- `set_config_value("REGIME_ANALYSIS", ...)` / `CURRENT_REGIME_KEY` 시도

`META_REGIME_KEY`는 **MetaGovernor cycle**에서만 확정 (`meta_governor.py`).

→ DB `meta_state_log`의 `BULL`은 **6/8 Governor 실행분**일 수 있고, `REGIME_ANALYSIS`는 **갱신 실패·미실행·UNKNOWN 잔존**일 수 있다. **한쪽만 살아있는 분열 상태가 코드상 허용**된다.

---

## Target 2: UNKNOWN 폴백 및 Kelly 0.01 클램프

### 2.1 `config_regime_key` → `UNKNOWN`이 되는 조건

**함수 SSOT**: `resolve_config_regime_key()` — `meta_state_store.py:40-60`

```text
1) REGIME_ANALYSIS.regime_key 정규화 → "", "UNKNOWN" 아니면 반환
2) 아니면 CURRENT_REGIME_KEY 정규화 (없으면 "UNKNOWN")
```

`normalize_regime_key()` (`meta_state_store.py:31-37`): 인식 불가 값 → **`UNKNOWN`**.

**UNKNOWN 강제 전환 트리거 (팩트)**

| 조건 | 위치 |
|------|------|
| `REGIME_ANALYSIS` 없음/빈 dict | `resolve_config_regime_key` 55-60행 |
| `REGIME_ANALYSIS.regime_key` ∈ `{"", "UNKNOWN"}` | 57-59행 ( `CURRENT_REGIME_KEY`가 BULL이어도 **RA가 UNKNOWN이면 UNKNOWN 반환**) |
| `CURRENT_REGIME_KEY` 누락 | 60행 기본 `"UNKNOWN"` |
| `load_system_config()` 실패 → 빈 dict | 49-54행 |

**중요**: `REGIME_ANALYSIS.regime_key=UNKNOWN` 이고 `CURRENT_REGIME_KEY=BULL` 인 **부분 동기화** 상태에서도 `resolve_config_regime_key`는 **`UNKNOWN`** 을 반환한다.

### 2.2 Kelly `0.01` (1.00%) 하드코딩·폴백 라인

| 파일 | 함수/위치 | 내용 |
|------|-----------|------|
| `meta_governor_consumer.py` | `resolve_trading_kelly_base()` **73행** | except 시 `DYNAMIC_KELLY_RISK` 기본 **`0.01`** |
| `regime_kelly_failsafe.py` | `resolve_graceful_base_kelly()` **126행** | `raw_base = c.get("DYNAMIC_KELLY_RISK", 0.01)` |
| `reports/report_state_binder.py` | `_resolve_kelly_display()` **180, 191행** | 동일 기본 **`0.01`** |
| `overseer_audit_binder.py` | `_resolve_overseer_kelly_display()` **224행** | 동일 |
| `bitget/forward/reports.py` | **77-78행** | `DYNAMIC_KELLY_RISK` 기본 **`0.01`** |
| `bitget/forward/ledger.py` | **170행** | hydrate 실패 시 `kelly_risk_pct = cfg.get("DYNAMIC_KELLY_RISK", 0.01)` |
| `meta_governor.py` | `ACTION_BY_REGIME["BEAR"]` **52행** | `kelly_cap: 0.01` (BEAR 국면 상한) |

**실매매 try_add 경로 (scanner)**

1. `scanner_regime_ssot.hydrate_intraday_scanner_config()` (`scanner_regime_ssot.py:14-66`)
   - `ensure_config_regime_aligned()` — 실패 시 debug skip
   - `resolve_trading_kelly_base()` → `regime_kelly_failsafe.resolve_graceful_base_kelly()`
2. `bitget/supernova_hunter.py:338-340` / `supernova_hunter.py:1327-1329` — scan 직전 hydrate 호출
3. `bitget/forward/ledger.py:166-168` — `resolve_trading_kelly_base` 우선, 실패 시 **0.01**
4. `meta_governor_consumer.apply_meta_kelly_merge()` **159-160행** — `KILL_SWITCH` → **0** (거래 0건)

**Graceful lift가 **안** 되는 조건** (`regime_kelly_failsafe.py:114-177`)

- Meta·Config **둘 다** UNKNOWN → `neutral_regime_default` / MA fallback (1% 고정은 아님)
- Meta BULL + Config UNKNOWN → **`meta_bull_forced_unlock`** (151-160행)로 상향 **설계됨**
- 그러나 `resolve_trading_kelly_base` **72-73행** 전체 except → **무조건 0.01**
- `hydrate_intraday_scanner_config` **63-64행** except → hydrate 실패, **config 원본 0.01 유지**
- `persist_kelly=False` (기본) → 메모리만 갱신, **DB/JSON에 0.01 영구 고착**

**감사관 1.00% 표시**

- `overseer_audit_binder._resolve_overseer_kelly_display()` — `apply_graceful_kelly_to_effective()` + `macro.effective_kelly_risk` max
- Meta hydrate 실패·`META_GLOBAL_KELLY_MULT` 극저·graceful except 시 **정확히 1.00%** 가능
- `detect_audit_anomalies()` **502-520행**: `eff_k < 0.015` + BULL + 당일 거래 0 → **`SIGNAL_MISMATCH` CRITICAL**

### 2.3 `system_config_atomic.py`

- **래퍼만** 존재 (`system_config_atomic.py:1-24`) → `config_manager` 위임
- UNKNOWN/Kelly 로직 없음. 실체는 `config_manager.py` + `regime_kelly_failsafe.py`

---

## Target 3: AI 상시 감사관 데이터 참조 불일치

### 3.1 주식 팩토리 감사관 (`ai_overseer.py` + `overseer_audit_binder.py`)

**데이터 수집 경로 (팩트)**

```text
ai_overseer.run_ai_auditor()
  ├─ cfg = load_config()                    # ai_overseer.py:51-63, 164
  │     └─ factory_data_paths.system_config_json_path() → **system_config.json 직접 read**
  ├─ ensure_meta_governor_state()         # 166-169 (degraded 아니면 cycle skip 가능)
  ├─ meta = load_meta_state_resolved()    # unified: market_db > config_kv > JSON
  └─ build_overseer_audit_dossier(cfg, meta, db_path)
        ├─ ensure_config_regime_aligned(m, force=True)  # 405-407
        ├─ cfg ← load_system_config()     # SQLite 병합 (성공 시에만)
        ├─ meta_regime_key = m["META_REGIME_KEY"]       # 431
        ├─ config_regime_key = _resolve_overseer_config_regime(m, cfg)  # 423, 457
        └─ effective_kelly_risk = _resolve_overseer_kelly_display(...)   # 424, 458
```

**`_resolve_overseer_config_regime()`** (`overseer_audit_binder.py:185-201`):

- `resolve_config_regime_key(cfg)`가 UNKNOWN이고 Meta가 BULL이면 → **표시용으로 Meta 반환**
- 즉 **최신 코드**에서 dossier `config_regime_key`는 UNKNOWN **마스킹** 가능

**그런데도 리포트에서 BULL vs UNKNOWN이 동시에 보이는 이유 (아키텍처 진단)**

| 현상 | 설명 |
|------|------|
| `meta_regime_key=BULL` | `load_meta_state_resolved()` / `META_REGIME_KEY` — **DB 스냅샷 직결** |
| `config_regime_key=UNKNOWN` (원시) | `ai_overseer` 초기 `cfg`는 **JSON**; sync 실패 시 `REGIME_ANALYSIS.regime_key` 여전히 UNKNOWN |
| Anomaly `REGIME_SSOT_SPLIT` | `detect_audit_anomalies` 616-629행 — dossier의 meta vs config **불일치 시 WARN** (마스킹 후에도 불일치면 meta·config 둘 다 non-UNKNOWN인데 다른 값) |
| Governor `2026-06-08` | dossier `meta_governor_last_run_at` ← `m["META_GOVERNOR_LAST_RUN_AT"]` (**스냅샷 내용**, wall clock 미검증) |

**단순 Lag vs 영구 사망**

| 유형 | 판별 |
|------|------|
| **단순 Lag** | `meta_governor_sync` 직후 `load_system_config()`에 BULL 반영, JSON만 지연 |
| **영구 사망 (현재 증상에 가까움)** | `META_GOVERNOR_LAST_RUN_AT` 2일+ 고착 + `_meta_needs_rebuild=False` + sync silent fail + JSON/SQLite 분열 |

`ai_overseer.py:175-186`: `is_meta_state_degraded(meta)`이면 **리포트 차단** — 그러나 **BULL+OK+타임스탬프 존재**면 degraded 아님 → **6/8 스냅샷으로도 감사 리포트 발송 허용**.

### 3.2 Bitget 감사관 (`bitget/ai_overseer.py`) — 별도 세계

- `CONFIG_PATH = bitget/bitget_system_config.json` (21행) — **MetaGovernor 체인 미연동**
- `gather_daily_system_facts()` 128행: `config.get("CURRENT_REGIME_KEY", "UNKNOWN")` only
- **`load_meta_state_resolved()` / `overseer_audit_binder` / `meta_governor_sync` 없음**

스크린샷이 **Rules-first + MetaGovernor SSOT + `REGIME_SSOT_SPLIT`** 형식이면 **주식 `ai_overseer.py`** 경로. Bitget 경로가 아님.

---

## 신경망 단절 지도 (아키텍처)

```text
                    ┌─────────────────────────────┐
                    │  regime_meta_analyzer         │
                    │  (yfinance → REGIME_ANALYSIS) │
                    └──────────────┬──────────────┘
                                   │ set_config_value (SQLite)
                                   ▼
┌──────────────┐    run_governor    ┌─────────────────────────────┐
│ market_data  │◄───cycle──────────│ MetaGovernor                 │
│ meta_state_  │    (6/8 고착?)    │ META_REGIME_KEY, LAST_RUN_AT │
│ log          │                   └──────────────┬──────────────┘
└──────┬───────┘                                  │
       │ load 우선                                 │ save_meta_governor_state_unified
       ▼                                           ▼
┌──────────────────┐              sync_config_regime_from_meta (실패 시 warning만)
│ load_meta_state_ │                              │
│ resolved()  BULL │                              ▼
└────────┬─────────┘              ┌─────────────────────────────┐
         │                        │ system_config.sqlite        │
         │                        │ REGIME_ANALYSIS /           │
         │                        │ CURRENT_REGIME_KEY /        │
         │                        │ DYNAMIC_KELLY_RISK (=0.01?) │
         │                        └──────────────┬──────────────┘
         │                                       │ load_system_config (일부 경로)
         │         ┌─────────────────────────────┘
         │         │  ✂️ 단절: JSON 미러 미동기
         ▼         ▼
┌─────────────────────────────────────────┐
│ system_config.json (stale UNKNOWN/0.01)   │◄── ai_overseer.load_config() 직결
└─────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────┐
│ scanner hydrate (실패 시 debug)         │
│ ledger try_add → Kelly 0.01 → 0 trades  │
└─────────────────────────────────────────┘
```

**끊어진 신경망 5곳 (우선순위)**

1. **Governor 신선도 검사** — `LAST_RUN_AT` 내용 age 미검사 (`meta_state_store.py:260-268`, `factory_artifact_guard.py:94-96`)
2. **Meta→Config sync** — silent fail (`meta_state_store.py:429-432`, `overseer_audit_binder.py:420-421`)
3. **SQLite ↔ JSON 이중 읽기** — `ai_overseer` JSON vs `config_manager` SQLite (`ai_overseer.py:51-63` vs `config_manager.py:631-670`)
4. **`REGIME_ANALYSIS` vs `CURRENT_REGIME_KEY` 우선순위** — RA=UNKNOWN이면 CR=BULL 무시 (`meta_state_store.py:55-60`)
5. **Bitget daily_audit** — `meta_governor_sync` step 부재 (`bitget/pipelines/bitget_pipelines.py:259-271`)

---

## 팩트 체크리스트 (운영 확인용, 코드 변경 없음)

```bash
# 1) Meta 스냅샷 나이
sqlite3 /path/to/market_data.sqlite \
  "SELECT updated_at_utc, regime_key, governor_status FROM meta_state_log ORDER BY id DESC LIMIT 3;"

# 2) Config KV 실제 값 (JSON 말고)
sqlite3 /path/to/system_config.sqlite \
  "SELECT key, value_json FROM config_kv WHERE key IN ('REGIME_ANALYSIS','CURRENT_REGIME_KEY','DYNAMIC_KELLY_RISK','META_GOVERNOR_STATE');"

# 3) JSON vs SQLite 불일치
python -c "
from config_manager import load_system_config, get_config_value
import json, os
from factory_data_paths import system_config_json_path
j=json.load(open(system_config_json_path()))
s=load_system_config()
for k in ['CURRENT_REGIME_KEY','DYNAMIC_KELLY_RISK']:
 print(k, 'json=', j.get(k), 'sqlite=', s.get(k))
ra_j=(j.get('REGIME_ANALYSIS') or {}).get('regime_key')
ra_s=(s.get('REGIME_ANALYSIS') or {}).get('regime_key')
print('REGIME_ANALYSIS.regime_key json=', ra_j, 'sqlite=', ra_s)
"

# 4) 최근 meta_governor_sync 성공 여부 (factory 로그)
grep -E "meta_governor_sync|config regime sync|MetaGovernor done" /path/to/factory/logs/*.log | tail -20
```

---

## 수정 방향 (참고 — 본 감사에서는 미적용)

코드 변경은 요청 범위 외. 추후 작업 시 우선순위만 기록:

1. `is_meta_state_degraded` / `_meta_needs_rebuild`에 **`META_GOVERNOR_LAST_RUN_AT` 최대 age** 추가
2. `sync_config_regime_from_meta` 실패 시 **critical step abort** (silent pass 제거)
3. `set_config_value` 후 **JSON 미러 동기화** 또는 `ai_overseer`가 **`load_system_config()`만** 사용
4. `resolve_config_regime_key`에서 `CURRENT_REGIME_KEY`와 `REGIME_ANALYSIS` **불일치 시 max/Meta 우선 규칙**
5. Bitget `daily_audit`에 `meta_governor_sync` 또는 동등 hook 추가

---

## 파일·함수 색인

| 주제 | 파일 | 핵심 행 |
|------|------|---------|
| Sync step | `factory_pipelines.py` | 21-57, 417-424 |
| Rebuild | `meta_state_store.py` | 151-257, 438-532 |
| Degraded 판정 | `meta_state_store.py` | 260-285 |
| Rebuild skip | `factory_artifact_guard.py` | 71-102, 220-254 |
| Governor timestamp | `meta_governor.py` | 1246-1248 |
| Config regime resolve | `meta_state_store.py` | 40-60 |
| Kelly 0.01 | `meta_governor_consumer.py` | 56-73 |
| Graceful Kelly | `regime_kelly_failsafe.py` | 114-177 |
| Scanner hydrate | `scanner_regime_ssot.py` | 14-66 |
| 감사 dossier | `overseer_audit_binder.py` | 185-235, 401-458, 502-520, 616-629 |
| 감사 진입 | `ai_overseer.py` | 51-63, 164-197 |
| Config SQLite | `config_manager.py` | 326-366, 631-670 |
| Bitget (무 sync) | `bitget/pipelines/bitget_pipelines.py` | 259-271 |
| Bitget 감사 | `bitget/ai_overseer.py` | 21-22, 125-134 |

---

*본 문서는 소스 정적 분석 결과이며, 운영 DB/로그 스냅샷과 교차 검증 시 원인 확정도가 올라간다.*
