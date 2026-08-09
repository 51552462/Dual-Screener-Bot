# CLAUDE → CURSOR (Handoff INBOX)

> **작성**: Claude Pro **만**  
> **현재**: 🔴 **F-GATE-01**(Claude OK ✅) · **F-RETIRE-02**(Claude OK ✅) — **두 건 모두 구현·검증 완료, 서버 배포는 디렉터 승인 대기**(F-GATE-01 먼저 배포 권장) · **C-FUNNEL-02**(배포 완료, T+1 검증 중) · **RP-1 + C-1 병합**(진행중, 병렬 — 파일 겹침 없음)

---

## 🔴 [CAT-F] F-GATE-01 — Registry State 기반 진입 차단 패치 (디렉터 승인 후 착수)

> **선행 확정 사실 (Cursor 회신 + VPS 실측, 재설계 아님)**: `resolve_group_treasury_mult`가 health 키 없을 때 `(1.0, "default")` 반환 확정. `evaluate_meta_group_entry_gate`는 mult≤0에서만 block — registry `state`(COOLED/RETIRED) 미참조 확정. VPS 교차조회: **COOLED/RETIRED 0건**(현재 활성 인스턴스 없음 — 구조적 결함이지 진행 중인 사고 아님)
> **순서**: 본 Handoff을 **F-RETIRE-02보다 먼저** 착수(사유는 §순서 참조)

### SSOT (변경/비변경)

- **변경**: `meta_treasury_entry_guard.py`(`evaluate_meta_group_entry_gate` — 신규 registry-state 체크 분기 **추가만**, 기존 mult 계산·block 조건식 무변경)
- **참조(읽기전용)**: `strategy_registry_store.load_registry_rows`(또는 governor 사이클 내 이미 로드된 `META_STRATEGY_REGISTRY` — 매 진입평가마다 재쿼리 금지, 아래 §성능 참조), `strategy_promotion_engine.py`(state 값 자체 — 산출 로직 비접촉)
- **비접촉**: `resolve_group_treasury_mult`의 default 반환값 자체(1.0 그대로 유지 — mult 의미 재정의 아님), Kelly sizing/`try_add_virtual_position`

### Spec

**A. 신규 판별 함수**

```text
resolve_registry_state_block(market: str, group_key: str, *, registry_rows: list[dict] | None = None) -> tuple[bool, str]
```

- `registry_rows`에서 `(market, group_key)` 최신 row의 `state`가 `COOLED` 또는 `RETIRED`면 `(True, "registry_state_block")`, 그 외(LIVE/CANDIDATE/OBSERVING 또는 row 없음)는 `(False, "")`
- **F-RETIRE-02 연동**: `state`가 observe_only 재발굴로 `CANDIDATE`로 복귀한 순간부터 자동으로 `(False, "")` — 별도 해제 로직 불필요(동일 SSOT 필드 재사용)

**B. `evaluate_meta_group_entry_gate` 결선**

- 기존 `mult <= 0.0` 체크와 **병렬 병기**(OR 조건, 둘 중 하나만 True여도 block): `state_blocked, state_reason = resolve_registry_state_block(...)` → `state_blocked`면 `block_entry=True`, reason에 기존 `"hard_cut"`류와 구분되는 `"registry_state_block"` 사용(사후 로그 분석 시 원인 구분 목적)
- 기존 mult 기반 block 경로·반환 스키마는 **한 글자도 변경 없음** — 신규 분기 추가만

**C. Kill switch (신규 config, additive)**

```text
ENABLE_REGISTRY_STATE_ENTRY_GATE: bool = True
```

`False` → 즉시 패치 이전 동작(mult-only)으로 완전 복귀

**D. 성능 주의 (Cursor 설계 재량, 스펙 강제 아님)**

진입평가는 고빈도 호출 경로 — `resolve_registry_state_block`이 매 호출마다 DB 재쿼리하지 않도록 governor 사이클 내 이미 로드된 `META_STRATEGY_REGISTRY`(메모리)를 우선 소비하고, 없을 때만 `load_registry_rows` fallback 권장(강제 아님, Cursor가 기존 캐싱 패턴 있으면 그대로 재사용)

### KR/US 분기

없음 — `market` 파라미터 그대로 전달, registry 자체가 이미 시장별 row. 시장별 if-하드코딩 대상 아님(Rule 8 해당 없음, 그대로 통과 원칙만 확인)

### 인접 CAT 영향

| CAT | 영향 | Critical |
|---|---|---|
| CAT-F | **전체 LIVE 진입 경로**에 신규 체크 삽입(주 변경) | 🔴 — 모든 시장·모든 그룹의 진입 평가가 이 함수를 거침. 단 **오늘 기준 COOLED/RETIRED 0건이라 배포 즉시 동작 변화는 없음**(잠재 리스크 차단이 목적, 활성 인시던트 대응 아님) |
| CAT-H | 비접촉 | — |
| CAT-B | 비접촉 | — |
| CAT-G | 비접촉 | — |

### 롤백 조건

`ENABLE_REGISTRY_STATE_ENTRY_GATE=False` → 즉시 패치 이전 상태(mult-only 게이트)로 완전 복귀. additive 분기라 기존 LIVE 그룹(현재 전부 state=LIVE/CANDIDATE로 추정) 진입 흐름에 영향 없음 — COOLED/RETIRED row가 생기는 시점(=F-RETIRE-02 가동 이후)부터 실효.

### Cursor 지시

1. **디렉터 승인 후 착수**(Rule 7 — CAT-F Critical, 전체 진입경로 터치) — 착수 전 디렉터 Go 명시 필요
2. `evaluate_meta_group_entry_gate` 기존 반환 스키마·호출부 시그니처 **무변경** — 내부 분기 추가만
3. §D 성능 권장(캐시 우선) — 강제 스펙 아님, 회귀만 없으면 Cursor 재량
4. 테스트: `tests/test_registry_state_entry_gate_f_gate_01.py`
   - (a) state=LIVE/CANDIDATE — 기존 mult 로직만으로 판정(회귀 없음)
   - (b) state=COOLED/RETIRED — mult=1.0(default)이어도 block 확인(핵심 케이스)
   - (c) F-RETIRE-02 redemption으로 state→CANDIDATE 복귀 시 즉시 unblock
   - (d) `ENABLE_REGISTRY_STATE_ENTRY_GATE=False` — 패치 이전 동작 정확히 재현
   - (e) registry row 자체가 없는 group(신규 discovery 전) — block 아님, 기존 `"empty_group"` 경로 유지 회귀 확인

### 위험도

🔴 **Critical(구조)** · 🟢 **활성 인시던트 없음**(오늘 기준 COOLED/RETIRED 0건) — 두 표현이 동시에 참. 디렉터 승인 후 착수, Cursor 구현·테스트 완료 후 Claude 검증 필수(C-FUNNEL-02와 동일 순서).

---

## 🔴 [CAT-F] 긴급 확인 요청 — capital_mult 미소비 / health 만료 후 실자본 재진입 가능성 (2026-08-09)

> **유형**: **확인 전용 — 구현 아님.** F-RETIRE-02(observe_only)와 별개 이슈, 그보다 **우선** 확인
> **트리거**: Cursor Step 0 회신(`CURSOR_TO_CLAUDE.md` §F-RETIRE-01 항목 1) — `capital_mult`가 진입 경로에서 read 0곳, `evaluate_meta_group_entry_gate`는 registry `state`가 아니라 `META_STRATEGY_HEALTH` mult만 참조

### 확인해야 하는 이유

`strategy_registry.state`(LIVE/COOLED/RETIRED)는 원래 "이 그룹에 실자본을 태워도 되는가"의 SSOT여야 한다. 그런데 Step 0 보고대로면 실제 진입 게이트는 그 `state`를 **한 번도 읽지 않고**, Treasury `health` mult(=최근 lookback 내 실거래 롤링 통계)만 본다. `health`는 슬라이딩 윈도우라 **나쁜 과거 실적이 lookback을 넘기면 자연스럽게 잊힌다** — 이때 해당 group_key가 `health` 딕셔너리에서 아예 사라지고, 소비 측(`evaluate_meta_group_entry_gate` 등)이 "키 없음"을 **default mult=1.0(허용)**으로 처리한다면, RETIRED로 명시적으로 강등된 전략이 **재승인 절차 없이 조용히 실자본을 다시 받을 수 있다.** 이건 observe_only(관측)와 무관하게 **지금 이 순간의 운영 리스크**일 수 있다.

### Cursor 확인 요청 (구현 없이 3가지만)

1. **default 동작 확정** — `forward/shared.py`(또는 `evaluate_meta_group_entry_gate`가 실제 정의된 파일) 코드 실측: `health.get(key)`가 없을 때 mult가 **1.0으로 폴백되는지, 0.0/block으로 폴백되는지** 정확한 분기 인용
2. **VPS 실측 교차조회** — `strategy_registry` 중 `state ∈ (COOLED, RETIRED)`인 group_key 목록 ∩ 현재 `META_STRATEGY_HEALTH` 스냅샷(라이브 `META_STRATEGY_REGISTRY`/`META_STRATEGY_HEALTH` 덤프)에서 **mult=1.0이거나 키 자체가 없는 항목**이 있는지. 있다면 = 스모킹건(현재 진행 중인 실자본 누출 가능성)
3. **최근 실거래 대조** — 1·2에서 위험군이 나오면, 해당 group_key로 `state` 강등 이후 실제 `forward_trades` CLOSED row가 발생했는지(=이미 실자본이 들어갔는지) 날짜 대조

### 회신 형식

- [CAT-F] 결론 3줄
- 위 3건 표
- 위험군 0건이면 "확인 완료, 리스크 없음" / 1건 이상이면 **디렉터 즉시 보고 대상** — Claude가 별도 패치 Handoff(가칭 F-GATE-01) 작성

### 위험도

🔴 **Critical (확인 단계)** — LIVE 자본 배분 게이트의 SSOT 정합성 문제. 패치는 아직 설계하지 않음(사실관계 확인 먼저, Rule 5·6 위반 없이 설계하려면 정확한 default 분기부터 알아야 함). 다중 CAT 파급 가능성(CAT-F 상태기계 + 진입 게이트 공유 경로) — 확인 결과에 따라 디렉터 에스컬레이션 여부 재판단.

---

## [CAT-F] F-RETIRE-02 — COOLED/RETIRED Observe-Only 사후추적 (Go 확정 — 위 🔴 긴급확인 이후 착수)

> **선행조건 (확정)**: VPS 실측 결과 COOLED/RETIRED **0건**으로 스모킹건은 없었으나, 구조적 결함(default mult=1.0, state 미참조) 자체는 확정됨 — **F-GATE-01을 먼저 착수·완료한 뒤 본 Handoff 착수**. observe_only가 COOLED/RETIRED 개체 수를 0→N으로 늘리는 기능인 만큼, 그 개체가 새는 게이트에 노출되는 순서(F-RETIRE-02 먼저)는 피함

### SSOT (변경/비변경)

- **변경**: `strategy_promotion_engine.py`(`run_registry_lifecycle` — COOLED/RETIRED 대상 observe_only 신호 소비 분기 **추가만**. LIVE/CANDIDATE/COOLED **기존 승격·강등 임계값·전이식은 한 줄도 변경 안 함**), `strategy_registry_store.py`(additive 컬럼), **`forward/shared.py`**(신규 — 아래 B' 참조, `evaluate_meta_group_entry_gate`/`RE_EVOL_SHADOW` 인접 지점에 additive 분기만 추가, 기존 LIVE 진입 판정식 비접촉)
- **참조(읽기전용, Adapter 재사용 — 함수 시그니처만 소비, 파일 내부 로직 이관 없음)**: `re_evolution_redemption_gate.py`(`compute_dynamic_shadow_verification_window`, `fetch_shadow_closed_rows`, `compute_shadow_stats`, `passes_redemption_gate`), `forward/shared.py`의 기존 `apply_shadow_entry_zero_notional`(RE_EVOL_SHADOW 전용 — **동일 패턴**을 LIFECYCLE_OBSERVE_ONLY용으로 별도 함수화, 기존 함수 수정 아님), `strategy_lifecycle_config.py`(`alpha_half_life_days` 등 기존 CAT-CONSTANTS — 신규 상수 추가 없음)
- **비접촉**: LIVE/CANDIDATE 승격 임계값(`passes_live_hard_gate` 등), Kelly/자본배분 로직, 기존 `RE_EVOL_SHADOW` 3-Strike 경로(태그 네임스페이스 분리로 간섭 없음)

### Spec

**A. 신규 태그** — `LIFECYCLE_OBSERVE_ONLY`(`forward_trades.sig_type` suffix). `RE_EVOL_SHADOW`와 **문자열 분리**(함수는 재사용, 태그는 분리 — 두 강등 경로의 사후분석이 섞이지 않도록)

**B. 관측 트리거** — `state ∈ {COOLED, RETIRED}` 전환 시점부터 시작:

```text
is_lifecycle_observe_only_row(row: dict) -> bool
```

기존 `is_re_evolution_observing_row`(state=="OBSERVING" 전용)와 동일 판정 패턴을 COOLED/RETIRED로 확장한 **별도 함수**(기존 함수 수정 아님)

**B'. 실행계층 결선 (신규, 필수 — Step 0로 확정된 스코프)**

Step 0 확인: `capital_mult`는 진입 경로 미참조, `RE_EVOL_SHADOW`만 `apply_shadow_entry_zero_notional`로 실제 $0 페이퍼가 `forward_trades`에 적재됨. **동일 지점·동일 패턴**으로 신규 함수를 결선:

```text
apply_lifecycle_observe_only_entry_zero_notional(row: dict, *, market: str, meta, sys_config) -> None
```

`is_lifecycle_observe_only_row(row)`가 True인 group_key에 한해, 기존 `apply_shadow_entry_zero_notional`과 같은 지점에서 호출 — $0 notional 신호를 `forward_trades`에 `sig_type` **`LIFECYCLE_OBSERVE_ONLY`** 태그로 적재. LIVE 그룹의 기존 진입 판정식·notional 계산은 **비접촉**.

**C. 보존창(retention) — 확정**

디렉터 90일(=US 기준) ÷ US half-life(30d) = `RETENTION_MULT = 3.0`으로 역산 확정(Rule 5 — 디렉터 지정값에서 도출, 임의 생성 아님):

| market | alpha_half_life_days | retention_days |
|---|---|---|
| KR | 10 | **30** |
| US | 30 | **90** |
| BG | 21 | 63(참고만, 본 Handoff 구현 대상 아님 — Rule 1) |

**D. 검증창(기존 재사용, 무변경)** — `compute_dynamic_shadow_verification_window`(half_life × 70~100% + HIGH_VOL/BEAR_PANIC ×0.5 dilation). 보존창(C) 소진 전까지 이 짧은 창 단위로 재발굴 시도를 반복

**E. 재발굴 게이트** — 기존 `passes_redemption_gate` 시그니처 그대로 재사용(신규 임계값 없음). 통과 시 `COOLED/RETIRED → CANDIDATE`(**LIVE 직행 아님** — 기존 CANDIDATE→LIVE Hard Gate 정상 재통과 필요)

**F. config 키(신규, additive)**

```text
LIFECYCLE_OBSERVE_ONLY_ENABLED: bool = True
LIFECYCLE_OBSERVE_ONLY_RETENTION_DAYS: {"KR": 30, "US": 90, "BG": 63}   # §C 확정값
```

**G. 함수 시그니처(신규 — `strategy_promotion_engine.py`/`forward/shared.py`/신규 파일, 파일 배치는 Cursor 판단)**

```text
is_lifecycle_observe_only_row(row: dict) -> bool
resolve_observe_only_retention_days(market: str, system_cfg: dict | None) -> int
apply_lifecycle_observe_only_entry_zero_notional(row: dict, *, market, meta, sys_config) -> None
evaluate_lifecycle_observe_only_redemption(row: dict, *, meta, sys_config, forward_db_path, now) -> tuple[bool, dict]
```

### KR/US 분기

공통 함수, `market` 파라미터로 `strategy_lifecycle_config.market_params()` 조회만 — 시장별 if-하드코딩 금지(Rule 8) 그대로 준수. BG는 SSOT상 값만 존재 — **본 Handoff 구현·실행계층 결선 대상 아님**(Rule 1).

### 인접 CAT 영향

| CAT | 영향 | Critical |
|---|---|---|
| CAT-F | 신규 관측 로직 + 실행계층 결선(B') 추가 — LIVE/CANDIDATE/COOLED 기존 임계값·판정식 무변경 | 🟡 (공유 실행파일 `forward/shared.py` 터치로 상향, LIVE 로직 자체는 비접촉) |
| CAT-H | 비접촉(`alpha_half_life_days` read-only 참조만, 재정의 없음) | — |
| CAT-B | 비접촉(신규 테이블 없음, `forward_trades.sig_type` 태그 추가만) | — |
| CAT-G | 비접촉 | — |

🔴 Critical 아님(위 🔴 긴급확인 항목과는 별개 — 그건 기존 게이트의 사실관계 확인, 이건 신규 관측 기능 추가). 단 `forward/shared.py` 공유 파일 터치로 🟢→🟡 상향.

### 롤백 조건

`LIFECYCLE_OBSERVE_ONLY_ENABLED=False` → 즉시 현행(RETIRED 터미널·무추적) 복귀. 태그·컬럼·B' 결선 모두 additive라 기존 LIVE 파이프라인 영향 없음.

### Cursor 지시

1. **선행 필수** — 위 🔴 긴급확인 항목 먼저 회신. "위험군 있음"이면 F-GATE-01(패치) 대기, "리스크 없음"이면 바로 착수
2. B'(실행계층 결선) 구현 시 `apply_shadow_entry_zero_notional` 정의부를 **그대로 복제하지 말고** 공통 헬퍼로 뽑을 수 있으면 제안(강제 아님 — Cursor 판단, 단 RE_EVOL_SHADOW 기존 동작 회귀 없어야 함)
3. §C retention 값 그대로 구현 — 코드 재작성 없이 config 값만 교체 가능하도록 §F 구조 유지
4. 테스트: `tests/test_lifecycle_observe_only_f_retire_02.py`
   - (a) COOLED/RETIRED 진입 시 `LIFECYCLE_OBSERVE_ONLY` 플래그 세팅 + `forward_trades` $0 notional 적재 확인
   - (b) KR 30d / US 90d 경과 후 관측 종료(추가 태깅 중단) 스모크
   - (c) 재발굴 게이트 통과 시 `CANDIDATE` 복귀(LIVE 아님) 회귀
   - (d) `RE_EVOL_SHADOW` 3-Strike 경로와 태그 네임스페이스·`apply_shadow_entry_zero_notional` 기존 동작 미간섭 확인

### 위험도

🟡 Medium(공유 실행파일 `forward/shared.py` 터치, 단 additive 분기·자본 미배분) — 착수 전 위 🔴 긴급확인 완료 필수.

---

## [CAT-C] C-FUNNEL-02 — 스캔 퍼널 탈락 계측 (insert 회귀 수정 + near-miss 이벤트 로그)

### SSOT (변경/비변경)
- **변경**: `scanner_funnel.py`(`ScanFunnelTracker.drop` 시그니처 확장 · near-miss 버퍼 · `finalize` flush), `proprietary_friction_store.py`(루트 파일 — `insert_scan_funnel_snapshot` 복구 + 신규 `insert_scan_funnel_drop_events` + `drops_json`/`scanner` 컬럼)
- **참조(읽기전용)**: 기존 `{market}_REGIME_KEY` / `REGIME_ENSEMBLE.markets.{market}.regime`(A-3와 동일 read 패턴 — CAT-G 로직·계산 변경 없음), `supernova_hunter.py`(`drop()` 호출부에 score 전달 추가만, 판정식 무변경)
- **비접촉**: 스캔 pass/drop 판정 로직 자체(cutoff 비교식) — 계측만 추가, 결과 불변

### Spec

**A. 회귀 수정 (선행, 독립 커밋)**

`insert_scan_funnel_snapshot(ts, market, universe_size, survivors, pass_rate_pct, scanner=None, drops_json=None) -> None`

- `finalize()`에서 `drop_summary`(Counter) → `json.dumps(dict(drop_summary))`로 `drops_json` 전달, 호출 스캐너명 `scanner`에 전달
- 무음 `try/except: pass` 제거 — 실패 시 `logger.warning` 최소 1줄(예외 전파 여부는 Cursor 판단, 무음만 금지)

**B. `scan_funnel_snapshot` 추가 컬럼**

| 컬럼 | 타입 | 비고 |
|------|------|------|
| scanner | TEXT NULL | 신규 — Audit 권장 |
| drops_json | TEXT NULL | 신규 — `{reason: count}` JSON |

**C. 신규 테이블 `scan_funnel_drop_event`**

| 컬럼 | 타입 | 비고 |
|------|------|------|
| id | INTEGER PK AUTOINCREMENT | |
| ts | TEXT(ISO UTC) | |
| market | TEXT | KR/US |
| scanner | TEXT | |
| code | TEXT NULL | 과도기 호환 — 없으면 NULL |
| reason | TEXT | 기존 Counter 키 재사용(신규 사유 문자열 창조 금지) |
| final_score | REAL NULL | |
| eff_cos_cutoff | REAL NULL | |
| eff_ml_cutoff | REAL NULL | |
| regime_key | TEXT NULL | finalize 1회 read로 denormalize |
| rank_in_slot | INTEGER NULL | near-miss 정렬 순위(1=cutoff 최근접) |

**D. `ScanFunnelTracker.drop()` 시그니처 확장** (하위호환 — 신규 인자 전부 keyword-only optional)

```text
drop(self, reason: str, n: int = 1, *, code: str | None = None,
     final_score: float | None = None,
     eff_cos_cutoff: float | None = None,
     eff_ml_cutoff: float | None = None) -> None
```

- 기존 `funnel.drop("DNA_FAIL")` 호출 **무변경 동작**. score 전달은 점진 적용 — **DNA_FAIL·LIQUIDITY 호출부부터** 우선

**E. Near-miss 샘플링 정책**
- 슬롯 = `(scan_date UTC, market, reason)` · **cap = 50/슬롯**
- 정렬 키: `|cutoff - final_score|` 오름차순(0에 근접할수록 우선) — score/cutoff 둘 다 없는 reason은 FIFO 50건 대체
- `finalize()` 1회 flush → `insert_scan_funnel_drop_events(rows: list[dict]) -> None`

**F. Regime denormalize**
- `finalize()`에서 market당 **1회만** `_read_current_regime_key(market: str) -> str | None` 호출 → 버퍼 전체 row에 stamp
- CAT-G 계산 로직 변경 없음 — A-3와 동일 read-only 패턴

**G. Retention**
- `KEEP_DAYS`/`KEEP_LAST` **디렉터 확인 요망** — v1은 pruning 없이 적재만

### KR/US 분기
- 공통 함수·테이블, `market` 컬럼 값만 KR/US 분기. 코드 내 시장별 if-하드코딩 금지(규칙 8)

### 인접 CAT 영향
| CAT | 영향 | Critical |
|-----|------|----------|
| CAT-C | 계측 코드 추가(주 변경) | 🟢 판정 로직 비접촉 |
| CAT-G | read-only 1회 조회만 | 🟢 로직/계산 무변경 |
| CAT-B | 비접촉(OHLCV 조인은 C-FUNNEL-03 별도 Handoff) | — |
| CAT-F | 비접촉 | — |

### 롤백 조건
- 신규 테이블·컬럼은 additive — 기존 파이프라인 영향 없음. 문제 시 `scan_funnel_drop_event` insert만 최소범위 try/except로 스킵, snapshot 경로는 유지
- `drop()` 시그니처는 하위호환 — 별도 롤백 불필요

### Cursor 지시
- Targeted diff only. 판정 로직(cutoff 비교, pass/drop 분기) **한 줄도 건드리지 말 것**
- 순서: **A(회귀 수정) 단독 커밋 먼저** → 검증 후 C~F(near-miss 계측)
- 테스트: `tests/test_scan_funnel_drop_event_c_funnel_02.py`
  - (a) 회귀 수정 후 `scan_funnel_snapshot` insert 성공 스모크
  - (b) `drops_json` 역직렬화 검증
  - (c) near-miss cap=50 초과 시 정렬·컷 스모크
  - (d) `drop()` 신규 인자 없이 호출 시 기존 동작 회귀 없음
  - (e) `regime_key` denormalize 1회 read 확인(mock)

### 위험도
🟢 Low(계측 전용, 판정 로직 비접촉, additive 스키마) — 디렉터 Critical 승인 불필요.

---

## [CAT-C] RP-1 + C-1 병합 — 15구간 레짐패널 baseline (RP-1) → 조건부 섹터부스트 A/B (C-1)

### SSOT (변경 금지 unless noted)
- 파일: `time_machine_backtester.py`(REGIME_PERIODS 확장, run_time_machine_regime_matrix 재사용)
- 참조(읽기전용): `performance_budget_governor.py`(tier 임계값), `meta_governor.py`(ACTION_BY_REGIME), `sector_rotation_store.py`(C-1)

### Stage 1 — RP-1 baseline (필수, 먼저)
- `REGIME_PERIODS` 6→15구간 확장 — 위 15구간 표 그대로. DB 미가용 구간은 백업 리스트로 즉시 치환(순연 금지, 치환 로그 남길 것)
- 시뮬 스택: S1(supernova) + S4(선택, timebox 되면) + S5(태그만) + **Phase A tier overlay**
- **Phase A overlay 스펙**: 라이브 모듈 풀 연동 아님. 백테스트 equity curve의 peak-to-trough 소진율을 계산해 `performance_budget_governor` tier 임계값(40/70/90%)과 동일 기준으로 `KELLY_THROTTLE_MULT`/`POSITION_QUOTA_MULT` **동일 로직으로 replay**만. config_kv 실제 write 없음.
- 시뮬레이션 단위: **KR+US 합산 포트폴리오** (개별 시장 분리 아님)
- 출력: `reports/regime_panel/rp1_{date}.json` — 구간별 CAGR/MDD/PF/n/진입0여부/tier소진로그
- **Lookahead**: v1(오늘 뇌 템플릿) 그대로, 리포트에 "상한선 추정치, Pass≠실전보장" 문구 고정 삽입. v2(point-in-time) 이번 스코프 아님.

### Stage 1 판정 (본 Handoff 규칙대로 자동 계산)
- Pass/Near-miss/Fail 상단 표 그대로 코드화
- Fail 시 원인 카테고리 A/B/C/D 자동 태깅 (§원인분석 트리 규칙 그대로 매핑: 진입n≈0→A, MDD>10%→C, 그 외 저수익→B)

### Stage 2 — C-1 A/B (조건부, Stage 1 결과로 자동 분기)
| Stage 1 결과 | Stage 2 행동 |
|--------------|---------------|
| Fail, 원인=A (신호부족) | **C-1 중단**. `report`에 "C-1 스킵: 원인 A" 명시하고 세션 종료 |
| Fail, 원인=C (MDD구조) | **C-1 중단**. 동일 처리 |
| Fail, 원인=B (수익부족·타이밍) | **C-1 축소 스코프**: sector spillover A/B만 (일반 기능화 아님), 15구간 중 원인B로 태깅된 구간만 재실행 |
| Near-miss (모든 원인) | **C-1 정상 진행** — baseline vs C-1 A/B, 15구간 전체 |
| Pass | C-1 진행(선택) — 이미 목표 달성이므로 우선순위 낮음, 스킵해도 무방 |

### 인접 CAT 영향
- CAT-F: read-only (tier 임계값 replay만, config_kv write 없음)
- CAT-G: read-only (REGIME_PERIODS 라벨 참조만)
- CAT-B: 신규 DB 없음 — JSON 리포트 파일만

### 롤백 조건
- 코드 자체가 배포되는 게 아니라 backtest 리포트 산출물이므로 롤백 대상 없음. 결과가 실망스러워도 **코드 삭제 금지** — 다음 Handoff 판단 자료로 보존.

### Cursor 지시
- Targeted diff only. Stage 1 무결론 2주 → **RP-1도 No-Go** (인프라/데이터 결함으로 태깅, C-1도 자동 스킵)
- 테스트: `tests/test_regime_panel_rp1.py` — 6→15 매핑 smoke, tier overlay 단위, Stage1→Stage2 분기 로직
- n<20 구간 자동판정 금지 로직 필수 (하드코딩 스킵)

### 위험도
🟡 High (목표 직결, 배포 아님) — 디렉터 승인 후 착수, 완료 후 Claude 검증
