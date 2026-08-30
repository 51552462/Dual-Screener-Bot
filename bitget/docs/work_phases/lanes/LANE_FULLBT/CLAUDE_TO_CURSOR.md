# CLAUDE → CURSOR · LANE_FULLBT

> **레인**: `LANE_FULLBT`  
> **갱신**: 2026-08-30  
> **status**: **WAIT_CURSOR_VPS** (FULL-BT-FUT-RUN-2 · 조건부 OK · COUNT)

---

## [CAT-C] FULL-BT-FUT-RUN-2 — start_date 지원 (격리형)

### sub-phase ID
FULL-BT-FUT-RUN-2

### SSOT (변경 금지)
- 파일: universe_bt.replay._load_ohlcv (원본 무변경)

### 변경 Spec
- 신규: load_full_bt_ohlcv(symbol, market_type, start_date=None) — bar_limit 없음, full_bt 로컬
- 확장: run_full_bt_batch / get_full_bt_window_batches(start_date=None)
- 가드: 로드 첫 바 > start_date → FullBtDataGapError + OUTBOX, 실행 중단
- SPOT/FUT: 공통 시그니처(market_type 파라미터), 이번 런은 FUT only

### 인접 CAT 영향
- CAT-C(라이브): 없음 (격리)

### 롤백 조건
- 신규 함수 삭제 + start_date 인자 제거

### Cursor 지시
- targeted diff only, 전체 rewrite 금지
- COUNT 실측 먼저 붙일 것 (② 실런 전 필수)
- 구현 후 세션종료 의무(05_진행로그 / CURSOR_TO_CLAUDE / NEXT_ACTION→WAIT_CLAUDE_OK)

### 위험도
🟡 Medium

---

## [CAT-B] FULL-BT-FUT — 기간 확장 재실행

### sub-phase ID
FULL-BT-FUT-RUN-2

### SSOT (변경 금지 unless noted)
- 파일: `bitget/full_bt/harness.py`, `bitget/full_bt/batch.py`, pilot 실행 스크립트 (RUN-1과 동일 경로)
- config: `FULLBT_DEFCON_BYPASS_ENABLED` (bitget_system_config.sqlite / config_kv) — **false 유지, 변경 없음**

### 변경 Spec
- 정책: RUN-1과 **동일 3심볼(BTC/ETH/SOL)·동일 staging market_db**. 워크 시작점만 staging에 **이미 merged된 최선두 바**로 확장.
- **신규 fetch/merge-write 금지** — 이미 확보된 구간만 재사용 (룰5: 임의 날짜 상수 발명 금지, 반드시 VPS COUNT 조회값 사용).
- 시작일 값: 하드코딩 금지 → **런 직전 staging COUNT 재조회 결과(first bar)를 그대로 인자화**.
- 기존 pilot 스크립트에 시작일 지정 파라미터가 있으면 그대로 사용. 없으면 최소 1개 인자만 추가하고 **시그니처만** Claude에 회신 (구현 전 회신, 긴 코드 금지).
- SPOT/FUT: **FUT only** (RUN-1과 동일, SPOT 확장 아님)

### Config 변경 (있으면)
| KEY | old | new | default |
|-----|-----|-----|---------|
| (없음) | — | — | — |

### 인접 CAT 영향
- CAT-C/D/E (원본 엔진/게이트): 읽기만, diff 없음
- CAT-N (doomsday/execution_safety): 비접촉, bypass=false 유지
- 프로덕션 스키마/paper: 비접촉

### 롤백 조건
- 새 `run_id` 결과만 대상 — 문제 시 해당 run_id 레코드만 무시, **RUN-1 판정 불변**
- 코드 추가 시 해당 diff만 revert (harness/batch 로직 변경 없음 전제)

### Cursor 지시
- Targeted diff only (파라미터/인자 수준). 전체 파일 rewrite 금지.
- **루트 주식 경로 수정 금지** — `bitget/` 하위만.
- 순서: ① staging COUNT 재조회(심볼별 first/last bar) → OUTBOX 기재 ② pilot 실행: start=COUNT의 first bar, 나머지 RUN-1과 동일 ③ 결과: RUN-1과 동일 스키마(call/candidate/trade_count/exception/step2_doomsday/defcon_bypassed/prod today/return/mdd/long_entered/short_entered)
- **금지**: 전체 유니버스, 심볼>3, 신규 OHLCV merge-write/재fetch, 프로덕션 write, bypass on, LIVE/R6/생존·달성 단정, DEPTH 확장
- 테스트: 코드 변경 없으면 pytest 생략 가능(Handoff 허용). 파라미터 추가 시 `pytest bitget/tests/full_bt/`

### 세션 종료 의무
- `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md` — OUTBOX 결과
- `lanes/LANE_FULLBT/NEXT_ACTION.md` → `WAIT_CLAUDE_OK`
- `lanes/LANE_FULLBT/09_쉬운요약.md` 갱신
- 루트 `05_진행로그.md`는 Claude OK 후 append (Cursor는 레인 파일만)

### 위험도
- 🟢 (게이팅/실전 비접촉, staging read-only 확장, IV L1 참고용)

---

## FULL-BT-FUT-RUN-1 — Claude OK · SUB_DONE [2026-08-30]

근거: 체크리스트 6항목 통과 (심볼≤3 · prod=0 · bypass off·태그0 · exception=0 완주 · IV L1 단정 없음 · 코드 diff 없음)  
비차단 확인 1건: engine_hit=3 vs trade_count=4 (심볼별 2회 진입 분포 — 후속 권장, SUB_DONE 비차단)  
판정: **Claude OK → SUB_DONE** · 런 이상 없음

---

## [CAT-D] FULL-BT FUT staging 백테 실런 — bypass 구간한정 · 가용 전체 바 1회 완주

### sub-phase ID
FULL-BT-FUT-RUN-1

### 디렉터 Critical 승인
2026-08-30 — `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md` OUTBOX 원지시("본 OUTBOX = 디렉터 Go 의사")를 승인 문구로 확인. 범위(FUT≤3 심볼, staging 격리, bypass 런 한정, 프로덕션 비접촉)를 벗어나지 않음. 범위 초과 시 재승인 필요.

### SSOT (변경 금지 unless noted)
- 파일: `bitget/full_bt/defcon_bypass.py` (기존, DEFCON-1 산출물 — 로직 수정 금지)
- 실행 진입점: DEFCON-1 재파일럿에 쓰인 harness 호출부 (`run_replay`) — 이전에 걸었던 "구간만" 제한만 해제
- config: `FULLBT_DEFCON_BYPASS_ENABLED` (기존 키, 신규 아님)
- 읽기 전용 참조: CAT-MAP §2 `bitget_forward_trades` writer=CAT-D

### 변경 Spec
- **코드**: 원칙적으로 신규 로직 없음. DEFCON-1 재파일럿에서 쓴 "구간만"(제한된 날짜 범위) 파라미터를 제거하고, staging DB(`bitget_full_bt.sqlite`)에 이미 적재된 BTC/ETH/SOL FUT 전체 바 범위를 harness 기본 완주 방식으로 1회 연속 실행.
- **기간/배치 상한**: 특정 날짜·바 수를 여기서 지정하지 않음(근거 없는 숫자 창조 금지, 룰5). 대신 안전판:
  - 가용 바 범위가 예상보다 크게(예: 수천 바 이상) 나오면 **실행 전** 규모를 확인하고, 자동으로 밀어붙이지 말고 1회 보고 → 그대로 진행할지 재확인 (사전 미승인 초대형 배치 자동완주 금지)
  - 그 외에는 harness 기존 batch 파라미터 그대로, 신규 배치 로직 발명 금지
- **시장/심볼**: FUT only · BTC/ETH/SOL만 (하드코딩 아니라 기존 context 심볼 리스트 재사용)
- **DB**: staging 격리(`bitget_full_bt.sqlite`)만. 프로덕션 `bitget_forward_trades`/OHLCV write 금지 유지
- **bypass 스코프**: 런 시작 직전 `FULLBT_DEFCON_BYPASS_ENABLED=true` → 런 종료 즉시(성공/실패/예외 무관) `false` 복귀. 이 반경 밖에서 true 유지 금지
- **market_type**: 기존 context 필드 그대로 사용 — 하드코딩 분기 신설 금지(CAT-SPOT-FUT §7)
- **run_id**: harness 기존 자동생성 형식 그대로 사용(신규 네이밍 규칙 발명 금지)

### Config 변경
| KEY | old | new | default |
|-----|-----|-----|---------|
| FULLBT_DEFCON_BYPASS_ENABLED | false (상시) | 런 구간만 true | **false** |

### 인접 CAT 영향
- CAT-N: 비접촉 — `execution_safety.py` 실전 게이트 미수정
- CAT-C: 없음 — 스캔/스코어링 로직 불변
- CAT-F: 없음 — Kelly/Treasury 비접촉
- CAT-G: 없음 — 레짐 판정 비접촉
- CAT-B: 없음 — OHLCV 스키마 비접촉, 프로덕션 write 금지 유지

### 롤백 조건
- 런 종료(정상/비정상 무관) 즉시 `FULLBT_DEFCON_BYPASS_ENABLED=false` — 미복귀 시 즉시 사고 취급
- bypass 태그 거래가 production `bitget_forward_trades`로 유입되는 정황 발견 시 → 즉시 flag off + 디렉터 보고
- 가용 바 규모가 예상 밖으로 크게 나와 보고 없이 진행된 경우 → 즉시 중단, 결과 폐기, 디렉터 보고

### OUTBOX 필수 키 (Cursor 보고)
- `run_id` (harness 생성값 그대로)
- 실제 사용된 first/last bar timestamp (Claude가 지정한 값 아님, 실측 그대로)
- call / candidate / trade_count
- bypass 발동 건수 (`defcon_bypassed` 태그 count)
- prod `bitget_forward_trades` entry_date ≥ 런 시작 시각 유입 건수 = **0** (SQL 결과)
- paper equity 요약 (있으면)
- exception 발생 여부 = **0**

### Cursor 지시
- Targeted diff only (구간 제한 파라미터 해제 수준). 전체 파일 rewrite 금지
- **루트 주식 경로 수정 금지** — bitget/ 하위만
- 전체런 금지 · 심볼>3 금지(BTC/ETH/SOL만) · 프로덕션 OHLCV/trades write 금지 · DEPTH-2 혼입 금지 · LIVE/R6/생존·CAGR 단정 금지
- CAT-N/F/G 본체 수정 금지
- 코드 변경이 발생하면(구간 제한 해제 diff): `pytest bitget/tests/full_bt/` + `test_fullbt_defcon_bypass.py` 재실행 필수. 코드 변경 없이 커맨드/config만이면 pytest 생략 가능
- 가용 바 규모 사전 보고 없이 실행 금지 (위 안전판)

### 세션 종료 의무
- `bitget/docs/work_phases/track_b_05_진행로그.md` FULL-BT-FUT-RUN-1 섹션(append)
- `lanes/LANE_FULLBT/NEXT_ACTION.md` · `NEXT_STEP.md` · `09_쉬운요약.md` 갱신(레인 폴더만)
- `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md` 갱신(OUTBOX 결과 키 전부 포함)
- 루트 `NEXT_ACTION.md` → **LANE_FULLBT 행만** `WAIT_CLAUDE_OK`로 upsert

### 위험도
🔴 Critical — paper 안전 게이트 우회 지속 + staging 전체 바 완주(배치 규모 확대). 3중 격리 게이팅 필수, 프로덕션 상시 해제 절대 금지, 런 종료 즉시 flag off 확인 필수.

---

## [CAT-D] FULL-BT-FUT-DEFCON-1 — 재파일럿 검증 OK (SUB_DONE)

Claude OK: 2026-08-30  
판정: 6항목 스펙 충족 (bypass=3, prod=0, FUT≤3, staging, 1회, 단정문구 없음)  
지시: VPS `FULLBT_DEFCON_BYPASS_ENABLED=false` 즉시 복귀  
`_load_bench_close` 노이즈: 비차단, 별건 처리

---

## [CAT-D] FULL-BT 격리 paper DEFCON 우회/격리 — ≤3 심볼 staging 재파일럿 데이터 축적

### sub-phase ID
FULL-BT-FUT-DEFCON-1

### 디렉터 Critical 승인
2026-08-29 — `LANE_FULLBT/CURSOR_TO_CLAUDE.md` OUTBOX 원지시("본 OUTBOX = 디렉터 Go 의사")를 승인 문구로 확인. 본 Handoff는 그 범위(FUT ≤3 심볼, staging 격리, 프로덕션 비접촉)를 벗어나지 않음. 범위 초과 시 재승인 필요.

### SSOT (변경 금지 unless noted)
- 파일(추정, STEP 0에서 확정): `bitget/forward/ledger.py` `try_add_virtual_position(...)` 호출부, `bitget/trading/*_gate.py` 둠스데이 판정 함수
- 읽기 전용 참조: CAT-MAP §2 `bitget_forward_trades` writer=CAT-D, §4 "N ⇢ D,F,G — 실전 gate, paper 경로 분리"(이번에 검증 대상)
- config: `FULLBT_DEFCON_BYPASS_ENABLED` (신규, bitget_system_config.sqlite / config_kv)

### 변경 Spec

**STEP 0 (선행, read-only grep — 같은 세션 1단계, 코드 변경 없음)**
- 목적: paper `try_add` 경로가 CAT-N `execution_safety.py` 실전 게이트와 물리적으로 분리돼 있는지 확인 (CAT-MAP §4 전제 검증 — DIAG-2 "열린질문: paper/N 경계 미확인" 해소)
- 확인 대상: 둠스데이 gate 함수 exact 위치·시그니처, `try_add_virtual_position` 호출부에서 이 함수를 부르는 지점, `step=2`/`detail` 라벨 생성 위치(harness vs gate 자체), FULL-BT harness가 이미 갖고 있는 "격리 컨텍스트" 식별 필드(있으면 재사용, 없으면 보고만 — 발명 금지)
- 산출: grep 결과를 `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md`에 먼저 기록
- **분기**: paper 경로가 CAT-N과 함수/모듈을 공유하면 → STEP 1 착수 금지, Adapter 필요 여부를 디렉터에 재확인 (임의 진행·SSOT 수정 금지, 규칙6)

**STEP 1 (STEP 0 결과로 분리 확인된 경우만 진행)**
- 함수/정책: 신규 wrapper `should_bypass_fullbt_doomsday(context) -> bool` — 기존 gate 함수 본체는 수정 금지, 호출 직전/직후 wrapper만 (CAT-MAP §3 D 허용 인터페이스 준수)
- 게이팅 조건 (AND, 3중 격리 — 하나라도 false면 기존 차단 그대로 유지):
  1. STEP 0에서 확인한 FULL-BT 격리 컨텍스트 플래그 = true
  2. 저장 대상 = FULL-BT 격리 DB(`bitget_full_bt.sqlite` / staging) — production `bitget_forward_trades` 아님
  3. `FULLBT_DEFCON_BYPASS_ENABLED` = true (신규 kill-switch, default **false**)
- SPOT/FUT: **FUT only**(DIAG-2 관측 범위) — SPOT 확장은 별도 Handoff, 이번 스코프 아님
- `market_type`은 기존 context 필드 그대로 사용 — 하드코딩 분기 금지(CAT-SPOT-FUT §7)
- 로깅: bypass 발동 시 `full_bt_diag` 또는 `ops_events`에 `defcon_bypassed=true` 태그, 결과에 "IV L1 참고용" 명시. **LIVE/R6/생존·달성 단정 문구 절대 금지**(규칙11)

### Config 변경
| KEY | old | new | default |
|-----|-----|-----|---------|
| FULLBT_DEFCON_BYPASS_ENABLED | (없음) | 신규 | false |

### 인접 CAT 영향
- CAT-N: 비접촉 — `execution_safety.py` 실전 게이트 미수정, 상시 해제 아님
- CAT-C: 없음 — 스캔/스코어링 로직 불변
- CAT-F: 없음 — Kelly/Treasury 비접촉
- CAT-B: 없음 — OHLCV 스키마 비접촉, 프로덕션 write 금지 유지

### 롤백 조건
- `FULLBT_DEFCON_BYPASS_ENABLED=false` → 즉시 기존 차단 복귀 (3중 조건 중 1개만 꺼도 동일 효과)
- bypass 태그 거래가 production `bitget_forward_trades`로 유입되는 정황 발견 시 → 즉시 flag off + 디렉터 보고(사고 취급)

### Cursor 지시
- STEP 0 grep 결과 기록 없이 STEP 1 코드 착수 금지
- Targeted diff only. 전체 파일 rewrite 금지
- **루트 주식 경로 수정 금지** — bitget/ 하위만
- 전체런 금지 · 심볼>3 금지(BTC/ETH/SOL만) · 프로덕션 OHLCV write 금지 · DEPTH-2 혼입 금지 · LIVE/R6/생존 단정 금지
- 충돌 시(예: paper 경로가 CAT-N과 공유) Adapter 제안 후 디렉터 Ask — 임의 SSOT 수정 금지
- 테스트: `pytest bitget/tests/full_bt/` + 신규 `test_fullbt_defcon_bypass.py` — 3중 게이팅 각 단독 실패 시 기존 차단 유지 검증 필수

### 세션 종료 의무
- `bitget/docs/work_phases/track_b_05_진행로그.md` FULL-BT-FUT-DEFCON-1 섹션 (append)
- `lanes/LANE_FULLBT/NEXT_ACTION.md` · `NEXT_STEP.md` · `09_쉬운요약.md` 갱신 (레인 폴더만)
- `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md` 갱신 (STEP 0 grep 결과 포함)
- 루트 `NEXT_ACTION.md` → **LANE_FULLBT 행만** `WAIT_CLAUDE_OK`로 upsert

### 위험도
🔴 Critical — paper 안전 게이트 우회. 3중 격리 게이팅 필수, 프로덕션 상시 해제 절대 금지.

---

## [CAT-Q] FULL-BT-FUT-DIAG-2 검증 OK · 분기=구조적→CAT-D

> **작성**: Claude Pro · 2026-08-29 · [FULL-BT-FUT-DIAG-2]  
> **판정**: **OK** · **구조적** (우연 B안 **기각**) · ops 재확인·≤3 DIAG-on **불필요**  
> **Cursor**: 코드·재실행·config **변경 없음** · CAT-D 착수 **금지** (별 Handoff + 디렉터 승인)

(이하 DIAG-2/1 보관 — 생략 없이 파일에 유지됨; STEP 0 분기 후 본 파일 상단만 DEFCON-1)
