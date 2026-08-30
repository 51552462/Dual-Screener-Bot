# CLAUDE → CURSOR · LANE_FULLBT

> **레인**: `LANE_FULLBT`  
> **갱신**: 2026-08-29  
> **status**: **WAIT_CLAUDE_OK** (Adapter A 구현 · 선차단 해소 회신 후 재검증)

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
