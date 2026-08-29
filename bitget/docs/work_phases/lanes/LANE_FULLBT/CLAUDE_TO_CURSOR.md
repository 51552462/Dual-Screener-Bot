# CLAUDE → CURSOR · LANE_FULLBT

> **레인**: `LANE_FULLBT`  
> **갱신**: 2026-08-29  
> **status**: WAIT_CURSOR_IMPL (DIAG-2)

---

## [CAT-C] FULL-BT FUT reject 원인 원문 확보 — VPS read-only 조회

### sub-phase ID
FULL-BT-FUT-DIAG-2

### SSOT (변경 금지)
- 조회만: VPS `bitget_full_bt.sqlite` (또는 실제 경로) `full_bt_diag`, `ops_events`
- 코드 변경 없음 · 신규 파일 없음

### 변경 Spec
- 실행(읽기 전용, write 금지):
  `SELECT symbol, step, detail FROM full_bt_diag WHERE run_id='pilot-fut-20260829T062221Z' AND metric='gate_reject'`
  + `ops_events WHERE event='fullbt_candidate_reject' AND run_id='pilot-fut-20260829T062221Z'` (DIAG-1 배포 후 재현분 있으면)
- 이미 구현된 `retag_rejects_from_full_bt_diag`는 VPS에 diag row 있을 때만 사용 가능 — 신규 아님
- **둘 다 0건이면**: 재실행하지 말고 그대로 "VPS도 0건" 보고 후 대기. 재파일럿(≤3, DIAG-on)은 다음 Claude Handoff에서 별도 지시

### 인접 CAT 영향
- CAT-D/N: 비접촉(조회만)

### 롤백 조건
- 해당 없음 (쓰기 없음)

### Cursor 지시
- write/재실행 금지 · 원문(symbol/step/detail 또는 msg) 가공·해석 없이 그대로 보고
- 테스트: 해당 없음(코드 변경 없음)

### 세션 종료 의무
- `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md` 갱신 (원문 표)
- `lanes/LANE_FULLBT/NEXT_ACTION.md` · `09_쉬운요약.md` 갱신
- `ACTIVE_LANES.md` LANE_FULLBT 행만 status → `WAIT_CLAUDE_OK`

### 위험도
🟢 Low — read-only, 쓰기 없음

---

## Claude OK — FULL-BT-FUT-DIAG-1 [2026-08-29]

판정: **OK**. 스펙 이탈·Adapter 필요 없음.  
Q2: VPS 조회 먼저 → 0건이면 ≤3 DIAG-on 재파일럿(별 Handoff).  
Q3: reject_msg 원문 전 CAT-D/B 분기 금지.

---

## [CAT-C] FULL-BT FUT candidate→trade 0건 — reject 원인 read-only 태그

### sub-phase ID
FULL-BT-FUT-DIAG-1

### A안 채택 이유 (1줄)
candidate=3·hit=reject=3·trade=0 — 이미 3/3 전부 try_add 단계에서 거절됨. 원인 모른 채 표본만 늘리면(B) 같은 거절이 반복될 뿐이라 진단(A) 먼저.

### SSOT (변경 금지 unless noted)
- 신규 파일만 추가: `bitget/observability/fullbt_candidate_diag_bg.py`
- 읽기 전용 참조(수정 금지): CAT-D `try_add_virtual_position(...)` 호출부/반환 시그니처 — 정확한 위치는 Cursor가 grep으로 확인 (`forward/ledger.py` 추정, 확정은 로컬 확인)
- config: `FULLBT_CANDIDATE_DIAG_ENABLED` (신규 kill-switch, bitget_system_config.sqlite / config_kv)

### 변경 Spec
- 함수: `tag_candidate_reject_reason(run_id, symbol, market_type, try_add_result) -> None`
- 정책: `try_add_virtual_position(...)` **호출 후**(CAT-MAP §3 허용 인터페이스) 이미 반환되는 값만 읽어 `ops_events`에 기록. **새 거절사유 코드 발명 금지** — 기존 반환 필드 그대로 태그.
- 만약 현재 반환 시그니처에 거절 사유 필드가 **없으면** → 코드 추가하지 말고 `CURSOR_TO_CLAUDE`에 충돌 보고(Adapter 필요) 후 대기. 임의로 사유 추정/생성 금지.
- SPOT/FUT: FUT only (이번 스코프) · `market_type`은 candidate 컨텍스트 값 그대로 전달 — 하드코딩 금지 (CAT-SPOT-FUT §7)

### Config 변경
| KEY | old | new | default |
|-----|-----|-----|---------|
| FULLBT_CANDIDATE_DIAG_ENABLED | (없음) | 신규 | true |

### 인접 CAT 영향
- CAT-D: **읽기만** — try_add 내부 로직/리턴 시그니처 변경 없음 (CAT-MAP §3 교차수정 금지 준수)
- CAT-N: **비접촉** — paper 경로는 N 실전 게이트 미경유(CAT-MAP §4) 재확인 필요 항목으로만 기록, 코드 접촉 없음
- CAT-C: 후보 호출부 직후 hook 추가만, 스캔/스코어링 로직 불변

### 롤백 조건
- `FULLBT_CANDIDATE_DIAG_ENABLED=false` → 즉시 no-op, 기존 파일럿 동작 완전 복귀

### Cursor 지시
- Targeted diff only. 전체 파일 rewrite 금지.
- **루트 주식 경로 수정 금지** — bitget/ 하위만.
- 1차: 기존 run `pilot-fut-20260829T062221Z` 로그/DB **재분석 우선** (재실행 없이). 부족할 때만 심볼≤3 재실행(디렉터 승인 범위 내, 전체런/심볼>3/프로덕션 write 금지).
- 다중 TF(DEPTH-2)는 이번 요청과 분리 — 착수 금지.
- 충돌 시 Adapter 제안 후 디렉터 Ask.
- 테스트: `pytest bitget/tests/test_fullbt_candidate_diag.py` (신규)

### 세션 종료 의무
- `bitget/docs/work_phases/track_b_05_진행로그.md` FULL-BT-FUT-DIAG-1 섹션 (append)
- `lanes/LANE_FULLBT/NEXT_ACTION.md` · `NEXT_STEP.md` · `09_쉬운요약.md` 갱신 (레인 폴더만)
- `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md` 갱신
- 루트 `NEXT_ACTION.md` → **LANE_FULLBT 행만** `WAIT_CLAUDE_OK`로 upsert

### 위험도
🟢 Low — read-only, CAT-D/N 비접촉, kill-switch 즉시 off

Mirror — 2026-08-29 · [CAT-C] · FULL-BT-FUT-DIAG-1 Handoff 발행
