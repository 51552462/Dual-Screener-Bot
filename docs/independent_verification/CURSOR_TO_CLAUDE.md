# CURSOR_TO_CLAUDE — Independent Verification OUTBOX

> **용도**: Cursor → Claude Pro 검증 요청  
> **규칙**: 3줄 요약 + 본 파일 해당 섹션 + (선택) diff · Claude OK 전 V-* Done 금지

---

## OUTBOX — V-1 Reality Audit + WF WARN meta (2026-08-09)

### sub-phase
**V-1** — `deploy_watch.reality_audit_check` + `strategy_promotion_engine` `meta.wf_warn`

### 변경 요약
1. `deploy_watch.py`: `reality_audit_check()` — CLOSED row 완전성 (KR/US `market` 분기) · `run_deploy_watch` checks에 추가
2. `validation/walk_forward.py`: `evaluate_oos_pass_from_returns()` (KR/US WF OOS)
3. `strategy_promotion_engine.py`: `meta.wf_warn` 태그 · `notify_wf_warn_telegram` · 승격 로직 **무변경**
4. env 롤백: `REALITY_AUDIT_CHECK_ENABLED` · `WF_WARN_TAG_ENABLED` · `WF_WARN_TELEGRAM_ENABLED` (default on)

### 테스트
`pytest tests/test_deploy_watch_l_obs_01.py tests/test_v1_wf_warn_meta.py` — **18 passed**

### 스펙 일치 확인 요청
- [ ] reality audit이 CAT-E-BARS-01 SQL (a)(c) 의도와 일치
- [ ] BLOCK 없음 · `WALK_FORWARD_PROMOTION_BLOCK_ENABLED` 미변경
- [ ] LIVE 승격 경로 회귀 없음 (`test_v1_wf_warn_meta` lifecycle 케이스)
- [ ] IV-21 · IV-04 WARN 측정 단계 충족

### 디렉터 3줄 요약
1. deploy_watch에 reality audit check 추가 — CLOSED 결측·bad exit_type 비율로 WARN/BREAK.
2. promotion cycle 끝에 WF OOS fail 그룹에 `meta.wf_warn=true`만 기록 — LIVE 차단 없음.
3. 18 tests passed · V-2 전 2주 WARN 오탐률 관측 권장.

### Claude OK 대기
- status: `WAIT_CLAUDE_OK`

---

## OUTBOX — V-0 SSOT 폴더 생성 (2026-08-09)

### sub-phase
**V-0** — `docs/independent_verification/` 독립 검증 SSOT

### 변경 요약
1. 신규 폴더 10파일: README·헌법·체크리스트 25항·KR/US/BG 매트릭스·코드맵·로드맵·IV 06·NEXT_ACTION·Handoff IN/OUT
2. `work_phases/README.md` · `claude_project/00_README` 에 링크
3. `05_진행로그.md` IV 섹션 추가

### 스펙 일치 확인 요청
- [ ] 6대 위험 + 추가 19항 누락 없음 (`02_편향_체크리스트`)
- [ ] L0/L1/L2/L3 층 정의가 RP-1·G2·WF shadow와 모순 없음
- [ ] V-* 로드맵이 work_phases B-3·D-3·F-GATE와 충돌 없음
- [ ] Track B R3·funding 항목 반영

### 디렉터 3줄 요약
1. 검증 SSOT를 `docs/independent_verification/` 단일 폴더로 고정 — 채널 엇갈림 방지.
2. IV-01~25 체크리스트 + V-0~V-4 로드맵 — 자기채점 구멍 문서화.
3. 다음: L1 배포 유지 후 V-1 (reality audit + WF WARN) Handoff.

### Claude OK 대기
- status: `WAIT_CLAUDE_OK`

---

*이전 OUTBOX: (없음)*
