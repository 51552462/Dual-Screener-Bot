# CLAUDE_TO_CURSOR — Independent Verification INBOX

> **용도**: Claude Pro → Cursor Handoff (V-* sub-phase)  
> **형식**: `docs/claude_project/CAT-HANDOFF_템플릿.md`  
> **규칙**: sub-phase **하나** · targeted diff · 세션 종료 시 `05_진행로그` IV 섹션 + 본 폴더 `NEXT_ACTION`

---

## INBOX — 발사 대기 (전송 금지)

> **상태**: `WAIT_READY` · **발사 조건 2개 모두** 충족 후에만 Cursor 구현 세션으로 전송  
> 1) `assess_v2_readiness() = READY` (9/6+ `--iv-observation --dry-run` 재확인)  
> 2) 디렉터 명시 **"가자"**  
> 스코프 확정 2026-09-04: **WF block만** · DSR/V-2b는 backlog

---

### [준비 완료 · 발사 대기] V-2-WFBLOCK-01

```text
[준비 완료 · 발사 대기 — 9/6 readiness=READY 재확인 + 디렉터 Go 후에만 전송]

sub-phase: V-2-WFBLOCK-01
위험도: 🔴 Critical (LIVE 승격 경로 최초 실제 차단)
전제: assess_v2_readiness() = READY (9/6 이후 재확인) · 디렉터 명시 승인

## 목표 (한 줄)
WALK_FORWARD_PROMOTION_BLOCK_ENABLED=1 로 켜서,
WF WARN이 걸린 후보가 CANDIDATE→LIVE로 자동 승격되는 걸 실제로 막는다.
DSR 연동은 이번 스코프 아님 (별도 sub-phase).

## Spec
1) env WALK_FORWARD_PROMOTION_BLOCK_ENABLED=1 적용
2) 적용 경로 4곳 회귀 확인 (이미 배선 확인됨, 새 코드 아님):
   - CANDIDATE→LIVE (live_hard_gate)
   - fast-track→LIVE
   - re-evolution 부활→LIVE
   - COOLED 복귀 경로
3) 정상 케이스(WF WARN 없는 후보) 승격이 그대로 되는지 회귀 확인 — 오탐 차단 없어야 함
4) wf_promotion_blocked 카운터가 실제 WARN 케이스에서 증가하는지 확인

## 하지 않을 일
- DSR 연동 시도 금지 (evaluate_ledger_deflated_sharpe 호출 추가 금지 — 별도 sub-phase)
- V-2b config_snapshot 관련 코드 추가 금지
- OOS_DSR_MIN 값 변경 금지

## 롤백
- env WALK_FORWARD_PROMOTION_BLOCK_ENABLED=0 즉시 원복 (문서 SSOT 명시된 방법)

## 완료 기준 (DoD)
1. env=1 적용 확인
2. 4개 경로 전부 정상 차단 동작 확인 (WARN 케이스로)
3. 정상 승격 케이스 회귀 없음 확인
4. wf_promotion_blocked 카운터 증가 실측
5. 배포 후 1주 관측: 오탐(정상 후보가 잘못 막힘) 발생 여부

## 세션 종료 시 필수
- 05_진행로그.md · 00_전체현황판.md 갱신
- IV 쪽 05_갭_및_로드맵.md V-2 상태도 갱신
```

---

## Backlog (미착수 · 순서만)

| ID | 내용 | 선행 |
|----|------|------|
| **V-2-DSR-01** | DSR을 승격 엔진에 연결 (`evaluate_ledger_deflated_sharpe` 배선 · threshold) | V-2-WFBLOCK-01 안정 후 |
| **V-2B-SNAPSHOT-01** | 승격 시점 `config_snapshot_json` 동결 | V-2-WFBLOCK-01 후 · 별도 |

---

## 완료 Handoff 아카이브

### V-1 — Reality Audit + WF WARN meta (2026-08-09)

- **상태**: Cursor 구현 완료 · Claude OK 대기
- **산출물**: `deploy_watch.reality_audit_check` · `strategy_promotion_engine` `meta.wf_warn`
- **검증**: `CURSOR_TO_CLAUDE.md` §V-1

### V-0 — SSOT 폴더 (2026-08-09)

- **상태**: Claude OK 대기 (V-0 단독)
- **산출물**: `docs/independent_verification/*`

---

*status: `NEXT_ACTION.md`*
