# CLAUDE_TO_CURSOR — Independent Verification INBOX

> **용도**: Claude Pro → Cursor Handoff (V-* sub-phase)  
> **형식**: `docs/claude_project/CAT-HANDOFF_템플릿.md`  
> **규칙**: sub-phase **하나** · targeted diff · 세션 종료 시 `05_진행로그` IV 섹션 + 본 폴더 `NEXT_ACTION`

---

## INBOX — V-2-WFBLOCK-01 발사됨 (2026-09-10)

> **상태**: **Claude OK 2026-09-10** · CLOSED · 배포 + 1주 오탐 추적  
> 전제: READY (2026-09-09) + 디렉터 「가자」 (2026-09-10)  
> 사인 SSOT: `docs/work_phases/CLAUDE_TO_CURSOR.md` 최상단 INBOX

원 Handoff 본문(스펙)은 아래 아카이브와 동일. 구현 결과는 work_phases OUTBOX.

---

## Backlog (미착수 · 순서만)

| ID | 내용 | 선행 |
|----|------|------|
| **V-2-DSR-01** | DSR을 승격 엔진에 연결 (`evaluate_ledger_deflated_sharpe` 배선 · threshold) | V-2-WFBLOCK-01 안정 후 |
| **V-2B-SNAPSHOT-01** | 승격 시점 `config_snapshot_json` 동결 | V-2-WFBLOCK-01 후 · 별도 |

---

## 완료 Handoff 아카이브

### V-2-WFBLOCK-01 — WF LIVE block env ON (2026-09-10 발사)

- **상태**: **Claude OK 2026-09-10** · CLOSED
- **적용**: 팩토리 엔트리포인트 `WALK_FORWARD_PROMOTION_BLOCK_ENABLED` 기본 1
- **검증**: `CURSOR_TO_CLAUDE.md` (work_phases + IV)

### V-1 — Reality Audit + WF WARN meta (2026-08-09)

- **상태**: Cursor 구현 완료 · Claude OK 대기
- **산출물**: `deploy_watch.reality_audit_check` · `strategy_promotion_engine` `meta.wf_warn`
- **검증**: `CURSOR_TO_CLAUDE.md` §V-1

### V-0 — SSOT 폴더 (2026-08-09)

- **상태**: Claude OK 대기 (V-0 단독)
- **산출물**: `docs/independent_verification/*`

---

*status: `NEXT_ACTION.md`*
