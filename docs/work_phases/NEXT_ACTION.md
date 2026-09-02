# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **NAV-REPLAY-BACKFILL-01** Step B |
| **status** | **`CLOSED`** / `WAIT_CLAUDE_OK` · NAV 정정·LOCKDOWN 정상 · INCUBATOR skip 코드 로컬 반영 |
| **직전** | Step A 확정값대로 VPS treasury 반영 확인 · governor sync |
| **앵커** | `SYNC-2026-09-02-NAV-REPLAY-B` |

---

## 디렉터 — 지금 할 일

1. Claude Pro: `docs/work_phases/CURSOR_TO_CLAUDE.md` 최상단 OUTBOX 검증 → OK면 다음 Handoff(또는 없음)를 파일에.
2. 로컬 커밋·푸시 후 VPS 배포 (INCUBATOR skip 반영):

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo bash ./update_factory.sh
```

### 메모

- KR `BLOCK_NEW_ENTRIES_KR=True` (LOCKDOWN) = **의도된 정상** — 우회 금지
- backlog **`KR-LOCKDOWN-LADDER-01`**: Track A 축소 재개 사다리 — **디렉터 승인 전 미착수**
- 실매매 OFF · 페이퍼 MDD breach만 기록됨 (실자본 피해 없음)

### 금지

- HWM 리셋/덮어쓰기 · mdd 축소 · LOCKDOWN 완화 코드 · 회복 사다리 임의 설계
