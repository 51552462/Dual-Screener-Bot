# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **S5-HARNESS-SCOPE-01** (VPS 실측) |
| **status** | `WAIT_CLAUDE_OK` |
| **git / VPS** | origin·VPS **`600c9cd`** |
| **앵커** | `SYNC-2026-08-17-Q` |

---

## 디렉터 — 지금 할 일

1. Claude에 `docs/work_phases/CURSOR_TO_CLAUDE.md` 최상단 OUTBOX 검증 요청.
2. VPS 실측 요약: KR/US n=0 · `short_pnl_column_present=false` · 게이트 활성분 0 (Pass/Fail 아님).
3. OPS cron·phase `post_bear_underdog_01` 유지.

```
docs/work_phases/CURSOR_TO_CLAUDE.md 검증. OK면 CLAUDE_TO_CURSOR.md에 다음 Handoff. 채팅 말고 파일에.
```

---

## S5 실측 스냅샷

| market | n | gate_min | short_pnl |
|--------|---|----------|-----------|
| KR | 0 | 0 | false |
| US | 0 | 0 | false |

경로: VPS `reports/s5_defense/s5_contribution_20260817.json`

---

## BEAR / SIDE / BULL

- 동결 · 재시도 금지
