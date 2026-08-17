# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **SIDE-ALPHA-01** |
| **status** | `WAIT_CURSOR_IMPL` — **1단계 진단만** (trade-level breakdown) |
| **Handoff** | [`CLAUDE_TO_CURSOR.md`](CLAUDE_TO_CURSOR.md) §SIDE-ALPHA-01 |
| **앵커** | `SYNC-2026-08-17-A` |

---

## 디렉터 — 지금 할 일

```text
Cursor 새 채팅: SIDE-ALPHA-01 1단계 진단만.
docs/work_phases/CLAUDE_TO_CURSOR.md §SIDE-ALPHA-01 유일 Handoff.
BULL-RECENCY 재현 full 금지 · 8/13 JSON SSOT 유지.
```

---

## Cursor — 지금 할 일 (SIDE-ALPHA-01 · 1단계만)

1. 새 세션 = 본 Handoff **하나**
2. SIDE_02 · SIDE_03 trade-level breakdown (WR · avg_pnl · exit_type · 보유기간)
3. 공통원인 vs 개별원인 → `CURSOR_TO_CLAUDE` OUTBOX
4. **2단계 조정·15구간 rerun은 진단 Claude OK 전 금지**

### 금지 (Handoff)

- BULL_03/05 bounds 재접촉
- BULL-RECENCY 재현 full
- Phase A · BEAR · C-1 · S5 · config_kv 라이브
- fix→rerun 2회차

---

## BULL-RECENCY-01 (종료 · 부분 Done)

| 항목 | 값 |
|------|-----|
| **SSOT** | `rp1_bull_recency_01_20260813.json` |
| **BULL_03** | NEAR_MISS · 15.40% · Done |
| **BULL_05** | FAIL · KR 레버 코드 동결(재시도 안 함) |
| **Claude VERDICT** | 2026-08-17 OK · 부분 Done |

---

## Cursor 새 채팅 부팅 (복붙)

```text
Track A 구현 — SIDE-ALPHA-01 1단계 진단만.
docs/work_phases/00_SESSION_SYNC.md §3 → NEXT_ACTION → CLAUDE_TO_CURSOR.md §SIDE-ALPHA-01.
코드 조정·full rerun은 진단 OK 전 금지.
```
