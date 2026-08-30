# NEXT_ACTION — Bitget (레인 대시보드)

> **본문 진실은 레인 폴더.** 표만 upsert · 다른 레인 행 삭제 금지.

| 레인 | sub-phase | status | 창이 쓸 파일 |
|------|-----------|--------|--------------|
| **LANE_FASTCHECK** | B0-SAMPLE-CONTRACT | **DONE** | `lanes/LANE_FASTCHECK/*` |
| **LANE_HIST3FIX** | FULL-BT-HIST-3-FIX | **DONE** | `lanes/LANE_HIST3FIX/*` |
| **LANE_FULLBT** | FULL-BT-FUT-DEFCON-1 | **WAIT_CURSOR_VPS** | `lanes/LANE_FULLBT/*` |

---

## 디렉터 한 줄

**FULLBT** — Claude OK 최종(2026-08-30). VPS: bypass=true · FUT≤3 staging 재파일럿 → 결과 OUTBOX.
