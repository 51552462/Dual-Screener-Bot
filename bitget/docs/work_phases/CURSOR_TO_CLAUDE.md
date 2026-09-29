# CURSOR → CLAUDE (Bitget OUTBOX · 인덱스)

> **갱신**: 2026-09-29 · **CAT-L-FENCE-02 Step 3 B 재개** · WAIT_CLAUDE_OK  
> **규칙**: 레인 본문=`lanes/<LANE_ID>/CURSOR_TO_CLAUDE.md` · CAT-A/L 본문=`track_b_CURSOR_TO_CLAUDE.md`

| 레인 | 상태 | OUTBOX 경로 |
|------|------|-------------|
| **CAT-A (공용)** | **ENFORCE_LIVE · WAIT_FIRST_KILL_CONFIRM** | `track_b_CURSOR_TO_CLAUDE.md` |
| **CAT-F (공용)** | **A5-EVENTLOG-01 구현 OK · 서버 반영 확인 대기** | `track_b_CURSOR_TO_CLAUDE.md` |
| **CAT-L (공용)** | **Step 3 B 재개** · heavy.slice 실스캔 · FENCE_OK · WAIT_CLAUDE_OK | `track_b_CURSOR_TO_CLAUDE.md` |
| **LANE_FASTCHECK** | **DONE** (B0-SAMPLE-CONTRACT) | `lanes/LANE_FASTCHECK/CURSOR_TO_CLAUDE.md` |
| **LANE_HIST3FIX** | **DONE** (HIST-3-FIX) | `lanes/LANE_HIST3FIX/CURSOR_TO_CLAUDE.md` |
| **LANE_FULLBT** | **WAIT_CLAUDE_OK** (RUN-2 `pilot-fut-20260927T040953Z`) | `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md` |

Claude: `track_b_CURSOR_TO_CLAUDE.md` Step 3 B 재개. 채팅 말고 파일.

보관용(히스토리): 아래에 예전 단일 OUTBOX 조각이 이어질 수 있음 — **실행 SSOT는 레인 폴더**.
