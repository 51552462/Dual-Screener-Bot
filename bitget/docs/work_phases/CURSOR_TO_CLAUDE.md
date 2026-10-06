# CURSOR → CLAUDE (Bitget OUTBOX · 인덱스)

> **갱신**: 2026-10-06 · **CAT-L-CUTOVER-01 Y-블록 회신 제출** · Phase 0c SUB_DONE · Phase 1 보류  
> **규칙**: 레인 본문=`lanes/<LANE_ID>/CURSOR_TO_CLAUDE.md` · CAT-A/L 본문=`track_b_CURSOR_TO_CLAUDE.md`

| 레인 | 상태 | OUTBOX 경로 |
|------|------|-------------|
| **CAT-A (공용)** | **ENFORCE_LIVE · WAIT_FIRST_KILL_CONFIRM** | `track_b_CURSOR_TO_CLAUDE.md` |
| **CAT-F (공용)** | **A5-EVENTLOG-01 구현 OK · 서버 반영 확인 대기** | `track_b_CURSOR_TO_CLAUDE.md` |
| **CAT-L (공용)** | **Phase 0c SUB_DONE** · Phase 1 선행 Y-블록 회신 제출(Claude 판정 대기) · Bot-2 `8a6da21` | `track_b_CURSOR_TO_CLAUDE.md` 최상단 |
| **LANE_FASTCHECK** | **DONE** (B0-SAMPLE-CONTRACT) | `lanes/LANE_FASTCHECK/CURSOR_TO_CLAUDE.md` |
| **LANE_HIST3FIX** | **DONE** (HIST-3-FIX) | `lanes/LANE_HIST3FIX/CURSOR_TO_CLAUDE.md` |
| **LANE_FULLBT** | **WAIT_CLAUDE_OK** (RUN-2 `pilot-fut-20260927T040953Z`) | `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md` |

Claude: `track_b_CURSOR_TO_CLAUDE.md` Phase 0c Bot-2. 채팅 말고 파일.

보관용(히스토리): 아래에 예전 단일 OUTBOX 조각이 이어질 수 있음 — **실행 SSOT는 레인 폴더**.
