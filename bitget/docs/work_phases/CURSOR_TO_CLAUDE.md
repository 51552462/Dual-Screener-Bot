# CURSOR → CLAUDE (Bitget OUTBOX · 인덱스)

> **갱신**: 2026-09-27 · **CAT-L-FENCE-02 Step 2 APPLIED · WAIT_CLAUDE_OK**  
> **규칙**: 레인 본문=`lanes/<LANE_ID>/CURSOR_TO_CLAUDE.md` · CAT-A/L 본문=`track_b_CURSOR_TO_CLAUDE.md`

| 레인 | 상태 | OUTBOX 경로 |
|------|------|-------------|
| **CAT-A (공용)** | **ENFORCE_LIVE · WAIT_FIRST_KILL_CONFIRM** | `track_b_CURSOR_TO_CLAUDE.md` |
| **CAT-F (공용)** | **A5-EVENTLOG-01 DEPLOYED · WAIT_2~4W** | `track_b_CURSOR_TO_CLAUDE.md` 상단 |
| **CAT-L (공용)** | **CAT-L-FENCE-02 Step 2 APPLIED · WAIT_CLAUDE_OK** (실스캔 cgroup 16:01 UTC) | `track_b_CURSOR_TO_CLAUDE.md` |
| **LANE_FASTCHECK** | **DONE** (B0-SAMPLE-CONTRACT) | `lanes/LANE_FASTCHECK/CURSOR_TO_CLAUDE.md` |
| **LANE_HIST3FIX** | **DONE** (HIST-3-FIX) | `lanes/LANE_HIST3FIX/CURSOR_TO_CLAUDE.md` |
| **LANE_FULLBT** | **WAIT_CLAUDE_OK** (RUN-2 `pilot-fut-20260927T040953Z`) | `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md` |

Claude: `track_b_CURSOR_TO_CLAUDE.md` Step 2 적용+프로브 cgroup. 실스캔 완주는 16:01 UTC `scan_spot_ema5_r2`. 채팅 말고 파일.

보관용(히스토리): 아래에 예전 단일 OUTBOX 조각이 이어질 수 있음 — **실행 SSOT는 레인 폴더**.
