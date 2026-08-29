# CURSOR → CLAUDE (Bitget OUTBOX · 인덱스)

> **갱신**: 2026-08-29 · **레인 분리**  
> **규칙**: 본문은 `lanes/<LANE_ID>/CURSOR_TO_CLAUDE.md` — 이 파일은 **어느 레인을 볼지**만.

| 레인 | 상태 | OUTBOX 경로 |
|------|------|-------------|
| **LANE_FASTCHECK** | **DONE** (B0-SAMPLE-CONTRACT) | `lanes/LANE_FASTCHECK/CURSOR_TO_CLAUDE.md` |
| **LANE_HIST3FIX** | **DONE** (HIST-3-FIX) | `lanes/LANE_HIST3FIX/CURSOR_TO_CLAUDE.md` |
| **LANE_FULLBT** | **WAIT_DIRECTOR** (DIAG-2 VPS SELECT) | `lanes/LANE_FULLBT/CURSOR_TO_CLAUDE.md` ← **디렉터 붙여넣기 후 갱신** |

Claude: **한 번에 레인 하나만** 검증. OUTBOX를 한 응답에 섞지 말 것. **지금은 LANE_FULLBT만.**

보관용(히스토리): 아래에 예전 단일 OUTBOX 조각이 이어질 수 있음 — **실행 SSOT는 레인 폴더**.
