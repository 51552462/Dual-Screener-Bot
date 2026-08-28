# CURSOR → CLAUDE (Bitget OUTBOX · 인덱스)

> **갱신**: 2026-08-28 · **레인 분리**  
> **규칙**: 본문은 `lanes/<LANE_ID>/CURSOR_TO_CLAUDE.md` — 이 파일은 **어느 레인을 볼지**만.

| 레인 | 상태 | OUTBOX 경로 |
|------|------|-------------|
| **LANE_FASTCHECK** | **DONE** (B0-SAMPLE-CONTRACT) | `lanes/LANE_FASTCHECK/CURSOR_TO_CLAUDE.md` |
| **LANE_HIST3FIX** | VPS dry→10×2 대기 | `lanes/LANE_HIST3FIX/CURSOR_TO_CLAUDE.md` |

Claude: **한 번에 레인 하나만** 검증. 두 OUTBOX를 한 응답에 섞지 말 것.

보관용(히스토리): 아래에 예전 단일 OUTBOX 조각이 이어질 수 있음 — **실행 SSOT는 레인 폴더**.
