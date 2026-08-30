# ACTIVE_LANES — Bitget 열린 창(레인) 표

> **규칙**: 각 Cursor 창은 **자기 행만** 수정. 다른 행 삭제·덮어쓰기 금지.  
> **SSOT 본문**: `lanes/<LANE_ID>/`  
> **프로토콜**: `../16_멀티창_레인_프로토콜.md`

| LANE_ID | sub-phase | status | 창 용도 | 마지막 갱신 | OUTBOX |
|---------|-----------|--------|---------|-------------|--------|
| **LANE_FASTCHECK** | B0-SAMPLE-CONTRACT | **DONE** (Claude OK 2026-08-28) | 표본 계약 승인 · R2 관측 | 2026-08-28 | `LANE_FASTCHECK/CURSOR_TO_CLAUDE.md` |
| **LANE_HIST3FIX** | FULL-BT-HIST-3-FIX | **DONE** (Claude OK · 백필 PARK) | FULL-BT warmup 좁은 수정 | 2026-08-28 | `LANE_HIST3FIX/CURSOR_TO_CLAUDE.md` |
| **LANE_FULLBT** | FULL-BT-FUT-RUN-2 | **WAIT_CURSOR_VPS** (COUNT) | 기간 확장 재실행 | 2026-08-31 | `LANE_FULLBT/CURSOR_TO_CLAUDE.md` |

## 부팅 한 줄 (창에 붙이기)

**FASTCHECK 창**
```
레인: LANE_FASTCHECK · B0-SAMPLE-CONTRACT DONE · 관측만 · lanes/LANE_FASTCHECK/ 만 쓰기
```

**HIST-3-FIX 창**
```
레인: LANE_HIST3FIX · FULL-BT-HIST-3-FIX DONE · 관측/재개 시 레인만 · 새 Handoff 전 구현 금지
```

**FULLBT 창**
```
레인: LANE_FULLBT · RUN-2 · WAIT_CURSOR_VPS COUNT · bypass off · lanes/LANE_FULLBT/ 만
```
