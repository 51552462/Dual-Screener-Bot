# NEXT_ACTION — Bitget (레인 대시보드)

> **본문 진실은 레인 폴더.** 표만 upsert · 다른 레인 행 삭제 금지.

| 레인 | sub-phase | status | 창이 쓸 파일 |
|------|-----------|--------|--------------|
| **CAT-A (공용)** | A-LIFECAP-01 | **ENFORCE_LIVE · WAIT_FIRST_KILL_CONFIRM** | `track_b_*` · OUTBOX=`track_b_CURSOR_TO_CLAUDE.md` |
| **CAT-L (공용·레인 아님)** | CAT-L-FENCE-02 Step 4E | **WAIT_CLAUDE_OK** (설치 반영 · FENCE_OK · grep -c 주석 오탐 기록) | `track_b_*` · OUTBOX=`track_b_CURSOR_TO_CLAUDE.md` |
| **CAT-F (공용)** | A5-EVENTLOG-01 | **구현 OK · 서버 반영 확인 대기** (적재 시계는 반영 확인일부터) | `track_b_*` |
| **CAT-L (에스컬레이션)** | FENCE-02 펜스 복구 | **WAIT_CLAUDE_OK** | live wrapped jobs 28 · FENCE_OK |
| **LANE_FASTCHECK** | B0-SAMPLE-CONTRACT | **DONE** | `lanes/LANE_FASTCHECK/*` |
| **LANE_HIST3FIX** | FULL-BT-HIST-3-FIX | **DONE** | `lanes/LANE_HIST3FIX/*` |
| **LANE_FULLBT** | FULL-BT-FUT-RUN-2 | **WAIT_CLAUDE_OK** (RUN-2 실런 OUTBOX) | `lanes/LANE_FULLBT/*` |

---

## 디렉터 한 줄

**MASTER 3** — VPS OK. COUNT BTC/ETH/SOL **각 302** (2025-10-31~2026-08-30). 실런 안 함. last가 08-30이라 약 4주 공백.  
**MASTER 6** — cutover **FAIL 유지**: `BITGET_PIPELINE_SSOT` **없음**(=0). `BITGET_ASYNC_TELEGRAM=1`. 레거시 `bitget.main`/`factory_launcher` 프로세스 없음. **SSOT=1로 올리지 않음.**  
**MASTER 9** — A5-EVENTLOG-01 **Claude OK · 구현 OK · 서버 반영 미확인**. 적재 시계는 반영 확인 후. 10=반영 확인 후 EFFECTVERIFY.  
**MASTER 5** — RUN-2 **실런 완료** · WAIT_CLAUDE_OK. 갭#4 프로덕션은 **11** (닫지 않음). **7** CAT-L 승인 대기.

