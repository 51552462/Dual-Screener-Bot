# NEXT_ACTION — Bitget (레인 대시보드)

> **본문 진실은 레인 폴더.** 표만 upsert · 다른 레인 행 삭제 금지.
> **현재 Bot-2 주소: 52.79.114.70 (2026-10-02~, 이전 3.36.90.195) · 정적 IP 여부: 디렉터 확인 대기** (호스트 키 ED25519 `SHA256:HkXnpIQ6…IVuM` 옛 주소와 일치 확인)

| 레인 | sub-phase | status | 창이 쓸 파일 |
|------|-----------|--------|--------------|
| **CAT-A (공용)** | A-LIFECAP-01 | **ENFORCE_LIVE · WAIT_FIRST_KILL_CONFIRM** | `track_b_*` · OUTBOX=`track_b_CURSOR_TO_CLAUDE.md` |
| **CAT-L (공용·레인 아님)** | CAT-L-FENCE-03 | **SUB_DONE** | `track_b_*` · OUTBOX=`track_b_CURSOR_TO_CLAUDE.md` |
| **CAT-L (공용·레인 아님)** | CAT-L-CUTOVER-01 Phase 0c | **Phase 0c SUB_DONE** · Y-블록 회신 Claude 수용(조건부) · L13–L16 로컬 확인 제출 · Phase 1 보류 · 순서: SECRET-01 → BACKUP-01 → Phase 1 | `track_b_*` · OUTBOX=`track_b_CURSOR_TO_CLAUDE.md` |
| **CAT-F (공용)** | A5-EVENTLOG-01 | **구현 OK · 서버 반영 확인 대기** (적재 시계는 반영 확인일부터) | `track_b_*` |
| **CAT-L (에스컬레이션)** | FENCE-02 | **SUB_DONE (1–2단계) · 3단계 판정 2026-10-13** (근거: CLAUDE_TO_CURSOR FENCE-03 Handoff 선행 상태 줄, Claude C-7) | live FENCE_OK · 실스캔 cgroup=heavy.slice |
| **CAT-L (공용)** | CAT-L-SCRIPT-AUDIT-01 | 등록 · Phase 1과 병행 · 대기 | `track_b_*` |
| **CAT-L (공용)** | CAT-L-BACKUP-01 | 2순위 · 입력 정리 완료(Y-판정 §5) · Handoff는 SECRET-01 다음 창 | `track_b_*` |
| **CAT-L (공용)** | CAT-L-QW-RESTART-01 | 범위 확장: watchdog 장시간 잡 종료(queue-worker 일일 재시작 + LIFECAP) · 조사 대기 · Phase 1 비차단 | `track_b_*` |
| **CAT-L (공용)** | CAT-L-SECRET-01 | 등록 · **다음 Claude 창 1순위** · 토큰 교체(디렉터 TTY) + 규칙 8 시행 확인 | `track_b_*` |
| **CAT-M (공용)** | CAT-M-TOKENLOG-01 | 등록 · 비차단 · URL이 예외 문구로 로그에 남는 경로 | `track_b_*` |
| **CAT-A (공용)** | CAT-A-LIFECAP-02 | 등록 · 비차단 · 스캔 실제 소요·kill 48회·부분 기록 감사(spot/fut 분리) | `track_b_*` |
| **LANE_FASTCHECK** | B0-SAMPLE-CONTRACT | **DONE** | `lanes/LANE_FASTCHECK/*` |
| **LANE_HIST3FIX** | FULL-BT-HIST-3-FIX | **DONE** | `lanes/LANE_HIST3FIX/*` |
| **LANE_FULLBT** | FULL-BT-FUT-RUN-2 | **WAIT_CLAUDE_OK** (RUN-2 실런 OUTBOX) | `lanes/LANE_FULLBT/*` |

---

## 디렉터 한 줄

**MASTER 3** — VPS OK. COUNT BTC/ETH/SOL **각 302** (2025-10-31~2026-08-30). 실런 안 함. last가 08-30이라 약 4주 공백.  
**MASTER 6** — cutover **FAIL 유지**: `BITGET_PIPELINE_SSOT` **없음**(=0). `BITGET_ASYNC_TELEGRAM=1`. 레거시 `bitget.main`/`factory_launcher` 프로세스 없음. **SSOT=1로 올리지 않음.**  
**MASTER 9** — A5-EVENTLOG-01 **Claude OK · 구현 OK · 서버 반영 미확인**. 적재 시계는 반영 확인 후. 10=반영 확인 후 EFFECTVERIFY.  
**MASTER 5** — RUN-2 **실런 완료** · WAIT_CLAUDE_OK. 갭#4 프로덕션은 **11** (닫지 않음). **7** CAT-L 승인 대기.

