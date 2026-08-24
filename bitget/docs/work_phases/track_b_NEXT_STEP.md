# NEXT STEP

> 갱신: 2026-08-23 · FULL-BT-2 Claude 검증 OK · FULL-BT-3 Handoff 발급

## 지금 상태
FULL-BT-2(배치+체크포인트) Claude 검증 **OK**. FULL-BT-3(§2 스키마 리포트 · CAT-J 비편입) Handoff 발급 완료, Cursor 구현 대기.

## 다음 행동
1. Cursor: 위 FULL-BT-3 Handoff 기준 `bitget/full_bt/report.py` 구현
2. Cursor: 세션 종료 시 `05_진행로그.md`/`00_전체현황판.md`/`CURSOR_TO_CLAUDE.md`/`NEXT_ACTION.md` 갱신 → `WAIT_CLAUDE_OK`
3. 디렉터: Cursor 완료 보고 오면 `CURSOR_TO_CLAUDE.md` FULL-BT-3 검증을 Claude에게 요청
4. 병행: R1a 매일 관측 유지, 게이팅 없음
5. FULL-BT 산출을 R6 대체·B1「달성」·LIVE 근거로 사용 금지 (전 단계 공통)
