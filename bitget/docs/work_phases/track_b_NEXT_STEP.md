# NEXT STEP

> 갱신: 2026-08-25 · FULL-BT-HIST-3 Claude 검증 OK · VPS 실행 지시

## 지금 상태
HIST-3(call/outcome/tf_coverage 분리 계측) 스펙 검증 **OK**(비차단 caveat 1건: Handoff 원문 보관 누락 재발). VPS dry(3)→10×2 실행 승인.

## 다음 행동
1. Cursor: 커밋·푸시 → VPS dry(3)→10×2, 5개 숫자 CURSOR_TO_CLAUDE.md 보고
2. Claude: 3원인(TF/호출경로/warmup) 판별 재판정
3. 판별되면 → lookback 조사 Handoff(신규) 또는 다음 조치
4. 미판별(3번째 실패) → HIST-4 금지, 디렉터 에스컬레이션
5. 이번 Handoff 원문은 CLAUDE_TO_CURSOR.md에 append 보관(재발 방지)
