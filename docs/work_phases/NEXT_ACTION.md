# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **MAE-ATR-REPLAY-01** |
| **status** | **WAIT_CLAUDE_OK** |
| **직전** | 읽기 전용 일봉 리플레이 스크립트 + VPS 표 |
| **앵커** | `SYNC-2026-09-11-MAE-ATR-REPLAY` |

---

## 디렉터 — 지금 할 일

1. `docs/work_phases/CURSOR_TO_CLAUDE.md` 최상단 OUTBOX 검증. OK면 `CLAUDE_TO_CURSOR.md`에 다음 Handoff. 채팅 말고 파일에.
2. **실전 `STAT_MAE` / ATR 우선순위 변경은 아직 금지** — 이번 결과는 섀도우 표만.
3. (병행 추적) V-2 1주 오탐 · 워치독 19:30 육안 — 이번 스코프 아님.

### 금지

- `forward/ledger.py` 청산 사다리 변경
- config_kv `DYNAMIC_MAE_SL` 채우기
- NAV/승격/텔레그램
- 평균 +0.31%p만 보고 “ATR이 낫다” 단정 (p50·이상치 반대)
