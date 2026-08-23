# NEXT_ACTION — Bitget

| 필드 | 값 |
|------|-----|
| **sub-phase** | **OVERSEER-AUDIT-01** (활동 부재 판독) · NS-BG-CRON-ISO 병행 |
| **status** | **WAIT_DIRECTOR** (+ Claude Ask: OVERSEER-FACTS 필요 여부) |
| **코드** | 구현 없음 · 감사 OUTBOX만 |

---

## 디렉터 (지금)

1. **활동 부재 결론(로컬 코드):** 킬스위치로 막아둔 것 **아님**. 감시관이 OPEN을 안 읽고 + 켈리 0.006(의도) + 당일 청산/R&D 0을 Gemini가 “활동 부재”로 쓴 것.
2. **파이프라인 생존 확인(서버 1줄):** `bitget_forward_trades` OPEN/CLOSED 카운트 — OPEN≥1이면 장부 살아 있음.
3. **코인 북극성 미수신:** 이전 안내 `diagnose_coin_digest.sh --send` 유지.
4. Claude: `track_b_CURSOR_TO_CLAUDE` 상단「활동 부재 감사」Ask 붙여넣기.

**금지:** C-2 · MDD 5% · live · Kelly 임의 상향 · `ENABLE_REAL_EXECUTION`
