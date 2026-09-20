# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **SWALLOW-GATE-FO-02** 배포 · leftover **EOD 776** |
| **status** | FO-02 **Claude OK** · 배포 후 ERROR 소멸 관측 · 776 RO 진행 |
| **직전** | 조기 게이트 복구. 톡식=`TOXIC-FADE-FO-CHECK` |
| **앵커** | `SYNC-2026-09-20-GATE-FO-02-OK` |

---

## 관측 (FO-02 성공 증거)

`entry_gate.meta_global_fail_open` 이 UnboundLocal로 **안 찍히면** 게이트가 다시 돈 것.

정상 진입(게이트 통과) 회귀 없음도 같이 본다.

## leftover 큐

1. **지금** `ledger.py` 776 EOD 청산 삼킴 (RO → 발동 흔적 → Handoff)
2. CB 51/66/363
3. live_nav 239
4. 선취매 가드 · Thompson · S1/S4 클램프
5. `TOXIC-FADE-FO-CHECK` (급하지 않음)

## 금지

- 톡식을 지금 구현
- fail-open 추가 뒤집기
- 컷오프 / bitget
