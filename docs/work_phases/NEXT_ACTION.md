# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **SMARTMONEY-PERSIST-FIX-01** (실측) |
| **status** | **WAIT_DIRECTOR** — 월 16:10 DoD#1 · 리플레이 **만들지 않음** |
| **직전** | 라이브 리플레이 HOLD (수급 신호 전 무가치) |
| **앵커** | `SYNC-2026-09-12-REPLAY-HOLD` |

---

## 디렉터 — 지금 할 일

1. **2026-09-14(월) 16:10 이후** `kr_investor_flow` 행수 0→N · 로그 `🗄️ [수급 시계열]` · 네이버 맵 대조. 미배포면 먼저 `update_factory`.
2. 수급이 **신호(행·값·스캔 맵 정합)**를 보이면, 그때 「라이브 코드 + 과거 가격 **수급 포함** 리플레이」를 **검토**(Handoff). 지금 하니스 금지.
3. B-4는 그대로 ~09-17 후. RADAR-SQLITE는 급하지 않음.

### 금지

- 라이브 리플레이 하니스 / RP-1 대체 런 / `ledger`·`shared` as-of 개조
- 수급 신호 전에 리플레이 착수
- DoD#1 전 SMARTMONEY CLOSED
- Phase B / B-4 지금 코드 · bitget
