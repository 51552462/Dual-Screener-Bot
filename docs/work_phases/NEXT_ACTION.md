# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **PHASEB-SCOPE-02** (문서 큐) · 병렬 **SMARTMONEY-PERSIST-FIX-01** (실측) |
| **status** | **WAIT_DIRECTOR** — B-4 **코드 착수 금지** · 수급 DoD#1 월 16:10 |
| **직전** | RADAR-SQLITE-01 범위 확정(코드 0) · B-4는 ~09-17 후 |
| **앵커** | `SYNC-2026-09-12-RADAR-SCOPE` |

---

## 디렉터 — 지금 할 일

1. **월요일 16:10 이후** `kr_investor_flow` 행수 0→N · 로그 `🗄️ [수급 시계열]` · 네이버 맵 대조 (SMARTMONEY DoD#1). 미배포면 먼저 `update_factory`.
2. **~2026-09-17** V-2 WF-BLOCK 1주 오탐 관측 종료 확인 후, Claude에 **B-4 단독** Handoff 재요청.
3. Handoff에 넣을 문구: **LIVE 승격 ≠ DSR 통계 검증**.
4. **참고만** — `SMARTMONEY-RADAR-SQLITE-01` 범위는 확정됨. 급하지 않음. 다음 정식 스코프 때 아래 OUTBOX 참고.

### 금지

- 지금 Phase B / B-4 / B-1 / B-2 / B-3 코드
- WF-BLOCK 1주 끝나기 전 승격 엔진 수정
- DSR 없이 B-1/B-2 재개 선언
- DoD#1 전 SMARTMONEY CLOSED
- PyKRX 로그인 · lookback 20 · `get_flow_score` · bitget
- `SMARTMONEY-RADAR-SQLITE-01` **지금** 코드 (급하지 않음)
