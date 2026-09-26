# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **지금 디렉터** | ENTRY-ATR 배포 후 2~4주 관측. Bitget 실적·B0는 **코인 창** 캡처. 수급 HOLD |
| **sub-phase** | ENTRY-ATR 관측 / DOCS-SYNC 완료 |
| **status** | **WAIT_CLAUDE_OK** (DOCS-SYNC 검증) · ENTRY-ATR는 조건부 OK로 배포 |
| **다음** | Phase 2 Kelly 실배선 금지. 전패구간 창분할은 누적 후 |
| **앵커** | `SYNC-2026-09-26-DOCS-SYNC-01` |
| **성공 기준 (thaw)** | 재평가 루프 재가동. **MDD 9% 밑 ≠ 성공** |

---

## 무장 완료 (VPS 실측)

| 항목 | 값 |
|------|-----|
| OPEN | **0** |
| ARMED before | **False** |
| ARMED after | **True** |
| 의미 | 한국 재오픈 아님. 통과 신호 **1건·ε-캡**만 허용 |

코드 추가 금지. 개입 없음.

## 관찰 (자동)

1. KR 스캔이 신호를 잡으면 극소 1건 진입. 없으면 그날 체결 0 = 정상.
2. band는 **LOCKDOWN** 표시 = 정상.
3. 그 1건 청산 → `kr_lockdown.stall_thaw_consumed` + ARMED 자동 0 → 승/패·실제 f·ΔNAV 보고.
4. 며칠 스캔만 돌고 통과 0 → 「무장은 됐는데 아직 신호가 없다」보고.

재무장하려면 청산 후 디렉터가 다시 켠다. OPEN이 이미 1이면 thaw는 적용되지 않음(설계).

## 병행 (2026-09-24)

- **WATCHDOG-FUNNEL-VISIBILITY-01**: **Claude OK: 2026-09-24**. 배포 후 19:30 3줄 육안.
- **REGIME-KEYS-RO**: 판단 승인. 통합 금지. 4키 표시 vs CURRENT 덮어쓰기 금지는 나중에 **하나만**.
- **다음 세션**: 워치독 DATA/EVAL/LIQ/DNA/surv. US~27 KR~26과 자릿수 다르면 즉시 보고. 컷≠등재.
- **실전 전**: `SIZING-DUAL-LEDGER-01` (3장부). 대기: TOP_ONLY · S5 불일치 · 라다 sqlite.
- **US-COSINE-CUTOFF-RO CLOSED**: 99%는 tb 노름. z-score 36%. 7/22 양립 재해석.
- **TOP_ONLY**: 미결선. 대기.
- **US-LIQ-FLOOR30K-SHADOW**: 추천 `$30k`·주수2000 제거. 섀도우 LIQ 5680 / 컷 5602 (현행 4822/4783). **단독 라이브 금지.**
- **US-LIQ-FLOOR-RO**: `$300k`는 5/28 스캐너 보정(KR 5만 주 US 이식 방지). 실제 `$300~460` 클립과 무관. `$50k`/`$100k`는 탈락의 ~44%/~22% 구제 감. 낮출지 디렉터.
- **US-COSINE-LIQ-RO**: 1153은 **DATA 위장 아님**. 오늘 chart 표본은 페니+$300k 미달 실측. LIQ 패치·국면 재검증 실행 **해당 없음**(코사인 미도달). 전문 OUTBOX.
- **US-COSINE-REGIME-RO**: 템플릿이 지금 국면에 원래 안 맞다는 가설은 코사인 층에서 **미증명**. 등재급 종목은 남아 있음. NA 라벨 단독 수정 ≠ 167 복귀(잔여 LIQ).
- **TOP_ONLY**: 미결선. DEFENSE 실효=켈리 0.2·쿼터 0.35. 배선은 TOP 정의가 큼. 직전 필수 아님.
