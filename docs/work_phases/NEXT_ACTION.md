# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **KR-LOCKDOWN-STALL-THAW-01 ARM** |
| **status** | **관측** — VPS `KR_LOCKDOWN_THAW_ARMED=1` (2026-09-21) |
| **git** | `de57746` |
| **앵커** | `SYNC-2026-09-21-STALL-THAW-ARMED` |
| **성공 기준** | 재평가 루프 재가동. **MDD 9% 밑 ≠ 성공** |

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

## 병행 RO (2026-09-21 · 코드 0 · 우선순위만)

전문: `CURSOR_TO_CLAUDE.md` OUTBOX.

- **US-COSINE-NA-RO**: 섀도우(chart). NA_LAST_CLOSE 하루 최대 13 · 코사인 도달 최대 3. last-finite-close는 5239 미부활.
- **TOP_ONLY**: 미결선. DEFENSE 실효=켈리 0.2·쿼터 0.35. 배선은 TOP 정의가 큼. 직전 필수 아님.
