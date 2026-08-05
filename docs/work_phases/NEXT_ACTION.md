# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **일괄 배포** (R2·R3·R4·A-4·A-5a rev.2) |
| **status** | `WAIT_DIRECTOR` — Critical 일괄 배포 승인 |
| **다음 담당** | **디렉터** → `update_factory.sh` |

---

## 디렉터 승인 대상 (일괄 배포)

**R2 · R3 · R4 · A-4 · A-5a(rev.2)** — 5건 모두 Claude OK ✅

🔴 Critical 포함 (A-5a, A-4) → `update_factory.sh` **전 디렉터 최종 승인 필요**

| 항목 | Claude OK |
|------|-----------|
| A-1-R2 | 2026-08-05 |
| A-1-R3 | 2026-08-06 |
| A-1-R4 | 2026-08-06 |
| A-4 | 2026-08-06 |
| A-5a rev.2 | 2026-08-06 |

### 문제 시 개별 킬스위치 (교차 조건 없음)

| 항목 | OFF |
|------|-----|
| A-5a | `ENABLE_WEIGHT_S5_MERGE=False` |
| A-4 | `ENABLE_ASYMMETRIC_HYSTERESIS=False` |

---

## 다음 Handoff 후보

| ID | CAT | 내용 |
|----|-----|------|
| **A-5b** | G (새 세션) | BEAR 국면 S5 자동 활성화 |
| A-5c | C | 스캐너 공식 연결 |
| fade/TOXIC_FADE | CAT-I (별도) | S5와 분리 — 카운터트레이드 성격, A-5c 또는 CAT-I 세션 |

**Phase A**: A-5a 완료(Claude OK) → **배포 대기** → A-5b → A-5c
