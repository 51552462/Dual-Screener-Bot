# CAT-Q · 진단 & 레거시 (Bitget)

> **위험도** 🟢 Low · **Tier T3** · 진단·Handoff·아키텍처 질문 시  
> **also_load**: CAT-MAP, `docs/01_architecture_mapping_and_diagnosis.md`

---

## 1. 역할

Architecture diagnosis, legacy entrypoint map, phase execution history, cutover checklist, known bugs.

---

## 2. 진단 SSOT 문서

| 문서 | 내용 |
|------|------|
| `docs/01_architecture_mapping_and_diagnosis.md` | 주식↔Bitget 4-layer map |
| `docs/13_institutional_grade_audit_and_roadmap.md` | P0/P1/P2 roadmap |
| `docs/07_phase8_feasibility_review.md` | Track A/B/C |
| `validation/architecture_checks.py` | automated guards |
| `validation/cutover.py` | cutover readiness |

---

## 3. Legacy / 제거 대상

| item | issue |
|------|-------|
| `main.py` direct loop | triple entrypoint |
| `auto_pilot.system_main_loop` | duplicate schedule |
| `BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget.main` | wrong component |
| JSON config direct read | use config_manager |
| root meta without Bitget DB | fixed 2026-07 |

---

## 4. Known Bugs Register

| ID | location | priority |
|----|----------|----------|
| Q-1 | `master_scanner.py:97` bse NameError | P0-6 |
| Q-2 | deathmatch key BG mismatch | B-1 |
| Q-3 | funding not in PnL | P1-3 |
| Q-4 | log disk exhaustion | P0-1 |

---

## 5. Phase History (implementation)

Phase 0–8 Track A documented in `bitget/docs/0x_*.md`, `implementation_phase_*.md`

**새 work**: `bitget/docs/work_phases/` 묶음A–D (institutional P0/P1/P2)

---

## 6. Claude 사용법

- Cursor Prompt **작성 전** 로컬 현황 확인용
- CAT-Q + MAP + specific CAT (1개)
- 코드 출력 금지 — 진단표·우선순위·Handoff 방향만

---

## 7. Cutover Checklist (summary)

- `BITGET_PIPELINE_SSOT=1`
- `architecture_checks` PASS
- 48h parallel run (Ubuntu)
- `ENABLE_REAL_EXECUTION` explicit false until P2-5
