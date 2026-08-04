# NEXT_STEP — 디렉터 다음 한 걸음

> **갱신**: 2026-08-03 · factory 복구 · work_phases bitget 대칭 보완

---

| 필드 | 값 |
|------|-----|
| **지금 단계** | M-R0 구현 ✅ → **Claude 검증** |
| **status** | `WAIT_CLAUDE_OK` (M-R0 + A-3 + A-1-R1 병렬) |
| **한 줄** | factory 살아남 — 문서·Claude OK 후 Phase A 전진 |

---

## 당신 체크리스트

- [ ] Claude에 `CURSOR_TO_CLAUDE.md` + `05` M-R0·A-3·A-1-R1 **검증** 요청
- [ ] 서버 `dante-factory`·watchdog 알림 **정상** 확인 (inverse_etf_sniper 수정 후)
- [ ] `.env` `DB_STORAGE_PATH` 한 줄만 `/var/lib/quant-factory/data` 인지 확인
- [ ] paper 가상매매 MDD·tier 관측 (효과 검증은 `06` 2~4주 후)

---

## Claude → Cursor (Mirror `ARCHITECT_MIRROR` #1~3)

| # | 제안 | Layer |
|---|------|-------|
| 1 | M-R0 + A-3 + A-1-R1 **병렬 검증** | ① 디렉터 |
| 2 | 서버 `.env` DB 경로 정리 | ① 디렉터 |
| 3 | A-4 전 RL hysteresis **Adapter** Handoff | 🟡 2 |

---

## 디렉터 → Claude (복붙)

```
CURSOR_TO_CLAUDE.md + 05 검증. M-R0 OK/수정 spec. (병렬) A-3·A-1-R1 판정.
```

**AI status 상세**: `NEXT_ACTION.md`
