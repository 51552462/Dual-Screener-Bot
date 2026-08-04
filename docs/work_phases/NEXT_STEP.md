# NEXT_STEP — 디렉터 다음 한 걸음

> **갱신**: 2026-08-05 · auto_forward_tester facade 복구 · import smoke 게이트

---

| 필드 | 값 |
|------|-----|
| **지금 단계** | factory import 복구 ✅ → **서버 배포** → Claude 검증 |
| **status** | `DEPLOY_FACTORY_FIX` |
| **한 줄** | `auto_forward_tester.py` 복구 후 `sudo ./update_factory.sh` 로 서버 반영 |

---

## 당신 체크리스트

- [ ] 서버에서 `git pull` 후 **`sudo ./update_factory.sh`** (import smoke 통과 시에만 재기동)
- [ ] `systemctl is-active dante-factory` → **active** · heartbeat 1~2분 이내 갱신 확인
- [ ] Claude에 `CURSOR_TO_CLAUDE.md` + `05` M-R0·A-3·A-1-R1 **검증** 요청
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
