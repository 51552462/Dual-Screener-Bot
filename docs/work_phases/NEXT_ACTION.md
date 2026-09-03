# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **F-GATE-REGISTRY-PATH-01** |
| **status** | **`WAIT_CLAUDE_OK`** · VPS 메인 DB registry 실효 확인 · 로컬 커밋·푸시 잔여 |
| **직전** | 스냅샷 경로 버그 수정 · MetaGovernor 1회 MAIN upsert · deploy_watch PASS |
| **앵커** | `SYNC-2026-09-03-F-GATE-PATH` |

---

## 디렉터 — 지금 할 일

1. **즉시 커밋·푸시** (VPS는 scp 반영 — `update_factory` git pull 시 옛 코드로 덮이면 안 됨)
2. Claude Pro: `docs/work_phases/CURSOR_TO_CLAUDE.md` 최상단 OUTBOX 검증

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot && sudo bash ./update_factory.sh
```

### 메모

- 메인 `strategy_registry` n=8 (LIVE 3 · CANDIDATE 5) · 현재 COOLED/RETIRED=0
- F-RETIRE-02 회귀 없음
- KR LOCKDOWN(NAV) 별건 유지

### 금지

- registry 임의 행 추가 · 승격/강등 로직 변경 · 스냅샷에 registry 쓰기 복귀
