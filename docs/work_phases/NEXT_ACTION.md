# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **OPS-LIQUIDITY-STALL-01** (CAT-B · LIQUIDITY 4분류) |
| **status** | 스크립트 Done · **VPS 실행 후** `WAIT_CLAUDE_OK` · OBS-HOLD · Alpha/cutoff **금지** |
| **직전** | OPS-OPEN-STALL-01 **Claude OK** · CLASS (a) |
| **앵커** | `SYNC-2026-08-19-A` |

---

## 디렉터 — 지금 할 일

1. **VPS** — pull 후 `python3 scripts/ops_liquidity_stall_01_diagnosis.py` 실행 · 로그를 Cursor/OUTBOX에.
2. **금지** — LIQUIDITY 임계 완화 · config_kv · cutoff (결과가 (c)/(d)여도 즉시 완화 금지).
3. **잔여** — NS-DIR-DASH 19:30 육안 · `L-DATA-ALARM-01`은 백로그만.

### VPS 실행 (복붙)

```bash
cd ~/dante_bots/Dual-Screener-Bot
git pull
set -a && source .env && set +a
python3 scripts/ops_liquidity_stall_01_diagnosis.py
```

### Claude 창 부팅 (실행 로그 후)

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md §3 (SYNC-2026-08-19-A)
2) docs/work_phases/NEXT_ACTION.md
3) docs/work_phases/CURSOR_TO_CLAUDE.md 최상단 (OPS-LIQUIDITY-STALL-01)

검증: (a)(b)(c)(d) 표 · VERDICT · 임계 변경 없음 확인.
```

---

## North Star SSOT (고정)

| 항목 | 값 |
|------|-----|
| **SSOT** | VPS `/var/lib/quant-factory/data/dual_north_star_ledger.json` |
| **로컬** | `*.LOCAL_DEV_DO_NOT_USE.json` — **사용 금지** |

---

## 근처놓침 레버 — 전원 소진·동결

BULL-RECENCY · SIDE-ALPHA · BEAR-S5-SIM · C-1-REDUCED — 규칙1 재접촉 금지
