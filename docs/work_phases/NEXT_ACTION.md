# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **S5-HARNESS-SCOPE-01** (VPS 실측 산출) |
| **status** | `WAIT_CURSOR_IMPL` — **Ops-lite 새 창** · 코드 diff 금지 |
| **디렉터 결정** | 2026-08-17 — 로컬 Claude OK · 다음=VPS CLI 1회 |
| **앵커** | `SYNC-2026-08-17-O` |

---

## 디렉터 — 지금 할 일

1. **Cursor Ops 새 창** (아래 부팅) — VPS에서 기존 CLI만 실행 · **코드 수정 금지**.
2. `--start` 날짜는 BEAR/HIGH_VOL 게이트 활성 구간 포함으로 디렉터가 지정.
3. SSH 불가면 즉시 `WAIT_DIRECTOR` OUTBOX (재시도 아님).

### Ops-lite 창 부팅 (복붙)

```text
Track A (KR/US) Ops-lite — S5 VPS 실측.

1) docs/work_phases/00_SESSION_SYNC.md §3
2) docs/work_phases/NEXT_ACTION.md — WAIT_CURSOR_IMPL (Ops-lite)
3) docs/work_phases/CLAUDE_TO_CURSOR.md 최상단 = VPS 실측 Go (코드 0줄)
4) bitget/ 금지 · 코드 diff 금지.

sub-phase: S5-HARNESS-SCOPE-01 (VPS 실측)

세션 종료: 05 · 00_SESSION_SYNC §3 · NEXT_ACTION · CURSOR_TO_CLAUDE · 3줄 요약.
```

### VPS 실행 예시 (경로·env는 기존 팩토리 관례)

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
# --start 는 디렉터 지정일
python3 scripts/run_s5_defense_contribution_report.py --start YYYY-MM-DD --as-of $(date -u +%Y%m%d)
```

산출: `reports/s5_defense/s5_contribution_{일자}.json` · `short_pnl_column_present` 유지 확인.

---

## §OPS-01 — Done (참고)

| # | 할 일 | 비고 |
|---|--------|------|
| 1–7 | 배포+1차 관측 | ✅ `0efc750` · overall PASS · `cursor_action=NONE` |

---

## S5-HARNESS-SCOPE-01

| 단계 | 상태 |
|------|------|
| 로컬 페이퍼 게이트 구현 | **Claude OK** · 부분 Done |
| VPS 실측 기여 로그 | **지금** · Ops-lite |

---

## BEAR-S5-SIM-01 / SIDE / BULL

- BEAR 1단계 Done · 2단계 보류 · SIDE/BULL 동결 · 재시도 금지
