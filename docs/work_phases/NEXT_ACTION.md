# NEXT_ACTION

| 필드 | 값 |
|------|-----|
| **sub-phase** | **RP-1 + C-1** harness ✅ |
| **status** | `WAIT_LIVE_RUN` |
| **Claude** | harness OK 2026-08-07 |

---

## 병렬 3건 (충돌 없음 — Claude 승인)

| # | 작업 | 비고 |
|---|------|------|
| ① | A-5b 일괄 배포 | `update_factory.sh` |
| ② | north star 일일 cron | ledger JSON |
| ③ | RP-1 live run | `reports/regime_panel/rp1_*.json` |

---

## RP-1 live run 순서

1. **KR 스모크** — KOSPI top N (파이프 확인)
2. **KR+US 합산** — 최종 North Star 판정본 (SSOT)
3. JSON → Claude **결과 재검증** (harness OK ≠ Pass/Fail)

```bash
# 서버 예시 — KR+US 유니버스는 Handoff 후 스크립트 확정
python -c "from regime_panel_rp1 import run_regime_panel_rp1; ..."
```

---

## 테스트 (harness)

`pytest tests/test_regime_panel_rp1.py` — **15 passed**

- `test_rp1_no_config_kv_write`
- `test_regime_periods_dates_ssot_snapshot`
- Stage2: `test_stage2_branch_*` ×5
