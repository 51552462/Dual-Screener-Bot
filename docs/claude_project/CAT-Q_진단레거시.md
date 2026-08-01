# CAT-Q · 진단·레거시·테스트 (보조)

> **위험도** 🟢 Low (diag) / 🟡 (repair) · **Tier T3** · 필요 시만 @멘션

---

## 1. scripts/ 진단 (Claude: 해석 / Cursor: 실행)

| script | purpose |
|--------|---------|
| `diag_forward_trade_identity_gaps.py` | identity gaps |
| `diag_forward_staleness.py` | stale open book |
| `diag_forward_open_book.py` | open book state |
| `diag_us_scan_pipeline.py` | US scan pipeline |
| `diag_cron_tz_effective.sh` | cron TZ |
| `diagnose_factory_data.sh` | data dir health |
| `calculate_historical_nav.py` | NAV backfill |
| `dump_evolution_tuning_md.py` | tuning dump |
| `smoke_alpha_mining_evolution.py` | smoke test |
| `verify_schedule_alignment.sh` | schedule check |
| `reset_factory_pipeline.sh` | pipeline reset |
| `repair_forward_trades_numeric_corruption.py` | 🟡 BLOB repair |

---

## 2. legacy_archive/ (CAT-R)

**신규 기능 추가 금지** — runtime import only.

| path | still used by |
|------|---------------|
| `legacy_archive/scanners/nulrim.py` | CAT-C KR nulrim |
| `legacy_archive/scanners/ema5.py`, `us_5ema.py` | 5EMA scans |
| `legacy_archive/scanners/kr.py`, `usa.py` | bowl |
| `legacy_archive/scanners/dante_*` | reverse breakout |
| `legacy_archive/scanners/master.py`, `us_master.py` | master |
| `legacy_archive/dashboard.py` | deprecated UI |

Refactor path: move to top-level scanner package — Cursor incremental.

---

## 3. tests/ (CAT-S)

- `tests/` — pytest (exclude bitget when KR/US only)
- `test_fast_safety_*.py` at repo root — CAT-N isolated

Claude: scenario design. Cursor: run pytest.

---

## 4. validation/

| file | role |
|------|------|
| `validation/walk_forward.py` | walk-forward validation |

---

## 5. Claude 사용법

디렉터가 로그/진단 결과 붙여넣을 때:
- 임계값 판단 → relevant CAT + CONSTANTS
- repair script spec → CAT-D Handoff with rollback

---

*오케스트레이션 reset: CAT-A · schema repair: CAT-B/D*
