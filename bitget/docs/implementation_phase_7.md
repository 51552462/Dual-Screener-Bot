# Bitget Phase 7 — Validation & Cutover

> 작성일: 2026-06-07  
> **주식 루트 미수정**

---

## Validation 패키지 (`bitget/validation/`)

| 모듈 | 역할 |
|------|------|
| `signal_parity.py` | master `sent_log` hit keys vs baseline (<1% diff) |
| `pnl_parity.py` | OPEN forward_trades fingerprint vs baseline |
| `load_test.py` | symbol×TF table count + read probe (<10min) |
| `cutover.py` | `BITGET_PIPELINE_SSOT` + 48h parallel window |
| `runner.py` | pipeline orchestration |

Baselines: `{BITGET_DB_STORAGE_PATH}/validation/*.json`

---

## Pipeline modes

| mode | 설명 |
|------|------|
| `record_baseline` | signal + PnL baseline 저장 |
| `validate` | parity check (baseline 필요) |
| `load_test` | DB scan benchmark |
| `cutover_check` | readiness report (non-critical) |
| `validate_all` | validate + load_test + cutover |
| `start_parallel` | 48h parallel-run 시작 |

```bash
./bitget/deploy/bitget.sh --record-baseline
./bitget/deploy/bitget.sh --validate
./bitget/deploy/bitget.sh --load-test
./bitget/deploy/bitget.sh --cutover-check
./bitget/deploy/bitget.sh --start-parallel
./bitget/deploy/bitget.sh --validate-all
```

---

## 환경 변수

| 변수 | 기본 | 설명 |
|------|------|------|
| `BITGET_SIGNAL_MAX_DIFF_PCT` | 1.0 | signal parity 임계 |
| `BITGET_LOAD_TEST_MIN_SYMBOLS` | 500 | load test |
| `BITGET_LOAD_TEST_MAX_SEC` | 600 | load test 시간 상한 |
| `BITGET_PARALLEL_RUN_HOURS` | 48 | parallel window |
| `BITGET_PIPELINE_SSOT` | 0 | 1 = cutover ready gate |

---

## Cutover 절차

1. `./bitget.sh --start-parallel` — 48h window 시작
2. systemd `dante-bitget-*` + cron `bitget.sh` 병렬 운영
3. `./bitget.sh --record-baseline` (window 시작 시점)
4. 매일 `./bitget.sh --validate` (또는 `--validate-all`)
5. 48h 후 `BITGET_PIPELINE_SSOT=1` + `--cutover-check` PASS
6. legacy `python -m bitget.main` / `factory_launcher` 중단

---

## Dashboard ops panel (7.6)

`dashboard.py` 상단 — `gauge.snapshot` + heartbeat from `bitget_ops_events.sqlite`

---

## Legacy deprecation (7.5)

- `bitget.main` — DeprecationWarning + log
- `factory_launcher` — sentinel dev-only; prod = systemd

---

## 버그 수정 (Phase 7)

- `master_scanner.run_scan` — `rows` undefined regression 수정

---

## 테스트

```bash
python -m unittest bitget.tests.test_validation_phase7 -v
python -m unittest bitget.tests.test_trading_phase5 -q
```

---

## Phase 7 완료 기준

- [x] signal / PnL parity tooling
- [x] load test mode
- [x] cutover + parallel run state
- [x] main/factory_launcher deprecated
- [x] dashboard ops gauges
- [ ] 7-day production soak (운영자 실행)
