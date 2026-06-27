# Bitget Phase 6 — Production Deploy

> 작성일: 2026-06-07  
> **주식 루트 미수정** — Bitget systemd·스크립트는 `bitget/deploy/` 전용.

---

## systemd 6 + timer 2

| 유닛 | 파일 |
|------|------|
| WS | `dante-bitget-ws.service.in` |
| Factory daemon | `dante-bitget-factory.service.in` |
| Async Telegram | `dante-bitget-async.service.in` |
| Dashboard :8511 | `dante-bitget-dashboard.service.in` **(신규)** |
| Heatmap :8512 | `dante-bitget-heatmap.service.in` **(신규)** |
| Watchdog | `dante-bitget-watchdog.service.in` + `.timer` |
| Snapshot | `dante-bitget-snapshot.service.in` + `.timer` |

Entrypoints: `bitget/deploy/entrypoints/run_bitget_*.sh`

---

## 배포 스크립트

| 스크립트 | 역할 |
|----------|------|
| `deploy_bitget_factory.sh` | 최초 systemd install + enable |
| `update_bitget.sh` | git pull + backup + graceful restart (**equity 무터치**) |
| `bitget.sh` | cron one-shot wrapper |

```bash
sudo INSTALL_ROOT=/path/to/repo ./bitget/deploy/update_bitget.sh
```

백업: `/var/backups/bitget-pre-update/<UTC>/`

---

## RUNBOOK

- **`bitget/RUNBOOK.md`** — 운영 1페이지 (로그·재시작·장애표)

---

## data_paths 이전 (Phase 6)

| 모듈 | 변경 |
|------|------|
| `dashboard.py` | `market_db_read_path()` |
| `heatmap_dashboard.py` | `market_db_read_path()` |
| `auto_pilot.py` | `market_data_db_path()`, `system_config_json_path()` |
| `system_auto_pilot.py` | 동일 |
| `charting.py` | `charts_dir()` |
| `shadow_tracking.py` | `market_data_db_path()` |
| `master_scanner.py` | `logs_dir()` for sent log |

---

## sentinel → systemd

- **프로덕션:** dashboard/heatmap/factory 각각 systemd 유닛
- **개발:** `python -m bitget.factory_launcher` (sentinel subprocess) — legacy

`run_bitget_daemon.sh` → `bitget.pipelines.bitget_auto_pilot --daemon` (cron과 분리)

---

## Phase 6 완료 기준

- [x] systemd 6유닛 + watchdog/snapshot timer
- [x] `update_bitget.sh` zero-downtime (Bitget only)
- [x] `bitget/RUNBOOK.md`
- [x] dashboard/heatmap systemd
- [x] 핵심 runtime `data_paths` SSOT

---

## 다음 (Phase 7)

- Parallel run 48h (legacy main vs pipeline)
- Signal / PnL parity
- Load test 500 symbols
- Cutover: cron SSOT only
