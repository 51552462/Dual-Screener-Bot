# CAT-L · 인프라 & 배포 (Bitget)

> **위험도** 🟡 Medium · **Tier T2 (ops only)** · Claude: 정책만 / Cursor: 셸·systemd

---

## 1. 역할

systemd units, cron, venv, snapshot, watchdog, backup, log rotation, resource limits, Ubuntu deploy.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| deploy wrapper | `deploy/bitget.sh`, `deploy_bitget_factory.sh` |
| update script | `deploy/update_bitget.sh` |
| systemd templates | `deploy/systemd/` (dante-bitget-*) |
| runtime lock | `infra/runtime.py` |
| ops logger | `ops_logger.py` (Bitget) |
| logging setup | `infra/logging_setup` (fix: doc 12) |
| disk / log | P0-1 target — logrotate / RotatingFileHandler |
| RUNBOOK | `bitget/RUNBOOK.md` |

---

## 3. systemd Topology

| unit | role |
|------|------|
| dante-bitget-factory | `bitget_auto_pilot --daemon` |
| dante-bitget-ws | WebSocket |
| dante-bitget-async | Telegram queue |
| dante-bitget-dashboard / heatmap | Streamlit :8511/:8512 |
| dante-bitget-watchdog.timer | 5m heartbeat |
| dante-bitget-snapshot.timer | 5m CQRS |

---

## 4. Watchdog (CAT-A link)

- every 5m: `ops_events` heartbeat.tick
- miss 100s × 3 → telegram + restart **factory only**
- **Gap P1-7**: WS / async / queue-worker not auto-restarted

---

## 5. Backup

| | 상태 |
|---|------|
| 5m CQRS snapshot | ✅ |
| institutional_db_backup (integrity) | exists, **cron 미연결** P0-5 |
| PRAGMA integrity_check | in institutional backup |

---

## 6. Log Rotation (P0-1 · Critical for 1yr unmanned)

- **현행**: unlimited timestamp log files → disk exhaustion
- **목표**: logrotate or RotatingFileHandler + `disk_manager` hook
- **L-1 (2026-08-02)**: `install_bitget_logrotate.sh` + journal vacuum timer — see `05` L-1

---

## 7. Environment

| var | role |
|-----|------|
| BITGET_ROOT | package root |
| BITGET_DB_STORAGE_PATH | prod data separation |
| BITGET_PIPELINE_SSOT | cutover flag |
| PYTHONPATH | must include repo root (root imports) |

---

## 8. Claude 설계 대상

- P0-1 log rotation policy
- P0-5 backup cron spec
- P1-7 watchdog extension matrix
- Windows dev vs Ubuntu prod matrix (`07_phase8`)

---

## 9. Resource Limits

4GB OOM motivation — global flock (CAT-A), MemoryMax in systemd
