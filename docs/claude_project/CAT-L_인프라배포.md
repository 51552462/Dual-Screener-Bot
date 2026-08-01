# CAT-L · 인프라 & 배포

> **위험도** 🟡 Medium · **Tier T2 (ops only)** · Claude: 정책만 / Cursor: 셸·systemd

---

## 1. 역할

systemd units, cron examples, venv, snapshot service, watchdog, backup, resource limits.

---

## 2. systemd topology

| unit | role |
|------|------|
| dante-factory.service | `--daemon` via run_factory_daemon.sh |
| dante-snapshot.timer (5m) | CQRS snapshot |
| dante-watchdog.timer (5m) | heartbeat check |
| dante-backup.timer | sqlite backup |
| dante-async.service | telegram daemon |
| dante-dashboard.service | streamlit |

Templates: `deploy/systemd/*.service.in`  
Installed: `deploy/ubuntu/*.service`

---

## 3. Entrypoints

| script | role |
|--------|------|
| run_factory_daemon.sh | 24/7 daemon |
| run_snapshot_service.sh | snapshot |
| run_async_daemon.sh | telegram |
| run_main_service.sh | main |

---

## 4. Cron policy (Claude)

**KR** (`factory.kr.crontab.example`): KST native, one line per slot  
**US**: only 2 polling lines → `factory_slot_dispatcher --market US`  
Generate: `deploy/generate_factory_crontab.py`

Install: `deploy/install_factory_cron.sh`, `/etc/cron.d/dual-screener-factory-{kr,us}`

---

## 5. Watchdog (CAT-A link)

- every 5m check `ops_events.sqlite` heartbeat.tick
- miss 100s × **3** → telegram + `systemctl restart dante-factory`

---

## 6. Ops utilities

| file | role |
|------|------|
| ops_logger.py | append-only ops_events |
| network_timeout.py | HTTP timeout |
| pipeline_error_util.py | step error format |
| factory_data_paths | DB path |
| deploy/fix_shell_lf.sh | CRLF fix |
| deploy/audit_factory_stack.sh | stack audit |

---

## 7. Resource limits

`deploy/ubuntu/factory_resource_limits.env.example`  
`system_config.resource_limits.fragment.json`

4GB OOM guard motivation for global flock (CAT-A).

---

## 8. Claude 설계

- cron frequency policy
- watchdog sensitivity
- snapshot interval vs stale 1800s (CAT-B)
- KR/US cron separation invariant

## 9. Cursor 구현

- shell paths, venv, LF, systemd install scripts, Ubuntu paths

---

*스케줄 detail: CAT-A · CQRS: CAT-B*
