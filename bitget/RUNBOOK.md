# Bitget Quant Factory — 운영 런북

**격리 원칙:** Bitget 스택(`dante-bitget-*`)은 주식 팩토리(`dante-factory`, `dante-dashboard` 등)와 **완전 분리**된다.  
배포·업데이트·로그는 Bitget 전용 스크립트만 사용한다.

---

## systemd 유닛 (6 + 2 timer)

| 유닛 | 포트/역할 |
|------|-----------|
| `dante-bitget-ws.service` | Public/private WebSocket (ticker + orders) |
| `dante-bitget-factory.service` | 24/7 daemon (`bitget_auto_pilot`) |
| `dante-bitget-async.service` | Bitget 전용 Telegram 큐 |
| `dante-bitget-dashboard.service` | Streamlit 관제탑 **:8511** |
| `dante-bitget-heatmap.service` | 섹터 히트맵 **:8512** |
| `dante-bitget-watchdog.timer` | 5분 heartbeat 감시 |
| `dante-bitget-snapshot.timer` | 5분 CQRS DB snapshot |

**프로덕션:** dashboard/heatmap은 `sentinel.py` subprocess가 아니라 **별도 systemd 유닛**으로 운영한다.

---

## 최초 설치 (Ubuntu)

```bash
chmod +x bitget/deploy/bitget.sh bitget/deploy/update_bitget.sh bitget/deploy/deploy_bitget_factory.sh
INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot ./bitget/deploy/deploy_bitget_factory.sh

sudo systemctl start dante-bitget-ws dante-bitget-async dante-bitget-factory
sudo systemctl start dante-bitget-dashboard dante-bitget-heatmap
sudo systemctl start dante-bitget-watchdog.timer dante-bitget-snapshot.timer
```

cron (선택): `bitget/deploy/bitget.crontab.example` → `/etc/cron.d/dual-screener-bitget`

---

## 표준 업데이트 (git pull + 재기동)

```bash
sudo INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot ./bitget/deploy/update_bitget.sh
```

동작:

1. `BITGET_DB_STORAGE_PATH`(또는 legacy `bitget/`) SQLite 백업 → `/var/backups/bitget-pre-update/<UTC>/`
2. `git pull --ff-only`
3. systemd 유닛 재설치 (`deploy_bitget_factory.sh`)
4. Bitget 서비스 graceful stop → 재시작
5. **주식 `dante-*` 유닛은 건드리지 않음**

---

## One-shot 작업 (cron / 수동)

```bash
./bitget/deploy/bitget.sh --health
./bitget/deploy/bitget.sh --scan-all
./bitget/deploy/bitget.sh --daily-audit
./bitget/deploy/bitget.sh --reconcile
./bitget/deploy/bitget.sh --snapshot
./bitget/deploy/bitget.sh --gap-heal
./bitget/deploy/bitget.sh --record-baseline
./bitget/deploy/bitget.sh --validate
./bitget/deploy/bitget.sh --load-test
./bitget/deploy/bitget.sh --cutover-check
```

---

## Validation & Cutover (Phase 7)

1. `./bitget/deploy/bitget.sh --start-parallel` — 48h window
2. `./bitget/deploy/bitget.sh --record-baseline` — baseline 저장
3. `./bitget/deploy/bitget.sh --validate` — signal + PnL parity
4. 48h 후 `.env`에 `BITGET_PIPELINE_SSOT=1` → `--cutover-check`

Legacy `python -m bitget.main` / `factory_launcher`는 **deprecated** — prod는 systemd + cron only.

상세: `bitget/docs/implementation_phase_7.md`

또는:

```bash
python -m bitget.pipelines.runner --mode scan_all
```

---

## 환경 변수 (핵심)

| 변수 | 기본 | 설명 |
|------|------|------|
| `BITGET_DB_STORAGE_PATH` | `bitget/` | 데이터 SSOT 루트 |
| `BITGET_DASHBOARD_PORT` | 8511 | 관제탑 |
| `BITGET_HEATMAP_PORT` | 8512 | 히트맵 |
| `BITGET_ACCESS_KEY` / `SECRET` / `PASSPHRASE` | — | REST/WS private |
| `BITGET_SKIP_INLINE_TELEGRAM` | 1 (systemd) | inline queue off |
| `BITGET_ASYNC_TELEGRAM` | 1 (systemd) | async daemon 사용 |
| `ENABLE_REAL_EXECUTION` | false | 실주문 마스터 스위치 |
| `REAL_EXECUTION_DRY_RUN` | true | dry-run (기본) |

예시: `bitget/deploy/bitget_resource_limits.env.example`

---

## 로그

```bash
journalctl -u dante-bitget-factory -f
journalctl -u dante-bitget-ws -u dante-bitget-async -f
journalctl -u dante-bitget-dashboard -u dante-bitget-heatmap -f
journalctl -u dante-bitget-watchdog -u dante-bitget-snapshot --since "1 hour ago"
```

cron one-shot 로그: `bitget/logs/bitget_<mode>_*.log` (`BITGET_LOG_DIR`로 변경 가능)

---

## 전체 재시작 / 중지

```bash
sudo systemctl restart dante-bitget-ws dante-bitget-factory dante-bitget-async
sudo systemctl restart dante-bitget-dashboard dante-bitget-heatmap
sudo systemctl restart dante-bitget-watchdog.timer dante-bitget-snapshot.timer
```

```bash
sudo systemctl stop dante-bitget-factory dante-bitget-ws dante-bitget-async \
  dante-bitget-dashboard dante-bitget-heatmap
```

---

## 타이머 상태

```bash
systemctl list-timers dante-bitget-watchdog.timer dante-bitget-snapshot.timer --no-pager
```

---

## 장애 대응

| 증상 | 확인 | 조치 |
|------|------|------|
| WS stale / gap | `bitget.sh --gap-heal` | REST backfill, `dante-bitget-ws` 재시작 |
| heartbeat miss | `bitget.sh --watchdog` 로그 | factory/ws 재시작, `BITGET_WATCHDOG_*` 확인 |
| scan DB lock | snapshot stale | `bitget.sh --snapshot`, `BITGET_SNAPSHOT_MAX_STALE_SEC` |
| Telegram 적체 | `bitget_ops_events` gauge | `dante-bitget-async` 재시작 |
| 실주문 차단 | config `ENABLE_REAL_EXECUTION` | 의도적이면 OK; slippage/meta KILL_SWITCH 확인 |

---

## 데이터 경로 (SSOT)

`bitget/infra/data_paths.py`:

- `bitget_market_data.sqlite` — 쓰기
- `bitget_market_data_snapshot.sqlite` — CQRS 읽기
- `bitget_ops_events.sqlite` — ops heartbeat/gauge
- `bitget_message_queue.sqlite` — Telegram 큐

---

## 개발 vs 프로덕션

| 용도 | 진입점 |
|------|--------|
| 프로덕션 daemon | `dante-bitget-factory` → `run_bitget_daemon.sh` |
| 프로덕션 UI | `dante-bitget-dashboard` / `heatmap` |
| 로컬 dev (legacy) | `python -m bitget.factory_launcher` (sentinel subprocess) |
| cron SSOT | `bitget.sh` / `pipelines.runner` |

---

## 관련 문서

- `bitget/docs/README.md` — 구현 phase 인덱스
- **`bitget/docs/ubuntu_isolated_deploy_guide.md`** — Ubuntu에서 주식·Bitget 격리 배포
- `bitget_architecture_upgrade_plan.md` — 전체 설계 (repo root)
