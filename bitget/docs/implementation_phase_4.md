# Bitget Phase 4 — WebSocket · data_paths · deploy

> 작성일: 2026-06-07  
> **주식 루트 미수정** — 모든 변경 `bitget/` 내부.

---

## Phase 4 — WebSocket Data Layer

| 파일 | 역할 |
|------|------|
| `bitget/data/stream_buffer.py` | 스레드 세이프 ticker 캐시 |
| `bitget/data/ws_public.py` | Bitget V2 public WS (aiohttp) |
| `bitget/data/ws_supervisor.py` | WS daemon CLI |
| `bitget/data/gap_healer.py` | 버퍼 stale 시 REST `mtf_data_updater` |
| `bitget/trading/slippage_guard.py` | WS spread bps pre-trade gate |

### 파이프라인

- `bitget.sh --gap-heal` → `gap_heal` mode
- `scan_all` / `data_refresh` 파이프라인 선행 step으로 `gap_heal` 포함

### 실행

```bash
./bitget/deploy/bitget.sh --ws-supervisor          # foreground WS
python -m bitget.data.ws_supervisor --symbols BTCUSDT,ETHUSDT
./bitget/deploy/bitget.sh --gap-heal
```

---

## data_paths SSOT 이전 (Phase 4)

| 모듈 | 변경 |
|------|------|
| `mtf_data_updater.py` | `market_data_db_path()`, `charts_dir()` |
| `master_scanner.py` | `market_data_db_path()`, `system_config_json_path()` |
| `schedule_lock.py` | `schedule_lock_state_path()` |
| `config_hub.py` | `system_config_json_path()` |

---

## Phase 6 (부분) — systemd · Telegram 격리

| 파일 | 역할 |
|------|------|
| `bitget/async_telegram_daemon.py` | `bitget_message_queue.sqlite` 전용 |
| `bitget/deploy/systemd/dante-bitget-*.service.in` | WS / factory / async |
| `bitget/deploy/systemd/dante-bitget-watchdog.timer` | 5분 watchdog |
| `bitget/deploy/deploy_bitget_factory.sh` | Ubuntu install (equity 유닛 미터치) |
| `bitget/deploy/entrypoints/run_bitget_ws.sh` | WS entry |
| `bitget/deploy/entrypoints/run_bitget_async.sh` | async Telegram entry |

### Ubuntu 설치

```bash
chmod +x bitget/deploy/deploy_bitget_factory.sh bitget/deploy/bitget.sh
INSTALL_ROOT=/home/ubuntu/Dual-Screener-Bot ./bitget/deploy/deploy_bitget_factory.sh
sudo systemctl start dante-bitget-ws dante-bitget-async dante-bitget-factory
```

---

## 환경 변수 (추가)

```bash
BITGET_WS_PUBLIC_URL=wss://ws.bitget.com/v2/ws/public
BITGET_WS_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
BITGET_WATCHDOG_RESTART_CMD="sudo systemctl restart dante-bitget-factory"
```

---

## 다음 단계

- [x] `ws_private.py` — fill/position stream (Phase 4b)
- [x] `executor.py`에 `slippage_guard` 연동
- [x] CQRS snapshot service (`bitget_market_data_snapshot.sqlite`)
- [ ] `forward/_core.py` 물리 분할 (Phase 3 잔여)
- [ ] Phase 5 OMS upgrade
