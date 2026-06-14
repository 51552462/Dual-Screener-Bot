# Bitget 구현 기록 — Phase 0 · 1 · 2

> 작성일: 2026-06-07  
> 원칙: **주식 루트 모듈 미수정**, Bitget 전용 코드는 **`bitget/` 하위만** 사용.

---

## 1. Phase 0 — 기반 수리

| 파일 | 설명 |
|------|------|
| `bitget/infra/logging_setup.py` | Bitget 전용 로깅 (stderr + ops SQLite handler) |
| `bitget/infra/data_paths.py` | `BITGET_DB_STORAGE_PATH` 등 경로 SSOT |
| `bitget/deploy/bitget.sh` | cron/systemd one-shot 래퍼 (v0→v1) |
| `bitget/deploy/bitget_resource_limits.env.example` | env 템플릿 |

### 변경 사항
- Dashboard 포트 **8511**, Heatmap **8512** (`main.py`, `sentinel.py`)
- 루트 `bitget_logger.py` **삭제** → `bitget.infra.logging_setup` 로 통합

---

## 2. Phase 1 — Infrastructure Layer

| 파일 | 주식 대응 |
|------|-----------|
| `bitget/infra/ops_logger.py` | `ops_logger.py` |
| `bitget/infra/config_manager.py` | `config_manager.py` |
| `bitget/infra/runtime.py` | `factory_runtime.py` |
| `bitget/watchdog.py` | `watchdog.py` |
| `bitget/pipelines/runner.py` | `system_auto_pilot.run_factory_cli` (초기 health) |

### DB (기본: `bitget/` 또는 `BITGET_DB_STORAGE_PATH`)
- `bitget_market_data.sqlite`
- `bitget_system_config.sqlite`
- `bitget_ops_events.sqlite`

---

## 3. Phase 2 — Pipeline Orchestration

| 파일 | 설명 |
|------|------|
| `bitget/pipelines/scanner_hooks.py` | **스캐너 Hooking 레이어** (조건식/시그널 로직 미포함) |
| `bitget/pipelines/bitget_pipelines.py` | StepSpec 파이프라인 SSOT |
| `bitget/pipelines/bitget_auto_pilot.py` | 24/7 pipeline orchestrator daemon |
| `bitget/pipelines/runner.py` | `--mode` CLI 진입점 |
| `bitget/job_cli.py` | runner 위임 (하위 호환) |
| `bitget/deploy/bitget.crontab.example` | 24H cron SSOT |
| `bitget/deploy/entrypoints/run_bitget_daemon.sh` | systemd용 daemon entry |

> 상세 Hooking 기록: [implementation_phase_2_3_hooking.md](./implementation_phase_2_3_hooking.md)

### 지원 모드 (`bitget.sh` / `runner --mode`)

| 모드 | 파이프라인 |
|------|------------|
| `health` | infra 자가진단 |
| `watchdog` | heartbeat stale 감지 |
| `data_refresh` | MTF OHLCV full update |
| `scan_spot` / `scan_futures` / `scan_all` | supernova + master_scanner (hooks) + track |
| `track_positions` | spot/futures virtual ledger |
| `daily_audit` | sentiment → track → deep dive → report → reconcile |
| `weekly_evolution` | `run_autonomous_analysis` |
| `reconcile` | OMS reconciliation |

### `main.py` / daemon 역할 분리
- scan/track/reconcile 위성 `_periodic_runner` 스레드 **제거** → cron `bitget.sh` SSOT
- `bitget_auto_pilot --daemon`: OMS + 위성 + daily_audit + supernova sniper만 관장
- legacy `main.py` inline threads (MTF updater, master scheduler, dashboards) **daemon에서 미생성**

### `master_scanner.py` / `supernova_hunter.py`
- 내부 조건식·시그널 로직 **미수정**
- `scanner_hooks.py`가 `run_scan()`, `execute_supernova_live_scan()` 등 기존 API만 호출

---

## 4. 디렉터리 구조 (Bitget 전용)

```
bitget/
├── infra/           # paths, logging, ops, config, runtime
├── pipelines/       # orchestration (Phase 2)
├── deploy/          # bitget.sh, crontab, entrypoints, env example
├── docs/            # 구현 기록 (본 파일)
├── logs/            # 기본 로그 (BITGET_LOG_DIR 미설정 시)
└── [domain modules] # scanner, forward_tester, oms, ...
```

---

## 5. 운영 명령

```bash
# infra 확인
./bitget/deploy/bitget.sh --health

# cron one-shot
./bitget/deploy/bitget.sh --scan-all
./bitget/deploy/bitget.sh --daily-audit
./bitget/deploy/bitget.sh --watchdog

# 24/7 (sentinel → main)
./bitget/deploy/bitget.sh --daemon
```

### 환경 변수 (`.env` 또는 `bitget/.env`)

```bash
BITGET_DB_STORAGE_PATH=/home/ubuntu/dante_data/bitget
BITGET_DASHBOARD_PORT=8511
BITGET_HEATMAP_PORT=8512
BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot
```

---

## 6. 다음 단계 (Phase 3+)

- [x] `forward_tester.py` → `bitget/forward/` 패키지 분리 (물리 분할 완료, [phase 2_3 hooking](./implementation_phase_2_3_hooking.md))
- [ ] 기존 모듈 `BASE_DIR` → `data_paths` 점진 이전
- [x] `dante-bitget-*` systemd 유닛 (`deploy/systemd/` under bitget)
- [x] WebSocket 데이터 레이어 (Phase 4 — 별도 문서)

---

## 7. 주식 시스템과의 격리 체크리스트

- [x] 루트 `factory_pipelines.py`, `forward/` 미수정
- [x] Bitget DB/ops/config 경로 prefix 분리 (`BITGET_*`)
- [x] Dashboard 포트 8511/8512 (주식 8501과 분리)
- [x] 루트에 Bitget Python 모듈 없음 (`bitget_logger.py` 제거)
- [x] cron SSOT: `bitget/deploy/bitget.crontab.example` (주식 cron과 별도)
