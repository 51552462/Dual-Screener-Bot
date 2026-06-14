# 04 — Phase 5 실행 보고서 (위성·스캐너 Config 통합 + 배포 가이드)

> **작성일:** 2026-06-14  
> **선행 작업:** `03_phase3_4_execution_report.md`  
> **수정 범위:** `bitget/` only

---

## 0. Executive Summary

Phase 5 목표는 **위성·스캐너·마이너 모듈의 JSON 직접 I/O 제거**와 **단일 config 진입점(`config_hub`) 통일**이었다.

| 항목 | 결과 |
|------|------|
| 위성 모듈 config_hub 이관 | ✅ 14개 모듈 |
| DB 경로 `data_paths` SSOT | ✅ 주요 스캐너·마이너 |
| JSON runtime read/write (위성) | ✅ 0건 (grep 검증) |
| deploy 가이드 보강 | ✅ `deploy_bitget_factory.sh` |

---

## 1. Config SSOT 아키텍처 (After)

```
모든 위성·스캐너·마이너
        │
        ▼
  bitget.config_hub
   load_config() / save_config() / save_config_atomic()
        │
        ▼
  bitget.infra.config_manager
   bitget_system_config.sqlite (KV)
        │
        └── bootstrap only → bitget_system_config.json
```

**`config_hub.py` 확장:**

```python
def load_config():
    return config_manager.load_system_config() or {}

def load_system_config():  # signal_engines 호환
    return load_config()

save_config = save_config_atomic
```

---

## 2. 이관 완료 모듈 목록

| 모듈 | 변경 내용 |
|------|-----------|
| `supernova_hunter.py` | `config_hub` + `market_data_db_path()` |
| `master_scanner.py` | `_load_system_config()` → `config_hub` |
| `signal_engines.py` | `load_system_config` → `config_hub` alias |
| `executor.py` | `_load_config()` → `config_hub` |
| `blackhole_hunter.py` | config + DB paths |
| `shadow_performance_tracker.py` | `_load/_save_config` → `config_hub` |
| `doomsday_bot.py` | JSON load/save 제거 |
| `underdog_miner.py` | config + DB paths |
| `toxic_graveyard_analyzer.py` | config + DB paths |
| `time_machine_backtester.py` | config + DB paths |
| `synthetic_data_generator.py` | config_hub |
| `pump_forensics.py` | config + DB paths (`forensics_pioneer`가 import) |
| `data_miner.py` | **2개 블록** 모두 config_hub + `flow_csv_path()` |

### 2.1 대표 스니펫 (Before → After)

**Before (`supernova_hunter.py`):**
```python
CONFIG_PATH = os.path.join(BASE_DIR, "bitget_system_config.json")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)
```

**After:**
```python
from bitget.config_hub import load_config, save_config
from bitget.infra.data_paths import market_data_db_path

DB_PATH = market_data_db_path()
# load_config / save_config — config_hub 위임 (SQLite SSOT)
```

**Before (`master_scanner.py`):**
```python
def _load_system_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)
```

**After:**
```python
def _load_system_config():
    from bitget.config_hub import load_config
    return load_config()
```

---

## 3. 데이터 경로 SSOT 정리

| 상수 | SSOT 함수 |
|------|-----------|
| `bitget_market_data.sqlite` | `market_data_db_path()` |
| `Supernova_Flow_Tracking_Master.csv` | `flow_csv_path()` |
| `bitget_system_config.sqlite` | `system_config_db_path()` (config_manager 내부) |

위성 모듈의 `BASE_DIR + "bitget_market_data.sqlite"` 하드코딩을 제거하여 **서버 `BITGET_DB_STORAGE_PATH` 변경 시에도 동일 DB 참조**.

---

## 4. 배포·운영 보강

### 4.1 `deploy_bitget_factory.sh`

설치 완료 시 필수 env 안내 추가:

```bash
echo "[deploy_bitget] ensure bitget/.env contains:"
echo "  BITGET_WATCHDOG_HEARTBEAT_COMPONENT=bitget_auto_pilot"
echo "  BITGET_DB_STORAGE_PATH=<optional data root>"
```

### 4.2 systemd (기존 유지·확인)

`dante-bitget-factory.service.in` — 이미 올바른 순서:

```
After=network-online.target dante-bitget-ws.service
ExecStart=.../run_bitget_daemon.sh  → bitget_auto_pilot --daemon
```

### 4.3 검증 명령

```bash
# 위성 모듈 JSON 직접 open 없음
rg 'with open\(CONFIG|bitget_system_config\.json' bitget --glob '*.py' \
  | rg -v 'config_manager|data_paths|docs|comment'

# config round-trip
python -c "
from bitget.config_hub import load_config, save_config
c = load_config()
c['_phase5_probe'] = 'ok'
save_config(c)
assert load_config().get('_phase5_probe') == 'ok'
print('config_hub OK')
"

# health
./bitget/deploy/bitget.sh --health
```

---

## 5. Phase 1~5 통합 SSOT (최종)

| 계층 | SSOT |
|------|------|
| **24/7 daemon** | `bitget.pipelines.bitget_auto_pilot --daemon` |
| **cron one-shot** | `bitget/deploy/bitget.sh` → `pipelines.runner` |
| **파이프라인 prelude** | `meta_sync` → `artifact_guard` → body |
| **Config** | `config_hub` → `config_manager` → SQLite |
| **Meta/Kelly** | `governance/meta_sync` + `governance/meta_consumer` |
| **Forward DB** | `infra/data_paths.market_data_db_path()` |
| **레거시 진입점** | `main.py`, `factory_launcher`, `sentinel` → **blocked** |

---

## 6. 남은 후속 (Phase 6+)

| 항목 | 설명 |
|------|------|
| `forward/reports.py` deep_dive SQL | bug #2 바인딩 수정 |
| `data_miner.py` 중복 모듈 블록 | line 419+ 두 번째 import 블록 구조 정리 (기능 영향 없음, 가독성) |
| `shadow_tracking.DB_PATH` | `data_paths` 위임 (선택) |
| 통합 pytest | pipeline + config_hub integration test |

---

## 7. 변경 파일 목록

- `bitget/config_hub.py`
- `bitget/supernova_hunter.py`
- `bitget/master_scanner.py`
- `bitget/signal_engines.py`
- `bitget/executor.py`
- `bitget/blackhole_hunter.py`
- `bitget/shadow_performance_tracker.py`
- `bitget/doomsday_bot.py`
- `bitget/underdog_miner.py`
- `bitget/toxic_graveyard_analyzer.py`
- `bitget/time_machine_backtester.py`
- `bitget/synthetic_data_generator.py`
- `bitget/pump_forensics.py`
- `bitget/data_miner.py`
- `bitget/deploy/deploy_bitget_factory.sh`
- `bitget/docs/04_phase5_satellite_config.md`
- `bitget/docs/README.md`

**루트 주식 파일: 미수정** ✅

---

*Phase 5 완료. Phase 6(버그 수정·회귀 테스트) 승인 시 진행.*
