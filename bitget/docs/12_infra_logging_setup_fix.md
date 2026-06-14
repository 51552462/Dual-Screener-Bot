# 12 — `bitget.infra` 복구 및 `logging_setup` ImportError 수정

> **작성일:** 2026-06-14  
> **증상:** `dante-bitget-factory` 기동 시 `ModuleNotFoundError: No module named 'bitget.infra.logging_setup'`  
> **수정 범위:** `bitget/infra/` 복구, `bitget/governance/infra/` shim 정리

---

## 0. Executive Summary

| 항목 | 내용 |
|------|------|
| **증상** | `bitget_auto_pilot.py:161` — `from bitget.infra.logging_setup import setup_logging` 실패 |
| **근본 원인** | `bitget/infra/` 패키지가 **삭제·이동**되어 `governance/infra/` 만 남음 (git merge conflict 포함) |
| **해결** | `bitget/infra/` SSOT **8개 모듈 복구** (`logging_setup.py` 포함) |
| **`bitget_auto_pilot.py`** | import 경로 **변경 없음** (원래 올바름) |

---

## 1. 에러 로그

```
File ".../bitget/pipelines/bitget_auto_pilot.py", line 161, in system_main_loop
  from bitget.infra.logging_setup import setup_logging
ModuleNotFoundError: No module named 'bitget.infra.logging_setup'
```

### 1.1 해당 코드 (수정 불필요 — 정상)

```python
def system_main_loop() -> None:
    from bitget.infra.logging_setup import setup_logging
    from bitget.infra import ops_logger

    setup_logging(default_component="bitget.auto_pilot")
    ops_logger.install_unhandled_exception_hooks()
    ...
```

`ops_logger` 등 **동일 패키지**를 쓰는 모듈이 40곳 이상 — import 경로를 `ops_logger`만으로 바꾸는 것은 부적절.

---

## 2. 원인 분석

### 2.1 패키지 위치 오류

| 기대 (SSOT) | 실제 (서버/저장소) |
|-------------|-------------------|
| `bitget/infra/logging_setup.py` | **없음** |
| `bitget/infra/ops_logger.py` | **없음** |
| `bitget/governance/infra/logging_setup.py` | 존재 (잘못된 위치) |

`governance/` 는 **MetaGovernor·meta_sync** 용도이며, `infra/` (paths, logging, runtime)와 **혼합되면 안 됨**.

### 2.2 git merge conflict

`governance/infra/*.py` 전 파일에 `<<<<<<< HEAD` / `=======` / `>>>>>>>` 마커가 남아 **Python 구문 오류** 상태였음.

### 2.3 영향 범위

`bitget.infra.*` 를 import 하는 모듈 (일부):

- `pipelines/bitget_auto_pilot.py`, `pipelines/runner.py`
- `deploy/update_bitget.sh` (backup 시 `data_paths`)
- `executor.py`, `mtf_data_updater.py`, `watchdog.py`
- `governance/meta_sync.py`, `forward/shared.py`
- `data/ws_*.py`, tests 등

→ **`logging_setup` 하나만 고치면 다른 import도 연쇄 실패**했을 것.

---

## 3. 수정 내용

### 3.1 `bitget/infra/` 복구 (신규/복원)

`governance/infra/` 에서 merge conflict **HEAD 쪽** 내용을 추출해 SSOT 경로에 복원:

| 파일 | 역할 |
|------|------|
| `__init__.py` | 패키지 |
| `logging_setup.py` | `setup_logging`, `get_logger` |
| `ops_logger.py` | heartbeat, gauge, ops SQLite |
| `data_paths.py` | `BITGET_DB_STORAGE_PATH` SSOT |
| `config_manager.py` | SQLite config KV |
| `artifact_guard.py` | DB 스키마 heal |
| `runtime.py` | pipeline dispatch, flock |
| `snapshot_service.py` | CQRS snapshot |

### 3.2 `bitget/governance/infra/` → deprecated shim

잘못된 경로에 남은 파일을 **재export shim** 으로 교체 (하위 호환):

```python
"""Deprecated shim — use `bitget.infra.logging_setup`."""
from bitget.infra.logging_setup import *  # noqa: F401,F403
```

### 3.3 `bitget_auto_pilot.py`

**코드 변경 없음** — 패키지 복구로 import 해결.

---

## 4. 서버 적용

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
git pull

# import 검증
source venv/bin/activate
python -c "from bitget.infra.logging_setup import setup_logging; setup_logging(); print('logging OK')"

# factory 재시작
sudo systemctl restart dante-bitget-factory
sudo journalctl -u dante-bitget-factory -f
```

**기대 로그:**

```
[bitget_auto_pilot] pipeline orchestrator started (OMS + satellites + daily_audit)
daemon boot artifact guard: {...}
```

---

## 5. 검증 명령

```bash
python -c "
from bitget.infra.logging_setup import setup_logging, get_logger
from bitget.infra import ops_logger
from bitget.pipelines.bitget_auto_pilot import system_main_loop
print('all imports OK')
"
```

---

## 6. 재발 방지

| 규칙 | 설명 |
|------|------|
| `bitget/infra/` | paths·logging·runtime·config **유일 SSOT** |
| `bitget/governance/` | meta_sync, meta_consumer **만** |
| merge conflict | `<<<<<<<` 마커 커밋 금지 — push 전 `grep -R '<<<<<<<' bitget/` |
| 배포 후 | `python -c "from bitget.infra.logging_setup import setup_logging"` 1회 |

---

## 7. 변경 파일

| 파일 | 변경 |
|------|------|
| `bitget/infra/*.py` | **복구** (8 files) |
| `bitget/governance/infra/*.py` | deprecated shim |
| `bitget/docs/12_infra_logging_setup_fix.md` | 본 문서 |
| `bitget/pipelines/bitget_auto_pilot.py` | **변경 없음** |

---

## 8. 요약

- **삭제·이동된 것은 `logging_setup` 함수가 아니라 `bitget/infra/` 패키지 전체**
- **해결:** SSOT 경로 `bitget/infra/logging_setup.py` 복구
- **서버:** `git pull` → `systemctl restart dante-bitget-factory`
