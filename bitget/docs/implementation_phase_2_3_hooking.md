# Bitget Phase 2 & 3 — 파이프라인 Hooking + forward/ 물리 분할

> 작성일: 2026-06-07  
> **원칙:** 주식 루트 모듈 미수정. 작업 범위는 **`bitget/` 하위만**.  
> **핵심 제약:** `master_scanner.py`, `supernova_hunter.py`, `signal_engines.py`의 조건식·시그널·타점 로직 **미수정** — Hooking(연결)만 수행.

---

## 1. Phase 2 — 선언형 파이프라인 + 오토파일럿

### 1.1 목표

| Before | After |
|--------|-------|
| `main.py` 내부 `_periodic_runner` 스레드가 scan/track 위성 실행 | cron `bitget.sh --mode` + `pipelines.runner` SSOT |
| 스캐너가 파이프라인과 직접 결합 | `scanner_hooks.py` 위임 레이어로 분리 |
| `bitget_auto_pilot.py`가 legacy loop 래핑 | OMS + 위성 + daily_audit + supernova sniper만 daemon 관장 |

### 1.2 신규/갱신 파일

| 파일 | 역할 |
|------|------|
| `bitget/pipelines/scanner_hooks.py` | **순수 위임** — 기존 스캐너 public API만 호출 |
| `bitget/pipelines/bitget_pipelines.py` | StepSpec 파이프라인 SSOT (모드별 체인) |
| `bitget/pipelines/bitget_auto_pilot.py` | 24/7 pipeline orchestrator daemon |

### 1.3 scanner_hooks — 알맹이 보존

```python
# bitget/pipelines/scanner_hooks.py (요약)
def run_master_scan(*, market_filter=None):
    from bitget.master_scanner import run_scan
    run_scan(market_filter=market_filter)

def run_supernova_live(market_type, timeframe="1H"):
    from bitget.supernova_hunter import execute_supernova_live_scan
    execute_supernova_live_scan(market_type, timeframe)

def run_supernova_sniper_scheduler():
    from bitget.supernova_hunter import run_live_sniper_scheduler
    run_live_sniper_scheduler()
```

**수정하지 않은 모듈 (알맹이):**
- `bitget/master_scanner.py`
- `bitget/supernova_hunter.py`
- `bitget/signal_engines.py`

### 1.4 파이프라인 체인 (scan 모드)

각 scan 스텝은 `_step_*` → `scanner_hooks` → 기존 함수 순으로 호출.

| 모드 | Step 순서 |
|------|-----------|
| `scan_spot` | config_bootstrap → artifact_guard → **supernova_spot** → **scan_spot** → track_spot |
| `scan_futures` | config_bootstrap → artifact_guard → **supernova_futures** → **scan_futures** → track_futures |
| `scan_all` | guard → gap_heal → data_refresh → supernova_spot → scan_spot → supernova_futures → scan_futures → track_spot → track_futures → shadow_eval |

- `critical=True`: master scan, track (실패 시 파이프라인 exit code 반영)
- `critical=False`: supernova, gap_heal, data_refresh 등 (실패해도 다음 스텝 진행)

### 1.5 bitget_auto_pilot — 역할 분리

```
┌─────────────────────────────────────────────────────────┐
│  cron / systemd timer  →  bitget.sh --mode              │
│    scan_all, scan_spot, track_positions, reconcile, …   │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  dante-bitget-daemon  →  bitget_auto_pilot --daemon     │
│    • heartbeat (60s)                                    │
│    • supernova sniper thread (run_live_sniper_scheduler)  │
│    • OMS hourly reconciliation                          │
│    • satellite modules (_safe_run_satellite, 기존 유지)   │
│    • daily_audit pipeline (UTC 날짜 변경 시 1회)         │
│    • weekly flow report (월요일)                        │
└─────────────────────────────────────────────────────────┘
```

**의도적으로 생성하지 않는 것 (main.py legacy):**
- MTF updater inline thread
- master scanner scheduler inline thread
- dashboard / heatmap inline thread
- disk manager inline thread
- `_periodic_runner` 위성 스레드

### 1.6 운영 명령

```bash
# one-shot (cron SSOT)
./bitget/deploy/bitget.sh --scan-spot
./bitget/deploy/bitget.sh --scan-all
./bitget/deploy/bitget.sh --daily-audit
./bitget/deploy/bitget.sh --health

# 24/7 daemon
sudo systemctl restart dante-bitget-daemon
# entry: python -m bitget.pipelines.bitget_auto_pilot --daemon
```

---

## 2. Phase 3 — forward_tester 물리적 패키지 분할

### 2.1 목표

2600줄 `forward_tester.py` God Module → 주식 `forward/` 패턴의 `bitget/forward/` 패키지.  
기존 import 경로는 **facade**로 하위 호환 유지.

### 2.2 분할 전후

| 항목 | Before | After |
|------|--------|-------|
| `bitget/forward/_core.py` | ~2,599줄 (전체 구현) | **34줄** (re-export facade) |
| `bitget/forward/ledger.py` | re-export only | **~1,016줄** (실제 구현) |
| `bitget/forward_tester.py` | monolith | **~45줄** facade |

### 2.3 패키지 구조

```
bitget/forward/
├── __init__.py           # 공개 API re-export
├── shared.py             # DB/Config/Telegram, init_forward_db, funding cache (~330줄)
├── gates.py              # anti-pattern, alpha, ATR, DNA gates (~260줄)
├── ledger.py             # try_add_virtual_position, track_daily_positions (~1016줄)
├── execution_bridge.py   # log_real_execution, practitioner leaderboard (~188줄)
├── mutant.py             # generate_mutant_strategies, auto-tune brain (~200줄)
├── reports.py            # daily report (V104.1), deep dive (~350줄)
└── _core.py              # 하위 호환 aggregate facade (34줄)
```

### 2.4 분할 스크립트

```bash
python bitget/scripts/split_forward_physical.py
```

- AST로 `_core.py` 함수 단위 추출
- 모듈별 `MODULE_MAP`에 따라 물리 이동
- legacy `send_comprehensive_daily_report` (6분할 버전) 제거 → **V104.1 9분할**만 유지
- 순환 import 방지: `init_forward_db()` 내부 auto-tune은 `mutant._auto_tune_brain_from_closed_df` **lazy import**

### 2.5 import 호환성

```python
# 기존 (그대로 동작)
from bitget.forward_tester import try_add_virtual_position, track_daily_positions

# 신규 권장
from bitget.forward.ledger import try_add_virtual_position
from bitget.forward.shared import load_system_config
from bitget.forward.gates import compute_evolved_alpha_bonus_score
```

### 2.6 모듈 의존 관계

```
shared ─────────────────────────────────────────┐
   ↑                                            │
gates ← ledger                                   │
   ↑         ↑                                  │
mutant ← reports ← execution_bridge              │
   ↑_____________________________________________│
         init_forward_db (lazy → mutant)         │
```

---

## 3. 검증

### 3.1 import 테스트 (로컬)

```bash
python -c "
from bitget.forward_tester import try_add_virtual_position, init_forward_db
from bitget.forward.ledger import track_daily_positions
from bitget.pipelines.bitget_pipelines import get_pipeline
print([s.name for s in get_pipeline('scan_spot')])
"
# 기대: ['config_bootstrap', 'artifact_guard', 'supernova_spot', 'scan_spot', 'track_spot']
```

### 3.2 격리 체크

- [x] `bitget/master_scanner.py` — scan hooking만, 내부 로직 미수정
- [x] `bitget/supernova_hunter.py` — hooking만, 내부 로직 미수정
- [x] `bitget/signal_engines.py` — 미수정
- [x] 루트 `factory_pipelines.py`, `forward/`, `system_auto_pilot.py` — 미수정

---

## 4. 관련 문서

| 문서 | 내용 |
|------|------|
| [implementation_phase_0_1_2.md](./implementation_phase_0_1_2.md) | Phase 0–2 인프라·파이프라인 개요 |
| [implementation_phase_3.md](./implementation_phase_3.md) | forward/ 패키지 SSOT·호환성 |
| [ubuntu_isolated_deploy_guide.md](./ubuntu_isolated_deploy_guide.md) | `dante-bitget-*` systemd 배포 |
| [../RUNBOOK.md](../RUNBOOK.md) | 운영 런북 |
