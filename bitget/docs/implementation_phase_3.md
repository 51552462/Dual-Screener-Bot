# Bitget Phase 3 — forward/ 패키지 분리

> 작성일: 2026-06-07 (물리 분할 완료: 2026-06-07)  
> **주식 루트(`forward/`, `factory_pipelines.py` 등) 수정 없음** — git status 기준 변경은 `bitget/` 내부만.

---

## 목표

2600줄 `forward_tester.py` God Module을 주식 `forward/` 패턴에 맞게 패키지화하고, 기존 import 경로는 facade로 유지.

---

## 최종 구조 (물리 분할 완료)

```
bitget/forward/
├── __init__.py           # 공개 API
├── shared.py             # DB/Config/Telegram, init_forward_db (~330줄)
├── gates.py              # anti-pattern, alpha, ATR helpers (~260줄)
├── ledger.py             # try_add_virtual_position, track_daily_positions (~1016줄)
├── reports.py            # deep_dive, comprehensive report V104.1 (~350줄)
├── mutant.py             # generate_mutant_strategies, auto-tune (~200줄)
├── execution_bridge.py   # log_real_execution, reconciliation (~188줄)
└── _core.py              # 하위 호환 aggregate facade (34줄)
```

| 파일 | Before | After |
|------|--------|-------|
| `_core.py` | ~2,599줄 monolith | **34줄** re-export facade |
| `forward_tester.py` | monolith | **~45줄** facade |

분할 도구: `bitget/scripts/split_forward_physical.py`

상세 기록: [implementation_phase_2_3_hooking.md](./implementation_phase_2_3_hooking.md) §2

---

## SSOT 변경

| Before | After |
|--------|-------|
| `BASE_DIR/bitget_market_data.sqlite` | `data_paths.market_data_db_path()` |
| `bitget_system_config.json` only | `config_manager` + JSON fallback |
| Telegram/env inline | `bitget/forward/shared.py` |

---

## 버그 수정 (Bitget only)

- `init_forward_db()` 내부 auto-tune SQL이 정의되지 않은 `market_type` 참조 → 전체 CLOSED trades 조회로 수정
- legacy 6분할 `send_comprehensive_daily_report` 중복 정의 제거 → V104.1 9분할만 유지

---

## 호환성

기존 코드는 그대로 동작:

```python
from bitget.forward_tester import try_add_virtual_position, track_daily_positions
```

신규 권장:

```python
from bitget.forward.ledger import try_add_virtual_position
from bitget.forward.shared import load_system_config
```

---

## 완료 체크리스트

- [x] `bitget/forward/` 패키지 생성
- [x] `forward_tester.py` facade 유지
- [x] `_core.py` → ledger/gates/reports/mutant/execution_bridge **물리적 분할**
- [x] import 호환성 검증
- [ ] `mtf_data_updater`, `master_scanner` 등 `data_paths` 점진 이전

---

## 격리 확인

**미수정 (주식):** `factory_pipelines.py`, `forward/`, `system_auto_pilot.py`, `deploy/systemd/dante-*`, `factory.sh` 등
