# CAT-A · 오케스트레이션 & 스케줄링 (Bitget)

> **위험도** 🟡 Medium · **Tier T2** · **also_load**: MAP, SPOT-FUT, CONSTANTS  
> **never_with**: CAT-B schema, CAT-L deploy paths 동시 설계

---

## 1. 역할

24/7 데몬 + cron factory 파이프라인. mode→step 순서, 글로벌 락, prelude(guard·meta sync), heartbeat.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| mode→StepSpec | `pipelines/bitget_pipelines.py` |
| flock·dispatch | `infra/runtime.py` |
| 마스터 진입 (cron) | `pipelines/runner.py` (`python -m bitget.pipelines.runner --mode`) |
| 24/7 데몬 SSOT | `pipelines/bitget_auto_pilot.py --daemon` |
| deploy wrapper | `deploy/bitget.sh`, `deploy_bitget_factory.sh` |
| watchdog | `watchdog.py` |
| factory launcher | `factory_launcher.py` |

---

## 3. 두 스케줄러 (핵심 개념)

1. **데몬** `bitget_auto_pilot --daemon` — KST loop, heartbeat, 위성 subprocess (HTC·toxic·data refresh)
2. **Factory cron** `bitget.sh --mode` — **동기**, Bitget lock 직렬, scan/audit/report

**프로덕션 SSOT**: `dante-bitget-factory` → `bitget_auto_pilot --daemon`  
**레거시 제거 대상**: `main.py` 직접 loop, `auto_pilot.system_main_loop` 이중 실행

---

## 4. BITGET_MODES (주요)

| Mode | 용도 |
|------|------|
| `scan_all` / `scan_spot` / `scan_futures` | 데이터 + 스캔 + track |
| `track_positions` | 가상 포지션 추적 |
| `daily_audit` | sentiment → track → deep_dive → report → overseer → reconcile |
| `weekly_evolution` | autonomous analysis |
| `reconcile` / `data_refresh` / `gap_heal` / `snapshot` | OMS·데이터 |
| `health` / `watchdog` / validation modes | 인프라·Phase 7 cutover |

---

## 5. Prelude 패턴 (주식 factory_pipelines 대응)

**목표 SSOT** (⚠️ 부분 누락 — `01_architecture_mapping`):

```
artifact_guard → meta_governor_sync → (sentiment) → domain steps
```

| Step | Bitget 파일 |
|------|-------------|
| artifact_guard | `infra/artifact_guard.py` |
| meta sync | `governance/meta_sync.py` |
| regime hydrate | `governance/meta_consumer.py` |

---

## 6. 동시성 정책

| 메커니즘 | 정책 |
|----------|------|
| Bitget runtime lock | 단일 flock; stale PID/orphan |
| zombie pipeline | critical step fail → 잔여 skip |
| cron overlap | 50min wait → skip slot |
| Windows dev | flock **비활성** — Ubuntu prod SSOT |

---

## 7. systemd (CAT-L link)

| unit | role |
|------|------|
| dante-bitget-factory | `--daemon` |
| dante-bitget-ws | WebSocket (코인 전용) |
| dante-bitget-async | Telegram |
| dante-bitget-watchdog.timer | 5m |
| dante-bitget-snapshot.timer | 5m |

---

## 8. Claude 설계 대상

- mode 추가 시 StepSpec 순서·prelude 필수 여부
- legacy 진입점 제거 순서 (main.py / auto_pilot)
- `BITGET_PIPELINE_SSOT=1` cutover 조건
- heartbeat component name SSOT (`bitget.pipelines.bitget_auto_pilot`)

---

## 9. 진단 참조

- `docs/01_architecture_mapping_and_diagnosis.md` §2 (진입점 3중화)
- `validation/architecture_checks.py`
