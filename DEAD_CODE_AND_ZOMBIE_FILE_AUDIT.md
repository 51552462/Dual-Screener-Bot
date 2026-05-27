# 데드 코드·좀비 파일 전수 감사 (DEAD_CODE_AND_ZOMBIE_FILE_AUDIT)

**작성일:** 2026-05-27  
**범위:** 로컬 `Dual-Screener-Bot` (Python 약 270개, 운영·테스트·deploy 포함)  
**제약:** 본 감사는 **코드 삭제·이동을 수행하지 않음**. 분석·역제안만 기록.

---

## 분석 방법론

### 엔트리포인트 (SSOT)

| 우선순위 | 진입점 | 용도 |
|----------|--------|------|
| **P0** | `factory.sh` → `system_auto_pilot.py --mode <…>` | Ubuntu cron **유일 공식** 팩토리 파이프라인 |
| **P0** | `factory_pipelines.py` + `factory_runtime.py` | 모드별 Step DAG (`scan_kr`, `daily_audit_us`, `weekly_master` 등) |
| **P1** | `system_auto_pilot.system_main_loop()` | `--daemon` / 인자 없이 실행 시 24h 루프 (**cron 금지**) |
| **P1** | `system_auto_pilot._spawn_py_script()` 등 | 데몬이 `subprocess`로 기동하는 위성 스크립트 |
| **P2** | `main.py` | 구 스케줄러 허브 (`QUANT_ENABLE_SYSTEM_AUTO_PILOT=1` 아니면 `system_main_loop` **비활성**) |
| **P2** | `async_telegram_daemon.py` | 텔레그램 큐 소비 (`DANTE_ASYNC_TELEGRAM_DAEMON=1`) |
| **별도 제품선** | `bitget_main.py` / `bitget/*` | 암호화폐·Bitget 팩토리 (**주식 factory.sh 와 무관**) |

### AST import 추적

- 모든 `.py`에 대해 `ast.parse` + `ast.walk`로 `import` / `from … import` 수집.
- 모듈명 → 파일 경로 해석 (`pkg/mod.py`, `pkg/__init__.py`, 루트 `mod.py`).
- **한계 (반드시 읽을 것):**
  - `importlib.import_module(variable)` 등 **동적 import** 미추적.
  - `subprocess` / `exec` 문자열 내 모듈명은 `_spawn_py_script("foo.py")` 및 `system_auto_pilot` 내 `import foo` 패턴만 **수동 보강**.
  - `from forward.shared import *` 로 가져온 `_` 접두 심볼은 정적 분석에서 호출처 누락 가능.
  - **데드 함수 판정**은 “파일 내 식별자 등장 횟수 ≤ 2” 휴리스틱(정의+1회) — **참고용**이며, 실제 데드 코드 확정에는 `vulture`/커버리지 필요.

### 수치 요약

| 집합 | 파일 수 (tests/deploy 제외) |
|------|---------------------------|
| 프로덕션 `.py` 전체 | **240** |
| **factory.sh SSOT** 에서 정적 도달 가능 (+ 데몬 spawn 보강) | **~132** |
| factory 기준 **고립(orphan)** | **109** |
| `tests/` | 28 |
| `deploy/` | 2 |

---

## 1. 좀비 파일 및 데드 코드 전수 색출 리스트 (P0)

### 1.1 factory.sh 기준 도달 가능 핵심 체인 (요약)

```
factory.sh
  └─ system_auto_pilot.run_factory_cli()
       ├─ factory_pipelines.get_pipeline(mode)
       │    ├─ factory_artifact_guard / meta_governor / sentiment_miner
       │    ├─ supernova_hunter (scan_kr / scan_us)
       │    ├─ kr.py / usa.py (optional bowl, 비필수 Step)
       │    ├─ auto_forward_tester → forward/{shared,ledger,deep_dive,...}
       │    ├─ data_updater, factory_us_health, cross_market_ssot
       │    ├─ sector_spillover_refresh, doomsday_bridge, ai_overseer
       │    └─ weekly_flow_report
       └─ factory_runtime.dispatch_factory_mode()
```

**도달 가능 모듈 예 (132개 중 대표):**  
`factory_*`, `forward/*`, `auto_forward_tester`, `supernova_hunter`, `data_updater`, `config_manager`, `meta_governor*`, `deathmatch_*`, `practitioner_*`, `report_*`, `telegram_message_queue`, `blackhole_hunter`, `incubator_engine`, `mutant_oos_validator`, `limit_up_forensics`, `toxic_graveyard_analyzer`, `synthetic_data_generator`, `time_machine_backtester`, `smart_money_tracker`, `macro_doomsday_bot`, `us_toxic_graveyard_analyzer`, `shadow_performance_tracker`, `forensics_pioneer`, `dna_schema_constants`, `legacy_archive/krx_equity_universe` (간접 import) 등.

### 1.2 factory 기준 고립 파일 — 전체 경로 (109개)

#### A. Bitget 병렬 스택 (79개) — 주식 팩토리와 **완전 분리**

**`bitget/` 패키지 (40)**

| 경로 |
|------|
| `bitget/__init__.py` |
| `bitget/ai_overseer.py` |
| `bitget/ai_report.py` |
| `bitget/alt_data_miner.py` |
| `bitget/auto_pilot.py` |
| `bitget/blackhole_hunter.py` |
| `bitget/charting.py` |
| `bitget/config_hub.py` |
| `bitget/dashboard.py` |
| `bitget/data_miner.py` |
| `bitget/disk_manager.py` |
| `bitget/doomsday_bot.py` |
| `bitget/env.py` |
| `bitget/executor.py` |
| `bitget/factory_launcher.py` |
| `bitget/forensics_pioneer.py` |
| `bitget/forward_tester.py` |
| `bitget/funding_fetcher.py` |
| `bitget/heatmap_dashboard.py` |
| `bitget/macro_doomsday_bot.py` |
| `bitget/main.py` |
| `bitget/manual_report_trigger.py` |
| `bitget/master_scanner.py` |
| `bitget/mtf_data_updater.py` |
| `bitget/oms.py` |
| `bitget/pump_forensics.py` |
| `bitget/rate_limit_guard.py` |
| `bitget/schedule_lock.py` |
| `bitget/sentiment_miner.py` |
| `bitget/sentinel.py` |
| `bitget/shadow_performance_tracker.py` |
| `bitget/shadow_tracking.py` |
| `bitget/signal_engines.py` |
| `bitget/supernova_hunter.py` |
| `bitget/symbol_utils.py` |
| `bitget/synthetic_data_generator.py` |
| `bitget/system_auto_pilot.py` |
| `bitget/time_machine_backtester.py` |
| `bitget/toxic_graveyard_analyzer.py` |
| `bitget/underdog_miner.py` |

**루트 `bitget_*` 호환 셈 (39)** — 위 패키지의 re-export, **이중 유지**

`bitget_ai_overseer.py`, `bitget_ai_report.py`, `bitget_alt_data_miner.py`, `bitget_auto_pilot.py`, `bitget_blackhole_hunter.py`, `bitget_charting.py`, `bitget_config_hub.py`, `bitget_dashboard.py`, `bitget_data_miner.py`, `bitget_disk_manager.py`, `bitget_doomsday_bot.py`, `bitget_env.py`, `bitget_executor.py`, `bitget_factory_launcher.py`, `bitget_forensics_pioneer.py`, `bitget_forward_tester.py`, `bitget_funding_fetcher.py`, `bitget_heatmap_dashboard.py`, `bitget_macro_doomsday_bot.py`, `bitget_main.py`, `bitget_manual_report_trigger.py`, `bitget_master_scanner.py`, `bitget_mtf_data_updater.py`, `bitget_oms.py`, `bitget_pump_forensics.py`, `bitget_rate_limit_guard.py`, `bitget_schedule_lock.py`, `bitget_sentiment_miner.py`, `bitget_sentinel.py`, `bitget_shadow_performance_tracker.py`, `bitget_shadow_tracking.py`, `bitget_signal_engines.py`, `bitget_supernova_hunter.py`, `bitget_symbol_utils.py`, `bitget_synthetic_data_generator.py`, `bitget_system_auto_pilot.py`, `bitget_time_machine_backtester.py`, `bitget_toxic_graveyard_analyzer.py`, `bitget_underdog_miner.py`

**근거:** `factory.sh` / `factory_pipelines` 어디에서도 `bitget` import 없음. Bitget 전용 cron·`bitget_main.py` 가 있다면 **별도 SSOT**로 관리해야 함.

---

#### B. `main.py` 데몬 전용 스캐너 봇 (8) — factory는 `supernova` + optional `kr`/`usa` 만 사용

| 경로 | 근거 |
|------|------|
| `main.py` | `QUANT_ENABLE_SYSTEM_AUTO_PILOT` 없으면 스케줄 루프 비활성; factory.sh 와 **이중 SSOT** |
| `master.py` | KR 마스터 시그널 봇 — `main.py`만 import |
| `kr.py` / `usa.py` | factory **optional** bowl Step에서만 사용 → orphan 목록 제외됨(도달 가능) |
| `nulrim.py` | KR 눌림목 — main 전용 |
| `ema5.py` | KR 5EMA — main 전용 |
| `us_master.py` | US 마스터 — main 전용 |
| `nulusa.py` | US 눌림 — main 전용 |
| `us_5ema.py` | US 5EMA — main 전용 |
| `dante_krx_reverse_breakout_screener.py` | KR 역추세 — main 전용 |
| `nasdaq_dante_reverse_breakout_screener.py` | US 역추세 — main 전용 |

**근거:** `factory_pipelines` Step 목록에 없음. `main.py` L50–99 일괄 import + 주말 패치.

---

#### C. 루트 기타 고립 (15)

| 경로 | 상태 | 근거 |
|------|------|------|
| `__init__.py` | 빈 패키지 마커 | import 그래프 루트 노이즈 |
| `ai_secretary.py` | 좀비(주식 factory) | `main.py` 스레드 전용; factory 미참조 |
| `alt_data_miner.py` | **중복·좀비** | `bitget/alt_data_miner.py` 와 별개; equity 파이프라인은 `sentiment_miner` / `data_miner` 사용 |
| `async_telegram_daemon.py` | **인프라** | `main.py`·env 로 기동; factory 직접 import 없음 (운영상 필요할 수 있음) |
| `dashboard.py` | 좀비 | Streamlit/로컬 대시보드; factory·cron 미연결 |
| `heatmap_dashboard.py` | **셈** | → `legacy_archive/heatmap_dashboard.py` |
| `factory_launcher.py` | 좀비(equity) | `factory.sh` 가 대체; Bitget은 `bitget/factory_launcher.py` |
| `forensics_pioneer.py` | **데몬 위성** | `system_main_loop` spawn — factory cron 아님 |
| `manual_report_trigger.py` | 운영 수동 | CLI one-shot |
| `dante_snapshot_runner.py` | 미연결 | 정적 import 0 (스냅샷 유틸 추정) |
| `practitioner_bitget_adapter.py` | Bitget 브리지 | equity factory 미사용 |
| `spillover_observe_log.py` | **준-데드** | `master.py` 등 main 트리에서만 호출 |
| `watchdog.py` | **인프라** | systemd heartbeat; factory 그래프 밖 |

---

#### D. `legacy_archive/` (이미 격리, 5)

| 경로 |
|------|
| `legacy_archive/heatmap_dashboard.py` |
| `legacy_archive/scripts/migrate_bitget_package.py` |
| `legacy_archive/scripts/repair_forward_trades_numeric_corruption.py` |
| `legacy_archive/scripts/split_forward_tester.py` |
| `legacy_archive/scripts/__init__.py` |

**근거:** 1차 격리 완료. `heatmap_dashboard.py` 는 루트 셈 `heatmap_dashboard.py` 로만 간접 노출.

---

#### E. `scripts/` 운영·마이그레이션 (3)

| 경로 | 용도 |
|------|------|
| `scripts/migrate_bitget_package.py` | 일회성 패키지 이전 |
| `scripts/repair_forward_trades_numeric_corruption.py` | DB 수리 |
| `scripts/split_forward_tester.py` | forward 패키지 분리 도구 |

---

### 1.3 도달 가능 파일 내 **멈춘 함수·클래스** (데드 코드 후보)

> 아래는 정적 휴리스틱·코멘트 교차검증 결과. **삭제 전 grep/테스트 필수.**

| 파일 | 후보 심볼 | 근거 |
|------|-----------|------|
| `system_auto_pilot.py` | `run_autonomous_analysis()` (~L371) | `sector_spillover_refresh` 모듈이 엔진 1.6/12.5 **대체**; factory cron은 `--mode`만 사용. `--run-autonomous-analysis-only` / 레거시 데몬에서만 호출 |
| `system_auto_pilot.py` | `system_main_loop()` (~L1849) | `factory.sh` SSOT와 **이중**; `main.py`도 기본 비활성 |
| `auto_forward_tester.py` | `run_daily_scheduler()` | docstring: DISABLED — cron은 `factory.sh` |
| `forward/shared.py` | (다수) reporter 전용 heal 함수 | factory Step으로 분리됐으나 deep_dive·리포트에서 사용 — **삭제 금지** |
| `main.py` | `run_db_updater_scheduler()`, `run_us_pipeline_heartbeat()` | factory가 `data_updater.run_us_incremental_db_update` 등으로 **대체** |
| Bitget 패키지 전반 | `run_scheduler()`, `system_main_loop()` 등 | equity factory 미도달; Bitget cron 내부에서만 의미 |

**Bitget `bitget/forward_tester.py` 등** — 패키지 내부에서는 `main`·`auto_pilot`이 쓰지만, 주식 factory 기준으로는 전체 파일이 고립.

---

## 2. 완전 삭제 vs 안전 격리(Quarantine) 역제안 (P0)

### 2.1 즉시 삭제 후보 (Trash) — equity factory 기준 영향 ≈ 0%

| 대상 | 조치 | 이유 |
|------|------|------|
| 루트 `bitget_*.py` 39개 셈 | **삭제** (선행: Bitget cron이 `bitget/` 직접 import 하도록 정리) | `bitget/foo.py` 와 100% 중복; import 혼선만 유발 |
| `__init__.py` (루트, 비어 있으면) | 삭제 | 패키지 아님 |
| `scripts/migrate_bitget_package.py` | 삭제 또는 `legacy_archive/scripts/` 로 이동 | 마이그레이션 완료 가정 |
| `legacy_archive/scripts/split_forward_tester.py` | 삭제 | forward 분리 완료 |
| `heatmap_dashboard.py` (루트 셈) | 삭제 | `legacy_archive` 직접 import 로 교체 후 |

**주의:** Bitget 운영 중이면 `bitget_*` 삭제는 **Bitget 파이프라인 grep·cron 점검 후** 단계적 수행.

---

### 2.2 `legacy_archive/` 격리 유지 (Quarantine)

| 대상 | 조치 |
|------|------|
| `legacy_archive/heatmap_dashboard.py` | 유지 — 루트 셈 제거 후 단일 경로화 |
| `legacy_archive/scripts/repair_forward_trades_numeric_corruption.py` | 유지 — 운영 수리용 |
| `legacy_archive/krx_equity_universe.py` | 유지 — 간접 import 가능성 |

---

### 2.3 격리 후보 (삭제 X, 폴더 이동·문서화)

| 대상 | 제안 위치 | 이유 |
|------|-----------|------|
| `main.py` + 8개 스캐너 봇 | `legacy_archive/scanners/` | factory SSOT 이전; `supernova_hunter` + `kr`/`usa` bowl 만 유지 |
| `dashboard.py`, `heatmap_dashboard` 계열 | `legacy_archive/dashboards/` | 로컬 시각화 |
| `alt_data_miner.py` (루트) | `legacy_archive/` | Bitget·equity 중복, 미참조 |
| `factory_launcher.py` | `legacy_archive/` | `factory.sh` 대체 |
| `dante_snapshot_runner.py` | `legacy_archive/` 또는 삭제 전 사용처 조사 | 미연결 |
| `ai_secretary.py` | `legacy_archive/` 또는 `services/` | main 데몬 전용 |
| `manual_report_trigger.py` | `scripts/` | 수동 ops |

---

### 2.4 유지 필수 (factory 밖이어도)

| 대상 | 이유 |
|------|------|
| `async_telegram_daemon.py` | 텔레그램 큐 SSOT (`DANTE_ASYNC_TELEGRAM_DAEMON`) |
| `watchdog.py` | systemd heartbeat·자동 재시작 |
| `tests/` (28) | CI·회귀 |
| `bitget/` 전체 | **별도 제품** — equity 정리 시 삭제하지 말고 Bitget SSOT 문서화 |
| 데몬 위성 (`incubator_engine`, `limit_up_forensics`, …) | `system_main_loop` spawn — cron을 데몬 없이만 쓸 경우 **별도 cron 등록** 또는 spawn 목록 문서화 |

---

### 2.5 이중 SSOT 정리 (논리 삭제)

| 레거시 | 현재 SSOT |
|--------|-----------|
| `main.py` 24h 스케줄 | `factory.sh` + systemd timer |
| `auto_forward_tester.run_daily_scheduler` | `factory_pipelines` |
| `system_auto_pilot.system_main_loop` (cron) | `run_factory_cli --mode` |
| `run_autonomous_analysis` 일괄 블록 | `meta_governor_sync` + `sector_spillover_refresh` Step |

---

## 3. 리소스 최적화 및 구조 압축 제안

### 3.1 현 구조 유지 전제 압축 (낮은 리스크)

1. **`forward/` 패키지** — 이미 `auto_forward_tester` 분리 완료.  
   - 추가: `forward/report_sections/` 로 `*_report_section.py` 3개 이동 (import 경로만 변경).  
   - 효과: 루트 240파일 → 체감 복잡도 감소, factory 도달성 동일.

2. **리포트·DNA 클러스터** — 루트에 흩어진 모듈을 `reports/` 패키지로 묶기 (이동만).  
   - 후보: `daily_report_context`, `practitioner_report_context`, `colosseum_report_context`, `forward_report_scalar`, `forward_report_tier`, `report_formatter`, `report_collectors`, `report_staleness_gate`, `report_timekeeper`, `report_state_binder`  
   - **통합하지 말 것:** 파일 병합은 diff 폭증 — **디렉터리 이동만**.

3. **Deathmatch / ACE 클러스터** — `ace_*.py` (11), `deathmatch_*.py` (6) → `evolution/` 또는 `deathmatch/` 디렉터리.  
   - factory Step·`ai_overseer` import 경로 일괄 수정.

4. **Bitget 셈 제거** — `bitget/` 단일 트리만 남기고 루트 `bitget_*` 39파일 삭제 시 **파일 수 ~16% 감소**.

### 3.2 중간 리스크 (기능 검증 필요)

5. **`main.py` 스캐너 군 통폐합**  
   - `master` / `nulrim` / `ema5` / `kr_rev` 등은 OHLCV·시그널·텔레그램 패턴 80% 동일.  
   - 제안: `scanners/base_kr_bot.py` 추상 + 시장별 `compute_signal` 플러그인 — **동작 동일성 회귀 테스트 필수**.  
   - 단기: 격리만; 중기: 통폐합.

6. **`system_auto_pilot.py` (~2160줄) 슬라이스**  
   - `run_autonomous_analysis` 레거시 블록 → `legacy_archive/autonomous_analysis_legacy.py` 로 이동 후 factory에서 import 제거.  
   - `system_main_loop` spawn 테이블 → `factory_satellite_registry.py` (이름·cron·스크립트 SSOT).

7. **`config_manager` vs `system_config_atomic`**  
   - 역할 중복 검토: KV SQLite vs JSON atomic — 문서화 후 하나로 수렴(장기).

### 3.3 하지 말아야 할 압축

- `supernova_hunter.py` + `scanner_funnel.py` 병합 — 파일 과대·병렬 스캔 경계 흐림.  
- `factory_pipelines.py` 와 `factory_runtime.py` 병합 — Step DAG 가독성 저하.  
- `forward/shared.py` (~2100줄) 급격한 분할 — `import *`·리포트 경로 광범위; **단계적**만.

### 3.4 목표 아키텍처 (참고)

```
Dual-Screener-Bot/
├── factory.sh              # 유일 cron SSOT
├── system_auto_pilot.py    # CLI + (선택) daemon
├── factory_pipelines.py
├── factory_runtime.py
├── forward/                # equity 장부·리포트
├── scanners/               # (제안) legacy main.py 봇
├── reports/                # (제안) 리포트 컨텍스트
├── evolution/              # (제안) ace + deathmatch
├── bitget/                 # 별도 제품 (루트 셈 제거)
├── legacy_archive/
├── scripts/                # ops one-shot
└── tests/
```

---

## 부록: 검증 체크리스트 (정리 작업 시)

```bash
# 1) factory dry-run
./factory.sh --daily-kr --dry-run

# 2) import 회귀
python -m pytest tests/ -q --tb=no

# 3) orphan 재측정 (vulture 권장)
pip install vulture
vulture . --exclude venv,legacy_archive,bitget,tests

# 4) Bitget 분리 확인
grep -r "bitget_" --include="*.py" factory_pipelines.py system_auto_pilot.py
```

---

## 변경 이력

| 날짜 | 내용 |
|------|------|
| 2026-05-27 | 초판 — AST import 그래프 + factory.sh SSOT 기준 orphan 109건 분류 |

*본 문서는 분석 전용이며, 실제 삭제·이동은 별도 PR·백업·서버 검증 후 진행할 것.*
