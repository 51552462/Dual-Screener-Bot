# SYSTEM TOTAL AUDIT — Dual-Screener-Bot

**감사 일자:** 2026-05-26 (KST)  
**감사 범위:** 저장소 루트 `Dual-Screener-Bot` — Python 약 **188**개( `venv` / `deploy/gen_*` 제외), systemd·cron 진입점 포함  
**방법:** 진입점 역추적 + `ast` 정적 import 그래프 + `factory_pipelines` StepSpec 수동 대조 + ReportTimekeeper 코드 리뷰  
**주의:** 동적 import(`importlib`, `_spawn_py_script`, cron 문자열)는 그래프에 **엣지가 없음** → “데드”로 분류되어도 **위성(satellite) 실행**일 수 있음.

---

# 챕터 1. 실시간 가동 의존성 맵 (True Dependency Map)

## 1.1 진입점(Entry Points) 총정리

| 진입 | 실제 명령 | 역할 |
|------|-----------|------|
| **cron / 수동** | `./factory.sh --scan-kr\|scan-us\|daily-kr\|daily-us\|daily\|weekly` | 팩토리 원샷 파이프라인 |
| **factory 내부** | `python system_auto_pilot.py --mode <MODE>` | `factory.sh`가 호출 |
| **dante-main** | `venv/bin/python -u main.py` | 24h 스캐너·autopilot·forward 스케줄러 |
| **dante-factory** (`.in` 템플릿) | `python factory_launcher.py` → `runpy`로 `main.py` | main과 동일 트리 + 텔레그램 분리 플래그 |
| **dante-streamlit / dante-dashboard** | `streamlit run dashboard.py` | 관제 UI (팩토리 미경유) |
| **dante-async** | `python async_telegram_daemon.py` | 텔레그램 큐 소비 |
| **dante-watchdog** | `python watchdog.py` | oneshot 헬스 |
| **dante-snapshot** | `python dante_snapshot_runner.py` | CQRS `market_data_snapshot.sqlite` |
| **bitget (별도 제품군)** | `bitget_main.py` | 암호화폐 스택 — equity cron과 **분리 섬** |

---

## 1.2 `factory.sh` → 메모리 상주 모듈 (공통 부트스트랩)

```
factory.sh
└─ cd ROOT, source .env, activate venv
└─ export TZ=Asia/Seoul, PYTHONPATH=ROOT
└─ exec python system_auto_pilot.py --mode <MODE>  >> logs/factory_<MODE>_*.log
```

**`system_auto_pilot.py` import 시 항상 로드되는 무거운 층 (모든 factory mode 공통):**

```
system_auto_pilot.py
├─ telegram_env
├─ yf_download_flatten
├─ config_manager (load_system_config)
├─ inverse_etf_sniper
├─ shadow_tracking
├─ ops_logger
├─ system_config_atomic
├─ pandas / numpy / yfinance / FinanceDataReader / requests / pytz
└─ run_factory_cli() 시 추가:
    ├─ factory_pipelines
    └─ factory_runtime
```

**중요:** `--mode daily-kr` 한 번 실행해도 **전체 `system_auto_pilot` 모듈**이 먼저 로드됨 → 메모리·기동 비용이 mode 대비 과대.

---

## 1.3 `factory_runtime` 실행 골격

```
run_factory_cli()
└─ factory_pipelines.get_pipeline(mode)
└─ factory_runtime.dispatch_factory_mode(mode, pipeline, send_fn=send_telegram_report)
    ├─ factory_job_lock(mode)          # Linux fcntl
    └─ for StepSpec in pipeline:
           run_step(spec) → spec.fn()  # 동기, lazy import
    └─ notify_factory_run (텔레그램, 실패 시)
    └─ ops_logger.record_heartbeat
```

---

## 1.4 Mode별 **진짜** Step 체인 (A → B → C)

### `scan_kr` (`./factory.sh --scan-kr`)

```
factory_artifact_guard          → factory_artifact_guard.ensure_factory_artifacts
kr_cross_market_hydrate         → cross_market_ssot.hydrate_kr_runtime_from_ssot
supernova_scan_kr (critical)    → supernova_hunter.execute_supernova_live_scan("KR")
kr_bowl_scan                    → kr.scan_market_1d
```

### `scan_us` (`./factory.sh --scan-us`)

```
factory_artifact_guard
us_health_gate                  → factory_us_health.assess_us_pipeline_health
us_health_repair                → factory_us_health.ensure_us_pipeline_ready_for_scan
us_data_incremental             → data_updater.run_us_incremental_db_update
supernova_scan_us (critical)    → supernova_hunter.execute_supernova_live_scan("US")
us_bowl_scan                    → usa.scan_market_1d
us_cross_market_publish         → cross_market_ssot.publish_us_snapshot_after_pipeline
```

### `daily_audit_kr` (`./factory.sh --daily-kr`, cron 16:35 KST)

```
meta_governor_sync              → meta_state_store.rebuild_meta_state
factory_artifact_guard
sentiment_mining                → sentiment_miner.run_sentiment_mining
sector_spillover_refresh        → sector_spillover_refresh.refresh_sector_spillover_state
kr_cross_market_hydrate
track_daily_positions_kr        → auto_forward_tester.track_daily_positions("KR")
deep_dive_kr (critical)         → auto_forward_tester.run_deep_dive_analysis("KR")
doomsday_bridge_sync            → doomsday_bridge.sync_doomsday_to_system_config
pil_practitioner_reports        → auto_forward_tester.send_group_practitioner_reports
comprehensive_daily_report      → auto_forward_tester.send_comprehensive_daily_report
ai_overseer                     → ai_overseer.run_ai_auditor
```

### `daily_audit_us` (`./factory.sh --daily-us`, cron 06:45 KST)

```
(위 KR prelude 1~5 동일)
us_health_gate_daily / us_health_repair_daily
us_data_incremental
track_daily_positions_us
deep_dive_us                    → run_deep_dive_analysis("US")
doomsday_bridge_sync
pil_practitioner_reports
comprehensive_daily_report
ai_overseer
us_cross_market_publish
```

### `daily_audit` (`./factory.sh --daily`)

```
KR prelude → track_kr → deep_dive_kr
→ us_data_incremental → track_us → deep_dive_us
→ doomsday → PIL → comprehensive → overseer (1회)
(US health gate/repair·us_cross_market_publish 없음)
```

### `weekly_master` (`./factory.sh --weekly`)

```
factory_artifact_guard
weekly_flow_master              → weekly_flow_report.send_weekly_flow_master_report
```

---

## 1.5 `deep_dive_kr` / `deep_dive_us` 내부 리포트 스택 (Timekeeper 적용 구간)

```
auto_forward_tester.run_deep_dive_analysis(market)
├─ report_db_read_path()           → market_data.sqlite MAIN 강제 (기본)
├─ ReportTimekeeper.for_market()   → KR: KST 영업일 / US: US Last Trading Day (ET)
├─ forward_dual_track_queries
│   ├─ fetch_live_today_closed      (exit_date = session_anchor)
│   ├─ fetch_hist_baseline_closed   (exit_date < session_anchor, rolling)
│   └─ fetch_champion_rolling_closed (cutoff ≤ exit ≤ anchor)
├─ report_staleness_gate.evaluate_staleness → GREEN/YELLOW/RED
├─ forward_score_bucket_deep_dive  (Micro-DNA · Dual-Track · 최우수 성적표 Dual-Track)
├─ report_feature_analyzer
├─ forward_flow_tag_deep_dive
└─ send_telegram_msg
```

---

## 1.6 `main.py` / `dante-main` (팩토리와 **별도** 장기 프로세스)

```
main.py
├─ ops_logger (전역)
├─ telegram_env
├─ 선택적 스캐너 import (us_master, kr, ema5, nulrim, …) — 실패 시 해당 봇만 비활성
├─ data_updater, auto_forward_tester, system_auto_pilot, supernova_hunter
├─ ai_overseer, ai_secretary, factory_artifact_guard
└─ __main__ 스레드:
    ├─ 각 스캐너 run_scheduler (최대 10)
    ├─ data_updater KR 07:00
    ├─ factory_us_health heartbeat 6h
    ├─ auto_forward_tester.run_daily_scheduler  (16:30/17:00/06:30 KST)
    ├─ system_auto_pilot.system_main_loop     (위성 subprocess spawn 포함)
    ├─ supernova_hunter.run_scheduler
    ├─ ai_overseer.overseer_loop
    ├─ ai_secretary.run_secretary
    └─ async_telegram_daemon (inline 또는 별도 서비스)
```

**`system_auto_pilot.system_main_loop` 위성 spawn (정적 그래프 밖):**

- `us_toxic_graveyard_analyzer.py`
- `toxic_graveyard_analyzer.py`
- `macro_doomsday_bot.py`
- `time_machine_backtester` (exec 문자열)
- `synthetic_data_generator` / `alt_data_miner` 등 (플래그·요일 조건)

---

## 1.7 `dashboard.py` / `dante-streamlit` (관제만)

```
streamlit run dashboard.py
├─ market_db_paths (market_db_read_path — 스냅샷 가능)
├─ ops_logger → ops_events.sqlite
└─ forward_trades 집계 UI
   ✗ factory_pipelines / system_auto_pilot / scanners 미로드
```

---

## 1.8 정적 import 그래프로 **도달 가능(Reachable)** — equity+core 약 **117** 모듈

`main.py` + `system_auto_pilot.py` + `factory_launcher` + `dashboard` + `async_telegram` + `watchdog` + `dante_snapshot_runner` + `bitget_main` 에서 BFS.

핵심 필수 파일(요약):

- **오케스트레이션:** `factory_pipelines.py`, `factory_runtime.py`, `system_auto_pilot.py`, `factory_artifact_guard.py`, `factory_us_health.py`
- **포워드·리포트:** `auto_forward_tester.py`, `forward_*`, `report_*`, `report_timekeeper.py`, `report_staleness_gate.py`, `market_db_paths.py`
- **스캔:** `supernova_hunter.py`, `scanner_*`, `kr.py` / `usa.py` / `us_master.py` / `master.py` …
- **메타·레짐:** `meta_state_store.py`, `meta_governor*.py`, `regime_*`, `doomsday_bridge.py`
- **인프라:** `telegram_*`, `ops_logger.py`, `config_manager.py`, `sqlite_schema_guard.py`

전체 목록은 감사 시 생성한 `_reachable_list.txt` (117행) 참고.

---

# 챕터 2. 좀비 및 데드코드 파일 목록 (Dead Code / Orphan Files)

## 2.1 분류 기준

| 등급 | 의미 | 조치 |
|------|------|------|
| **A — 삭제 후보** | 정적·동적 진입 모두 없음, 아카이브 명시 | 삭제 또는 `archive/` 이동 |
| **B — 위성(Satellite)** | `system_auto_pilot._spawn_py_script` / 주말 cron만 | `satellites/`로 격리·문서화 |
| **C — Bitget 섬** | `bitget_main` 없이는 미도달 | 별도 repo 또는 `bitget/` 패키지 |
| **D — 테스트** | `tests/*` | 유지, CI 전용 |
| **E — 오탐(실제 live)** | 동적 spawn·문자열 exec | 유지, 그래프에 주석 엣지 추가 권장 |

---

## 2.2 A등급 — 즉시 정리 후보 (정적 미연결 + 위성 아님)

| 파일 | 사유 |
|------|------|
| `_ARCHIVED_smart_money_kalman.py` | 파일명에 ARCHIVED, 무참조 |
| `config_manager_from_git.py` | 운영 경로 미사용 (일회성 마이그레이션 추정) |
| `ace_evolution_consumer.py` | ace_evolution_refresh 등으로 대체된 소비자 잔재 |
| `dna_schema_constants.py` | 정적 import 0 (스키마 상수 미배선) |
| `incubator_engine.py` | 정적 미연결 (`auto_forward_tester` 내 인큐베이터 로직 인라인) |
| `forensics_pioneer.py` | 정적 미연결 |
| `limit_up_forensics.py` | 정적 미연결 |
| `mutant_oos_validator.py` | 정적 미연결 |
| `smart_money_tracker.py` | `satellite_intel_brief`·SSOT 주석상 실험 트랙 미사용 (radar는 meta 경유) |

---

## 2.3 B등급 — 위성·스케줄 spawn (데드 아님, 구조적 좀비)

| 파일 | 호출 주체 |
|------|-----------|
| `toxic_graveyard_analyzer.py` | `system_auto_pilot` spawn (KR 일/주) |
| `us_toxic_graveyard_analyzer.py` | spawn 07:00 KST |
| `macro_doomsday_bot.py` | spawn 08:00 / 17:00 |
| `time_machine_backtester.py` | spawn 주말·문자열 exec |
| `synthetic_data_generator.py` | bitget_autopilot·SAP 위성 플래그 |
| `alt_data_miner.py` | 위성 플래그 (equity) |

**문제:** IDE·정적 분석기에는 “죽은 파일”로 보이나 **야간에 살아남** → 유지보수 혼란의 주범.

---

## 2.4 C등급 — Bitget 평행 우주 (~35 모듈, `bitget_main` 전용)

`bitget_main.py`·`bitget_factory_launcher`·`bitget_dashboard` 진입 시에만 도달.  
equity `factory.sh` / `dante-main` 과 **DB·텔레그램·리포트 SSOT 공유 없음**.

**삭제 대상이 아니라 “제품 분리” 대상:**

- `bitget_forward_tester.py`, `bitget_supernova_hunter.py`, `bitget_auto_pilot.py`, `bitget_system_auto_pilot.py`, … (전체 `bitget_*`)

**권장:** `bitget/` 서브패키지 또는 별도 git submodule — equity 감사 범위에서 제외 명시.

---

## 2.5 D등급 — 테스트 (유지)

`tests/test_*.py` (14파일) — pytest 미설치 환경에서도 수동 실행 가능.

---

## 2.6 E등급 — 오탐 주의 (reachable 목록에 포함된 핵심)

다음은 “죽지 않았음”이 확인된 핵심 (챕터 2 후보에서 **제외**):

`auto_forward_tester.py`, `report_timekeeper.py`, `forward_dual_track_queries.py`, `supernova_hunter.py`, `capital_deathmatch.py`, `weekly_flow_*.py`, …

---

## 2.7 파편화 지표 (정량)

| 항목 | 수치 |
|------|------|
| Python 모듈( tests 포함) | ~188 |
| 정적 reachable (core entry) | ~117 |
| Bitget 섬 | ~35 |
| 위성·아카이브·미배선 | ~20 |
| `auto_forward_tester.py` 단일 파일 라인 | 3,500+ (God module) |
| `system_auto_pilot.py` | 2,000+ |

---

# 챕터 3. [Cursor AI 구조적 역제안] 파일 슬림화 플랜

## 3.1 설계자 관점의 한계(부끄러운 점)

1. **이중 런타임:** `factory.sh`(원샷) vs `main.py`(상주) vs `auto_forward_tester.run_daily_scheduler` — 동일 `run_deep_dive`가 **서로 다른 시계**로 호출될 수 있었음 → Timekeeper로 1차 통합했으나 **진입점은 여전히 3개**.
2. **God modules:** `auto_forward_tester.py` = 장부 + 청산 + 리포트 + 텔레그램 + 인큐베이터 + 스케줄러.
3. **이름 중복:** `forward_*` / `report_*` / `deathmatch_*` / `ace_evolution_*` — 역할 경계가 파일명에 안 드러남.
4. **CQRS 반쪽:** 쓰기는 `market_data.sqlite`, 읽기는 `market_db_read_path()` vs `report_db_read_path()` — 정책이 파일마다 달랐음 (지금은 report 측 MAIN 강제).
5. **Bitget 미분리:** equity 117 + bitget 35 = 검색·리팩터 비용 2배.

---

## 3.2 목표 아키텍처 — **25개 내외**顶-level 패키지

```
dual_screener/
├── __init__.py
├── paths.py                 # market_db_paths + factory_data_paths 통합
├── config/
│   ├── ssot.py              # system_config_atomic + config_manager
│   └── regime.py            # meta_governor + meta_state_store + regime_*
├── factory/
│   ├── runtime.py           # factory_runtime + lock + notify
│   ├── pipelines.py         # StepSpec SSOT
│   └── cli.py               # run_factory_cli (system_auto_pilot factory 부분만)
├── forward/
│   ├── ledger.py            # init DB, track, close, insert
│   ├── deep_dive.py         # run_deep_dive_analysis
│   └── daily_report.py      # send_comprehensive_daily_report
├── report/
│   ├── timekeeper.py        # ReportTimekeeper
│   ├── staleness.py         # 3단계 Gate
│   ├── dual_track.py        # queries + bucket + format HTML
│   ├── features.py          # report_feature_analyzer
│   └── collectors.py        # report_collectors, satellite, weekly
├── scan/
│   ├── supernova.py         # supernova_hunter 핵심
│   ├── funnel.py            # scanner_funnel
│   └── markets/             # kr.py, usa.py, us_master.py → thin adapters
├── risk/
│   ├── deathmatch.py        # capital_* + deathmatch_* 통합
│   ├── doomsday.py          # doomsday_bridge + inverse
│   └── toxic.py             # toxic_* + blackhole (KR/US)
├── data/
│   ├── updater.py           # data_updater
│   └── sentiment.py         # sentiment_miner + news_data_paths
├── io/
│   ├── telegram.py          # telegram_env + queue + async daemon hook
│   └── ops.py               # ops_logger + shadow ops_snapshot
├── overseer/
│   └── auditor.py           # ai_overseer + overseer_audit_binder
├── satellites/              # spawn 전용 (명시적)
│   ├── time_machine.py
│   ├── toxic_graveyard_kr.py
│   └── ...
├── apps/
│   ├── main_daemon.py       # 현 main.py 스레드 기동만
│   └── dashboard.py         # streamlit (thin)
└── bitget/                  # 전면 이동 또는 submodule
    └── ...
```

**顶-level 파일 수:** 약 **22~28** (markets 하위 제외).

---

## 3.3 통합(Consolidation) 매핑 — “무엇을 합치고 버릴지”

| 현재 군 | 통합 대상 | 버릴 것 |
|---------|-----------|---------|
| `forward_score_bucket_deep_dive.py` + `forward_dual_track_queries.py` + `forward_flow_tag_deep_dive.py` + `forward_report_scalar.py` | `report/dual_track.py` | 중복 formatter |
| `report_timekeeper.py` + `report_staleness_gate.py` | `report/timekeeper.py` (Gate 내장) | `assess_live_staleness` 레거시 |
| `report_feature_analyzer.py` + `report_collectors.py` + `report_formatter.py` | `report/features.py` | |
| `factory_pipelines.py` + `factory_runtime.py` | `factory/` | |
| `meta_governor.py` + `meta_state_store.py` + `regime_meta_analyzer.py` + `meta_governor_consumer.py` | `config/regime.py` | consumer는 facade |
| `deathmatch_*` (6파일) + `capital_deathmatch.py` | `risk/deathmatch.py` | |
| `ace_evolution_*` (8파일) | `report/ace_evolution.py` 또는 `config/ace.py` | `ace_evolution_consumer.py` |
| `practitioner_*` (5파일) | `report/practitioner.py` | |
| `weekly_flow_*` (3) + `weekly_action_plan.py` | `report/weekly.py` | |
| `kr.py` / `master.py` / `ema5.py` / … | `scan/markets/kr_*.py` — **공통 scan_core** 추출 | 복붙 scoring |
| `bitget_*.py` (35) | `bitget/` 패키지 | equity에서 import 금지 |
| `_ARCHIVED_*`, `config_manager_from_git.py` | 삭제 | |
| `smart_money_tracker.py` | `satellites/` 또는 SSOT-only | standalone 실험 |

---

## 3.4 단계적 실행 로드맵 (리스크 순)

| Phase | 작업 | 기대 효과 |
|-------|------|-----------|
| **0 (완료)** | ReportTimekeeper + Staleness + report MAIN DB | Time-Freeze 1차 |
| **1** | `auto_forward_tester` → `forward/ledger` + `forward/deep_dive` + `forward/daily_report` 물리 분할 | God module 해체 |
| **2** | `report/*` 통합, HTML formatter 단일화 | 텔레그램 포맷 SSOT |
| **3** | `satellites/` 이동 + spawn registry 테이블화 | “좀비” 가시화 |
| **4** | Bitget subtree 분리 | equity 감사 단순화 |
| **5** | `system_auto_pilot` → `factory/cli` + `apps/main_daemon` | factory 기동 경량화 |

---

# 챕터 4. KR/US 데이터 동기화 및 타임프리즈 무결성 체크

## 4.1 ReportTimekeeper SSOT — 구현 상태 (커밋 `666039c` 기준)

| 항목 | KR | US |
|------|----|----|
| `session_anchor` | `kr_session_anchor_date` — KST 토·일 → 금요일 | `us_last_trading_session_date` — **ET 16:00** 근사, KST 달력 아님 |
| 예시 | 금요일 장 마감 후 daily-kr → anchor=당일 KST 영업일 | KST **화 06:45** daily-us → anchor=**US 월요일** |
| 롤링 컷오프 | `anchor - 90d` (config 90/180) | 동일 |
| DB 읽기 | `report_db_read_path()` → **MAIN** | 동일 |

**검증 테스트:** `tests/test_report_timekeeper.py` (`test_us_anchor_kst_tuesday_morning_is_us_monday`).

---

## 4.2 쿼리 3분할 (Ghost Data 방지)

| 쿼리 | SQL 의미 | 용도 |
|------|----------|------|
| **LIVE_TODAY** | `exit_date = session_anchor` (+ trade_date 컬럼 시 OR) | 당일 실전 |
| **HIST_BASELINE** | `cutoff ≤ exit_date < anchor` | 과거 기준(Sim) — anchor 당일 **제외** |
| **CHAMPION_ROLLING** | `cutoff ≤ exit_date ≤ anchor` | 최우수 성적표 — anchor 당일 **포함** |

**Ghost Data 메커니즘:** 과거 +10% 고정 청산이 HIST에 남고, LIVE가 비어 있으면 **예전에는 롤링 단일 쿼리가 HIST를 “오늘”처럼 보여줌** → Dual-Track + Staleness로 분리·경고.

---

## 4.3 3단계 Staleness Gate

| 등급 | 조건 | 리포트 동작 |
|------|------|-------------|
| **GREEN** | `db_watermark ≥ session_anchor` | 전체 Micro-DNA + Dual-Track 최우수 |
| **YELLOW** | lag 1영업일 또는 LIVE 0건 | 경고 배너, 본문 유지 |
| **RED** | lag ≥ 2영업일 | Fail-safe 카드, **최우수 성적표 생략** |

**저장:** `system_config.LAST_REPORT_STALENESS_{KR|US}`, `ops_events.report.staleness`, `ops_snapshot.kr_exit_watermark` 등.

---

## 4.4 KR/US 파이프라인 대칭성 감사

| 체크포인트 | KR (`daily_audit_kr`) | US (`daily_audit_us`) | 판정 |
|------------|----------------------|------------------------|------|
| Timekeeper market 인자 | `"KR"` | `"US"` | ✅ |
| track → deep_dive 순서 | ✅ | ✅ | ✅ |
| US OHLCV 증분 | N/A (KR bulk 07:00) | `us_data_incremental` | ✅ (비대칭이나 의도됨) |
| session_anchor 규칙 | KST 영업일 | US Last Trading Day | ✅ (하달 반영) |
| `report_db_read_path` | MAIN | MAIN | ✅ |
| comprehensive report | 1회 (시장별 pipeline은 시장별 deep_dive 후) | 동일 | ⚠️ combined `daily`는 KR·US deep_dive 후 **리포트 1회** — 시장 혼합 주의 |
| `run_daily_scheduler` (main) | 17:00 KR+US deep_dive 연속 | 동일 | ⚠️ **factory cron과 중복 실행** 가능 |

---

## 4.5 잔존 리스크 및 개선 역제안

### R1 — 이중 스케줄 (중요)

- **cron:** `factory.sh --daily-kr` / `--daily-us`
- **main:** `auto_forward_tester.run_daily_scheduler` (17:00 KR+US)

→ 동일 날 **딥다이브 2회** 가능.  
**역제안:** `run_daily_scheduler`에서 deep_dive 제거하고 factory만 SSOT, 또는 factory 비활성 시에만 scheduler fallback.

### R2 — `send_comprehensive_daily_report` vs deep_dive Timekeeper

- comprehensive는 아직 **구형 `market_db_read_path()`** 경로 잔존 가능 (별도 감사 필요).
- **역제안:** comprehensive·colosseum·PIL 전부 `ReportTimekeeper` + `report_db_read_path` 주입.

### R3 — dashboard 스냅샷 착시

- Streamlit은 `market_db_read_path()` — mtime 신선·데이터 워터마크 불일치 가능.
- **역제안:** dashboard 헤더에 `MAX(exit_date)` by market 표시.

### R4 — US health vs combined daily

- `daily_audit` (combined)는 US health gate 생략.
- **역제안:** combined에도 `us_health_gate` 최소 1회 포함.

### R5 — Ghost Data in HIST (의도적)

- HIST는 과거 Sim **포함이 정상**. “Ghost”는 LIVE에 섞일 때만 버그.
- **역제안:** config `HIST_EXCLUDE_SIG_TYPES` (R&D, INCUBATOR 이미 제외) 확장.

### R6 — 위성 spawn과 DB 워터마크

- `toxic_graveyard` 등은 장부와 무관 — 워터마크 lag RED는 **track/청산 실패** 진단용.
- **역제안:** RED 시 텔레그램 + `factory_meta_alerts` 이중 알림.

---

## 4.6 무결성 체크리스트 (운영자용)

배포 후 매 거래일:

1. `./factory.sh --daily-kr` 로그 → `세션앵커` = KST 영업일, `DB청산워터마크` ≥ anchor, `Staleness GREEN`
2. `./factory.sh --daily-us` (06:45 이후) → `세션앵커(US Last Trading Day)` = **직전 US 거래일**
3. `system_config` → `LAST_REPORT_STALENESS_KR/US`
4. SQL: `SELECT market, MAX(exit_date) FROM forward_trades WHERE status LIKE 'CLOSED%' GROUP BY market`

---

# 부록 A. systemd / cron Quick Reference

```
# cron (deploy/factory.crontab.example)
16:35 Mon-Fri  ./factory.sh --daily-kr
06:45 Tue-Sat  ./factory.sh --daily-us

# systemd
dante-main.service      → python main.py
dante-streamlit.service → streamlit run dashboard.py
dante-async.service     → async_telegram_daemon.py
dante-snapshot.timer    → dante_snapshot_runner.py
```

---

# 부록 B. 즉시 실행 가능한 정리 액션 (우선순위)

1. **삭제/이동:** `_ARCHIVED_smart_money_kalman.py`, `config_manager_from_git.py`, `ace_evolution_consumer.py`
2. **문서화:** `satellites/README.md` — spawn 대상 6~8 스크립트 목록
3. **코드:** `run_daily_scheduler` vs `factory.sh` deep_dive 중복 제거 (챕터 4.5 R1)
4. **코드:** `send_comprehensive_daily_report`에 ReportTimekeeper 주입 (R2)
5. **장기:** 챕터 3.2 패키지 구조 마이그레이션 (Phase 1~2)

---

*본 문서는 저장소 정적 분석 결과이며, 런타임 환경(.env, DB 실데이터, cron 실제 등록)은 서버에서 별도 검증이 필요합니다.*
