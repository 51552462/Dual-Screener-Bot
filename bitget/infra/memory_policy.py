"""
Bitget 메모리·보관 정책 SSOT (Institutional Memory Tier Model).

3-Tier 모델 (24/7 코인 데몬 · 4GB RAM 서버 기준):
  Tier-1 HOT RAM   — WebSocket/live cache (ms~sec TTL, hard key/depth caps)
  Tier-2 WARM SQL  — append-only ops/shadow/queue (day/week retention + keep_last)
  Tier-3 COLD DISK — charts/logs (TTL days, disk_manager)

한국/미국 `HISTORY_CAP`·`keep_last`·`low_ram_sqlite_pragmas` 패턴의 코인 전용 집약.
모듈별 magic number 는 여기서만 정의하고 `memory_retention`·`stream_buffer` 등이 참조한다.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Tier-1: In-memory (hot)
# ---------------------------------------------------------------------------
STREAM_BUFFER_MAX_SYMBOLS: int = 2_000
STREAM_BUFFER_ORDERBOOK_DEPTH: int = 25  # top-of-book spread gate — full book 불필요
STREAM_BUFFER_STORE_RAW: bool = False  # WS payload 전체 저장 금지 (spread gate 만 필요)
PRIVATE_STREAM_MAX_EVENTS: int = 500
# OMS may trust private WS position index only when fresher than this (else REST)
PRIVATE_POS_INDEX_MAX_AGE_SEC: float = 20.0
# Public ticker last for order normalize — tighter than slippage spread gate (30s)
PUBLIC_REF_PRICE_MAX_AGE_SEC: float = 5.0
# OMS book source health — warn when REST dominates while private WS is enabled
OMS_REST_SHARE_WARN: float = 0.85
OMS_REST_SHARE_MIN_SAMPLES: int = 20
OMS_REST_SHARE_ALERT_MIN_INTERVAL_SEC: float = 3600.0
# Portfolio NAV risk gates (execution_safety — capital survival plane)
NAV_DD_REDUCE_PCT: float = 15.0   # stage: shrink new entry size
NAV_DD_BLOCK_PCT: float = 20.0    # stage: block new entries
NAV_DD_HALT_PCT: float = 30.0     # stage: block + throttled CRITICAL
NAV_DD_REDUCE_SIZE_MULT: float = 0.5
NAV_DD_ALERT_MIN_INTERVAL_SEC: float = 3600.0
DEFAULT_MAX_LEVERAGE: float = 5.0  # hard cap floor for resolve_leverage
# OMS orphan escalation (exchange-only positions — block new entries, never flatten)
OMS_ORPHAN_STREAK_PROPOSE_KILL: int = 2  # consecutive recon hits → propose KILL_SWITCH
OMS_ORPHAN_ALERT_MIN_INTERVAL_SEC: float = 3600.0
# Portfolio gross notional cap (open sim_kelly_invest sum / portfolio NAV)
# ≤0 disables the gate. Default 200% = 2× NAV before blocking new entries.
GROSS_NOTIONAL_MAX_PCT: float = 200.0
# BTC-proxy concentration (high corr same-side cluster / NAV) — ≤0 disables
CORR_BTC_MIN: float = 0.60          # pearson vs BTC → high-β cluster
CORR_CLUSTER_MAX_PCT: float = 100.0  # high-β same-side open sum / NAV
CORR_BTC_WINDOW: int = 60            # overlapping daily returns
CORR_BTC_MIN_OVERLAP: int = 20       # soft-pass if fewer bars
CORR_BENCH_SYMBOL: str = "BTC_USDT"
CORR_BENCH_TF: str = "1D"
# Doomsday DEFCON — block new LONG when level ≤ this (never flatten; SHORT may hedge)
DOOMSDAY_BLOCK_LEVEL: int = 2
# Tail-risk reserve (accrual → crisis 1:1 release; underfund size / empty+DD block)
TAIL_RISK_ACCRUAL_PCT: float = 1.5          # % of each treasury → fund target
TAIL_RISK_MIN_COVERAGE_PCT: float = 0.5     # fund/NAV %; below → size shrink (≤0 disables)
TAIL_RISK_UNDERFUND_SIZE_MULT: float = 0.5
TAIL_RISK_CRISIS_ATR_PCT: float = 6.0       # BEAR + BTC_ATR >= this → 1:1 release
TAIL_RISK_EMPTY_BLOCK: bool = True          # empty fund + MDD>=reduce → block new entries
# Bad-tick / flash-crash price sanity (≤0 on a threshold disables that sub-check)
BAD_TICK_LOOKBACK_BARS: int = 5
BAD_TICK_MAX_GAP_PCT: float = 15.0          # |px/prev_close−1|%
BAD_TICK_MAX_VS_MEDIAN_PCT: float = 20.0    # |px/median(N)−1|%
BAD_TICK_MAX_BAR_RANGE_PCT: float = 40.0    # (high−low)/close %
BAD_TICK_OHLCV_TF: str = "1H"               # live path default OHLCV TF
# Live WS/OMS smoke (read-only observational — never starts sockets / places orders)
WS_OMS_SMOKE_HEARTBEAT_MAX_AGE_SEC: float = 180.0  # auto_pilot HB is 60s
WS_OMS_SMOKE_HB_LOOKBACK_HOURS: float = 2.0
WS_OMS_SMOKE_BUF_STALE_SEC: float = 120.0  # buf_age soft warn while WS started

FUNDING_SNAP_CACHE_MAX_KEYS: int = 500
FUNDING_SNAP_TTL_SEC: float = 55.0

RATE_LIMIT_MAX_KEYS: int = 200
RATE_LIMIT_KEY_TTL_SEC: float = 3600.0

# Tier-1 network resilience (REST retry SSOT — bitget.infra.network_retry)
NETWORK_RETRY_MAX_ATTEMPTS: int = 3
NETWORK_BACKOFF_BASE_SEC: float = 2.0   # Connection/Timeout: 2 → 4 → 8
NETWORK_BACKOFF_CAP_SEC: float = 8.0
NETWORK_429_BACKOFF_BASE_SEC: float = 4.0  # Rate limit: longer floor
NETWORK_429_BACKOFF_CAP_SEC: float = 32.0
NETWORK_BACKOFF_JITTER_SEC: float = 0.15

# Tier-1 WebSocket resilience (bitget.infra.websocket_client — Bitget v2 public)
WS_PUBLIC_URL: str = "wss://ws.bitget.com/v2/ws/public"
WS_PRIVATE_URL: str = "wss://ws.bitget.com/v2/ws/private"
WS_PING_INTERVAL_SEC: float = 30.0       # Bitget: client ping every 30s
WS_PONG_TIMEOUT_SEC: float = 10.0        # no pong → reconnect
WS_STALE_RECV_SEC: float = 90.0          # no inbound frame → reconnect (server drops at ~2min idle)
WS_RECONNECT_BASE_SEC: float = 1.0
WS_RECONNECT_CAP_SEC: float = 60.0
WS_RECONNECT_JITTER_SEC: float = 0.25
WS_MAX_CHANNELS_PER_CONN: int = 50       # Bitget stability guidance
WS_MAX_OUTBOUND_PER_SEC: float = 10.0    # Bitget hard limit (incl. ping)
WS_SUBSCRIBE_BATCH_SIZE: int = 40        # leave headroom under channel cap
WS_LOGIN_TIMEOUT_SEC: float = 12.0       # private WS login ack wait
WS_UNIVERSE_REFRESH_SEC: float = 300.0   # re-resolve OPEN+benchmarks, then reconnect
WS_INCLUDE_BOOKS: bool = True            # ticker+books5 → ~25 symbols / connection
WS_OPEN_WATCH_SYMBOL_LIMIT: int = 40     # DISTINCT OPEN symbols SQL cap (before channel budget)
WS_BENCHMARK_SYMBOLS: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")

TELEGRAM_MEM_QUEUE_MAXSIZE: int = 4_000

# ---------------------------------------------------------------------------
# Tier-2: SQLite append / bounded reads
# ---------------------------------------------------------------------------
# shadow / friction (market_data.sqlite)
SHADOW_HISTORY_KEEP_DAYS: int = 90
SHADOW_BLOCKED_READ_LIMIT: int = 500  # shadow_performance_tracker blocked history cap
SCAN_FUNNEL_KEEP_DAYS: int = 60
REGIME_FRICTION_KEEP_DAYS: int = 90
SCAN_FUNNEL_KEEP_LAST: int = 50_000

# ops_events (bitget_ops_events.sqlite)
OPS_EVENTS_KEEP_DAYS: int = 60

# task_queue (bitget_task_queue.sqlite) — DONE/FAILED 누적 방지
TASK_QUEUE_DONE_KEEP_DAYS: int = 14
TASK_QUEUE_FAILED_KEEP_DAYS: int = 30
TASK_QUEUE_DONE_KEEP_LAST: int = 5_000
TASK_QUEUE_FAILED_KEEP_LAST: int = 1_000
# RUNNING stuck recovery: finished_at NULL + picked_at > N hours → FAILED (별도 heal)

# forward_trades bounded reads
FORWARD_CLOSED_TRADES_LIMIT: int = 500
FORWARD_REPORT_CLOSED_LIMIT: int = 1_200
GRAND_REPORT_CLOSED_LIMIT: int = 8_000  # 주/월 결산 — 기간 필터 + safety cap
GRAND_REPORT_GENESIS_LIMIT: int = 500   # champion_precursor_genesis period window
GRAND_REPORT_DEATHMATCH_CHAMPION_LIMIT: int = 64  # deathmatch_champion snapshot
GRAND_REPORT_ELIMINATION_EVENT_LIMIT: int = 200  # deathmatch_elimination_event period
GRAND_REPORT_STRATEGY_REGISTRY_LIMIT: int = 1_000  # strategy_registry lifecycle snapshot
GRAND_REPORT_REGISTRY_WINDOW_DETAIL_LIMIT: int = 24  # promoted/demoted detail rows
FORWARD_OPEN_MAX_SAFETY: int = 1_000  # 초과 시 ops 경고 (OOM 전조)
OPEN_SAFETY_ALERT_MIN_INTERVAL_SEC: float = 3600.0  # OPEN cap 초과 CRITICAL 텔레그램 throttle
ELASTIC_VOL_OPEN_LIMIT: int = 500       # elastic vol proxy — OPEN MFE sample cap
ELASTIC_VOL_CLOSED_LIMIT: int = 2_000   # elastic vol proxy — rolling CLOSED σ cap
EXIT_RATCHET_RUNNER_LIMIT: int = 500    # weekly κ RL — free_runner CLOSED window cap
FORWARD_INTEGRITY_CLOSED_WINDOW_DAYS: int = 7  # open book integrity — rolling CLOSED count
FORWARD_HEATMAP_OPEN_LIMIT: int = 500
FORWARD_PIL_CLOSED_LIMIT: int = 2_500  # PIL — OPEN 전건 + CLOSED recent N
FORWARD_DASHBOARD_CLOSED_LIMIT: int = 8_000
FORWARD_IDENTITY_CLOSED_LIMIT: int = 8_000  # identity diagnose — rolling-window CLOSED cap
FORWARD_BRAIN_TUNE_CLOSED_LIMIT: int = 120
FORWARD_PRI_WEEK_CLOSED_LIMIT: int = 5_000
PRI_FUNNEL_WEEK_LIMIT: int = 400          # scan_funnel_snapshot — weekly PRI window cap
PRI_FRICTION_WEEK_LIMIT: int = 200        # regime_friction_event — weekly PRI window cap
GENESIS_CLOSED_TRADES_LIMIT: int = 4_000  # champion_genesis — recent CLOSED per market
GENESIS_ARM_SNAPSHOT_LIMIT: int = 500     # deathmatch_arm_snapshot series cap
GENESIS_PENDING_BACKFILL_LIMIT: int = 200  # pending champion causal resolve batch
GENESIS_UNRESOLVED_PREDICTION_LIMIT: int = 200  # unresolved prediction backfill batch
EXPLORATION_BUDGET_ROLLING_CLOSED_LIMIT: int = 5_000  # MAB 7d bucket — exit_date window cap
UNDERDOG_MINER_CLOSED_LIMIT: int = 2_000  # GMM cluster — high-ret low-score CLOSED cap
BLACKHOLE_HUNTER_CLOSED_LIMIT: int = 3_000  # 14d toxic scan CLOSED cap
TOXIC_GRAVEYARD_CLOSED_LIMIT: int = 5_000  # graveyard ML — recent CLOSED anti-pattern cap
FORWARD_WEEKLY_TF_ROTATION_LIMIT: int = 2_000  # auto_pilot 주간 TF 궤적  # weekly PRI per-market safety cap

# bitget_real_execution — leaderboard / practitioner samples
REAL_EXECUTION_READ_LIMIT: int = 5_000
REAL_EXECUTION_PRACTITIONER_SAMPLE_LIMIT: int = 200
REAL_EXECUTION_KEEP_DAYS: int = 180
REAL_EXECUTION_KEEP_LAST: int = 20_000

# ai_overseer daily audit (date-filtered, column-projected)
OVERSEER_DAILY_CLOSED_LIMIT: int = 2_000
OVERSEER_CSV_STATUS_ROW_CAP: int = 2_000  # CSV status line-count cap (no full DataFrame load)

# gap_healer — REST backfill flood 방지
GAP_HEAL_MIN_INTERVAL_SEC: float = 300.0
GAP_HEAL_MAX_AGE_SEC: float = 120.0
GAP_HEAL_MAX_SYMBOLS_SCAN: int = 50

# OHLCV bar limits (use-case별)
OHLCV_SIGNAL_BAR_LIMIT: int = 300      # scanner / supernova / data_miner
OHLCV_REGIME_BAR_LIMIT: int = 220       # EMA200 · regime (auto_pilot bench)
GATES_BREADTH_BENCH_BAR_LIMIT: int = 80   # gates.py BTC/ETH breadth benchmark tail
OHLCV_FORENSICS_BAR_LIMIT: int = 400     # pump/forensics 1D scan

# alt_data.sqlite macro_daily lookback (hydrate fallback)
MACRO_DAILY_LOOKBACK_MAX_ROWS: int = 365
OHLCV_ENTRY_LOOKBACK_DAYS: int = 120     # data_miner per-trade window
DATA_MINER_MFE_WINNERS_LIMIT: int = 3_000  # GMM DNA mining — per-TF CLOSED MFE cap
DATA_MINER_MFE_TRAINING_LIMIT: int = 500  # AST alpha evolution — 30d MFE sample cap

# build_supernova_csv / KMeans cluster mining (4GB peak guard)
SUPERNOVA_CLUSTER_MAX_TABLES: int = 400       # OHLCV tables scanned per run
SUPERNOVA_CLUSTER_MIN_BARS: int = 240         # DNA extract minimum bar count
SUPERNOVA_CLUSTER_OUT_MAX_ROWS: int = 800     # CSV output safety cap
SUPERNOVA_CLUSTER_FORWARD_SYMBOL_LIMIT: int = 500  # forward_trades symbol resolution
SUPERNOVA_CLUSTER_SYMBOL_LOOKBACK_DAYS: int = 90     # recent symbol window (UTC)
SUPERNOVA_CLUSTER_GC_EVERY_N: int = 25        # batch gc interval during table scan
SUPERNOVA_SCAN_MAX_WORKERS: int = 4           # live scan ThreadPool — 4GB RAM conn peak guard

# forward zombie reporter cleanup (batch UPDATE, OPEN book safety)
FORWARD_ZOMBIE_CLEANUP_BATCH_LIMIT: int = 500
# blank-symbol synthetic label repair (identity) — batch UPDATE
FORWARD_IDENTITY_BLANK_REPAIR_BATCH_LIMIT: int = 500

# time_machine_backtester — crash-period OHLCV scan cap
TIME_MACHINE_MAX_TABLES: int = 300
TIME_MACHINE_MAX_BARS_PER_TABLE: int = 5_000  # per-table crash-window bar ceiling

# retention sweep throttle (INSERT hook — 매 write 마다 purge 금지)
RETENTION_SWEEP_MIN_INTERVAL_SEC: float = 3600.0

# heavy-cycle gc — bitget.infra.gc_cycle.flush_gc() 호출 지점 참고용 라벨
GC_AFTER_OHLCV_BATCH: str = "ohlcv_batch"
GC_AFTER_BACKTEST_TABLE: str = "backtest_table"
GC_AFTER_GMM_FIT: str = "gmm_fit"
GC_AFTER_SCAN_TABLE: str = "scan_table"
GC_AFTER_AST_EVOLUTION: str = "ast_evolution"
GC_AFTER_CLUSTER_MINING: str = "cluster_mining"

# ---------------------------------------------------------------------------
# Tier-3: Filesystem
# ---------------------------------------------------------------------------
CHART_IMAGE_RETENTION_DAYS: int = 3
# bitget.sh stamped cron/daemon logs: bitget_<mode>_YYYYMMDD_HHMMSS.log
# Never touch RotatingFileHandler set (bitget.log / bitget.log.N)
STAMPED_LOG_RETENTION_DAYS: int = 5
# Integrity-verified tar.gz archives (institutional_db_backup) — keep newest N
DB_BACKUP_KEEP_ARCHIVES: int = 7

# production logging (RotatingFileHandler SSOT)
LOG_ROTATE_MAX_BYTES: int = 50 * 1024 * 1024  # 50MB per file
LOG_ROTATE_BACKUP_COUNT: int = 5
LOG_FILE_NAME: str = "bitget.log"
LOG_FORMAT: str = "[%(asctime)s] [%(levelname)s] %(message)s"
LOG_DATEFMT: str = "%Y-%m-%d %H:%M:%S"
