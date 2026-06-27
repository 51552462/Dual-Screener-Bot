# Bitget Phase 4 & 4b — WebSocket · Gap Heal · CQRS · Slippage Guard

> 작성일: 2026-06-07  
> **주식 루트 미수정** — 모든 변경 `bitget/` 내부.

---

## 1. 인메모리 스트림 버퍼

**파일:** `bitget/data/stream_buffer.py`

| 캐시 | 클래스 | 내용 |
|------|--------|------|
| Ticker | `StreamBuffer` | last / bid / ask / 24h volume |
| Orderbook | `StreamBuffer` | books1 top-of-book (best bid/ask) |
| Position/Order | `PrivateStreamBuffer` | private WS orders / positions / account |

- `threading.RLock` 기반 thread-safe
- `spread_bps()` — **orderbook 우선**, ticker fallback
- `get_stream_buffer()` / `get_private_stream_buffer()` 싱글톤

---

## 2. WebSocket 레이어

| 파일 | 역할 |
|------|------|
| `bitget/data/ws_public.py` | V2 public WS — **ticker + books1** → `StreamBuffer` |
| `bitget/data/ws_private.py` | V2 private WS — login + orders/positions/account |
| `bitget/data/ws_supervisor.py` | public + private **asyncio 동시 실행** |

### 환경 변수

```bash
BITGET_WS_PUBLIC_URL=wss://ws.bitget.com/v2/ws/public
BITGET_WS_PRIVATE_URL=wss://ws.bitget.com/v2/ws/private
BITGET_WS_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT
BITGET_WS_ORDERBOOK_CHANNEL=books1   # books1 | books5 | books15
BITGET_ACCESS_KEY / BITGET_SECRET_KEY / BITGET_PASSPHRASE
```

### 실행

```bash
./bitget/deploy/bitget.sh --ws-supervisor
python -m bitget.data.ws_supervisor --symbols BTCUSDT,ETHUSDT
python -m bitget.data.ws_supervisor --public-only
sudo systemctl start dante-bitget-ws
```

---

## 3. Gap Healing

**파일:** `bitget/data/gap_healer.py`

- `assess_buffer_health()` — global + per-symbol ticker/orderbook stale 검사
- `heal_if_stale()` — stale 시 `mtf_data_updater.run_mtf_update()` REST 백필
- 파이프라인: `scan_all`, `data_refresh` 선행 step `gap_heal`

```bash
./bitget/deploy/bitget.sh --gap-heal
BITGET_GAP_HEAL_MAX_AGE_SEC=120
```

---

## 4. CQRS Snapshot

**파일:** `bitget/infra/snapshot_service.py`

| DB | 용도 |
|----|------|
| `bitget_market_data.sqlite` | 쓰기 SSOT |
| `bitget_market_data_snapshot.sqlite` | 읽기 복제본 (5분 주기) |

- `market_db_read_path()` — snapshot fresh면 읽기 경로로 snapshot 사용
- systemd: `dante-bitget-snapshot.timer` (`OnUnitActiveSec=5min`)

```bash
./bitget/deploy/bitget.sh --snapshot
python -m bitget.infra.snapshot_service --loop   # dev: 300s loop
BITGET_SNAPSHOT_INTERVAL_SEC=300
BITGET_SNAPSHOT_MAX_STALE_SEC=1800
```

---

## 5. 3단계 Slippage Guard

**파일:** `bitget/trading/slippage_guard.py`  
**연동:** `bitget/executor.py` (pre-trade), `forward/ledger.py` (pre-scan), `reconciliation.py` (post-trade)

| Stage | 함수 | 시점 |
|-------|------|------|
| 1 Pre-scan | `check_pre_scan_liquidity()` | 가상 진입 (seed vs 24h vol) |
| 2 Pre-trade | `run_pre_trade_gate()` | **실주문 직전** — WS orderbook spread |
| 3 Post-trade | `audit_post_trade_slippage()` | 체결 후 fill vs expected |

### Config 키

| 키 | 기본값 | 설명 |
|----|--------|------|
| `ENABLE_SLIPPAGE_GUARD` | `true` | pre-trade gate on/off |
| `SLIPPAGE_MAX_SPREAD_BPS` | `30` | 최대 허용 spread (bps) — **초과 시 `slippage_blocked`** |
| `SLIPPAGE_MAX_STALE_SEC` | `30` | WS 데이터 stale 차단 |
| `SLIPPAGE_REQUIRE_ORDERBOOK` | `false` | true면 orderbook 없으면 차단 |

Pre-trade gate는 **books1 호가창 spread**를 우선 참조하고, 없을 때만 ticker bid/ask 사용.  
WS 데이터가 전혀 없으면 `no_ws_data_skip`으로 통과 (개발/WS 미기동 환경).

---

## 6. 테스트

```bash
python -m unittest bitget.tests.test_data_phase4 bitget.tests.test_trading_phase5 -v
```

---

## 7. 아키텍처 다이어그램

```
Bitget WS (public)          Bitget WS (private)
  ticker + books1              orders/positions
        |                            |
        v                            v
  StreamBuffer              PrivateStreamBuffer
        |                            |
        +-------- slippage_guard -----+
        |         (pre-trade)         |
        v                             v
   executor.py                   OMS / reconciliation
        |
   gap_healer <-- stale --> mtf_data_updater (REST)

market_data.sqlite --[5min snapshot]--> market_data_snapshot.sqlite
                              ^
                    master_scanner read (CQRS)
```

---

## 8. 격리 확인

- [x] 루트 `forward/`, `factory_pipelines.py` 미수정
- [x] 스캐너 알맹이 (`master_scanner`, `supernova_hunter`) 미수정
- [x] Bitget 전용 systemd (`dante-bitget-ws`, `dante-bitget-snapshot.timer`)
