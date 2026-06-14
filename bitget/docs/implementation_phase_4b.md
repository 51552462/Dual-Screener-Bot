# Bitget Phase 4b — Private WS · CQRS Snapshot · Slippage Gate

> 작성일: 2026-06-07  
> **주식 루트 미수정** — 모든 변경 `bitget/` 내부.

Phase 4a(공개 WS·gap_heal·deploy) 이후 잔여 항목 완료.

---

## 1. Private WebSocket

| 파일 | 역할 |
|------|------|
| `bitget/data/ws_private.py` | V2 private WS — login + orders/positions/account |
| `bitget/data/stream_buffer.py` | `PrivateStreamBuffer` — 주문·포지션 in-memory 캐시 |
| `bitget/data/ws_supervisor.py` | public + private 동시 실행 (API 키 있을 때) |

### 인증

- URL: `BITGET_WS_PRIVATE_URL` (기본 `wss://ws.bitget.com/v2/ws/private`)
- Sign: `HMAC-SHA256(timestamp + "GET" + "/user/verify")` → Base64
- Env: `BITGET_ACCESS_KEY`, `BITGET_SECRET_KEY`, `BITGET_PASSPHRASE` (`bitget/env.py`)

### 실행

```bash
./bitget/deploy/bitget.sh --ws-supervisor
python -m bitget.data.ws_supervisor --public-only   # private 스킵
```

---

## 2. CQRS Snapshot Service

| 파일 | 역할 |
|------|------|
| `bitget/infra/snapshot_service.py` | main DB → snapshot SQLite backup |
| `bitget/infra/data_paths.py` | `market_db_read_path()`, `report_db_read_path()` |
| `bitget/deploy/systemd/dante-bitget-snapshot.*` | 5분 주기 timer |

### 경로

- 쓰기 SSOT: `bitget_market_data.sqlite`
- 읽기 복제본: `bitget_market_data_snapshot.sqlite`
- 스캐너 OHLCV 읽기: `master_scanner.py` → `market_db_read_path()`
- 스냅샷 stale 임계: `BITGET_SNAPSHOT_MAX_STALE_SEC` (기본 1800초)

### 실행

```bash
./bitget/deploy/bitget.sh --snapshot
python -m bitget.infra.snapshot_service
sudo systemctl start dante-bitget-snapshot.timer
```

---

## 3. Slippage Guard → Executor

`bitget/executor.py` 실주문 직전 `estimate_slippage_bps()` 호출.

| Config 키 | 기본값 | 설명 |
|-----------|--------|------|
| `ENABLE_SLIPPAGE_GUARD` | `true` | WS spread gate on/off |
| `SLIPPAGE_MAX_SPREAD_BPS` | `30` | 최대 허용 spread (bps) |
| `SLIPPAGE_MAX_STALE_SEC` | `30` | ticker stale 차단 임계 |

차단 시 status: `slippage_blocked` (WS 버퍼 없으면 `no_ws_ticker_skip`으로 통과).

---

## 4. Windows / cron 안전 출력

`mtf_data_updater.py` 이모지 print → `[WARN]` / `[OK]` / `[START]` 등 ASCII 접두사로 교체 (cp949 UnicodeEncodeError 방지).

---

## Phase 4 완료 기준

- [x] public WS + gap_heal
- [x] private WS (orders/positions/account)
- [x] CQRS snapshot + read path
- [x] executor slippage gate
- [x] systemd snapshot timer

---

## 다음 단계 (Phase 5)

- `position_manager.py` / `leverage_manager.py` / `reconciliation.py` OMS 분리
- 3-stage slippage (pre-scan / pre-trade / post-trade)
- `forward/_core.py` 물리 분할 (Phase 3 잔여)
