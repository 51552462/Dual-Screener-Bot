# CAT-B · 데이터 계층 (Bitget)

> **위험도** 🔴 High (schema) · **Tier T2** · **also_load**: MAP, CONSTANTS  
> **never_with**: CAT-D book logic 동시 schema redesign

---

## 1. 역할

OHLCV SQLite, WebSocket 실시간, data refresh, gap heal, symbol universe, CQRS snapshot.

---

## 2. SSOT

| 역할 | 파일 |
|------|------|
| 경로 SSOT | `infra/data_paths.py` |
| DB init / lock | `forward/shared.py` (Bitget fork) |
| OHLCV refresh | `data_miner.py`, pipelines `data_refresh` |
| WebSocket market | WS service (systemd `dante-bitget-ws`) |
| WebSocket private/OMS | `trading/oms_core.py`, WS private |
| snapshot CQRS | snapshot timer + `infra/` |
| institutional backup | `institutional_db_backup.py` (PRAGMA integrity) |

---

## 3. DB 격리

| DB | 경로 키 |
|----|---------|
| market_data | `BITGET_DB_STORAGE_PATH` / `bitget_market_data.sqlite` |
| forward trades | 동일 root |
| system config | `bitget_system_config.sqlite` |
| ops events | `ops_events.sqlite` (Bitget 전용) |

**격리 원칙**: 주식 `market_data.sqlite` **절대 공유 금지**

---

## 4. 테이블 네이밍

| prefix | 의미 |
|--------|------|
| `BITGET_SPOT_*` | 현물 OHLCV |
| `BITGET_FUT_*` | 선물 OHLCV |
| `bitget_forward_trades` | paper book |

---

## 5. WebSocket (코인 전용 · CAT-A link)

- market WS: 가격·캔들 feed
- private WS: position·order·margin (OMS hydrate)
- 재연결: 지수 백오프 + systemd restart

---

## 6. 알려진 갭

| # | 이슈 | Phase |
|---|------|-------|
| 1 | load_test symbol_count=0 (dev DB) | Track B |
| 2 | integrity backup cron 미연결 | P0-5 |
| 3 | bad tick filter 없음 | P1-6 (CAT-C link) |
| 4 | SPOT/FUT **initial backfill lookback 비대칭** — VPS FUT_1D 과거 n≈90 vs SPOT≈300. **원인**: `mtf_data_updater` tail-only(limit, no since). **API**: Bitget swap 1D `since`+paginate로 90일 이전 **제공 확인**(2026-08-29 · LANE_FULLBT). **조치**: `bitget/data/ohlcv_history_backfill.py` 파일럿 Adapter(BTC/ETH/SOL) · 기본 refresh 비접촉. VPS 적용·Claude OK 대기 | LANE_FULLBT FULL-BT-FUT-DEPTH-1 |

---

## 7. Claude 설계 대상

- schema migration OCC (forward book)
- WS→OMS price snapshot SSOT
- backup cron + integrity gate
- dev vs prod `BITGET_DB_STORAGE_PATH` 분리

---

## 8. Single Writer

- OHLCV rows: `data_miner` / refresh pipelines only
- Readers: C, D, G, validation
