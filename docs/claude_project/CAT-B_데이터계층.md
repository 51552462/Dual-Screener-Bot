# CAT-B · 데이터 계층 & 저장소

> **위험도** 🔴 High (schema=Critical) · **Tier T2** · **also_load**: MAP, CONSTANTS  
> **never_with**: CAT-C scanner logic

---

## 1. 역할

SQLite SSOT, OHLCV, CQRS snapshot, 경로·스키마 가드. **모든 CAT의 바닥**.

---

## 2. 경로 SSOT

`factory_data_paths.factory_data_dir()`  
우선순위: env `DB_STORAGE_PATH` → `system_config.json` → legacy `~/dante_bots/Dual-Screener-Bot`

---

## 3. DB 카탈로그

| DB | SSOT module | 핵심 테이블/용도 |
|----|-------------|-----------------|
| `market_data.sqlite` | `market_db_paths` | `forward_trades`, `KR_*`/`US_*` OHLCV, meta logs |
| `market_data_snapshot.sqlite` | `data_updater` | CQRS read replica |
| `system_config.sqlite` | `config_manager` | `config_kv` |
| `message_queue.sqlite` | `telegram_message_queue` | `msg_queue` |
| `ops_events.sqlite` | `ops_logger` | heartbeat, gauge |
| `news_data.sqlite` | `news_data_paths` | sentiment |
| `alt_data.sqlite` | alt miners | macro_daily |
| `short_data.sqlite` | `blackhole_hunter` | **forward와 분리** |
| `synthetic_market.sqlite` | synthetic_data_generator | HTC |
| `regime_task_queue.sqlite` | regime_memory | weekend queue |
| `deep_archive_history.sqlite` | deep_archive_history | 10y+ analog |

**JSON 미러** (파일): treasury, regime ensemble, meta_governor, meta_trust_matrix

---

## 4. CQRS 읽기 정책 (Claude 설계)

`market_db_read_path()`:
- snapshot 존재 + age ≤ `MARKET_SNAPSHOT_MAX_STALE_SEC`(1800) → snapshot
- else → MAIN

**리포트**: `report_db_read_path()` — 기본 **MAIN 강제** (`REPORT_DEEP_DIVE_FORCE_MAIN_DB=1`)  
이유: stale snapshot 워터마크 착시 방지.

---

## 5. 스키마 가드

`sqlite_schema_guard.py`:
- `KNOWN_COLUMN_MIGRATIONS` — **ALTER ADD only**, DROP 금지
- `ensure_market_db_core_schema(heal=True)` — forward_trades 누락 자가치유

**Claude**: 새 컬럼 **의미·nullable·default** 정의  
**Cursor**: migration SQL, heal 로직

---

## 6. OHLCV 수집

| market | method | module |
|--------|--------|--------|
| KR | FDR per-ticker | `data_updater`, `market_data_fetcher` |
| US | yfinance batch 100 | + `yf_download_flatten` |

`data_updater.create_read_only_snapshot()`: online `sqlite3.backup` → atomic CQRS.

---

## 7. Pragmas & memory

- 모든 연결: `low_ram_sqlite_pragmas.apply_oom_safe_pragmas`
- `memory_bounds.py` — 대용량 read guard

---

## 8. 데이터 소유권 (MAP 참조)

- **short_forward_trades** ≠ forward_trades — 절대 merge
- forward_trades writer = CAT-D only

---

## 9. Claude 설계 범위

- 새 DB/테이블 **필요성** (가급적 기존 DB 확장)
- CQRS stale 정책
- 컬럼 semantics (forward_trades → CAT-D와 협의)

## 10. Cursor 구현

- WAL, backup, yfinance sleep, FDR retry, path env

---

## 11. 관련 config 키

`DB_STORAGE_PATH`, `MARKET_SNAPSHOT_MAX_STALE_SEC`, `REPORT_DEEP_DIVE_FORCE_MAIN_DB`

*상수: CAT-CONSTANTS*
