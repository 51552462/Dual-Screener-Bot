# TRACK_POSITIONS_AUDIT — `track_daily_positions` Bytes/Float 데이터 오염 전수 감사

**작성일:** 2026-05-26  
**증상 (Factory Job · `track_daily_positions_kr`)**  
```text
ValueError: could not convert string to float: b'\xc8\xc2\x01\x00\x00\x00\x00\x00'
```
- `track_daily_positions_kr`가 **critical=True** 스텝으로 실패 → 이후 `deep_dive_kr` 등 일일 감사 파이프라인 연쇄 중단 가능.

**호출 체인 (SSOT)**  
`factory_pipelines._step_track_kr` → `auto_forward_tester.track_daily_positions` (facade) → `forward/ledger.py::track_daily_positions(market)`

**DB 경로:** `market_db_paths.MARKET_DATA_DB_PATH` = `{factory_data_dir()}/market_data.sqlite`  
(`factory_data_paths.factory_data_dir()` — `DB_STORAGE_PATH` / `system_config.json` / 레거시 `~/dante_bots/Dual-Screener-Bot`)

---

## Executive Summary

| 항목 | 결론 |
|------|------|
| **오염 패턴** | 8바이트 리틀엔디언 IEEE-754 double BLOB (`\xc8\xc2\x01\x00\x00\x00\x00\x00`) — 텍스트·일반 주가 문자열이 **아님** |
| **1차 발원지 (P0)** | **`forward_trades` SQLite REAL 컬럼에 BLOB으로 잘못 적재** → `pd.read_sql`이 `bytes`로 반환 → **루프 밖** `float(r['entry_price'])`에서 즉시 크래시 |
| **2차 가능성** | 동일 패턴의 `max_high`, `min_low`, `up_vol_sum`, `v_energy` 등 — 다만 대부분은 `try` 내부라 **해당 행만 조용히 스킵** (`except: pass`) |
| **yfinance/FDR (OHLCV)** | 동일 `ValueError` 메시지는 이론상 가능하나, **해당 hex 패턴·8바이트 고정 길이**는 **장부 DB BLOB**과 일치도가 높음 |
| **기존 방어 코드** | `forward_report_scalar.scalar_float` 존재 — **`ledger.py`는 미사용** |
| **즉시 조치** | (1) DB 오염 행 탐지·복구 (3장) (2) `safe_float_cast` + 루프 밖 방어 (2장) (3) `except: pass` 제거·로깅 (2장 역제안) |

---

## 1. 이진 데이터(Bytes) 오염원 역추적 (P0)

### 1.1 에러 메시지 해부

Python 3에서 `float(b'\xc8\xc2\x01\x00\x00\x00\x00\x00')` 호출 시:

```text
ValueError: could not convert string to float: b'\xc8\xc2\x01\x00\x00\x00\x00\x00'
```

- `bytes` 객체를 **문자열처럼** 파싱하려다 실패한 전형적인 메시지.
- 길이 **8바이트** → SQLite에서 REAL 대신 **BLOB**으로 저장된 **바이너리 부동소수** 또는 손상된 double 패킷과 일치.
- `struct.unpack('<d', b'\xc8\xc2\x01\x00\x00\x00\x00\x00')` → 약 `5.7e-319` (비정상·denormal 수준) — **의미 있는 주가가 아님** → **데이터 오염**으로 판정.

### 1.2 크래시 정확 위치 (코드 증거)

```56:62:forward/ledger.py
    for _, r in df_active.iterrows():
        code = r['code']
        ep = float(r['entry_price'] or 0)
        _sig_raw = str(r.get('sig_type') or '')
        _is_observe_only = 'OBSERVE_ONLY' in _sig_raw

        try:
```

| 구간 | `float()` | `try` 블록 | Factory 영향 |
|------|-----------|------------|--------------|
| **L58 `entry_price`** | ✅ | ❌ **루프 밖** | **전체 `track_daily_positions_kr` 즉사** |
| L81 OHLCV (`df['Close']` 등) | ✅ | ✅ | 행 단위 스킵 (`pass`) |
| L100–104, 113, 126–128, 180+, 197, 343–346 | ✅ | ✅ | 행 단위 스킵 |
| L394 `base_seed` | ✅ | 루프 이후 | OPEN 0건이면 미도달 |

```387:387:forward/ledger.py
        except Exception as e: pass
```

- **첫 번째 OPEN 행**의 `entry_price`만 BLOB이어도 **함수 전체가 종료**되어 Factory critical FAIL.
- 이후 행은 처리되지 않음 → **KR 전 종목 일일 추적·청산 엔진 마비**.

### 1.3 데이터 유입 경로 판별

#### A. SQLite `forward_trades` (발원지 **최우선 · P0**)

```673:681:forward/shared.py
        CREATE TABLE IF NOT EXISTS forward_trades (
            ...
            entry_price REAL, ... max_high REAL, min_low REAL, ...
            up_vol_sum REAL DEFAULT 0, down_vol_sum REAL DEFAULT 0, ...
```

- 스키마상 **REAL** — 의도적 BLOB 컬럼 없음.
- `pd.read_sql("SELECT * FROM forward_trades WHERE market=? AND status='OPEN'", ...)` (ledger L14) 사용 시, 셀에 BLOB이 있으면 pandas는 **`dtype=object` + Python `bytes`** 로 노출.
- INSERT SSOT `_insert_forward_trade_row`는 `ep` 등 **float/int**를 기대 (`forward/shared.py` L2095 `entry_price: ep`) — **정상 경로만으로는 BLOB 생성 어려움** → 아래 **비정상 경로** 의심.

**BLOB 유입 가능 시나리오 (우선순위):**

1. **과거 버그/수동 SQL** — `UPDATE forward_trades SET entry_price=?` 에 `bytes` / `numpy` raw buffer / 잘못된 직렬화 객체 바인딩  
2. **DB 파일 손상·부분 복구** — WAL/스냅샷 불일치, `market_data_snapshot.sqlite`와 메인 DB 혼용 후 잘못된 행 복사  
3. **외부 도구** — DB Browser로 바이너리 붙여넣기, 다른 프로젝트 스키마에서 `ATTACH` 후 마이그레이션  
4. **간접 오염** — OHLCV 테이블(`"{code}"`)이 아닌 **`forward_trades` 장부**가 대상 (에러 스택이 ledger·OPEN 조회와 일치)

**정상 INSERT 경로 (참고 — 오염 원인 아님으로 추정):**

```2081:2113:forward/shared.py
    insert_row = {
        ...
        "entry_price": ep,
        "max_high": ep,
        "min_low": ep,
        ...
    }
    _insert_forward_trade_row(cursor, insert_row)
```

`ep`는 `try_add_virtual_position` 내 시세·ATR 계산 후 `ep * 1.005` (float) — **진입 시점에는 숫자**.

#### B. yfinance / FinanceDataReader (2차 · 낮은 확률)

```81:81:forward/ledger.py
            c, o, h, l, v = float(df['Close'].iloc[-1]), ...
```

- KR: `fdr.DataReader(code, start_date)`  
- US: `yf.download(code, ...)`

**반박 근거 (BLOB 패턴과 불일치):**

- API/CSV 캐시 손상 시 보통 `NaN`, 빈 문자열, `'N/A'` → 다른 예외 또는 `NaN` 전파.
- **고정 8바이트 `\xc8\xc2\x01\x00...`** 는 **SQLite BLOB REAL 오염** 패턴과 동형.
- OHLCV 오류는 **L58 이후** `try` 안에서만 터지므로 Factory 메시지가 **“모듈 전체 뻗음”** 이라기보다 **첫 행에서 즉시 종료**인 점과 **L58 우선** 가설이 맞음.

**예외:** `market_data.sqlite` 내 per-ticker OHLCV 테이블에 BLOB `Close`가 있으면 L81에서 동일 메시지 가능 — 이 경우에도 **DB BLOB**이 근원이며 API 직접 오염과는 구분해 **DB 무결성 스캔**으로 통합 처리 (3장).

#### C. 캐시 파일 / pandas 중복 컬럼

- `forward_report_scalar.dedupe_columns` — **딥다이브·리포트용**, `ledger.track_daily_positions` **미적용**.
- US MultiIndex는 L69–70에서 처리 — bytes 오염과 무관.

### 1.4 오염 가능 컬럼 전수 (`track_daily_positions`가 읽는 REAL 계열)

| 컬럼 | 읽기 위치 | 루프 밖? |
|------|-----------|----------|
| `entry_price` | L58 | **예 · 치명** |
| `max_high`, `min_low` | L100–101 | 아니오 |
| `up_vol_sum`, `down_vol_sum` | L103–104 (산술; bytes면 `TypeError` 가능) | 아니오 |
| `sim_kelly_invest`, `invest_amount` | L90–94 | 아니오 |
| `entry_atr` | L116–118 | 아니오 |
| `v_energy`, `total_score`, `dyn_rs`, `dyn_cpv`, `v_rs`, `v_cpv` | L197, 268, 343–346 | 아니오 |
| `entry_breadth` | L232–233 (`pd.isna`만) | 아니오 |

**P0 복구·검사 우선순위:** `entry_price` → `max_high` / `min_low` → `up_vol_sum` / `down_vol_sum` → `v_energy` / `total_score` / `dyn_*`

### 1.5 Factory가 “완전히 뻗는” 메커니즘

```138:157:factory_runtime.py
def run_step(spec: StepSpec) -> StepResult:
    ...
    except Exception as e:
        tb = traceback.format_exc()
        ...
        return StepResult(..., ok=False, error=f"{e}\n{tb[-800:]}")
```

```359:359:factory_pipelines.py
                StepSpec("track_daily_positions_kr", _step_track_kr, critical=True, ...),
```

- L58 예외는 **잡히지 않음** → `run_step`이 FAIL 반환 → 텔레그램 `[Factory Job] ... FAIL · track_daily_positions_kr (critical)`.

### 1.6 [역제안] 발원지 확정 절차 (운영 DB에서 5분 이내)

운영 서버에서 `factory_data_dir()` 확인 후 실행:

```sql
-- 1) OPEN KR 행 중 BLOB 의심 (entry_price)
SELECT id, code, name, typeof(entry_price) AS t_ep,
       length(entry_price) AS len_ep,
       hex(entry_price) AS hex_ep
FROM forward_trades
WHERE market = 'KR' AND status = 'OPEN'
  AND typeof(entry_price) = 'blob';

-- 2) 모든 REAL 컬럼 일괄 스캔 (동일 패턴)
SELECT id, code,
       typeof(entry_price) AS t1, typeof(max_high) AS t2,
       typeof(up_vol_sum) AS t3, typeof(v_energy) AS t4
FROM forward_trades
WHERE market = 'KR' AND status = 'OPEN'
  AND (typeof(entry_price) = 'blob'
    OR typeof(max_high) = 'blob'
    OR typeof(min_low) = 'blob'
    OR typeof(up_vol_sum) = 'blob'
    OR typeof(v_energy) = 'blob');
```

Python 원라이너 (pandas와 동일 조건):

```python
import sqlite3, pandas as pd
from market_db_paths import MARKET_DATA_DB_PATH
conn = sqlite3.connect(MARKET_DATA_DB_PATH, timeout=60)
df = pd.read_sql("SELECT * FROM forward_trades WHERE market='KR' AND status='OPEN'", conn)
conn.close()
NUM = ["entry_price","max_high","min_low","up_vol_sum","down_vol_sum",
       "v_energy","total_score","dyn_rs","dyn_cpv","sim_kelly_invest"]
for col in NUM:
    if col not in df.columns: continue
    bad = df[df[col].map(lambda x: isinstance(x, (bytes, bytearray)))]
    if not bad.empty:
        print(col, bad[["id","code","name",col]].to_string())
```

**로컬 개발 환경:** 이 워크스페이스 기준 `market_data.sqlite` **미존재** (`~/dante_bots/Dual-Screener-Bot` 경로) — **운영 DB에서만** 위 쿼리로 확정 가능.

---

## 2. Defensive Casting — 방어적 형변환 방어벽 (P0)

### 2.1 현황: 방어 모듈은 있으나 장부 엔진은 미연결

```25:42:forward_report_scalar.py
def scalar_float(val: Any, default: float = 0.0) -> float:
    ...
    try:
        f = float(val)
    except (TypeError, ValueError):
        return float(default)
```

- `bytes` → `float()` → **ValueError** → `default` 반환 (**크래시는 막음**).
- **한계:** 8바이트 IEEE double BLOB을 **의도된 가격으로 디코딩하지 않음** (오염값을 0.0으로 떨어뜨림 — **안전하지만 회계 복구는 3장과 병행**).

`forward/shared.py`는 `scalar_float`를 import하지만 **`forward/ledger.py`는 사용하지 않음**.

### 2.2 [역제안] `safe_float_cast` — SSOT 확장

`forward_report_scalar.py`에 추가 (기존 `scalar_float` 호출부 호환 유지):

```python
import struct
from typing import Any
import numpy as np
import pandas as pd

def safe_float_cast(val: Any, default: float = float("nan"), *, log_context: str = "") -> float:
    """
    bytes(BLOB) · Series · NaN · 쓰레기 문자열 → float 또는 nan.
    - 4/8바이트 little-endian IEEE float/double 디코딩 1회 시도
    - 실패 시 default (ledger 진입가는 0.0, 지표는 nan 권장)
    """
    if val is None:
        return default
    if isinstance(val, (bytes, bytearray)):
        b = bytes(val)
        for fmt in ("<d", "<f"):
            if len(b) == struct.calcsize(fmt):
                try:
                    x = struct.unpack(fmt, b)[0]
                    return x if np.isfinite(x) else default
                except struct.error:
                    pass
        try:
            return scalar_float(b.decode("utf-8", errors="ignore").strip(), default)
        except Exception:
            return default
    if isinstance(val, (np.floating, np.integer)):
        val = val.item()
    return scalar_float(val, default)
```

**OHLCV 마지막 봉 (ledger L81 대체):**

```python
def ohlcv_last_floats(df: pd.DataFrame) -> tuple[float, float, float, float, float]:
    cols = ["Close", "Open", "High", "Low", "Volume"]
    out = []
    for c in cols:
        s = pd.to_numeric(df[c], errors="coerce")
        out.append(safe_float_cast(s.iloc[-1], default=float("nan")))
    return tuple(out)  # c,o,h,l,v — nan이면 상위에서 continue
```

### 2.3 `forward/ledger.py` 적용 체크리스트

| 우선순위 | 위치 | 변경 |
|----------|------|------|
| **P0** | L58 | `ep = safe_float_cast(r.get("entry_price"), 0.0, log_context=f"id={r['id']}")` — **`try` 밖 유지 가능** |
| **P0** | L56–57 | `for _, r in df_active.iterrows():` → **`try` 전체를 행 단위 try/finally로 감싸고**, L58도 내부화 **또는** L58만 safe cast |
| **P0** | L14 직후 | `df_active = prepare_forward_trades_df(df_active, context="track_daily_positions")` + REAL 컬럼 `pd.to_numeric` 일괄 |
| P1 | L81 | `ohlcv_last_floats(df)` — `any(np.isnan([c,o,h,l,v]))` 시 continue |
| P1 | L100–104, 90–94, 113, 126–128, 180+, 197, 268, 343–346 | `safe_float_cast` / `row_scalar(r, col)` |
| P1 | L103–104 | `new_up_vol = safe_float_cast(r.get("up_vol_sum"), 0.0) + ...` |
| P2 | L387 | `except Exception as e: pass` → **로깅 + `continue`** (`logger.warning("track skip id=%s: %s", r['id'], e)`) |
| P2 | INSERT | `_insert_forward_trade_row` 직전 `insert_row` REAL 필드 `safe_float_cast` 검증 — **BLOB 재유입 차단** |

### 2.4 [역제안] 정책

1. **`entry_price`가 nan/0이고 id 존재** → 해당 OPEN 행 **스킵 + 텔레그램 1회 요약** (팩토리 전체 중단 금지).  
2. **BLOB 디코딩 성공해도** 가격 범위 sanity (`0 < ep < 1e9` KR / US) 실패 시 **스킵 + DB 플래그** `exit_reason='DATA_CORRUPT_ENTRY_PRICE'`.  
3. **리포트·딥다이브**는 기존 `scalar_float` 유지, **장부 엔진만 `safe_float_cast`**로 통일해 SSOT 분리 최소화 (`forward_report_scalar` 한 파일).

### 2.5 yfinance/FDR 측 (보조)

- L81 전 `df = dedupe_columns(df, context=code)` (선택).  
- `pd.to_numeric(df[col], errors="coerce")` on all OHLCV columns after download.  
- **근본 해결은 DB 청소(3장)** — API만 막아서는 이미 오염된 `entry_price` BLOB이 L58에서 계속 터짐.

---

## 3. 장부(DB) 무결성 복구 스크립트

### 3.1 사전 백업 (필수)

```bash
# PowerShell 예시 — DATA 루트는 환경에 맞게 수정
$DATA = "$env:USERPROFILE\dante_bots\Dual-Screener-Bot"   # 또는 DB_STORAGE_PATH
Copy-Item "$DATA\market_data.sqlite" "$DATA\market_data.sqlite.bak_$(Get-Date -Format yyyyMMdd_HHmmss)"
```

WAL 사용 중이면 **`.sqlite-wal` / `.sqlite-shm`** 도 함께 백업하거나, 복구 전 `PRAGMA wal_checkpoint(TRUNCATE);` 실행.

### 3.2 탐지 SQL (전체 장부)

```sql
-- BLOB으로 저장된 REAL 컬럼 후보 (CLOSED 포함 전수)
SELECT id, market, code, status, 'entry_price' AS col, typeof(entry_price) AS tp, hex(entry_price) AS hx
FROM forward_trades WHERE typeof(entry_price) = 'blob'
UNION ALL
SELECT id, market, code, status, 'max_high', typeof(max_high), hex(max_high)
FROM forward_trades WHERE typeof(max_high) = 'blob'
UNION ALL
SELECT id, market, code, status, 'up_vol_sum', typeof(up_vol_sum), hex(up_vol_sum)
FROM forward_trades WHERE typeof(up_vol_sum) = 'blob';
```

### 3.3 복구 전략 (택 1 또는 병행)

| 전략 | 대상 | 조치 |
|------|------|------|
| **A. 디코딩 복구** | 8바이트 hex가 유효 double이고 `0 < price < 1e7` | Python으로 decode 후 `UPDATE ... SET entry_price=?` |
| **B. OHLCV 재조회** | OPEN + `entry_date` 있음 | `fdr`/`yf` 해당일 `Close`로 `entry_price`·`max_high`·`min_low` 재설정 |
| **C. 강제 청산** | 복구 불가·거래정지 | 기존 L77 패턴: `CLOSED_LOSS`, `exit_reason='DATA_CORRUPT_AUTO_CLOSE'` |
| **D. 행 삭제** | 테스트·중복 OPEN만 | **최후 수단** — id·code 수동 확인 후 `DELETE` |

### 3.4 [역제안] 일회성 복구 스크립트 (`scripts/repair_forward_trades_numeric_corruption.py`)

저장소에 아래를 추가해 운영에서 `--dry-run` / `--apply` 로 실행하는 것을 권장:

```python
#!/usr/bin/env python3
"""forward_trades REAL 컬럼 BLOB/bytes 오염 탐지·복구."""
from __future__ import annotations

import argparse
import sqlite3
import struct
from typing import Any, Optional

import numpy as np

from market_db_paths import MARKET_DATA_DB_PATH

REAL_COLS = (
    "entry_price", "max_high", "min_low", "up_vol_sum", "down_vol_sum",
    "final_ret", "mfe", "total_score", "dyn_rs", "dyn_cpv", "dyn_tb",
    "v_cpv", "v_yang", "v_rs", "v_energy", "entry_atr",
    "invest_amount", "sim_kelly_invest", "sim_kelly_risk_pct",
    "market_breadth", "entry_breadth", "entry_cos_score", "entry_dtw_score",
)


def blob_to_float(b: bytes) -> Optional[float]:
    for fmt in ("<d", "<f", ">d", ">f"):
        if len(b) != struct.calcsize(fmt):
            continue
        try:
            x = struct.unpack(fmt, b)[0]
            if np.isfinite(x):
                return float(x)
        except struct.error:
            continue
    return None


def scan(conn: sqlite3.Connection) -> list[tuple]:
    bad = []
    for col in REAL_COLS:
        try:
            rows = conn.execute(
                f"SELECT id, market, code, status, ? FROM forward_trades "
                f"WHERE typeof({col}) = 'blob'",
                (col,),
            ).fetchall()
        except sqlite3.OperationalError:
            continue
        for rid, mkt, code, st, raw in rows:
            if isinstance(raw, (bytes, bytearray)):
                bad.append((rid, mkt, code, st, col, bytes(raw)))
    return bad


def repair_row(conn: sqlite3.Connection, rid: int, col: str, raw: bytes, apply: bool) -> str:
    x = blob_to_float(raw)
    if x is None or not (0 < abs(x) < 1e10):
        sql = (
            "UPDATE forward_trades SET status='CLOSED_LOSS', final_ret=-15.0, "
            "exit_reason=? WHERE id=?"
        )
        args = (f"DATA_CORRUPT_{col}", rid)
        action = "FORCE_CLOSE"
    else:
        sql = f"UPDATE forward_trades SET {col}=? WHERE id=?"
        args = (x, rid)
        action = f"SET_{col}={x}"
    if apply:
        conn.execute(sql, args)
    return action


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=MARKET_DATA_DB_PATH)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true", default=True)
    args = p.parse_args()
    apply = args.apply and not args.dry_run
    conn = sqlite3.connect(args.db, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL;")
    bad = scan(conn)
    print(f"found {len(bad)} blob cells")
    for item in bad:
        rid, mkt, code, st, col, raw = item
        act = repair_row(conn, rid, col, raw, apply)
        print(f"id={rid} {mkt} {code} {st} {col} hex={raw.hex()[:32]} -> {act}")
    if apply:
        conn.commit()
        print("committed")
    else:
        print("dry-run only; use --apply to write")
    conn.close()


if __name__ == "__main__":
    main()
```

**실행 예:**

```bash
python scripts/repair_forward_trades_numeric_corruption.py --dry-run
python scripts/repair_forward_trades_numeric_corruption.py --apply
```

### 3.5 OPEN `entry_price`만 긴급 완화 (SQL)

디코딩 없이 **팩토리 재가동만** 목적일 때 (가격은 임시 0 — **반드시 3.4로 정밀 복구**):

```sql
-- 위험: 회계 왜곡. 임시 unblock 용도만.
UPDATE forward_trades
SET entry_price = 0.0,
    exit_reason = COALESCE(exit_reason, '') || ' [TEMP_BLOB_ZEROED]'
WHERE status = 'OPEN' AND typeof(entry_price) = 'blob';
```

권장: **BLOB → decode 성공 시만 UPDATE**, 실패 시 **C. 강제 청산**.

### 3.6 재발 방지

1. `_insert_forward_trade_row` 직전 REAL 필드 `safe_float_cast` (2장).  
2. 주간 cron: `scan()` 결과를 `ops_logger` / 텔레그램 메타 알림.  
3. `market_data_snapshot.sqlite` 생성 파이프라인에서 **메인 DB 무결성 검사 후** 스냅샷 복제.

---

## 4. 구현 우선순위 (역제안 요약)

| 순서 | 작업 | 효과 |
|------|------|------|
| **1** | 운영 DB 1.6절 SQL로 **오염 id·컬럼 확정** | 발원지 팩트 고정 |
| **2** | 3.4 복구 스크립트 `--apply` 또는 B/OHLCV 재조회 | **재발 크래시 제거** |
| **3** | `safe_float_cast` + **ledger L58·L14** | Factory critical FAIL 방지 |
| **4** | L387 `pass` → log + continue | 일부 행 오염 시에도 나머지 OPEN 추적 |
| **5** | INSERT 게이트 | BLOB 재유입 차단 |

---

## 5. 참고 — 관련 파일

| 파일 | 역할 |
|------|------|
| `forward/ledger.py` | `track_daily_positions` 본체 |
| `forward/shared.py` | `init_forward_db`, `_insert_forward_trade_row`, `try_add_virtual_position` |
| `forward_report_scalar.py` | `scalar_float`, `prepare_forward_trades_df` (확장 대상) |
| `factory_pipelines.py` | `track_daily_positions_kr` critical 스텝 |
| `factory_runtime.py` | 스텝 실패 → 텔레그램 FAIL |
| `market_db_paths.py` | `market_data.sqlite` 경로 |
| `auto_forward_tester.py` | public facade |

---

*본 문서는 코드 정적 전수 감사 기준이며, 로컬 워크스페이스에 `market_data.sqlite`가 없어 BLOB 행 id는 운영 DB 스캔으로 확정해야 합니다.*
