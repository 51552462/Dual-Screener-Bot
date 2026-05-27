# DEEP_SCAN_AND_RECOVERY_TOOLS

요청사항에 따라 워크스페이스에는 별도 `.sh`/`.py` 파일을 만들지 않고,  
서버에서 바로 복붙 실행 가능한 **Deep Scan 스크립트**와 **SQLite Merge 유틸리티**를 본 문서에 제공한다.

---

## 1) Deep Scan Shell Script (서버 투입용)

아래 스크립트는 Ubuntu 서버에서 백업/유실 의심 파일을 광범위하게 탐색한다.

- 대상 패턴:
  - `dante-sqlite-*.tar.gz`
  - `market_data*.sqlite`
  - `ops_events*.sqlite`
  - `*.bak`
  - `*.snapshot`
- 기본 필터: `1MB 이상` 파일만 출력
- 권한 에러(`Permission denied`)는 자동 무시
- 출력: `SIZE_MB|PATH|MTIME`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ==============================
# Deep backup scan for Ubuntu
# ==============================
#
# Usage:
#   bash deep_scan_backups.sh
#   MIN_MB=5 bash deep_scan_backups.sh
#   EXTRA_ROOTS="/mnt,/data,/backup,/srv" bash deep_scan_backups.sh
#
# Output format:
#   SIZE_MB|PATH|MTIME
#

MIN_MB="${MIN_MB:-1}"
EXTRA_ROOTS="${EXTRA_ROOTS:-/mnt,/media,/data,/backup,/srv,/var/backups,/home}"

tmpfile="$(mktemp)"
trap 'rm -f "$tmpfile"' EXIT

# 기본 루트 + 추가 루트
ROOTS=("/")
IFS=',' read -r -a EXTRA <<< "$EXTRA_ROOTS"
for r in "${EXTRA[@]}"; do
  r="$(echo "$r" | xargs || true)"
  [[ -n "$r" && -d "$r" ]] && ROOTS+=("$r")
done

# 중복 제거
mapfile -t ROOTS < <(printf '%s\n' "${ROOTS[@]}" | awk '!seen[$0]++')

# find 표현식 생성
NAME_EXPR=(
  -name "dante-sqlite-*.tar.gz" -o
  -name "market_data*.sqlite"   -o
  -name "ops_events*.sqlite"    -o
  -name "*.bak"                 -o
  -name "*.snapshot"            -o
  -name "*.sqlite.bak*"         -o
  -name "*backup*.sqlite"       -o
  -name "*restore*.sqlite"
)

# 실제 스캔
for root in "${ROOTS[@]}"; do
  find "$root" \
    -xdev \
    -type f \
    \( "${NAME_EXPR[@]}" \) \
    -size +"${MIN_MB}"M \
    -print0 2>/dev/null || true
done >"$tmpfile"

# 결과 없으면 종료
if [[ ! -s "$tmpfile" ]]; then
  echo "No candidate files found (>= ${MIN_MB}MB)."
  exit 0
fi

# 출력 정리 (size/path/mtime) + size desc sort
python3 - <<'PY' "$tmpfile"
import os
import sys
import datetime

tmp = sys.argv[1]
raw = open(tmp, "rb").read().split(b"\x00")
paths = []
seen = set()
for b in raw:
    if not b:
        continue
    p = b.decode("utf-8", errors="ignore")
    if p in seen:
        continue
    seen.add(p)
    if os.path.isfile(p):
        paths.append(p)

rows = []
for p in paths:
    try:
        st = os.stat(p)
    except OSError:
        continue
    size_mb = st.st_size / (1024 * 1024)
    mtime = datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    rows.append((size_mb, p, mtime))

rows.sort(key=lambda x: x[0], reverse=True)
for size_mb, p, mtime in rows:
    print(f"{size_mb:.2f}|{p}|{mtime}")
PY
```

### 추천 실행 예시

```bash
# 1MB 이상 후보 전체
bash deep_scan_backups.sh

# 10MB 이상만 빠르게
MIN_MB=10 bash deep_scan_backups.sh

# 커스텀 경로 추가
EXTRA_ROOTS="/mnt,/data,/backup,/srv,/var/lib" MIN_MB=5 bash deep_scan_backups.sh
```

---

## 2) SQLite Data Merge Utility (ATTACH DATABASE 기반)

아래 파이썬 유틸리티는 **현재 운영 DB(target)**에 **과거 DB(source)**의 핵심 테이블을 중복 없이 병합한다.

핵심 특성:
- `ATTACH DATABASE` 사용
- 트랜잭션(`BEGIN IMMEDIATE`) 기반
- 테이블별로 PK/Unique 충돌을 피하기 위해 `INSERT OR IGNORE`
- 기본 병합 대상:
  - `forward_trades`
  - `shadow_trades`
  - `trade_logs`
  - `ops_events`
- 테이블 존재 여부를 자동 점검하여 없는 테이블은 스킵
- Dry-run 모드 지원

```python
#!/usr/bin/env python3
import argparse
import sqlite3
from datetime import datetime


DEFAULT_TABLES = [
    "forward_trades",
    "shadow_trades",
    "trade_logs",
    "ops_events",
]


def table_exists(conn: sqlite3.Connection, schema: str, table: str) -> bool:
    row = conn.execute(
        f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def get_columns(conn: sqlite3.Connection, schema: str, table: str):
    rows = conn.execute(f"PRAGMA {schema}.table_info('{table}')").fetchall()
    return [r[1] for r in rows]  # cid, name, type, notnull, dflt_value, pk


def ensure_indexes(conn: sqlite3.Connection):
    # 자주 쓰는 테이블 성능 보조 (있으면 무시)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_forward_trades_exit_date ON forward_trades(exit_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ops_events_ts ON ops_events(ts_utc)"
    )


def merge_table(conn: sqlite3.Connection, table: str, dry_run: bool = False):
    if not table_exists(conn, "main", table):
        print(f"[SKIP] main.{table} not found")
        return
    if not table_exists(conn, "src", table):
        print(f"[SKIP] src.{table} not found")
        return

    main_cols = get_columns(conn, "main", table)
    src_cols = get_columns(conn, "src", table)
    common_cols = [c for c in main_cols if c in src_cols]

    if not common_cols:
        print(f"[SKIP] {table} has no common columns")
        return

    col_sql = ", ".join([f'"{c}"' for c in common_cols])

    # INSERT OR IGNORE: PK/UNIQUE 충돌 시 자동 스킵
    sql = f'''
        INSERT OR IGNORE INTO "{table}" ({col_sql})
        SELECT {col_sql} FROM src."{table}"
    '''

    src_count = conn.execute(f'SELECT COUNT(*) FROM src."{table}"').fetchone()[0]
    main_before = conn.execute(f'SELECT COUNT(*) FROM main."{table}"').fetchone()[0]

    if dry_run:
        print(
            f"[DRY-RUN] {table}: src_rows={src_count}, main_before={main_before}, common_cols={len(common_cols)}"
        )
        return

    conn.execute(sql)
    main_after = conn.execute(f'SELECT COUNT(*) FROM main."{table}"').fetchone()[0]
    inserted = main_after - main_before
    print(
        f"[OK] {table}: src_rows={src_count}, inserted={inserted}, main_before={main_before}, main_after={main_after}"
    )


def main():
    ap = argparse.ArgumentParser(description="Merge historical sqlite into current db safely")
    ap.add_argument("--target-db", required=True, help="Current live DB path (main)")
    ap.add_argument("--source-db", required=True, help="Historical DB path (src)")
    ap.add_argument(
        "--tables",
        default=",".join(DEFAULT_TABLES),
        help=f"Comma-separated tables (default: {','.join(DEFAULT_TABLES)})",
    )
    ap.add_argument("--dry-run", action="store_true", help="Analyze only, no writes")
    args = ap.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    conn = sqlite3.connect(args.target_db, timeout=120)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    try:
        conn.execute("ATTACH DATABASE ? AS src", (args.source_db,))
        print(f"[INFO] target={args.target_db}")
        print(f"[INFO] source={args.source_db}")
        print(f"[INFO] tables={tables}")
        print(f"[INFO] started_at={datetime.now().isoformat(timespec='seconds')}")

        if not args.dry_run:
            conn.execute("BEGIN IMMEDIATE")

        for t in tables:
            merge_table(conn, t, dry_run=args.dry_run)

        if not args.dry_run:
            ensure_indexes(conn)
            conn.commit()
            print("[COMMIT] merge committed")
        else:
            print("[DRY-RUN] no data changed")
    except Exception as e:
        if not args.dry_run:
            conn.rollback()
        raise
    finally:
        try:
            conn.execute("DETACH DATABASE src")
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    main()
```

---

## 3) 사용 가이드 (권장 순서)

## Step A. 후보 백업 스캔

1. 서버에 접속
2. 위 `deep_scan_backups.sh`를 파일로 저장 후 실행
3. 결과에서 `market_data*.sqlite` 또는 `dante-sqlite-*.tar.gz` 중 대용량 파일 우선 확인

## Step B. 백업이 tar.gz인 경우 먼저 추출

```bash
mkdir -p /tmp/dante-restore
tar -xzf /path/to/dante-sqlite-YYYYMMDDTHHMMSSZ.tar.gz -C /tmp/dante-restore
ls -lh /tmp/dante-restore
```

## Step C. Merge 유틸리티 Dry-run

```bash
python3 sqlite_merge_utility.py \
  --target-db /var/lib/dante-quant-factory/data/market_data.sqlite \
  --source-db /tmp/dante-restore/market_data.sqlite \
  --dry-run
```

## Step D. 실제 Merge 실행

```bash
python3 sqlite_merge_utility.py \
  --target-db /var/lib/dante-quant-factory/data/market_data.sqlite \
  --source-db /tmp/dante-restore/market_data.sqlite
```

## Step E. 검증 쿼리

```bash
sqlite3 /var/lib/dante-quant-factory/data/market_data.sqlite "SELECT COUNT(*) FROM forward_trades;"
sqlite3 /var/lib/dante-quant-factory/data/market_data.sqlite "SELECT MAX(exit_date) FROM forward_trades;"
sqlite3 /var/lib/dante-quant-factory/data/market_data.sqlite "SELECT COUNT(*) FROM ops_events;"
```

---

## 4) 안전 수칙 (반드시 준수)

- Merge 전 운영 DB를 반드시 백업:

```bash
cp -a /var/lib/dante-quant-factory/data/market_data.sqlite \
      /var/lib/dante-quant-factory/data/market_data.sqlite.premerge.$(date -u +%Y%m%dT%H%M%SZ)
```

- 운영 서비스가 DB write 중이면 merge 전에 중지 권장:
  - `sudo systemctl stop dante-factory dante-main dante-async dante-dashboard`
- 먼저 `--dry-run`으로 대상 테이블/컬럼 호환 여부 확인
- 복구 완료 후 `sqlite_schema_guard`/`factory_artifact_guard` 실행으로 파생물 정합성 회복

---

## 5) 확장 포인트 (필요 시)

- 테이블별 커스텀 키를 지정한 UPSERT(`ON CONFLICT DO UPDATE`) 모드 추가
- 날짜 범위 병합 (`WHERE exit_date >= ...`) 옵션 추가
- 병합 리포트(JSON) 자동 저장

---

## 6) 실서버 원클릭 실행 순서 (복붙용)

아래 블록은 서버에서 **한 번에 복붙**해서 실행할 수 있는 최소 절차다.

```bash
set -euo pipefail

# 0) 작업 디렉터리
mkdir -p /tmp/dante-recovery && cd /tmp/dante-recovery

# 1) Deep Scan 스크립트 저장
cat > deep_scan_backups.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
MIN_MB="${MIN_MB:-1}"
EXTRA_ROOTS="${EXTRA_ROOTS:-/mnt,/media,/data,/backup,/srv,/var/backups,/home}"
tmpfile="$(mktemp)"; trap 'rm -f "$tmpfile"' EXIT
ROOTS=("/"); IFS=',' read -r -a EXTRA <<< "$EXTRA_ROOTS"
for r in "${EXTRA[@]}"; do r="$(echo "$r" | xargs || true)"; [[ -n "$r" && -d "$r" ]] && ROOTS+=("$r"); done
mapfile -t ROOTS < <(printf '%s\n' "${ROOTS[@]}" | awk '!seen[$0]++')
NAME_EXPR=(-name "dante-sqlite-*.tar.gz" -o -name "market_data*.sqlite" -o -name "ops_events*.sqlite" -o -name "*.bak" -o -name "*.snapshot" -o -name "*.sqlite.bak*" -o -name "*backup*.sqlite" -o -name "*restore*.sqlite")
for root in "${ROOTS[@]}"; do find "$root" -xdev -type f \( "${NAME_EXPR[@]}" \) -size +"${MIN_MB}"M -print0 2>/dev/null || true; done >"$tmpfile"
python3 - <<'PY' "$tmpfile"
import os,sys,datetime
raw=open(sys.argv[1],"rb").read().split(b"\x00"); seen=set(); rows=[]
for b in raw:
    if not b: continue
    p=b.decode("utf-8","ignore")
    if p in seen or not os.path.isfile(p): continue
    seen.add(p); st=os.stat(p)
    rows.append((st.st_size/(1024*1024), p, datetime.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")))
rows.sort(key=lambda x:x[0], reverse=True)
for sz,p,t in rows: print(f"{sz:.2f}|{p}|{t}")
PY
EOF
chmod +x deep_scan_backups.sh

# 2) Deep Scan 실행 (결과 저장)
MIN_MB=1 ./deep_scan_backups.sh | tee deep_scan_results.txt

# 3) 후보 확인 (예: 상위 20개)
head -n 20 deep_scan_results.txt

# 4) Merge 유틸 저장
cat > sqlite_merge_utility.py <<'EOF'
import argparse,sqlite3
DEFAULT_TABLES=["forward_trades","shadow_trades","trade_logs","ops_events"]
def ex(c,s,t): return c.execute(f"SELECT 1 FROM {s}.sqlite_master WHERE type='table' AND name=?",(t,)).fetchone() is not None
def cols(c,s,t): return [r[1] for r in c.execute(f"PRAGMA {s}.table_info('{t}')").fetchall()]
def merge(c,t,dry=False):
    if not ex(c,"main",t) or not ex(c,"src",t): print(f"[SKIP] {t}"); return
    common=[x for x in cols(c,"main",t) if x in cols(c,"src",t)]
    if not common: print(f"[SKIP] {t} no common cols"); return
    q=", ".join([f'"{x}"' for x in common]); b=c.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
    if dry: print(f"[DRY] {t} main_before={b}"); return
    c.execute(f'INSERT OR IGNORE INTO "{t}" ({q}) SELECT {q} FROM src."{t}"')
    a=c.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]; print(f"[OK] {t} inserted={a-b}")
ap=argparse.ArgumentParser(); ap.add_argument("--target-db",required=True); ap.add_argument("--source-db",required=True); ap.add_argument("--tables",default=",".join(DEFAULT_TABLES)); ap.add_argument("--dry-run",action="store_true"); args=ap.parse_args()
conn=sqlite3.connect(args.target_db,timeout=120); conn.execute("PRAGMA journal_mode=WAL;"); conn.execute("ATTACH DATABASE ? AS src",(args.source_db,))
try:
    if not args.dry_run: conn.execute("BEGIN IMMEDIATE")
    for t in [x.strip() for x in args.tables.split(",") if x.strip()]: merge(conn,t,args.dry_run)
    if args.dry_run: print("[DRY-RUN] done")
    else: conn.commit(); print("[COMMIT] done")
except Exception:
    if not args.dry_run: conn.rollback()
    raise
finally:
    conn.close()
EOF

# 5) 사용 예시 (아래 SOURCE_DB/TARGET_DB를 실제 경로로 변경)
echo 'python3 sqlite_merge_utility.py --target-db /var/lib/dante-quant-factory/data/market_data.sqlite --source-db /tmp/dante-restore/market_data.sqlite --dry-run'
echo 'python3 sqlite_merge_utility.py --target-db /var/lib/dante-quant-factory/data/market_data.sqlite --source-db /tmp/dante-restore/market_data.sqlite'
```

