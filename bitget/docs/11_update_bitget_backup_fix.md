# 11 — `update_bitget.sh` 백업 권한 오류 수정 보고서

> **작성일:** 2026-06-14  
> **증상:** Ubuntu 첫 배포 시 `[1/5] pre-update backup` 단계에서 `Permission denied`  
> **수정 파일:** `bitget/deploy/update_bitget.sh`  
> **관련:** [09_ubuntu_deployment_and_update_guide.md](./09_ubuntu_deployment_and_update_guide.md)

---

## 0. Executive Summary

| 항목 | 내용 |
|------|------|
| **증상** | `sqlite3.OperationalError: unable to open database file` → `PermissionError` on backup dest |
| **근본 원인** | root가 만든 타임스탬프 폴더에 **ubuntu** 사용자로 Python 백업이 쓰기 시도 |
| **해결** | `chown DEPLOY_USER:DEPLOY_USER` on leaf backup dir + 첫 배포 자동 스킵 + 수동 스킵 env |
| **chmod 777이 안 된 이유** | 부모만 777이어도 **하위 stamp 폴더는 root 소유(755)** → ubuntu 쓰기 불가 |

---

## 1. 재현 환경

```bash
sudo INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot \
  ./bitget/deploy/update_bitget.sh
```

**에러 로그 (요약):**

```
[1/5] pre-update backup
Traceback ...
sqlite3.OperationalError: unable to open database file   # backup_sqlite — dest 파일 생성 실패
...
PermissionError: [Errno 13] Permission denied:
  '/var/backups/bitget-pre-update/20260614_163026_utc/bitget_market_data.sqlite'
```

사용자가 시도했으나 해결되지 않은 조치:

- `/var/backups/bitget-pre-update` 에 `chmod 777`
- 원본 DB를 `sqlite3`로 수동 생성

---

## 2. 원인 분석

### 2.1 권한 드롭 구조 (수정 전)

```
sudo ./update_bitget.sh          ← EUID=0 (root)
    │
    ├─ mkdir -p .../20260614_163026_utc   ← 소유자 root:root, mode 755
    │
    └─ sudo -u ubuntu python -c "..."     ← EUID=ubuntu (권한 드롭)
            │
            └─ sqlite3.connect(out) / shutil.copy2(src, out)
                    └─ ubuntu가 root 소유 디렉터리에 파일 생성 → EACCES
```

**핵심:** 스크립트 전체는 `sudo`로 돌지만, 백업 Python만 **의도적으로 `DEPLOY_USER`(기본 `ubuntu`)** 로 실행한다.  
(`bitget_data_dir()`·venv·git과 동일 사용자 컨텍스트 유지)

### 2.2 왜 부모 폴더 777만으로는 부족한가

| 경로 | 생성 주체 | 일반 권한 | ubuntu 쓰기 |
|------|-----------|-----------|-------------|
| `/var/backups/bitget-pre-update` | root (mkdir) | 777 (사용자가 chmod) | ✅ 하위 폴더 **생성** 가능 |
| `.../20260614_163026_utc` | root (mkdir) | **755** (기본) | ❌ **내부 파일 생성 불가** |

Linux 디렉터리 쓰기 권한은 **해당 디렉터리의 소유자/모드**에 따른다.  
부모가 777이어도, **이미 root가 만든 자식 폴더 안**에는 ubuntu가 쓸 수 없다.

### 2.3 에러가 두 단계로 보인 이유

1. `sqlite3.connect(out)` — 백업 **대상 파일**을 dest에 만들지 못함 → `unable to open database file`
2. `except` 폴백 `shutil.copy2` — 동일 경로에 쓰기 → `Permission denied`

(수정 전 코드는 `except Exception:` 으로 삼켜 첫 에러 원인이 불명확했음)

### 2.4 추가 가능 원인 (DB를 root로 만든 경우)

`BITGET_DB_STORAGE_PATH` 아래 sqlite를 **root**로 생성하면, 백업 **소스 읽기**도 실패할 수 있다.  
이번 케이스는 **dest 쓰기**가 먼저 실패한 패턴이다.

```bash
sudo chown -R ubuntu:ubuntu /var/lib/bitget-factory   # data root 예시
```

---

## 3. 수정 내용 (`update_bitget.sh`)

### 3.1 변경 요약

| # | 변경 |
|---|------|
| 1 | `mkdir` 후 **`chown "${DEPLOY_USER}:${DEPLOY_USER}" "$dest"`** |
| 2 | **`BITGET_SKIP_PREUPDATE_BACKUP=1`** 이면 [1/5] 전체 스킵 |
| 3 | data dir에 bitget sqlite **0개** → 첫 배포로 간주, 백업 스킵 (exit 0) |
| 4 | dest **`os.access(..., W_OK)`** 사전 검사 |
| 5 | 소스 DB **`file:...?mode=ro`** URI 로 읽기 전용 연결 |
| 6 | 백업 실패 시 명확한 stderr 메시지 + bash `return 1` |

### 3.2 수정된 bash 핵심 (발췌)

```bash
mkdir -p "$backup_root"
mkdir -p "$dest"
chown "${DEPLOY_USER}:${DEPLOY_USER}" "$dest"

sudo -E -u "$DEPLOY_USER" env \
  INSTALL_ROOT="$INSTALL_ROOT" \
  PYTHONPATH="$INSTALL_ROOT" \
  _BG_BACKUP_DEST="$dest" \
  "$DANTE_PY" -c "..."
```

### 3.3 Python 백업 로직 (발췌)

```python
if not os.access(dest, os.W_OK):
    sys.exit(1)

present = [n for n in db_names if os.path.isfile(os.path.join(data, n))]
if not present:
    print('  no bitget sqlite in ... — first deploy, backup skipped')
    sys.exit(0)

s = sqlite3.connect(f'file:{src}?mode=ro', uri=True, timeout=60)
d = sqlite3.connect(out, timeout=60)
s.backup(d)
```

---

## 4. 서버 적용 방법

### 4.1 수정본 반영

```bash
cd /home/ubuntu/dante_bots/Dual-Screener-Bot
git pull   # 본 수정이 포함된 커밋 이후

sudo INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot \
  ./bitget/deploy/update_bitget.sh
```

### 4.2 첫 배포 — 백업 명시적 스킵

DB가 아직 없거나 백업 단계를 건너뛰려면:

```bash
sudo BITGET_SKIP_PREUPDATE_BACKUP=1 \
  INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot \
  ./bitget/deploy/update_bitget.sh
```

### 4.3 data 디렉터리 소유권 (권장 1회)

```bash
# .env의 BITGET_DB_STORAGE_PATH 확인
sudo -u ubuntu bash -lc '
  cd /home/ubuntu/dante_bots/Dual-Screener-Bot
  source venv/bin/activate
  python -c "from bitget.infra.data_paths import bitget_data_dir; print(bitget_data_dir())"
'

# 출력 경로 예: /var/lib/bitget-factory/data
sudo mkdir -p /var/lib/bitget-factory/data /var/lib/bitget-factory/logs
sudo chown -R ubuntu:ubuntu /var/lib/bitget-factory
```

### 4.4 백업 경로 사전 준비 (선택)

```bash
sudo mkdir -p /var/backups/bitget-pre-update
# leaf 폴더는 매 실행 시 root가 만들고 chown 하므로 부모만 있으면 충분
```

---

## 5. 정상 동작 시 기대 출력

### 첫 배포 (DB 없음)

```
[1/5] pre-update backup
  no bitget sqlite in /var/lib/bitget-factory/data — first deploy, backup skipped
[update_bitget] backup -> /var/backups/bitget-pre-update/20260614_xxxxxx_utc
[2/5] git pull (ubuntu)
...
```

### 운영 중 업데이트 (DB 있음)

```
[1/5] pre-update backup
  sqlite: bitget_market_data.sqlite
  sqlite: bitget_system_config.sqlite
  ...
  data_dir=/var/lib/bitget-factory/data
[update_bitget] backup -> /var/backups/bitget-pre-update/20260614_xxxxxx_utc
```

---

## 6. 트러블슈팅

| 증상 | 확인 |
|------|------|
| 여전히 Permission denied | `ls -la /var/backups/bitget-pre-update/<stamp>/` — 소유자가 ubuntu인지 |
| unable to open database (src) | 원본 sqlite 소유자 `ls -la $BITGET_DB_STORAGE_PATH/*.sqlite` → `chown ubuntu` |
| backup skipped만 나오고 진행 안 됨 | 이후 [2/5]~[5/5] 로그 확인; `set -e` 로 다른 단계 실패 가능 |
| venv not found | `${INSTALL_ROOT}/venv` 존재 여부 |

**백업 폴더 권한 확인:**

```bash
STAMP=20260614_163026_utc   # 실제 stamp로 교체
ls -la /var/backups/bitget-pre-update/
ls -la /var/backups/bitget-pre-update/$STAMP/
# 기대: drwxr-xr-x ubuntu ubuntu ... $STAMP
```

---

## 7. `update_bitget.sh` 전체 단계 (참고)

| 단계 | 내용 | 주식 영향 |
|------|------|-----------|
| [1/5] | pre-update backup (본 문서) | 없음 |
| [2/5] | `git pull --ff-only` (ubuntu) | 없음 |
| [3/5] | `deploy_bitget_factory.sh` | 없음 |
| [4/5] | `dante-bitget-*` graceful stop | 없음 |
| [5/5] | `dante-bitget-*` restart + timer | 없음 |

---

## 8. 변경 파일

| 파일 | 변경 |
|------|------|
| `bitget/deploy/update_bitget.sh` | 백업 권한·첫 배포 스킵·에러 처리 |
| `bitget/docs/11_update_bitget_backup_fix.md` | 본 문서 |
| `bitget/docs/README.md` | 인덱스 갱신 |

---

## 9. 요약

- **원인:** root가 만든 backup leaf dir + ubuntu로 실행되는 Python = **쓰기 권한 불일치**
- **해결:** `chown ubuntu:ubuntu` on `$dest` + 첫 배포/수동 스킵
- **chmod 777** 은 하위 stamp 디렉터리 문제를 해결하지 **못함**
