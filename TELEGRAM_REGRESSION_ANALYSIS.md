# 텔레그램 다중 봇 키 회귀(Regression) 분석 및 100% 복구 가이드

**증상:** Bitget 코인 팩토리 세팅 직후, 어제까지 정상이던 주식 스캔·리포트 텔레그램이 **전면 먹통**.  
**의심:** `EQUITY_KR_MAIN_BOT_TOKEN`, `FACTORY_CHAT_ID` 등 다중 키 미인식 → 구형 `TELEGRAM_BOT_TOKEN` 단일 키만 찾는 버전으로 퇴행.

---

## Executive Summary

| 질문 | 답 |
|------|-----|
| 코드가 구형 단일 키로 **덮어씌워졌나?** | **아니오.** `telegram_env.py`·`async_telegram_daemon.py`는 다중 키 체인이 **그대로 유지**됨. |
| 실제 원인 후보 | ① **`.env` 편집 시 EQUITY_* 키 유실** (Bitget 가이드의 `TELEGRAM_BOT_TOKEN`만 남김) ② **`dante-async` 미기동·exit 2** ③ **`DB_STORAGE_PATH` 변경으로 큐 DB 경로 불일치** ④ Bitget async가 **주식 토큰으로 부트스트랩**하던 설계 결함(코인 전용 분리 완료) |
| 큐 DB 충돌? | **파일 분리 설계** — 주식 `message_queue.sqlite` vs 코인 `bitget_message_queue.sqlite`. **같은 data dir만 쓰면 디렉터리 공유 위험**은 있으나 파일명은 다름. |
| 복구 핵심 | `.env` 다중 키 복원 + `sudo systemctl restart dante-async` + 큐·로그 검증 |

---

## 1. 로직 퇴행 분석 (코드 팩트)

### 1.1 다중 봇 SSOT — `telegram_env.py` (정상 유지)

주식 스캐너·데몬이 읽는 키 체인 (우선순위 순):

| 용도 | 토큰 체인 | 채팅 ID 체인 |
|------|-----------|--------------|
| **KR 스캔** | `EQUITY_KR_MAIN_BOT_TOKEN` → `KR_MAIN_BOT_TOKEN` → `MAIN_BOT_TOKEN` → `TELEGRAM_TOKEN_MAIN` | `EQUITY_KR_FACTORY_CHAT_ID` → `KR_FACTORY_CHAT_ID` → `FACTORY_CHAT_ID` → `TELEGRAM_CHAT_ID` |
| **US 스캔** | `EQUITY_US_MAIN_BOT_TOKEN` → … (동일 패턴) | `EQUITY_US_FACTORY_CHAT_ID` → … |
| **일일 리포트** (`forward/shared.py`, `supernova_hunter.py`) | `REPORT_BOT_TOKEN` → `TELEGRAM_TOKEN_MAIN` → `TELEGRAM_TOKEN` → `get_main_token()` | `REPORT_BOT_CHAT_ID` → `TELEGRAM_CHAT_ID` → `get_factory_chat_id()` |
| **async 데몬 부트스트랩** (`async_telegram_daemon.py`) | `MAIN_BOT_TOKEN` → `EQUITY_KR_*` → `EQUITY_US_*` | `FACTORY_CHAT_ID` → `EQUITY_KR_*` → `EQUITY_US_*` |

**중요:** `TELEGRAM_BOT_TOKEN`은 원래 **`get_lab_token()`(실험실 봇) 전용**이었고, 리포트·스캔 큐 데몬 체인에는 **없었음**.

→ Bitget 배포 가이드 일부가 주식에 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` 만 적도록 안내한 경우, **기존 `EQUITY_*` / `MAIN_BOT_TOKEN` / `FACTORY_CHAT_ID`를 지우면 즉시 먹통**이 됨. 이는 **코드 퇴행이 아니라 문서·운영 불일치**다.

**패치 반영 (2026-06):** 하위 호환을 위해 `get_main_token()`·`get_report_token()`에 `TELEGRAM_BOT_TOKEN` 별칭을 **추가**함 (`telegram_env.py`).

### 1.2 발송 경로 2종 (혼동 주의)

```
┌─────────────────────────────────────────────────────────────┐
│  A. 스캔 알림 (kr.py, ema5, usa, …)                          │
│     enqueue_telegram("MAIN"|"PROMO") → message_queue.sqlite │
│     소비: dante-async.service (async_telegram_daemon.py)    │
└─────────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────────┐
│  B. 일일 리포트·딥다이브 (forward/shared send_telegram_msg)   │
│     HTTP 직접 전송 — get_report_token() + get_report_chat_id()│
│     dante-async 불필요 (단, 스캔과 토큰 체인이 다를 수 있음)   │
└─────────────────────────────────────────────────────────────┘
```

**어제 정상이었다면** 최소한:
- 스캔: `dante-async` **active** + `EQUITY_*` 또는 `MAIN_BOT_TOKEN` + `FACTORY_CHAT_ID`
- 리포트: `REPORT_BOT_TOKEN` 또는 `MAIN_BOT_TOKEN` 또는 `TELEGRAM_TOKEN_MAIN` + `FACTORY_CHAT_ID` 등

### 1.3 `dante-async` 부트스트랩 실패 시 (exit 2)

`async_telegram_daemon.main()` — 토큰·채팅이 하나도 없으면 **프로세스 종료 코드 2**:

```475:519:async_telegram_daemon.py
    token_main = (
        telegram_env.get_main_token()
        or telegram_env.get_equity_kr_main_token()
        or telegram_env.get_equity_us_main_token()
    )
    ...
    if not send_enabled:
        return None
...
        sys.exit(2)
```

**journalctl에 아래가 보이면 .env 문제 확정:**

```text
⚠️ [async_telegram_daemon] 큐 데몬 등록 없음 — .env 에 아래 키 중 하나 이상...
```

### 1.4 Bitget 병합 시 코인 async 결함 (주식 간섭 가능성)

**이전:** `bitget/async_telegram_daemon.py`가 큐 DB만 Bitget으로 패치한 뒤, **주식과 동일한** `async_telegram_daemon.main()`을 호출 → **EQUITY/MAIN 토큰으로 Bitget 큐를 소비**하려 시도.

**수정 후:** Bitget async는 `BITGET_TELEGRAM_*` / `BITGET_BOT_*` 만으로 부트스트랩하고, `run_async_telegram_daemon()`만 호출. **주식 `dante-async`와 자격 증명 경로 완전 분리.**

---

## 2. Ubuntu 서버 내부 충돌 검사

### 2.1 `update_factory.sh`가 텔레그램을 망가뜨릴 수 있는 지점

| 단계 | 동작 | 텔레그램 영향 |
|------|------|----------------|
| [4/7] | `systemctl stop dante-async` | 큐 소비 **중단** (enqueue만 쌓임) |
| [6/7] | `systemctl restart dante-async` | `.env` 재로드 후 부트스트랩 — **키 없으면 failed** |
| `chmod 600 .env` | `deploy_quant_factory.sh` | ubuntu 소유면 읽기 가능; **소유자 root·권한 오류** 시 빈 env |
| `git pull` | 코드 갱신 | 현재 repo 기준 **다중 키 로직 유지** |

`update_bitget.sh`는 **`dante-async`를 stop하지 않음** — 코인 업데이트만으로 주식 async가 직접 stop되지는 않음.  
단, **동시에 `update_factory.sh` 실행** 시 주식 async 재시작 중 `.env` 오류면 먹통.

### 2.2 큐 DB 경로 — `DB_STORAGE_PATH` 변경이 치명적

`telegram_message_queue.py` 모듈 로드 시:

```python
_BOT_DIR = factory_data_dir()  # DB_STORAGE_PATH
MESSAGE_QUEUE_DB_PATH = .../message_queue.sqlite
```

**시나리오:** Bitget 세팅 중 `DB_STORAGE_PATH`를 `/var/lib/bitget-factory/data` 등으로 **잘못 변경**  
→ 스캐너는 새 경로에 enqueue, `dante-async`는 재시작 전 옛 경로를 보거나, 반대로 빈 큐만 폴링 → **발송 0건**.

### 2.3 큐 파일 분리 (정상 설계)

| 스택 | 큐 파일 | 데몬 |
|------|---------|------|
| 주식 | `{DB_STORAGE_PATH}/message_queue.sqlite` | `dante-async.service` |
| 코인 | `{BITGET_DB_STORAGE_PATH}/bitget_message_queue.sqlite` | `dante-bitget-async.service` |

**절대 금지:** `DB_STORAGE_PATH` = `BITGET_DB_STORAGE_PATH` (같은 폴더).

### 2.4 `.env` / `bitget/.env` 로드 차이

| 서비스 | EnvironmentFile |
|--------|-----------------|
| `dante-async` | **루트 `.env` 만** |
| `dante-bitget-async` | 루트 `.env` + `bitget/.env` |

→ 주식 키는 **루트 `.env`에만** 있어야 함.  
→ `bitget/.env`에 `MAIN_BOT_TOKEN=` 빈 값으로 넣어도 **주식 async에는 영향 없음**.  
→ **루트 `.env`를 Bitget 가이드로 덮어쓴 경우** 주식 전멸.

---

## 3. 100% 복구 절차 (Ubuntu 복붙)

### 3-A. 1분 진단

```bash
export INSTALL_ROOT=/home/ubuntu/dante_bots/Dual-Screener-Bot   # 본인 경로
cd "$INSTALL_ROOT"

echo "=== [1] dante-async 상태 ==="
systemctl is-active dante-async
sudo journalctl -u dante-async -n 30 --no-pager

echo "=== [2] .env 키 존재 여부 (값은 출력 안 함) ==="
for k in EQUITY_KR_MAIN_BOT_TOKEN EQUITY_US_MAIN_BOT_TOKEN MAIN_BOT_TOKEN \
         FACTORY_CHAT_ID EQUITY_KR_FACTORY_CHAT_ID REPORT_BOT_TOKEN \
         TELEGRAM_TOKEN_MAIN TELEGRAM_BOT_TOKEN DB_STORAGE_PATH BITGET_DB_STORAGE_PATH; do
  if grep -q "^${k}=" .env 2>/dev/null; then echo "  OK $k"; else echo "  MISSING $k"; fi
done

echo "=== [3] telegram_env 해석 (venv) ==="
source venv/bin/activate
set -a && source .env && set +a
python - <<'PY'
import telegram_env as t
checks = [
    ("KR scan token", t.get_equity_kr_main_token()),
    ("US scan token", t.get_equity_us_main_token()),
    ("main token", t.get_main_token()),
    ("report token", t.get_report_token()),
    ("factory chat", t.get_factory_chat_id()),
    ("report chat", t.get_report_chat_id()),
]
for label, v in checks:
    print(f"  {label}: {'SET' if v else 'EMPTY'}")
PY

echo "=== [4] 큐 DB 경로·적체 ==="
python - <<'PY'
import os, sqlite3
from factory_data_paths import factory_data_dir
from telegram_message_queue import MESSAGE_QUEUE_DB_PATH
print("  queue path:", MESSAGE_QUEUE_DB_PATH)
print("  exists:", os.path.isfile(MESSAGE_QUEUE_DB_PATH))
if os.path.isfile(MESSAGE_QUEUE_DB_PATH):
    c = sqlite3.connect(MESSAGE_QUEUE_DB_PATH)
    n = c.execute("SELECT COUNT(*) FROM msg_queue WHERE status='PENDING'").fetchone()[0]
    c.close()
    print("  PENDING rows:", n)
PY
```

**판정**

| 관측 | 의미 |
|------|------|
| `dante-async` **failed** / journal `exit 2` | `.env` 키 부족 → §3-B |
| 키 `EMPTY` 다수 | `.env` 복원 → §3-B |
| PENDING 수백+ & async inactive | 데몬만 재기동 → §3-C |
| queue path가 예상과 다름 | `DB_STORAGE_PATH` 수정 후 재시작 → §3-D |

### 3-B. `.env` 다중 키 복원 (어제 설정 예시)

```bash
cd "$INSTALL_ROOT"
cp .env .env.bak.$(date +%Y%m%d_%H%M%S)
nano .env
```

**최소 권장 세트 (본인 어제 값으로 채울 것):**

```bash
# --- 주식 데이터 (Bitget과 다른 경로) ---
DB_STORAGE_PATH=/var/lib/quant-factory/data

# --- 스캔용 다중 봇 (KR / US) ---
EQUITY_KR_MAIN_BOT_TOKEN=...
EQUITY_KR_FACTORY_CHAT_ID=...    # 또는 공통 FACTORY_CHAT_ID
EQUITY_US_MAIN_BOT_TOKEN=...
EQUITY_US_FACTORY_CHAT_ID=...

# --- 공통 (리포트·데몬 부트스트랩) ---
MAIN_BOT_TOKEN=...               # 또는 REPORT_BOT_TOKEN
FACTORY_CHAT_ID=...
REPORT_BOT_TOKEN=...             # 리포트 전용 봇이 있으면

# --- Bitget (주식 키와 별도 접두사) ---
BITGET_DB_STORAGE_PATH=/var/lib/bitget-factory/data
BITGET_TELEGRAM_TOKEN=...
BITGET_TELEGRAM_CHAT_ID=...
```

**하지 말 것**

- 주식 `EQUITY_*` / `MAIN_BOT_TOKEN` / `FACTORY_CHAT_ID` 삭제 후 `TELEGRAM_BOT_TOKEN`만 남기기 (가이드 오해)
- `DB_STORAGE_PATH`를 Bitget 경로와 동일하게 설정

```bash
chmod 600 .env
chown ubuntu:ubuntu .env
```

### 3-C. 주식 텔레그램 데몬만 재기동 (코인 분리)

```bash
cd "$INSTALL_ROOT"
# 주식 큐 락만 (코인 .bitget_runtime.lock 과 무관)
rm -f .factory_runtime.lock 2>/dev/null || true

sudo systemctl restart dante-async
sleep 2
systemctl is-active dante-async
sudo journalctl -u dante-async -n 20 --no-pager

# 코인 async는 별도 — 주식 복구 시 필수 아님
# sudo systemctl restart dante-bitget-async
```

성공 시 journal에 **에러 없이** 프로세스 유지, PENDING 건수가 줄어듦.

### 3-D. `DB_STORAGE_PATH` 되돌리기

```bash
# 백업에서 이전 경로 확인
ls -la /var/backups/dante-pre-update/*/market_data.sqlite 2>/dev/null | tail -3
grep DB_STORAGE_PATH .env.bak.*

# 수정 후
sudo systemctl restart dante-async dante-factory
```

### 3-E. 스모크 테스트

```bash
cd "$INSTALL_ROOT"
source venv/bin/activate
set -a && source .env && set +a

# 큐 경유 테스트 (스캔과 동일 경로)
python - <<'PY'
from telegram_message_queue import enqueue_telegram, start_telegram_queue_daemons
import telegram_env, os
os.environ["DANTE_ASYNC_TELEGRAM_DAEMON"] = "1"
tm = telegram_env.get_main_token() or telegram_env.get_equity_kr_main_token()
tp = telegram_env.get_promo_token() or tm
cid = telegram_env.get_factory_chat_id() or telegram_env.get_equity_kr_factory_chat_id()
start_telegram_queue_daemons(tm, tp, cid, bool(tm and cid))
enqueue_telegram("MAIN", None, "🧪 [복구 테스트] 주식 큐 enqueue — dante-async가 소비해야 함.")
print("enqueued — 30초 내 텔레그램 수신 확인")
PY
```

리포트 경로 직접 테스트:

```bash
python -c "
from forward.shared import send_telegram_msg
send_telegram_msg('🧪 [복구 테스트] 리포트 직접 HTTP 경로')
"
```

### 3-F. 코인과 연결 고리 끊기 (재발 방지)

```bash
# 1) 업데이트 스크립트 분리
#    주식: sudo ./update_factory.sh
#    코인: sudo ./bitget/deploy/update_bitget.sh

# 2) 데이터 경로 분리 확인
grep -E '^DB_STORAGE_PATH|^BITGET_DB_STORAGE_PATH' .env bitget/.env 2>/dev/null

# 3) Bitget async는 BITGET_* 키만 사용 (코드 패치 반영 후 재배포)
sudo systemctl restart dante-bitget-async

# 4) 주식 async는 dante-async 만
systemctl is-active dante-async dante-bitget-async
```

---

## 4. 코드 변경 요약 (이번 워크스페이스)

| 파일 | 변경 | 목적 |
|------|------|------|
| `telegram_env.py` | `TELEGRAM_BOT_TOKEN` → `get_main_token()` / `get_report_token()` 폴백 추가 | Bitget 가이드·레거시 키 하위 호환 |
| `bitget/async_telegram_daemon.py` | Bitget 전용 부트스트랩 (`BITGET_*`만) | 주식 EQUITY/MAIN 키와 **완전 분리** |

서버 반영:

```bash
cd "$INSTALL_ROOT"
sudo ./update_factory.sh                    # 주식
sudo ./bitget/deploy/update_bitget.sh       # 코인 (선택)
sudo systemctl restart dante-async dante-bitget-async
```

---

## 5. 체크리스트 — “어제처럼 100%”

- [ ] `systemctl is-active dante-async` → **active**
- [ ] `python` 해석에서 KR/US/report token, factory chat → **SET**
- [ ] `message_queue.sqlite` 경로 = `DB_STORAGE_PATH` 아래
- [ ] PENDING 적체가 재시작 후 감소
- [ ] 스캔·리포트 스모크 메시지 수신
- [ ] `BITGET_DB_STORAGE_PATH` ≠ `DB_STORAGE_PATH`
- [ ] Bitget async journal에 equity 키 관련 오류 없음

---

## 6. 관련 문서

| 파일 | 내용 |
|------|------|
| `STOCK_COIN_CONFLICT_RESOLUTION.md` | 주식·코인 서버 충돌 전반 |
| `bitget/docs/ubuntu_isolated_deploy_guide.md` | 코인 격리 (§6.3 주의) |
| `FACTORY_FULL_OPS_MANUAL.md` | 주식 운영·건강 검진 |

---

## 7. 한 줄 결론

**다중 봇 로직은 코드에서 퇴행하지 않았다.** Bitget 세팅 직후 먹통은 대부분 **루트 `.env`에서 `EQUITY_*` / `MAIN_BOT_TOKEN` / `FACTORY_CHAT_ID` 유실**, **`dante-async` failed**, 또는 **`DB_STORAGE_PATH` 변경**이다. `.env` 복원 + `dante-async` 재기동 + (패치 후) Bitget async를 `BITGET_*` 전용으로 분리하면 어제와 동일한 다중 봇 발송이 복구된다.
