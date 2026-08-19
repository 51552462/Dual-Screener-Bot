# I-GMM 배포 후 확인 (서버에서 한 장)

> **날짜**: 2026-08-17 · **모드**: paper 관측만  
> **전제**: 디렉터 확인 — I-GMM-DNA-01 코드는 **이미 서버 배포됨**.  
> **금지**: 실전 ON · funding(C-2) · MDD 5% 튜닝 · live 배분.

데이터 경로가 서버마다 다를 수 있음. 먼저:

```bash
echo "DATA=${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"
DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"
ls -l "$DATA"/*.sqlite
```

---

## A. 가상 장부 — OPEN / CLOSED 가 생겼는가

**기대**: 배포 전 OPEN=0 이 깨지고, 1~2일 안에 OPEN 또는 CLOSED 가 **0보다 큼**.  
(스캔 텔레그램은 그대로여도, 장부가 0이면 배선이 아직 안 먹은 것.)

```bash
sqlite3 "$DATA/bitget_market_data.sqlite" \
  "SELECT status, COUNT(*) FROM bitget_forward_trades GROUP BY status;"
```

| 결과 | 의미 |
|------|------|
| OPEN 또는 CLOSED ≥ 1 | 배선이 paper 장부에 도달함 → 관측 계속 |
| 둘 다 0 | Cos / DNA 키를 B·C에서 이어서 확인 |

---

## B. Cos_eff=0.000 고정이 깨졌는가

**기대**: 최근 로그에 `Cos_eff=0.000` **만** 반복되면 실패. `Cos_eff=` 뒤에 0이 아닌 숫자가 보이면 성공 쪽.

```bash
# 최근 Cos_eff 샘플 (journal 또는 파일 로그 중 있는 쪽)
journalctl -u 'dante-bitget*' --since "48 hours ago" | grep -E 'Cos_eff=' | tail -n 30

# 파일 로그를 쓰는 경우 (경로가 다르면 BITGET_LOG_DIR 확인)
grep -E 'Cos_eff=' "${BITGET_LOG_DIR:-/var/log/bitget}"/bitget*.log 2>/dev/null | tail -n 30
```

| 결과 | 의미 |
|------|------|
| `Cos_eff=0.000` 만 | DNA 미연결 또는 sync 미실행 → C 확인 |
| `Cos_eff=0.xxx` (0 아님) 가 보임 | 시계열 게이트가 더 이상 전원 거절이 아님 |

---

## C. DNA 키 · shape_source

**기대**: `CRYPTO_DNA_ALPHA_RANK1` ~ `RANK3` (또는 유사 키) 가 config에 **있음**.

```bash
sqlite3 "$DATA/bitget_system_config.sqlite" \
  "SELECT key, substr(value,1,80) FROM config_kv WHERE key LIKE 'CRYPTO_DNA_ALPHA%' ORDER BY key;"
```

shape_source (neutral_fallback / prototype_ohlcv 등) 는 DNA JSON 안 필드. 키 값이 JSON이면:

```bash
sqlite3 "$DATA/bitget_system_config.sqlite" \
  "SELECT key, value FROM config_kv WHERE key LIKE 'CRYPTO_DNA_ALPHA_RANK%';" \
  | grep -o 'shape_source[^,}]*' | head
```

| 결과 | 의미 |
|------|------|
| RANK 키 있음 | sync 가 한 번은 먹은 상태 |
| RANK 키 없음 · `BITGET_GMM_DNA_TEMPLATES` 만 | **C′** 강제 sync 필요 |

---

## C′. (필요할 때만) gmm_dna_alpha_sync --force

RANK 키가 **이미 있으면 다시 돌리지 말 것** (manual DNA 덮어쓰기 위험).  
키가 없을 때만:

```bash
cd ~/dante_bots/Dual-Screener-Bot
.venv/bin/python -m bitget.evolution.gmm_dna_alpha_sync --force
sqlite3 "$DATA/bitget_system_config.sqlite" \
  "SELECT key FROM config_kv WHERE key LIKE 'CRYPTO_DNA_ALPHA%';"
```

로컬/채팅으로는 “이미 --force 돌았는지” **알 수 없음**. 위 SELECT가 증거.

---

## 영구 조치 (서버 · 임시 땜빵 아님)

> 대상: 대시보드 🔴 DNA / L-2 / AI 감사관. **실전·funding·MDD5% 금지.**  
> 전제: `cd ~/dante_bots/Dual-Screener-Bot` · `DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"`

### 0) 공통 (한 번)

```bash
cd ~/dante_bots/Dual-Screener-Bot
git pull
# venv 있으면 활성화 (.venv 또는 venv)
source .venv/bin/activate 2>/dev/null || source venv/bin/activate 2>/dev/null || true
export DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"
```

### 1) DNA RANK — GMM 채운 뒤 sync 1회 (quick recover ≠ GMM)

**영구 본체:** I-GMM (주간 mine → sync).  
**주의:** `--recover-artifacts-quick` 은 KMeans/`LIVE_CLUSTER_*` 만. RANK는 `BITGET_GMM_DNA_TEMPLATES` 필요.  
키만 있고 값이 빈 `{}`이면 sync → `no_gmm_templates`.

```bash
cd ~/dante_bots/Dual-Screener-Bot
source venv/bin/activate   # 이 VPS: venv/ (점 없음)
export DATA="${BITGET_DB_STORAGE_PATH:-/var/lib/quant-bitget/data}"

# A) 진단 — 템플릿이 비 dict / 빈 dict 인지
python -c "
from bitget.config_hub import load_config
cfg=load_config()
g=cfg.get('BITGET_GMM_DNA_TEMPLATES')
print('type=', type(g).__name__)
print('empty=', (not g) if isinstance(g, dict) else 'N/A')
print('keys=', list(g.keys())[:12] if isinstance(g, dict) else repr(g)[:200])
print('updated_at=', cfg.get('BITGET_GMM_DNA_UPDATED_AT'))
"

# B) 비었으면 GMM만 채굴 (전체 data_miner/AST보다 가벼움)
python -c "from bitget.data_miner import mine_bitget_dna_templates; print(mine_bitget_dna_templates())"

# C) sync (mine가 sync 실패했어도)
python -m bitget.evolution.gmm_dna_alpha_sync --force

# D) RANK 확인
sqlite3 "$DATA/bitget_system_config.sqlite" \
  "SELECT key FROM config_kv WHERE key LIKE 'CRYPTO_DNA_ALPHA_RANK%';" 2>/dev/null \
  || sqlite3 "$DATA/bitget_system_config.sqlite" \
  "SELECT key FROM kv_store WHERE key LIKE 'CRYPTO_DNA_ALPHA_RANK%';"
```

B에서 에러(데이터 부족 등)면 **멈추고** 로그를 Cursor에. `--force`만 반복하지 말 것.  
성공 후 대시보드 DNA 🟢. 다음날 또 비면 자동 sync/주간 mine 경로 조사.

### 2) L-2 DB 백업 — systemd 타이머 설치 (재부팅 후에도 유지)

```bash
sudo INSTALL_ROOT=$PWD bash bitget/deploy/install_bitget_backup.sh
# (권장) 시험
sudo INSTALL_ROOT=$PWD bash bitget/deploy/install_bitget_backup.sh --test

# 영구로 켜졌는지
systemctl is-enabled dante-bitget-backup.timer
systemctl is-active dante-bitget-backup.timer
systemctl list-timers dante-bitget-backup.timer
```

`inactive` → `active` / `enabled` 되면 영구 조치 완료.

### 3) AI 감사관 — 상시 루프 (systemd · SSH 한 번 실행 ≠ 영구)

`daily_audit`의 일회 `run_ai_auditor`만으로는 D-2 폴링이 안 됨. **루프 프로세스**가 필요.

```bash
# .env 확인 (없으면 기동 실패 → exit=1 흔함)
grep -E '^(REPORT_BOT_TOKEN|REPORT_BOT_CHAT_ID|GEMINI_API_KEY|AI_PROPOSAL)' \
  .env bitget/.env 2>/dev/null | sed 's/=.*/=***/'
```

영구 유닛 설치 (한 번만):

```bash
sudo tee /etc/systemd/system/dante-bitget-overseer.service >/dev/null <<EOF
[Unit]
Description=Bitget AI overseer loop (daily audit + proposal poll)
After=network-online.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/dante_bots/Dual-Screener-Bot
EnvironmentFile=-/home/ubuntu/dante_bots/Dual-Screener-Bot/.env
EnvironmentFile=-/home/ubuntu/dante_bots/Dual-Screener-Bot/bitget/.env
Environment=PYTHONPATH=/home/ubuntu/dante_bots/Dual-Screener-Bot
# VPS SSOT: 이 서버는 `.venv` 가 아니라 `venv/` (점 없음). `.venv` → 203/EXEC.
ExecStart=/home/ubuntu/dante_bots/Dual-Screener-Bot/venv/bin/python -m bitget.ai_overseer
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now dante-bitget-overseer.service
systemctl status dante-bitget-overseer.service --no-pager
```

확인:

```bash
systemctl is-active dante-bitget-overseer.service
pgrep -af ai_overseer
```

**서버 확정 (2026-08-18 UTC):** `active (running)` · `enabled` · ExecStart=`.../venv/bin/python -m bitget.ai_overseer` — 재부팅 후에도 유지.

실패 시: `journalctl -u dante-bitget-overseer -n 80 --no-pager` (대개 env/키 누락 또는 잘못된 venv 경로).

### 4) 조치 후

```bash
bash bitget/deploy/bitget.sh --post-deploy-obs-digest
```

대시보드에서 DNA·L-2·감사관이 🔴에서 빠졌는지 보면 됨.

---

## 긴급 롤백 (장부가 이상할 때만)

```bash
sqlite3 "$DATA/bitget_system_config.sqlite" \
  "DELETE FROM config_kv WHERE key LIKE 'CRYPTO_DNA_ALPHA_RANK%';"
```

실전 스위치·MDD·funding 은 건드리지 말 것.

---

## 매일 자동 (텔레그램)

배포 후 cron이 **매일 20:00 KST**에 REPORT_BOT으로 요약+복붙을 보냄.

```bash
# 수동 1회 테스트
bash bitget/deploy/bitget.sh --post-deploy-obs-digest
# 전송 없이 미리보기
bash bitget/deploy/bitget.sh --post-deploy-obs-digest --dry-run
```

끄기: `POST_DEPLOY_OBS_DIGEST_ENABLED=false`

---

## 이 장에서 안 하는 것

- C-2 funding · 포트폴리오 MDD 5% · B-2 live 배분 · `ENABLE_REAL_EXECUTION`
- 로컬 PC에서 OPEN 카운트 확인 (서버 DB만 진실)
