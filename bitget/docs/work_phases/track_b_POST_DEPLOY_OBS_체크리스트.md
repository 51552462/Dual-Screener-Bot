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
