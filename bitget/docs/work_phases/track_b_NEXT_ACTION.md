# NEXT_ACTION — Bitget

| 필드 | 값 |
|------|-----|
| **sub-phase** | **I-GMM-DNA-01** |
| **status** | Claude 조건부 OK ✅ · R1/R2 반영 · **git push + 서버 배포** |

---

## 디렉터 (지금)

1. **git commit + push** (Cursor 또는 디렉터 지시)
2. **서버 배포**
   ```bash
   cd ~/dante_bots/Dual-Screener-Bot && git pull
   sudo INSTALL_ROOT=$PWD bash bitget/deploy/update_bitget.sh
   .venv/bin/python -m bitget.evolution.gmm_dna_alpha_sync --force
   ```
3. **24~48h 확인**
   - 로그: `Cos_eff=0.000` 고정 사라졌는지 · `sn_score` 분포
   - DB: `SELECT status, COUNT(*) FROM bitget_forward_trades GROUP BY status;`
   - config: `CRYPTO_DNA_ALPHA_RANK*` · `shape_source` 필드

4. **2~4주** — `06_검증체크리스트` paper 관측 (fill-rate·MFE 이상 시 롤백: ALPHA_RANK 키 삭제)

---

## 롤백 (긴급)

```bash
sqlite3 /var/lib/quant-bitget/data/bitget_system_config.sqlite \
  "DELETE FROM config_kv WHERE key LIKE 'CRYPTO_DNA_ALPHA_RANK%';"
```

---

## 다음 Handoff (live 전환 전, CAT-F)

- `ENABLE_REAL_EXECUTION=true` 시 sn_score 폴백 fail-closed 재확인 (규칙11)
- neutral shape 도플갱어 ±10/-30 제외 검토

---

## 병렬 (무관)

- DEPLOY_WATCH `dante-factory` = **주식 트랙** (코인 VPS면 cron 정리)
- D-3a cost_report 관측
