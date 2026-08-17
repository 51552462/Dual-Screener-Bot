# NEXT_ACTION — Bitget

| 필드 | 값 |
|------|-----|
| **sub-phase** | **I-GMM-DNA-01b** |
| **status** | **DONE** (Claude OK **2026-08-17**) · 효과 검증(`06`)은 서버 1~2주 |
| **코드** | 신규 Handoff **없음** |

---

## 디렉터 (지금) — 서버 1~2주 관측 대기

설계·코딩 없음. 아래만 보면 됨.

1. **POST_DEPLOY_OBS** — OPEN/CLOSED · Cos_eff · `CRYPTO_DNA_ALPHA_RANK*` (`track_b_POST_DEPLOY_OBS_체크리스트.md`)
2. **01b** — 주간 `ops_events` `gmm_dna_alpha_report_weekly` 행이 쌓이는지
3. **병행** — L-1 logrotate · L-2 backup · ai_overseer + REPORT_BOT 기동 여부 (아직 미확인이면 확인)
4. **조건부** — 2주 연속 `cos_eff_sample_count=0` + `log_source_used=unavailable` 이면 **서버 로그 경로**만 조사 (코드 재배선 아님)

**금지**: C-2 funding · MDD 5% · B-2 live · `ENABLE_REAL_EXECUTION`

다음 Handoff는 필요 시 **파일**(`CLAUDE_TO_CURSOR`)에만. 지금 후보 없음.

---

## 롤백 (01b)

```text
GMM_DNA_ALPHA_REPORT_ENABLED=false
```
