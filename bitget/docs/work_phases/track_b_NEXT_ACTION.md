# NEXT_ACTION — Bitget

| 필드 | 값 |
|------|-----|
| **sub-phase** | **POST_DEPLOY_OBS-DNA-UX-01** |
| **status** | **SUB_DONE (Claude OK)** · 서버 배포·텔레그램 육안만 |
| **코드** | DNA why-진단 구현 완료 · 신규 Handoff 없음 |

---

## 디렉터 (지금)

1. 서버: `git pull` → (필요 시) `update_bitget.sh` → `bash bitget/deploy/bitget.sh --post-deploy-obs-digest`
2. 텔레그램 DNA 칸이 **「재료가 아직 덜 모였어요」**(🟡)인지 확인
3. 숫자 메모에 `state=DATA_WAIT_LOW_MFE` · `n_mfe8_by_tf=...` 보이는지 확인
4. 이후 평소 관측 유지

**후속(착수 금지·메모만):** DATA_WAIT 연속일수 카운터 · 01b/digest 계산 통합 — Mirror #1·#2  
**금지:** C-2 · MDD 5% · B-2 live · `ENABLE_REAL_EXECUTION`

끄기: `POST_DEPLOY_OBS_DIGEST_ENABLED=false` / DNA만 `POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED=false`
