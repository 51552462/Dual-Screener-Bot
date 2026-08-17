# NEXT_ACTION — Bitget

| 필드 | 값 |
|------|-----|
| **sub-phase** | **POST_DEPLOY_OBS** (+ 일일 텔레그램 다이제스트) |
| **status** | **OBS_RUNNING** — 코드 Handoff 없음 · 매일 REPORT_BOT 수신 |
| **코드** | `post_deploy_obs_digest_bg` 배포 후 cron 활성 필요 |

---

## 디렉터 (지금)

1. **서버 배포** (01b + digest): `git pull` → `update_bitget.sh` (cron에 `--post-deploy-obs-digest` 포함 확인)
2. **매일 20:00 KST** REPORT_BOT으로 관측 요약 + Cursor/Claude 복붙 수신
3. 수동 1회: `bash bitget/deploy/bitget.sh --post-deploy-obs-digest` (또는 `--dry-run`)
4. 이상 메시지면 텔레그램의 **Cursor/Claude 복붙**만 붙여넣기

**관측 5항**: OPEN/CLOSED · Cos_eff · DNA RANK · 01b 주간행 · L-1/L-2/overseer

**금지**: C-2 · MDD 5% · B-2 live · `ENABLE_REAL_EXECUTION`

끄기: `POST_DEPLOY_OBS_DIGEST_ENABLED=false`
