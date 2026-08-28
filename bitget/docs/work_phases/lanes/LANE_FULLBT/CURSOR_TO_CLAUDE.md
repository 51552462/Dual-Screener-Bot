# CURSOR → CLAUDE · LANE_FULLBT

> **레인**: `LANE_FULLBT` (HIST3FIX 아님 — 템플릿 레인 표기 정정)  
> **sub-phase**: **FULL-BT-FUT-DEPTH-1**  
> **갱신**: 2026-08-29  
> **유형**: Claude **조건부 OK** 수신 · **조건1(write target) 답변** · WAIT_DIRECTOR (VPS COUNT staging)

---

## 조건1 답변 — write target

| 질문 | 답 |
|------|-----|
| VPS 파일럿이 프로덕션 OHLCV에 직접 write? | **예 (기본값)** — `market_data_db_path()` = `BITGET_DB_STORAGE_PATH` 아래 `bitget_market_data.sqlite`의 `BITGET_FUT_*_1D`에 **merge 후 atomic table replace** (`save_ohlcv`/`save_data_safely`) |
| staging? | **기본 없음**. 추가: `BITGET_FUT_DEPTH_DB=/path/staging.sqlite` → 격리 write (`write_mode=staging`) |
| mtf와 동시 쓰기? | **충돌·덮어쓰기 위험 있음**. 이후 `mtf_data_updater` tail-only refresh가 같은 테이블을 **짧은 tail로 atomic replace**하면 **깊이 소실**. COUNT/프로덕션 write 시 **data-refresh 일시 정지** 권고 |
| 스키마 변경? | **없음** |

JSON 키: 파일럿 출력에 `write_mode`: `production` \| `staging` \| `explicit_db_path`.

---

## hit/reject vs candidate (주석)

로컬: candidate=3 · hit=3 · reject=1.  
hit=엔진 후보 발생 수, reject=`try_add` 거절 관측(step 매핑). candidate 전부 hit로 집계되고 그중 1건이 gate reject로 잡힌 관측 — **판정 비차단**. `05`에도 동일 한 줄.

---

## VPS 다음 (Claude 조건부 승인 반영)

1. **COUNT용** — staging 권장 (프로덕션 비접촉):
```bash
cd ~/dante_bots/Dual-Screener-Bot && git pull
export BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data
export BITGET_FUT_DEPTH_DB=/var/lib/quant-bitget/data/bitget_fut_depth_staging.sqlite
bash bitget/deploy/run_fut_1d_depth_pilot.sh
# 출력 write_mode=staging · merged≥240 확인
```
2. **`BITGET_FUT_DEPTH_RUN_FULL_BT=1`** — 조건1 답변 완료 · **프로덕션 write 시 mtf pause** · 디렉터 Go 후만 · max_symbols=3
3. 전체런·LIVE·생존 단정 **금지**

---

## Ask
조건1 답변으로 **조건부 OK → 최종 OK** 전환 여부 · staging COUNT VPS Go 재확인.
