# CLAUDE → CURSOR · POST_DEPLOY_OBS-DNA-UX-01

> **작성**: Claude Pro (Architect) · 2026-08-20
> **요청 출처**: `CURSOR_TO_CLAUDE.md` OUTBOX 2026-08-20 Ask (DNA 일일진단 미니 Handoff)
> **디렉터 승인**: DNA 「제대로 된 진단」 UX — OK. 정책(문턱완화)은 범위 밖.
> **CAT**: CAT-J (리포팅/관측, read-only) · 🟢 Low — gate/live 비접촉
> **구현**: Cursor only. 설계 변경 필요 시 CLAUDE_TO_CURSOR 상단 `## Claude 수정 spec`로.

---

## 목적

일일 텔레그램 DNA 칸이 RANK1~3 유무만 보고 매일 같은 🔴 문구를 반복한다.
2026-08-19 실측처럼 "재료가 아직 안 모였을 뿐"인 정상 대기 상태와 실제로 손봐야 하는
상태를 구분하지 못한다. → 상태 enum + why 한 줄 + 담당 분기(`cursor_action`)를
digest에 추가한다. **업로드 고장이 아니라 진단력 부족이 원인.**

## 배경 실측 (2026-08-19 VPS, `BITGET_DB_STORAGE_PATH=/var/lib/quant-bitget/data`)

- CLOSED=10 (1H=2, 2H=1, 4H=7) · n_mfe8=0, n_mfe5=0 전 TF · max_mfe≈3.55
- `mine_bitget_dna_templates` → 0 templates · `gmm_dna_alpha_sync --force` → `no_rankable_clusters`
- 코드 조건: TF당 `mfe≥BITGET_MIN_MFE_FOR_MINING`(기본 8) CLOSED가 feature dropna 후 **≥12행**이어야 GMM fit
- overseer active(running) · L-2 timer active (본 Handoff 범위 밖)

## 수정 범위 (엄수)

| 허용 | 금지 |
|---|---|
| `bitget/observability/post_deploy_obs_digest_bg.py` | `forward/gates.py` |
| `bitget/observability/gmm_dna_alpha_report_bg.py` — 읽기전용 헬퍼 추가만 | `evolution/gmm_dna_alpha_sync.py` 본체 로직 |
| 신규 테스트 파일 | `execution_safety.py` |
| | `BITGET_MIN_MFE_FOR_MINING` 기본값 변경 |
| | C-2 · MDD5% · B-2 live · `ENABLE_REAL_EXECUTION` |

---

## Spec 1 — 상태 enum + 판정표

판정은 **위에서부터 순서대로, 처음 true인 조건에서 멈춤** (동시 조건 방지 — Cursor 재량으로 순서 변경 금지, 변경 시 Claude 재검증).

| # | state | 조건 | 아이콘 | `cursor_action` |
|---|---|---|---|---|
| 1 | `DB_PATH_OR_ENV` | `config_hub.load_config()` 실패 또는 `BITGET_DB_STORAGE_PATH` sqlite open 실패 | 🔴 | `DIRECTOR_SSH_CHECK` |
| 2 | `RANK_OK` | `CRYPTO_DNA_ALPHA_RANK1~3` config_kv에 전부 존재·non-null | 🟢 | `NONE` |
| 3 | `DATA_WAIT_LOW_MFE` | (1,2 아님) AND 모든 TF에서 `n_mfe8_by_tf[TF] < gmm_min_rows`(코드 조건 12) | 🟡 | `OBSERVE_HOLD` |
| 4 | `GMM_EMPTY` | (1,2 아님) AND 어느 TF든 `n_mfe8_by_tf[TF] ≥ gmm_min_rows` AND `BITGET_GMM_DNA_TEMPLATES` 비어있음/dict 아님 | 🔴 | `DIRECTOR_SSH_CHECK` |
| 5 | `SYNC_FAIL` | (1,2 아님) AND templates 존재(비어있지 않음) AND sync 후에도 RANK 미존재 | 🔴 | `REPORT_TO_CLAUDE` |
| 6 | `UNKNOWN` | 위 전부 미해당 (진단 자체 실패 포함) | 🔴 | `REPORT_TO_CLAUDE` |

`gmm_min_rows`(12)는 신규 상수 아님 — mining 코드의 기존 dropna 조건을 관측 코드에서 재참조.
코드 상수로 안 박혀 있으면 digest 쪽에도 매직넘버로 중복 박지 말고 **주석에 출처(mining 함수명) 명시**.

## Spec 2 — 대시보드 1줄 (초등 kid dashboard 톤, `09_디렉터_쉬운요약.md` 톤 준용)

| state | 문구 |
|---|---|
| `RANK_OK` | "DNA 다 컸어요 – 오늘은 그냥 넘어가도 돼요" |
| `DATA_WAIT_LOW_MFE` | "DNA 재료가 아직 덜 모였어요 – 계속 기다리면 돼요" |
| `GMM_EMPTY` | "재료는 쌓였는데 DNA를 안 만들었어요 – 디렉터가 서버에서 한 번 돌려주세요" |
| `SYNC_FAIL` | "DNA는 만들었는데 연결이 안 붙어요 – Cursor·Claude에게 보여주세요" |
| `DB_PATH_OR_ENV` | "저장소를 못 찾았어요 – 디렉터가 서버 상태를 봐주세요" |
| `UNKNOWN` | "무슨 상황인지 애매해요 – 숫자 메모를 Cursor·Claude에게 보여주세요" |

## Spec 3 — 숫자 메모 필드 (기존 "숫자 메모" 섹션에 추가)

| 필드 | 타입 | 비고 |
|---|---|---|
| `state` | str | 위 enum 값 |
| `checked_at` | str (KST) | 진단 실행 시각 |
| `n_closed_by_tf` | dict{TF:int} | CAT-D read (`bitget_forward_trades`) |
| `n_mfe8_by_tf` | dict{TF:int} | mfe≥`BITGET_MIN_MFE_FOR_MINING`, dropna 후 |
| `templates_present` | bool | `BITGET_GMM_DNA_TEMPLATES` non-empty dict 여부 |
| `gmm_cluster_n` | int\|null | 마지막 sync rankable cluster 수. 소스 없으면 null (추정 금지 — D-3a 관례 동일) |
| `last_error` | str\|null | mine/sync 마지막 에러. 소스 없으면 null |
| `cursor_action` | enum | Spec 1 매핑 |

`gmm_cluster_n`/`last_error` 소스가 기존 코드에 없으면 새 로그 파이프라인을 만들지 말 것 — **null 허용**.

## Spec 4 — 함수 시그니처 (제안, 구현은 Cursor)

```
def diagnose_dna_state(
    config: dict,
    n_closed_by_tf: dict[str, int],
    n_mfe8_by_tf: dict[str, int],
    gmm_min_rows: int,
) -> DnaDiagnosis  # state, action, memo_fields 보유
```

## Spec 5 — 구조 통합 위치

기존 3분할 구조(`09` §텔레그램이 보내는 것) 그대로 재사용:
1. **대시보드** — DNA 칸 문구를 Spec 2로 교체 (기존 "RANK1~3 있음/없음" 이진 대신)
2. **숫자 메모** — Spec 3 필드 추가
3. **복붙** — `cursor_action ∈ {DIRECTOR_SSH_CHECK, REPORT_TO_CLAUDE}`일 때만 노출:
   - `DIRECTOR_SSH_CHECK` → `POST_DEPLOY_OBS_체크리스트.md` §1(DNA RANK) 절차 요약 1~2줄
   - `REPORT_TO_CLAUDE` → 기존 CURSOR_TO_CLAUDE Ask 포맷 안내 1줄 ("state=SYNC_FAIL, 숫자 메모 첨부해 Ask 작성")

## SPOT/FUT

이번 스코프는 TF 집계만. `BITGET_GMM_DNA_TEMPLATES`가 SPOT/FUT 통합 pool인지 분리인지
로컬 구조 확인 후, 분리돼 있으면 market_type별 판정을 별도 Ask로. market_type 하드코딩 금지.

## 안전장치 (kill-switch, 기존 컨벤션 동일 패턴)

`POST_DEPLOY_OBS_DNA_DIAGNOSIS_ENABLED` (config_kv, default `true`) — `false`면 기존
이진 RANK1~3 문구로 즉시 폴백. `POST_DEPLOY_OBS_DIGEST_ENABLED=false`(digest 전체 끄기)와 별개.

## 테스트

fixture 3~5개면 충분(Ask 원문). 최소 권장:
1. `RANK_OK`
2. `DATA_WAIT_LOW_MFE` — 2026-08-19 실측값 그대로 (n_mfe8=0 전 TF, CLOSED=10)
3. `GMM_EMPTY` 또는 `SYNC_FAIL` 중 1개 (조건 유사, 나머지는 재량)
4. `DB_PATH_OR_ENV`
5. (여유 있으면) `UNKNOWN` fallback

`pytest bitget/tests/test_post_deploy_obs_digest_bg.py` 확장 — 기존 3 passed 유지 + 위 fixture.

## 금지 (재확인)

C-2 · MDD 5% · B-2 live · `ENABLE_REAL_EXECUTION` · `BITGET_MIN_MFE_FOR_MINING` 기본값 변경 ·
`gates.py`/`gmm_dna_alpha_sync.py` 본체 수정 · `cursor_action`에 문턱완화·실전 권고 문구 추가

## 완료 정의

- 코드 + 위 fixture 테스트 all pass
- `05_진행로그.md` · `00_전체현황판.md` · `CURSOR_TO_CLAUDE.md` · `NEXT_ACTION.md`(→`WAIT_CLAUDE_OK`) ·
  `NEXT_STEP.md` · `09_디렉터_쉬운요약.md` 갱신
- 「로컬 구조 스냅샷」을 `CURSOR_TO_CLAUDE.md`에 포함 (다음 세션 Architect Mirror 대상)

## sub-phase ID

`POST_DEPLOY_OBS-DNA-UX-01`
