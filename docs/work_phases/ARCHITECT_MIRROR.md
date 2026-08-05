# Architect Mirror — Claude 능동 의견 로그 (KR/US · Track A)

> **역할**: Cursor 로컬 변경을 Claude가 **구조 지도**로 대입해 OK/거절 외 **추가 설계·우선순위·리스크**를 남기는 SSOT.  
> **갱신**: Claude Pro — 검증·Handoff·갭 리뷰 세션 **종료 시 상단에** 날짜 블록 추가.  
> **Cursor**: `CURSOR_TO_CLAUDE.md` §로컬 구조 스냅샷 최신 유지.  
> **Bitget 대응**: `bitget/docs/work_phases/ARCHITECT_MIRROR.md` (Track B, **내용 섞지 않음**)

---

## Mirror — 2026-08-06 · [CAT-G] · A-4 검증 완료

### 로컬 구조 이해
- Adapter·6쌍·crisis bypass·killswitch·UNKNOWN fallback 확인 · 16 passed
- 비고(블로킹 아님): 6쌍 명시목록·`HYSTERESIS_MIN_DAYS` 재사용 문서 한 줄 권고

### 다음 Handoff 후보
| # | 후보 | 비고 |
|---|------|------|
| 1 | **A-1-R3** CAT-F | L243 clutch — 즉시 착수 |
| 2 | **A-5** S5 | F+G+C — Rule4 충돌 · 디렉터 (a)/(b) 확인 후 |

### 디렉터 한 줄
> A-4 Done — R3 또는 배포 승인 먼저. A-5는 세션 방식 결정 전 설계 안 함.

---

## Mirror — 2026-08-05 · [CAT-F] · A-1-R2 검증 완료

### 로컬 구조 이해
- 맞게 반영된 점: L234 `resolve_config_float` · 2+7 테스트 · tier/거버너 쓰기 미변경
- grep 부가 등록: L243→**A-1-R3** · L73/L411→🟡 후속 · doomsday→🟢 종결

### 추가 제안
| # | 제안 | Layer | 우선순위 |
|---|------|-------|----------|
| 1 | **A-4** CAT-G 히스테리시스 Adapter Handoff (권장 다음) | 🟡 G | 최고 |
| 2 | A-1-R3 L243 clutch mult (디렉터 선택 시 CAT-F 단독) | 🔴 F | 중간 |
| 3 | A-1-R2 **디렉터 Critical 배포 승인** + `update_factory.sh` | ① 디렉터 | 높음 |

### 디렉터 한 줄
> R2 Done — **A-4(G) 새 대화** 권장. R3·배포 승인은 병렬 선택.

---

## Mirror — 2026-08-05 · [CAT-F] · M-R0 + A-3 + A-1-R1 검증 완료

### 로컬 구조 이해
- 맞게 반영된 점: A-3 `POSITION_QUOTA_REGIME_MAP`·곱연산(floor) Handoff 일치 · A-1-R1 `resolve_config_float` SSOT · LOCKDOWN 3중 방어 `00` 명문화 유지 · M-R0 비침투·D-2 분리
- 빠졌거나 불일치: `meta_governor_consumer` L234 `META_GLOBAL_KELLY_MULT` `or 1.0` — A-1-R1 스코프 밖, **A-1-R2 후보**

### 추가 제안
| # | 제안 | Layer | 우선순위 |
|---|------|-------|----------|
| 1 | `META_GLOBAL_KELLY_MULT` falsy 0.0 감사 — **A-1-R2** 또는 A-2 정식 편입 | 🔴 F | 높음 |
| 2 | A-4 Handoff — `hysteresis_days_f`(RL) vs 고정 1일/2일 **Adapter** (CAT-G 단독 세션) | 🟡 2 | 높음 |
| 3 | 서버 배포·paper MDD/tier 관측 (`06` 3단계 준비) | ① 디렉터 | 중간 |

### 다음 Handoff 후보 재정렬
1. **A-4** 비대칭 히스테리시스 Adapter (디렉터 확인 후 새 대화)
2. **A-1-R2** META_GLOBAL_KELLY_MULT falsy (A-4 전·후 병렬 가능)

### 디렉터 한 줄
> M-R0·A-3·A-1-R1 Done — **A-4 Adapter Handoff**로 Phase A 후반 재개. R2는 Kelly chain 잔여 falsy.

---

## Mirror — 2026-08-04 · [CAT-M] · M-R0 + factory 긴급 복구

### 로컬 구조 이해 (Cursor 스냅샷 대비)
- 맞게 반영된 점: `llm_provider_core`·`overseer_quality`·degraded audit·watchdog heartbeat thread·`inverse_etf_sniper` 741/797 문법 수정 — 비침투·D-2 분리 유지
- 빠졌거나 불일치: Bitget `overseer_audit_binder` **import 없음**(격리 헌법) — `overseer_audit_contract.py` 계약만. Anthropic 실연동·서버 `.env` 중복 `DB_STORAGE_PATH`는 **운영 잔여**

### 추가 제안
| # | 제안 | Layer | 우선순위 |
|---|------|-------|----------|
| 1 | Claude OK: M-R0 + A-3 + A-1-R1 **병렬 검증** (`CURSOR_TO_CLAUDE`) | ① 디렉터 | 최고 |
| 2 | 서버 `.env` — `DB_STORAGE_PATH` **한 줄만** `/var/lib/quant-factory/data` | ① 디렉터 | 최고 |
| 3 | A-4 Handoff 전 RL `hysteresis_days_f` vs `predictive_regime_ensemble` **Adapter 제안** 필수 | 🟡 2 | 높음 |

### 디렉터 한 줄
> factory 살아난 뒤 **문서·Claude OK**로 Phase A 다시 전진. 코인(Bitget) Mirror와 파일만 대칭, 내용은 분리.

---

## Claude가 쓸 블록 형식

```markdown
## Mirror — YYYY-MM-DD · [CAT-X] · {sub-phase}

### 로컬 구조 이해
- 맞게 반영된 점:
- 빠졌거나 불일치:

### 추가 제안 (1~3개)
| # | 제안 | Layer | 우선순위 | 근거 |

### 다음 Handoff 후보 재정렬
1.
2.

### 디렉터 한 줄
>
```

---

## Cursor OUTBOX 스냅샷 (`CURSOR_TO_CLAUDE.md`에 포함)

- 변경 파일·함수·config 키
- 기존 모듈과 **겹침/중복**
- 알려진 부채 (테스트·서버·defer)
- Attribution (🟢/🟡/🔴)

---

## 디렉터 사용법

1. Cursor 구현 후 → `CURSOR_TO_CLAUDE` 복붙  
2. Claude 답변에 **OK + Mirror 블록** 요청 → 본 파일 상단 갱신  
3. `09`·`NEXT_STEP`에 Claude 제안 반영 여부 결정

**금지**: Mirror가 Handoff 없이 🔴 live 변경 지시.
