# Architect Mirror — Claude 능동 의견 로그 (KR/US · Track A)

> **역할**: Cursor 로컬 변경을 Claude가 **구조 지도**로 대입해 OK/거절 외 **추가 설계·우선순위·리스크**를 남기는 SSOT.  
> **갱신**: Claude Pro — 검증·Handoff·갭 리뷰 세션 **종료 시 상단에** 날짜 블록 추가.  
> **Cursor**: `CURSOR_TO_CLAUDE.md` §로컬 구조 스냅샷 최신 유지.  
> **Bitget 대응**: `bitget/docs/work_phases/ARCHITECT_MIRROR.md` (Track B, **내용 섞지 않음**)

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
