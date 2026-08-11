# SRV-01 · Claude Pro STRATEGIC REVIEW — 붙여넣기 초안

> 디렉터: **아래 전체를 Claude Pro 새 채팅에 복사** + `rp1_20260811_v233.json` 첨부  
> Claude OK 후 → `CLAUDE_TO_CURSOR.md` 상단에 Handoff append → Cursor 착수

---

## Claude Pro 부팅 (맨 위에 함께 붙여넣기)

```text
역할: Claude Pro Architect. 구현 코드 작성 금지.

먼저 읽기:
1) docs/work_phases/00_SESSION_SYNC.md §3
2) docs/work_phases/15_POST_RP1_단계별로드맵.md
3) 첨부 rp1_20260811.json (v2.3.3)

트랙: KR/US docs/work_phases/ SSOT. bitget/ 제외.
이번 창 목적: SRV-01 STRATEGIC REVIEW — POST-RP-1 다음 sub-phase 1개만 Go.
```

---

## 본문 (디렉터 → Claude)

```text
## SRV-01 STRATEGIC REVIEW 요청

RP-1 Alpha Proof가 v2.3.3 baseline으로 확정됐다. 이제 "다음에 무엇을 하나"만 고르고 싶다.

### 배경
- 목표 헌법: CAGR 40~70% · MDD ≤10% (00_마스터_로드맵)
- RP-1: KR200+US200 · 15구간(상승5·횡보5·하락5) · lookahead v1 (오늘 뇌 템플릿)
- 측정: daily_equal_weight + Phase A tier replay + A-3 쿼터 + ACTION_BY_REGIME kelly_cap
- 결과 JSON: rp1_20260811.json (schema v2.3.3)

### RP-1 핵심 결과 (재검증 요청)
| 항목 | 값 |
|------|-----|
| overall_verdict | PASS (구조적 하한) |
| mdd_crosscheck | MDD_OK — tier MDD 8.2~9.3%, 위반 0 |
| BULL bucket | 3 PASS / 2 FAIL (BULL_03 최근상승, BULL_05 글로벌리플레이) |
| SIDEWAYS | 3 PASS / 2 NEAR_MISS (SIDE_02, SIDE_03) |
| BEAR | 2 PASS / 3 NEAR_MISS |
| Stage2 C-1 | OPTIONAL_SKIP (Stage1 PASS) |
| North Star CAGR | 미증명 — BULL 일부만 고수익, FAIL 2구간 |

### Fail/Near 구간 요약 (첨부 JSON에서 period_return_pct·pf·fail_cause 확인)
- BULL_03: period_ret ≈ -0.5% — recency drift 의심 (원인 B)
- BULL_05: period_ret ≈ -9% — 짧은 구간 수익 부족 (원인 B)
- SIDE_02/03: NEAR_MISS — PF·CAGR 근처
- BEAR 3 NEAR: MDD는 OK, 수익 edge 부족 (손실 억제 ≠ S5 헤지 증거)

### Cursor 추천 후보 (하나만 Go 해달라)
| ID | 내용 | 기대 |
|----|------|------|
| BULL-RECENCY-01 | BULL_03/05 집중 — 템플릿 fit·DNA recency 진단 | 상승장 CAGR 목표에 직격 |
| SIDE-ALPHA-01 | SIDE_02/03 NEAR 개선 | 횡보 알파 |
| BEAR-S5-SIM-01 | BEAR 포트폴리오에 S5/인버스 반영 시뮬 (RP-1 밖) | 하락장 수익 edge |
| C-1-REDUCED | 섹터 부스트 A/B (Stage1 PASS라 우선순위 낮음) | Near-miss 상방 |

### 질문 (반드시 답해줘)
1. **위 4개 중 Go 1개 / 보류 3개** — 이유 3줄씩
2. Go한 sub-phase의 **Handoff 초안** (범위·금지·완료 기준·2주 timebox 여부)
3. RP-1 PASS가 **North Star 달성 증명이 아님**을 전제로, 현 구조에서 **현실적 상한** 한 줄
4. C-1을 지금 안 해도 되는지 — 데이터 신뢰도 vs 우선순위 관점
5. OPS(VPS F-GATE/F-RETIRE/BEAR-UD 배포)를 Alpha와 **병렬**해도 되는지

### 출력 형식
1) STRATEGIC VERDICT 한 줄
2) Go sub-phase 표 (1개)
3) Handoff 블록 (CLAUDE_TO_CURSOR.md에 바로 붙일 수 있는 형식)
4) 디렉터 3줄 요약
5) Cursor에게 전달할 다음 액션 1줄

코드 전문 출력 금지. 판정·우선순위·Handoff만.
```

---

## Claude 답변 수신 후 (디렉터 체크리스트)

- [ ] `CLAUDE_TO_CURSOR.md` 상단에 Handoff append
- [ ] `15_POST_RP1_단계별로드맵.md` 단계 1 ✅ · 단계 2 ID 기입
- [ ] `05_진행로그.md` §SRV-01에 Claude OK 날짜
- [ ] `NEXT_ACTION.md` → 단계 2 sub-phase로 갱신
- [ ] `00_SESSION_SYNC.md` §3 앵커 bump (`SYNC-2026-08-11-B`)

---

*작성: Cursor 2026-08-11 · OUTBOX: `CURSOR_TO_CLAUDE.md` §SRV-01*
