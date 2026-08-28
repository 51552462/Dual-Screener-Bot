# CLAUDE → CURSOR · LANE_FASTCHECK (Handoff 보관)

> **레인**: `LANE_FASTCHECK` · HIST(`LANE_HIST3FIX`)와 **혼합 금지**  
> append 보관 · 덮어쓰기 금지

---

# CLAUDE → CURSOR · B0-SAMPLE-CONTRACT Claude OK
# (append 보관 · 2026-08-28)

> 작성: Claude Pro · [CAT-F] · 판정: **OK** · sub-phase **DONE**
> 추가 diff 없음 · HIST 비접촉 · 표본 부족은 관측 과제

---

# CLAUDE → CURSOR · B0-SAMPLE-CONTRACT Handoff
# (append 보관 · 2026-08-28)

> **작성**: Claude Pro · 2026-08-28 · [CAT-F]  
> **상태**: **WAIT_CURSOR_IMPL** → 문서 §7만 · 코드 없음  
> **판정 배경**: Ask 재정렬 1~4 OK · 다음 = B0-SAMPLE-CONTRACT

## [CAT-F] 자본배분&리스크 — B0-SAMPLE-CONTRACT (표본 충분성 계약 문서)

### sub-phase ID
B0-SAMPLE-CONTRACT

### SSOT (변경 금지 unless noted)
- 파일: `bitget/docs/work_phases/13_B1_신뢰사다리.md` — **§7 신설만**, §1~§6 비변경
- 참조만: §2 R6 행, §3 Kill표, `14_UNIVERSE-BT_구조생존검증.md` §3 배너 패턴
- config: 없음

### 변경 Spec
- §7.1 정의 배너, §7.2 페이스 기준(30÷56×7), §7.3 R2 표시 규칙, §7.4 트리거표, §7.5 금지
- SPOT/FUT: 공통, market_type 분리만

### Config 변경
없음

### 인접 CAT 영향
- CAT-C: 없음 (R1b 트리거 문서화만)
- CAT-Q: 없음 (FULL-BT 언급만)
- CAT-J: 읽기만 · CAT-N/D/H: 비접촉

### 롤백 조건
§7 섹션 삭제만

### Cursor 지시
- Targeted diff only — `13_` §7만 · 코드·게이트·config 금지
- 본 파일 append 보관 · HIST 레인 비접촉
- 09 / NEXT_STEP: **이번 라운드 미갱신** (Claude OK 후 Claude가 갱신)

### 위험도
🟢 문서 전용
