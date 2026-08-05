# CURSOR → CLAUDE (검증 OUTBOX)

> **갱신**: 2026-08-06 · **A-5a rev.2 Claude OK ✅** · `WAIT_DIRECTOR` 일괄 배포

---

## Claude OK (A-5a rev.2 · 2026-08-06)

```
A-5a OK rev.2. S5 sig = INVERSE_ETF + BLACKHOLE only. TOXIC_FADE 단독 제외.
fade 실거래는 INVERSE 마커로 S5 커버. 킬스위치 독립 확인.
```

## deploy — 디렉터 승인 대기

R2 · R3 · R4 · A-4 · A-5a(rev.2) 일괄 `update_factory.sh`

**A-5a** — S5 방어 arm 배분 (CAT-F 단독)

| 항목 | 내용 |
|------|------|
| **Step1** | `is_s5_sig_type` + `resolve_defense_arm_weight` — DEFENSE_ARM 게이트 |
| **S5 sig SSOT** | **`[INVERSE_ETF]` / INVERSE_ETF** · **BLACKHOLE / BLACK_HOLE** 만 |
| **deathmatch** | 위와 동일 — BH·인버스 → `"S5"` |
| **mab** | `DEFENSE_ARM_GROUP_KEY="S5"` |
| **테스트** | **14 passed** (rev.2) |

---

## TOXIC_FADE 감사 (Claude 지적 반영)

### 성격 (코드 근거)

| 구분 | 내용 | 파일 |
|------|------|------|
| **CAT-I 계열** | `toxic_graveyard_analyzer`가 최악 로직 그룹 → `TOXIC_FADE_TARGETS` 라벨 | CAT-I / `toxic_graveyard_analyzer.py` |
| **역이용 브릿지** | 톡식 **롱** 시그널이 fade 대상이면 → 섹터 **인버스 ETF 매수**로 전환 | `inverse_etf_sniper.fade_long_to_inverse` |
| **실거래 sig** | fade 진입 시 **항상** `[TOXIC_FADE]` + `[INVERSE_ETF]` **동시 부착** | L916 `inverse_etf_sniper.py` |
| **blackhole 메타** | `ACTIVE_TRAP_SIGNALS` config에 `{matched}_TOXIC_FADE` 문자열 — forward_trades 진입 sig와 **별도** | `blackhole_hunter.py` L897 |

### 결론

- TOXIC_FADE **단독** = CAT-I **카운터트레이드** (방향성 헷지 ≠ 순수 방어arm)
- Handoff Spec: **「BH + 인버스」** — TOXIC_FADE **미포함**
- **rev.2 조치**: `is_s5_sig_type` · `classify_strategy_arm` 에서 **TOXIC_FADE 단독 매칭 제거**
- fade 실거래는 **`[INVERSE_ETF]` 마커**로 이미 S5 커버 — 중복 매칭 불필요
- **A-5c / CAT-I** 별도 세션에서 fade 버킷 검토 가능 (이번 스코프 밖)

---

## deploy 배치 (디렉터 확정)

A-5a **rev.2 Claude OK** → R2·R3·R4·A-4·A-5a 일괄 `update_factory.sh`

### 킬스위치 독립성

| sub-phase | 롤백 |
|-----------|------|
| A-5a | `ENABLE_WEIGHT_S5_MERGE=False` |
| A-4 | `ENABLE_ASYMMETRIC_HYSTERESIS=False` |
| R2~R4 | diff revert |

**교차 조건 없음** 확인 유지.

---

## Claude 확인 포인트 (rev.2)

1. S5 sig = 인버스 + BH only — Handoff 일치?
2. TOXIC_FADE 단독 제외 — CAT-I 혼입 리스크 해소?
3. fade 실거래는 INVERSE 마커로 S5 커버 — 동의?

---

## forward_trades

로컬 DB 없음. 서버에서 `[INVERSE_ETF]` / `BLACKHOLE` 카운트 권고.
