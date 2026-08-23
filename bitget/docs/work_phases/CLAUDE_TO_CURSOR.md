# CLAUDE → CURSOR · LS-GOAL-UX-01 (롱/숏 표시 분리 · 표시만)

> **작성**: Claude Pro (Architect) · 2026-08-23  
> **요청 출처**: Cursor OUTBOX 2026-08-23 · LS-GOAL-UX Ask  
> **상태**: **DONE** — Claude OK 2026-08-23  
> **CAT**: J(주) · D(read-only hydrate만)  
> **금지**: C-2 · MDD5% tier · live · ENABLE_REAL_EXECUTION · Kelly/gate 임계값 변경 · 신규 하드캡 · side별 목표선 신설

---

## 목표 한 줄

롱/숏 진행 상황을 「쉬운판」·digest에 **표시만** 분리(목표 숫자는 Track B 공유 유지) — **LS-GOAL-UX-01**.

## Ask 4문항 회신 (확정)

| # | 질문 | 회신 |
|---|------|------|
| 1 | sub-phase ID | **LS-GOAL-UX-01** (표시만). LS-NORTH-STAR-01(하드캡 분리) 🔴 defer |
| 2 | 스펙 | §2 필드 + 중복 제거 규칙 |
| 3 | Critical 비접촉 | OK — Kelly·gate·live·MDD5%·ENABLE_REAL_EXECUTION 미접촉 |
| 4 | 순서 | SECTOR 최종 OK와 **병렬 가능** · 이번 범위 밖 |

---

## 구현 범위

- [ ] `collect_ls_split_summary()` — `bitget_forward_trades` GROUP BY `position_side`, status (read-only)
- [ ] `north_star_panel_bg.py` L/S 2열 렌더 추가
- [ ] `post_deploy_obs_digest_bg.py` kid dashboard 요약줄 1줄 확장 (4칸 구조 비접촉)
- [ ] SHORT `blocked_today` = `short_funnel_report_bg` import (재계산 금지)
- [ ] LONG `blocked_today` = 이번 범위 제외 (표시 생략)
- [ ] config kill-switch `POST_DEPLOY_OBS_LS_SPLIT_ENABLED` (default true)
- [ ] SPOT SHORT=0 각주 문구

## 반환 필드 (side별)

- `open_count`
- `closed_today` / `closed_cum`
- `win_cum` / `loss_cum` (CLOSED_WIN / CLOSED_LOSS)
- `pnl_cum_usdt` (기존 pnl 필드 합, **null 허용** — 추정 금지)
- SHORT only: `blocked_today` from short_funnel 총합 import
- LONG: `blocked_today` 키 **없음**

## 중복 제거

차단 버킷 상세는 short_funnel 단일 출처. L/S 표는 숫자 총합만 + 「상세→숏 퍼널 참고」.

## 금지 구현

- `dual_north_star_ledger.py` 목표값 side 분리
- MDD5%/연복리 side별 하드캡
- short_funnel 버킷을 L/S 표에 재구성

## 테스트

- LONG/SHORT 키 존재, LONG에 `blocked_today` 없음 assert
- SHORT `blocked_today` == short_funnel 총합 (import 검증)
- kill-switch false → 기존 출력과 동일

## Cursor 응답 형식

```
LS-GOAL-UX-01: OK | 수정 spec: …
로컬 구조 스냅샷 + 비접촉 확인(forward/gates.py, gmm_dna_alpha_sync.py 등)
```
