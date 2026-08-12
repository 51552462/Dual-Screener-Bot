# Cursor Rules Index

> 레포 루트 `.cursorrules` = 요약 헌법  
> 본 폴더 `.mdc` = **자동 적용** 상세 규칙  
> 부팅 문구 SSOT: `docs/work_phases/17_Cursor_세션_부팅_가이드.md`

## 항상 적용 (`alwaysApply: true`)

| 파일 | 내용 |
|------|------|
| `00-core-session-ssot.mdc` | 멀티창 · SESSION_SYNC · status · 트랙 라우팅 · 충돌 해결 |
| `01-engineering-efficiency.mdc` | 브리핑 · targeted diff · 토큰 연소 방지 · Adapter |
| `03-dual-ai-handoff.mdc` | 작업함 · CLAUDE_TO_CURSOR / CURSOR_TO_CLAUDE |
| `04-telegram-ops-hooks.mdc` | DEPLOY_WATCH · IV_OBS · OPS-01 |
| `05-risk-and-verification-layers.mdc` | MDD/DEFCON 방어 · L0~L3 과신 금지 |

## 조건부 적용 (globs)

| 파일 | 트리거 | 내용 |
|------|--------|------|
| `02-track-bitget-coin.mdc` | `bitget/**` | Track B 격리 · 세션 종료 09/NEXT_STEP |
| `06-rp1-vps-lab.mdc` | `regime_panel*` · `run_rp1*` | metrics-only · VPS workers · pkill 주의 |
| `07-independent-verification.mdc` | `docs/independent_verification/**` | V-* 헌법 · WF block 금지 |

## 멀티 창 운용 (권장)

```
창 1: Track A (KR/US) — docs/work_phases/ — 17 §3-A
창 2: Track B (Bitget) — bitget/docs/work_phases/ — 17 §3-B
창 3: Ops 또는 IV — 17 §3-C / §3-E / §3-F
```

**트랙 혼합 금지** · 세션 종료 시 `00_SESSION_SYNC.md` §3 bump.

## Bitget

`bitget/.cursorrules` = Track B 요약. 상세는 `02-track-bitget-coin.mdc`.
