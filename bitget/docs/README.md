# Bitget 구현 기록 인덱스

| Phase | 문서 | 상태 |
|-------|------|------|
| 설계 | [../bitget_architecture_upgrade_plan.md](../../bitget_architecture_upgrade_plan.md) (루트) | 완료 |
| 0–1 | [implementation_phase_0_1_2.md](./implementation_phase_0_1_2.md) | 완료 |
| 2 | [implementation_phase_2_3_hooking.md](./implementation_phase_2_3_hooking.md) | 완료 |
| 3 | [implementation_phase_3.md](./implementation_phase_3.md) | 완료 (물리 분할) |
| 4 | [implementation_phase_4.md](./implementation_phase_4.md) | 완료 |
| 4b | [implementation_phase_4b.md](./implementation_phase_4b.md) · [4_4b_complete](./implementation_phase_4_4b_complete.md) | 완료 |
| 5 | [implementation_phase_5.md](./implementation_phase_5.md) | 완료 |
| 6 | [implementation_phase_6.md](./implementation_phase_6.md) | 완료 |
| 7 | [implementation_phase_7.md](./implementation_phase_7.md) | 완료 |
| RUNBOOK | [../RUNBOOK.md](../RUNBOOK.md) | 완료 |
| Ubuntu 배포 | [ubuntu_isolated_deploy_guide.md](./ubuntu_isolated_deploy_guide.md) | 가이드 |

## 격리 원칙

- 모든 Bitget 코드·설정·문서는 **`bitget/` 하위**
- 주식 팩토리 루트 모듈은 **읽기 전용 import만** (예: `meta_governor_consumer`, `reports/*`)
- `git status` 기준 주식 경로(`forward/`, `factory_pipelines.py`, `system_auto_pilot.py`, `deploy/systemd/dante-*`) **미수정**
