# Bitget 구현 기록 인덱스

| Phase | 문서 | 상태 |
|-------|------|------|
| **1단계 진단** | [01_architecture_mapping_and_diagnosis.md](./01_architecture_mapping_and_diagnosis.md) · [루트 복사본](../01_architecture_mapping_and_diagnosis.md) | 완료 |
| **Phase 1–2 실행** | [02_phase1_2_execution_report.md](./02_phase1_2_execution_report.md) | 완료 |
| **Phase 3–4 실행** | [03_phase3_4_execution_report.md](./03_phase3_4_execution_report.md) · [루트 요약](../03_phase3_4_execution_report.md) | 완료 |
| **Phase 5 실행** | [04_phase5_satellite_config.md](./04_phase5_satellite_config.md) · [루트 요약](../04_phase5_satellite_config.md) | 완료 |
| **Phase 6 실행** | [05_phase6_bugfix_and_validation.md](./05_phase6_bugfix_and_validation.md) · [루트 요약](../05_phase6_bugfix_and_validation.md) | 완료 |
| **Phase 7 실행** | [06_phase7_pipeline_e2e_and_cutover.md](./06_phase7_pipeline_e2e_and_cutover.md) · [루트 요약](../06_phase7_pipeline_e2e_and_cutover.md) | 완료 |
| **Phase 8 검토** | [07_phase8_feasibility_review.md](./07_phase8_feasibility_review.md) · [루트 요약](../07_phase8_feasibility_review.md) | 계획 |
| **Phase 8 Track A** | [08_phase8_track_a_execution_report.md](./08_phase8_track_a_execution_report.md) · [루트 요약](../08_phase8_track_a_execution_report.md) | 완료 |
| **Ubuntu 배포·업데이트** | [09_ubuntu_deployment_and_update_guide.md](./09_ubuntu_deployment_and_update_guide.md) · [루트 요약](../09_ubuntu_deployment_and_update_guide.md) | 가이드 |
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
