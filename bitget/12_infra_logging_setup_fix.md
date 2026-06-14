# 12 — infra/logging_setup ImportError 수정 요약

> **정본:** `bitget/docs/12_infra_logging_setup_fix.md`

## 원인

`bitget/infra/` 패키지가 없어짐 → `governance/infra/` 로 잘못 이동 + merge conflict

## 해결

- `bitget/infra/` 8개 모듈 **복구** (`logging_setup.py` 포함)
- `bitget_auto_pilot.py` import **변경 없음**

## 서버

```bash
git pull
python -c "from bitget.infra.logging_setup import setup_logging; print('OK')"
sudo systemctl restart dante-bitget-factory
```
