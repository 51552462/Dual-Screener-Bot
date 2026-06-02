# PRACTITIONER_REPORT_FIX — markets 인자 충돌(TypeError) 수술

## 원인

`factory_pipelines.py`의 `_step_pil_practitioner_reports(markets=...)`가
`auto_forward_tester.send_group_practitioner_reports(...)`를 호출할 때
대상 함수(`forward/deep_dive.py`) 시그니처가 `markets`를 받지 않아 아래 크래시 발생:

- `send_group_practitioner_reports() got an unexpected keyword argument 'markets'`

즉, **호출부는 다중 시장 필터 인자를 전달하지만, 구현 함수는 인자 미지원** 상태였습니다.

## 수정 내용

수정 파일: `forward/deep_dive.py`

함수 시그니처를 다음처럼 확장:

- 기존:
  - `def send_group_practitioner_reports(ctx=None, *, cleanup_zombie_trades: bool = True):`
- 변경:
  - `def send_group_practitioner_reports(ctx=None, *, cleanup_zombie_trades: bool = True, markets: tuple[str, ...] | list[str] | None = None, **kwargs):`

내부 로직:

1. `markets`가 전달되면 대문자 집합(`market_allow`)으로 정규화  
2. 활성 그룹 루프에서 `market_allow`에 없는 시장은 스킵  
3. 하위 호환을 위해 `market="KR"` 단일 인자 호출도 `**kwargs`로 수용하여 동일 필터에 반영

이로써 `factory_pipelines.py`의 다음 호출과 시그니처가 일치:

- `markets=("KR",)`
- `markets=("US",)`
- `markets=("KR", "US")`

## 교차 검증

- `factory_pipelines.py`의 PIL 호출부 파라미터명은 모두 `markets`로 일관됨
- 수정 후 시그니처 확인 완료:
  - `(ctx=None, *, cleanup_zombie_trades: bool = True, markets: tuple[str, ...] | list[str] | None = None, **kwargs)`

## 배포

로컬에서:

```bash
git add . && git commit -m "Fix practitioner reports parameter mismatch" && git push origin main
```

서버에서:

```bash
sudo bash update_bot.sh
```
