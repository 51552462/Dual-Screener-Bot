"""
상한가 코호트 DNA vs 당일 봉형 DNA — 설정 키·의미 분리 (코드 가독성).

영속 저장소의 키 문자열은 레거시 호환을 위해 `LIMIT_UP_DNA` 로 유지한다.
코드에서는 `LIMIT_UP_COHORT_DNA_CONFIG_KEY` 만 참조해 '상한가 역추적 코호트'임을 명시한다.
"""

# system_config / SQLite KV 에 저장되는 실제 키 (변경 시 기존 배포와 호환 깨짐)
LIMIT_UP_COHORT_DNA_CONFIG_KEY = "LIMIT_UP_DNA"
