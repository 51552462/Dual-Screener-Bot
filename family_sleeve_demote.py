"""FAMILY-SLEEVE-DEMOTE-01 — 원조 슬리브 그룹 Kelly 0.25 override SSOT.

apply_meta_kelly_merge · resolve_group_treasury_mult 가 이 테이블만 참조한다.
스캔/INSERT/F-GATE/LOCKDOWN/MDD 무접촉. 데스매치가 맵을 0으로 써도 읽기 경로에서 0.25.
"""
from __future__ import annotations

FAMILY_SLEEVE_DEMOTE_MULT = 0.25

# Claude 확정 12키. 대세 S1 두 줄은 장부 하이픈(-) + 디렉터 슬래시(/) 철자 모두 포함.
FAMILY_SLEEVE_DEMOTE_KEYS: frozenset[str] = frozenset(
    {
        "[STANDARD] B (일반)",
        "[SUPERNOVA_COSINE] RANK_A_장기매집",
        "[SUPERNOVA_COSINE] US_RANK_A_장기매집",
        "🔥 S1 (5선 관통 / 448 완전정배열)",
        "🔥 S1 (대세 추세 돌파 - 소형/테마주)",
        "🔥 S1 (대세 추세 돌파 / 소형/테마주)",
        "🔥 S1 (대세 추세 돌파 - 우량/중견주)",
        "🔥 S1 (대세 추세 돌파 / 우량/중견주)",
        "🔥 US S1 (5선 관통 / 448 완전정배열)",
        "🔥 S4 (역배열 바닥 탈출 - 우량/중견주)",
        "🔥 S4 (역배열 바닥 탈출 - 초소형 텐배거)",
        "🔥",
        "👑",
        "💎",
    }
)

FAMILY_SLEEVE_DEMOTE_EXCLUDE_EXAMPLE_KEYS: frozenset[str] = frozenset(
    {
        "🌱",
        "[SUPERNOVA_COSINE] RANK_B_중기스윙",
        "[SUPERNOVA_COSINE] US_RANK_B_중기스윙",
        "[SUPERNOVA_COSINE] US_MEME_슈팅",
    }
)


def is_family_sleeve_demote_key(core_group_name: str) -> bool:
    return str(core_group_name or "").strip() in FAMILY_SLEEVE_DEMOTE_KEYS


def apply_family_sleeve_group_mult(core_group_name: str, current_mult: float) -> float:
    """데모션 키면 0.25로 교체(곱셈 아님). 그 외 current_mult 그대로."""
    if is_family_sleeve_demote_key(core_group_name):
        return float(FAMILY_SLEEVE_DEMOTE_MULT)
    try:
        return float(current_mult)
    except (TypeError, ValueError):
        return 1.0
