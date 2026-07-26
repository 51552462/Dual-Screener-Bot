from __future__ import annotations

from typing import Any

import numpy as np


def check_apoptosis(
    bandit: Any,
    arm_index: int,
    recent_contexts: Any,
    loss_threshold: float,
) -> tuple[bool, np.ndarray | None]:
    r"""
    Contextual LinUCB 전략의 알파 붕괴 여부와 치명적 문맥을 반환한다.

    Apoptosis 조건
    ----------------
    다음 조건 중 하나라도 참이면 True다.

    1. 지속적인 하위 UCB:
       - 각 최근 문맥에서 전체 arm 중 하위 10%에 속하는지 계산한다.
       - 대상 arm의 문맥별 하위 10% 포함 비율이 80% 이상이고,
       - 최근 문맥 평균 UCB도 arm 평균 UCB 분포의 하위 10%에 속하면 발동한다.

    2. 누적 손실:
           bandit.cumulative_rewards[arm_index] < loss_threshold

    치명적 문맥
    -----------
    함수 입력에는 문맥별 실제 reward 이력이 없으므로, 대상 arm의 선형 모델이
    가장 낮은 기대보상을 예측하는 문맥을 치명적 문맥으로 반환한다.

        theta_a = A_a^{-1} b_a
        fatal_context = argmin_x(theta_a^T x)

    Parameters
    ----------
    bandit:
        다음 속성을 가진 ContextualLinUCB 객체:
        A_inv, b, exploration_alpha, cumulative_rewards,
        n_arms, context_dim.
    arm_index:
        검사할 전략 인덱스.
    recent_contexts:
        shape=(N, D)의 최근 문맥 배열.
    loss_threshold:
        누적 리스크 조정 보상의 사형선.

    Returns
    -------
    tuple[bool, np.ndarray | None]
        (apoptosis_triggered, fatal_context_vector)

        - 사형 조건이 아니면 (False, None)
        - 사형 조건이며 문맥이 있으면 (True, 치명적 문맥의 복사본)
        - 누적 손실로 사형이지만 문맥이 비어 있으면 (True, None)

    Notes
    -----
    "지속적" 기준은 최근 문맥의 80% 이상으로 고정했다.
    운영 환경에서 다른 기준이 필요하면 상수 PERSISTENCE_THRESHOLD를 조정한다.
    """
    BOTTOM_FRACTION = 0.10
    PERSISTENCE_THRESHOLD = 0.80
    EPSILON = 1e-12

    # ------------------------------------------------------------------
    # 1. Bandit 상태 검증
    # ------------------------------------------------------------------
    try:
        n_arms = int(bandit.n_arms)
        context_dim = int(bandit.context_dim)
        exploration_alpha = float(bandit.exploration_alpha)

        a_inv = np.asarray(bandit.A_inv, dtype=np.float64)
        b = np.asarray(bandit.b, dtype=np.float64)
        cumulative_rewards = np.asarray(
            bandit.cumulative_rewards,
            dtype=np.float64,
        ).reshape(-1)
    except (AttributeError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("bandit has an invalid LinUCB state.") from exc

    if n_arms <= 0 or context_dim <= 0:
        raise ValueError("bandit dimensions must be positive.")
    if isinstance(arm_index, bool):
        raise TypeError("arm_index must be an integer.")

    try:
        arm = int(arm_index)
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError("arm_index must be an integer.") from exc

    if arm != arm_index:
        raise TypeError("arm_index must be an integer.")
    if not 0 <= arm < n_arms:
        raise IndexError(f"arm_index must be in [0, {n_arms - 1}].")

    if a_inv.shape != (n_arms, context_dim, context_dim):
        raise ValueError("bandit.A_inv has an invalid shape.")
    if b.shape != (n_arms, context_dim):
        raise ValueError("bandit.b has an invalid shape.")
    if cumulative_rewards.shape != (n_arms,):
        raise ValueError("bandit.cumulative_rewards has an invalid shape.")
    if not np.isfinite(exploration_alpha) or exploration_alpha < 0.0:
        raise ValueError("bandit.exploration_alpha must be finite and >= 0.")
    if not (
        np.all(np.isfinite(a_inv))
        and np.all(np.isfinite(b))
        and np.all(np.isfinite(cumulative_rewards))
    ):
        raise ValueError("bandit state must contain only finite values.")

    try:
        threshold = float(loss_threshold)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("loss_threshold must be numeric.") from exc
    if not np.isfinite(threshold):
        raise ValueError("loss_threshold must be finite.")

    cumulative_loss_trigger = cumulative_rewards[arm] < threshold

    # ------------------------------------------------------------------
    # 2. 최근 문맥 검증
    # ------------------------------------------------------------------
    try:
        contexts = np.asarray(recent_contexts, dtype=np.float64)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("recent_contexts must be numeric.") from exc

    if contexts.size == 0:
        return bool(cumulative_loss_trigger), None

    if contexts.ndim == 1:
        contexts = contexts.reshape(1, -1)

    if contexts.ndim != 2 or contexts.shape[1] != context_dim:
        raise ValueError(
            f"recent_contexts must have shape (N, {context_dim})."
        )
    if not np.all(np.isfinite(contexts)):
        raise ValueError("recent_contexts must contain only finite values.")

    # ------------------------------------------------------------------
    # 3. 모든 문맥 × 모든 arm의 기대보상 및 UCB를 벡터화 계산
    # ------------------------------------------------------------------
    # theta[a] = A_inv[a] @ b[a]
    theta = np.einsum(
        "aij,aj->ai",
        a_inv,
        b,
        optimize=True,
    )

    # expected[n, a] = context[n] dot theta[a]
    expected_rewards = contexts @ theta.T

    # uncertainty_sq[n, a] = x_n^T A_inv[a] x_n
    uncertainty_sq = np.einsum(
        "nd,adj,nj->na",
        contexts,
        a_inv,
        contexts,
        optimize=True,
    )
    uncertainty = np.sqrt(np.maximum(uncertainty_sq, 0.0))

    ucb = expected_rewards + exploration_alpha * uncertainty

    # ------------------------------------------------------------------
    # 4. 하위 10%의 지속성 판정
    # ------------------------------------------------------------------
    # arm이 하나뿐이면 상대적 하위 10% 개념이 없으므로 UCB 트리거를 끈다.
    low_ucb_trigger = False

    if n_arms > 1:
        bottom_k = max(
            1,
            int(np.ceil(BOTTOM_FRACTION * n_arms)),
        )

        # 각 문맥에서 bottom_k번째로 낮은 UCB가 하위 10% 경계다.
        per_context_cutoff = np.partition(
            ucb,
            kth=bottom_k - 1,
            axis=1,
        )[:, bottom_k - 1]

        target_ucb = ucb[:, arm]
        bottom_membership = (
            target_ucb <= per_context_cutoff + EPSILON
        )
        persistence_rate = float(np.mean(bottom_membership))

        # 최근 문맥 평균 UCB의 arm별 하위 10% 여부도 함께 확인한다.
        mean_ucb_by_arm = np.mean(ucb, axis=0)
        mean_cutoff = float(
            np.partition(
                mean_ucb_by_arm,
                kth=bottom_k - 1,
            )[bottom_k - 1]
        )
        mean_is_bottom = (
            mean_ucb_by_arm[arm] <= mean_cutoff + EPSILON
        )

        low_ucb_trigger = bool(
            mean_is_bottom
            and persistence_rate >= PERSISTENCE_THRESHOLD
        )

    apoptosis_triggered = bool(
        low_ucb_trigger or cumulative_loss_trigger
    )

    if not apoptosis_triggered:
        return False, None

    # ------------------------------------------------------------------
    # 5. 치명적 문맥: 대상 arm의 예상 보상이 가장 낮은 문맥
    # ------------------------------------------------------------------
    target_expected_rewards = expected_rewards[:, arm]
    minimum_expected = float(np.min(target_expected_rewards))
    candidates = np.flatnonzero(
        np.isclose(
            target_expected_rewards,
            minimum_expected,
            rtol=1e-12,
            atol=1e-12,
        )
    )

    if candidates.size == 1:
        fatal_index = int(candidates[0])
    else:
        # 기대보상이 같으면 대상 arm의 UCB가 더 낮은 문맥을 선택한다.
        fatal_index = int(
            candidates[
                np.argmin(ucb[candidates, arm])
            ]
        )

    return True, contexts[fatal_index].copy()


def test_check_apoptosis() -> None:
    """하위 UCB, 누적 손실, 정상 arm 및 빈 문맥을 검증한다."""

    class MockBandit:
        n_arms = 10
        context_dim = 3
        exploration_alpha = 0.10

        A_inv = np.repeat(
            np.eye(3, dtype=np.float64)[None, :, :],
            10,
            axis=0,
        )
        b = np.zeros((10, 3), dtype=np.float64)
        cumulative_rewards = np.zeros(10, dtype=np.float64)

    bandit = MockBandit()

    # arm 0은 첫 번째 특성이 양수인 문맥에서 지속적으로 낮은 기대보상을 갖는다.
    bandit.b[0] = np.array([-2.0, 0.0, 0.0])

    # 나머지 arm들은 대체로 양의 기대보상.
    for index in range(1, bandit.n_arms):
        bandit.b[index] = np.array([0.2 + index * 0.02, 0.0, 0.0])

    contexts = np.array(
        [
            [0.2, 0.0, 1.0],
            [0.5, 0.2, 1.0],
            [0.8, -0.3, 1.0],
            [1.0, 0.1, 1.0],
            [1.5, -0.5, 1.0],
        ],
        dtype=np.float64,
    )

    killed, fatal = check_apoptosis(
        bandit,
        arm_index=0,
        recent_contexts=contexts,
        loss_threshold=-5.0,
    )
    assert killed is True
    assert fatal is not None
    # theta=[-2,0,0]이므로 첫 특성이 가장 큰 문맥에서 최저 예상보상.
    assert np.allclose(fatal, contexts[-1])

    # UCB는 정상이어도 누적 보상이 사형선 아래면 제거.
    bandit.b[1] = np.array([1.0, 0.0, 0.0])
    bandit.cumulative_rewards[1] = -2.0

    killed_by_loss, fatal_by_loss = check_apoptosis(
        bandit,
        arm_index=1,
        recent_contexts=contexts,
        loss_threshold=-1.0,
    )
    assert killed_by_loss is True
    assert fatal_by_loss is not None

    # 정상 arm은 제거하지 않는다.
    bandit.cumulative_rewards[9] = 2.0
    alive, no_fatal = check_apoptosis(
        bandit,
        arm_index=9,
        recent_contexts=contexts,
        loss_threshold=-1.0,
    )
    assert alive is False
    assert no_fatal is None

    # 문맥이 없어도 누적 손실 조건은 동작한다.
    bandit.cumulative_rewards[2] = -3.0
    killed_without_context, empty_fatal = check_apoptosis(
        bandit,
        arm_index=2,
        recent_contexts=[],
        loss_threshold=-1.0,
    )
    assert killed_without_context is True
    assert empty_fatal is None

    print("test_check_apoptosis: all tests passed")
    print("fatal context:", fatal)


if __name__ == "__main__":
    test_check_apoptosis()
