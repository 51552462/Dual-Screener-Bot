from __future__ import annotations

from typing import Any

import numpy as np


class ContextualLinUCB:
    r"""
    Contextual Multi-Armed Bandit using the disjoint LinUCB algorithm.

    각 전략(arm) a는 독립적인 선형 보상 모델을 가진다.

        E[r | x, a] = theta_a^T x

    Bayesian ridge linear regression의 충분통계:

        A_a = lambda * I + sum(x_t x_t^T)
        b_a = sum(r_t x_t)
        theta_a = A_a^{-1} b_a

    Upper Confidence Bound:

        UCB_a(x)
            = theta_a^T x
            + alpha * sqrt(x^T A_a^{-1} x)

    A_inv는 Sherman-Morrison 공식으로 O(D^2)에 갱신한다.
    """

    def __init__(
        self,
        n_arms: int,
        context_dim: int,
        *,
        exploration_alpha: float = 1.0,
        ridge_alpha: float = 1.0,
        reward_clip: float | None = None,
        dtype: Any = np.float64,
    ) -> None:
        if isinstance(n_arms, bool) or int(n_arms) != n_arms or n_arms <= 0:
            raise ValueError("n_arms must be a positive integer.")
        if (
            isinstance(context_dim, bool)
            or int(context_dim) != context_dim
            or context_dim <= 0
        ):
            raise ValueError("context_dim must be a positive integer.")

        exploration_alpha = float(exploration_alpha)
        ridge_alpha = float(ridge_alpha)

        if not np.isfinite(exploration_alpha) or exploration_alpha < 0.0:
            raise ValueError("exploration_alpha must be finite and >= 0.")
        if not np.isfinite(ridge_alpha) or ridge_alpha <= 0.0:
            raise ValueError("ridge_alpha must be finite and > 0.")

        if reward_clip is not None:
            reward_clip = float(reward_clip)
            if not np.isfinite(reward_clip) or reward_clip <= 0.0:
                raise ValueError("reward_clip must be finite and > 0.")

        self.n_arms = int(n_arms)
        self.context_dim = int(context_dim)
        self.exploration_alpha = exploration_alpha
        self.ridge_alpha = ridge_alpha
        self.reward_clip = reward_clip
        self.dtype = np.dtype(dtype)

        identity = np.eye(self.context_dim, dtype=self.dtype)

        # Ridge prior: A_a = lambda I, 따라서 시작부터 양의 정부호이다.
        self.A = np.repeat(
            (self.ridge_alpha * identity)[None, :, :],
            self.n_arms,
            axis=0,
        )
        self.A_inv = np.repeat(
            (identity / self.ridge_alpha)[None, :, :],
            self.n_arms,
            axis=0,
        )
        self.b = np.zeros(
            (self.n_arms, self.context_dim),
            dtype=self.dtype,
        )
        self.update_counts = np.zeros(self.n_arms, dtype=np.int64)
        self.cumulative_rewards = np.zeros(
            self.n_arms,
            dtype=self.dtype,
        )

    def _validate_context(self, context_vector: Any) -> np.ndarray:
        """문맥을 shape=(D,)의 유한 실수 벡터로 검증한다."""
        try:
            context = np.asarray(
                context_vector,
                dtype=self.dtype,
            ).reshape(-1)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("context_vector must be numeric.") from exc

        if context.shape != (self.context_dim,):
            raise ValueError(
                f"context_vector must have shape ({self.context_dim},), "
                f"got {context.shape}."
            )
        if not np.all(np.isfinite(context)):
            raise ValueError("context_vector must contain only finite values.")
        return context

    def _validate_arm_index(self, arm_index: int) -> int:
        """arm 인덱스를 검증한다."""
        if isinstance(arm_index, bool):
            raise TypeError("arm_index must be an integer.")
        try:
            arm = int(arm_index)
        except (TypeError, ValueError, OverflowError) as exc:
            raise TypeError("arm_index must be an integer.") from exc

        if arm != arm_index:
            raise TypeError("arm_index must be an integer.")
        if not 0 <= arm < self.n_arms:
            raise IndexError(
                f"arm_index must be in [0, {self.n_arms - 1}]."
            )
        return arm

    def parameter_estimates(self) -> np.ndarray:
        r"""각 arm의 theta_a = A_a^{-1} b_a를 반환한다."""
        return np.einsum(
            "aij,aj->ai",
            self.A_inv,
            self.b,
            optimize=True,
        )

    def predict_mean_rewards(self, context_vector: Any) -> np.ndarray:
        """현재 문맥에서 각 arm의 기대 보상 theta_a^T x를 반환한다."""
        context = self._validate_context(context_vector)
        return self.parameter_estimates() @ context

    def ucb_scores(self, context_vector: Any) -> np.ndarray:
        r"""
        score_a = theta_a^T x + exploration_alpha * sqrt(x^T A_a^-1 x)
        """
        context = self._validate_context(context_vector)
        expected_rewards = self.parameter_estimates() @ context

        uncertainty_sq = np.einsum(
            "i,aij,j->a",
            context,
            self.A_inv,
            context,
            optimize=True,
        )
        uncertainty = np.sqrt(np.maximum(uncertainty_sq, 0.0))
        scores = expected_rewards + self.exploration_alpha * uncertainty
        return np.asarray(scores, dtype=self.dtype)

    def select_arm(self, context_vector: Any) -> int:
        """
        현재 문맥에서 UCB가 가장 높은 arm 인덱스를 반환한다.

        동점이면 업데이트 횟수가 가장 적은 arm을 선택해 초기 탐색을 분산한다.
        """
        scores = self.ucb_scores(context_vector)
        best_score = float(np.max(scores))
        tied = np.flatnonzero(
            np.isclose(scores, best_score, rtol=1e-12, atol=1e-12)
        )
        if tied.size == 1:
            return int(tied[0])
        return int(tied[np.argmin(self.update_counts[tied])])

    def allocation_weights(
        self,
        context_vector: Any,
        *,
        temperature: float = 0.25,
        min_weight: float = 0.0,
    ) -> np.ndarray:
        r"""
        UCB 점수를 자본 배분용 softmax 가중치로 변환한다.

            w_a = exp(score_a / T) / sum_j exp(score_j / T)

        min_weight를 지정하면 각 arm에 최소 탐색 비중을 남긴다.
        """
        temperature = float(temperature)
        min_weight = float(min_weight)

        if not np.isfinite(temperature) or temperature <= 0.0:
            raise ValueError("temperature must be finite and > 0.")
        if not np.isfinite(min_weight) or min_weight < 0.0:
            raise ValueError("min_weight must be finite and >= 0.")
        if min_weight * self.n_arms >= 1.0:
            raise ValueError("min_weight * n_arms must be < 1.")

        scaled = self.ucb_scores(context_vector) / temperature
        scaled -= np.max(scaled)  # 안정적 softmax
        exp_scores = np.exp(scaled)
        total = float(np.sum(exp_scores))

        if not np.isfinite(total) or total <= 0.0:
            weights = np.full(
                self.n_arms,
                1.0 / self.n_arms,
                dtype=self.dtype,
            )
        else:
            weights = exp_scores / total

        if min_weight > 0.0:
            residual = 1.0 - min_weight * self.n_arms
            weights = min_weight + residual * weights

        weights /= np.sum(weights)
        return np.asarray(weights, dtype=self.dtype)

    def update(
        self,
        arm_index: int,
        context_vector: Any,
        reward: float,
    ) -> None:
        r"""
        선택 arm의 Bayesian ridge 충분통계를 업데이트한다.

            A_a <- A_a + x x^T
            b_a <- b_a + reward * x

        Sherman-Morrison:

            A_new^-1 = A^-1
                       - (A^-1 x x^T A^-1)
                         / (1 + x^T A^-1 x)
        """
        arm = self._validate_arm_index(arm_index)
        context = self._validate_context(context_vector)

        try:
            observed_reward = float(reward)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("reward must be numeric.") from exc

        if not np.isfinite(observed_reward):
            raise ValueError("reward must be finite.")
        if self.reward_clip is not None:
            observed_reward = float(
                np.clip(
                    observed_reward,
                    -self.reward_clip,
                    self.reward_clip,
                )
            )

        old_inverse = self.A_inv[arm].copy()
        inverse_times_x = old_inverse @ context
        denominator = 1.0 + float(context @ inverse_times_x)

        self.A[arm] += np.outer(context, context)
        self.b[arm] += observed_reward * context

        if np.isfinite(denominator) and denominator > 1e-12:
            updated_inverse = (
                old_inverse
                - np.outer(inverse_times_x, inverse_times_x) / denominator
            )
            # 부동소수 연산이 만든 미세 비대칭 제거.
            self.A_inv[arm] = 0.5 * (
                updated_inverse + updated_inverse.T
            )
        else:
            # ridge가 있으므로 일반적으로 도달하지 않는 방어 경로.
            try:
                self.A_inv[arm] = np.linalg.inv(self.A[arm])
            except np.linalg.LinAlgError:
                self.A_inv[arm] = np.linalg.pinv(
                    self.A[arm],
                    hermitian=True,
                )

        self.update_counts[arm] += 1
        self.cumulative_rewards[arm] += observed_reward

    def reset_arm(self, arm_index: int) -> None:
        """특정 arm을 ridge prior 상태로 초기화한다."""
        arm = self._validate_arm_index(arm_index)
        identity = np.eye(self.context_dim, dtype=self.dtype)
        self.A[arm] = self.ridge_alpha * identity
        self.A_inv[arm] = identity / self.ridge_alpha
        self.b[arm].fill(0.0)
        self.update_counts[arm] = 0
        self.cumulative_rewards[arm] = 0.0


def test_contextual_linucb() -> None:
    """문맥별 선택, 가중치, ridge 안정성과 이상 입력을 테스트한다."""
    bandit = ContextualLinUCB(
        n_arms=3,
        context_dim=3,
        exploration_alpha=0.35,
        ridge_alpha=1.0,
        reward_clip=0.20,
    )

    low_vol_context = np.array([1.0, 0.0, 0.0])
    bear_context = np.array([0.0, 0.0, 1.0])

    for _ in range(40):
        bandit.update(0, low_vol_context, 0.08)
        bandit.update(1, low_vol_context, -0.03)
        bandit.update(2, low_vol_context, 0.01)

    for _ in range(40):
        bandit.update(0, bear_context, -0.04)
        bandit.update(1, bear_context, 0.01)
        bandit.update(2, bear_context, 0.10)

    assert bandit.select_arm(low_vol_context) == 0
    assert bandit.select_arm(bear_context) == 2

    weights = bandit.allocation_weights(
        bear_context,
        temperature=0.20,
        min_weight=0.05,
    )
    assert weights.shape == (3,)
    assert np.all(weights >= 0.05 - 1e-12)
    assert abs(float(np.sum(weights)) - 1.0) < 1e-12
    assert int(np.argmax(weights)) == 2

    eigenvalues = np.linalg.eigvalsh(bandit.A[0])
    assert np.all(eigenvalues > 0.0)
    assert np.all(np.isfinite(bandit.A_inv))

    before = float(bandit.cumulative_rewards[1])
    bandit.update(1, low_vol_context, 999.0)
    after = float(bandit.cumulative_rewards[1])
    assert abs((after - before) - 0.20) < 1e-12

    try:
        bandit.select_arm([1.0, 2.0])
        raise AssertionError("dimension mismatch was not rejected")
    except ValueError:
        pass

    try:
        bandit.update(0, low_vol_context, float("nan"))
        raise AssertionError("NaN reward was not rejected")
    except ValueError:
        pass

    print("test_contextual_linucb: all tests passed")
    print("low-vol scores:", bandit.ucb_scores(low_vol_context))
    print("bear scores   :", bandit.ucb_scores(bear_context))
    print("bear weights  :", weights)


if __name__ == "__main__":
    test_contextual_linucb()
