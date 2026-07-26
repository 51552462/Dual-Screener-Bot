from __future__ import annotations

import math
from typing import NamedTuple


class RoutingDecision(NamedTuple):
    """
    evaluate_routing()의 경량 불변 반환값.

    Attributes
    ----------
    route:
        "TAKER_IMMEDIATE", "MAKER_PASSIVE", "HYBRID_TWAP" 중 하나.
    urgency_score:
        즉시 체결 필요도 U ∈ [0, 1].
    impact_cost:
        예상 시장충격 비용 점수 C ∈ [0, 1].
    maker_ratio:
        지정가 주문 비율.
    taker_ratio:
        시장가/즉시체결 주문 비율.
    reason:
        라우팅 결정 또는 안전 폴백 사유.
    """

    route: str
    urgency_score: float
    impact_cost: float
    maker_ratio: float
    taker_ratio: float
    reason: str


class DynamicOrderRouter:
    """
    실시간 단일 틱/봉 단위의 동적 주문 라우터.

    Pandas/NumPy 및 외부 I/O를 사용하지 않는다. 모든 계산은 O(1)이다.

    Parameters
    ----------
    size_scale_usd:
        주문 규모를 0~1 시장충격 신호로 정규화하는 기준 금액.
        이 값과 같은 주문은 size_pressure = 1 - exp(-1) ≈ 0.632가 된다.
        종목의 ADV, 호가 깊이 또는 계좌 규모에 맞춰 조정하는 것이 바람직하다.
    atr_scale_pct:
        ATR%를 0~1 변동성 충격 신호로 정규화하는 기준값.
    hybrid_min_taker:
        HYBRID_TWAP에서 허용할 최소 Taker 비율.
    hybrid_max_taker:
        HYBRID_TWAP에서 허용할 최대 Taker 비율.

    Notes
    -----
    orderbook_imbalance의 부호는 사용자 명세를 따른다.

        +1.0: 매수잔량 우위
        -1.0: 매도잔량 우위

    단일 시점의 imbalance만으로는 "잔량이 급격히 줄어드는 속도"를 직접 측정할
    수 없다. 따라서 본 모델은 양(+)의 imbalance를 매수 압력 및 상대적 매도호가
    희소성의 스냅샷 대용치로 사용한다. 실제 잔량 감소 속도를 반영하려면 향후
    imbalance_delta 같은 시계열 입력을 추가하는 것이 더 정확하다.
    """

    __slots__ = (
        "size_scale_usd",
        "atr_scale_pct",
        "hybrid_min_taker",
        "hybrid_max_taker",
    )

    def __init__(
        self,
        *,
        size_scale_usd: float = 100_000.0,
        atr_scale_pct: float = 3.0,
        hybrid_min_taker: float = 0.10,
        hybrid_max_taker: float = 0.90,
    ) -> None:
        params = (
            size_scale_usd,
            atr_scale_pct,
            hybrid_min_taker,
            hybrid_max_taker,
        )
        if not all(self._is_finite_number(value) for value in params):
            raise ValueError("All constructor parameters must be finite numbers.")

        if size_scale_usd <= 0.0:
            raise ValueError("size_scale_usd must be greater than 0.")
        if atr_scale_pct <= 0.0:
            raise ValueError("atr_scale_pct must be greater than 0.")
        if not 0.0 <= hybrid_min_taker < hybrid_max_taker <= 1.0:
            raise ValueError(
                "Require 0 <= hybrid_min_taker < hybrid_max_taker <= 1."
            )

        self.size_scale_usd = float(size_scale_usd)
        self.atr_scale_pct = float(atr_scale_pct)
        self.hybrid_min_taker = float(hybrid_min_taker)
        self.hybrid_max_taker = float(hybrid_max_taker)

    @staticmethod
    def _is_finite_number(value: object) -> bool:
        """bool을 제외한 유한 실수 변환 가능 여부를 검사한다."""
        if isinstance(value, bool):
            return False
        try:
            return math.isfinite(float(value))
        except (TypeError, ValueError, OverflowError):
            return False

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        """value를 폐구간 [lower, upper]에 제한한다."""
        return max(lower, min(upper, float(value)))

    @staticmethod
    def _stable_sigmoid(value: float) -> float:
        """
        수치적으로 안정적인 Logistic 함수.

            sigmoid(z) = 1 / (1 + exp(-z))

        z가 큰 음수일 때 exp(-z)가 overflow하지 않도록 부호별로 계산한다.
        """
        if value >= 0.0:
            exp_neg = math.exp(-value)
            return 1.0 / (1.0 + exp_neg)

        exp_pos = math.exp(value)
        return exp_pos / (1.0 + exp_pos)

    @staticmethod
    def _fallback(reason: str) -> RoutingDecision:
        """
        비정상 입력에 대한 보수적 중립 폴백.

        시장가 또는 지정가 한쪽으로 잘못 몰리지 않게 50/50 HYBRID_TWAP을 반환한다.
        """
        return RoutingDecision(
            route="HYBRID_TWAP",
            urgency_score=0.5,
            impact_cost=0.5,
            maker_ratio=0.5,
            taker_ratio=0.5,
            reason=reason,
        )

    def _validate_inputs(
        self,
        signal_score: object,
        atr_pct: object,
        orderbook_imbalance: object,
        order_size_usd: object,
    ) -> tuple[float, float, float, float] | None:
        """
        입력 범위를 검사하고 float 튜플로 변환한다.

        signal_score=0과 음수 imbalance는 명세상 정상 범위이므로 허용한다.
        ATR과 주문금액은 0 이하일 경우 경제적으로 유효한 라우팅 계산이 불가능해
        안전 폴백 대상으로 처리한다.
        """
        values = (
            signal_score,
            atr_pct,
            orderbook_imbalance,
            order_size_usd,
        )
        if not all(self._is_finite_number(value) for value in values):
            return None

        signal = float(signal_score)
        atr = float(atr_pct)
        imbalance = float(orderbook_imbalance)
        size = float(order_size_usd)

        if not 0.0 <= signal <= 10.0:
            return None
        if atr <= 0.0:
            return None
        if not -1.0 <= imbalance <= 1.0:
            return None
        if size <= 0.0:
            return None

        return signal, atr, imbalance, size

    def _compute_urgency(
        self,
        signal_score: float,
        orderbook_imbalance: float,
    ) -> float:
        r"""
        Urgency Score U ∈ [0, 1].

        1) 시그널 정규화:
               s = signal_score / 10

        2) Logistic logit:
               z = 4(s - 0.55)
                   + 1.8 I
                   + 0.8 s max(I, 0)

           I는 orderbook_imbalance이다.

        3) 최종 긴급도:
               U = 1 / (1 + exp(-z))

        해석:
        - 강한 시그널일수록 U 증가.
        - 양(+)의 호가 불균형, 즉 매수잔량 우위가 강할수록 U 증가.
        - 강한 시그널과 양의 불균형이 동시에 나타나면 교호항이 추가 상승시킨다.
        - 강한 매도잔량 우위에서는 무리한 Taker 진입을 억제한다.
        """
        signal_norm = self._clamp(signal_score / 10.0, 0.0, 1.0)
        positive_book_pressure = max(orderbook_imbalance, 0.0)

        logit = (
            4.0 * (signal_norm - 0.55)
            + 1.8 * orderbook_imbalance
            + 0.8 * signal_norm * positive_book_pressure
        )

        return self._clamp(self._stable_sigmoid(logit), 0.0, 1.0)

    def _compute_impact_cost(
        self,
        atr_pct: float,
        order_size_usd: float,
    ) -> float:
        r"""
        Market Impact Cost C ∈ [0, 1].

        주문 규모와 ATR%를 지수 포화 함수로 정규화한다.

            S = 1 - exp(-order_size_usd / size_scale_usd)
            V = 1 - exp(-atr_pct / atr_scale_pct)

        결합식:

            C = 0.60 S + 0.30 V + 0.10 S V

        특징:
        - 주문 규모 또는 변동성이 커질수록 단조 증가한다.
        - S·V가 매우 커져도 1을 넘지 않는다.
        - 교호항 S·V는 대형 주문과 고변동성이 동시에 발생할 때 추가 비용을 반영한다.
        """
        size_pressure = 1.0 - math.exp(
            -order_size_usd / self.size_scale_usd
        )
        volatility_pressure = 1.0 - math.exp(
            -atr_pct / self.atr_scale_pct
        )

        impact = (
            0.60 * size_pressure
            + 0.30 * volatility_pressure
            + 0.10 * size_pressure * volatility_pressure
        )

        return self._clamp(impact, 0.0, 1.0)

    def _compute_hybrid_ratios(
        self,
        urgency_score: float,
        impact_cost: float,
    ) -> tuple[float, float]:
        r"""
        중립 구간의 Maker/Taker 비율.

            T_raw = 0.5
                    + 0.80(U - 0.5)
                    - 0.70(C - 0.5)

        - U가 높으면 즉시 체결 비율을 높인다.
        - C가 높으면 시장충격을 줄이기 위해 지정가 비율을 높인다.
        - 최종 Taker 비율은 설정된 안전 구간으로 클램핑한다.

            T = clip(T_raw, T_min, T_max)
            M = 1 - T
        """
        taker_raw = (
            0.5
            + 0.80 * (urgency_score - 0.5)
            - 0.70 * (impact_cost - 0.5)
        )

        taker_ratio = self._clamp(
            taker_raw,
            self.hybrid_min_taker,
            self.hybrid_max_taker,
        )
        maker_ratio = 1.0 - taker_ratio
        return maker_ratio, taker_ratio

    def evaluate_routing(
        self,
        signal_score: float,
        atr_pct: float,
        orderbook_imbalance: float,
        order_size_usd: float,
    ) -> RoutingDecision:
        """
        현재 시장 상태를 평가하여 주문 라우팅 결정을 반환한다.

        Routing rules
        -------------
        1. U >= 0.75
           -> TAKER_IMMEDIATE, 시장가/즉시체결 100%

        2. U < 0.35 and C >= 0.50
           -> MAKER_PASSIVE, 지정가 100%

        3. 그 외
           -> HYBRID_TWAP, U와 C에 따라 Maker/Taker 비율 동적 산출

        잘못된 값, NaN/Inf, 범위 밖 입력, ATR<=0, 주문금액<=0은
        HYBRID_TWAP 50/50으로 안전하게 폴백한다.
        """
        parsed = self._validate_inputs(
            signal_score,
            atr_pct,
            orderbook_imbalance,
            order_size_usd,
        )
        if parsed is None:
            return self._fallback("invalid_or_non_finite_input")

        signal, atr, imbalance, size = parsed

        urgency = self._compute_urgency(signal, imbalance)
        impact = self._compute_impact_cost(atr, size)

        if urgency >= 0.75:
            return RoutingDecision(
                route="TAKER_IMMEDIATE",
                urgency_score=round(urgency, 6),
                impact_cost=round(impact, 6),
                maker_ratio=0.0,
                taker_ratio=1.0,
                reason="urgency_at_or_above_0.75",
            )

        if urgency < 0.35 and impact >= 0.50:
            return RoutingDecision(
                route="MAKER_PASSIVE",
                urgency_score=round(urgency, 6),
                impact_cost=round(impact, 6),
                maker_ratio=1.0,
                taker_ratio=0.0,
                reason="low_urgency_and_high_impact",
            )

        maker_ratio, taker_ratio = self._compute_hybrid_ratios(
            urgency,
            impact,
        )

        # 반올림 뒤에도 합이 정확히 1.0이 되도록 Taker를 먼저 반올림하고
        # Maker를 1 - Taker로 계산한다.
        taker_ratio = round(taker_ratio, 6)
        maker_ratio = round(1.0 - taker_ratio, 6)

        return RoutingDecision(
            route="HYBRID_TWAP",
            urgency_score=round(urgency, 6),
            impact_cost=round(impact, 6),
            maker_ratio=maker_ratio,
            taker_ratio=taker_ratio,
            reason="balanced_urgency_and_impact",
        )


def test_dynamic_order_router() -> None:
    """주요 라우팅 및 수치 안정성 시나리오를 검증한다."""
    router = DynamicOrderRouter()

    # 1. 강한 시그널 + 매수잔량 우위: 긴급 즉시 체결
    urgent = router.evaluate_routing(
        signal_score=9.5,
        atr_pct=1.2,
        orderbook_imbalance=0.80,
        order_size_usd=50_000.0,
    )
    assert urgent.route == "TAKER_IMMEDIATE"
    assert urgent.urgency_score >= 0.75
    assert urgent.maker_ratio == 0.0
    assert urgent.taker_ratio == 1.0

    # 2. 낮은 긴급도 + 대형 주문 + 고변동성: 지정가 수동 체결
    passive = router.evaluate_routing(
        signal_score=1.5,
        atr_pct=6.0,
        orderbook_imbalance=-0.30,
        order_size_usd=500_000.0,
    )
    assert passive.route == "MAKER_PASSIVE"
    assert passive.urgency_score < 0.35
    assert passive.impact_cost >= 0.50
    assert passive.maker_ratio == 1.0
    assert passive.taker_ratio == 0.0

    # 3. 중립 상태: Hybrid TWAP
    hybrid = router.evaluate_routing(
        signal_score=5.5,
        atr_pct=2.0,
        orderbook_imbalance=0.05,
        order_size_usd=40_000.0,
    )
    assert hybrid.route == "HYBRID_TWAP"
    assert 0.0 < hybrid.maker_ratio < 1.0
    assert 0.0 < hybrid.taker_ratio < 1.0
    assert abs(hybrid.maker_ratio + hybrid.taker_ratio - 1.0) < 1e-12

    # 4. 충격 비용이 커질수록 Hybrid의 Maker 비율이 증가해야 한다.
    low_impact = router.evaluate_routing(
        signal_score=6.0,
        atr_pct=0.8,
        orderbook_imbalance=0.0,
        order_size_usd=10_000.0,
    )
    higher_impact = router.evaluate_routing(
        signal_score=6.0,
        atr_pct=4.0,
        orderbook_imbalance=0.0,
        order_size_usd=150_000.0,
    )
    assert low_impact.route == "HYBRID_TWAP"
    assert higher_impact.route == "HYBRID_TWAP"
    assert higher_impact.impact_cost > low_impact.impact_cost
    assert higher_impact.maker_ratio > low_impact.maker_ratio

    # 5. 음수 imbalance는 정상 입력이다.
    valid_negative_imbalance = router.evaluate_routing(
        signal_score=4.0,
        atr_pct=1.5,
        orderbook_imbalance=-0.5,
        order_size_usd=20_000.0,
    )
    assert valid_negative_imbalance.reason != "invalid_or_non_finite_input"
    assert 0.0 <= valid_negative_imbalance.urgency_score <= 1.0

    # 6. signal_score=0은 명시된 정상 범위이다.
    zero_signal = router.evaluate_routing(
        signal_score=0.0,
        atr_pct=1.0,
        orderbook_imbalance=0.0,
        order_size_usd=10_000.0,
    )
    assert zero_signal.reason != "invalid_or_non_finite_input"

    # 7. NaN은 반드시 50/50 폴백.
    nan_fallback = router.evaluate_routing(
        signal_score=float("nan"),
        atr_pct=2.0,
        orderbook_imbalance=0.0,
        order_size_usd=10_000.0,
    )
    assert nan_fallback == RoutingDecision(
        "HYBRID_TWAP",
        0.5,
        0.5,
        0.5,
        0.5,
        "invalid_or_non_finite_input",
    )

    # 8. Inf도 반드시 50/50 폴백.
    inf_fallback = router.evaluate_routing(
        signal_score=5.0,
        atr_pct=float("inf"),
        orderbook_imbalance=0.0,
        order_size_usd=10_000.0,
    )
    assert inf_fallback.route == "HYBRID_TWAP"
    assert inf_fallback.maker_ratio == 0.5
    assert inf_fallback.taker_ratio == 0.5

    # 9. 주문금액 0 이하 및 ATR 0 이하는 폴백.
    zero_size = router.evaluate_routing(
        signal_score=5.0,
        atr_pct=2.0,
        orderbook_imbalance=0.0,
        order_size_usd=0.0,
    )
    zero_atr = router.evaluate_routing(
        signal_score=5.0,
        atr_pct=0.0,
        orderbook_imbalance=0.0,
        order_size_usd=10_000.0,
    )
    assert zero_size.maker_ratio == zero_size.taker_ratio == 0.5
    assert zero_atr.maker_ratio == zero_atr.taker_ratio == 0.5

    # 10. 명세 범위를 벗어난 imbalance는 폴백.
    invalid_imbalance = router.evaluate_routing(
        signal_score=5.0,
        atr_pct=2.0,
        orderbook_imbalance=1.1,
        order_size_usd=10_000.0,
    )
    assert invalid_imbalance.reason == "invalid_or_non_finite_input"

    # 11. 극단적으로 큰 유한 주문도 overflow 없이 [0,1]에 머문다.
    huge_order = router.evaluate_routing(
        signal_score=5.0,
        atr_pct=100_000.0,
        orderbook_imbalance=0.0,
        order_size_usd=1e308,
    )
    assert 0.0 <= huge_order.urgency_score <= 1.0
    assert 0.0 <= huge_order.impact_cost <= 1.0
    assert abs(huge_order.maker_ratio + huge_order.taker_ratio - 1.0) < 1e-12

    print("test_dynamic_order_router: all tests passed")
    print("urgent :", urgent)
    print("passive:", passive)
    print("hybrid :", hybrid)


if __name__ == "__main__":
    test_dynamic_order_router()
