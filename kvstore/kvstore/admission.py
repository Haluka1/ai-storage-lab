from __future__ import annotations

from dataclasses import dataclass

from .cost_model import CostModel, RequestContext
from .metadata import BlockLocation, KVMetadata


@dataclass(frozen=True)
class AdmissionDecision:
    admit: bool
    reason: str


class MinReuseAdmission:
    def __init__(self, min_reuse_count: int = 1):
        if min_reuse_count < 0:
            raise ValueError("min_reuse_count must be non-negative")
        self.min_reuse_count = min_reuse_count

    def admit(self, metadata: KVMetadata) -> AdmissionDecision:
        if metadata.reuse_count >= self.min_reuse_count:
            return AdmissionDecision(True, "reuse_count_threshold_met")
        return AdmissionDecision(False, "reuse_count_below_threshold")


class CostAwareAdmission:
    def __init__(self, cost_model: CostModel, min_benefit_ms: float = 0.0):
        self.cost_model = cost_model
        self.min_benefit_ms = min_benefit_ms

    def admit(self, locations: list[BlockLocation], ctx: RequestContext, slo_budget_ms: float | None = None) -> AdmissionDecision:
        decision = self.cost_model.decide(locations, ctx, slo_budget_ms=slo_budget_ms)
        if decision.action in {"load", "prefetch"} and decision.benefit_ms >= self.min_benefit_ms:
            return AdmissionDecision(True, f"cost_model_{decision.reason}")
        return AdmissionDecision(False, f"cost_model_{decision.reason}")
