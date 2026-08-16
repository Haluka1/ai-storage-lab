from __future__ import annotations

from dataclasses import dataclass

from .metadata import BlockLocation, TierName


@dataclass
class TierProfile:
    fixed_latency_ms: float
    bandwidth_gbps: float
    deserialize_ms_per_mb: float = 0.02
    h2d_bandwidth_gbps: float = 24.0

    def estimate_load_ms(self, bytes_: int, queue_wait_ms: float = 0.0) -> float:
        mb = bytes_ / (1024 * 1024)
        gb = bytes_ / (1024**3)
        transfer_ms = gb / max(self.bandwidth_gbps, 1e-9) * 1000
        deserialize_ms = mb * self.deserialize_ms_per_mb
        h2d_ms = gb / max(self.h2d_bandwidth_gbps, 1e-9) * 1000
        return self.fixed_latency_ms + transfer_ms + deserialize_ms + h2d_ms + queue_wait_ms


@dataclass
class RequestContext:
    missing_prefill_tokens: int = 0
    measured_prefill_ms_per_token: float = 0.08
    worker_queue_wait_ms: float = 0.0
    reuse_probability: float = 0.5
    is_reuse_heavy: bool = False


@dataclass
class Decision:
    action: str
    tier: TierName
    estimated_load_ms: float
    estimated_recompute_ms: float
    benefit_ms: float
    reason: str


class CostModel:
    def __init__(
        self,
        profiles: dict[TierName, TierProfile],
        load_benefit_threshold_ms: float = 5.0,
        s3_load_benefit_threshold_ms: float = 50.0,
        slo_budget_guard_ms: float = 100.0,
        s3_min_missing_prefill_tokens: int = 1024,
        s3_min_reuse_probability: float = 0.75,
    ):
        self.profiles = profiles
        self.load_benefit_threshold_ms = load_benefit_threshold_ms
        self.s3_load_benefit_threshold_ms = s3_load_benefit_threshold_ms
        self.slo_budget_guard_ms = slo_budget_guard_ms
        self.s3_min_missing_prefill_tokens = s3_min_missing_prefill_tokens
        self.s3_min_reuse_probability = s3_min_reuse_probability

    def estimate_recompute_ms(self, ctx: RequestContext) -> float:
        return ctx.missing_prefill_tokens * ctx.measured_prefill_ms_per_token

    def can_sync_load_from_s3(self, ctx: RequestContext, load_ms: float, slo_budget_ms: float | None = None) -> bool:
        if ctx.missing_prefill_tokens < self.s3_min_missing_prefill_tokens:
            return False
        if ctx.reuse_probability < self.s3_min_reuse_probability and not ctx.is_reuse_heavy:
            return False
        if slo_budget_ms is not None and load_ms + self.slo_budget_guard_ms > slo_budget_ms:
            return False
        return True

    def decide(self, locations: list[BlockLocation], ctx: RequestContext, slo_budget_ms: float | None = None) -> Decision:
        if not locations:
            return Decision("recompute", TierName.MEMORY, 0.0, self.estimate_recompute_ms(ctx), 0.0, "block_not_found")
        recompute_ms = self.estimate_recompute_ms(ctx)
        decisions: list[Decision] = []
        for loc in locations:
            profile = self.profiles[loc.tier]
            load_ms = profile.estimate_load_ms(loc.bytes, ctx.worker_queue_wait_ms)
            benefit = recompute_ms - load_ms
            threshold = self.s3_load_benefit_threshold_ms if loc.tier == TierName.S3 else self.load_benefit_threshold_ms
            if benefit <= threshold:
                action = "recompute"
                reason = "recompute_better_than_load"
            elif slo_budget_ms is not None and load_ms + self.slo_budget_guard_ms > slo_budget_ms:
                action = "prefetch" if loc.tier == TierName.S3 else "recompute"
                reason = "s3_cold_tier_prefetch_only" if loc.tier == TierName.S3 else "slo_budget_not_enough"
            elif loc.tier == TierName.S3 and not self.can_sync_load_from_s3(ctx, load_ms, slo_budget_ms):
                action = "prefetch"
                reason = "s3_cold_tier_prefetch_only"
            else:
                action = "load"
                reason = "load_benefit_positive"
            decisions.append(Decision(action, loc.tier, load_ms, recompute_ms, benefit, reason))
        priority = {"load": 0, "prefetch": 1, "recompute": 2}
        decisions.sort(key=lambda d: (priority[d.action], -d.benefit_ms, d.estimated_load_ms))
        return decisions[0]
