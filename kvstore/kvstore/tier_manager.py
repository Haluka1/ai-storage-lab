from __future__ import annotations

import threading
from dataclasses import replace

from .cost_model import CostModel, RequestContext
from .errors import BlockNotFound, KVStoreError, TierUnavailable
from .interface import Tier
from .metadata import BlockKey, BlockLocation, KVMetadata, LoadResult, StoreResult, TierName
from .metadata_store import MetadataStore
from .metrics import KVStoreMetrics
from .prefetch import Prefetcher
from .s3_fault_injection import is_timeout_exception


class MultiTierKVBlockStore:
    def __init__(self, tiers: list[Tier], metadata_store: MetadataStore, cost_model: CostModel, metrics: KVStoreMetrics | None = None):
        if not tiers:
            raise ValueError("at least one tier is required")
        self.tiers = {tier.name: tier for tier in tiers}
        if len(self.tiers) != len(tiers):
            raise ValueError("tier names must be unique")
        self.metadata_store = metadata_store
        self.cost_model = cost_model
        self.metrics = metrics
        self.lookup_order = [
            tier
            for tier in (TierName.MEMORY, TierName.NVME, TierName.S3)
            if tier in self.tiers
        ]
        self._prefetcher = Prefetcher(self)
        self._close_lock = threading.Lock()
        self._closed = False

    def lookup(self, key: BlockKey) -> BlockLocation | None:
        for tier_name in self.lookup_order:
            loc = self.tiers[tier_name].lookup(key)
            if loc is not None:
                return loc
        return None

    def locations(self, key: BlockKey) -> list[BlockLocation]:
        out: list[BlockLocation] = []
        for tier_name in self.lookup_order:
            loc = self.tiers[tier_name].lookup(key)
            if loc is not None:
                out.append(loc)
        return out

    def store(self, key: BlockKey, data: bytes, metadata: KVMetadata, preferred_tier: TierName | None = None) -> StoreResult:
        tier_name = preferred_tier or self.lookup_order[0]
        if tier_name not in self.tiers:
            raise BlockNotFound(f"tier_unavailable:{tier_name.value}")
        return self.tiers[tier_name].store(key, data, metadata)

    def load(self, key: BlockKey, target_tier: TierName = TierName.MEMORY, slo_budget_ms: float | None = None) -> LoadResult:
        if target_tier in self.tiers and self.tiers[target_tier].contains(key):
            return self.tiers[target_tier].load(key)
        locations = self.locations(key)
        if not locations:
            raise BlockNotFound(key.block_hash)
        ctx = self._request_context(key, locations)
        decision = self.cost_model.decide(locations, ctx, slo_budget_ms=slo_budget_ms)
        self._metric(lambda m: m.kv_onload_decision_total.inc(tier=decision.tier.value, decision=decision.action, strategy="cost_based"))
        if decision.action == "prefetch":
            submission = self._prefetcher.submit(key, target_tier)
            self._metric(
                lambda m: m.kv_prefetch_total.inc(
                    tier=target_tier.value, outcome=submission.reason
                )
            )
            raise BlockNotFound(f"cost_model_selected_prefetch:{submission.reason}")
        if decision.action != "load":
            raise BlockNotFound("cost_model_selected_recompute")
        source = self.tiers[decision.tier]
        try:
            result = source.load(key)
        except TierUnavailable as exc:
            if decision.tier == TierName.S3:
                if is_timeout_exception(exc):
                    self._metric(lambda m: m.kv_onload_timeout_total.inc(tier=decision.tier.value, operation="load", outcome="timeout"))
                self._metric(lambda m: m.kv_onload_fallback_total.inc(tier=decision.tier.value, decision="recompute", reason_class="tier_unavailable"))
                raise BlockNotFound("s3_load_unavailable_recompute") from exc
            raise
        if target_tier in self.tiers and target_tier != result.tier:
            self._store_promotion(target_tier, result)
        return result

    def prefetch(self, keys: list[BlockKey], target_tier: TierName = TierName.MEMORY) -> None:
        for key in keys:
            self._metric(lambda m: m.kv_prefetch_total.inc(tier=target_tier.value, outcome="requested"))
            if target_tier in self.tiers and self.tiers[target_tier].contains(key):
                continue
            for loc in self.locations(key):
                if loc.tier != target_tier and self._promote_from_tier(key, loc.tier, target_tier, suppress_errors=True):
                    break

    def evict(self, key: BlockKey, tier: TierName | None = None) -> bool:
        if tier is not None:
            return tier in self.tiers and self.tiers[tier].evict(key)
        removed = False
        for item in self.tiers.values():
            removed = item.evict(key) or removed
        return removed

    def promote(self, key: BlockKey, target_tier: TierName = TierName.MEMORY) -> bool:
        try:
            self.load(key, target_tier=target_tier)
            return target_tier in self.tiers and self.tiers[target_tier].contains(key)
        except BlockNotFound:
            return False

    def demote(self, key: BlockKey, source_tier: TierName, target_tier: TierName) -> bool:
        if source_tier not in self.tiers or target_tier not in self.tiers:
            return False
        result = self.tiers[source_tier].load(key)
        self._store_promotion(target_tier, result)
        return self.tiers[source_tier].evict(key)

    def release(self, key: BlockKey) -> None:
        for tier_name in self.lookup_order:
            self.metadata_store.release(key, tier_name)

    def stats(self) -> dict:
        return {
            "tiers": {tier.value: self.tiers[tier].stats() for tier in self.lookup_order},
            "prefetch": self._prefetcher.stats(),
        }

    def close(self, wait: bool = True) -> None:
        with self._close_lock:
            if self._closed:
                return
            # The SQLite owner cannot be released while a background prefetch
            # may still use it.  A safe full close therefore always quiesces
            # executor workers; ``wait`` remains accepted for API compatibility.
            _ = wait
            self._prefetcher.shutdown(wait=True)
            self.metadata_store.close()
            self._closed = True

    def _request_context(self, key: BlockKey, locations: list[BlockLocation]) -> RequestContext:
        tokens = 0
        for loc in locations:
            metadata = self.metadata_store.get_metadata(key, loc.tier)
            if metadata is not None:
                tokens = max(tokens, metadata.tokens)
        return RequestContext(missing_prefill_tokens=tokens)

    def _promote_from_tier(self, key: BlockKey, source_tier: TierName, target_tier: TierName, suppress_errors: bool) -> bool:
        if source_tier not in self.tiers or target_tier not in self.tiers or source_tier == target_tier:
            return False
        try:
            result = self.tiers[source_tier].load(key)
            self._store_promotion(target_tier, result)
            return True
        except (KVStoreError, OSError):
            if suppress_errors:
                return False
            raise

    def _store_promotion(self, target_tier: TierName, result: LoadResult) -> None:
        promoted_meta = replace(result.metadata)
        self.tiers[target_tier].store(result.key, result.data, promoted_meta)

    def _metric(self, fn) -> None:
        if self.metrics is None:
            return
        try:
            fn(self.metrics)
        except Exception:
            pass
