from __future__ import annotations

from dataclasses import dataclass

from .interface import Tier
from .metadata import TierName
from .metadata_store import MetadataStore


@dataclass(frozen=True)
class EvictionResult:
    tier: TierName
    requested_bytes: int
    evicted_blocks: int
    evicted_bytes: int
    still_over_bytes: int


class LRUEvictionController:
    def __init__(self, metadata_store: MetadataStore, tiers: dict[TierName, Tier]):
        self.metadata_store = metadata_store
        self.tiers = tiers

    def evict_bytes(self, tier: TierName, bytes_to_free: int, batch_size: int = 64) -> EvictionResult:
        if bytes_to_free <= 0:
            return EvictionResult(tier, bytes_to_free, 0, 0, 0)
        if tier not in self.tiers:
            return EvictionResult(tier, bytes_to_free, 0, 0, bytes_to_free)
        evicted_blocks = 0
        evicted_bytes = 0
        while evicted_bytes < bytes_to_free:
            candidates = self.metadata_store.lru_candidates(tier, batch_size)
            if not candidates:
                break
            progressed = False
            for loc in candidates:
                if self.tiers[tier].evict(loc.key):
                    evicted_blocks += 1
                    evicted_bytes += loc.bytes
                    progressed = True
                    if evicted_bytes >= bytes_to_free:
                        break
            if not progressed:
                break
        return EvictionResult(tier, bytes_to_free, evicted_blocks, evicted_bytes, max(bytes_to_free - evicted_bytes, 0))
