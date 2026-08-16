from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path

from kvstore.admission import CostAwareAdmission, MinReuseAdmission
from kvstore.cost_model import CostModel, RequestContext, TierProfile
from kvstore.eviction import LRUEvictionController
from kvstore.memory_tier import MemoryTier
from kvstore.metadata import BlockKey, BlockLocation, KVMetadata, TierName
from kvstore.metadata_store import MetadataStore
from kvstore.prefetch import Prefetcher


class AdmissionEvictionPrefetchTest(unittest.TestCase):
    def test_min_reuse_admission(self) -> None:
        key = _key("a")
        meta = KVMetadata(key, "bf16", 1, 1, 1, 16, 4, reuse_count=1)
        self.assertTrue(MinReuseAdmission(1).admit(meta).admit)
        self.assertFalse(MinReuseAdmission(2).admit(meta).admit)

    def test_cost_aware_admission(self) -> None:
        key = _key("b")
        loc = BlockLocation(key, TierName.NVME, "x", 1024 * 1024, "", 0, 0)
        cm = CostModel({TierName.NVME: TierProfile(0.1, 5.0)})
        decision = CostAwareAdmission(cm).admit([loc], RequestContext(missing_prefill_tokens=4096))
        self.assertTrue(decision.admit)

    def test_lru_eviction_skips_inflight(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = MemoryTier(16, meta)
            key1 = _key("c")
            key2 = _key("d")
            tier.store(key1, b"1234", KVMetadata(key1, "bf16", 1, 1, 1, 16, 4))
            tier.store(key2, b"5678", KVMetadata(key2, "bf16", 1, 1, 1, 16, 4))
            meta.acquire(key1, TierName.MEMORY)
            result = LRUEvictionController(meta, {TierName.MEMORY: tier}).evict_bytes(TierName.MEMORY, 4)
            self.assertEqual(result.evicted_blocks, 1)
            self.assertTrue(tier.contains(key1))
            meta.release(key1, TierName.MEMORY)

    def test_prefetcher_dedup_queue_and_exception_release(self) -> None:
        key1 = _key("e")
        key2 = _key("f")
        gate = threading.Event()
        store = _FakePrefetchStore(gate)
        prefetcher = Prefetcher(store, max_workers=1, max_queue=1)
        try:
            self.assertTrue(prefetcher.submit(key1).submitted)
            self.assertFalse(prefetcher.submit(key1).submitted)
            self.assertFalse(prefetcher.submit(key2).submitted)
            gate.set()
            for _ in range(50):
                if prefetcher.stats()["pending"] == 0:
                    break
                time.sleep(0.01)
            self.assertEqual(prefetcher.stats()["pending"], 0)
            self.assertTrue(prefetcher.submit(key2).submitted)
        finally:
            gate.set()
            prefetcher.shutdown()


class _FakePrefetchStore:
    def __init__(self, gate: threading.Event):
        self.gate = gate
        self.calls = 0

    def prefetch(self, keys: list[BlockKey], target_tier: TierName) -> None:
        self.calls += 1
        self.gate.wait(timeout=1.0)
        if keys[0].block_hash.startswith("f"):
            raise RuntimeError("boom")


def _key(seed: str) -> BlockKey:
    return BlockKey("t", "m", "r", "tok", seed * 64)


if __name__ == "__main__":
    unittest.main()
