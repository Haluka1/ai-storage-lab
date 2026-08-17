from __future__ import annotations

import hashlib
import io
import tempfile
import threading
import time
import unittest
from pathlib import Path

from kvstore.cost_model import CostModel, Decision, TierProfile
from kvstore.errors import BlockNotFound, TierUnavailable
from kvstore.memory_tier import MemoryTier
from kvstore.metadata import BlockKey, KVMetadata, TierName
from kvstore.metadata_store import MetadataStore
from kvstore.metrics import KVStoreMetrics
from kvstore.nvme_tier import NVMeTier
from kvstore.s3_fault_injection import FaultInjectingS3Client, S3FaultInjectionConfig
from kvstore.s3_tier import S3Tier
from kvstore.tier_manager import MultiTierKVBlockStore


class TierManagerTest(unittest.TestCase):
    def test_requires_at_least_one_tier(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            with self.assertRaisesRegex(ValueError, "at least one tier"):
                MultiTierKVBlockStore([], meta, CostModel({}))

    def test_close_is_idempotent_and_releases_metadata_owner(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = _store(root)

            store.close()
            store.close(wait=False)

            reopened = MetadataStore(root / "meta.sqlite3")
            reopened.close()

    def test_cost_model_prefetch_is_non_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = _store(Path(td))
            key = _key("z")
            store.store(
                key,
                b"payload",
                _metadata(key, 4096, 7),
                preferred_tier=TierName.NVME,
            )
            store.cost_model = _PrefetchOnlyCostModel()
            nvme = store.tiers[TierName.NVME]
            original_load = nvme.load
            started = threading.Event()
            release = threading.Event()

            def slow_load(load_key):
                started.set()
                if not release.wait(2):
                    raise TimeoutError("test prefetch release timed out")
                return original_load(load_key)

            nvme.load = slow_load
            try:
                begin = time.perf_counter()
                with self.assertRaisesRegex(BlockNotFound, "submitted"):
                    store.load(key, target_tier=TierName.MEMORY)
                self.assertLess(time.perf_counter() - begin, 0.2)
                self.assertTrue(started.wait(1))
                self.assertFalse(store.tiers[TierName.MEMORY].contains(key))
                release.set()
                deadline = time.monotonic() + 2
                while (
                    not store.tiers[TierName.MEMORY].contains(key)
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.01)
                self.assertTrue(store.tiers[TierName.MEMORY].contains(key))
            finally:
                release.set()
                store.close()

    def test_async_prefetch_respects_requested_target_tier(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meta = MetadataStore(root / "meta.sqlite3")
            client = _FakeS3Client()
            memory = MemoryTier(1024, meta)
            nvme = NVMeTier(root / "nvme", 1024, meta)
            s3 = S3Tier("bucket", "blocks", meta, client=client)
            store = MultiTierKVBlockStore(
                [memory, nvme, s3],
                meta,
                _PrefetchOnlyCostModel(TierName.S3),
            )
            key = _key("target")
            store.store(
                key,
                b"payload",
                _metadata(key, 4096, 7),
                preferred_tier=TierName.S3,
            )
            try:
                with self.assertRaisesRegex(BlockNotFound, "submitted"):
                    store.load(key, target_tier=TierName.NVME)
                deadline = time.monotonic() + 2
                while not nvme.contains(key) and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(nvme.contains(key))
                self.assertFalse(memory.contains(key))
            finally:
                store.close()

    def test_nvme_load_promotes_to_memory(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = _store(Path(td))
            key = _key("a")
            store.store(key, b"payload", _metadata(key, 4096, 7), preferred_tier=TierName.NVME)
            result = store.load(key, target_tier=TierName.MEMORY)
            self.assertEqual(result.data, b"payload")
            self.assertTrue(store.tiers[TierName.MEMORY].contains(key))

    def test_cost_model_can_skip_short_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = _store(Path(td))
            key = _key("b")
            store.store(key, b"payload", _metadata(key, 1, 7), preferred_tier=TierName.NVME)
            with self.assertRaises(BlockNotFound):
                store.load(key, target_tier=TierName.MEMORY)
            self.assertFalse(store.tiers[TierName.MEMORY].contains(key))

    def test_prefetch_promotes_without_cost_model_recursion(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = _store(Path(td))
            key = _key("p")
            store.store(key, b"payload", _metadata(key, 1, 7), preferred_tier=TierName.NVME)
            store.prefetch([key], target_tier=TierName.MEMORY)
            self.assertTrue(store.tiers[TierName.MEMORY].contains(key))

    def test_evict_all_tiers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            store = _store(Path(td))
            key = _key("c")
            store.store(key, b"payload", _metadata(key, 4096, 7), preferred_tier=TierName.NVME)
            store.load(key, target_tier=TierName.MEMORY)
            self.assertTrue(store.evict(key))
            self.assertIsNone(store.lookup(key))

    def test_s3_timeout_falls_back_to_recompute(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meta = MetadataStore(root / "meta.sqlite3")
            metrics = KVStoreMetrics()
            client = FaultInjectingS3Client(
                _FakeS3Client(),
                S3FaultInjectionConfig(timeout_rate=1.0, operations=("get_object",), seed=7),
            )
            memory = MemoryTier(1024 * 1024, meta, metrics=metrics)
            s3 = S3Tier("bucket", "blocks", meta, client=client, metrics=metrics)
            store = MultiTierKVBlockStore(
                [memory, s3],
                meta,
                CostModel(
                    {TierName.MEMORY: TierProfile(0.01, 80.0), TierName.S3: TierProfile(1.0, 10.0)},
                    load_benefit_threshold_ms=0.0,
                    s3_load_benefit_threshold_ms=0.0,
                    s3_min_missing_prefill_tokens=1,
                    s3_min_reuse_probability=0.0,
                ),
                metrics=metrics,
            )
            key = _key("s")
            store.store(key, b"payload", _metadata(key, 4096, 7), preferred_tier=TierName.S3)
            with self.assertRaises(BlockNotFound) as ctx:
                store.load(key, target_tier=TierName.MEMORY, slo_budget_ms=1000.0)
            self.assertIn("s3_load_unavailable_recompute", str(ctx.exception))
            self.assertFalse(memory.contains(key))
            self.assertEqual(sum(metrics.kv_onload_timeout_total.values.values()), 1.0)
            self.assertEqual(sum(metrics.kv_onload_fallback_total.values.values()), 1.0)

    def test_s3_head_timeout_falls_back_to_recompute(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meta = MetadataStore(root / "meta.sqlite3")
            metrics = KVStoreMetrics()
            base_client = _FakeS3Client()
            memory = MemoryTier(1024 * 1024, meta, metrics=metrics)
            s3 = S3Tier("bucket", "blocks", meta, client=base_client, metrics=metrics)
            store = MultiTierKVBlockStore(
                [memory, s3],
                meta,
                CostModel(
                    {
                        TierName.MEMORY: TierProfile(0.01, 80.0),
                        TierName.S3: TierProfile(1.0, 10.0),
                    }
                ),
                metrics=metrics,
            )
            key = _key("head-timeout")
            store.store(
                key,
                b"payload",
                _metadata(key, 4096, 7),
                preferred_tier=TierName.S3,
            )
            s3.client = FaultInjectingS3Client(
                base_client,
                S3FaultInjectionConfig(
                    timeout_rate=1.0, operations=("head_object",), seed=9
                ),
            )

            with self.assertRaises(TierUnavailable):
                store.lookup(key)

            with self.assertRaises(BlockNotFound) as raised:
                store.load(key, target_tier=TierName.MEMORY)

            self.assertIn("tier_lookup_unavailable_recompute", str(raised.exception))
            self.assertEqual(sum(metrics.kv_onload_timeout_total.values.values()), 2.0)
            self.assertEqual(sum(metrics.kv_onload_fallback_total.values.values()), 1.0)

    def test_unavailable_s3_lookup_does_not_hide_healthy_nvme_location(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meta = MetadataStore(root / "meta.sqlite3")
            base_client = _FakeS3Client()
            memory = MemoryTier(1024 * 1024, meta)
            nvme = NVMeTier(root / "nvme", 1024 * 1024, meta)
            s3 = S3Tier("bucket", "blocks", meta, client=base_client)
            store = MultiTierKVBlockStore(
                [memory, nvme, s3],
                meta,
                CostModel(
                    {
                        TierName.MEMORY: TierProfile(0.01, 80.0),
                        TierName.NVME: TierProfile(0.3, 5.0),
                        TierName.S3: TierProfile(1.0, 10.0),
                    },
                    load_benefit_threshold_ms=0.0,
                ),
            )
            key = _key("healthy-nvme")
            metadata = _metadata(key, 4096, 7)
            store.store(key, b"payload", metadata, preferred_tier=TierName.NVME)
            store.store(
                key,
                b"payload",
                _metadata(key, 4096, 7),
                preferred_tier=TierName.S3,
            )
            s3.client = FaultInjectingS3Client(
                base_client,
                S3FaultInjectionConfig(
                    timeout_rate=1.0, operations=("head_object",), seed=11
                ),
            )

            result = store.load(key, target_tier=TierName.MEMORY)

            self.assertEqual(result.data, b"payload")
            self.assertTrue(memory.contains(key))


def _store(root: Path) -> MultiTierKVBlockStore:
    meta = MetadataStore(root / "meta.sqlite3")
    profiles = {
        TierName.MEMORY: TierProfile(0.01, 80.0),
        TierName.NVME: TierProfile(0.3, 5.0),
    }
    return MultiTierKVBlockStore(
        [MemoryTier(1024, meta), NVMeTier(root / "nvme", 1024, meta)],
        meta,
        CostModel(profiles),
    )


def _key(seed: str) -> BlockKey:
    return BlockKey("t", "m", "r", "tok", hashlib.sha256(seed.encode()).hexdigest())


def _metadata(key: BlockKey, tokens: int, bytes_: int) -> KVMetadata:
    return KVMetadata(key, "bf16", 1, 1, 1, tokens, bytes_)


class _PrefetchOnlyCostModel:
    def __init__(self, source_tier: TierName = TierName.NVME):
        self.source_tier = source_tier

    def decide(self, locations, ctx, slo_budget_ms=None):
        return Decision("prefetch", self.source_tier, 1.0, 2.0, 1.0, "test")


class _FakeS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.metadata: dict[tuple[str, str], dict[str, str]] = {}

    def put_object(self, Bucket: str, Key: str, Body: bytes, Metadata: dict[str, str]):
        self.objects[(Bucket, Key)] = bytes(Body)
        self.metadata[(Bucket, Key)] = dict(Metadata)
        return {}

    def get_object(self, Bucket: str, Key: str):
        key = (Bucket, Key)
        if key not in self.objects:
            raise _FakeNotFound()
        return {"Body": io.BytesIO(self.objects[key]), "Metadata": self.metadata.get(key, {})}

    def head_object(self, Bucket: str, Key: str):
        key = (Bucket, Key)
        if key not in self.objects:
            raise _FakeNotFound()
        return {"Metadata": self.metadata.get(key, {}), "ContentLength": len(self.objects[key])}

    def delete_object(self, Bucket: str, Key: str):
        key = (Bucket, Key)
        if key not in self.objects:
            raise _FakeNotFound()
        self.objects.pop(key, None)
        self.metadata.pop(key, None)
        return {}


class _FakeNotFound(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


if __name__ == "__main__":
    unittest.main()
