from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path

from kvstore.errors import BlockNotFound, ChecksumMismatch, TierUnavailable
from kvstore.metadata import BlockKey, KVMetadata, TierName
from kvstore.metadata_store import MetadataStore
from kvstore.s3_fault_injection import FaultInjectingS3Client, S3FaultInjectionConfig, S3InjectedTimeout
from kvstore.s3_tier import S3Tier


class S3TierTest(unittest.TestCase):
    def test_store_load_lookup_and_evict(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "blocks/", meta, endpoint_url="http://minio.local", client=client)
            key = _key("a")
            result = tier.store(key, b"payload", _metadata(key, 7))
            self.assertEqual(result.tier, TierName.S3)
            self.assertEqual(client.last_metadata["kv-sha256"], result.checksum)
            self.assertIsNotNone(tier.lookup(key))
            self.assertEqual(tier.load(key).data, b"payload")
            self.assertTrue(tier.evict(key))
            self.assertIsNone(tier.lookup(key))
            with self.assertRaises(BlockNotFound):
                tier.load(key)

    def test_lookup_lazy_rehydrates_metadata_from_object(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta1 = MetadataStore(Path(td) / "meta1.sqlite3")
            tier1 = S3Tier("bucket", "blocks", meta1, client=client)
            key = _key("b")
            tier1.store(key, b"payload", _metadata(key, 7))

            meta2 = MetadataStore(Path(td) / "meta2.sqlite3")
            tier2 = S3Tier("bucket", "blocks", meta2, client=client)
            loc = tier2.lookup(key)
            self.assertIsNotNone(loc)
            self.assertEqual(meta2.get_metadata(key, TierName.S3).key, key)

    def test_checksum_mismatch_evicts_bad_object(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key = _key("c")
            tier.store(key, b"payload", _metadata(key, 7))
            object_key = tier.object_key(key)
            client.objects[("bucket", object_key)] = client.objects[("bucket", object_key)][:-1] + b"x"
            with self.assertRaises(ChecksumMismatch):
                tier.load(key)
            self.assertIsNone(tier.lookup(key))

    def test_object_key_includes_namespace_dimensions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tier = S3Tier("bucket", "blocks", MetadataStore(Path(td) / "meta.sqlite3"), client=FakeS3Client())
            key = BlockKey("tenant/a", "model", "rev", "tok", "d" * 64, lora_id="adapter", modality_key="image")
            object_key = tier.object_key(key)
            self.assertIn("tenant%2Fa", object_key)
            self.assertIn("adapter", object_key)
            self.assertIn("image", object_key)

    def test_fault_injection_times_out_selected_operation(self) -> None:
        client = FaultInjectingS3Client(
            FakeS3Client(),
            S3FaultInjectionConfig(timeout_rate=1.0, seed=1, operations=("get_object",)),
        )
        with self.assertRaises(S3InjectedTimeout):
            client.get_object(Bucket="bucket", Key="missing")
        stats = client.stats()
        self.assertEqual(stats["timeout_total"], 1.0)
        self.assertEqual(stats["operation_total"], 1.0)

    def test_fault_injection_throttles_body_reads(self) -> None:
        base = FakeS3Client()
        base.put_object(Bucket="bucket", Key="key", Body=b"payload", Metadata={})
        client = FaultInjectingS3Client(
            base,
            S3FaultInjectionConfig(throttle_mbps=1_000_000.0, operations=("get_object",)),
        )
        response = client.get_object(Bucket="bucket", Key="key")
        self.assertEqual(response["Body"].read(), b"payload")
        self.assertEqual(client.stats()["throttled_bytes_total"], 7.0)

    def test_delete_failure_keeps_tombstone_and_retry_survives_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "meta.sqlite3"
            client = FakeS3Client()
            meta = MetadataStore(db_path)
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key = _key("e")
            tier.store(key, b"payload", _metadata(key, 7))
            object_key = tier.object_key(key)
            client.delete_failures_remaining = 1

            with self.assertRaises(TierUnavailable):
                tier.evict(key)

            self.assertIn(("bucket", object_key), client.objects)
            self.assertTrue(meta.is_deleting(key, TierName.S3))
            head_calls = client.head_calls
            self.assertIsNone(tier.lookup(key))
            self.assertEqual(client.head_calls, head_calls)
            meta.close()

            reopened_meta = MetadataStore(db_path)
            # Retry uses the persisted tombstone URI, not a recomputed key from
            # possibly changed runtime configuration.
            reopened = S3Tier("bucket", "changed-prefix", reopened_meta, client=client)
            self.assertIsNone(reopened.lookup(key))
            self.assertTrue(reopened.evict(key))
            self.assertNotIn(("bucket", object_key), client.objects)
            self.assertFalse(reopened_meta.is_deleting(key, TierName.S3))

    def test_delete_finishes_after_object_removed_before_metadata_commit(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "meta.sqlite3"
            client = FakeS3Client()
            meta = MetadataStore(db_path)
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key = _key("f")
            tier.store(key, b"payload", _metadata(key, 7))
            self.assertIsNotNone(meta.begin_delete(key, TierName.S3))
            client.delete_object(Bucket="bucket", Key=tier.object_key(key))
            meta.close()  # Simulate a crash before finish_delete().

            reopened_meta = MetadataStore(db_path)
            reopened = S3Tier("bucket", "blocks", reopened_meta, client=client)
            self.assertIsNone(reopened.lookup(key))
            self.assertTrue(reopened.evict(key))
            self.assertFalse(reopened_meta.is_deleting(key, TierName.S3))

    def test_delete_ack_without_removal_keeps_tombstone(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key = _key("g")
            tier.store(key, b"payload", _metadata(key, 7))
            client.delete_noop = True

            with self.assertRaisesRegex(TierUnavailable, "remains visible"):
                tier.evict(key)

            self.assertTrue(meta.is_deleting(key, TierName.S3))
            head_calls = client.head_calls
            self.assertIsNone(tier.lookup(key))
            self.assertEqual(client.head_calls, head_calls)


class FakeS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.metadata: dict[tuple[str, str], dict[str, str]] = {}
        self.last_metadata: dict[str, str] = {}
        self.delete_failures_remaining = 0
        self.delete_noop = False
        self.head_calls = 0

    def put_object(self, Bucket: str, Key: str, Body: bytes, Metadata: dict[str, str]):
        self.objects[(Bucket, Key)] = bytes(Body)
        self.metadata[(Bucket, Key)] = dict(Metadata)
        self.last_metadata = dict(Metadata)
        return {}

    def get_object(self, Bucket: str, Key: str):
        key = (Bucket, Key)
        if key not in self.objects:
            raise FakeNotFound()
        return {"Body": io.BytesIO(self.objects[key]), "Metadata": self.metadata.get(key, {})}

    def head_object(self, Bucket: str, Key: str):
        self.head_calls += 1
        key = (Bucket, Key)
        if key not in self.objects:
            raise FakeNotFound()
        return {"Metadata": self.metadata.get(key, {}), "ContentLength": len(self.objects[key])}

    def delete_object(self, Bucket: str, Key: str):
        if self.delete_failures_remaining > 0:
            self.delete_failures_remaining -= 1
            raise TimeoutError("injected delete timeout")
        key = (Bucket, Key)
        if key not in self.objects:
            raise FakeNotFound()
        if self.delete_noop:
            return {}
        del self.objects[key]
        self.metadata.pop(key, None)
        return {}


class FakeNotFound(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


def _key(seed: str) -> BlockKey:
    return BlockKey("t", "m", "r", "tok", seed * 64)


def _metadata(key: BlockKey, bytes_: int) -> KVMetadata:
    return KVMetadata(key, "bf16", 1, 1, 1, 16, bytes_)


if __name__ == "__main__":
    unittest.main()
