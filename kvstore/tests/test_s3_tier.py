from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from kvstore.errors import (
    BlockNotFound,
    ChecksumMismatch,
    CorruptionCleanupFailed,
    ImmutableBlockConflict,
    MetadataMismatch,
    RecordFormatError,
    TierUnavailable,
)
from kvstore.layout import storage_key_parts
from kvstore.metadata import BlockKey, BlockLocation, KVMetadata, TierName
from kvstore.metadata_store import MetadataStore
from kvstore.record import HEADER_LEN_STRUCT, MAGIC
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

    def test_orphan_object_is_not_implicitly_published(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta1 = MetadataStore(Path(td) / "meta1.sqlite3")
            tier1 = S3Tier("bucket", "blocks", meta1, client=client)
            key = _key("b")
            tier1.store(key, b"payload", _metadata(key, 7))

            meta2 = MetadataStore(Path(td) / "meta2.sqlite3")
            tier2 = S3Tier("bucket", "blocks", meta2, client=client)
            self.assertIsNone(tier2.lookup(key))
            self.assertIsNone(meta2.get_metadata(key, TierName.S3))

    def test_store_rejects_cross_key_metadata_and_conflicting_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key_a = _key("identity-a")
            key_b = _key("identity-b")

            with self.assertRaises(MetadataMismatch):
                tier.store(key_a, b"payload", _metadata(key_b, 7))

            tier.store(key_a, b"payload", _metadata(key_a, 7))
            tier.store(key_a, b"payload", _metadata(key_a, 7))
            with self.assertRaises(ImmutableBlockConflict):
                tier.store(key_a, b"changed", _metadata(key_a, 7))
            changed_metadata = _metadata(key_a, 7)
            changed_metadata.dtype = "fp16"
            with self.assertRaises(ImmutableBlockConflict):
                tier.store(key_a, b"payload", changed_metadata)
            self.assertEqual(tier.load(key_a).data, b"payload")

    def test_persisted_location_outside_configured_bucket_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key = _key("foreign-bucket")
            payload = b"payload"
            tier.store(key, payload, _metadata(key, len(payload)))
            canonical = meta.lookup(key, TierName.S3)[0]
            foreign_uri = canonical.uri.replace("s3://bucket/", "s3://other-bucket/")
            meta.upsert(
                BlockLocation(
                    key,
                    TierName.S3,
                    foreign_uri,
                    canonical.bytes,
                    canonical.checksum,
                    canonical.created_at,
                    canonical.last_access,
                ),
                meta.get_metadata(key, TierName.S3),
            )
            client.objects[("other-bucket", "do-not-delete")] = b"sentinel"

            with self.assertRaises(MetadataMismatch):
                tier.lookup(key)
            with self.assertRaises(MetadataMismatch):
                tier.evict(key)

            self.assertEqual(
                client.objects[("other-bucket", "do-not-delete")], b"sentinel"
            )

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

    def test_structural_header_corruption_invalidates_location(self) -> None:
        corrupt_headers = [
            b"{not-json",
            json.dumps(
                {
                    "version": 999,
                    "checksum": "0" * 64,
                    "payload_bytes": 7,
                    "metadata": {},
                }
            ).encode(),
            json.dumps(
                {"version": 1, "checksum": "0" * 64, "metadata": {}}
            ).encode(),
        ]
        for index, encoded_header in enumerate(corrupt_headers):
            with self.subTest(index=index), tempfile.TemporaryDirectory() as td:
                client = FakeS3Client()
                meta = MetadataStore(Path(td) / "meta.sqlite3")
                tier = S3Tier("bucket", "blocks", meta, client=client)
                key = _key(f"bad-header-{index}")
                tier.store(key, b"payload", _metadata(key, 7))
                object_key = tier.object_key(key)
                client.objects[("bucket", object_key)] = (
                    MAGIC
                    + HEADER_LEN_STRUCT.pack(len(encoded_header))
                    + encoded_header
                    + b"payload"
                )

                with self.assertRaises(RecordFormatError):
                    tier.load(key)

                self.assertIsNone(tier.lookup(key))

    def test_corruption_cleanup_failure_is_not_reclassified_as_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key = _key("cleanup")
            tier.store(key, b"payload", _metadata(key, 7))
            object_key = tier.object_key(key)
            client.objects[("bucket", object_key)] = (
                client.objects[("bucket", object_key)][:-1] + b"x"
            )
            client.delete_failures_remaining = 1
            with self.assertRaises(CorruptionCleanupFailed) as ctx:
                tier.load(key)
            self.assertEqual(ctx.exception.operation, "evict")
            self.assertIsInstance(ctx.exception.__cause__, ChecksumMismatch)
            self.assertTrue(meta.is_deleting(key, TierName.S3))

    def test_object_key_includes_namespace_dimensions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tier = S3Tier("bucket", "blocks", MetadataStore(Path(td) / "meta.sqlite3"), client=FakeS3Client())
            key = BlockKey("tenant/a", "model", "rev", "tok", "d" * 64, lora_id="adapter", modality_key="image")
            object_key = tier.object_key(key)
            self.assertEqual(object_key.split("/")[1:], storage_key_parts(key))
            self.assertNotIn("tenant/a", object_key)

    def test_uri_significant_prefix_round_trips_canonically(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "cache ?#%/雪", meta, client=client)
            key = _key("uri-prefix")
            tier.store(key, b"payload", _metadata(key, 7))

            literal_key = tier.object_key(key)
            self.assertIn(("bucket", literal_key), client.objects)
            location = meta.lookup(key, TierName.S3)[0]
            self.assertNotIn("?", location.uri)
            self.assertNotIn("#", location.uri)
            self.assertIn("%", location.uri)
            self.assertEqual(tier.load(key).data, b"payload")

    def test_invalid_bucket_authority_is_rejected_before_client_use(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            for bucket in ("bad/bucket", "bad@bucket", "bad bucket", "bad:bucket"):
                with self.subTest(bucket=bucket), self.assertRaises(ValueError):
                    S3Tier(bucket, "blocks", meta, client=client)
            self.assertEqual(client.objects, {})

    def test_streaming_body_read_failure_is_unavailable_and_body_is_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key = _key("body-read-failure")
            tier.store(key, b"payload", _metadata(key, 7))
            body = ReadFailureBody()
            client.body_override = body

            with self.assertRaises(TierUnavailable) as raised:
                tier.load(key)

            self.assertIsInstance(raised.exception.__cause__, TimeoutError)
            self.assertTrue(body.closed)
            self.assertEqual(len(meta.lookup(key, TierName.S3)), 1)

    def test_streaming_body_close_failure_is_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key = _key("body-close-failure")
            tier.store(key, b"payload", _metadata(key, 7))
            object_key = tier.object_key(key)
            body = CloseFailureBody(client.objects[("bucket", object_key)])
            client.body_override = body

            with self.assertRaises(TierUnavailable) as raised:
                tier.load(key)

            self.assertIsInstance(raised.exception.__cause__, OSError)
            self.assertEqual(body.close_calls, 1)

    def test_successful_streaming_body_is_closed_once(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key = _key("body-success")
            tier.store(key, b"payload", _metadata(key, 7))
            object_key = tier.object_key(key)
            body = TrackingBody(client.objects[("bucket", object_key)])
            client.body_override = body

            self.assertEqual(tier.load(key).data, b"payload")
            self.assertEqual(body.close_calls, 1)

    def test_body_read_error_remains_primary_when_close_also_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "blocks", meta, client=client)
            key = _key("body-read-and-close-failure")
            tier.store(key, b"payload", _metadata(key, 7))
            body = ReadAndCloseFailureBody()
            client.body_override = body

            with self.assertRaises(TierUnavailable) as raised:
                tier.load(key)

            self.assertIsInstance(raised.exception.__cause__, TimeoutError)
            self.assertIsInstance(raised.exception.close_error, OSError)
            self.assertEqual(body.close_calls, 1)

    def test_noncanonical_percent_escape_in_persisted_uri_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = S3Tier("bucket", "cache?", meta, client=client)
            key = _key("noncanonical-uri")
            tier.store(key, b"payload", _metadata(key, 7))
            location = meta.lookup(key, TierName.S3)[0]
            self.assertIn("%3F", location.uri)
            location.uri = location.uri.replace("%3F", "%3f", 1)
            metadata = meta.get_metadata(key, TierName.S3)
            self.assertIsNotNone(metadata)
            meta.upsert(location, metadata)

            with self.assertRaises(MetadataMismatch):
                tier.lookup(key)

    def test_prefix_length_and_controls_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            client = FakeS3Client()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            for prefix in ("x" * 1025, "line\nbreak", "nul\x00byte"):
                with self.subTest(prefix_length=len(prefix)), self.assertRaises(ValueError):
                    S3Tier("bucket", prefix, meta, client=client)
            self.assertEqual(client.objects, {})

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
            # A retry revalidates the persisted URI against the same configured
            # bucket and prefix before issuing a destructive operation.
            reopened = S3Tier("bucket", "blocks", reopened_meta, client=client)
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
        self.body_override = None

    def put_object(self, Bucket: str, Key: str, Body: bytes, Metadata: dict[str, str]):
        self.objects[(Bucket, Key)] = bytes(Body)
        self.metadata[(Bucket, Key)] = dict(Metadata)
        self.last_metadata = dict(Metadata)
        return {}

    def get_object(self, Bucket: str, Key: str):
        key = (Bucket, Key)
        if key not in self.objects:
            raise FakeNotFound()
        body = self.body_override
        if body is None:
            body = io.BytesIO(self.objects[key])
        return {"Body": body, "Metadata": self.metadata.get(key, {})}

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


class ReadFailureBody:
    def __init__(self) -> None:
        self.closed = False

    def read(self) -> bytes:
        raise TimeoutError("injected streaming body timeout")

    def close(self) -> None:
        self.closed = True


class CloseFailureBody:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self.close_calls = 0

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        self.close_calls += 1
        raise OSError("injected streaming body close failure")


class TrackingBody:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self.close_calls = 0

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        self.close_calls += 1


class ReadAndCloseFailureBody:
    def __init__(self) -> None:
        self.close_calls = 0

    def read(self) -> bytes:
        raise TimeoutError("injected streaming body timeout")

    def close(self) -> None:
        self.close_calls += 1
        raise OSError("injected streaming body close failure")


def _key(seed: str) -> BlockKey:
    return BlockKey("t", "m", "r", "tok", hashlib.sha256(seed.encode()).hexdigest())


def _metadata(key: BlockKey, bytes_: int) -> KVMetadata:
    return KVMetadata(key, "bf16", 1, 1, 1, 16, bytes_)


if __name__ == "__main__":
    unittest.main()
