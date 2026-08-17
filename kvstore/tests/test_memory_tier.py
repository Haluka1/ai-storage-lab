from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from kvstore.errors import (
    BlockNotFound,
    ChecksumMismatch,
    CorruptionCleanupFailed,
    ImmutableBlockConflict,
    MetadataMismatch,
)
from kvstore.memory_tier import MemoryTier
from kvstore.metadata import BlockKey, KVMetadata
from kvstore.metadata_store import MetadataStore


class MemoryTierTest(unittest.TestCase):
    def test_store_load_and_lru_evict(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = MemoryTier(8, meta)
            key1 = BlockKey("t", "m", "r", "tok", "a" * 64)
            key2 = BlockKey("t", "m", "r", "tok", "b" * 64)
            tier.store(key1, b"1234", KVMetadata(key1, "bf16", 1, 1, 1, 1, 4))
            self.assertEqual(tier.load(key1).data, b"1234")
            tier.store(key2, b"567890", KVMetadata(key2, "bf16", 1, 1, 1, 1, 6))
            self.assertFalse(tier.contains(key1))
            self.assertTrue(tier.contains(key2))
            with self.assertRaises(BlockNotFound):
                tier.load(key1)

    def test_same_payload_is_idempotent_but_conflicting_overwrite_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = MemoryTier(8, meta)
            key = BlockKey("t", "m", "r", "tok", "a" * 64)
            tier.store(key, b"1234", KVMetadata(key, "bf16", 1, 1, 1, 1, 4))
            tier.store(key, b"1234", KVMetadata(key, "bf16", 1, 1, 1, 1, 4))
            self.assertEqual(tier.stats()["used_bytes"], 4)

            with self.assertRaises(ImmutableBlockConflict):
                tier.store(
                    key,
                    b"12345678",
                    KVMetadata(key, "bf16", 1, 1, 1, 1, 8),
                )
            with self.assertRaises(ImmutableBlockConflict):
                tier.store(
                    key,
                    b"1234",
                    KVMetadata(key, "fp16", 1, 1, 1, 1, 4),
                )

            self.assertEqual(tier.load(key).data, b"1234")
            self.assertEqual(tier.stats()["used_bytes"], 4)

    def test_store_rejects_metadata_for_a_different_key(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = MemoryTier(8, meta)
            key_a = BlockKey("t", "m", "r", "tok", "a" * 64)
            key_b = BlockKey("t", "m", "r", "tok", "b" * 64)

            with self.assertRaises(MetadataMismatch):
                tier.store(
                    key_a,
                    b"1234",
                    KVMetadata(key_b, "bf16", 1, 1, 1, 1, 4),
                )

            self.assertFalse(tier.contains(key_a))

    def test_metadata_commit_failure_rolls_back_memory_and_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = MemoryTier(8, meta)
            key = BlockKey("t", "m", "r", "tok", "c" * 64)

            with mock.patch.object(
                meta, "upsert", side_effect=RuntimeError("metadata commit failed")
            ):
                with self.assertRaisesRegex(RuntimeError, "metadata commit failed"):
                    tier.store(
                        key,
                        b"1234",
                        KVMetadata(key, "bf16", 1, 1, 1, 1, 4),
                    )

            self.assertFalse(tier.contains(key))
            self.assertEqual(tier.stats()["used_bytes"], 0)
            self.assertEqual(tier.stats()["blocks"], 0)

    def test_unpublished_memory_orphan_is_removed_on_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = MemoryTier(8, meta)
            key = BlockKey("t", "m", "r", "tok", "d" * 64)
            with tier._lock:
                tier._data[key] = b"1234"
                tier._used_bytes = 4

            self.assertIsNone(tier.lookup(key))
            self.assertEqual(tier.stats()["used_bytes"], 0)

    def test_checksum_corruption_invalidates_memory_location(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = MemoryTier(8, meta)
            key = BlockKey("t", "m", "r", "tok", "e" * 64)
            tier.store(key, b"1234", KVMetadata(key, "bf16", 1, 1, 1, 1, 4))
            with tier._lock:
                tier._data[key] = b"5678"

            with self.assertRaises(ChecksumMismatch):
                tier.load(key)

            self.assertFalse(tier.contains(key))

    def test_corruption_cleanup_failure_preserves_corruption_classification(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = MemoryTier(8, meta)
            key = BlockKey("t", "m", "r", "tok", "f" * 64)
            tier.store(key, b"1234", KVMetadata(key, "bf16", 1, 1, 1, 1, 4))
            with tier._lock:
                tier._data[key] = b"5678"

            with mock.patch.object(tier, "evict", side_effect=OSError("read only")):
                with self.assertRaises(CorruptionCleanupFailed) as raised:
                    tier.load(key)

            self.assertIsInstance(raised.exception.corruption_error, ChecksumMismatch)


if __name__ == "__main__":
    unittest.main()
