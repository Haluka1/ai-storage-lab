from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kvstore.errors import BlockNotFound
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

    def test_replace_same_key_uses_new_size_for_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = MemoryTier(8, meta)
            key = BlockKey("t", "m", "r", "tok", "a" * 64)
            tier.store(key, b"1234", KVMetadata(key, "bf16", 1, 1, 1, 1, 4))
            tier.store(key, b"12345678", KVMetadata(key, "bf16", 1, 1, 1, 1, 8))
            self.assertEqual(tier.load(key).data, b"12345678")


if __name__ == "__main__":
    unittest.main()
