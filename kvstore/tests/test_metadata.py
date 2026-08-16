from __future__ import annotations

import unittest

from kvstore.metadata import BlockKey, KVMetadata


class MetadataTest(unittest.TestCase):
    def test_namespace_isolation(self) -> None:
        base = BlockKey("tenant-a", "model", "rev", "tok", "a" * 64)
        other_tenant = BlockKey("tenant-b", "model", "rev", "tok", "a" * 64)
        other_lora = BlockKey("tenant-a", "model", "rev", "tok", "a" * 64, lora_id="adapter")
        self.assertNotEqual(base.namespace(), other_tenant.namespace())
        self.assertNotEqual(base.namespace(), other_lora.namespace())

    def test_metadata_roundtrip(self) -> None:
        key = BlockKey("t", "m", "r", "tok", "b" * 64)
        meta = KVMetadata(key, "bf16", 32, 8, 128, 16, 1024, shape=(32, 2, 8, 16, 128))
        self.assertEqual(KVMetadata.from_dict(meta.to_dict()).shape, meta.shape)


if __name__ == "__main__":
    unittest.main()
