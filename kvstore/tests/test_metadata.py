from __future__ import annotations

import json
import unittest

from kvstore.metadata import BlockKey, KVMetadata


class MetadataTest(unittest.TestCase):
    def test_namespace_isolation(self) -> None:
        base = BlockKey("tenant-a", "model", "rev", "tok", "a" * 64)
        other_tenant = BlockKey("tenant-b", "model", "rev", "tok", "a" * 64)
        other_lora = BlockKey("tenant-a", "model", "rev", "tok", "a" * 64, lora_id="adapter")
        self.assertNotEqual(base.namespace(), other_tenant.namespace())
        self.assertNotEqual(base.namespace(), other_lora.namespace())

    def test_namespace_encoding_is_injective_for_delimiter_values(self) -> None:
        key_a = BlockKey("a/model=b", "c", "r", "tok", "a" * 64)
        key_b = BlockKey("a", "b/model=c", "r", "tok", "a" * 64)
        self.assertNotEqual(key_a.namespace(), key_b.namespace())

    def test_empty_and_literal_optional_values_are_distinct(self) -> None:
        absent = BlockKey("t", "m", "r", "tok", "a" * 64)
        literal = BlockKey(
            "t", "m", "r", "tok", "a" * 64, lora_id="none", modality_key="text"
        )
        self.assertNotEqual(absent.namespace(), literal.namespace())

    def test_unicode_namespace_round_trips_without_collision(self) -> None:
        key = BlockKey("租户", "模型/甲", "版本", "分词器", "a" * 64)
        encoded = json.loads(key.namespace())
        self.assertEqual(encoded[0], "kv-block-key-v1")
        self.assertEqual(encoded[1:5], ["租户", "模型/甲", "版本", "分词器"])

    def test_empty_required_identity_fields_are_rejected(self) -> None:
        for values in [
            ("", "m", "r", "tok"),
            ("t", "", "r", "tok"),
            ("t", "m", "", "tok"),
            ("t", "m", "r", ""),
        ]:
            with self.subTest(values=values), self.assertRaises(ValueError):
                BlockKey(*values, "a" * 64)

    def test_invalid_block_hash_and_control_characters_are_rejected(self) -> None:
        for block_hash in ["", "/tmp/escape", "../escape", "A" * 64, "g" * 64]:
            with self.subTest(block_hash=block_hash), self.assertRaises(ValueError):
                BlockKey("t", "m", "r", "tok", block_hash)
        with self.assertRaisesRegex(ValueError, "control characters"):
            BlockKey("tenant\nname", "m", "r", "tok", "a" * 64)

    def test_metadata_roundtrip(self) -> None:
        key = BlockKey("t", "m", "r", "tok", "b" * 64)
        meta = KVMetadata(key, "bf16", 32, 8, 128, 16, 1024, shape=(32, 2, 8, 16, 128))
        self.assertEqual(KVMetadata.from_dict(meta.to_dict()).shape, meta.shape)


if __name__ == "__main__":
    unittest.main()
