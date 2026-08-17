from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kvstore.layout import ContentAddressedLayout, SegmentedLayout, storage_key_parts
from kvstore.metadata import BlockKey


class LayoutSafetyTest(unittest.TestCase):
    def test_dot_and_dotdot_components_cannot_escape_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "root"
            key = BlockKey("..", ".", "../revision", "tok", "a" * 64)
            block_path = ContentAddressedLayout(root).block_path(key)
            segment_dir = SegmentedLayout(root / "segments").namespace_dir(key)
            block_path.relative_to(root.resolve())
            segment_dir.relative_to((root / "segments").resolve())
            self.assertNotIn("..", block_path.parts[len(root.resolve().parts) :])

    def test_slash_in_identity_is_encoded_as_one_component(self) -> None:
        key = BlockKey("tenant/a", "model/b", "r", "tok", "b" * 64)
        parts = storage_key_parts(key)
        self.assertEqual(len(parts), 8)
        self.assertNotIn("tenant/a", parts)
        self.assertTrue(all(part not in {"", ".", ".."} for part in parts))

    def test_generated_path_is_always_descendant_of_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "root"
            layout = ContentAddressedLayout(root)
            for tenant in ["normal", "..", ".", "a/b", "租户"]:
                with self.subTest(tenant=tenant):
                    candidate = layout.block_path(
                        BlockKey(tenant, "model", "rev", "tok", "c" * 64)
                    )
                    candidate.relative_to(root.resolve())

    def test_existing_namespace_symlink_cannot_redirect_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "root"
            outside = Path(td) / "outside"
            root.mkdir()
            outside.mkdir()
            key = BlockKey("tenant", "model", "rev", "tok", "d" * 64)
            first_component = storage_key_parts(key)[0]
            (root / first_component).symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "escapes the configured root"):
                ContentAddressedLayout(root).block_path(key)


if __name__ == "__main__":
    unittest.main()
