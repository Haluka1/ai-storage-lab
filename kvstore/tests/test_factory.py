from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from kvstore.factory import build_store_from_config
from kvstore.metadata import BlockKey, KVMetadata, TierName


class FactoryTest(unittest.TestCase):
    def test_construction_failure_closes_metadata_store(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg_path = root / "kvcache.json"
            cfg_path.write_text(
                json.dumps(
                    {
                        "kvstore": {"metadata_db": str(root / "meta.sqlite3")},
                        "tiers": {
                            "memory": {"enabled": False},
                            "nvme": {"enabled": True},
                            "s3": {"enabled": False},
                        },
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch("kvstore.factory.MetadataStore") as metadata_cls:
                with mock.patch(
                    "kvstore.factory.NVMeTier",
                    side_effect=RuntimeError("injected construction failure"),
                ):
                    with self.assertRaisesRegex(RuntimeError, "construction failure"):
                        build_store_from_config(cfg_path)

                metadata_cls.return_value.close.assert_called_once_with()

    def test_nvme_segment_layout_from_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg_path = root / "kvcache.json"
            cfg_path.write_text(
                json.dumps(
                    {
                        "kvstore": {"metadata_db": str(root / "meta.sqlite3")},
                        "tiers": {
                            "memory": {"enabled": False},
                            "nvme": {
                                "enabled": True,
                                "root_dir": str(root / "nvme"),
                                "max_bytes": 4096,
                                "layout_mode": "segment",
                                "segment_bytes": 2048,
                            },
                            "s3": {"enabled": False},
                        },
                        "cost_model": {},
                    }
                ),
                encoding="utf-8",
            )
            store = build_store_from_config(cfg_path)
            tier = store.tiers[TierName.NVME]
            self.assertEqual(tier.stats()["layout_mode"], "segment")
            key = BlockKey("tenant", "model", "rev", "tok", "aa" + "1" * 62)
            store.store(key, b"payload", KVMetadata(key, "bf16", 1, 1, 1, 16, 7), preferred_tier=TierName.NVME)
            self.assertEqual(store.load(key, target_tier=TierName.NVME).data, b"payload")
            loc = store.lookup(key)
            self.assertIsNotNone(loc)
            self.assertTrue(loc.uri.startswith("segment://"))
            store.close()


if __name__ == "__main__":
    unittest.main()
