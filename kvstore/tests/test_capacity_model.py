from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kvstore.capacity_model import ModelCapacityConfig, estimate_capacity, kv_bytes_per_token, write_capacity_report


class CapacityModelTest(unittest.TestCase):
    def test_kv_bytes_and_concurrency(self) -> None:
        cfg = ModelCapacityConfig("unit", 2, 4, 8, 2, 16)
        self.assertEqual(kv_bytes_per_token(cfg), 256)
        report = estimate_capacity(cfg, {"gpu": 4096})
        self.assertEqual(report.per_request_kv_bytes, 4096)
        self.assertEqual(report.tiers[0].estimated_full_context_concurrency, 1)

    def test_write_report(self) -> None:
        cfg = ModelCapacityConfig("unit", 2, 4, 8, 2, 16)
        report = estimate_capacity(cfg, {"gpu": 4096})
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "capacity.md"
            write_capacity_report(report, out)
            self.assertIn("KV bytes per token", out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
