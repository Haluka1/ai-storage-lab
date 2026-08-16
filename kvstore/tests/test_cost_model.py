from __future__ import annotations

import unittest

from kvstore.cost_model import CostModel, RequestContext, TierProfile
from kvstore.metadata import BlockKey, BlockLocation, TierName


def loc(tier: TierName, bytes_: int) -> BlockLocation:
    return BlockLocation(BlockKey("t", "m", "r", "tok", "a" * 64), tier, "x", bytes_, "", 0, 0)


class CostModelTest(unittest.TestCase):
    def test_short_prefix_recompute_better(self) -> None:
        cm = CostModel({TierName.NVME: TierProfile(0.3, 5.0)})
        decision = cm.decide([loc(TierName.NVME, 16 * 1024 * 1024)], RequestContext(missing_prefill_tokens=8))
        self.assertEqual(decision.action, "recompute")
        self.assertEqual(decision.reason, "recompute_better_than_load")

    def test_long_prefix_nvme_load_better(self) -> None:
        cm = CostModel({TierName.NVME: TierProfile(0.3, 5.0)})
        decision = cm.decide([loc(TierName.NVME, 1 * 1024 * 1024)], RequestContext(missing_prefill_tokens=4096))
        self.assertEqual(decision.action, "load")

    def test_s3_non_reuse_prefetch(self) -> None:
        cm = CostModel({TierName.S3: TierProfile(20.0, 1.0)})
        decision = cm.decide([loc(TierName.S3, 1 * 1024 * 1024)], RequestContext(missing_prefill_tokens=4096, reuse_probability=0.2, is_reuse_heavy=False))
        self.assertEqual(decision.action, "prefetch")
        self.assertEqual(decision.reason, "s3_cold_tier_prefetch_only")

    def test_s3_sync_load_allowed_for_long_reuse_heavy_prefix(self) -> None:
        cm = CostModel({TierName.S3: TierProfile(20.0, 1.0)})
        decision = cm.decide([loc(TierName.S3, 1 * 1024 * 1024)], RequestContext(missing_prefill_tokens=4096, reuse_probability=0.9, is_reuse_heavy=True), slo_budget_ms=1000)
        self.assertEqual(decision.action, "load")


if __name__ == "__main__":
    unittest.main()
