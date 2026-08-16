from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from kvstore.cost_model import Decision
from kvstore.decision_log import OnloadDecisionLogger, make_onload_decision_record
from kvstore.metadata import BlockKey, TierName


class DecisionLogTest(unittest.TestCase):
    def test_record_uses_hashes_and_prefix_only(self) -> None:
        key = BlockKey("tenant-secret", "model", "rev", "tok", "a" * 64)
        decision = Decision("load", TierName.NVME, 3.0, 100.0, 97.0, "load_benefit_positive")
        record = make_onload_decision_record("run", "request-secret", key, decision, "posix", 4.0, None, None, strategy="cost_based")
        data = record.__dict__
        self.assertEqual(data["block_hash_prefix"], "a" * 16)
        self.assertNotIn("tenant_id", data)
        self.assertNotIn("block_hash", data)
        self.assertNotEqual(data["request_id_hash"], "request-secret")

    def test_logger_writes_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "decisions.jsonl"
            logger = OnloadDecisionLogger(path)
            key = BlockKey("t", "m", "r", "tok", "b" * 64)
            logger.log("run", "req", key, Decision("recompute", TierName.S3, 50.0, 2.0, -48.0, "recompute_better_than_load"), "s3", None, 2.5, 100.0, strategy="cost_based")
            obj = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(obj["decision_type"], "kv_onload")
            self.assertEqual(obj["decision"], "recompute")


if __name__ == "__main__":
    unittest.main()
