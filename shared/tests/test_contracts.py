from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "kvstore"))
sys.path.insert(0, str(ROOT))

from kvstore.metadata import TierName  # noqa: E402
from kvstore.tier_profile_import import import_profiles_from_tier_profile  # noqa: E402
from shared.python.blockhash import IsolationKey, compute_blocks  # noqa: E402
from shared.python.tokenization import CONFIG_HASH, TOKENIZER_REVISION, approximate_tokenize  # noqa: E402


class SharedContractTest(unittest.TestCase):
    def test_python_blockhash_vectors(self) -> None:
        values = json.loads(
            (ROOT / "shared/fixtures/blockhash_vectors.json").read_text(encoding="utf-8")
        )["vectors"]
        for value in values:
            with self.subTest(vector=value["name"]):
                key = IsolationKey.from_dict(value["isolation_key"])
                self.assertEqual(
                    compute_blocks(value["tokens"], key, value["block_size_tokens"]),
                    value["expected_hashes"],
                )

    def test_python_tokenization_vectors(self) -> None:
        document = json.loads(
            (ROOT / "shared/fixtures/tokenization_vectors.json").read_text(encoding="utf-8")
        )
        self.assertEqual(document["config_hash"], CONFIG_HASH)
        self.assertEqual(document["tokenizer_revision"], TOKENIZER_REVISION)
        for value in document["vectors"]:
            with self.subTest(vector=value["name"]):
                result = approximate_tokenize(value["prompt"], value["tokenizer_revision"])
                self.assertEqual(result.units, value["expected_units"])
                self.assertEqual(result.tokens, value["expected_tokens"])
                self.assertEqual(
                    compute_blocks(
                        result.tokens,
                        IsolationKey.from_dict(value["isolation_key"]),
                        value["block_size_tokens"],
                    ),
                    value["expected_hashes"],
                )

    def test_tokenizer_revision_changes_identity(self) -> None:
        document = json.loads(
            (ROOT / "shared/fixtures/tokenization_vectors.json").read_text(encoding="utf-8")
        )
        base = next(value for value in document["vectors"] if value["name"] == "basic_ascii")
        other = next(
            value for value in document["vectors"] if value["name"] == "different_tokenizer_revision"
        )
        self.assertNotEqual(base["tokenizer_revision"], other["tokenizer_revision"])
        self.assertNotEqual(base["expected_hashes"], other["expected_hashes"])

    def test_checked_in_tier_profile_reaches_cost_model_import(self) -> None:
        imported = import_profiles_from_tier_profile(
            ROOT / "shared/fixtures/local-contract.tier-profile.json"
        )
        self.assertEqual(imported.provenance["contract_version"], 1)
        self.assertEqual(imported.profiles[TierName.NVME].fixed_latency_ms, 5.0)
        self.assertIn("local_file_pread", imported.sources[TierName.NVME])


if __name__ == "__main__":
    unittest.main()
