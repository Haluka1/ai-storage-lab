from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from kvstore.metadata import TierName
from kvstore.tier_profile_import import import_profiles_from_tier_profile, profiles_from_tier_profile


class TierProfileImportTest(unittest.TestCase):
    def test_profile_mode_changes_nvme_latency(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "profile.json"
            path.write_text(json.dumps(_profile()), encoding="utf-8")
            p50 = profiles_from_tier_profile(path, "p50")[TierName.NVME]
            p95 = profiles_from_tier_profile(path, "p95")[TierName.NVME]
            p99 = profiles_from_tier_profile(path, "p99")[TierName.NVME]
            self.assertEqual(p50.fixed_latency_ms, 1.0)
            self.assertEqual(p95.fixed_latency_ms, 5.0)
            self.assertEqual(p99.fixed_latency_ms, 9.0)
            self.assertAlmostEqual(p95.bandwidth_gbps, 2.0)

    def test_schema_incompatible_profile_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bad.json"
            path.write_text(json.dumps({"profiles": {}}), encoding="utf-8")
            with self.assertRaises(ValueError):
                profiles_from_tier_profile(path, "p95")

    def test_transport_profile_import_records_s3_fallback_source(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "profile.json"
            path.write_text(json.dumps(_transport_profile()), encoding="utf-8")
            imported = import_profiles_from_tier_profile(path, "p95")
            self.assertEqual(imported.profiles[TierName.NVME].fixed_latency_ms, 2.0)
            self.assertIn("file_posix_default", imported.sources[TierName.NVME])
            self.assertIn("explicit_fallback:s3_http_default:unavailable", imported.sources[TierName.S3])
            self.assertEqual(imported.provenance["artifact_path"], str(path))
            self.assertEqual(
                imported.provenance["artifact_sha256"],
                hashlib.sha256(path.read_bytes()).hexdigest(),
            )
            self.assertEqual(imported.provenance["local_file_profile"], "file_posix_default")

    def test_import_preserves_embedded_measurement_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "profile.json"
            profile = _profile()
            profile["provenance"] = {
                "generator": "unit-generator",
                "source_csv_sha256": "a" * 64,
                "source_row_count": 7,
            }
            path.write_text(json.dumps(profile), encoding="utf-8")

            imported = import_profiles_from_tier_profile(path, "p95")

            self.assertEqual(imported.provenance["embedded"]["generator"], "unit-generator")
            self.assertEqual(imported.provenance["embedded"]["source_row_count"], 7)


def _profile() -> dict:
    return {
        "contract_version": 1,
        "generated_at": "2026-07-02T00:00:00Z",
        "env_id": "unit",
        "source_csv": "unit.csv",
        "profiles": {
            "local_file_pread": {
                "available": True,
                "p50_ms": 1.0,
                "p95_ms": 5.0,
                "p99_ms": 9.0,
                "bandwidth_MBps": 2048.0,
                "page_cache_policy": "unit",
                "filesystem": "tmpfs",
                "mount_options": "rw",
                "kernel": "unit",
                "driver": "posix",
            }
        },
    }


def _transport_profile() -> dict:
    return {
        "contract_version": 1,
        "generated_at": "2026-07-02T00:00:00Z",
        "env_id": "unit",
        "profiles": {
            "file_posix_default": {
                "available": True,
                "p50_ms": 1.0,
                "p95_ms": 2.0,
                "p99_ms": 3.0,
                "bandwidth_MBps": 1024.0,
                "transport_name": "file_posix_default",
                "measured_by_tier_profile": True,
            },
            "s3_http_default": {
                "available": False,
                "skipped": True,
                "reason": "unit minio unavailable",
                "unavailable_reason": "unit minio unavailable",
                "transport_name": "s3_http_default",
                "measured_by_tier_profile": False,
            },
        },
    }


if __name__ == "__main__":
    unittest.main()
