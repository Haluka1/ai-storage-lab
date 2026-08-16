from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator

from io_path_bench.report import write_report
from io_path_bench.tier_profile import generate_tier_profile


ROOT = Path(__file__).resolve().parents[2]


class TierProfileTest(unittest.TestCase):
    def test_multithread_benchmark_emits_thread_ids(self) -> None:
        binary = ROOT / "io-profile/build/io_path_bench"
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(td) / "out.csv"
            summary_path = Path(td) / "summary.json"
            subprocess.check_call(
                [
                    str(binary),
                    "--engine",
                    "pread",
                    "--op",
                    "read",
                    "--path",
                    str(Path(td) / "file.bin"),
                    "--file-size-mb",
                    "2",
                    "--block-size-kb",
                    "256",
                    "--threads",
                    "4",
                    "--iterations",
                    "3",
                    "--warmup",
                    "1",
                    "--access",
                    "random",
                    "--output",
                    str(csv_path),
                    "--summary",
                    str(summary_path),
                ]
            )
            with csv_path.open("r", encoding="utf-8", newline="") as f:
                rows = list(csv.DictReader(f))
            summary = json.loads(summary_path.read_text(encoding="utf-8"))

        self.assertEqual(len(rows), 16)
        self.assertEqual({row["thread_id"] for row in rows}, {"0", "1", "2", "3"})
        self.assertEqual(sum(row["warmup"] == "false" for row in rows), 12)
        self.assertEqual({row["threads"] for row in rows}, {"4"})
        self.assertEqual(summary["threads"], 4)
        self.assertEqual(summary["iterations_per_thread"], 3)
        self.assertEqual(summary["total_iterations"], 12)

    def test_direct_policy_requires_odirect_engine(self) -> None:
        binary = ROOT / "io-profile/build/io_path_bench"
        with tempfile.TemporaryDirectory() as td:
            result = subprocess.run(
                [
                    str(binary),
                    "--engine",
                    "buffered",
                    "--op",
                    "read",
                    "--path",
                    str(Path(td) / "file.bin"),
                    "--file-size-mb",
                    "1",
                    "--block-size-kb",
                    "256",
                    "--iterations",
                    "1",
                    "--warmup",
                    "0",
                    "--cache-policy",
                    "direct",
                    "--output",
                    str(Path(td) / "out.csv"),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(result.returncode, 2)
        self.assertIn("--cache-policy direct requires --engine odirect", result.stderr)

    def test_profile_generation_matches_schema(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(td) / "raw.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "engine",
                        "op",
                        "path",
                        "warmup",
                        "latency_us",
                        "bandwidth_MBps",
                        "cpu_user_us",
                        "cpu_system_us",
                        "cache_policy",
                        "error",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "engine": "buffered",
                        "op": "read",
                        "path": str(Path(td) / "file.bin"),
                        "warmup": "false",
                        "latency_us": "2000",
                        "bandwidth_MBps": "400",
                        "cpu_user_us": "10",
                        "cpu_system_us": "5",
                        "cache_policy": "warm",
                        "error": "",
                    }
                )
                writer.writerow(
                    {
                        "engine": "buffered",
                        "op": "read",
                        "path": str(Path(td) / "file.bin"),
                        "warmup": "false",
                        "latency_us": "3000",
                        "bandwidth_MBps": "300",
                        "cpu_user_us": "10",
                        "cpu_system_us": "5",
                        "cache_policy": "coldish_fadvise_drop",
                        "error": "",
                    }
                )
                writer.writerow(
                    {
                        "engine": "pread",
                        "op": "read",
                        "path": str(Path(td) / "file.bin"),
                        "warmup": "false",
                        "latency_us": "1000",
                        "bandwidth_MBps": "500",
                        "cpu_user_us": "10",
                        "cpu_system_us": "5",
                        "cache_policy": "warm",
                        "error": "",
                    }
                )
                writer.writerow(
                    {
                        "engine": "odirect",
                        "op": "read",
                        "path": str(Path(td) / "file.bin"),
                        "warmup": "false",
                        "latency_us": "0",
                        "bandwidth_MBps": "0",
                        "cpu_user_us": "0",
                        "cpu_system_us": "0",
                        "cache_policy": "direct",
                        "error": "odirect_open_failed:Invalid argument",
                    }
                )
            out = Path(td) / "profile.json"
            profile = generate_tier_profile(csv_path, out)
            schema = json.loads((ROOT / "shared/schema/tier_profile.schema.json").read_text(encoding="utf-8"))
            Draft202012Validator.check_schema(schema)
            errors = list(Draft202012Validator(schema).iter_errors(profile))
            self.assertEqual(errors, [])
            self.assertIn("local_file_pread", profile["profiles"])
            self.assertIn("local_file_buffered", profile["profiles"])
            self.assertIn("local_file_buffered_coldish_fadvise_drop", profile["profiles"])
            self.assertIn("local_file_odirect", profile["profiles"])
            self.assertEqual(profile["profiles"]["local_file_buffered"]["page_cache_policy"], "warm")
            self.assertEqual(
                profile["profiles"]["local_file_buffered_coldish_fadvise_drop"]["page_cache_policy"],
                "coldish_fadvise_drop",
            )
            self.assertEqual(profile["profiles"]["local_file_odirect"]["page_cache_policy"], "direct")
            self.assertFalse(profile["profiles"]["local_file_odirect"]["available"])
            self.assertEqual(profile["profiles"]["local_file_pread"]["measurement_count"], 1)
            self.assertEqual(profile["provenance"]["source_row_count"], 4)
            self.assertEqual(profile["provenance"]["eligible_measurement_row_count"], 3)
            self.assertEqual(
                profile["provenance"]["source_csv_sha256"],
                hashlib.sha256(csv_path.read_bytes()).hexdigest(),
            )
            report = Path(td) / "report.md"
            write_report(profile, report)
            report_text = report.read_text(encoding="utf-8")
            self.assertIn("| profile | page_cache_policy |", report_text)
            self.assertIn("coldish_fadvise_drop", report_text)
            self.assertIn(profile["provenance"]["source_csv_sha256"], report_text)
            self.assertIn("Rows: 4 total / 3 measured", report_text)


if __name__ == "__main__":
    unittest.main()
