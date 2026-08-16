from __future__ import annotations

import csv
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "io-profile/build/io_path_bench"


class LocalEngineSmokeTest(unittest.TestCase):
    def test_five_local_engines_emit_structured_results(self) -> None:
        for engine in ("buffered", "pread", "mmap", "vectored", "odirect"):
            with self.subTest(engine=engine), tempfile.TemporaryDirectory() as td:
                output = Path(td) / f"{engine}.csv"
                result = subprocess.run(
                    [
                        str(BINARY),
                        "--engine",
                        engine,
                        "--op",
                        "read",
                        "--path",
                        str(Path(td) / "payload.bin"),
                        "--file-size-mb",
                        "1",
                        "--block-size-kb",
                        "256",
                        "--threads",
                        "1",
                        "--iterations",
                        "1",
                        "--warmup",
                        "0",
                        "--access",
                        "sequential",
                        "--cache-policy",
                        "direct" if engine == "odirect" else "warm",
                        "--output",
                        str(output),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                with output.open("r", encoding="utf-8", newline="") as handle:
                    rows = list(csv.DictReader(handle))
                self.assertGreaterEqual(len(rows), 1)
                self.assertEqual({row["engine"] for row in rows}, {engine})
                if engine != "odirect":
                    self.assertEqual({row["error"] for row in rows}, {""})


if __name__ == "__main__":
    unittest.main()
