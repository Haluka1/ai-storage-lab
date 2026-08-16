from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / "io-profile/build/file_generator"


class FileGeneratorPathGuardTest(unittest.TestCase):
    def test_allows_only_component_bounded_safe_roots(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = [root / "file.bin", root / "nested/file.bin"]
            rejected = [Path("/"), Path("/etc/io-path-bench.bin"), root.parent / "sibling.bin"]
            for path in allowed:
                with self.subTest(path=path, expected="allowed"):
                    result = _check_path(path, root)
                    self.assertEqual(result.returncode, 0, result.stderr)
            for path in rejected:
                with self.subTest(path=path, expected="rejected"):
                    result = _check_path(path, root)
                    self.assertEqual(result.returncode, 2, result.stdout)
                    self.assertIn("refusing unsafe output path", result.stderr)

    def test_rejects_symlink_escape_from_tmp(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as td:
            root = Path(td)
            link = Path(td) / "escape"
            link.symlink_to("/etc", target_is_directory=True)
            result = _check_path(link / "io-path-bench.bin", root)
        self.assertEqual(result.returncode, 2)
        self.assertIn("refusing unsafe output path", result.stderr)

    def test_rejects_metadata_sidecar_symlink_escape(self) -> None:
        with (
            tempfile.TemporaryDirectory(dir="/tmp") as safe_td,
            tempfile.TemporaryDirectory() as outside_td,
        ):
            output = Path(safe_td) / "payload.bin"
            metadata = output.with_name(f"{output.name}.metadata.json")
            victim = Path(outside_td) / "victim.json"
            victim.write_text("unchanged", encoding="utf-8")
            metadata.symlink_to(victim)

            result = subprocess.run(
                [str(BINARY), "--allowed-root", safe_td, "--path", str(output), "--size-mb", "0", "--mode", "repeating"],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 2)
            self.assertIn("refusing unsafe metadata path", result.stderr)
            self.assertEqual(victim.read_text(encoding="utf-8"), "unchanged")
            self.assertFalse(output.exists())

    def test_generator_writes_inside_explicit_root(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / "empty.bin"
            metadata = output.with_name(f"{output.name}.metadata.json")
            result = subprocess.run(
                [str(BINARY), "--allowed-root", td, "--path", str(output), "--size-mb", "0", "--mode", "repeating"],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(output.exists())
            self.assertTrue(metadata.exists())

    def test_invalid_mode_is_rejected_before_output_is_opened(self) -> None:
        with tempfile.TemporaryDirectory(dir="/tmp") as td:
            output = Path(td) / "existing.bin"
            output.write_bytes(b"must-not-be-truncated")

            result = subprocess.run(
                [
                    str(BINARY),
                    "--allowed-root",
                    td,
                    "--path",
                    str(output),
                    "--size-mb",
                    "0",
                    "--mode",
                    "invalid",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(result.returncode, 2)
            self.assertIn("invalid mode", result.stderr)
            self.assertEqual(output.read_bytes(), b"must-not-be-truncated")
            self.assertFalse(output.with_name(f"{output.name}.metadata.json").exists())


def _check_path(path: Path, allowed_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(BINARY), "--allowed-root", str(allowed_root), "--check-path", str(path)],
        check=False,
        capture_output=True,
        text=True,
    )


if __name__ == "__main__":
    unittest.main()
