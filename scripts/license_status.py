#!/usr/bin/env python3
"""Report whether publication licensing has been resolved."""

from __future__ import annotations

import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_NAME = "LICENSE"
EXPECTED_SHA256 = "590d139966805b1fcadf4c930d1f3d5d09af5266dcf1134ccd9cf25122c378f3"


def main() -> int:
    licenses = [path for path in ROOT.glob("LICENSE*") if path.is_file()]
    expected_path = ROOT / EXPECTED_NAME
    if licenses != [expected_path]:
        found = ", ".join(path.name for path in licenses) or "none"
        print(f"License status: FAIL (expected only {EXPECTED_NAME}; found {found})")
        return 1

    digest = hashlib.sha256(expected_path.read_bytes()).hexdigest()
    if digest != EXPECTED_SHA256:
        print("License status: FAIL (LICENSE differs from the reviewed MIT text)")
        return 1

    print("License status: PASS (MIT; Copyright (c) 2026 Haluka1)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
