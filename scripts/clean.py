#!/usr/bin/env python3
"""Remove only known local build/cache outputs."""

from __future__ import annotations

import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXPLICIT_DIRS = [
    ROOT / "io-profile/build",
    ROOT / ".pytest_cache",
    Path("/tmp/ai-inference-showcase-gocache"),
]


def main() -> int:
    removed = 0
    for path in EXPLICIT_DIRS:
        if path.exists():
            shutil.rmtree(path)
            removed += 1
    for name in ("__pycache__", ".pytest_cache"):
        for path in sorted(ROOT.rglob(name), reverse=True):
            if path.is_dir():
                shutil.rmtree(path)
                removed += 1
    for path in ROOT.rglob("*.py[co]"):
        path.unlink()
        removed += 1
    print(f"clean: removed {removed} generated path(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
