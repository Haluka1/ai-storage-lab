#!/usr/bin/env python3
"""Check repository-local links in Markdown files."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote


LINK_RE = re.compile(r"!?(?:\[[^\]]*\])\(([^)]+)\)")
EXCLUDED_DIRS = {".git", ".venv", "build", "__pycache__"}


def markdown_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.md")
        if not any(part in EXCLUDED_DIRS for part in path.relative_to(root).parts)
    )


def clean_target(raw: str) -> str:
    target = raw.strip()
    if target.startswith("<") and ">" in target:
        return target[1 : target.index(">")]
    if " \"" in target:
        target = target.split(" \"", 1)[0]
    if " '" in target:
        target = target.split(" '", 1)[0]
    return target


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    failures: list[str] = []
    checked = 0
    for source in markdown_files(root):
        text = source.read_text(encoding="utf-8")
        for match in LINK_RE.finditer(text):
            target = clean_target(match.group(1))
            if not target or target.startswith(("#", "https://", "http://", "mailto:")):
                continue
            path_part = unquote(target.split("#", 1)[0])
            if not path_part:
                continue
            checked += 1
            candidate = (source.parent / path_part).resolve()
            try:
                candidate.relative_to(root)
            except ValueError:
                failures.append(f"{source.relative_to(root)}: link escapes repository: {target}")
                continue
            if not candidate.exists():
                failures.append(f"{source.relative_to(root)}: missing link target: {target}")
    if failures:
        print("Markdown link audit: FAIL", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        return 1
    print(f"Markdown link audit: PASS ({len(markdown_files(root))} files, {checked} local links)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
