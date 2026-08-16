#!/usr/bin/env python3
"""Validate checked-in tier-profile contract fixtures."""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "shared/schema/tier_profile.schema.json"


def main() -> int:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    fixtures = sorted((ROOT / "shared/fixtures").glob("*.tier-profile.json"))
    if not fixtures:
        raise SystemExit("Tier-profile contract audit: FAIL (no fixture)")
    for fixture in fixtures:
        value = json.loads(fixture.read_text(encoding="utf-8"))
        errors = sorted(validator.iter_errors(value), key=lambda error: list(error.path))
        if errors:
            first = errors[0]
            location = ".".join(str(part) for part in first.path) or "<root>"
            raise SystemExit(f"Tier-profile contract audit: FAIL ({fixture.name}:{location}: {first.message})")
    print(f"Tier-profile contract audit: PASS ({len(fixtures)} fixture)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
