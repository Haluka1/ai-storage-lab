from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

from .report import write_report
from .tier_profile import generate_tier_profile


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--matrix", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--tier-profile-out", required=True)
    parser.add_argument("--report-out", required=True)
    parser.add_argument("--profile-mode", choices=["p50", "p95", "p99"], default="p95")
    parser.add_argument("--binary")
    args = parser.parse_args()

    cfg = _load_json_compatible_yaml(Path(args.config))
    matrix = _load_json_compatible_yaml(Path(args.matrix))
    run_uid = f"{os.getpid()}_{uuid.uuid4().hex[:8]}"
    binary = Path(args.binary or cfg.get("binary", "io-profile/build/io_path_bench"))
    file_path = Path(cfg.get("file_path", "/tmp/ai-inference-storage-showcase-io/profile.bin"))
    raw_dir = Path(cfg.get("raw_dir", "/tmp/ai-inference-storage-showcase-io/cases")) / f"run_{run_uid}"
    raw_dir.mkdir(parents=True, exist_ok=True)

    case_csvs: list[Path] = []
    for idx, case in enumerate(matrix["cases"]):
        case_out = raw_dir / f"case_{idx}_{case['engine']}_{case['op']}.csv"
        cmd = [
            str(binary),
            "--engine",
            case["engine"],
            "--op",
            case.get("op", "read"),
            "--path",
            str(file_path),
            "--file-size-mb",
            str(case.get("file_size_mb", cfg.get("file_size_mb", 32))),
            "--block-size-kb",
            str(case.get("block_size_kb", 1024)),
            "--threads",
            str(case.get("threads", 1)),
            "--iterations",
            str(case.get("iterations", 32)),
            "--warmup",
            str(case.get("warmup", 4)),
            "--access",
            case.get("access", "sequential"),
            "--cache-policy",
            case.get("page_cache_policy", "direct" if case["engine"] == "odirect" else "warm"),
            "--output",
            str(case_out),
        ]
        subprocess.check_call(cmd)
        case_csvs.append(case_out)

    merged = Path(args.out)
    profile_out = Path(args.tier_profile_out)
    report_out = Path(args.report_out)
    merged.parent.mkdir(parents=True, exist_ok=True)
    profile_out.parent.mkdir(parents=True, exist_ok=True)
    report_out.parent.mkdir(parents=True, exist_ok=True)
    merged_tmp = merged.with_name(f"{merged.name}.{run_uid}.tmp")
    profile_tmp = profile_out.with_name(f"{profile_out.name}.{run_uid}.tmp")
    report_tmp = report_out.with_name(f"{report_out.name}.{run_uid}.tmp")
    _merge_csv(case_csvs, merged_tmp)
    profile = generate_tier_profile(merged_tmp, profile_tmp, profile_mode=args.profile_mode)
    profile["source_csv"] = str(merged)
    profile["provenance"]["source_csv_path"] = str(merged)
    profile_tmp.write_text(json.dumps(profile, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_report(profile, report_tmp)
    os.replace(merged_tmp, merged)
    os.replace(profile_tmp, profile_out)
    os.replace(report_tmp, report_out)
    return 0


def _load_json_compatible_yaml(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _merge_csv(paths: list[Path], out: Path) -> None:
    fieldnames: list[str] | None = None
    rows: list[dict[str, str]] = []
    for path in paths:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError(f"{path}: empty csv or missing header")
            if fieldnames is None:
                fieldnames = list(reader.fieldnames or [])
            elif list(reader.fieldnames or []) != fieldnames:
                raise ValueError(f"{path}: csv schema mismatch")
            rows.extend(reader)
    with out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames or [])
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    raise SystemExit(main())
