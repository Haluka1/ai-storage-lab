from __future__ import annotations

from pathlib import Path
from typing import Any


def write_report(profile: dict[str, Any], out: str | Path) -> None:
    provenance = profile.get("provenance") or {}
    lines = [
        "# IO Path Benchmark Report",
        "",
        f"Generated at: {profile['generated_at']}",
        f"Source CSV: `{profile.get('source_csv', '')}`",
        f"Source CSV SHA-256: `{provenance.get('source_csv_sha256', 'unknown')}`",
        f"Rows: {provenance.get('source_row_count', 'unknown')} total / "
        f"{provenance.get('eligible_measurement_row_count', 'unknown')} measured",
        "",
        "| profile | page_cache_policy | threads | p50_ms | p95_ms | p99_ms | bandwidth_MBps | filesystem |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for name, item in sorted(profile["profiles"].items()):
        lines.append(
            f"| {name} | {item.get('page_cache_policy', 'unknown')} | {item.get('threads', 1)} | "
            f"{item.get('p50_ms', 0):.6f} | {item.get('p95_ms', 0):.6f} | "
            f"{item.get('p99_ms', 0):.6f} | {item.get('bandwidth_MBps', 0):.3f} | {item.get('filesystem', 'unknown')} |"
        )
    lines.extend(
        [
            "",
            "Notes:",
            "",
            "- This checkpoint measures local POSIX file paths only.",
            "- `warm` is buffered POSIX IO with page-cache warming hints and is not a raw-device latency claim.",
            "- `coldish_fadvise_drop` uses per-file `posix_fadvise(..., POSIX_FADV_DONTNEED)` before measured IO; it is not a privileged global `drop_caches` run.",
            "- `direct` is used only for the O_DIRECT engine. If O_DIRECT is unavailable, the row is marked unavailable instead of silently falling back.",
            "- CUDA, GDS, RDMA, NIXL, and CXL are not claimed by this report.",
        ]
    )
    out_path = Path(out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
