from __future__ import annotations

import csv
import hashlib
import io
import json
import platform
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def generate_tier_profile(source_csv: str | Path, out: str | Path, profile_mode: str = "p95") -> dict[str, Any]:
    source = Path(source_csv)
    source_bytes = source.read_bytes()
    rows = _read_rows(source_bytes)
    generated_at = datetime.now(timezone.utc).isoformat()
    profiles: dict[str, Any] = {}
    for engine, cache_policy, threads in sorted({(row["engine"], _cache_policy(row), _threads(row)) for row in rows}):
        measured = [
            row
            for row in rows
            if row["engine"] == engine
            and _cache_policy(row) == cache_policy
            and _threads(row) == threads
            and row.get("warmup") == "false"
            and not row.get("error")
        ]
        lat_ms = [float(row["latency_us"]) / 1000.0 for row in measured]
        bandwidths = [float(row["bandwidth_MBps"]) for row in measured]
        cpu_us = [float(row["cpu_user_us"]) + float(row["cpu_system_us"]) for row in measured]
        p50 = _percentile(lat_ms, 50)
        p95 = _percentile(lat_ms, 95)
        p99 = _percentile(lat_ms, 99)
        selected = {"p50": p50, "p95": p95, "p99": p99}[profile_mode]
        profiles[_profile_name(engine, cache_policy, threads)] = {
            "available": bool(measured),
            "measurement_count": len(measured),
            "p50_ms": p50,
            "p95_ms": p95,
            "p99_ms": p99,
            "selected_profile_mode": profile_mode,
            "selected_latency_ms": selected,
            "bandwidth_MBps": statistics.mean(bandwidths) if bandwidths else 0.0,
            "bandwidth_GBps": (statistics.mean(bandwidths) / 1024.0) if bandwidths else 0.0,
            "cpu_us_per_op": statistics.mean(cpu_us) if cpu_us else 0.0,
            "page_cache_policy": cache_policy,
            "threads": threads,
            "filesystem": _filesystem(Path(measured[0]["path"])) if measured else "unknown",
            "mount_options": _mount_options(Path(measured[0]["path"])) if measured else "unknown",
            "kernel": platform.release(),
            "driver": "posix",
        }
    profile = {
        "contract_version": 1,
        "generated_at": generated_at,
        "env_id": platform.node() or "local",
        "source_csv": str(source),
        "profile_mode": profile_mode,
        "provenance": {
            "contract": "shared/schema/tier_profile.schema.json",
            "generator": "io_path_bench.tier_profile.generate_tier_profile",
            "generator_version": 1,
            "source_csv_path": str(source),
            "source_csv_sha256": hashlib.sha256(source_bytes).hexdigest(),
            "source_csv_bytes": len(source_bytes),
            "source_row_count": len(rows),
            "eligible_measurement_row_count": sum(
                row.get("warmup") == "false" and not row.get("error") for row in rows
            ),
        },
        "profiles": profiles,
    }
    out_path = Path(out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(profile, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return profile


def _cache_policy(row: dict[str, str]) -> str:
    policy = row.get("cache_policy") or ""
    if policy:
        return policy
    if row.get("engine") == "odirect":
        return "direct"
    return "warm"


def _threads(row: dict[str, str]) -> int:
    raw = row.get("threads") or "1"
    try:
        return max(1, int(raw))
    except ValueError:
        return 1


def _profile_name(engine: str, cache_policy: str, threads: int) -> str:
    suffix = "" if threads == 1 else f"_{threads}t"
    if cache_policy == "warm" or (engine == "odirect" and cache_policy == "direct"):
        return f"local_file_{engine}{suffix}"
    return f"local_file_{engine}_{cache_policy}{suffix}"


def _read_rows(source_bytes: bytes) -> list[dict[str, str]]:
    with io.StringIO(source_bytes.decode("utf-8"), newline="") as f:
        return list(csv.DictReader(f))


def _percentile(values: list[float], pct: int) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[pct - 1]


def _filesystem(path: Path) -> str:
    try:
        output = subprocess.check_output(["df", "-T", str(path)], text=True, stderr=subprocess.DEVNULL)
        lines = output.strip().splitlines()
        if len(lines) >= 2:
            return lines[1].split()[1]
    except Exception:
        pass
    return "unknown"


def _mount_options(path: Path) -> str:
    try:
        target = path.resolve()
        best_mount = Path("/")
        best_opts = "unknown"
        for line in Path("/proc/mounts").read_text(encoding="utf-8").splitlines():
            parts = line.split()
            if len(parts) < 4:
                continue
            mount = Path(parts[1])
            try:
                target.relative_to(mount)
            except ValueError:
                continue
            if len(str(mount)) >= len(str(best_mount)):
                best_mount = mount
                best_opts = parts[3]
        return best_opts
    except Exception:
        return "unknown"
