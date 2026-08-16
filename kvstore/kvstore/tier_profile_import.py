from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .cost_model import TierProfile
from .metadata import TierName


@dataclass(frozen=True)
class TierProfileImport:
    profiles: dict[TierName, TierProfile]
    sources: dict[TierName, str]
    provenance: dict[str, Any] = field(default_factory=dict)


def profiles_from_tier_profile(
    path: str | Path,
    profile_mode: str = "p95",
    local_file_profile: str = "local_file_pread",
    schema_path: str | Path | None = None,
) -> dict[TierName, TierProfile]:
    return import_profiles_from_tier_profile(
        path, profile_mode, local_file_profile, schema_path=schema_path
    ).profiles


def import_profiles_from_tier_profile(
    path: str | Path,
    profile_mode: str = "p95",
    local_file_profile: str = "local_file_pread",
    schema_path: str | Path | None = None,
) -> TierProfileImport:
    if profile_mode not in {"p50", "p95", "p99"}:
        raise ValueError("profile_mode must be one of p50, p95, p99")
    artifact_path = Path(path)
    artifact_bytes = artifact_path.read_bytes()
    obj = json.loads(artifact_bytes.decode("utf-8"))
    _validate_schema(obj, schema_path)
    profiles = obj.get("profiles", {})
    out = _default_profiles()
    sources = {
        TierName.MEMORY: "synthetic_default_explicit_fallback",
        TierName.NVME: "synthetic_default_explicit_fallback",
        TierName.S3: "synthetic_default_explicit_fallback",
    }
    nvme_profile_name = _select_first_available_profile(profiles, [local_file_profile, "file_posix_default"])
    if nvme_profile_name is None:
        raise ValueError(f"missing profile {local_file_profile} or file_posix_default")
    out[TierName.NVME] = _profile_from_entry(profiles[nvme_profile_name], profile_mode)
    sources[TierName.NVME] = f"tier_profile:{nvme_profile_name}:{profile_mode}"

    s3_profile_name = _select_first_present_profile(profiles, ["object_s3", "s3_http_default"])
    if s3_profile_name is not None:
        entry = profiles[s3_profile_name]
        if entry.get("available") is False:
            reason = str(entry.get("unavailable_reason") or entry.get("reason") or "unavailable")
            sources[TierName.S3] = f"explicit_fallback:{s3_profile_name}:unavailable:{reason}"
        else:
            out[TierName.S3] = _profile_from_entry(entry, profile_mode)
            sources[TierName.S3] = f"tier_profile:{s3_profile_name}:{profile_mode}"
    else:
        sources[TierName.S3] = "explicit_fallback:missing_s3_profile"
    embedded_provenance = obj.get("provenance")
    provenance = {
        "artifact_path": str(artifact_path),
        "artifact_sha256": hashlib.sha256(artifact_bytes).hexdigest(),
        "contract_version": int(obj["contract_version"]),
        "generated_at": str(obj.get("generated_at") or ""),
        "env_id": str(obj.get("env_id") or ""),
        "source_csv": str(obj.get("source_csv") or ""),
        "profile_mode": profile_mode,
        "local_file_profile": nvme_profile_name,
        "embedded": dict(embedded_provenance) if isinstance(embedded_provenance, dict) else {},
    }
    return TierProfileImport(out, sources, provenance)


def _select_first_available_profile(profiles: dict[str, Any], names: list[str]) -> str | None:
    for name in names:
        entry = profiles.get(name)
        if entry is not None and entry.get("available") is not False:
            return name
    return None


def _select_first_present_profile(profiles: dict[str, Any], names: list[str]) -> str | None:
    for name in names:
        if name in profiles:
            return name
    return None


def _profile_from_entry(entry: dict[str, Any], profile_mode: str) -> TierProfile:
    if entry.get("available") is False:
        raise ValueError("selected tier profile is unavailable")
    latency_key = f"{profile_mode}_ms"
    if latency_key not in entry:
        raise ValueError(f"missing {latency_key}")
    bandwidth_mbps = float(entry.get("bandwidth_MBps", 0.0))
    if bandwidth_mbps <= 0:
        bandwidth_mbps = float(entry.get("bandwidth_GBps", 0.0)) * 1024.0
    if bandwidth_mbps <= 0:
        raise ValueError("tier profile must include positive bandwidth")
    return TierProfile(
        fixed_latency_ms=float(entry[latency_key]),
        bandwidth_gbps=bandwidth_mbps / 1024.0,
        deserialize_ms_per_mb=0.02,
        h2d_bandwidth_gbps=24.0,
    )


def _default_profiles() -> dict[TierName, TierProfile]:
    return {
        TierName.MEMORY: TierProfile(0.02, 80.0, 0.005, 24.0),
        TierName.NVME: TierProfile(0.35, 5.0, 0.02, 24.0),
        TierName.S3: TierProfile(25.0, 1.0, 0.03, 24.0),
    }


def _validate_schema(obj: dict[str, Any], schema_path: str | Path | None) -> None:
    resolved_schema = (
        Path(schema_path)
        if schema_path is not None
        else Path(__file__).resolve().parents[2] / "shared/schema/tier_profile.schema.json"
    )
    if not resolved_schema.is_file():
        raise ValueError(
            "tier profile schema not found; pass schema_path when using the "
            "kvstore package outside this repository"
        )
    schema = json.loads(resolved_schema.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    errors = sorted(Draft202012Validator(schema).iter_errors(obj), key=lambda e: list(e.path))
    if errors:
        first = errors[0]
        path = ".".join(str(p) for p in first.path) or "<root>"
        raise ValueError(f"tier_profile schema error at {path}: {first.message}")
