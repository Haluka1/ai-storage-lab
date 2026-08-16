from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .cost_model import CostModel, TierProfile
from .memory_tier import MemoryTier
from .metadata import TierName
from .metadata_store import MetadataStore
from .metrics import KVStoreMetrics
from .nvme_tier import NVMeTier
from .s3_tier import S3Tier
from .tier_profile_import import profiles_from_tier_profile
from .tier_manager import MultiTierKVBlockStore


def build_store_from_config(config_path: str | Path) -> MultiTierKVBlockStore:
    config_file = Path(config_path)
    cfg = _load_config(config_file)
    base_dir = _config_base_dir(config_file)
    metadata_db = _resolve_path(base_dir, cfg.get("kvstore", {}).get("metadata_db", "./data/kvstore/metadata.sqlite3"))
    metadata_store = MetadataStore(metadata_db)
    metrics = KVStoreMetrics()
    tiers = []
    tier_cfg = cfg.get("tiers", {})
    memory_cfg = tier_cfg.get("memory", {})
    if memory_cfg.get("enabled", True):
        tiers.append(MemoryTier(int(memory_cfg.get("max_bytes", 64 * 1024 * 1024)), metadata_store, metrics=metrics))
    nvme_cfg = tier_cfg.get("nvme", {})
    if nvme_cfg.get("enabled", True):
        root_dir = _resolve_path(base_dir, nvme_cfg.get("root_dir", "./data/kvstore/nvme"))
        tiers.append(
            NVMeTier(
                root_dir=root_dir,
                max_bytes=int(nvme_cfg.get("max_bytes", 512 * 1024 * 1024)),
                metadata_store=metadata_store,
                fsync_on_store=bool(nvme_cfg.get("fsync_on_store", False)),
                use_direct_io=bool(nvme_cfg.get("use_direct_io", False)),
                layout_mode=str(nvme_cfg.get("layout_mode", "content_addressed")),
                segment_bytes=int(nvme_cfg.get("segment_bytes", 64 * 1024 * 1024)),
                metrics=metrics,
            )
        )
    s3_cfg = tier_cfg.get("s3", {})
    if s3_cfg.get("enabled", False):
        tiers.append(
            S3Tier(
                endpoint_url=str(s3_cfg.get("endpoint_url", "")) or None,
                bucket=str(s3_cfg.get("bucket", "kv-cache")),
                prefix=str(s3_cfg.get("prefix", "blocks/")),
                access_key_env=str(s3_cfg.get("access_key_env", "S3_ACCESS_KEY")),
                secret_key_env=str(s3_cfg.get("secret_key_env", "S3_SECRET_KEY")),
                connect_timeout_ms=int(s3_cfg.get("connect_timeout_ms", 500)),
                read_timeout_ms=int(s3_cfg.get("read_timeout_ms", 2000)),
                max_retries=int(s3_cfg.get("max_retries", 2)),
                fault_injection=s3_cfg.get("fault_injection"),
                metadata_store=metadata_store,
                metrics=metrics,
            )
        )
    return MultiTierKVBlockStore(tiers, metadata_store, _build_cost_model(cfg.get("cost_model", {})), metrics=metrics)


def _build_cost_model(cfg: dict[str, Any]) -> CostModel:
    tier_profile_path = str(cfg.get("tier_profile_path", "") or "")
    if tier_profile_path:
        profiles = profiles_from_tier_profile(tier_profile_path, str(cfg.get("tier_profile_mode", "p95")))
    else:
        profiles = {
            TierName.MEMORY: TierProfile(0.02, 80.0, 0.005, 24.0),
            TierName.NVME: TierProfile(0.35, 5.0, 0.02, 24.0),
            TierName.S3: TierProfile(25.0, 1.0, 0.03, 24.0),
        }
    for name, profile_cfg in cfg.get("profiles", {}).items():
        tier = TierName(name)
        profiles[tier] = TierProfile(
            fixed_latency_ms=float(profile_cfg.get("fixed_latency_ms", profiles[tier].fixed_latency_ms)),
            bandwidth_gbps=float(profile_cfg.get("bandwidth_gbps", profiles[tier].bandwidth_gbps)),
            deserialize_ms_per_mb=float(profile_cfg.get("deserialize_ms_per_mb", profiles[tier].deserialize_ms_per_mb)),
            h2d_bandwidth_gbps=float(profile_cfg.get("h2d_bandwidth_gbps", profiles[tier].h2d_bandwidth_gbps)),
        )
    return CostModel(
        profiles,
        load_benefit_threshold_ms=float(cfg.get("load_benefit_threshold_ms", 5.0)),
        s3_load_benefit_threshold_ms=float(cfg.get("s3_load_benefit_threshold_ms", 50.0)),
        slo_budget_guard_ms=float(cfg.get("slo_budget_guard_ms", 100.0)),
    )


def _load_config(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    stripped = raw.lstrip()
    if stripped.startswith("{"):
        return json.loads(raw)
    return _parse_simple_yaml(raw)


def _parse_simple_yaml(raw: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for line in raw.splitlines():
        line = line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        key, sep, value = line.strip().partition(":")
        if not sep:
            raise ValueError(f"invalid config line: {line}")
        while stack and indent <= stack[-1][0]:
            stack.pop()
        current = stack[-1][1]
        if value.strip() == "":
            child: dict[str, Any] = {}
            current[key] = child
            stack.append((indent, child))
        else:
            current[key] = _parse_scalar(value.strip())
    return root


def _parse_scalar(value: str) -> Any:
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _resolve_path(base_dir: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _config_base_dir(config_file: Path) -> Path:
    parent = config_file.resolve().parent
    if parent.name == "configs":
        return parent.parent
    return parent
