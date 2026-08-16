from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ModelCapacityConfig:
    name: str
    num_layers: int
    num_kv_heads: int
    head_dim: int
    dtype_bytes: int
    context_length: int

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelCapacityConfig":
        return cls(
            name=str(data.get("name", "model")),
            num_layers=int(data["num_layers"]),
            num_kv_heads=int(data["num_kv_heads"]),
            head_dim=int(data["head_dim"]),
            dtype_bytes=int(data["dtype_bytes"]),
            context_length=int(data["context_length"]),
        )


@dataclass(frozen=True)
class TierCapacity:
    name: str
    capacity_bytes: int
    capacity_tokens: int
    estimated_full_context_concurrency: int


@dataclass(frozen=True)
class CapacityReport:
    model: ModelCapacityConfig
    kv_bytes_per_token: int
    per_request_kv_bytes: int
    tiers: list[TierCapacity]


def kv_bytes_per_token(cfg: ModelCapacityConfig) -> int:
    return cfg.num_layers * 2 * cfg.num_kv_heads * cfg.head_dim * cfg.dtype_bytes


def estimate_capacity(cfg: ModelCapacityConfig, tiers: dict[str, int]) -> CapacityReport:
    bpt = kv_bytes_per_token(cfg)
    per_request = bpt * cfg.context_length
    tier_rows = [
        TierCapacity(
            name=name,
            capacity_bytes=capacity,
            capacity_tokens=capacity // bpt,
            estimated_full_context_concurrency=capacity // per_request if per_request else 0,
        )
        for name, capacity in sorted(tiers.items())
    ]
    return CapacityReport(cfg, bpt, per_request, tier_rows)


def load_capacity_config(path: str | Path) -> tuple[ModelCapacityConfig, dict[str, int]]:
    obj = json.loads(Path(path).read_text(encoding="utf-8"))
    model = ModelCapacityConfig.from_dict(obj["model"])
    tiers = {name: int(item["capacity_bytes"]) for name, item in obj["tiers"].items()}
    return model, tiers


def write_capacity_report(report: CapacityReport, out: str | Path) -> None:
    lines = [
        "# KV Capacity Model",
        "",
        f"Model: `{report.model.name}`",
        "",
        f"KV bytes per token: `{report.kv_bytes_per_token}`",
        f"Per-request KV bytes at context length {report.model.context_length}: `{report.per_request_kv_bytes}`",
        "",
        "| tier | capacity_bytes | capacity_tokens | full_context_concurrency |",
        "|---|---:|---:|---:|",
    ]
    for tier in report.tiers:
        lines.append(
            f"| {tier.name} | {tier.capacity_bytes} | {tier.capacity_tokens} | {tier.estimated_full_context_concurrency} |"
        )
    lines.extend(
        [
            "",
            "Interpretation:",
            "",
            "- KV bytes/token includes both K and V tensors across all layers.",
            "- Full-context concurrency is a capacity estimate, not an SLO guarantee.",
            "- Multi-tier KV increases reusable context capacity but adds load latency, so it must be paired with the CostModel.",
        ]
    )
    out_path = Path(out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
