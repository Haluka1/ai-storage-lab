from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any
import time


class TierName(str, Enum):
    MEMORY = "memory"
    NVME = "nvme"
    S3 = "s3"


@dataclass(frozen=True)
class BlockKey:
    tenant_id: str
    model_id: str
    model_revision: str
    tokenizer_revision: str
    block_hash: str
    lora_id: str = ""
    modality_key: str = ""

    def namespace(self) -> str:
        return (
            f"tenant={self.tenant_id}/model={self.model_id}/"
            f"revision={self.model_revision}/tokenizer={self.tokenizer_revision}/"
            f"lora={self.lora_id or 'none'}/modality={self.modality_key or 'text'}"
        )


@dataclass
class KVMetadata:
    key: BlockKey
    dtype: str
    num_layers: int
    num_kv_heads: int
    head_dim: int
    tokens: int
    bytes: int
    shape: tuple[int, ...] = field(default_factory=tuple)
    checksum: str = ""
    checksum_algo: str = "sha256"
    created_at: float = field(default_factory=time.time)
    last_access: float = field(default_factory=time.time)
    reuse_count: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["shape"] = list(self.shape)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KVMetadata":
        key_data = data["key"]
        copied = dict(data)
        copied["key"] = BlockKey(**key_data)
        copied["shape"] = tuple(copied.get("shape") or ())
        return cls(**copied)


@dataclass
class BlockLocation:
    key: BlockKey
    tier: TierName
    uri: str
    bytes: int
    checksum: str
    created_at: float
    last_access: float
    locality: str = "unknown"
    transport: str = "unknown"
    cloud: str = "local"
    region: str = "local"
    zone: str = "local"
    cluster_id: str = "local"
    node_id: str = ""
    estimated_load_p95_ms: float | None = None
    estimated_transfer_p95_ms: float | None = None
    egress_cost_class: str = "none"
    ttl_seconds: float | None = None
    confidence: float = 1.0


@dataclass
class StoreResult:
    key: BlockKey
    tier: TierName
    bytes: int
    latency_ms: float
    checksum: str


@dataclass
class LoadResult:
    key: BlockKey
    tier: TierName
    data: bytes
    metadata: KVMetadata
    latency_ms: float
    from_prefetch: bool = False
