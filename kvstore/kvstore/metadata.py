from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import json
import re
from typing import Any
import time


MAX_IDENTITY_COMPONENT_BYTES = 128
_BLOCK_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


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

    def __post_init__(self) -> None:
        required = {
            "tenant_id": self.tenant_id,
            "model_id": self.model_id,
            "model_revision": self.model_revision,
            "tokenizer_revision": self.tokenizer_revision,
        }
        for name, value in required.items():
            _validate_identity_component(name, value, allow_empty=False)
        _validate_identity_component("lora_id", self.lora_id, allow_empty=True)
        _validate_identity_component(
            "modality_key", self.modality_key, allow_empty=True
        )
        if not isinstance(self.block_hash, str) or not _BLOCK_HASH_RE.fullmatch(
            self.block_hash
        ):
            raise ValueError("block_hash must be 64 lowercase hexadecimal characters")

    def namespace(self) -> str:
        """Return the versioned, injective SQLite namespace identity.

        A JSON array is deliberate: field boundaries and empty optional values
        cannot collide with user-controlled delimiter text.
        """

        return json.dumps(
            [
                "kv-block-key-v1",
                self.tenant_id,
                self.model_id,
                self.model_revision,
                self.tokenizer_revision,
                self.lora_id,
                self.modality_key,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )


def _validate_identity_component(
    name: str, value: str, *, allow_empty: bool
) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    if not allow_empty and not value:
        raise ValueError(f"{name} must not be empty")
    if len(value.encode("utf-8")) > MAX_IDENTITY_COMPONENT_BYTES:
        raise ValueError(
            f"{name} must be at most {MAX_IDENTITY_COMPONENT_BYTES} UTF-8 bytes"
        )
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{name} must not contain control characters")


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
