from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import json
import math
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

    def validate(self, *, require_checksum: bool = False) -> None:
        if not isinstance(self.key, BlockKey):
            raise ValueError("metadata key must be a BlockKey")
        if (
            not isinstance(self.dtype, str)
            or not self.dtype
            or len(self.dtype.encode("utf-8")) > 64
            or any(ord(char) < 32 or ord(char) == 127 for char in self.dtype)
        ):
            raise ValueError("metadata dtype must be a bounded non-empty string")
        for name in ("num_layers", "num_kv_heads", "head_dim"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"metadata {name} must be a positive integer")
        for name in ("tokens", "bytes", "reuse_count"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"metadata {name} must be a non-negative integer")
        if not isinstance(self.shape, tuple) or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in self.shape
        ):
            raise ValueError("metadata shape must contain non-negative integers")
        if self.checksum_algo != "sha256":
            raise ValueError("metadata checksum_algo must be sha256")
        if (require_checksum or self.checksum) and (
            not isinstance(self.checksum, str)
            or not _BLOCK_HASH_RE.fullmatch(self.checksum)
        ):
            raise ValueError("metadata checksum must be lowercase SHA-256 hex")
        for name in ("created_at", "last_access"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(f"metadata {name} must be a finite timestamp")
        if not isinstance(self.extra, dict):
            raise ValueError("metadata extra must be an object")
        try:
            json.dumps(self.extra, sort_keys=True, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("metadata extra must be finite JSON data") from exc

    def payload_descriptor(self) -> tuple[Any, ...]:
        """Fields that must remain stable for an active BlockKey."""

        return (
            self.key,
            self.dtype,
            self.num_layers,
            self.num_kv_heads,
            self.head_dim,
            self.tokens,
            self.bytes,
            self.shape,
            self.checksum,
            self.checksum_algo,
            self.extra,
        )

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
