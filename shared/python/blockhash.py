from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass

EMPTY_PARENT = "0" * 64
FIELD_ORDER = (
    "tenant_id",
    "tenant_salt",
    "model_id",
    "model_revision",
    "tokenizer_revision",
    "lora_id",
    "modality_key",
    "cache_salt",
)


@dataclass(frozen=True)
class IsolationKey:
    tenant_id: str
    tenant_salt: str
    model_id: str
    model_revision: str
    tokenizer_revision: str
    lora_id: str = ""
    modality_key: str = ""
    cache_salt: str = ""

    @classmethod
    def from_dict(cls, data: dict) -> "IsolationKey":
        return cls(**{field: data.get(field, "") for field in FIELD_ORDER})


def _write_string(h: "hashlib._Hash", value: str) -> None:
    encoded = value.encode("utf-8")
    h.update(struct.pack(">Q", len(encoded)))
    h.update(encoded)


def _write_uint64(h: "hashlib._Hash", value: int) -> None:
    if value < 0:
        raise ValueError("token ids must be non-negative")
    h.update(struct.pack(">Q", value))


def compute_blocks(tokens: list[int], key: IsolationKey, block_size_tokens: int = 16) -> list[str]:
    if block_size_tokens <= 0:
        raise ValueError("block_size_tokens must be positive")
    parent = EMPTY_PARENT
    out: list[str] = []
    for start in range(0, len(tokens), block_size_tokens):
        block = tokens[start : start + block_size_tokens]
        h = hashlib.sha256()
        _write_string(h, parent)
        for field in FIELD_ORDER:
            _write_string(h, getattr(key, field))
        _write_uint64(h, len(block))
        for token in block:
            _write_uint64(h, int(token))
        parent = h.hexdigest()
        out.append(parent)
    return out
