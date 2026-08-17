from __future__ import annotations

import base64
from pathlib import Path

from .metadata import BlockKey


class ContentAddressedLayout:
    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir).expanduser().resolve(strict=False)

    def block_path(self, key: BlockKey) -> Path:
        candidate = self.root_dir.joinpath(*storage_key_parts(key))
        return _require_descendant(self.root_dir, candidate)


class SegmentedLayout:
    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir).expanduser().resolve(strict=False)

    def namespace_dir(self, key: BlockKey) -> Path:
        candidate = self.root_dir.joinpath(*namespace_parts(key))
        return _require_descendant(self.root_dir, candidate)


def _component(label: str, value: str) -> str:
    encoded = base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")
    return f"{label}-v1-{encoded.rstrip('=')}"


def namespace_parts(key: BlockKey) -> list[str]:
    return [
        _component("tenant", key.tenant_id),
        _component("model", key.model_id),
        _component("revision", key.model_revision),
        _component("tokenizer", key.tokenizer_revision),
        _component("lora", key.lora_id),
        _component("modality", key.modality_key),
        f"hash-v1-{key.block_hash[:2]}",
    ]


def storage_key_parts(key: BlockKey) -> list[str]:
    return [*namespace_parts(key), f"hash-v1-{key.block_hash}.kv"]


def _require_descendant(root: Path, candidate: Path) -> Path:
    resolved_root = root.resolve(strict=False)
    resolved_candidate = candidate.resolve(strict=False)
    try:
        resolved_candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError("generated block path escapes the configured root") from exc
    return resolved_candidate
