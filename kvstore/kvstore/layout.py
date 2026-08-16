from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

from .metadata import BlockKey


class ContentAddressedLayout:
    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir)

    def block_path(self, key: BlockKey) -> Path:
        return self.root_dir.joinpath(*namespace_parts(key), f"{key.block_hash}.kv")


class SegmentedLayout:
    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir)

    def namespace_dir(self, key: BlockKey) -> Path:
        return self.root_dir.joinpath(*namespace_parts(key))


def _escape(value: str) -> str:
    return quote(value, safe="")


def namespace_parts(key: BlockKey) -> list[str]:
    return [
        _escape(key.tenant_id),
        _escape(key.model_id),
        _escape(key.model_revision),
        _escape(key.tokenizer_revision),
        _escape(key.lora_id or "none"),
        _escape(key.modality_key or "text"),
        key.block_hash[:2],
    ]
