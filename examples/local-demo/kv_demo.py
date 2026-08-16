#!/usr/bin/env python3
"""Small file-backed KV correctness demo; no external service is used."""

from __future__ import annotations

import hashlib
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "kvstore"))

from kvstore.errors import ChecksumMismatch  # noqa: E402
from kvstore.metadata import BlockKey, KVMetadata  # noqa: E402
from kvstore.metadata_store import MetadataStore  # noqa: E402
from kvstore.nvme_tier import NVMeTier  # noqa: E402


def _key(label: str) -> BlockKey:
    return BlockKey(
        tenant_id="demo-tenant",
        model_id="demo-model",
        model_revision="revision-1",
        tokenizer_revision="approx-revision-1",
        block_hash=hashlib.sha256(label.encode("utf-8")).hexdigest(),
        lora_id="none",
        modality_key="text",
    )


def _metadata(key: BlockKey, size: int) -> KVMetadata:
    return KVMetadata(
        key=key,
        dtype="bytes",
        num_layers=1,
        num_kv_heads=1,
        head_dim=1,
        tokens=1,
        bytes=size,
        shape=(size,),
    )


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="ai-showcase-kv-demo-") as td:
        root = Path(td)
        metadata_store = MetadataStore(root / "metadata.sqlite3")
        tier = NVMeTier(root / "files", 1 << 20, metadata_store, fsync_on_store=False)
        try:
            corrupt_key = _key("corruption")
            payload = b"cache-block-payload"
            tier.store(corrupt_key, payload, _metadata(corrupt_key, len(payload)))
            loaded = tier.load(corrupt_key)
            if loaded.data != payload:
                raise RuntimeError("loaded payload differs from stored payload")
            print("store -> load: PASS")

            path = tier.layout.block_path(corrupt_key)
            raw = bytearray(path.read_bytes())
            raw[-1] ^= 0x01
            path.write_bytes(raw)
            try:
                tier.load(corrupt_key)
            except ChecksumMismatch:
                print("corruption detection: PASS")
            else:
                raise RuntimeError("corruption was not detected")

            delete_key = _key("delete")
            tier.store(delete_key, payload, _metadata(delete_key, len(payload)))
            if not tier.evict(delete_key) or tier.lookup(delete_key) is not None:
                raise RuntimeError("tombstone-backed delete did not complete")
            print("tombstone delete: PASS")
        finally:
            metadata_store.close()
    print("local KV demo: PASS (temporary state removed)")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError) as exc:
        print(f"local KV demo: FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
