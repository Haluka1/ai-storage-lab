from __future__ import annotations

import threading
import time
from collections import OrderedDict

from .checksum import sha256_hex, verify_sha256
from .errors import BlockNotFound, ChecksumMismatch, StoreFull
from .metadata import BlockKey, BlockLocation, KVMetadata, LoadResult, StoreResult, TierName
from .metadata_store import MetadataStore
from .metrics import KVStoreMetrics


class MemoryTier:
    name = TierName.MEMORY

    def __init__(self, max_bytes: int, metadata_store: MetadataStore, metrics: KVStoreMetrics | None = None):
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self.max_bytes = max_bytes
        self.metadata_store = metadata_store
        self.metrics = metrics
        self._data: OrderedDict[BlockKey, bytes] = OrderedDict()
        self._used_bytes = 0
        self._lock = threading.RLock()

    def lookup(self, key: BlockKey) -> BlockLocation | None:
        with self._lock:
            locations = self.metadata_store.lookup(key, self.name)
            present = bool(locations and key in self._data)
            if locations and not present:
                # Metadata can outlive the in-process memory map after restart or
                # synthetic fault injection. Treat it as a stale entry instead of
                # returning a location that cannot be loaded.
                self.metadata_store.delete(key, self.name)
                locations = []
            self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="hit" if locations else "miss"))
            return locations[0] if locations else None

    def store(self, key: BlockKey, data: bytes, metadata: KVMetadata) -> StoreResult:
        start = time.perf_counter()
        if len(data) > self.max_bytes:
            raise StoreFull("block exceeds memory tier capacity")
        with self._lock:
            existing_bytes = len(self._data[key]) if key in self._data else 0
            self._ensure_capacity(len(data) - existing_bytes, exclude=key)
            if key in self._data:
                self._used_bytes -= len(self._data[key])
                del self._data[key]
            checksum = sha256_hex(data)
            metadata.checksum = checksum
            metadata.bytes = len(data)
            metadata.last_access = time.time()
            self._data[key] = bytes(data)
            self._used_bytes += len(data)
            loc = BlockLocation(key=key, tier=self.name, uri=f"memory://{key.namespace()}/{key.block_hash}", bytes=len(data), checksum=checksum, created_at=metadata.created_at, last_access=metadata.last_access)
            self.metadata_store.upsert(loc, metadata)
        latency_ms = (time.perf_counter() - start) * 1000
        self._metric(lambda m: m.kv_store_latency_seconds.observe(latency_ms / 1000.0, tier=self.name.value, outcome="ok"))
        self._metric(lambda m: m.kv_bytes_written_total.inc(len(data), tier=self.name.value, outcome="ok"))
        return StoreResult(key, self.name, len(data), latency_ms, checksum)

    def load(self, key: BlockKey) -> LoadResult:
        start = time.perf_counter()
        with self._lock:
            if key not in self._data:
                raise BlockNotFound(key.block_hash)
            self.metadata_store.acquire(key, self.name)
            mismatch = False
            try:
                data = self._data[key]
                metadata = self.metadata_store.get_metadata(key, self.name)
                if metadata is None:
                    raise BlockNotFound(key.block_hash)
                if not verify_sha256(data, metadata.checksum):
                    mismatch = True
                    self._metric(lambda m: m.kv_checksum_mismatch_total.inc(tier=self.name.value, outcome="error"))
                    raise ChecksumMismatch(key.block_hash)
                self._data.move_to_end(key)
                self.metadata_store.touch(key, self.name)
                metadata.last_access = time.time()
                metadata.reuse_count += 1
                latency_ms = (time.perf_counter() - start) * 1000
                self._metric(lambda m: m.kv_load_latency_seconds.observe(latency_ms / 1000.0, tier=self.name.value, outcome="ok"))
                self._metric(lambda m: m.kv_bytes_read_total.inc(len(data), tier=self.name.value, outcome="ok"))
                return LoadResult(key, self.name, data, metadata, latency_ms)
            finally:
                self.metadata_store.release(key, self.name)
                if mismatch:
                    self.evict(key)

    def evict(self, key: BlockKey) -> bool:
        with self._lock:
            if key not in self._data:
                return False
            if not self.metadata_store.delete(key, self.name):
                return False
            self._used_bytes -= len(self._data[key])
            del self._data[key]
            return True

    def contains(self, key: BlockKey) -> bool:
        with self._lock:
            return key in self._data

    def stats(self) -> dict:
        with self._lock:
            return {"tier": self.name.value, "used_bytes": self._used_bytes, "max_bytes": self.max_bytes, "blocks": len(self._data)}

    def _ensure_capacity(self, incoming: int, exclude: BlockKey) -> None:
        if incoming <= 0:
            return
        while self._used_bytes + incoming > self.max_bytes:
            evicted = False
            for key in list(self._data.keys()):
                if key == exclude:
                    continue
                if self.evict(key):
                    evicted = True
                    break
            if not evicted:
                raise StoreFull("no evictable memory blocks")

    def _metric(self, fn) -> None:
        if self.metrics is None:
            return
        try:
            fn(self.metrics)
        except Exception:
            pass
