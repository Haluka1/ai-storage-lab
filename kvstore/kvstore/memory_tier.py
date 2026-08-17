from __future__ import annotations

import threading
import time
from collections import OrderedDict

from .checksum import sha256_hex, verify_sha256
from .errors import (
    BlockNotFound,
    ChecksumMismatch,
    CorruptionCleanupFailed,
    CorruptionDetected,
    ImmutableBlockConflict,
    MetadataMismatch,
    StoreFull,
)
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
            if locations and key not in self._data:
                # Metadata can outlive the in-process memory map after restart or
                # synthetic fault injection. Treat it as a stale entry instead of
                # returning a location that cannot be loaded.
                self.metadata_store.delete(key, self.name)
                locations = []
            elif not locations and key in self._data:
                # Data without metadata was never published. Remove the orphan
                # rather than letting contains() disagree with lookup().
                self._used_bytes -= len(self._data.pop(key))
            if locations:
                self._validate_location(key, locations[0])
            self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="hit" if locations else "miss"))
            return locations[0] if locations else None

    def store(self, key: BlockKey, data: bytes, metadata: KVMetadata) -> StoreResult:
        start = time.perf_counter()
        if metadata.key != key:
            raise MetadataMismatch("store key does not match metadata key")
        data = bytes(data)
        if len(data) > self.max_bytes:
            raise StoreFull("block exceeds memory tier capacity")
        with self._lock:
            checksum = sha256_hex(data)
            metadata.checksum = checksum
            metadata.bytes = len(data)
            metadata.last_access = time.time()
            metadata.validate(require_checksum=True)
            existing_location = self.lookup(key)
            if existing_location is not None:
                if self._data[key] != data:
                    raise ImmutableBlockConflict(
                        f"{key.block_hash}: immutable BlockKey already has different payload"
                    )
                persisted = self.metadata_store.get_metadata(key, self.name)
                if (
                    persisted is None
                    or persisted.payload_descriptor()
                    != metadata.payload_descriptor()
                ):
                    raise ImmutableBlockConflict(
                        f"{key.block_hash}: immutable BlockKey metadata descriptor changed"
                    )
                existing_location.last_access = metadata.last_access
                self.metadata_store.upsert(existing_location, metadata)
                self._data.move_to_end(key)
                latency_ms = (time.perf_counter() - start) * 1000
                self._metric(
                    lambda m: m.kv_store_latency_seconds.observe(
                        latency_ms / 1000.0, tier=self.name.value, outcome="ok"
                    )
                )
                self._metric(
                    lambda m: m.kv_bytes_written_total.inc(
                        len(data), tier=self.name.value, outcome="ok"
                    )
                )
                return StoreResult(key, self.name, len(data), latency_ms, checksum)

            existing_bytes = len(self._data[key]) if key in self._data else 0
            self._ensure_capacity(len(data) - existing_bytes, exclude=key)
            previous = self._data.get(key)
            if key in self._data:
                self._used_bytes -= len(self._data[key])
                del self._data[key]
            self._data[key] = data
            self._used_bytes += len(data)
            loc = self._location_for(key, metadata, checksum, len(data))
            try:
                self.metadata_store.upsert(loc, metadata)
            except Exception:
                self._used_bytes -= len(self._data.pop(key))
                if previous is not None:
                    self._data[key] = previous
                    self._used_bytes += len(previous)
                raise
        latency_ms = (time.perf_counter() - start) * 1000
        self._metric(lambda m: m.kv_store_latency_seconds.observe(latency_ms / 1000.0, tier=self.name.value, outcome="ok"))
        self._metric(lambda m: m.kv_bytes_written_total.inc(len(data), tier=self.name.value, outcome="ok"))
        return StoreResult(key, self.name, len(data), latency_ms, checksum)

    def load(self, key: BlockKey) -> LoadResult:
        start = time.perf_counter()
        with self._lock:
            location = self.lookup(key)
            if location is None or key not in self._data:
                raise BlockNotFound(key.block_hash)
            if not self.metadata_store.acquire(key, self.name):
                raise BlockNotFound(key.block_hash)
            corruption_error: CorruptionDetected | None = None
            try:
                data = self._data[key]
                metadata = self.metadata_store.get_metadata(key, self.name)
                if metadata is None:
                    raise BlockNotFound(key.block_hash)
                if metadata.key != key:
                    raise MetadataMismatch(key.block_hash)
                if location.bytes != len(data) or metadata.bytes != len(data):
                    raise MetadataMismatch(f"{key.block_hash}: memory byte count mismatch")
                if location.checksum != metadata.checksum:
                    raise MetadataMismatch(f"{key.block_hash}: memory checksum mismatch")
                if not verify_sha256(data, metadata.checksum):
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
            except CorruptionDetected as exc:
                corruption_error = exc
                raise
            finally:
                try:
                    self.metadata_store.release(key, self.name)
                except Exception as cleanup_error:
                    if corruption_error is not None:
                        raise CorruptionCleanupFailed(
                            key.block_hash,
                            "release",
                            cleanup_error,
                            corruption_error,
                        ) from corruption_error
                    raise
                if corruption_error is not None:
                    try:
                        removed = self.evict(key)
                        if not removed and self.metadata_store.lookup(key, self.name):
                            raise RuntimeError("corrupt memory location remains active")
                    except Exception as cleanup_error:
                        raise CorruptionCleanupFailed(
                            key.block_hash,
                            "evict",
                            cleanup_error,
                            corruption_error,
                        ) from corruption_error

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
        return self.lookup(key) is not None

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

    def _location_for(
        self, key: BlockKey, metadata: KVMetadata, checksum: str, bytes_: int
    ) -> BlockLocation:
        return BlockLocation(
            key=key,
            tier=self.name,
            uri=f"memory://block/{key.block_hash}",
            bytes=bytes_,
            checksum=checksum,
            created_at=metadata.created_at,
            last_access=metadata.last_access,
        )

    def _validate_location(self, key: BlockKey, location: BlockLocation) -> None:
        if (
            location.key != key
            or location.tier != self.name
            or location.uri != f"memory://block/{key.block_hash}"
        ):
            raise MetadataMismatch("invalid persisted memory location")

    def _metric(self, fn) -> None:
        if self.metrics is None:
            return
        try:
            fn(self.metrics)
        except Exception:
            pass
