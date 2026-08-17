from __future__ import annotations

import json
import os
import struct
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

from .checksum import sha256_hex, verify_sha256
from .errors import BlockNotFound, ChecksumMismatch, CorruptionCleanupFailed, MetadataMismatch, StoreFull
from .layout import ContentAddressedLayout, SegmentedLayout
from .metadata import BlockKey, BlockLocation, KVMetadata, LoadResult, StoreResult, TierName
from .metadata_store import MetadataStore
from .metrics import KVStoreMetrics

MAGIC = b"KVBLK001"
HEADER_LEN_STRUCT = struct.Struct(">I")
DEFAULT_SEGMENT_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class SegmentRef:
    path: Path
    offset: int
    record_bytes: int


class NVMeTier:
    name = TierName.NVME

    def __init__(
        self,
        root_dir: str | Path,
        max_bytes: int,
        metadata_store: MetadataStore,
        fsync_on_store: bool = False,
        use_direct_io: bool = False,
        layout_mode: str = "content_addressed",
        segment_bytes: int = DEFAULT_SEGMENT_BYTES,
        metrics: KVStoreMetrics | None = None,
    ):
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if layout_mode not in {"content_addressed", "segment"}:
            raise ValueError("layout_mode must be content_addressed or segment")
        if segment_bytes <= 0:
            raise ValueError("segment_bytes must be positive")
        if use_direct_io:
            raise NotImplementedError(
                "FileBackedTier does not implement O_DIRECT; use io-profile for O_DIRECT measurement"
            )
        self.root_dir = Path(root_dir).expanduser().resolve(strict=False)
        self.max_bytes = max_bytes
        self.metadata_store = metadata_store
        self.fsync_on_store = fsync_on_store
        self.use_direct_io = use_direct_io
        self.layout_mode = layout_mode
        self.segment_bytes = segment_bytes
        self.metrics = metrics
        self.layout = ContentAddressedLayout(self.root_dir)
        self.segment_layout = SegmentedLayout(self.root_dir / "segments")
        self._capacity_lock = threading.RLock()
        self._segment_lock = threading.RLock()
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def lookup(self, key: BlockKey) -> BlockLocation | None:
        locations = self.metadata_store.lookup(key, self.name)
        if locations:
            exists = self._location_exists(locations[0])
            self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="hit" if exists else "stale"))
            if exists:
                return locations[0]
            with self.metadata_store.mutation():
                locations = self.metadata_store.lookup(key, self.name)
                if locations and self._location_exists(locations[0]):
                    return locations[0]
                self.metadata_store.delete(key, self.name)
            return None
        if self.metadata_store.is_deleting(key, self.name):
            self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="miss"))
            return None
        if self.layout_mode == "segment":
            self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="miss"))
            return None
        with self.metadata_store.mutation():
            # Recheck after entering the shared mutation boundary.  A second
            # Tier instance may have installed a tombstone after the optimistic
            # lookup above.
            locations = self.metadata_store.lookup(key, self.name)
            if locations:
                if self._location_exists(locations[0]):
                    return locations[0]
                self.metadata_store.delete(key, self.name)
                return None
            if self.metadata_store.is_deleting(key, self.name):
                return None
            path = self.layout.block_path(key)
            if not path.exists():
                self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="miss"))
                return None
            metadata, header = self._read_metadata_header(path)
            if metadata.key != key:
                return None
            loc = self._location_for(key, path, int(header["payload_bytes"]), str(header["checksum"]), metadata.created_at, metadata.last_access)
            self.metadata_store.upsert(loc, metadata)
            return loc

    def store(self, key: BlockKey, data: bytes, metadata: KVMetadata) -> StoreResult:
        start = time.perf_counter()
        with self.metadata_store.mutation(), self._capacity_lock:
            if self.metadata_store.is_deleting(key, self.name):
                raise RuntimeError(
                    "cannot store a block while its delete tombstone is pending"
                )
            if len(data) > self.max_bytes:
                raise StoreFull("block exceeds nvme tier capacity")
            self._ensure_capacity(len(data), exclude=key)
            checksum = sha256_hex(data)
            metadata.checksum = checksum
            metadata.bytes = len(data)
            metadata.last_access = time.time()
            header = {
                "version": 1,
                "layout_mode": self.layout_mode,
                "checksum": checksum,
                "payload_bytes": len(data),
                "metadata": metadata.to_dict(),
            }
            encoded_header = json.dumps(
                header, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            if self.layout_mode == "segment":
                ref = self._append_segment_record(key, encoded_header, data)
                loc = self._segment_location_for(
                    key,
                    ref,
                    len(data),
                    checksum,
                    metadata.created_at,
                    metadata.last_access,
                )
            else:
                path = self.layout.block_path(key)
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp_path = self._write_tmp(path, encoded_header, data)
                os.replace(tmp_path, path)
                loc = self._location_for(
                    key,
                    path,
                    len(data),
                    checksum,
                    metadata.created_at,
                    metadata.last_access,
                )
            self.metadata_store.upsert(loc, metadata)
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

    def load(self, key: BlockKey) -> LoadResult:
        start = time.perf_counter()
        if self.lookup(key) is None:
            raise BlockNotFound(key.block_hash)
        loc = self.metadata_store.lookup_and_acquire(key, self.name)
        if loc is None:
            raise BlockNotFound(key.block_hash)
        corruption_error: ChecksumMismatch | None = None
        try:
            header, payload = self._read_location(loc)
            metadata = KVMetadata.from_dict(header["metadata"])
            if metadata.key != key:
                raise MetadataMismatch(key.block_hash)
            if len(payload) != int(header["payload_bytes"]):
                self._metric(
                    lambda m: m.kv_checksum_mismatch_total.inc(
                        tier=self.name.value, outcome="error"
                    )
                )
                raise ChecksumMismatch(key.block_hash)
            if not verify_sha256(payload, str(header["checksum"])):
                self._metric(
                    lambda m: m.kv_checksum_mismatch_total.inc(
                        tier=self.name.value, outcome="error"
                    )
                )
                raise ChecksumMismatch(key.block_hash)
            metadata.checksum = str(header["checksum"])
            metadata.bytes = len(payload)
            self.metadata_store.touch(key, self.name)
            persisted = self.metadata_store.get_metadata(key, self.name)
            if persisted is not None:
                metadata.last_access = persisted.last_access
                metadata.reuse_count = persisted.reuse_count
            latency_ms = (time.perf_counter() - start) * 1000
            self._metric(
                lambda m: m.kv_load_latency_seconds.observe(
                    latency_ms / 1000.0, tier=self.name.value, outcome="ok"
                )
            )
            self._metric(
                lambda m: m.kv_bytes_read_total.inc(
                    len(payload), tier=self.name.value, outcome="ok"
                )
            )
            return LoadResult(key, self.name, payload, metadata, latency_ms)
        except ChecksumMismatch as exc:
            corruption_error = exc
            raise
        finally:
            try:
                self.metadata_store.release(key, self.name)
            except Exception as cleanup_error:
                if corruption_error is not None:
                    raise CorruptionCleanupFailed(
                        key.block_hash, "release", cleanup_error
                    ) from corruption_error
                raise
            if corruption_error is not None:
                try:
                    removed = self.evict(key)
                    if not removed and self.metadata_store.lookup(key, self.name):
                        raise RuntimeError("corrupt file location remains active")
                except Exception as cleanup_error:
                    raise CorruptionCleanupFailed(
                        key.block_hash, "evict", cleanup_error
                    ) from corruption_error

    def evict(self, key: BlockKey) -> bool:
        with self.metadata_store.mutation(), self._capacity_lock:
            location = self.metadata_store.begin_delete(key, self.name)
            if location is None:
                return False
            if location.uri.startswith("segment://"):
                # A segment record is append-only and is made unreachable by the
                # metadata tombstone.  Offline compaction reclaims its bytes.
                if not self.metadata_store.finish_delete(key, self.name):
                    raise RuntimeError("delete tombstone disappeared before metadata commit")
                return True
            path = self._path_from_location(location) or self.layout.block_path(key)
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            # Any other unlink error deliberately leaves the durable tombstone in
            # place.  lookup() will not lazy-rehydrate the still-present file and a
            # later evict() can retry the physical deletion.
            if not self.metadata_store.finish_delete(key, self.name):
                raise RuntimeError("delete tombstone disappeared before metadata commit")
            return True

    def contains(self, key: BlockKey) -> bool:
        return self.lookup(key) is not None

    def stats(self) -> dict:
        return {
            "tier": self.name.value,
            "used_bytes": self.metadata_store.bytes_used(self.name),
            "max_bytes": self.max_bytes,
            "root_dir": str(self.root_dir),
            "fsync_on_store": self.fsync_on_store,
            "use_direct_io": self.use_direct_io,
            "layout_mode": self.layout_mode,
            "segment_bytes": self.segment_bytes if self.layout_mode == "segment" else 0,
            "physical_bytes": self._physical_bytes(),
        }

    def compact_segments(self, dry_run: bool = False) -> dict[str, Any]:
        if self.layout_mode != "segment":
            return {
                "layout_mode": self.layout_mode,
                "dry_run": dry_run,
                "status": "skipped",
                "reason": "layout_mode_not_segment",
                "logical_bytes": self.metadata_store.bytes_used(self.name),
                "physical_bytes_before": self._physical_bytes(),
                "physical_bytes_after": self._physical_bytes(),
                "moved_records": 0,
                "skipped_records": 0,
                "removed_segment_files": 0,
                "bytes_reclaimed": 0,
            }
        with self.metadata_store.mutation(), self._capacity_lock, self._segment_lock:
            before = self._physical_bytes()
            entries = [
                (loc, metadata)
                for loc, metadata, _in_flight in self.metadata_store.tier_entries(self.name, include_in_flight=False)
                if loc.uri.startswith("segment://") and self._location_exists(loc)
            ]
            inflight = [
                loc
                for loc, _metadata, in_flight in self.metadata_store.tier_entries(self.name, include_in_flight=True)
                if in_flight > 0 and loc.uri.startswith("segment://")
            ]
            if dry_run:
                return {
                    "layout_mode": self.layout_mode,
                    "dry_run": True,
                    "status": "ok",
                    "logical_bytes": self.metadata_store.bytes_used(self.name),
                    "physical_bytes_before": before,
                    "physical_bytes_after": before,
                    "active_segment_records": len(entries),
                    "inflight_segment_records": len(inflight),
                    "moved_records": 0,
                    "skipped_records": 0,
                    "removed_segment_files": 0,
                    "bytes_reclaimed": 0,
                }
            compaction_id = f"{time.time_ns()}"
            moved = 0
            skipped = 0
            for loc, snapshot_metadata in entries:
                self.metadata_store.acquire(loc.key, self.name)
                try:
                    current = self.metadata_store.lookup(loc.key, self.name)
                    if not current or current[0].uri != loc.uri:
                        skipped += 1
                        continue
                    header, payload = self._read_location(loc)
                    header_metadata = KVMetadata.from_dict(header["metadata"])
                    if header_metadata.key != loc.key:
                        raise MetadataMismatch(loc.key.block_hash)
                    if len(payload) != int(header["payload_bytes"]) or not verify_sha256(payload, str(header["checksum"])):
                        raise ChecksumMismatch(loc.key.block_hash)
                    metadata = snapshot_metadata
                    metadata.checksum = str(header["checksum"])
                    metadata.bytes = len(payload)
                    metadata.last_access = loc.last_access
                    header["metadata"] = metadata.to_dict()
                    encoded_header = json.dumps(header, sort_keys=True, separators=(",", ":")).encode("utf-8")
                    ref = self._append_compacted_segment_record(loc.key, encoded_header, payload, compaction_id)
                    new_loc = self._segment_location_for(loc.key, ref, len(payload), str(header["checksum"]), loc.created_at, loc.last_access)
                    self.metadata_store.upsert(new_loc, metadata)
                    moved += 1
                finally:
                    self.metadata_store.release(loc.key, self.name)
            removed, removed_bytes = self._remove_unreferenced_segment_files()
            after = self._physical_bytes()
            return {
                "layout_mode": self.layout_mode,
                "dry_run": False,
                "status": "ok",
                "logical_bytes": self.metadata_store.bytes_used(self.name),
                "physical_bytes_before": before,
                "physical_bytes_after": after,
                "active_segment_records": len(entries),
                "inflight_segment_records": len(inflight),
                "moved_records": moved,
                "skipped_records": skipped,
                "removed_segment_files": removed,
                "removed_segment_bytes": removed_bytes,
                "bytes_reclaimed": max(before - after, 0),
            }

    def _location_for(self, key: BlockKey, path: Path, bytes_: int, checksum: str, created_at: float, last_access: float) -> BlockLocation:
        return BlockLocation(
            key=key,
            tier=self.name,
            uri=f"file://{path}",
            bytes=bytes_,
            checksum=checksum,
            created_at=created_at,
            last_access=last_access,
            locality="same_node",
            transport="file_posix_default",
        )

    def _segment_location_for(self, key: BlockKey, ref: SegmentRef, bytes_: int, checksum: str, created_at: float, last_access: float) -> BlockLocation:
        return BlockLocation(
            key=key,
            tier=self.name,
            uri=self._segment_uri(ref),
            bytes=bytes_,
            checksum=checksum,
            created_at=created_at,
            last_access=last_access,
            locality="same_node",
            transport="file_posix_default",
        )

    def _ensure_capacity(self, incoming: int, exclude: BlockKey) -> None:
        current = self.metadata_store.bytes_used(self.name)
        existing = self.lookup(exclude)
        if existing is not None:
            current -= existing.bytes
        while current + incoming > self.max_bytes:
            evicted = False
            for loc in self.metadata_store.lru_candidates(self.name, 16):
                if loc.key == exclude:
                    continue
                if self.evict(loc.key):
                    current -= loc.bytes
                    evicted = True
                    break
            if not evicted:
                raise StoreFull("no evictable nvme blocks")

    def _write_tmp(self, path: Path, encoded_header: bytes, data: bytes) -> Path:
        fd, tmp = tempfile.mkstemp(prefix=f".{path.stem}.", suffix=".tmp", dir=path.parent)
        tmp_path = Path(tmp)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(MAGIC)
                f.write(HEADER_LEN_STRUCT.pack(len(encoded_header)))
                f.write(encoded_header)
                f.write(data)
                if self.fsync_on_store:
                    f.flush()
                    os.fsync(f.fileno())
            return tmp_path
        except Exception:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            raise

    def _append_segment_record(self, key: BlockKey, encoded_header: bytes, data: bytes) -> SegmentRef:
        record = MAGIC + HEADER_LEN_STRUCT.pack(len(encoded_header)) + encoded_header + data
        with self._segment_lock:
            path = self._segment_path_for_append(key, len(record))
            path.parent.mkdir(parents=True, exist_ok=True)
            offset = path.stat().st_size if path.exists() else 0
            with path.open("ab") as f:
                f.write(record)
                if self.fsync_on_store:
                    f.flush()
                    os.fsync(f.fileno())
            return SegmentRef(path=path, offset=offset, record_bytes=len(record))

    def _append_compacted_segment_record(self, key: BlockKey, encoded_header: bytes, data: bytes, compaction_id: str) -> SegmentRef:
        record = MAGIC + HEADER_LEN_STRUCT.pack(len(encoded_header)) + encoded_header + data
        path = self._compact_segment_path_for_append(key, len(record), compaction_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        offset = path.stat().st_size if path.exists() else 0
        with path.open("ab") as f:
            f.write(record)
            if self.fsync_on_store:
                f.flush()
                os.fsync(f.fileno())
        return SegmentRef(path=path, offset=offset, record_bytes=len(record))

    def _segment_path_for_append(self, key: BlockKey, record_bytes: int) -> Path:
        namespace_dir = self.segment_layout.namespace_dir(key)
        existing = sorted(namespace_dir.glob("segment-*.kvseg")) if namespace_dir.exists() else []
        if existing:
            last = existing[-1]
            last_size = last.stat().st_size
            if last_size == 0 or last_size + record_bytes <= self.segment_bytes:
                return last
            last_id = _segment_id(last)
            return namespace_dir / f"segment-{last_id + 1:06d}.kvseg"
        return namespace_dir / "segment-000000.kvseg"

    def _compact_segment_path_for_append(self, key: BlockKey, record_bytes: int, compaction_id: str) -> Path:
        namespace_dir = self.segment_layout.namespace_dir(key)
        pattern = f"compact-{compaction_id}-*.kvseg"
        existing = sorted(namespace_dir.glob(pattern)) if namespace_dir.exists() else []
        if existing:
            last = existing[-1]
            last_size = last.stat().st_size
            if last_size == 0 or last_size + record_bytes <= self.segment_bytes:
                return last
            last_id = _segment_id(last)
            return namespace_dir / f"compact-{compaction_id}-{last_id + 1:06d}.kvseg"
        return namespace_dir / f"compact-{compaction_id}-000000.kvseg"

    def _read_location(self, location: BlockLocation) -> tuple[dict[str, Any], bytes]:
        if location.uri.startswith("segment://"):
            try:
                ref = self._segment_ref_from_uri(location.uri)
            except ValueError as exc:
                raise ChecksumMismatch(location.key.block_hash) from exc
            return self._read_segment_record(ref)
        path = self._path_from_location(location) or self.layout.block_path(location.key)
        return self._read_file(path)

    def _read_segment_record(self, ref: SegmentRef) -> tuple[dict[str, Any], bytes]:
        with ref.path.open("rb") as f:
            f.seek(ref.offset)
            start = f.tell()
            header, payload = self._read_record(f, str(ref.path))
            if ref.record_bytes > 0 and f.tell() - start != ref.record_bytes:
                raise ChecksumMismatch(str(ref.path))
        return header, payload

    def _read_metadata_header(self, path: Path) -> tuple[KVMetadata, dict[str, Any]]:
        with path.open("rb") as f:
            header, _payload = self._read_record(f, str(path), read_payload=False)
        return KVMetadata.from_dict(header["metadata"]), header

    def _read_file(self, path: Path) -> tuple[dict[str, Any], bytes]:
        with path.open("rb") as f:
            header, payload = self._read_record(f, str(path))
        return header, payload

    def _read_record(self, f, source: str, read_payload: bool = True) -> tuple[dict[str, Any], bytes]:
        magic = f.read(len(MAGIC))
        if magic != MAGIC:
            raise ChecksumMismatch(source)
        raw_len = f.read(HEADER_LEN_STRUCT.size)
        if len(raw_len) != HEADER_LEN_STRUCT.size:
            raise ChecksumMismatch(source)
        header_len = HEADER_LEN_STRUCT.unpack(raw_len)[0]
        raw_header = f.read(header_len)
        if len(raw_header) != header_len:
            raise ChecksumMismatch(source)
        header = json.loads(raw_header.decode("utf-8"))
        if not read_payload:
            return header, b""
        payload = f.read(int(header["payload_bytes"]))
        return header, payload

    def _location_exists(self, location: BlockLocation) -> bool:
        if location.uri.startswith("segment://"):
            try:
                ref = self._segment_ref_from_uri(location.uri)
            except ValueError:
                return False
            return ref.path.exists() and ref.offset + ref.record_bytes <= ref.path.stat().st_size
        path = self._path_from_location(location) or self.layout.block_path(location.key)
        return path.exists()

    def _path_from_location(self, location: BlockLocation) -> Path | None:
        if not location.uri.startswith("file://"):
            return None
        parsed = urlparse(location.uri)
        return Path(unquote(parsed.path))

    def _segment_uri(self, ref: SegmentRef) -> str:
        return f"segment://{quote(str(ref.path), safe='/')}?offset={ref.offset}&record_bytes={ref.record_bytes}"

    def _segment_ref_from_uri(self, uri: str) -> SegmentRef:
        parsed = urlparse(uri)
        if parsed.scheme != "segment":
            raise ValueError("not a segment uri")
        query = parse_qs(parsed.query)
        try:
            offset = int(query["offset"][0])
            record_bytes = int(query["record_bytes"][0])
        except (KeyError, IndexError, ValueError) as exc:
            raise ValueError("invalid segment uri") from exc
        if offset < 0 or record_bytes <= 0:
            raise ValueError("invalid segment bounds")
        return SegmentRef(Path(unquote(parsed.path)), offset, record_bytes)

    def _physical_bytes(self) -> int:
        if not self.root_dir.exists():
            return 0
        suffixes = {".kv", ".kvseg"}
        return sum(path.stat().st_size for path in self.root_dir.rglob("*") if path.is_file() and path.suffix in suffixes)

    def _remove_unreferenced_segment_files(self) -> tuple[int, int]:
        referenced: set[Path] = set()
        for loc, _metadata, _in_flight in self.metadata_store.tier_entries(self.name, include_in_flight=True):
            if not loc.uri.startswith("segment://"):
                continue
            try:
                referenced.add(self._segment_ref_from_uri(loc.uri).path)
            except ValueError:
                continue
        removed = 0
        removed_bytes = 0
        if not self.segment_layout.root_dir.exists():
            return removed, removed_bytes
        for path in sorted(self.segment_layout.root_dir.rglob("*.kvseg")):
            if path in referenced:
                continue
            try:
                size = path.stat().st_size
                path.unlink()
                removed += 1
                removed_bytes += size
            except FileNotFoundError:
                pass
        return removed, removed_bytes

    def _metric(self, fn) -> None:
        if self.metrics is None:
            return
        try:
            fn(self.metrics)
        except Exception:
            pass


def _segment_id(path: Path) -> int:
    try:
        return int(path.stem.split("-")[-1])
    except ValueError:
        return 0
