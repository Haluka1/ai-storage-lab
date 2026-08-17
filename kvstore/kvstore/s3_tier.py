from __future__ import annotations

import os
import time
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

from .checksum import sha256_hex, verify_sha256
from .errors import (
    BlockNotFound,
    ChecksumMismatch,
    CorruptionCleanupFailed,
    CorruptionDetected,
    ImmutableBlockConflict,
    MetadataMismatch,
    RecordFormatError,
    StoreFull,
    TierUnavailable,
)
from .layout import storage_key_parts
from .metadata import BlockKey, BlockLocation, KVMetadata, LoadResult, StoreResult, TierName
from .metadata_store import MetadataStore
from .metrics import KVStoreMetrics
from .record import (
    HEADER_LEN_STRUCT,
    MAGIC,
    MAX_HEADER_BYTES,
    decode_record_header,
    encode_record_header,
)
from .s3_fault_injection import FaultInjectingS3Client, S3FaultInjectionConfig

MAX_S3_PAYLOAD_BYTES = 16 * 1024 * 1024 * 1024


class S3Tier:
    name = TierName.S3

    def __init__(
        self,
        bucket: str,
        prefix: str,
        metadata_store: MetadataStore,
        endpoint_url: str | None = None,
        access_key_env: str = "S3_ACCESS_KEY",
        secret_key_env: str = "S3_SECRET_KEY",
        connect_timeout_ms: int = 500,
        read_timeout_ms: int = 2000,
        max_retries: int = 2,
        client: Any | None = None,
        fault_injection: dict[str, Any] | S3FaultInjectionConfig | None = None,
        metrics: KVStoreMetrics | None = None,
    ):
        if not bucket:
            raise ValueError("bucket must not be empty")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.metadata_store = metadata_store
        self.endpoint_url = endpoint_url
        self.connect_timeout_ms = connect_timeout_ms
        self.read_timeout_ms = read_timeout_ms
        self.max_retries = max_retries
        self.metrics = metrics
        base_client = client or self._build_client(endpoint_url, access_key_env, secret_key_env, connect_timeout_ms, read_timeout_ms, max_retries)
        self.client = _maybe_fault_inject(base_client, fault_injection)

    def lookup(self, key: BlockKey) -> BlockLocation | None:
        locations = self.metadata_store.lookup(key, self.name)
        if locations:
            location = locations[0]
            bucket, object_key = self._location_target(location)
            if self._head_exists(object_key, bucket=bucket):
                self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="hit"))
                return location
            # A store from another Tier instance may have replaced the object
            # after the optimistic HEAD.  Recheck while holding the shared
            # mutation boundary before deleting metadata as stale.
            with self.metadata_store.mutation():
                locations = self.metadata_store.lookup(key, self.name)
                if locations:
                    bucket, object_key = self._location_target(locations[0])
                    if self._head_exists(object_key, bucket=bucket):
                        return locations[0]
                self.metadata_store.delete(key, self.name)
            self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="stale"))
            return None
        # Object existence alone is not a publish operation. A payload left by
        # a failed metadata commit remains an orphan until explicit recovery.
        self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="miss"))
        return None

    def store(self, key: BlockKey, data: bytes, metadata: KVMetadata) -> StoreResult:
        start = time.perf_counter()
        if metadata.key != key:
            raise MetadataMismatch("store key does not match metadata key")
        data = bytes(data)
        if len(data) > MAX_S3_PAYLOAD_BYTES:
            raise StoreFull("block exceeds S3 record protocol limit")
        checksum = sha256_hex(data)
        with self.metadata_store.mutation():
            if self.metadata_store.is_deleting(key, self.name):
                raise RuntimeError(
                    "cannot store a block while its delete tombstone is pending"
                )
            metadata.checksum = checksum
            metadata.bytes = len(data)
            metadata.last_access = time.time()
            metadata.validate(require_checksum=True)
            existing = self.lookup(key)
            if existing is not None:
                loaded = self.load(key)
                if loaded.data != data:
                    raise ImmutableBlockConflict(
                        f"{key.block_hash}: immutable BlockKey already has different payload"
                    )
                if (
                    loaded.metadata.payload_descriptor()
                    != metadata.payload_descriptor()
                ):
                    raise ImmutableBlockConflict(
                        f"{key.block_hash}: immutable BlockKey metadata descriptor changed"
                    )
                existing.bytes = len(data)
                existing.checksum = checksum
                existing.last_access = metadata.last_access
                self.metadata_store.upsert(existing, metadata)
            else:
                header = {
                    "version": 1,
                    "checksum": checksum,
                    "payload_bytes": len(data),
                    "metadata": metadata.to_dict(),
                }
                encoded_header = encode_record_header(header)
                body = MAGIC + HEADER_LEN_STRUCT.pack(len(encoded_header)) + encoded_header + data
                object_key = self.object_key(key)
                try:
                    self.client.put_object(Bucket=self.bucket, Key=object_key, Body=body, Metadata={"kv-sha256": checksum})
                except Exception as exc:
                    raise TierUnavailable(f"s3 put_object failed: {exc}") from exc
                loc = self._location_for(key, object_key, len(data), checksum, metadata.created_at, metadata.last_access)
                self.metadata_store.upsert(loc, metadata)
        latency_ms = (time.perf_counter() - start) * 1000
        self._metric(lambda m: m.kv_store_latency_seconds.observe(latency_ms / 1000.0, tier=self.name.value, outcome="ok"))
        self._metric(lambda m: m.kv_bytes_written_total.inc(len(data), tier=self.name.value, outcome="ok"))
        return StoreResult(key, self.name, len(data), latency_ms, checksum)

    def load(self, key: BlockKey) -> LoadResult:
        start = time.perf_counter()
        if self.lookup(key) is None:
            raise BlockNotFound(key.block_hash)
        location = self.metadata_store.lookup_and_acquire(key, self.name)
        if location is None:
            raise BlockNotFound(key.block_hash)
        corruption_error: CorruptionDetected | None = None
        try:
            bucket, object_key = self._location_target(location)
            header, payload = self._read_object(object_key, bucket=bucket)
            metadata = KVMetadata.from_dict(header["metadata"])
            if metadata.key != key:
                raise MetadataMismatch(key.block_hash)
            if location.bytes != len(payload) or location.bytes != int(header["payload_bytes"]):
                raise MetadataMismatch(f"{key.block_hash}: location byte count mismatch")
            if location.checksum != str(header["checksum"]):
                raise MetadataMismatch(f"{key.block_hash}: location checksum mismatch")
            if not verify_sha256(payload, str(header["checksum"])):
                self._metric(lambda m: m.kv_checksum_mismatch_total.inc(tier=self.name.value, outcome="error"))
                raise ChecksumMismatch(key.block_hash)
            metadata.checksum = str(header["checksum"])
            metadata.bytes = len(payload)
            self.metadata_store.touch(key, self.name)
            persisted = self.metadata_store.get_metadata(key, self.name)
            if persisted is None or persisted.key != key:
                raise MetadataMismatch(key.block_hash)
            if persisted.payload_descriptor() != metadata.payload_descriptor():
                raise MetadataMismatch(
                    f"{key.block_hash}: persisted metadata does not match payload header"
                )
            metadata.last_access = persisted.last_access
            metadata.reuse_count = persisted.reuse_count
            latency_ms = (time.perf_counter() - start) * 1000
            self._metric(lambda m: m.kv_load_latency_seconds.observe(latency_ms / 1000.0, tier=self.name.value, outcome="ok"))
            self._metric(lambda m: m.kv_bytes_read_total.inc(len(payload), tier=self.name.value, outcome="ok"))
            return LoadResult(key, self.name, payload, metadata, latency_ms)
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
                        raise RuntimeError("corrupt S3 location remains active")
                except Exception as cleanup_error:
                    raise CorruptionCleanupFailed(
                        key.block_hash,
                        "evict",
                        cleanup_error,
                        corruption_error,
                    ) from corruption_error

    def evict(self, key: BlockKey) -> bool:
        with self.metadata_store.mutation():
            location = self.metadata_store.begin_delete(key, self.name)
            if location is None:
                return False
            bucket, object_key = self._location_target(location)
            try:
                self.client.delete_object(Bucket=bucket, Key=object_key)
            except Exception as exc:
                if not _is_not_found(exc):
                    # Keep the durable tombstone: the object remains physically
                    # present but must not become visible through lazy rehydrate.
                    raise TierUnavailable(f"s3 delete_object failed: {exc}") from exc
            if self._head_exists(object_key, bucket=bucket):
                raise TierUnavailable(
                    "s3 delete_object returned successfully but the object remains visible"
                )
            if not self.metadata_store.finish_delete(key, self.name):
                raise RuntimeError("delete tombstone disappeared before metadata commit")
            return True

    def contains(self, key: BlockKey) -> bool:
        return self.lookup(key) is not None

    def stats(self) -> dict:
        data = {
            "tier": self.name.value,
            "bucket": self.bucket,
            "prefix": self.prefix,
            "endpoint_url": self.endpoint_url or "aws-default",
            "used_bytes": self.metadata_store.bytes_used(self.name),
            "connect_timeout_ms": self.connect_timeout_ms,
            "read_timeout_ms": self.read_timeout_ms,
            "max_retries": self.max_retries,
        }
        if isinstance(self.client, FaultInjectingS3Client):
            data["fault_injection"] = self.client.stats()
        return data

    def object_key(self, key: BlockKey) -> str:
        path = str(PurePosixPath(*storage_key_parts(key)))
        return f"{self.prefix}/{path}" if self.prefix else path

    def _location_target(self, location: BlockLocation) -> tuple[str, str]:
        if location.key is None or location.tier != self.name:
            raise MetadataMismatch("persisted S3 location key/tier mismatch")
        parsed = urlparse(location.uri)
        object_key = parsed.path.lstrip("/")
        if (
            parsed.scheme != "s3"
            or parsed.netloc != self.bucket
            or parsed.query
            or parsed.fragment
            or object_key != self.object_key(location.key)
        ):
            raise MetadataMismatch("persisted S3 location is outside configured bucket/prefix")
        # Keep percent escapes verbatim: they are literal characters in object_key().
        return self.bucket, object_key

    def _head_exists(self, object_key: str, bucket: str | None = None) -> bool:
        try:
            self.client.head_object(Bucket=bucket or self.bucket, Key=object_key)
            return True
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise TierUnavailable(f"s3 head_object failed: {exc}") from exc

    def _read_object(self, object_key: str, bucket: str | None = None) -> tuple[dict[str, Any], bytes]:
        try:
            obj = self.client.get_object(Bucket=bucket or self.bucket, Key=object_key)
        except Exception as exc:
            if _is_not_found(exc):
                raise BlockNotFound(object_key) from exc
            raise TierUnavailable(f"s3 get_object failed: {exc}") from exc
        raw = _read_body(obj["Body"])
        if len(raw) < len(MAGIC) + HEADER_LEN_STRUCT.size:
            raise ChecksumMismatch(object_key)
        if raw[: len(MAGIC)] != MAGIC:
            raise ChecksumMismatch(object_key)
        offset = len(MAGIC)
        header_len = HEADER_LEN_STRUCT.unpack(raw[offset : offset + HEADER_LEN_STRUCT.size])[0]
        offset += HEADER_LEN_STRUCT.size
        if header_len <= 0 or header_len > MAX_HEADER_BYTES:
            raise RecordFormatError(f"{object_key}: invalid record header length")
        header_end = offset + header_len
        if header_end > len(raw):
            raise RecordFormatError(f"{object_key}: truncated record header")
        header = decode_record_header(
            raw[offset:header_end],
            object_key,
            max_payload_bytes=MAX_S3_PAYLOAD_BYTES,
            require_layout_mode=False,
        )
        payload = raw[header_end:]
        if len(payload) != int(header["payload_bytes"]):
            raise ChecksumMismatch(f"{object_key}: payload length mismatch")
        return header, payload

    def _location_for(self, key: BlockKey, object_key: str, bytes_: int, checksum: str, created_at: float, last_access: float) -> BlockLocation:
        return BlockLocation(
            key=key,
            tier=self.name,
            uri=f"s3://{self.bucket}/{object_key}",
            bytes=bytes_,
            checksum=checksum,
            created_at=created_at,
            last_access=last_access,
            locality="cross_zone",
            transport="s3_http_default",
        )

    def _metric(self, fn) -> None:
        if self.metrics is None:
            return
        try:
            fn(self.metrics)
        except Exception:
            pass

    def _build_client(
        self,
        endpoint_url: str | None,
        access_key_env: str,
        secret_key_env: str,
        connect_timeout_ms: int,
        read_timeout_ms: int,
        max_retries: int,
    ):
        try:
            import boto3
            from botocore.config import Config
        except Exception as exc:
            raise TierUnavailable("boto3/botocore is required for S3Tier unless a client is injected") from exc
        config = Config(
            connect_timeout=connect_timeout_ms / 1000.0,
            read_timeout=read_timeout_ms / 1000.0,
            retries={"max_attempts": max_retries, "mode": "standard"},
        )
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=os.environ.get(access_key_env),
            aws_secret_access_key=os.environ.get(secret_key_env),
            config=config,
        )


def _read_body(body: Any) -> bytes:
    if isinstance(body, bytes):
        return body
    if hasattr(body, "read"):
        return body.read()
    raise TierUnavailable("s3 body does not support read")


def _is_not_found(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound", "NoSuchBucket"}:
            return True
    name = exc.__class__.__name__.lower()
    msg = str(exc).lower()
    return "nosuchkey" in name or "notfound" in name or "not found" in msg or "404" in msg


def _maybe_fault_inject(client: Any, config: dict[str, Any] | S3FaultInjectionConfig | None) -> Any:
    if config is None:
        return client
    parsed = config if isinstance(config, S3FaultInjectionConfig) else S3FaultInjectionConfig.from_mapping(config)
    if not parsed.enabled:
        return client
    return FaultInjectingS3Client(client, parsed)
