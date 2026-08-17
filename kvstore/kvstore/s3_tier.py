from __future__ import annotations

import json
import os
import struct
import time
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

from .checksum import sha256_hex, verify_sha256
from .errors import BlockNotFound, ChecksumMismatch, CorruptionCleanupFailed, MetadataMismatch, TierUnavailable
from .layout import storage_key_parts
from .metadata import BlockKey, BlockLocation, KVMetadata, LoadResult, StoreResult, TierName
from .metadata_store import MetadataStore
from .metrics import KVStoreMetrics
from .s3_fault_injection import FaultInjectingS3Client, S3FaultInjectionConfig

MAGIC = b"KVBLK001"
HEADER_LEN_STRUCT = struct.Struct(">I")


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
        object_key = self.object_key(key)
        locations = self.metadata_store.lookup(key, self.name)
        if locations:
            if self._head_exists(object_key):
                self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="hit"))
                return locations[0]
            # A store from another Tier instance may have replaced the object
            # after the optimistic HEAD.  Recheck while holding the shared
            # mutation boundary before deleting metadata as stale.
            with self.metadata_store.mutation():
                locations = self.metadata_store.lookup(key, self.name)
                if locations and self._head_exists(object_key):
                    return locations[0]
                self.metadata_store.delete(key, self.name)
            self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="stale"))
            return None
        with self.metadata_store.mutation():
            locations = self.metadata_store.lookup(key, self.name)
            if locations:
                return locations[0] if self._head_exists(object_key) else None
            if self.metadata_store.is_deleting(key, self.name):
                self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="miss"))
                return None
            if not self._head_exists(object_key):
                self._metric(lambda m: m.kv_lookup_total.inc(tier=self.name.value, outcome="miss"))
                return None
            try:
                metadata, header = self._read_metadata_header(object_key)
            except BlockNotFound:
                return None
            if metadata.key != key:
                return None
            loc = self._location_for(key, object_key, int(header["payload_bytes"]), str(header["checksum"]), metadata.created_at, metadata.last_access)
            self.metadata_store.upsert(loc, metadata)
            return loc

    def store(self, key: BlockKey, data: bytes, metadata: KVMetadata) -> StoreResult:
        start = time.perf_counter()
        with self.metadata_store.mutation():
            if self.metadata_store.is_deleting(key, self.name):
                raise RuntimeError(
                    "cannot store a block while its delete tombstone is pending"
                )
            checksum = sha256_hex(data)
            metadata.checksum = checksum
            metadata.bytes = len(data)
            metadata.last_access = time.time()
            header = {
                "version": 1,
                "checksum": checksum,
                "payload_bytes": len(data),
                "metadata": metadata.to_dict(),
            }
            encoded_header = json.dumps(header, sort_keys=True, separators=(",", ":")).encode("utf-8")
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
        object_key = self.object_key(key)
        if self.lookup(key) is None:
            raise BlockNotFound(key.block_hash)
        if self.metadata_store.lookup_and_acquire(key, self.name) is None:
            raise BlockNotFound(key.block_hash)
        corruption_error: ChecksumMismatch | None = None
        try:
            header, payload = self._read_object(object_key)
            metadata = KVMetadata.from_dict(header["metadata"])
            if metadata.key != key:
                raise MetadataMismatch(key.block_hash)
            if len(payload) != int(header["payload_bytes"]):
                self._metric(lambda m: m.kv_checksum_mismatch_total.inc(tier=self.name.value, outcome="error"))
                raise ChecksumMismatch(key.block_hash)
            if not verify_sha256(payload, str(header["checksum"])):
                self._metric(lambda m: m.kv_checksum_mismatch_total.inc(tier=self.name.value, outcome="error"))
                raise ChecksumMismatch(key.block_hash)
            metadata.checksum = str(header["checksum"])
            metadata.bytes = len(payload)
            self.metadata_store.touch(key, self.name)
            metadata.last_access = time.time()
            metadata.reuse_count += 1
            latency_ms = (time.perf_counter() - start) * 1000
            self._metric(lambda m: m.kv_load_latency_seconds.observe(latency_ms / 1000.0, tier=self.name.value, outcome="ok"))
            self._metric(lambda m: m.kv_bytes_read_total.inc(len(payload), tier=self.name.value, outcome="ok"))
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
                        raise RuntimeError("corrupt S3 location remains active")
                except Exception as cleanup_error:
                    raise CorruptionCleanupFailed(
                        key.block_hash, "evict", cleanup_error
                    ) from corruption_error

    def evict(self, key: BlockKey) -> bool:
        with self.metadata_store.mutation():
            location = self.metadata_store.begin_delete(key, self.name)
            if location is None:
                return False
            bucket, object_key = self._delete_target(location)
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

    def _delete_target(self, location: BlockLocation) -> tuple[str, str]:
        parsed = urlparse(location.uri)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
            raise TierUnavailable(f"invalid persisted s3 location: {location.uri}")
        # Keep percent escapes verbatim: they are literal characters in object_key().
        return parsed.netloc, parsed.path.lstrip("/")

    def _head_exists(self, object_key: str, bucket: str | None = None) -> bool:
        try:
            self.client.head_object(Bucket=bucket or self.bucket, Key=object_key)
            return True
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise TierUnavailable(f"s3 head_object failed: {exc}") from exc

    def _read_metadata_header(self, object_key: str) -> tuple[KVMetadata, dict[str, Any]]:
        header, _payload = self._read_object(object_key)
        return KVMetadata.from_dict(header["metadata"]), header

    def _read_object(self, object_key: str) -> tuple[dict[str, Any], bytes]:
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=object_key)
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
        header = json.loads(raw[offset : offset + header_len].decode("utf-8"))
        payload = raw[offset + header_len :]
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
