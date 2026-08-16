from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any

from .errors import LoadTimeout, TierUnavailable


DEFAULT_OPERATIONS = ("get_object", "put_object", "head_object", "delete_object")


class S3InjectedError(TierUnavailable):
    pass


class S3InjectedTimeout(LoadTimeout):
    pass


@dataclass(frozen=True)
class S3FaultInjectionConfig:
    latency_ms: float = 0.0
    error_rate: float = 0.0
    timeout_rate: float = 0.0
    throttle_mbps: float = 0.0
    seed: int = 0
    operations: tuple[str, ...] = DEFAULT_OPERATIONS
    enabled: bool = True

    @classmethod
    def from_mapping(cls, raw: dict[str, Any] | None) -> "S3FaultInjectionConfig":
        raw = raw or {}
        operations = _parse_operations(raw.get("operations", DEFAULT_OPERATIONS))
        return cls(
            latency_ms=float(raw.get("latency_ms", 0.0) or 0.0),
            error_rate=float(raw.get("error_rate", 0.0) or 0.0),
            timeout_rate=float(raw.get("timeout_rate", 0.0) or 0.0),
            throttle_mbps=float(raw.get("throttle_mbps", raw.get("throttle_MBps", 0.0)) or 0.0),
            seed=int(raw.get("seed", 0) or 0),
            operations=operations,
            enabled=bool(raw.get("enabled", True)),
        ).validated()

    def validated(self) -> "S3FaultInjectionConfig":
        if self.latency_ms < 0:
            raise ValueError("latency_ms must be >= 0")
        if not 0.0 <= self.error_rate <= 1.0:
            raise ValueError("error_rate must be between 0 and 1")
        if not 0.0 <= self.timeout_rate <= 1.0:
            raise ValueError("timeout_rate must be between 0 and 1")
        if self.throttle_mbps < 0:
            raise ValueError("throttle_mbps must be >= 0")
        unknown = sorted(set(self.operations) - set(DEFAULT_OPERATIONS))
        if unknown:
            raise ValueError(f"unknown S3 fault injection operations: {', '.join(unknown)}")
        return self


class FaultInjectingS3Client:
    def __init__(self, client: Any, config: S3FaultInjectionConfig | dict[str, Any]):
        self.client = client
        self.config = config if isinstance(config, S3FaultInjectionConfig) else S3FaultInjectionConfig.from_mapping(config)
        self._rng = random.Random(self.config.seed)
        self._stats: dict[str, float] = {
            "operation_total": 0.0,
            "latency_injected_total_ms": 0.0,
            "error_total": 0.0,
            "timeout_total": 0.0,
            "throttled_bytes_total": 0.0,
            "throttle_sleep_total_ms": 0.0,
        }

    def put_object(self, **kwargs):
        return self._call("put_object", self.client.put_object, **kwargs)

    def get_object(self, **kwargs):
        response = self._call("get_object", self.client.get_object, **kwargs)
        if self._enabled_for("get_object") and self.config.throttle_mbps > 0 and isinstance(response, dict) and "Body" in response:
            copied = dict(response)
            copied["Body"] = _ThrottledBody(copied["Body"], self.config.throttle_mbps, self._stats)
            return copied
        return response

    def head_object(self, **kwargs):
        return self._call("head_object", self.client.head_object, **kwargs)

    def delete_object(self, **kwargs):
        return self._call("delete_object", self.client.delete_object, **kwargs)

    def stats(self) -> dict[str, float]:
        return dict(self._stats)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    def _call(self, operation: str, fn, **kwargs):
        if not self._enabled_for(operation):
            return fn(**kwargs)
        self._stats["operation_total"] += 1.0
        if self.config.latency_ms > 0:
            time.sleep(self.config.latency_ms / 1000.0)
            self._stats["latency_injected_total_ms"] += self.config.latency_ms
        if self.config.timeout_rate > 0 and self._rng.random() < self.config.timeout_rate:
            self._stats["timeout_total"] += 1.0
            raise S3InjectedTimeout(f"injected S3 timeout during {operation}")
        if self.config.error_rate > 0 and self._rng.random() < self.config.error_rate:
            self._stats["error_total"] += 1.0
            raise S3InjectedError(f"injected S3 error during {operation}")
        if operation == "put_object" and self.config.throttle_mbps > 0:
            _throttle_len(_len_hint(kwargs.get("Body")), self.config.throttle_mbps, self._stats)
        return fn(**kwargs)

    def _enabled_for(self, operation: str) -> bool:
        return self.config.enabled and operation in self.config.operations


class _ThrottledBody:
    def __init__(self, body: Any, throttle_mbps: float, stats: dict[str, float]):
        self.body = body
        self.throttle_mbps = throttle_mbps
        self.stats = stats

    def read(self, *args, **kwargs) -> bytes:
        if isinstance(self.body, bytes):
            data = self.body
        else:
            data = self.body.read(*args, **kwargs)
        _throttle_len(len(data), self.throttle_mbps, self.stats)
        return data

    def __getattr__(self, name: str) -> Any:
        return getattr(self.body, name)


def is_timeout_exception(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, TimeoutError | LoadTimeout):
            return True
        name = current.__class__.__name__.lower()
        message = str(current).lower()
        if "timeout" in name or "timed out" in message or "timeout" in message:
            return True
        current = current.__cause__
    return False


def _parse_operations(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return DEFAULT_OPERATIONS
    if isinstance(raw, str):
        return tuple(item.strip() for item in raw.split(",") if item.strip())
    return tuple(str(item).strip() for item in raw if str(item).strip())


def _len_hint(body: Any) -> int:
    if body is None:
        return 0
    if isinstance(body, bytes | bytearray | memoryview):
        return len(body)
    if hasattr(body, "getbuffer"):
        try:
            return len(body.getbuffer())
        except Exception:
            return 0
    return 0


def _throttle_len(num_bytes: int, throttle_mbps: float, stats: dict[str, float]) -> None:
    if num_bytes <= 0 or throttle_mbps <= 0:
        return
    stats["throttled_bytes_total"] += float(num_bytes)
    sleep_s = (num_bytes / (1024 * 1024)) / throttle_mbps
    if sleep_s > 0:
        time.sleep(sleep_s)
        stats["throttle_sleep_total_ms"] += sleep_s * 1000.0
