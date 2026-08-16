from __future__ import annotations

import math
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable


FORBIDDEN_LABELS = {
    "request_id",
    "tenant_id",
    "block_hash",
    "prompt",
    "raw_prompt",
    "file_path",
    "hostname",
    "gpu_uuid",
}
ALLOWED_LABELS = {
    "tier",
    "decision",
    "strategy",
    "outcome",
    "operation",
    "reason_class",
}
DEFAULT_BUCKETS = (0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0)


def validate_metric_labels(labels: dict[str, str]) -> None:
    bad = sorted(set(labels) & FORBIDDEN_LABELS)
    if bad:
        raise ValueError(f"forbidden metric labels: {', '.join(bad)}")
    unknown = sorted(set(labels) - ALLOWED_LABELS)
    if unknown:
        raise ValueError(f"unknown metric labels: {', '.join(unknown)}")


@dataclass
class Counter:
    name: str
    help: str
    values: dict[tuple[tuple[str, str], ...], float] = field(
        default_factory=lambda: defaultdict(float)
    )
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def inc(self, amount: float = 1.0, **labels: str) -> None:
        validate_metric_labels(labels)
        with self._lock:
            self.values[_label_key(labels)] += amount

    def snapshot(self) -> dict[tuple[tuple[str, str], ...], float]:
        with self._lock:
            return dict(self.values)


@dataclass
class HistogramValue:
    count: int
    total: float
    bucket_counts: list[int]


@dataclass
class Histogram:
    name: str
    help: str
    buckets: tuple[float, ...] = DEFAULT_BUCKETS
    values: dict[tuple[tuple[str, str], ...], HistogramValue] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def observe(self, value: float, **labels: str) -> None:
        validate_metric_labels(labels)
        if not math.isfinite(value):
            raise ValueError("histogram observation must be finite")
        key = _label_key(labels)
        with self._lock:
            state = self.values.get(key)
            if state is None:
                state = HistogramValue(0, 0.0, [0] * len(self.buckets))
                self.values[key] = state
            state.count += 1
            state.total += value
            for index, bound in enumerate(self.buckets):
                if value <= bound:
                    state.bucket_counts[index] += 1

    def snapshot(self) -> dict[tuple[tuple[str, str], ...], HistogramValue]:
        with self._lock:
            return {
                key: HistogramValue(value.count, value.total, list(value.bucket_counts))
                for key, value in self.values.items()
            }


class KVStoreMetrics:
    def __init__(self):
        self.kv_lookup_total = Counter("kv_lookup_total", "KV lookup count")
        self.kv_load_latency_seconds = Histogram("kv_load_latency_seconds", "KV load latency")
        self.kv_store_latency_seconds = Histogram("kv_store_latency_seconds", "KV store latency")
        self.kv_bytes_read_total = Counter("kv_bytes_read_total", "KV bytes read")
        self.kv_bytes_written_total = Counter("kv_bytes_written_total", "KV bytes written")
        self.kv_onload_decision_total = Counter("kv_onload_decision_total", "KV onload decisions")
        self.kv_onload_timeout_total = Counter("kv_onload_timeout_total", "KV onload timeout count")
        self.kv_onload_fallback_total = Counter("kv_onload_fallback_total", "KV onload fallback count")
        self.kv_checksum_mismatch_total = Counter("kv_checksum_mismatch_total", "KV checksum mismatches")
        self.kv_prefetch_total = Counter("kv_prefetch_total", "KV prefetch count")

    def export_prometheus_text(self) -> str:
        lines: list[str] = []
        for metric in self._all_metrics():
            lines.append(f"# HELP {metric.name} {metric.help}")
            metric_type = "histogram" if isinstance(metric, Histogram) else "counter"
            lines.append(f"# TYPE {metric.name} {metric_type}")
            if isinstance(metric, Counter):
                for labels, value in sorted(metric.snapshot().items()):
                    lines.append(f"{metric.name}{_format_labels(labels)} {value}")
                continue
            for labels, state in sorted(metric.snapshot().items()):
                for bound, count in zip(metric.buckets, state.bucket_counts):
                    bucket_labels = (*labels, ("le", _format_bound(bound)))
                    lines.append(f"{metric.name}_bucket{_format_labels(bucket_labels)} {count}")
                infinity_labels = (*labels, ("le", "+Inf"))
                lines.append(
                    f"{metric.name}_bucket{_format_labels(infinity_labels)} {state.count}"
                )
                label_text = _format_labels(labels)
                lines.append(f"{metric.name}_count{label_text} {state.count}")
                lines.append(f"{metric.name}_sum{label_text} {state.total}")
        return "\n".join(lines) + "\n"

    def _all_metrics(self) -> Iterable[Counter | Histogram]:
        return [
            self.kv_lookup_total,
            self.kv_load_latency_seconds,
            self.kv_store_latency_seconds,
            self.kv_bytes_read_total,
            self.kv_bytes_written_total,
            self.kv_onload_decision_total,
            self.kv_onload_timeout_total,
            self.kv_onload_fallback_total,
            self.kv_checksum_mismatch_total,
            self.kv_prefetch_total,
        ]


def _label_key(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((key, str(value)) for key, value in labels.items()))


def _format_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{key}="{value}"' for key, value in sorted(labels))
    return "{" + inner + "}"


def _format_bound(value: float) -> str:
    return f"{value:g}"
