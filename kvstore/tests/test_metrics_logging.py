from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from kvstore.logging import StructuredLogger
from kvstore.metrics import KVStoreMetrics, validate_metric_labels
from kvstore.metrics_http import create_metrics_server
from kvstore.memory_tier import MemoryTier
from kvstore.metadata import BlockKey, KVMetadata
from kvstore.metadata_store import MetadataStore


class MetricsLoggingTest(unittest.TestCase):
    def test_metric_label_guard_rejects_high_cardinality_labels(self) -> None:
        with self.assertRaises(ValueError):
            validate_metric_labels({"tenant_id": "secret"})
        with self.assertRaises(ValueError):
            validate_metric_labels({"request_id": "req"})
        with self.assertRaises(ValueError):
            validate_metric_labels({"unknown": "value"})

    def test_metrics_export_uses_allowed_labels(self) -> None:
        metrics = KVStoreMetrics()
        metrics.kv_lookup_total.inc(tier="memory", outcome="hit")
        metrics.kv_load_latency_seconds.observe(0.01, tier="nvme", outcome="ok")
        text = metrics.export_prometheus_text()
        self.assertIn("kv_lookup_total", text)
        self.assertIn('tier="memory"', text)

    def test_metrics_are_thread_safe_and_histograms_are_bounded(self) -> None:
        metrics = KVStoreMetrics()

        def record() -> None:
            for _ in range(1000):
                metrics.kv_lookup_total.inc(tier="memory", outcome="hit")
                metrics.kv_load_latency_seconds.observe(0.01, tier="memory", outcome="ok")

        threads = [threading.Thread(target=record) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(sum(metrics.kv_lookup_total.snapshot().values()), 8000)
        states = list(metrics.kv_load_latency_seconds.snapshot().values())
        self.assertEqual(states[0].count, 8000)
        self.assertEqual(
            len(states[0].bucket_counts), len(metrics.kv_load_latency_seconds.buckets)
        )
        self.assertIn('le="+Inf"', metrics.export_prometheus_text())

    def test_structured_logger_redacts_forbidden_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "log.jsonl"
            logger = StructuredLogger(path)
            logger.info("decision", tenant_id="tenant-secret", nested={"block_hash": "a" * 64}, tier="nvme")
            obj = json.loads(path.read_text(encoding="utf-8"))
            self.assertNotIn("tenant_id", obj)
            self.assertTrue(obj["tenant_id_redacted"])
            self.assertTrue(obj["nested"]["block_hash_redacted"])

    def test_memory_tier_records_metrics_without_affecting_main_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            metrics = KVStoreMetrics()
            meta = MetadataStore(Path(td) / "meta.sqlite3")
            tier = MemoryTier(1024, meta, metrics=metrics)
            key = BlockKey("t", "m", "r", "tok", "a" * 64)
            tier.store(key, b"payload", KVMetadata(key, "bf16", 1, 1, 1, 16, 7))
            tier.load(key)
            text = metrics.export_prometheus_text()
            self.assertIn("kv_store_latency_seconds_count", text)
            self.assertIn("kv_bytes_read_total", text)

    def test_metrics_http_server_exports_prometheus_text(self) -> None:
        metrics = KVStoreMetrics()
        metrics.kv_lookup_total.inc(tier="memory", outcome="hit")
        try:
            server = create_metrics_server(metrics, port=0)
        except PermissionError as exc:
            self.skipTest(f"local sockets are unavailable in this environment: {exc}")
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_address[1]}"
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(base + "/metrics", timeout=2.0) as response:
                body = response.read().decode("utf-8")
                content_type = response.headers.get("Content-Type", "")
            self.assertEqual(response.status, 200)
            self.assertIn("text/plain", content_type)
            self.assertIn("kv_lookup_total", body)
            self.assertIn('tier="memory"', body)

            with opener.open(base + "/healthz", timeout=2.0) as response:
                self.assertEqual(response.read(), b"ok\n")

            with self.assertRaises(urllib.error.HTTPError) as ctx:
                opener.open(base + "/not-found", timeout=2.0)
            try:
                self.assertEqual(ctx.exception.code, 404)
            finally:
                ctx.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)


if __name__ == "__main__":
    unittest.main()
