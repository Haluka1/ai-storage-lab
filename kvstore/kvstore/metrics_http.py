from __future__ import annotations

import hashlib
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

from .metrics import KVStoreMetrics


def create_metrics_server(metrics: KVStoreMetrics, host: str = "127.0.0.1", port: int = 0) -> ThreadingHTTPServer:
    handler_cls = _handler_for(metrics)
    return ThreadingHTTPServer((host, port), handler_cls)


def _handler_for(metrics: KVStoreMetrics) -> type[BaseHTTPRequestHandler]:
    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/healthz":
                _write_response(self, 200, "text/plain; charset=utf-8", b"ok\n")
                return
            if self.path == "/metrics":
                body = metrics.export_prometheus_text().encode("utf-8")
                _write_response(self, 200, "text/plain; version=0.0.4; charset=utf-8", body)
                return
            _write_response(self, 404, "text/plain; charset=utf-8", b"not found\n")

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return MetricsHandler


def _write_response(handler: BaseHTTPRequestHandler, status: int, content_type: str, body: bytes) -> None:
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    _write_correlation_header(handler)
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _write_correlation_header(handler: BaseHTTPRequestHandler) -> None:
    trace_id = _trace_id_from_traceparent(handler.headers.get("traceparent", ""))
    if trace_id:
        handler.send_header("X-Trace-Id", trace_id)
        return
    request_id = handler.headers.get("X-Request-ID") or str(time.time_ns())
    handler.send_header("X-Request-Hash", hashlib.sha256(request_id.encode("utf-8")).hexdigest()[:16])


def _trace_id_from_traceparent(value: str) -> str:
    parts = value.strip().split("-")
    if len(parts) < 4:
        return ""
    trace_id = parts[1]
    if re.fullmatch(r"[0-9a-f]{32}", trace_id) is None:
        return ""
    if trace_id == "0" * 32:
        return ""
    return trace_id


def serve_forever(metrics: KVStoreMetrics, host: str, port: int, ready: Callable[[ThreadingHTTPServer], None] | None = None) -> None:
    server = create_metrics_server(metrics, host, port)
    if ready is not None:
        ready(server)
    try:
        server.serve_forever()
    finally:
        server.server_close()
