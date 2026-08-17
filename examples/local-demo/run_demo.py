#!/usr/bin/env python3
"""Hermetic Router demo with two loopback-only fake Workers."""

from __future__ import annotations

import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Iterable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
ROUTER_DIR = ROOT / "router"
ROUTER_PORT = 18080
ADMIN_PORT = 19090
WORKERS = {"worker-a": 18081, "worker-b": 18082}
DEMO_PORTS = (ROUTER_PORT, ADMIN_PORT, *WORKERS.values())


def _assert_ports_available(ports: Iterable[int] = DEMO_PORTS) -> None:
    reservations: list[socket.socket] = []
    try:
        for port in ports:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # Match the reuse semantics of Python's HTTPServer and Go's
                # TCP listener. This permits an immediate rerun after the
                # previous demo's accepted connections enter TIME_WAIT while
                # still rejecting a genuinely active listener at listen().
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(("127.0.0.1", port))
                sock.listen(1)
            except OSError as exc:
                sock.close()
                raise RuntimeError(f"required demo port 127.0.0.1:{port} is unavailable: {exc}") from exc
            reservations.append(sock)
    finally:
        for sock in reservations:
            sock.close()


def _handler_for(worker_id: str) -> type[BaseHTTPRequestHandler]:
    class FakeWorkerHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib callback name
            length = int(self.headers.get("Content-Length", "0"))
            try:
                request = json.loads(self.rfile.read(length) or b"{}")
            except json.JSONDecodeError:
                self.send_error(400, "invalid JSON")
                return
            if self.path not in {"/v1/completions", "/v1/chat/completions"}:
                self.send_error(404)
                return
            if bool(request.get("stream")):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                event = {
                    "id": f"local-{worker_id}",
                    "object": "chat.completion.chunk",
                    "choices": [{"index": 0, "delta": {"content": f"stream from {worker_id}"}}],
                }
                self.wfile.write(f"data: {json.dumps(event)}\n\n".encode("utf-8"))
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                return
            if self.path == "/v1/chat/completions":
                payload: dict[str, Any] = {
                    "id": f"local-{worker_id}",
                    "object": "chat.completion",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": f"response from {worker_id}"},
                            "finish_reason": "stop",
                        }
                    ],
                }
            else:
                payload = {
                    "id": f"local-{worker_id}",
                    "object": "text_completion",
                    "choices": [{"index": 0, "text": f"response from {worker_id}", "finish_reason": "stop"}],
                }
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *args: object) -> None:
            _ = args

    return FakeWorkerHandler


def _start_workers() -> list[tuple[ThreadingHTTPServer, threading.Thread]]:
    workers: list[tuple[ThreadingHTTPServer, threading.Thread]] = []
    try:
        for worker_id, port in WORKERS.items():
            server = ThreadingHTTPServer(("127.0.0.1", port), _handler_for(worker_id))
            server.daemon_threads = True
            thread = threading.Thread(target=server.serve_forever, name=worker_id, daemon=True)
            thread.start()
            workers.append((server, thread))
        return workers
    except BaseException:
        _stop_workers(workers)
        raise


def _stop_workers(
    workers: list[tuple[ThreadingHTTPServer, threading.Thread]],
) -> None:
    for server, thread in workers:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _config(strategy: str) -> dict[str, Any]:
    return {
        "server": {
            "listen_addr": f"127.0.0.1:{ROUTER_PORT}",
            "admin_addr": f"127.0.0.1:{ADMIN_PORT}",
        },
        "router": {
            "run_id": "local-demo",
            "strategy": strategy,
            "block_size_tokens": 16,
            "cache_ttl_ms": 30_000,
            "decision_log_path": "",
            "trace_log_path": "",
            "entry_topology": {
                "cloud": "local",
                "region": "local",
                "zone": "local-a",
                "cluster_id": "local-demo",
                "node_id": "router",
            },
        },
        "workers": [
            {
                "id": worker_id,
                "url": f"http://127.0.0.1:{port}",
                "health": "ready",
                "readiness_state": "ready",
                "weight": 1.0,
                "topology": {
                    "cloud": "local",
                    "region": "local",
                    "zone": "local-a",
                    "cluster_id": "local-demo",
                    "node_id": worker_id,
                },
            }
            for worker_id, port in WORKERS.items()
        ],
    }


def _build_router(binary: Path, cache_dir: Path) -> None:
    env = os.environ.copy()
    env["GOCACHE"] = str(cache_dir)
    subprocess.run(
        ["go", "build", "-buildvcs=false", "-o", str(binary), "./cmd/router"],
        cwd=ROUTER_DIR,
        env=env,
        check=True,
    )


def _start_router(
    binary: Path,
    config_path: Path,
    log_file: Any,
    readiness_timeout_seconds: float = 5.0,
) -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env["NO_PROXY"] = "127.0.0.1,localhost"
    env["no_proxy"] = "127.0.0.1,localhost"
    process = subprocess.Popen(
        [str(binary), "-config", str(config_path)],
        cwd=ROUTER_DIR,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    try:
        deadline = time.monotonic() + readiness_timeout_seconds
        while time.monotonic() < deadline:
            if process.poll() is not None:
                log_file.seek(0)
                details = log_file.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Router exited before readiness:\n{details}")
            try:
                status, _, _ = _request(ADMIN_PORT, "GET", "/readyz")
                if status == 200:
                    return process
            except OSError:
                pass
            time.sleep(0.05)
        raise RuntimeError(
            f"Router did not become ready within {readiness_timeout_seconds:g} seconds"
        )
    except BaseException:
        _stop_router(process)
        raise


def _stop_router(process: subprocess.Popen[bytes] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=2)


def _request(
    port: int,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request_headers = dict(headers or {})
    if body is not None:
        request_headers["Content-Type"] = "application/json"
        request_headers["Content-Length"] = str(len(body))
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request(method, path, body=body, headers=request_headers)
        response = connection.getresponse()
        response_body = response.read()
        response_headers = {name.lower(): value for name, value in response.getheaders()}
        return response.status, response_headers, response_body
    finally:
        connection.close()


def _print_route(label: str, headers: dict[str, str]) -> str:
    worker = headers.get("x-router-worker-id", "")
    strategy = headers.get("x-router-strategy", "")
    request_hash = headers.get("x-router-request-hash", "")
    if not worker or not strategy or not request_hash:
        raise RuntimeError(f"{label}: Router decision headers are missing")
    print(f"{label}: selected_worker={worker} strategy={strategy} request_hash={request_hash}")
    return worker


def _first_block(prompt_text: str) -> str:
    sys.path.insert(0, str(ROOT))
    from shared.python.blockhash import IsolationKey, compute_blocks
    from shared.python.tokenization import approximate_tokenize

    tokenized = approximate_tokenize(prompt_text)
    blocks = compute_blocks(
        tokenized.tokens,
        IsolationKey(
            tenant_id="default-tenant",
            tenant_salt="default-salt",
            model_id="showcase-model",
            model_revision="default-revision",
            tokenizer_revision=tokenized.tokenizer_revision,
            lora_id="none",
            modality_key="text",
            cache_salt="cache-v1",
        ),
        block_size_tokens=16,
    )
    if not blocks:
        raise RuntimeError("cache-aware demo text produced no approximate blocks")
    return blocks[0]


def main() -> int:
    _assert_ports_available()
    workers: list[tuple[ThreadingHTTPServer, threading.Thread]] = []
    router_process: subprocess.Popen[bytes] | None = None
    try:
        workers = _start_workers()
        with tempfile.TemporaryDirectory(prefix="ai-showcase-demo-") as td:
            temp_root = Path(td)
            binary = temp_root / "router"
            _build_router(binary, temp_root / "gocache")
            with tempfile.TemporaryFile() as router_log:
                round_robin_config = temp_root / "round-robin.json"
                round_robin_config.write_text(json.dumps(_config("round_robin")), encoding="utf-8")
                router_process = _start_router(binary, round_robin_config, router_log)

                status, headers, _ = _request(
                    ROUTER_PORT,
                    "POST",
                    "/v1/completions",
                    {"model": "showcase-model", "prompt": "first local request", "max_tokens": 4},
                    {"X-Request-ID": "demo-round-one"},
                )
                if status != 200:
                    raise RuntimeError(f"round-robin completion returned HTTP {status}")
                if _print_route("round-robin completion", headers) != "worker-a":
                    raise RuntimeError("first round-robin request did not select worker-a")

                status, headers, body = _request(
                    ROUTER_PORT,
                    "POST",
                    "/v1/chat/completions",
                    {
                        "model": "showcase-model",
                        "messages": [{"role": "user", "content": "second local request"}],
                        "max_tokens": 4,
                        "stream": True,
                    },
                    {"X-Request-ID": "demo-round-two"},
                )
                if status != 200 or b"data: [DONE]" not in body:
                    raise RuntimeError("streaming chat response did not complete")
                if _print_route("round-robin streaming chat", headers) != "worker-b":
                    raise RuntimeError("second round-robin request did not select worker-b")

                _stop_router(router_process)
                router_process = None
                router_log.seek(0)
                router_log.truncate()

                prefix_config = temp_root / "prefix-hash.json"
                prefix_config.write_text(json.dumps(_config("prefix_hash")), encoding="utf-8")
                router_process = _start_router(binary, prefix_config, router_log)
                shared_text = "shared prefix alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi"
                event = {
                    "event_id": "local-demo-cache-event",
                    "event_type": "block_stored",
                    "worker_id": "worker-b",
                    "block_hash": _first_block(shared_text),
                    "tier": "gpu",
                    "tokens": 16,
                    "seq_no": 1,
                }
                status, _, _ = _request(ADMIN_PORT, "POST", "/admin/events", event)
                if status != 200:
                    raise RuntimeError(f"cache event returned HTTP {status}")
                status, headers, _ = _request(
                    ROUTER_PORT,
                    "POST",
                    "/v1/completions",
                    {"model": "showcase-model", "prompt": shared_text, "max_tokens": 4},
                    {"X-Request-ID": "demo-cache-aware"},
                )
                if status != 200:
                    raise RuntimeError(f"cache-aware completion returned HTTP {status}")
                if _print_route("cache-aware completion", headers) != "worker-b":
                    raise RuntimeError("prefix_hash did not select the Worker with injected cache metadata")
        print("local Router demo: PASS (all services stopped; no tracked files written)")
        return 0
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"local Router demo: FAIL: {exc}", file=sys.stderr)
        return 1
    finally:
        _stop_router(router_process)
        _stop_workers(workers)


if __name__ == "__main__":
    raise SystemExit(main())
