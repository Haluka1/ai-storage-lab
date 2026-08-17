from __future__ import annotations

import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from run_demo import _assert_ports_available, _start_router, _start_workers


class DemoPortCheckTest(unittest.TestCase):
    def test_active_listener_is_rejected(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            port = int(listener.getsockname()[1])

            with self.assertRaisesRegex(RuntimeError, "is unavailable"):
                _assert_ports_available((port,))

    def test_time_wait_does_not_block_immediate_reuse(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = int(listener.getsockname()[1])
        served = threading.Event()

        def close_from_server() -> None:
            connection, _ = listener.accept()
            try:
                connection.recv(1)
            finally:
                connection.close()
                served.set()

        thread = threading.Thread(target=close_from_server, daemon=True)
        thread.start()
        with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
            client.sendall(b"x")
            self.assertEqual(client.recv(1), b"")
        self.assertTrue(served.wait(2))
        thread.join(timeout=2)
        listener.close()

        # A probe using the same reuse semantics must succeed immediately.
        _assert_ports_available((port,))

    def test_router_readiness_timeout_stops_started_process(self) -> None:
        process = mock.Mock()
        process.poll.return_value = None
        process.wait.return_value = 0
        with tempfile.TemporaryFile() as log_file:
            with (
                mock.patch("run_demo.subprocess.Popen", return_value=process),
                mock.patch("run_demo._request", side_effect=OSError("not ready")),
            ):
                with self.assertRaisesRegex(RuntimeError, "did not become ready"):
                    _start_router(
                        Path("/tmp/fake-router"),
                        Path("/tmp/fake-config"),
                        log_file,
                        readiness_timeout_seconds=0.01,
                    )
        process.send_signal.assert_called_once()
        process.wait.assert_called_once()

    def test_partial_worker_start_failure_cleans_started_worker(self) -> None:
        first_server = mock.Mock()
        with mock.patch(
            "run_demo.ThreadingHTTPServer",
            side_effect=[first_server, OSError("second bind failed")],
        ):
            with self.assertRaisesRegex(OSError, "second bind failed"):
                _start_workers()
        first_server.shutdown.assert_called_once()
        first_server.server_close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
