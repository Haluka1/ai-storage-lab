from __future__ import annotations

import socket
import threading
import unittest

from run_demo import _assert_ports_available


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


if __name__ == "__main__":
    unittest.main()
