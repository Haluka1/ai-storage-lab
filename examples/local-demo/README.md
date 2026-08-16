# Local demos

`run_demo.py` starts two loopback-only fake Workers and a temporary Go Router binary. It verifies round-robin completion, streamed chat, and a cache-aware decision after controlled metadata injection. The fake Workers are test doubles and do not perform model inference.

`kv_demo.py` uses a temporary SQLite database and file-backed tier to demonstrate store, load, checksum corruption detection, and tombstone-backed deletion.

Run from the repository root:

```bash
make demo
make demo-kv
```

Both scripts clean up processes and temporary state. The Router Demo intentionally uses fixed ports so conflicts fail visibly instead of selecting an unexpected service.
