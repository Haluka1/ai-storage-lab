# Local I/O Profile

This supporting module keeps a small CPU-only path from local POSIX file reads to a versioned tier-profile artifact. It exists to test the file contract consumed by the KV cost model, not to establish general storage performance.

## Included surface

- C++17 engines: buffered I/O, `pread`, `mmap`, vectored I/O, and `O_DIRECT`.
- A file generator that requires an explicit `--allowed-root` and rejects symlink/path escapes.
- A JSON-compatible local matrix runner.
- A Python profile generator and human-readable report helper.
- Schema/contract and five-engine smoke tests.

No cloud object runner, hardware inventory, storage-to-GPU path, or specialized transfer probe is included. An unavailable `O_DIRECT` path is emitted as a structured unavailable/error result rather than silently falling back.

## Build and test

```bash
make test-io
```

Direct build:

```bash
cmake -S io-profile -B io-profile/build -DCMAKE_BUILD_TYPE=Release
cmake --build io-profile/build
```

The matrix configuration writes under a temporary directory. Generated numbers describe only the invoking machine/configuration and are not automatically evidence for any README conclusion.

## File contract

[`tier_profile.py`](python/io_path_bench/tier_profile.py) emits `contract_version: 1`, percentile fields, environment context, and source CSV provenance. [`tier_profile.schema.json`](../shared/schema/tier_profile.schema.json) validates its shape; the KV importer independently validates the same file and records its digest.

The checked-in [contract fixture](../shared/fixtures/local-contract.tier-profile.json) is synthetic and exists only to exercise this interface.
