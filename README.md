# AI Inference Storage Showcase

## Cache-Aware LLM Router & KV Storage Protocols

> A curated, CPU-reproducible systems showcase for cache-aware LLM request routing, KV storage correctness, and evidence-bounded experimentation.

## Project statement

This repository studies two narrow systems problems: how a Router with an OpenAI-compatible completion/chat HTTP subset can use approximate prefix-cache metadata without violating Worker lifecycle safety, and how an independent multi-tier KV protocol can preserve identity and deletion correctness across host memory, local files, and an S3-compatible interface.

The live path is a real Go HTTP Router and streaming proxy. The KVStore is a separate Python protocol prototype; it is not wired into a model engine's KV hot path. A small C++/Python I/O module supplies a schema-validated local tier-profile file contract. Everything checked by the default build runs on an ordinary CPU without Docker, a GPU, a cluster, an object service, or a cloud account.

## System shape

```text
Legend
  ─────▶  live runtime call
  ═════▶  versioned file contract
  - - -   independent modules; no runtime payload integration

Client ──completion/chat HTTP subset──▶ Go Router ──HTTP / streamed body──▶ Worker
                                      │
                                      ├─────▶ approximate block identity
                                      ├─────▶ CacheIndex metadata
                                      └─────▶ routing + locked revalidation/inflight

Local I/O Profile ══ tier_profile.json + Schema ══▶ KVStore cost model

Go Router - - - Python KVStore protocol
                   ├─────▶ host-memory tier
                   ├─────▶ file-backed tier
                   └─────▶ S3-compatible abstraction
```

The dashed line is deliberately not a call arrow: Router cache metadata and KVStore payload bytes have distinct identities and lifecycles. The local Demo replaces a model Worker with two explicitly labelled fake Workers.

## What is implemented

- **Go Router:** completion/chat subset parsing and proxying, deterministic approximate tokenization, namespace-isolated chained block hashes, CacheIndex TTL/event/sequence handling, routable Worker filtering, round-robin/cache-aware/cost-aware strategies, post-pick locked revalidation, Router-owned inflight lifecycle accounting, drain/remove checks, streaming proxying, and hashed request/tenant identifiers. Opt-in decision logs retain Worker/control-plane metadata for debugging.
- **Python KVStore protocol:** validated, injectively encoded full namespace keys, memory and file-backed tiers, an optional S3-compatible client boundary, payload-first/metadata-last stores, identity/length/checksum validation, atomic lookup-and-acquire/release, single-process owner locking and epoch cleanup, deleting tombstones, segmented append/compaction, target-aware prefetch/eviction, and load/prefetch/recompute decisions.
- **Local I/O Profile:** C++17 buffered, `pread`, `mmap`, vectored, and `O_DIRECT` read paths; a small matrix runner; a versioned JSON Schema; a profile generator; and a tested importer into the KV cost model.

## Quick start

Prerequisites are Go 1.22+, Python 3.10+, CMake 3.16+, a C++17 compiler, and the one Python package listed in `requirements-ci.txt`.

```bash
python3 -m pip install -r requirements-ci.txt
make test
make demo
make demo-kv
make audit
make clean
```

Tests and demos do not contact an external endpoint. `make demo` uses loopback ports `18080`, `18081`, `18082`, and `19090`; it fails with an explicit message if any is unavailable.

## Design highlights

- The Router's block identity chains each block to its parent and includes tenant/model/tokenizer/adapter/modality/cache-salt namespace fields. It is stable for this repository's approximation, not authoritative model-engine identity.
- CacheIndex filters expired observations, deduplicates non-empty event IDs, and requires positive producer sequences to advance per Worker. Sequence zero is an internal unordered local mode and is rejected by the admin event API.
- Selection is advisory until the Router reacquires the Worker lock, revalidates routability, and increments Router-owned inflight lifecycle accounting. This count does not reserve Worker compute, memory, or KV capacity, and the proxy does not retry a failed upstream/stream on another Worker.
- File-backed and S3-compatible stores publish payload before metadata. Loads reacquire metadata and recheck the complete key, byte length, and checksum.
- Deletion becomes logically visible through a tombstone before physical cleanup; append-only segment bytes are reclaimed only when a caller explicitly runs synchronous compaction.
- The cost model makes `load`, asynchronous `prefetch`, or `recompute` explicit instead of treating all located bytes as immediately useful.

The detailed rationale is in [docs/design-decisions.md](docs/design-decisions.md).

## Local demo

`make demo` builds the Router into a temporary directory, starts two local fake Workers, and exercises:

1. two round-robin requests, including a streamed chat response;
2. a controlled CacheIndex event for one approximate block;
3. a `prefix_hash` request selecting the Worker with matching cache metadata.

It prints the selected Worker, strategy, and redacted request hash for every decision. All services and temporary files are removed in a `finally` path. The fake Workers are HTTP test doubles, not vLLM.

`make demo-kv` independently demonstrates store → load → corruption detection → tombstone-backed delete using a temporary file tier.

## Testing

| Command | Scope |
|---|---|
| `make test-router` | format check, build, vet, unit/contract tests, and race tests |
| `make test-kv` | protocol, tier, owner-epoch, tombstone, corruption, and compaction tests |
| `make test-io` | C++ build plus five-engine smoke and profile-contract tests |
| `make test-demo` | loopback port-conflict, immediate-rerun, and failure-cleanup regression tests |
| `make audit` | local Markdown links, privacy/secret patterns, tier-profile Schema, and license status |
| `make test` | all three CPU test suites |

CI runs the same commands, both local Demos, cleanup, and a final Git worktree-drift check on Python 3.10 and 3.12.

## Evidence

Current implementation claims are grounded in local tests. A very small set of historical hardware/cluster observations is retained only as bounded functional context in [docs/evidence-and-limitations.md](docs/evidence-and-limitations.md). Raw private cloud artifacts are intentionally absent and those observations do not validate current HEAD wholesale.

The checked-in tier-profile fixture is synthetic contract data, not benchmark evidence. See [docs/reproduction.md](docs/reproduction.md) for the reproducibility split.

## Limitations

- This is not a production-grade end-to-end LLM serving platform.
- OpenAI compatibility is limited to the implemented `POST /v1/completions` and `POST /v1/chat/completions` request/response proxy subset; it is not full API conformance.
- Approximate Router block identity is not vLLM's authoritative KV identity.
- Cache overlap is a count of individually observed, non-expired chained block IDs. It is not proof of a contiguous reusable prefix or valid payload bytes.
- The KVStore has not been integrated into the real vLLM/LMCache KV payload hot path.
- There is no production telemetry, automatic Worker discovery, Router HA, or transparent automatic failover.
- Router-owned inflight is single-Router lifecycle accounting, not a capacity reservation. Once an upstream attempt starts—especially after stream bytes are written—the Router does not retry it elsewhere.
- No RDMA, NIXL, GDS, CXL, or SPDK data plane is implemented.
- `MemoryTier` means host memory, not HBM. The historical `NVMeTier` class name wraps a file-backed abstraction and is not a validated production NVMe engine.
- The file-backed tier does not implement `O_DIRECT`; requesting it fails explicitly. `O_DIRECT` exists only in the independent I/O Profile measurement module.
- File-backed stores default to no strong durable commit guarantee across sudden power loss.
- The SQLite owner lock is single-process coordination, not a distributed lease.
- Segment compaction is an explicit synchronous maintenance call, not an online background compactor. A checksum mismatch is surfaced and invalidates the bad location; it is not automatically converted into recomputation.
- Tier profiles enter through validated files; they are not live telemetry.
- Historical LMCache work showed a functional path but no stable or general acceleration.
- Historical experiment summaries cannot prove the current revision's complete behavior; some lack a recorded source revision.

This repository is licensed under the [MIT License](LICENSE).

## Development approach

This repository is a curated public edition of a larger private experimental workspace. AI tools assisted substantial portions of the initial implementation and documentation. The maintainer defined the problem framing, architecture, system invariants, experiment design, review criteria, and claim boundaries, and has reviewed and validated the components published here.

The disclosure does not replace code review or test evidence. The review workflow and responsibility boundaries are detailed in [docs/development-approach.md](docs/development-approach.md).

## Repository map

- [`router/`](router/) — Go Router runtime, configuration, and tests.
- [`kvstore/`](kvstore/) — independent Python KV storage protocol and tests.
- [`io-profile/`](io-profile/) — minimal local I/O measurement/profile support.
- [`shared/`](shared/) — cross-language vectors, Schema, and file-contract fixture.
- [`examples/local-demo/`](examples/local-demo/) — hermetic Router and KV demos.
- [`docs/`](docs/) — architecture, decisions, evidence boundaries, and reproduction.
- [`scripts/`](scripts/) — repository-local audit and cleanup gates.

## Review path

For a focused code review, follow this order:

1. [Router request path](router/internal/proxy/handler.go) and [routing strategies](router/internal/routing/strategies.go).
2. [Approximate tokenization](router/internal/blockhash/tokenization.go), [chained identity](router/internal/blockhash/blockhash.go), and [CacheIndex](router/internal/cacheindex/index.go).
3. [Metadata owner/tombstone protocol](kvstore/kvstore/metadata_store.py), [file-backed tier](kvstore/kvstore/nvme_tier.py), and [tier manager](kvstore/kvstore/tier_manager.py).
4. [Tier-profile Schema](shared/schema/tier_profile.schema.json), [generator](io-profile/python/io_path_bench/tier_profile.py), and [importer](kvstore/kvstore/tier_profile_import.py).
5. [Evidence boundaries](docs/evidence-and-limitations.md) before repeating any experimental conclusion.
