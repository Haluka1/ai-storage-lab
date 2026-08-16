# Architecture and boundaries

## Real request chain

The executable request chain is deliberately short:

```text
completion/chat-compatible client
  ──POST /v1/completions or /v1/chat/completions──▶ Go Router
  ──same request path + bounded Router headers────▶ selected Worker
  ◀──────────────────── buffered or streamed body─┘
```

Inside the Router, [`handleProxy`](../router/internal/proxy/handler.go) parses only the request fields required to derive text/model/output limits. It computes deterministic approximate tokens and chained block hashes, reads CacheIndex metadata, asks a strategy to pick a candidate, then reacquires the Worker lock. Only a still-routable Worker receives a Router-owned inflight lifecycle count. The count is released after the upstream response body has finished copying, including streaming responses; it does not reserve Worker compute, memory, or KV capacity. The Router makes no second-Worker retry if the upstream attempt or response stream fails.

The public listener exposes health, readiness, redacted metrics, and the two inference paths. Those paths implement a tested OpenAI-compatible proxy subset, not the complete OpenAI API. Worker replacement, drain/undrain, and cache events live on a separate admin listener that configuration validation restricts to loopback.

## Router and KVStore relationship

The Go Router and Python KVStore are independent prototypes. There is no Router call that stores or loads KV payload bytes through the Python code, and there is no adapter from the KVStore into vLLM or LMCache.

Their useful relationship is conceptual and contractual:

- the Router reasons about *metadata* that says an approximate prefix block may be associated with a Worker or tier;
- the KVStore reasons about *payload correctness* for a complete `BlockKey`, metadata record, and byte sequence;
- both isolate namespaces, but their keys are not asserted to be identical to a model engine's authoritative KV key.

The dashed line in the README diagram therefore denotes independence, not an asynchronous or hidden runtime call.

## CacheIndex metadata versus KV payload

[`CacheIndex`](../router/internal/cacheindex/index.go) stores expiring `BlockLocation` observations. They include a Router block hash, Worker, tier, sequence, confidence, and optional topology/cost hints. Overlap is the number of requested block IDs with an individually non-expired observation for a Worker; the index does not additionally prove that those observations form a contiguous reusable prefix. An observation can influence selection; it cannot prove that payload bytes still exist or are valid.

The KVStore tiers own bytes. A file/S3 load first acquires a metadata record, then validates the full namespace identity, declared length, and checksum before returning data. Corruption is an error and invalidates the location. A routing prediction and a successful KV load are therefore different facts.

## I/O Profile to KV cost-model file contract

This is the curated equivalent of the private workspace's former **F→B profile contract**:

```text
local C++ I/O rows
  ══▶ Python profile generator
  ══▶ tier_profile.json (contract_version = 1)
  ══▶ JSON Schema validation
  ══▶ KVStore TierProfile importer
  ══▶ CostModel parameters
```

The generator and importer share only a file governed by [`tier_profile.schema.json`](../shared/schema/tier_profile.schema.json). The importer records the artifact SHA-256 and embedded provenance. If the requested local profile is absent or unavailable it fails; an absent/unavailable S3 profile is recorded as an explicit fallback rather than silently presented as measured data. This is a file import, not an online telemetry or adaptive calibration path.

The checked-in fixture is synthetic contract input. It is not a hardware result and is never used as public performance evidence.

## Current boundary

- The Router uses deterministic approximate tokenization in its live request path; the approximation is not engine-accurate tokenization or KV identity.
- Cache-event idempotence applies to non-empty event IDs, and ordered delivery checks apply to positive per-Worker sequence numbers. Sequence zero is accepted as unordered. Expired locations and remembered event IDs are not continuously garbage-collected in-process.
- Cache events and Worker metrics are supplied by config/admin calls. There is no production telemetry pipeline or automatic Worker discovery.
- Drain and locked revalidation prevent new unsafe reservations; they do not implement transparent retry or automatic failover.
- `MemoryTier` uses host memory. `NVMeTier` is a retained API name for a file-backed abstraction.
- The file tier defaults to `fsync_on_store=False`; it does not promise strong commit durability after abrupt power loss.
- SQLite owner epoch and `flock` enforce one live process per metadata database, not a distributed lease.
- The S3-compatible boundary is covered with local fakes/fault injection. Default tests do not contact an object service.
- Corruption remains an explicit checksum error after invalidating the location; only the tested S3-unavailable path is converted to a miss-like recompute result by the tier manager.
- Segment compaction is a caller-invoked synchronous maintenance operation under local locks, not an online background service.
- Topology and transport values are metadata consumed by a cost function. They do not implement a specialized transfer data plane.
