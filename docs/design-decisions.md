# Design decisions

Each decision records the implemented guarantee and the edge it does not cross.

## 1. Namespace-isolated chained block identity

### Problem

A plain hash of one token block can collide semantically across tenants, model revisions, tokenizer revisions, adapters, modalities, or cache-policy revisions. Independently hashing blocks also loses prefix ancestry: identical later tokens after different prefixes should not imply an identical prefix-cache block.

### Decision

Hash each block from its parent hash, block-token count and token IDs, plus an ordered isolation key containing tenant ID/salt, model ID/revision, tokenizer revision, adapter ID, modality key, and cache salt. The first block uses a fixed zero parent. Router token IDs come from this repository's deterministic approximation.

### Alternatives

- Hash prompt text directly: simpler, but it does not define block boundaries or reusable ancestry.
- Hash token blocks without namespace: unsafe across isolation domains.
- Reuse model-engine hashes: preferable for an integrated engine, but no authoritative engine identity is available on this independent Router path.

### Failure semantics

Changing an isolation field or tokenizer revision produces a different chain and therefore a cache miss. An empty text produces no blocks. The algorithm does not attempt to translate a mismatched engine identity.

### Current guarantees

Go and Python implementations are checked against shared vectors. Chaining and namespace fields are deterministic for the declared approximation revision.

### Current limitations

The approximation is not vLLM tokenization and the resulting hash is not vLLM's authoritative KV block identity. It is suitable for Router-local metadata experiments only.

### Code anchors

- [Go chained hash](../router/internal/blockhash/blockhash.go)
- [Go approximate tokenizer](../router/internal/blockhash/tokenization.go)
- [Cross-language vectors](../shared/fixtures/blockhash_vectors.json)

## 2. Cache TTL, event ID, and per-Worker sequence

### Problem

Cache metadata is an observation that can become stale, arrive twice, or arrive out of order. Counting every event or allowing an older eviction/store to overwrite newer state can misroute requests.

### Decision

Give locations an expiry, validate an event before deduplication, deduplicate non-empty event IDs, and track the highest positive producer sequence number per Worker separately from local revisions. Reads ignore expired locations. `ApplyEvent` rejects a positive sequence that is not strictly newer for that Worker; sequence zero remains available for internal local callers that do not claim ordered delivery. The admin event API requires a positive sequence, and reserves the maximum signed sequence so a subsequent local revision remains representable.

### Alternatives

- Trust the latest arrival time: network arrival order is not source order.
- Require a global sequence: creates unnecessary coupling between independent Workers.
- Keep metadata forever: makes disappeared payloads increasingly likely to be treated as hits.

### Failure semantics

An event rejected by `ApplyEvent` validation returns an error without consuming its event ID or advancing a watermark, so a corrected retry can be applied. A repeated valid non-empty event ID becomes a no-op. A valid event whose positive sequence is not newer than the Worker's accepted sequence is ignored but still consumes that immutable event ID; a zero sequence makes no ordering claim and does not advance the producer watermark. Expired locations can remain in the in-memory map until explicit eviction, overwrite, or reconstruction but do not count as overlap.

### Current guarantees

Event application and location maps are lock-protected. Snapshot schema v2 persists producer watermarks separately from local revisions. Tests cover TTL, duplicate IDs, corrected invalid-event retries, stale store/evict events, zero-sequence isolation, sequence bounds, snapshots, and event replay.

### Current limitations

The Router does not persist a production event stream in this edition. Admin-injected events and local event-log helpers do not constitute telemetry delivery guarantees. Expired map entries and remembered event IDs have no continuous in-process retention bound, so the index is not a production metadata-retention service.

### Code anchors

- [CacheIndex](../router/internal/cacheindex/index.go)
- [Event model and replay](../router/internal/cacheindex/eventlog.go)
- [CacheIndex tests](../router/internal/cacheindex/index_test.go)

## 3. Post-pick revalidation and Router-owned inflight

### Problem

A strategy selects from a snapshot. A Worker can begin draining or be replaced after that snapshot but before proxying. Worker-reported inflight also cannot account for a Router request that has just been selected but not yet observed upstream.

### Decision

Treat strategy selection as advisory. Reacquire the Worker mutex, find the selected registration, re-run `Routable`, and increment a Router-owned inflight counter while holding the same lock. Keep that lifecycle count until the complete response body, including a stream, has been copied. Removal checks the sum of Worker-reported and Router-owned inflight along with drain/queue/decode state. The counter does not reserve Worker compute, memory, or KV capacity. A stable Worker ID cannot change URL or topology in place: the old identity must be drained and removed, which also purges its CacheIndex observations, before a later explicit registration.

### Alternatives

- Proxy directly from the selection snapshot: admits a drain race.
- Overwrite Worker telemetry with Router counts: conflates owners and can under/over-count.
- Automatically retry another Worker: unsafe once request effects or stream bytes may have begun, and not implemented here.

### Failure semantics

If the Worker disappeared or is no longer routable at reservation time, the Router returns an unavailable error before sending upstream. It does not claim transparent failover. An upstream request or response-copy error still releases the lifecycle count through `defer`; the Router does not retry on another Worker, including after any stream bytes may have been written. Worker endpoints are explicit HTTP(S) URLs, and an upstream redirect is returned as a fixed Router error rather than followed. An attempted in-place URL/topology mutation is rejected without changing the registration.

### Current guarantees

The reservation mutation and routability recheck share one lock. Tests force a drain between selection and reservation, hold a streaming body open while testing removal safety, reject in-place identity mutation, and verify that retirement purges cache observations before ID reuse.

### Current limitations

This is single-Router process state. It is neither capacity admission nor Router HA, a distributed inflight ledger, or automatic recovery across Router crashes. Worker IDs have no generation field, so the control plane must quiesce an old event producer before intentionally reusing its ID.

### Code anchors

- [Proxy request and reservation path](../router/internal/proxy/handler.go)
- [Worker lifecycle types](../router/internal/common/types.go)
- [Concurrency tests](../router/internal/proxy/proxy_test.go)

## 4. Payload-first, metadata-last stores

### Problem

Publishing metadata before bytes are completely available lets a concurrent lookup discover a location that contains a partial payload. File and object APIs have different mechanics, but both need the same visibility rule.

### Decision

Write the payload representation first, then commit the authoritative metadata record. The file-backed content-addressed layout writes a temporary file and atomically replaces the final path before metadata upsert. Segments append a complete header/payload record before metadata upsert. The S3-compatible tier uploads the object before metadata upsert. A physical payload without active metadata is not implicitly rehydrated. Within one metadata database, an active `BlockKey` is immutable across tiers: identical bytes plus the same dtype/shape/token descriptor are an idempotent replay, while conflicting bytes or descriptor fields are rejected before physical replacement. `BlockKey` uses a versioned canonical JSON namespace for SQLite and shared prefixed base64url components for file/S3-compatible keys; required fields and the lowercase SHA-256 block hash are validated before any path is generated. S3 object keys stay literal at the client boundary while their persisted location paths use a strict canonical percent-encoding.

### Alternatives

- Metadata-first: exposes partial/missing payloads.
- One distributed transaction across payload and SQLite: unavailable across local files/S3 without a much larger protocol.
- Trust existence alone on load: does not detect mismatched identity or corruption.

### Failure semantics

A failure before metadata commit can leave an orphan payload, but normal lookup does not publish or return it. A load revalidates a persisted location against the tier's configured root or S3 bucket/prefix, then checks the bounded record header, version, required metadata, identity, length, and checksum. Structural or content corruption raises a distinct error and invalidates the bad location. MemoryTier rolls its payload and capacity counters back if metadata commit fails.

### Current guarantees

Within one live metadata owner, mutation locking serializes competing store/delete operations. Metadata upsert enforces equality across the call key, metadata key, location key, length, checksum, and tier, and compares the payload descriptor with every active/deleting tier row for that key. Tests cover concurrent and sequential cross-tier conflicts, immutable active keys, failed metadata commit, orphan invisibility, adversarial namespace values, persisted-location boundaries and canonical encodings for URIs, file roots, and buckets, bounded malformed headers, identity/length/checksum mismatch, and S3 unavailability as a separate class.

### Current limitations

Default file stores do not call a strong durable commit path (`fsync_on_store=False`), so payload-first ordering is not a sudden-power-loss durability guarantee. There is no automatic orphan-recovery or orphan-GC service. S3 semantics depend on the supplied compatible client.

### Code anchors

- [File-backed store/load](../kvstore/kvstore/nvme_tier.py)
- [S3-compatible store/load](../kvstore/kvstore/s3_tier.py)
- [Metadata store](../kvstore/kvstore/metadata_store.py)

## 5. Deleting tombstone before physical cleanup

### Problem

Deleting a file/object before changing metadata can race with lookup. Deleting metadata first without recording intent loses the durable distinction between an intentionally deleting payload and an orphan left by a failed store.

### Decision

`begin_delete` changes the metadata state to `deleting` and removes it from active lookup/capacity accounting. Physical deletion follows. `finish_delete` removes the tombstone only after cleanup. Segment records are append-only: logical tombstone completion makes the record unreachable, and a caller may later run synchronous compaction to reclaim bytes.

### Alternatives

- Best-effort physical delete followed by metadata delete: leaves race windows and ambiguous crash recovery.
- Immediately erase metadata: loses deletion intent while physical bytes may still exist.
- In-place segment rewriting for every eviction: increases write amplification and concurrency complexity.

### Failure semantics

If physical deletion fails, the tombstone remains and lookup returns a miss; a later eviction retries cleanup. If physical bytes disappeared but `finish_delete` did not commit before a crash, a reopened owner can complete the tombstone. Stores to the same key are rejected while deletion is pending.

### Current guarantees

Tests cover unlink failure, crash points before/after physical deletion, tombstone persistence across reopen, store/delete serialization, segment logical eviction, and compaction skipping acquired records.

### Current limitations

Tombstones live in one SQLite database under a single-process owner. They are not distributed consensus and do not coordinate independent hosts. Compaction is an explicit, lock-held maintenance call, not a continuously online background compactor.

### Code anchors

- [Tombstone state transitions](../kvstore/kvstore/metadata_store.py)
- [File-backed eviction and compaction](../kvstore/kvstore/nvme_tier.py)
- [Tombstone tests](../kvstore/tests/test_nvme_tier.py)

## 6. Load versus recompute

### Problem

A located KV block is not automatically worth loading. Fixed latency, transfer time, deserialization, host-to-device assumptions, missing-token recomputation, reuse probability, and an SLO guard can make load, prefetch, or recompute preferable.

### Decision

Represent those inputs in a small cost model. A synchronous `load` returns bytes; `prefetch` queues a non-blocking promotion request; `recompute` returns a miss-like decision to the caller. File-derived parameters can enter through the versioned tier-profile contract, with source/provenance recorded by the importer.

### Alternatives

- Always load the closest located tier: ignores small-prefix and latency-budget cases.
- Hard-code one measured machine: creates false portability and stale evidence.
- Hide prefetch behind `load`: obscures non-blocking behavior and error handling.

### Failure semantics

Missing or corrupt payloads do not silently become hits. Record-format damage, metadata/identity mismatch, and checksum mismatch invalidate the source and propagate as corruption subclasses; an invalidation failure becomes `CorruptionCleanupFailed` with both the original corruption and cleanup cause preserved. The tier manager does not translate corruption into recomputation. Tested S3 HEAD, GET-call, bounded body-read, and body-close unavailability is instead isolated from healthy tiers and becomes a miss-like recompute result only when no usable location remains. S3 records are read as a bounded prefix/header/payload/trailing-byte sequence, and only an explicit `NoSuchKey` is object absence; bucket, authorization, and ambiguous `404` failures remain unavailable. Every acquired streaming response body is closed. A selected asynchronous prefetch does not block the request, is deduplicated by key and target tier, promotes to the requested target, and rejects an unconfigured target before submission.

### Current guarantees

Unit tests cover short-prefix recompute, longer-prefix load, reuse-sensitive S3 decisions, target-aware asynchronous prefetch, missing-target rejection, promotion, structural/content corruption and cleanup classification, bounded S3 record reads, explicit object-not-found versus service-unavailable classification, S3 lookup/call/body timeout fallback and response-body cleanup without hiding a healthy file tier, and Schema-validated tier-profile import.

### Current limitations

Default profiles are explicit synthetic assumptions unless replaced with a validated artifact. Profile import is a file contract, not live telemetry. The model does not prove end-to-end latency benefit, and the KVStore is not connected to a model engine that performs the recomputation.

### Code anchors

- [Cost model](../kvstore/kvstore/cost_model.py)
- [Tier manager decision path](../kvstore/kvstore/tier_manager.py)
- [Tier-profile importer](../kvstore/kvstore/tier_profile_import.py)
- [Cost-model tests](../kvstore/tests/test_cost_model.py)
