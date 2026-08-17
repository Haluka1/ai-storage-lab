# KVStore protocol prototype

This Python module explores correctness contracts for a complete namespaced `BlockKey` across host memory, local files, and an optional S3-compatible client abstraction. It is independent of the Go Router and is not integrated into a real vLLM/LMCache KV payload hot path.

## Local commands

```bash
make test-kv
make demo-kv
```

Tests use temporary directories and in-process client fakes. The default [`configs/local.yaml`](configs/local.yaml) disables S3 and writes only beneath an explicit temporary path.

## Tiers

- `MemoryTier` stores bytes in **host memory**, not HBM.
- `NVMeTier` is the retained class name for a **file-backed abstraction** with content-addressed and segmented layouts. `FileBackedTier` is the clearer public alias. This code is not a validated production NVMe engine.
- `S3Tier` accepts an injected S3-compatible client or an optional package-managed client. Default tests neither require credentials nor contact an endpoint.

## Correctness contracts

- `BlockKey` requires non-empty bounded tenant/model/revision/tokenizer fields and a 64-character lowercase SHA-256 block hash. A versioned canonical JSON namespace distinguishes field boundaries and absent optional values; file and S3-compatible layouts share prefixed base64url path components.
- Store publishes payload first and metadata last. A physical orphan from a failed metadata commit is not implicitly rehydrated by normal lookup.
- Within one metadata database, an active `BlockKey` is immutable across all tiers: storing the same bytes and payload descriptor is idempotent, while conflicting bytes or dtype/shape/token metadata require explicit tombstone-backed deletion before a new publication.
- Metadata upsert requires matching metadata/location keys, tier, length, and checksum. Memory writes roll back bytes and capacity if that commit fails.
- Load uses `lookup_and_acquire`, revalidates persisted file/segment locations against the configured root and canonical percent-encoded S3 location URIs against the configured bucket/prefix, then checks the bounded record structure, identity, payload length, and SHA-256 before release.
- A single live process owns one SQLite metadata database through an adjacent `flock` and owner epoch. Crash reopening clears counters left by the dead epoch.
- `deleting` tombstones make deletion logically visible before physical cleanup and survive incomplete cleanup.
- Segment records are append-only; a caller-invoked synchronous compaction pass reclaims logical deletions and skips acquired blocks. There is no online background compactor.
- Cost decisions explicitly distinguish synchronous load, asynchronous prefetch, and recompute.
- S3-compatible HEAD/GET/body-read timeout or unavailability and record/content corruption are separate failure classes. Obtained streaming bodies are closed, including failure paths. An unavailable S3 observation does not hide a healthy local location; with no healthy location the tier manager returns a miss-like recompute result. Corruption is surfaced and is not automatically converted to recompute.
- Asynchronous prefetch deduplicates by `(BlockKey, target tier)` and promotes to the caller-requested tier.

## Durability and coordination boundary

The file tier defaults to `fsync_on_store: false`; atomic visibility is not a strong durable commit guarantee after abrupt power loss. There is no automatic orphan recovery/garbage collector. It does not implement `O_DIRECT`, and `use_direct_io: true` fails explicitly. The configured file root is assumed to be controlled by the process; this prototype is not a hardened multi-user filesystem sandbox. SQLite locking is one-process coordination, not a distributed lease or multi-host ownership protocol.

The tier-profile importer validates [`shared/schema/tier_profile.schema.json`](../shared/schema/tier_profile.schema.json) and records artifact SHA-256/provenance before supplying values to the cost model.
