# Go Router

This module is the repository's live request path. It implements an OpenAI-compatible subset for `POST /v1/completions` and `POST /v1/chat/completions`, derives Router-local approximate prefix identities, selects a routable Worker, revalidates/accounts for it under a lock, and proxies the upstream body without buffering the complete response. It does not claim full OpenAI API conformance.

## Local commands

From the repository root:

```bash
make test-router
make demo
```

Or build directly:

```bash
cd router
go build -buildvcs=false ./...
go test -buildvcs=false ./...
go test -buildvcs=false -race ./...
```

The example [`configs/router.local.json`](configs/router.local.json) binds both control and request listeners to loopback and points to two local placeholder Worker ports. Use `make demo` to start actual fake Workers for that shape.

## HTTP surface

Public listener:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `POST /v1/completions`
- `POST /v1/chat/completions`

Loopback-only admin listener:

- `GET|POST /admin/workers`
- `POST /admin/workers/{id}/drain`
- `POST /admin/workers/{id}/undrain`
- `POST /admin/events`

Admin Worker/event input is a controlled metadata plane, not automatic discovery or telemetry. Configuration rejects a non-loopback admin address.

## Identity boundary

[`internal/blockhash`](internal/blockhash/) implements a deterministic approximation and chained namespace isolation. Its revision string identifies this repository's algorithm. It does not reproduce a model engine's tokenizer or authoritative KV identity.

[`internal/cacheindex`](internal/cacheindex/) holds expiring observations. Its overlap score counts individually observed block IDs and does not prove a contiguous reusable prefix or payload existence. Full reasoning is in [architecture.md](../docs/architecture.md).

## Lifecycle boundary

Selection and lifecycle accounting are separate steps. [`reserveRoutableWorker`](internal/proxy/handler.go) holds the Worker lock while it rechecks readiness/drain state and increments Router-owned inflight. The count prevents unsafe removal while this Router owns a request; it does not reserve Worker capacity. Removal requires an explicitly draining, idle Worker and zero Worker-reported plus Router-owned inflight. A Worker ID cannot change URL/topology in place; safe removal purges that ID's cache observations before an explicit later registration. Worker IDs have no process-generation fence, so an old event producer must be quiesced before ID reuse. There is no transparent retry—including after a stream starts—automatic discovery, or Router HA.

## Logging and metrics

Decision/trace files are disabled by default. Request-derived and tenant-derived identifiers are hashed, and records do not contain raw request text or full block hashes. Opt-in decision logs still contain Worker IDs, topology, and candidate control-plane metadata for debugging. The proxy response exposes selected Worker, strategy, and a redacted request hash for reviewability.
