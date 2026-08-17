# Reproduction boundary

## Fully reproducible locally

The following paths are intended to run on an ordinary Linux CPU with no external service:

```bash
make test-router
make test-kv
make test-io
make test-demo
make demo
make demo-kv
make audit
```

The Router tests use Go in-process HTTP test doubles. The Router Demo binds only loopback ports and builds into a temporary directory. KV tests use temporary host memory, files, SQLite, and in-process S3 client fakes. I/O tests create small temporary files and accept a structured unavailable result if the local filesystem rejects `O_DIRECT`; the `mmap` operation scans every byte in its selected mapped block rather than reporting page touches as a full-block read.

The Python dependency in `requirements-ci.txt` is required for JSON Schema validation. Installing packages is an environment-preparation step; tests and demos themselves do not make outbound requests.

## Historical hardware experiment summaries

[Evidence and limitations](evidence-and-limitations.md) retains three bounded historical observations: a dual-H20 Router run, a dual-A10 controlled-routing run, and a functional-but-not-accelerating LMCache path. They are context, not part of `make test`, and no script in this repository attempts to recreate them.

## Unpublished or non-reproducible cloud environment

Cloud manifests, account/resource identifiers, private registry details, operational runbooks, billing helpers, raw logs, and hardware inventories are not present. Reproduction of those historical environments is therefore intentionally outside this repository.

No local command requires Kubernetes, a GPU, an object-service endpoint, CSI, or a cloud account. Adding such a dependency would violate [AGENTS.md](../AGENTS.md) unless the repository mission is explicitly reconsidered.

## Interpreting generated output

- I/O matrix output belongs in a caller-selected temporary path and is not checked in.
- The shared tier-profile fixture is synthetic schema/contract data and must never be summarized as a measured latency or bandwidth result.
- A successful fake-Worker request proves Router mechanics, not model inference quality.
- A passing KV corruption test proves the implemented checksum path under its tested failure, not storage-device durability.
