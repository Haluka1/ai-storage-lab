# Repository Guidance

## Mission

- Keep this repository small, CPU-reproducible, and reviewable at code level.
- Treat the Router and KVStore as the two primary implementation lines.
- Keep I/O Profile as a supporting file-contract module, not a third serving stack.
- Trace every public conclusion to code, tests, or clearly labelled historical evidence.

## Non-claims

Future Codex tasks and human-authored changes must not describe this repository as having implemented:

- production-grade serving;
- engine-accurate vLLM KV identity;
- end-to-end KV payload integration with vLLM or LMCache;
- production telemetry;
- automatic failover;
- Router high availability;
- RDMA, GDS, CXL, or SPDK data planes;
- production NVMe performance;
- stable LMCache acceleration.

Metadata fields, protocol abstractions, historical experiments, and fake Workers are not substitutes for those implementations.

## Required commands

```bash
make test
make demo
make audit
make clean
```

Run all four before proposing a release candidate. `make demo-kv` is also available for the independent KV correctness walkthrough.

## Change policy

- Tests and demos must not rewrite tracked evidence or source files.
- Never present synthetic fixture values as performance gains or hardware measurements.
- Do not add cloud-resource, GPU, cluster, object-service, or external-endpoint dependencies.
- Every feature change must add focused tests, update its boundary statement, and update relevant documentation.
- Every number in a README must have a corresponding entry in [docs/evidence-and-limitations.md](docs/evidence-and-limitations.md).
- Keep control-plane mutation bound to loopback in local examples.
- The repository is licensed under MIT. Changing or removing the license requires an explicit maintainer decision.
