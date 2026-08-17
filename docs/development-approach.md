# Development approach

## Disclosure

This repository is a curated public edition of a larger private experimental workspace. AI tools assisted substantial portions of the initial implementation and documentation. The maintainer defined the problem framing, architecture, system invariants, experiment design, review criteria, and claim boundaries, and has reviewed and validated the components published here.

本仓库是较大私有实验工作区的精选公开版。AI 工具参与了大量初始实现和文档整理；维护者负责问题定义、高层架构、关键不变量、实验设计、审查标准和结论边界，并对本公开版保留的核心组件进行了重新审查与验证。

This statement neither minimizes the assistance nor attributes independent authorship that the work record cannot support.

## External-system boundary

vLLM and LMCache were external components in historical private experiments; Kubernetes and MinIO/S3-compatible services were environment infrastructure. None is vendored, embedded, or required by this curated repository. Current local tests use explicit HTTP Worker test doubles and in-process S3 client fakes, and the present review validates only the Router, KVStore, I/O contract, and documentation published here.

## Maintainer-defined review boundaries

The curated edition is organized around explicit invariants rather than a complete experiment history:

- Router namespace isolation, event ordering, routability revalidation, and inflight ownership;
- KV payload/metadata commit order, acquire/release, owner epochs, tombstones, corruption handling, and compaction;
- a versioned tier-profile file contract with provenance;
- local CPU reproduction and claim/evidence separation.

The maintainer's validation here means reviewing the selected code surface, running the published commands, and constraining documentation to what those artifacts support. It is not a claim that every line originated without assistance, or that local tests constitute production validation.

## Machine-enforced review

`make test` compiles and tests the three implementation areas. `make demos` exercises the live loopback Router path and the independent KV correctness walkthrough. `make audit` checks local links, private-workspace/secret patterns, contract fixtures, and the repository's MIT license integrity. CI cleans generated files and checks worktree drift after all tasks.

These gates are intentionally deterministic and local. They do not invoke a model service, cloud API, cluster, or object endpoint.

## Human review still required

- Architectural judgment and code-level reasoning cannot be reduced to test counts.
- Historical evidence wording must be checked against the bounded source summaries.
- Security scanning is heuristic; publication still needs a manual path-by-path review.
- Any future license change requires an explicit maintainer decision and a dependency-compatibility review.
- Any future capability claim needs a code anchor, focused test, failure boundary, and documentation update.
