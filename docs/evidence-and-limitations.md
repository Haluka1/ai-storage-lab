# Evidence and limitations

## Evidence policy

Public implementation claims should be justified first by current code and local tests. The observations below are narrow historical summaries from a larger private experimental workspace. Their raw cloud artifacts are intentionally not included here.

They are not production QPS, SLO, availability, HA, or current-HEAD equivalence evidence. Where a historical source revision was not recorded, this page says so instead of inferring one.

## Historical H20 dual-Worker Router summary

- Recorded outcome: **512/512** completed, with **four scenarios reported as 64/64 each** in the retained summary.
- What it supports: a Router/fake-or-real-upstream functional request path was exercised on that historical setup.
- What it does not support: throughput gain, optimal load balance, production telemetry, automatic failover, or proof of every behavior in the current revision.
- Provenance caveat: the raw private artifacts are not published and the summary does not provide a source revision that can be mapped confidently to current HEAD.

## Historical Kubernetes dual-A10 controlled-routing summary

- Recorded outcome: **192/192 expected-Worker matches**.
- The routing metadata was **controlled admin-plane injection**, not automatically collected production telemetry.
- The run used the historical ordinary `cost_aware` strategy. It did not validate the current `topology_aware_cost_aware` path and must not be cited as topology-aware evidence.
- It is a functional controlled-routing observation, not balancing, performance, discovery, SLO, or HA evidence.

## Historical LMCache summary

- The historical work established that a functional LMCache path could be exercised in that environment.
- Results were neutral or negative in relevant comparisons; there is **no stable or generally applicable acceleration evidence**.
- The independent KVStore in this repository was not inserted into that real KV payload hot path.

## Explicit exclusions

- A `200/200` health-probe result is not counted as successful inference.
- Controlled Worker drain/removal/recovery is not described as automatic failover.
- No production performance percentage is derived from synthetic fixtures or local correctness tests.
- No historical cloud observation is used to claim production readiness or current-HEAD equivalence.
- The omitted recovery, object-storage, CSI, tensor-parallel, and other secondary experiments are outside this curated evidence surface.

## Evidence index for README numbers

The README files intentionally contain no experimental result counts. If a future README adds a number, it must link to a bounded entry on this page and identify whether it is current local evidence, a synthetic fixture, or historical private evidence.
