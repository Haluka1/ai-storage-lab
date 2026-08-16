# Shared contracts

This directory contains only cross-module contracts retained by the curated edition:

- `fixtures/blockhash_vectors.json` and `fixtures/tokenization_vectors.json` keep Go/Python Router approximation behavior aligned.
- `schema/tier_profile.schema.json` defines tier-profile contract version 1.
- `fixtures/local-contract.tier-profile.json` is synthetic contract input, not measured evidence.
- `python/` is the reference Python implementation used by cross-language tests and the local Demo.

Changing an identity vector or contract version requires synchronized implementation/tests and an explanation in the relevant design decision.
