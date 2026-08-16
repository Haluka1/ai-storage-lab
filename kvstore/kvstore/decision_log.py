from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .cost_model import Decision
from .metadata import BlockKey


@dataclass
class OnloadDecisionRecord:
    run_id: str
    timestamp_ms: int
    request_id_hash: str
    block_hash_prefix: str
    strategy: str
    tier: str
    selected_tier: str
    selected_transport: str
    decision: str
    estimated_load_ms: float
    actual_load_ms: float | None
    estimated_recompute_ms: float
    actual_recompute_ms: float | None
    decision_error_ms: float | None
    slo_budget_ms: float | None
    reason: str
    preferred_transport: str | None = None
    transport_available: bool | None = None
    optional_transport_available: bool | None = None
    decision_type: str = "kv_onload"


class OnloadDecisionLogger:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: OnloadDecisionRecord) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(record), sort_keys=True, separators=(",", ":")) + "\n")

    def log(
        self,
        run_id: str,
        request_id: str,
        key: BlockKey,
        decision: Decision,
        selected_transport: str,
        actual_load_ms: float | None,
        actual_recompute_ms: float | None,
        slo_budget_ms: float | None,
        strategy: str = "cost_based",
        preferred_transport: str | None = None,
        transport_available: bool | None = None,
        optional_transport_available: bool | None = None,
    ) -> None:
        self.append(
            make_onload_decision_record(
                run_id=run_id,
                request_id=request_id,
                key=key,
                decision=decision,
                selected_transport=selected_transport,
                actual_load_ms=actual_load_ms,
                actual_recompute_ms=actual_recompute_ms,
                slo_budget_ms=slo_budget_ms,
                strategy=strategy,
                preferred_transport=preferred_transport,
                transport_available=transport_available,
                optional_transport_available=optional_transport_available,
            )
        )


def make_onload_decision_record(
    run_id: str,
    request_id: str,
    key: BlockKey,
    decision: Decision,
    selected_transport: str,
    actual_load_ms: float | None,
    actual_recompute_ms: float | None,
    slo_budget_ms: float | None,
    strategy: str = "cost_based",
    timestamp_ms: int | None = None,
    preferred_transport: str | None = None,
    transport_available: bool | None = None,
    optional_transport_available: bool | None = None,
) -> OnloadDecisionRecord:
    chosen_actual = actual_load_ms if decision.action == "load" else actual_recompute_ms
    estimated_chosen = decision.estimated_load_ms if decision.action == "load" else decision.estimated_recompute_ms
    decision_error_ms = None if chosen_actual is None else chosen_actual - estimated_chosen
    return OnloadDecisionRecord(
        run_id=run_id,
        timestamp_ms=timestamp_ms if timestamp_ms is not None else int(time.time() * 1000),
        request_id_hash=_hash_prefix(request_id, 16),
        block_hash_prefix=key.block_hash[:16],
        strategy=strategy,
        tier=decision.tier.value,
        selected_tier=decision.tier.value,
        selected_transport=selected_transport,
        decision=decision.action,
        estimated_load_ms=decision.estimated_load_ms,
        actual_load_ms=actual_load_ms,
        estimated_recompute_ms=decision.estimated_recompute_ms,
        actual_recompute_ms=actual_recompute_ms,
        decision_error_ms=decision_error_ms,
        slo_budget_ms=slo_budget_ms,
        reason=decision.reason,
        preferred_transport=preferred_transport,
        transport_available=transport_available,
        optional_transport_available=optional_transport_available,
    )


def _hash_prefix(value: str, length: int) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def sanitize_decision_for_top_candidates(decision: Decision) -> dict[str, Any]:
    return {
        "tier": decision.tier.value,
        "action": decision.action,
        "estimated_load_ms": decision.estimated_load_ms,
        "estimated_recompute_ms": decision.estimated_recompute_ms,
        "benefit_ms": decision.benefit_ms,
        "reason": decision.reason,
    }
