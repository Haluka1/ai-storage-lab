from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass

from .metadata import BlockKey, TierName


@dataclass(frozen=True)
class PrefetchSubmitResult:
    submitted: bool
    reason: str


class Prefetcher:
    def __init__(self, store, target_tier: TierName = TierName.MEMORY, max_workers: int = 2, max_queue: int = 64):
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        if max_queue <= 0:
            raise ValueError("max_queue must be positive")
        self.store = store
        self.target_tier = target_tier
        self.max_queue = max_queue
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()
        self._seen: set[BlockKey] = set()
        self._submitted = 0
        self._dropped = 0
        self._failed = 0

    def submit(self, key: BlockKey) -> PrefetchSubmitResult:
        with self._lock:
            if key in self._seen:
                self._dropped += 1
                return PrefetchSubmitResult(False, "duplicate")
            if len(self._seen) >= self.max_queue:
                self._dropped += 1
                return PrefetchSubmitResult(False, "queue_full")
            self._seen.add(key)
            self._submitted += 1
        future = self._executor.submit(self._run_one, key)
        future.add_done_callback(lambda fut, k=key: self._done(k, fut))
        return PrefetchSubmitResult(True, "submitted")

    def stats(self) -> dict:
        with self._lock:
            return {
                "pending": len(self._seen),
                "submitted": self._submitted,
                "dropped": self._dropped,
                "failed": self._failed,
            }

    def shutdown(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)

    def _run_one(self, key: BlockKey) -> None:
        self.store.prefetch([key], target_tier=self.target_tier)

    def _done(self, key: BlockKey, future: Future) -> None:
        failed = future.exception() is not None
        with self._lock:
            self._seen.discard(key)
            if failed:
                self._failed += 1
