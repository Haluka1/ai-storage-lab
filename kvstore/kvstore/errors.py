class KVStoreError(Exception):
    """Base error for KV store failures."""


class BlockNotFound(KVStoreError):
    pass


class CorruptionDetected(KVStoreError):
    """A persisted payload or record cannot be trusted."""


class ChecksumMismatch(CorruptionDetected):
    pass


class RecordFormatError(CorruptionDetected):
    pass


class CorruptionCleanupFailed(CorruptionDetected):
    """Corruption was detected but invalidation could not be completed."""

    def __init__(
        self,
        block_hash: str,
        operation: str,
        cleanup_error: Exception,
        corruption_error: CorruptionDetected,
    ):
        super().__init__(
            f"{block_hash}: corruption detected; {operation} cleanup failed"
        )
        self.block_hash = block_hash
        self.operation = operation
        self.cleanup_error = cleanup_error
        self.corruption_error = corruption_error


class MetadataMismatch(CorruptionDetected):
    pass


class ImmutableBlockConflict(KVStoreError):
    """An existing BlockKey was presented with different payload bytes."""


class TierUnavailable(KVStoreError):
    pass


class StoreFull(KVStoreError):
    pass


class LoadTimeout(KVStoreError):
    pass
