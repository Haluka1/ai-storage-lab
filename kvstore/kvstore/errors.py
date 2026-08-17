class KVStoreError(Exception):
    """Base error for KV store failures."""


class BlockNotFound(KVStoreError):
    pass


class ChecksumMismatch(KVStoreError):
    pass


class CorruptionCleanupFailed(ChecksumMismatch):
    """Corruption was detected but invalidation could not be completed."""

    def __init__(self, block_hash: str, operation: str, cleanup_error: Exception):
        super().__init__(
            f"{block_hash}: corruption detected; {operation} cleanup failed"
        )
        self.block_hash = block_hash
        self.operation = operation
        self.cleanup_error = cleanup_error


class MetadataMismatch(KVStoreError):
    pass


class TierUnavailable(KVStoreError):
    pass


class StoreFull(KVStoreError):
    pass


class LoadTimeout(KVStoreError):
    pass
