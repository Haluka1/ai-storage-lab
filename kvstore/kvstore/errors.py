class KVStoreError(Exception):
    """Base error for KV store failures."""


class BlockNotFound(KVStoreError):
    pass


class ChecksumMismatch(KVStoreError):
    pass


class MetadataMismatch(KVStoreError):
    pass


class TierUnavailable(KVStoreError):
    pass


class StoreFull(KVStoreError):
    pass


class LoadTimeout(KVStoreError):
    pass
