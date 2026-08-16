"""Multi-tier KV storage protocol prototype."""

from .nvme_tier import NVMeTier

# Public wording uses ``FileBackedTier``.  ``NVMeTier`` remains as a
# compatibility name, but the implementation is a local-file abstraction.
FileBackedTier = NVMeTier

__all__ = ["FileBackedTier", "NVMeTier"]
