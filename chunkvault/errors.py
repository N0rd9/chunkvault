"""Domain exceptions raised by ChunkVault."""


class VaultError(Exception):
    """Base class for ChunkVault failures."""


class ConfigurationError(VaultError):
    """Raised when a vault is configured with invalid settings."""


class VaultFileNotFoundError(VaultError):
    """Raised when metadata for a requested file does not exist."""


class ChunkUnavailableError(VaultError):
    """Raised when no valid online replica exists for a chunk."""


class IntegrityError(VaultError):
    """Raised when reconstructed content fails integrity validation."""
