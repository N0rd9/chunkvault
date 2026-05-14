"""ChunkVault public package interface."""

from chunkvault.errors import (
    ChunkUnavailableError,
    ConfigurationError,
    IntegrityError,
    VaultError,
    VaultFileNotFoundError,
)
from chunkvault.models import FileRecord, VerificationIssue, VerificationReport
from chunkvault.vault import Vault

__all__ = [
    "ChunkUnavailableError",
    "ConfigurationError",
    "FileRecord",
    "IntegrityError",
    "Vault",
    "VaultError",
    "VaultFileNotFoundError",
    "VerificationIssue",
    "VerificationReport",
]
