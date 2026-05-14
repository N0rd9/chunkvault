"""Dataclasses used by the ChunkVault engine and API."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class FileRecord:
    id: str
    name: str
    size: int
    sha256: str
    chunk_size: int
    chunk_count: int
    created_at: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class VerificationIssue:
    kind: str
    chunk_index: int
    node_id: str
    detail: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class VerificationReport:
    file_id: str
    chunk_count: int
    healthy: bool
    recoverable: bool
    issues: tuple[VerificationIssue, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "file_id": self.file_id,
            "chunk_count": self.chunk_count,
            "healthy": self.healthy,
            "recoverable": self.recoverable,
            "issues": [issue.as_dict() for issue in self.issues],
        }
