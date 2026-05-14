"""Hashing and chunk streaming helpers."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path


def sha256_bytes(data: bytes) -> str:
    """Return the SHA-256 digest for an in-memory byte string."""
    return hashlib.sha256(data).hexdigest()


def iter_file_chunks(path: Path, chunk_size: int) -> Iterator[bytes]:
    """Yield file chunks without loading the full file into memory."""
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            yield chunk


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return the SHA-256 digest for a file."""
    digest = hashlib.sha256()
    for chunk in iter_file_chunks(path, chunk_size):
        digest.update(chunk)
    return digest.hexdigest()
