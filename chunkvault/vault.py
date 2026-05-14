"""Core ChunkVault storage engine."""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from chunkvault.errors import (
    ChunkUnavailableError,
    ConfigurationError,
    IntegrityError,
    VaultFileNotFoundError,
)
from chunkvault.hashing import iter_file_chunks, sha256_bytes, sha256_file
from chunkvault.models import FileRecord, VerificationIssue, VerificationReport


class Vault:
    """A local simulator for replicated, chunked file storage."""

    def __init__(
        self,
        root: str | Path = ".chunkvault",
        *,
        node_count: int = 3,
        replication_factor: int = 2,
        chunk_size: int = 1024 * 1024,
    ) -> None:
        if node_count < 1:
            raise ConfigurationError("node_count must be at least 1")
        if replication_factor < 1:
            raise ConfigurationError("replication_factor must be at least 1")
        if replication_factor > node_count:
            raise ConfigurationError("replication_factor cannot exceed node_count")
        if chunk_size < 1:
            raise ConfigurationError("chunk_size must be at least 1")

        self.root = Path(root)
        self.node_count = node_count
        self.replication_factor = replication_factor
        self.chunk_size = chunk_size
        self.db_path = self.root / "metadata.sqlite3"
        self.nodes_path = self.root / "nodes"

    def init(self) -> None:
        """Create vault directories, node folders, and database tables."""
        self.root.mkdir(parents=True, exist_ok=True)
        self.nodes_path.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            self._create_schema(connection)
            self._write_config(connection)
            for node_id in self._configured_node_ids():
                (self.nodes_path / node_id).mkdir(parents=True, exist_ok=True)
                connection.execute(
                    "INSERT OR IGNORE INTO nodes (id, online) VALUES (?, 1)",
                    (node_id,),
                )
        self._load_config()

    def put(self, source: str | Path, *, name: str | None = None) -> FileRecord:
        """Split a file into chunks and replicate each chunk across online nodes."""
        self._ensure_ready()
        source_path = Path(source)
        if not source_path.is_file():
            raise FileNotFoundError(source_path)

        online_nodes = self._online_node_ids()
        if len(online_nodes) < self.replication_factor:
            raise ChunkUnavailableError("not enough online nodes to satisfy replication factor")

        file_id = uuid.uuid4().hex
        display_name = name or source_path.name
        file_size = source_path.stat().st_size
        file_hash = sha256_file(source_path, self.chunk_size)
        created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        chunk_count = 0

        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO files (id, name, size, sha256, chunk_size, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (file_id, display_name, file_size, file_hash, self.chunk_size, created_at),
                )
                for chunk_index, chunk in enumerate(iter_file_chunks(source_path, self.chunk_size)):
                    chunk_hash = sha256_bytes(chunk)
                    connection.execute(
                        """
                        INSERT INTO chunks (file_id, chunk_index, size, sha256)
                        VALUES (?, ?, ?, ?)
                        """,
                        (file_id, chunk_index, len(chunk), chunk_hash),
                    )
                    for node_id in self._replica_nodes(online_nodes, chunk_index):
                        chunk_path = self._chunk_path(node_id, file_id, chunk_index)
                        chunk_path.parent.mkdir(parents=True, exist_ok=True)
                        chunk_path.write_bytes(chunk)
                        connection.execute(
                            """
                            INSERT INTO replicas (file_id, chunk_index, node_id, path)
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                file_id,
                                chunk_index,
                                node_id,
                                str(chunk_path.relative_to(self.root)),
                            ),
                        )
                    chunk_count += 1
        except Exception:
            self._remove_file_chunks(file_id)
            raise

        return FileRecord(
            id=file_id,
            name=display_name,
            size=file_size,
            sha256=file_hash,
            chunk_size=self.chunk_size,
            chunk_count=chunk_count,
            created_at=created_at,
        )

    def restore(self, file_id: str, destination: str | Path) -> Path:
        """Reconstruct a file from the first valid online replica of every chunk."""
        self._ensure_ready()
        record = self.get_file(file_id)
        destination_path = Path(destination)
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        with destination_path.open("wb") as output:
            for chunk in self._chunks_for_file(file_id):
                data = self._read_first_valid_replica(
                    file_id=file_id,
                    chunk_index=chunk["chunk_index"],
                    expected_hash=chunk["sha256"],
                )
                output.write(data)

        restored_hash = sha256_file(destination_path, record.chunk_size)
        if restored_hash != record.sha256:
            raise IntegrityError(
                f"restored file hash {restored_hash} did not match expected {record.sha256}"
            )
        return destination_path

    def list_files(self) -> list[FileRecord]:
        """Return stored files ordered by newest first."""
        self._ensure_ready()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT files.*, COUNT(chunks.chunk_index) AS chunk_count
                FROM files
                LEFT JOIN chunks ON files.id = chunks.file_id
                GROUP BY files.id
                ORDER BY files.created_at DESC
                """
            ).fetchall()
        return [self._record_from_row(row) for row in rows]

    def get_file(self, file_id: str) -> FileRecord:
        """Return metadata for one stored file."""
        self._ensure_ready()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT files.*, COUNT(chunks.chunk_index) AS chunk_count
                FROM files
                LEFT JOIN chunks ON files.id = chunks.file_id
                WHERE files.id = ?
                GROUP BY files.id
                """,
                (file_id,),
            ).fetchone()
        if row is None:
            raise VaultFileNotFoundError(file_id)
        return self._record_from_row(row)

    def delete(self, file_id: str) -> None:
        """Delete stored chunks and metadata for one file."""
        self._ensure_ready()
        self.get_file(file_id)
        self._remove_file_chunks(file_id)
        with self._connect() as connection:
            connection.execute("DELETE FROM files WHERE id = ?", (file_id,))

    def verify(self, file_id: str) -> VerificationReport:
        """Check replica presence, online availability, and chunk checksums."""
        self._ensure_ready()
        record = self.get_file(file_id)
        issues: list[VerificationIssue] = []
        recoverable = True

        for chunk in self._chunks_for_file(file_id):
            valid_online_replicas = 0
            replicas = self._replicas_for_chunk(file_id, chunk["chunk_index"])
            if not replicas:
                recoverable = False
                issues.append(
                    VerificationIssue(
                        kind="missing_replica",
                        chunk_index=chunk["chunk_index"],
                        node_id="none",
                        detail="chunk has no recorded replicas",
                    )
                )
                continue

            for replica in replicas:
                issue = self._inspect_replica(replica, expected_hash=chunk["sha256"])
                if issue is not None:
                    issues.append(issue)
                    continue
                valid_online_replicas += 1

            if valid_online_replicas == 0:
                recoverable = False

        return VerificationReport(
            file_id=file_id,
            chunk_count=record.chunk_count,
            healthy=not issues,
            recoverable=recoverable,
            issues=tuple(issues),
        )

    def set_node_online(self, node_id: str, online: bool) -> None:
        """Mark a storage node online or offline for failure simulation."""
        self._ensure_ready()
        with self._connect() as connection:
            result = connection.execute(
                "UPDATE nodes SET online = ? WHERE id = ?",
                (1 if online else 0, node_id),
            )
            if result.rowcount == 0:
                raise ConfigurationError(f"unknown node: {node_id}")

    def corrupt_replica(self, file_id: str, chunk_index: int, node_id: str) -> Path:
        """Intentionally corrupt one stored replica for integrity testing."""
        self._ensure_ready()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT path FROM replicas
                WHERE file_id = ? AND chunk_index = ? AND node_id = ?
                """,
                (file_id, chunk_index, node_id),
            ).fetchone()
        if row is None:
            raise ChunkUnavailableError(
                f"replica not found for file={file_id}, chunk={chunk_index}, node={node_id}"
            )
        path = self.root / row["path"]
        if not path.exists():
            raise ChunkUnavailableError(f"replica path is missing: {path}")
        with path.open("ab") as handle:
            handle.write(b"\nCHUNKVAULT_CORRUPTION_MARKER\n")
        return path

    def status(self) -> dict[str, object]:
        """Return vault configuration and node health."""
        self._ensure_ready()
        with self._connect() as connection:
            nodes = [
                {"id": row["id"], "online": bool(row["online"])}
                for row in connection.execute("SELECT id, online FROM nodes ORDER BY id").fetchall()
            ]
            file_count = connection.execute("SELECT COUNT(*) AS count FROM files").fetchone()["count"]
            chunk_count = connection.execute("SELECT COUNT(*) AS count FROM chunks").fetchone()["count"]
        return {
            "root": str(self.root),
            "node_count": len(nodes),
            "replication_factor": self.replication_factor,
            "chunk_size": self.chunk_size,
            "file_count": file_count,
            "chunk_count": chunk_count,
            "nodes": nodes,
        }

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _ensure_ready(self) -> None:
        if not self.db_path.exists():
            self.init()
        else:
            self._load_config()

    def _create_schema(self, connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS nodes (
                id TEXT PRIMARY KEY,
                online INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS files (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                size INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                chunk_size INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS chunks (
                file_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                size INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                PRIMARY KEY (file_id, chunk_index),
                FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS replicas (
                file_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                node_id TEXT NOT NULL,
                path TEXT NOT NULL,
                PRIMARY KEY (file_id, chunk_index, node_id),
                FOREIGN KEY (file_id, chunk_index)
                    REFERENCES chunks(file_id, chunk_index) ON DELETE CASCADE,
                FOREIGN KEY (node_id) REFERENCES nodes(id)
            );
            """
        )

    def _write_config(self, connection: sqlite3.Connection) -> None:
        values = {
            "node_count": str(self.node_count),
            "replication_factor": str(self.replication_factor),
            "chunk_size": str(self.chunk_size),
        }
        for key, value in values.items():
            connection.execute(
                "INSERT OR IGNORE INTO meta (key, value) VALUES (?, ?)",
                (key, value),
            )

    def _load_config(self) -> None:
        with self._connect() as connection:
            rows = connection.execute("SELECT key, value FROM meta").fetchall()
        config = {row["key"]: row["value"] for row in rows}
        if config:
            self.node_count = int(config.get("node_count", self.node_count))
            self.replication_factor = int(
                config.get("replication_factor", self.replication_factor)
            )
            self.chunk_size = int(config.get("chunk_size", self.chunk_size))

    def _configured_node_ids(self) -> list[str]:
        return [f"node-{index}" for index in range(1, self.node_count + 1)]

    def _node_ids(self) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute("SELECT id FROM nodes ORDER BY id").fetchall()
        return [row["id"] for row in rows]

    def _online_node_ids(self) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id FROM nodes WHERE online = 1 ORDER BY id"
            ).fetchall()
        return [row["id"] for row in rows]

    def _replica_nodes(self, online_nodes: list[str], chunk_index: int) -> list[str]:
        return [
            online_nodes[(chunk_index + offset) % len(online_nodes)]
            for offset in range(self.replication_factor)
        ]

    def _chunk_path(self, node_id: str, file_id: str, chunk_index: int) -> Path:
        return self.nodes_path / node_id / f"{file_id}_{chunk_index:06d}.chunk"

    def _chunks_for_file(self, file_id: str) -> list[sqlite3.Row]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT chunk_index, size, sha256
                FROM chunks
                WHERE file_id = ?
                ORDER BY chunk_index
                """,
                (file_id,),
            ).fetchall()
        return rows

    def _replicas_for_chunk(self, file_id: str, chunk_index: int) -> list[sqlite3.Row]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT replicas.*, nodes.online
                FROM replicas
                JOIN nodes ON nodes.id = replicas.node_id
                WHERE replicas.file_id = ? AND replicas.chunk_index = ?
                ORDER BY replicas.node_id
                """,
                (file_id, chunk_index),
            ).fetchall()
        return rows

    def _read_first_valid_replica(
        self,
        *,
        file_id: str,
        chunk_index: int,
        expected_hash: str,
    ) -> bytes:
        for replica in self._replicas_for_chunk(file_id, chunk_index):
            if not replica["online"]:
                continue
            path = self.root / replica["path"]
            if not path.exists():
                continue
            data = path.read_bytes()
            if sha256_bytes(data) == expected_hash:
                return data
        raise ChunkUnavailableError(
            f"no valid online replica for file={file_id}, chunk={chunk_index}"
        )

    def _inspect_replica(
        self,
        replica: sqlite3.Row,
        *,
        expected_hash: str,
    ) -> VerificationIssue | None:
        chunk_index = replica["chunk_index"]
        node_id = replica["node_id"]
        if not replica["online"]:
            return VerificationIssue(
                kind="node_offline",
                chunk_index=chunk_index,
                node_id=node_id,
                detail="node is marked offline",
            )

        path = self.root / replica["path"]
        if not path.exists():
            return VerificationIssue(
                kind="missing_replica",
                chunk_index=chunk_index,
                node_id=node_id,
                detail=f"replica file does not exist: {path}",
            )

        actual_hash = sha256_bytes(path.read_bytes())
        if actual_hash != expected_hash:
            return VerificationIssue(
                kind="corrupt_replica",
                chunk_index=chunk_index,
                node_id=node_id,
                detail=f"expected {expected_hash}, got {actual_hash}",
            )
        return None

    def _remove_file_chunks(self, file_id: str) -> None:
        if not self.db_path.exists():
            return
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT path FROM replicas WHERE file_id = ?",
                (file_id,),
            ).fetchall()
        for row in rows:
            path = self.root / row["path"]
            if path.exists():
                path.unlink()

    def _record_from_row(self, row: sqlite3.Row) -> FileRecord:
        return FileRecord(
            id=row["id"],
            name=row["name"],
            size=row["size"],
            sha256=row["sha256"],
            chunk_size=row["chunk_size"],
            chunk_count=row["chunk_count"],
            created_at=row["created_at"],
        )
