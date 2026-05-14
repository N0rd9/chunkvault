# ChunkVault

ChunkVault is a university-level mini distributed file storage simulator. It stores files by splitting them into fixed-size chunks, replicating each chunk across simulated storage nodes, and validating recovery with SHA-256 checksums.

The project is intentionally local-first: every node is represented by a folder on disk, while SQLite stores metadata about files, chunks, replicas, and node health. This makes distributed-systems behavior easy to inspect without needing containers or cloud infrastructure.

## Features

- Chunk-based file storage with configurable chunk size
- SHA-256 hashing for full-file and per-chunk integrity checks
- Replication across multiple simulated storage nodes
- Offline node simulation for fault-tolerance testing
- Corruption simulation for integrity testing
- File recovery from any valid online replica
- SQLite metadata index
- Typer CLI for local workflows
- FastAPI service for HTTP workflows
- Pytest test suite and GitHub Actions CI

## Architecture

```text
source file
   |
   v
chunker + SHA-256
   |
   +-- chunk 0 --> node-1, node-2
   +-- chunk 1 --> node-2, node-3
   +-- chunk 2 --> node-3, node-1
   |
   v
metadata.sqlite3
```

Each stored file has:

- one file metadata record
- one record per chunk
- one record per chunk replica
- physical chunk files under `.chunkvault/nodes/node-*`

During restore, ChunkVault reads chunks in order and chooses the first online replica whose checksum matches the metadata.

## Quick Start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

Initialize a vault:

```powershell
chunkvault init --nodes 3 --replication 2 --chunk-size 1048576
```

Store a file:

```powershell
chunkvault put .\example.pdf
```

List files:

```powershell
chunkvault list
```

Restore a file:

```powershell
chunkvault get <file-id> .\restored-example.pdf
```

Simulate a node failure:

```powershell
chunkvault node-offline node-1
chunkvault verify <file-id>
```

Simulate corruption:

```powershell
chunkvault corrupt <file-id> 0 node-2
chunkvault verify <file-id>
```

## Run the API

```powershell
uvicorn chunkvault.api:app --reload
```

Useful endpoints:

- `GET /health`
- `GET /status`
- `GET /files`
- `POST /files`
- `GET /files/{file_id}/download`
- `GET /files/{file_id}/verify`
- `PATCH /nodes/{node_id}?online=false`
- `DELETE /files/{file_id}`

FastAPI's interactive docs are available at `http://127.0.0.1:8000/docs`.

## Development

Run tests:

```powershell
pytest
```

Run linting:

```powershell
ruff check .
```

## Why This Project Matters

ChunkVault demonstrates several computer science topics in one focused codebase:

- distributed storage concepts
- redundancy and fault tolerance
- hashing and integrity validation
- metadata indexing
- API design
- CLI design
- automated testing

Possible extensions:

- add erasure coding instead of simple replication
- add a web dashboard for node and chunk visualization
- add background repair for missing or corrupted replicas
- add compression and encryption per chunk
- add Docker Compose nodes with real HTTP communication
