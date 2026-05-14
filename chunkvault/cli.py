"""Command line interface for ChunkVault."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from chunkvault.errors import VaultError
from chunkvault.vault import Vault

app = typer.Typer(help="ChunkVault: a replicated mini file storage simulator.")


def _vault(root: Path) -> Vault:
    return Vault(root=root)


def _handle_error(error: Exception) -> None:
    if isinstance(error, VaultError):
        raise typer.BadParameter(str(error)) from error
    raise error


@app.command()
def init(
    root: Path = typer.Option(Path(".chunkvault"), "--root", "-r", help="Vault data directory."),
    nodes: int = typer.Option(3, "--nodes", "-n", help="Number of simulated storage nodes."),
    replication: int = typer.Option(2, "--replication", help="Replicas written per chunk."),
    chunk_size: int = typer.Option(1024 * 1024, "--chunk-size", help="Chunk size in bytes."),
) -> None:
    """Initialize a new vault."""
    try:
        vault = Vault(
            root=root,
            node_count=nodes,
            replication_factor=replication,
            chunk_size=chunk_size,
        )
        vault.init()
    except Exception as error:
        _handle_error(error)
    typer.echo(f"Initialized vault at {root}")


@app.command()
def put(
    source: Path = typer.Argument(..., exists=True, dir_okay=False, help="File to store."),
    name: str | None = typer.Option(None, "--name", help="Display name inside the vault."),
    root: Path = typer.Option(Path(".chunkvault"), "--root", "-r", help="Vault data directory."),
) -> None:
    """Store a file as replicated chunks."""
    try:
        record = _vault(root).put(source, name=name)
    except Exception as error:
        _handle_error(error)
    typer.echo(json.dumps(record.as_dict(), indent=2))


@app.command("list")
def list_files(
    root: Path = typer.Option(Path(".chunkvault"), "--root", "-r", help="Vault data directory."),
) -> None:
    """List files stored in the vault."""
    try:
        records = _vault(root).list_files()
    except Exception as error:
        _handle_error(error)

    if not records:
        typer.echo("No files stored.")
        return

    for record in records:
        typer.echo(
            f"{record.id}  {record.name}  {record.size} bytes  "
            f"{record.chunk_count} chunks  {record.created_at}"
        )


@app.command()
def get(
    file_id: str = typer.Argument(..., help="Stored file id."),
    destination: Path = typer.Argument(..., help="Where to write the restored file."),
    root: Path = typer.Option(Path(".chunkvault"), "--root", "-r", help="Vault data directory."),
) -> None:
    """Restore a file from valid online replicas."""
    try:
        path = _vault(root).restore(file_id, destination)
    except Exception as error:
        _handle_error(error)
    typer.echo(f"Restored to {path}")


@app.command()
def delete(
    file_id: str = typer.Argument(..., help="Stored file id."),
    root: Path = typer.Option(Path(".chunkvault"), "--root", "-r", help="Vault data directory."),
) -> None:
    """Delete a file and its replicas."""
    try:
        _vault(root).delete(file_id)
    except Exception as error:
        _handle_error(error)
    typer.echo(f"Deleted {file_id}")


@app.command()
def verify(
    file_id: str = typer.Argument(..., help="Stored file id."),
    root: Path = typer.Option(Path(".chunkvault"), "--root", "-r", help="Vault data directory."),
) -> None:
    """Verify replica availability and checksums."""
    try:
        report = _vault(root).verify(file_id)
    except Exception as error:
        _handle_error(error)
    typer.echo(json.dumps(report.as_dict(), indent=2))


@app.command()
def status(
    root: Path = typer.Option(Path(".chunkvault"), "--root", "-r", help="Vault data directory."),
) -> None:
    """Show vault configuration and node states."""
    try:
        snapshot = _vault(root).status()
    except Exception as error:
        _handle_error(error)
    typer.echo(json.dumps(snapshot, indent=2))


@app.command("node-online")
def node_online(
    node_id: str = typer.Argument(..., help="Node id such as node-1."),
    root: Path = typer.Option(Path(".chunkvault"), "--root", "-r", help="Vault data directory."),
) -> None:
    """Mark a simulated node online."""
    try:
        _vault(root).set_node_online(node_id, True)
    except Exception as error:
        _handle_error(error)
    typer.echo(f"{node_id} is online")


@app.command("node-offline")
def node_offline(
    node_id: str = typer.Argument(..., help="Node id such as node-1."),
    root: Path = typer.Option(Path(".chunkvault"), "--root", "-r", help="Vault data directory."),
) -> None:
    """Mark a simulated node offline."""
    try:
        _vault(root).set_node_online(node_id, False)
    except Exception as error:
        _handle_error(error)
    typer.echo(f"{node_id} is offline")


@app.command()
def corrupt(
    file_id: str = typer.Argument(..., help="Stored file id."),
    chunk_index: int = typer.Argument(..., help="Chunk index to corrupt."),
    node_id: str = typer.Argument(..., help="Node id containing the replica."),
    root: Path = typer.Option(Path(".chunkvault"), "--root", "-r", help="Vault data directory."),
) -> None:
    """Intentionally corrupt one replica for testing."""
    try:
        path = _vault(root).corrupt_replica(file_id, chunk_index, node_id)
    except Exception as error:
        _handle_error(error)
    typer.echo(f"Corrupted {path}")
