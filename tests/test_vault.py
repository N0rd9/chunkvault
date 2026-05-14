from pathlib import Path

import pytest

from chunkvault import ChunkUnavailableError, Vault, VaultFileNotFoundError


def make_vault(tmp_path: Path) -> Vault:
    vault = Vault(
        tmp_path / "vault",
        node_count=3,
        replication_factor=2,
        chunk_size=8,
    )
    vault.init()
    return vault


def test_put_restore_round_trip(tmp_path: Path) -> None:
    source = tmp_path / "paper.txt"
    source.write_text("distributed systems need boringly good checksums", encoding="utf-8")
    vault = make_vault(tmp_path)

    record = vault.put(source)
    restored = tmp_path / "restored.txt"
    vault.restore(record.id, restored)

    assert restored.read_bytes() == source.read_bytes()
    assert record.chunk_count > 1
    assert vault.verify(record.id).healthy


def test_restore_survives_one_offline_node(tmp_path: Path) -> None:
    source = tmp_path / "notes.bin"
    source.write_bytes(b"abcdefghijklmno")
    vault = make_vault(tmp_path)
    record = vault.put(source)

    vault.set_node_online("node-1", False)
    report = vault.verify(record.id)
    restored = tmp_path / "notes-restored.bin"
    vault.restore(record.id, restored)

    assert restored.read_bytes() == source.read_bytes()
    assert not report.healthy
    assert report.recoverable
    assert any(issue.kind == "node_offline" for issue in report.issues)


def test_corrupt_replica_is_detected_but_recoverable(tmp_path: Path) -> None:
    source = tmp_path / "image.raw"
    source.write_bytes(b"1234567890abcdef")
    vault = make_vault(tmp_path)
    record = vault.put(source)

    vault.corrupt_replica(record.id, 0, "node-1")
    report = vault.verify(record.id)
    restored = tmp_path / "image-restored.raw"
    vault.restore(record.id, restored)

    assert not report.healthy
    assert report.recoverable
    assert any(issue.kind == "corrupt_replica" for issue in report.issues)
    assert restored.read_bytes() == source.read_bytes()


def test_restore_fails_when_all_replicas_for_a_chunk_are_unavailable(tmp_path: Path) -> None:
    source = tmp_path / "unlucky.txt"
    source.write_text("node failure simulation", encoding="utf-8")
    vault = make_vault(tmp_path)
    record = vault.put(source)

    vault.set_node_online("node-1", False)
    vault.set_node_online("node-2", False)

    assert not vault.verify(record.id).recoverable
    with pytest.raises(ChunkUnavailableError):
        vault.restore(record.id, tmp_path / "should-not-exist.txt")


def test_delete_removes_file_metadata(tmp_path: Path) -> None:
    source = tmp_path / "delete-me.txt"
    source.write_text("short lived", encoding="utf-8")
    vault = make_vault(tmp_path)
    record = vault.put(source)

    vault.delete(record.id)

    assert vault.list_files() == []
    with pytest.raises(VaultFileNotFoundError):
        vault.get_file(record.id)
