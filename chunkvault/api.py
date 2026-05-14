"""FastAPI application for ChunkVault."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse

from chunkvault.errors import ChunkUnavailableError, VaultError, VaultFileNotFoundError
from chunkvault.vault import Vault


def create_app(root: str | Path | None = None) -> FastAPI:
    vault = Vault(root or os.getenv("CHUNKVAULT_ROOT", ".chunkvault"))
    vault.init()
    app = FastAPI(
        title="ChunkVault",
        version="0.1.0",
        description="A fault-tolerant mini distributed file storage simulator.",
    )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/status")
    def get_status() -> dict[str, object]:
        return vault.status()

    @app.get("/files")
    def list_files() -> list[dict[str, object]]:
        return [record.as_dict() for record in vault.list_files()]

    @app.post("/files", status_code=status.HTTP_201_CREATED)
    async def upload_file(
        file: UploadFile = File(...),
        name: str | None = Query(None, description="Optional display name."),
    ) -> dict[str, object]:
        temp_path: Path | None = None
        try:
            suffix = Path(file.filename or "upload.bin").suffix
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
                shutil.copyfileobj(file.file, temp_file)
                temp_path = Path(temp_file.name)
            record = vault.put(temp_path, name=name or file.filename or temp_path.name)
            return record.as_dict()
        except VaultError as error:
            raise _http_error(error) from error
        finally:
            await file.close()
            if temp_path and temp_path.exists():
                temp_path.unlink()

    @app.get("/files/{file_id}")
    def get_file(file_id: str) -> dict[str, object]:
        try:
            return vault.get_file(file_id).as_dict()
        except VaultError as error:
            raise _http_error(error) from error

    @app.get("/files/{file_id}/download")
    def download_file(file_id: str) -> FileResponse:
        try:
            record = vault.get_file(file_id)
            downloads = Path(tempfile.gettempdir()) / "chunkvault-downloads"
            downloads.mkdir(parents=True, exist_ok=True)
            output = downloads / f"{record.id}-{_safe_filename(record.name)}"
            vault.restore(file_id, output)
            return FileResponse(output, filename=record.name)
        except VaultError as error:
            raise _http_error(error) from error

    @app.get("/files/{file_id}/verify")
    def verify_file(file_id: str) -> dict[str, object]:
        try:
            return vault.verify(file_id).as_dict()
        except VaultError as error:
            raise _http_error(error) from error

    @app.delete("/files/{file_id}", status_code=status.HTTP_204_NO_CONTENT)
    def delete_file(file_id: str) -> None:
        try:
            vault.delete(file_id)
        except VaultError as error:
            raise _http_error(error) from error

    @app.patch("/nodes/{node_id}")
    def set_node_state(
        node_id: str,
        online: bool = Query(..., description="True for online, false for offline."),
    ) -> dict[str, object]:
        try:
            vault.set_node_online(node_id, online)
            return vault.status()
        except VaultError as error:
            raise _http_error(error) from error

    return app


def _http_error(error: VaultError) -> HTTPException:
    if isinstance(error, VaultFileNotFoundError):
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error))
    if isinstance(error, ChunkUnavailableError):
        return HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error))
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error))


def _safe_filename(name: str) -> str:
    safe = "".join(character if character.isalnum() or character in "._-" else "_" for character in name)
    return safe or "download.bin"


app = create_app()
