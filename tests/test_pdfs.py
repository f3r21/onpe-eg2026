"""Tests de onpe.pdfs — partes puras (sharding, checksum, disk precheck)."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from onpe.pdfs import (
    PDFS_SHARD_DEPTH,
    check_disk_space,
    compute_checksum,
    pdf_local_path,
)


def test_pdf_local_path_sharding():
    """archivoId '69dce34ad7b6147f63e6fe04' → pdfs/69/dc/69dce34a....pdf"""
    base = Path("/data/pdfs")
    result = pdf_local_path(base, "69dce34ad7b6147f63e6fe04")
    expected = base / "69" / "dc" / "69dce34ad7b6147f63e6fe04.pdf"
    assert result == expected


def test_pdf_local_path_diferentes_ids_shards_distintos():
    """IDs distintos terminan en shards distintos."""
    base = Path("/data/pdfs")
    a = pdf_local_path(base, "abcdef1234567890")
    b = pdf_local_path(base, "123456abcdef7890")
    assert a.parent != b.parent


def test_pdf_local_path_id_muy_corto_fallback():
    """ID más corto que el sharding → va al root sin shards."""
    base = Path("/data/pdfs")
    result = pdf_local_path(base, "abc")  # 3 chars < 2*2=4
    assert result == base / "abc.pdf"


def test_pdf_local_path_shard_depth_consistente():
    """El path tiene exactamente PDFS_SHARD_DEPTH niveles de sharding."""
    base = Path("/data/pdfs")
    result = pdf_local_path(base, "1234567890abcdef")
    # base / 2 niveles / filename = 4 partes
    relative = result.relative_to(base)
    assert len(relative.parts) == PDFS_SHARD_DEPTH + 1  # shards + filename


def test_check_disk_space_ok(tmp_path: Path):
    """Si hay espacio, no raise."""
    # Pedir 0 GB siempre pasa
    check_disk_space(tmp_path, required_gb=0.0)


def test_check_disk_space_insuficiente(tmp_path: Path):
    """Si se piden TBs imposibles → RuntimeError."""
    with pytest.raises(RuntimeError, match="espacio insuficiente"):
        check_disk_space(tmp_path, required_gb=999999.0)


def test_compute_checksum_matches_sha256(tmp_path: Path):
    """compute_checksum devuelve SHA256 hex del contenido."""
    path = tmp_path / "test.bin"
    content = b"hello onpe eg2026"
    path.write_bytes(content)

    expected = hashlib.sha256(content).hexdigest()
    assert compute_checksum(path) == expected


def test_compute_checksum_archivo_grande_chunked(tmp_path: Path):
    """compute_checksum lee en chunks — funciona con archivos > chunk_size."""
    path = tmp_path / "big.bin"
    # 200 KB para forzar múltiples reads (chunk=64KB)
    content = b"A" * (200 * 1024)
    path.write_bytes(content)

    expected = hashlib.sha256(content).hexdigest()
    assert compute_checksum(path) == expected


def test_fetch_signed_url_parse_respuesta_ok():
    """fetch_signed_url extrae `data` cuando success=true y data es URL https."""
    import asyncio
    from unittest.mock import AsyncMock

    from onpe.pdfs import fetch_signed_url

    mock = AsyncMock()
    mock.get_json = AsyncMock(
        return_value={
            "success": True,
            "message": "",
            "data": "https://bucket.s3.amazonaws.com/x.pdf?X-Amz=1",
        }
    )

    async def run():
        url = await fetch_signed_url(mock, "abc123")
        assert url.startswith("https://")
        mock.get_json.assert_awaited_once_with("/actas/file", params={"id": "abc123"})

    asyncio.run(run())


def test_fetch_signed_url_success_false_raises():
    """success=false → OnpeError."""
    import asyncio
    from unittest.mock import AsyncMock

    from onpe.client import OnpeError
    from onpe.pdfs import fetch_signed_url

    mock = AsyncMock()
    mock.get_json = AsyncMock(
        return_value={"success": False, "message": "archivo no encontrado", "data": None}
    )

    async def run():
        with pytest.raises(OnpeError, match="success=false"):
            await fetch_signed_url(mock, "abc123")

    asyncio.run(run())


def test_fetch_signed_url_data_invalido_raises():
    """data sin prefijo https → OnpeError."""
    import asyncio
    from unittest.mock import AsyncMock

    from onpe.client import OnpeError
    from onpe.pdfs import fetch_signed_url

    mock = AsyncMock()
    mock.get_json = AsyncMock(return_value={"success": True, "data": "not-a-url"})

    async def run():
        with pytest.raises(OnpeError, match="URL firmada inválida"):
            await fetch_signed_url(mock, "abc123")

    asyncio.run(run())
