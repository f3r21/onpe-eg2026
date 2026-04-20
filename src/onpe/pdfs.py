"""Descarga de PDFs binarios de actas escaneadas.

**Flujo**:
1. `fetch_signed_url(client, archivo_id)` llama a ONPE backend
   `GET /presentacion-backend/actas/file?id={archivo_id}` (descubierto via
   DevTools 2026-04-19). Devuelve `{"success": true, "data": "<signed_s3_url>"}`.
2. La signed URL expira en 660s (X-Amz-Expires); descargar inmediatamente.
3. Destino: local filesystem (sharded `data/pdfs/XX/YY/{archivo_id}.pdf`) o GCS
   (`gs://bucket/archivo_id.pdf`) en streaming via `upload_to_gcs_from_url`.

**Target recomendado: GCS**. 1 TB de PDFs desborda el disco local (175 GB libres).
Streaming directo S3→GCS evita pasar por disco local entirely.

**Reintentos y atomicidad**: descarga a `.tmp` + rename para local; GCS upload
usa `if_generation_match=0` para no sobrescribir.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from onpe.client import OnpeClient, OnpeError

if TYPE_CHECKING:
    from google.cloud.storage import Bucket

log = logging.getLogger(__name__)

# Endpoint que devuelve {success, message, data: signed_url} para un archivoId.
# La signed URL expira a los 660s.
_SIGNED_URL_ENDPOINT: str = "/actas/file"

# ONPE emite archivoIds como hex de 24 chars (p.ej. ObjectId-like). Validar
# antes de usar en paths/URLs previene path traversal si el API devolviera
# un valor malformado o malicioso.
_ARCHIVO_ID_RE = re.compile(r"^[0-9a-fA-F]{24}$")


def _validate_archivo_id(archivo_id: str) -> None:
    """Rechaza archivoIds que no cumplen el formato hex-24.

    Defensa en profundidad: ONPE es fuente confiable pero el formato protege
    contra path traversal (archivoId="../etc/passwd") si alguna vez cambia.
    """
    if not _ARCHIVO_ID_RE.match(archivo_id):
        raise OnpeError(f"archivoId invalido: {archivo_id!r} (esperado hex de 24 chars)")


# Path base de los PDFs descargados. Sharding por primeros 4 caracteres del
# archivoId para evitar 900k archivos en un solo directorio (ext4 soporta
# muchos pero los listados se vuelven lentos; macOS HFS+ aún más).
#
# Ej: archivo 69dce34ad7b6147f63e6fe04 → data/pdfs/69/dc/69dce34ad7b6147f63e6fe04.pdf
PDFS_SHARD_DEPTH = 2
PDFS_SHARD_WIDTH = 2  # 2 chars por nivel → 256 × 256 = 65,536 dirs máximo


def pdf_local_path(pdfs_dir: Path, archivo_id: str) -> Path:
    """Devuelve el path local sharded para un archivoId."""
    if len(archivo_id) < PDFS_SHARD_DEPTH * PDFS_SHARD_WIDTH:
        # Fallback: sin sharding si el id es muy corto (no debería pasar).
        return pdfs_dir / f"{archivo_id}.pdf"
    parts: list[str] = []
    for i in range(PDFS_SHARD_DEPTH):
        start = i * PDFS_SHARD_WIDTH
        parts.append(archivo_id[start : start + PDFS_SHARD_WIDTH])
    parts.append(f"{archivo_id}.pdf")
    return pdfs_dir.joinpath(*parts)


def check_disk_space(dst_dir: Path, required_gb: float = 60.0) -> None:
    """Precheck: aborta si no hay al menos `required_gb` libres en el volumen."""
    dst_dir.mkdir(parents=True, exist_ok=True)
    stat = shutil.disk_usage(dst_dir)
    free_gb = stat.free / (1024**3)
    if free_gb < required_gb:
        raise RuntimeError(
            f"espacio insuficiente en {dst_dir}: {free_gb:.1f} GB libres, "
            f"se requieren ≥{required_gb:.0f} GB (~45 GB para 900k PDFs + margen)"
        )
    log.info("disk space: %.1f GB libres en %s (OK)", free_gb, dst_dir)


async def fetch_signed_url(client: OnpeClient, archivo_id: str) -> str:
    """Obtiene la URL firmada S3 para un archivoId desde el backend ONPE.

    Endpoint: GET /presentacion-backend/actas/file?id={archivo_id}
    Response: {"success": true, "message": "", "data": "<signed_s3_url>"}

    La URL devuelta tiene TTL ~660s (X-Amz-Expires). Descargar inmediatamente.
    """
    _validate_archivo_id(archivo_id)
    data = await client.get_json(_SIGNED_URL_ENDPOINT, params={"id": archivo_id})
    if not isinstance(data, dict):
        raise OnpeError(f"respuesta no-dict para archivoId={archivo_id}: {type(data).__name__}")
    if not data.get("success"):
        raise OnpeError(f"success=false para archivoId={archivo_id}: {data.get('message') or data}")
    url = data.get("data")
    if not isinstance(url, str) or not url.startswith("https://"):
        raise OnpeError(f"URL firmada invalida para archivoId={archivo_id}: {url!r}")
    return url


@dataclass(frozen=True)
class DownloadResult:
    archivo_id: str
    path: Path
    bytes_written: int
    status: Literal["downloaded", "skipped_existing", "failed"]
    error: str | None = None

    def __post_init__(self) -> None:
        # Invariant de dominio: status=failed siempre lleva error; otros no.
        if self.status == "failed" and self.error is None:
            raise ValueError("DownloadResult con status='failed' debe tener error != None")
        if self.status != "failed" and self.error is not None:
            raise ValueError(f"DownloadResult con status={self.status!r} no debe tener error")


@retry(
    reraise=True,
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.TransportError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
)
async def _download_to_path(session: httpx.AsyncClient, signed_url: str, dst: Path) -> int:
    """Descarga stream a dst. Atómico via tmp + rename.

    Raises:
        httpx.HTTPStatusError si 4xx/5xx tras retries.
    """
    tmp = dst.with_suffix(".pdf.tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    bytes_written = 0
    async with session.stream("GET", signed_url) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            async for chunk in r.aiter_bytes(chunk_size=65536):
                f.write(chunk)
                bytes_written += len(chunk)
    if bytes_written < 1024:  # PDFs deberían ser ≥1 KB; <1KB sugiere respuesta corrupta
        tmp.unlink(missing_ok=True)
        raise OnpeError(f"PDF sospechosamente pequeño: {bytes_written} bytes en {signed_url}")
    tmp.replace(dst)
    return bytes_written


async def download_pdf(
    onpe: OnpeClient,
    s3_session: httpx.AsyncClient,
    archivo_id: str,
    dst_base: Path,
    skip_existing: bool = True,
) -> DownloadResult:
    """Pipeline completo para un PDF: signed URL -> stream download -> atomic write."""
    _validate_archivo_id(archivo_id)
    dst = pdf_local_path(dst_base, archivo_id)
    if skip_existing and dst.exists() and dst.stat().st_size > 0:
        return DownloadResult(archivo_id, dst, dst.stat().st_size, "skipped_existing")
    try:
        url = await fetch_signed_url(onpe, archivo_id)
        n = await _download_to_path(s3_session, url, dst)
        return DownloadResult(archivo_id, dst, n, "downloaded")
    except asyncio.CancelledError:
        # Propagar cancellation para que asyncio.gather/wait_for la detecten.
        # Capturarla aqui la convertiria silenciosamente en un DownloadResult failed.
        raise
    except Exception as e:
        return DownloadResult(archivo_id, dst, 0, "failed", error=str(e))


def compute_checksum(path: Path) -> str:
    """SHA256 del PDF — útil para integridad en checkpoint futuro."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


# --- GCS streaming upload (evita disco local para 1 TB de PDFs) ---


def gcs_object_name(archivo_id: str, elecciones: str = "eg2026") -> str:
    """Path dentro del bucket GCS. Sharding por primeros 4 chars del archivoId
    para mantener prefijos útiles para consumers (igual que local layout).
    """
    if len(archivo_id) < 4:
        return f"{elecciones}/{archivo_id}.pdf"
    return f"{elecciones}/{archivo_id[0:2]}/{archivo_id[2:4]}/{archivo_id}.pdf"


async def upload_to_gcs_from_url(
    http_session: httpx.AsyncClient,
    signed_url: str,
    bucket: Bucket,
    object_name: str,
    content_type: str = "application/pdf",
) -> int:
    """Stream HTTP response body → GCS blob sin pasar por disco.

    El upload a GCS usa el cliente oficial google-cloud-storage que es
    sincrónico; se ejecuta en thread pool via `asyncio.to_thread` para no
    bloquear el event loop async. Sin esto, con concurrency=20 el sync upload
    convierte el paralelismo en serial y la tasa efectiva cae a <1 PDF/s.

    Args:
        http_session: httpx.AsyncClient — descarga la signed URL.
        signed_url: URL firmada S3 (válida 660s).
        bucket: google.cloud.storage.Bucket ya obtenido.
        object_name: path relativo dentro del bucket.
        content_type: tipo MIME para metadata GCS.

    Returns:
        bytes uploaded.
    """
    import io

    buf = io.BytesIO()
    async with http_session.stream("GET", signed_url) as r:
        r.raise_for_status()
        async for chunk in r.aiter_bytes(chunk_size=65536):
            buf.write(chunk)
    n = buf.tell()
    if n < 1024:
        raise OnpeError(f"PDF sospechosamente pequeño: {n} bytes en {signed_url[:80]}")
    buf.seek(0)
    blob = bucket.blob(object_name)

    def _sync_upload() -> None:
        # if_generation_match=0 → falla si ya existe (idempotencia/no sobrescribir).
        blob.upload_from_file(buf, content_type=content_type, if_generation_match=0)

    await asyncio.to_thread(_sync_upload)
    return n


async def _blob_exists_async(bucket: Bucket, object_name: str) -> tuple[bool, int]:
    """Check blob existence en thread pool (sync call blocks event loop)."""

    def _check() -> tuple[bool, int]:
        blob = bucket.blob(object_name)
        if blob.exists():
            blob.reload()
            return True, blob.size or 0
        return False, 0

    return await asyncio.to_thread(_check)


async def download_pdf_to_gcs(
    onpe: OnpeClient,
    s3_session: httpx.AsyncClient,
    bucket: Bucket,
    archivo_id: str,
    *,
    skip_existing: bool = True,
) -> DownloadResult:
    """Pipeline completo GCS: fetch signed URL -> stream upload to GCS."""
    _validate_archivo_id(archivo_id)
    object_name = gcs_object_name(archivo_id)
    dst_path = Path(f"gs://{bucket.name}/{object_name}")

    if skip_existing:
        exists, size = await _blob_exists_async(bucket, object_name)
        if exists:
            return DownloadResult(
                archivo_id=archivo_id,
                path=dst_path,
                bytes_written=size,
                status="skipped_existing",
            )

    try:
        url = await fetch_signed_url(onpe, archivo_id)
        n = await upload_to_gcs_from_url(s3_session, url, bucket, object_name)
        return DownloadResult(archivo_id, dst_path, n, "downloaded")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        # PreconditionFailed (GCS if_generation_match=0 con blob existente) indica
        # race entre _blob_exists_async y el upload. Re-chequeamos y reportamos como
        # skipped para no loopear infinitamente intentando re-subir.
        name = type(e).__name__
        if name == "PreconditionFailed" or "precondition" in str(e).lower():
            exists, size = await _blob_exists_async(bucket, object_name)
            if exists:
                log.info(
                    "race detectada en GCS (blob %s ya existe, tratado como skipped)",
                    object_name,
                )
                return DownloadResult(archivo_id, dst_path, size, "skipped_existing")
        return DownloadResult(archivo_id, dst_path, 0, "failed", error=str(e))
