"""Descarga masiva de PDFs de actas escaneadas (~725k archivos, ~1 TB).

Precondiciones:
- data/curated/actas_cabecera.parquet (estado por acta)
- data/curated/actas_archivos.parquet (metadata con archivoId)
- `gcloud auth application-default login` si se usa --gcs-bucket

SLI del 100%: ≥95% de actas C con archivoId tienen PDF válido en disk/GCS.
Denominador = actas en estado C con archivoId poblado (excluye P y las 240
"mesa no instalada").

Modos:
    --gcs-bucket gs://<tu-bucket>   # streaming a GCS (recomendado para 1 TB)
    (default)                                    # escribe a data/pdfs/XX/YY/... local

Uso:
    # Smoke a GCS (5 PDFs)
    uv run python scripts/download_pdfs.py --gcs-bucket gs://<tu-bucket> --limit 5

    # Full a GCS
    uv run python scripts/download_pdfs.py --gcs-bucket gs://<tu-bucket> \\
        --rps 5 --concurrency 10

    # Reanudar (idempotente via skip_existing)
    uv run python scripts/download_pdfs.py --gcs-bucket gs://<tu-bucket>

    # Subset por tipo (tipos 3+4+5 solo = ~51 GB)
    uv run python scripts/download_pdfs.py --gcs-bucket gs://... --tipos 3,4,5

    # Descarga distribuida entre varios hosts (ver docs/DISTRIBUTED_DOWNLOAD.md)
    # Mac:       --shard 0/3
    # Win PC 1:  --shard 1/3
    # Win PC 2:  --shard 2/3
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import polars as pl

from onpe.client import ClientConfig, OnpeClient
from onpe.pdfs import (
    DownloadResult,
    check_disk_space,
    download_pdf,
    download_pdf_to_gcs,
)
from onpe.storage import DATA_DIR, ms_to_lima_iso, utc_now_ms

log = logging.getLogger(__name__)

PDFS_DIR = DATA_DIR / "pdfs"
STATE_DIR = DATA_DIR / "state"
BATCH = 100  # concurrent downloads in flight


@dataclass
class PdfCheckpoint:
    run_ts_ms: int
    started_lima_iso: str
    total_expected: int
    completed: set[str] = field(default_factory=set)
    failed: list[dict[str, Any]] = field(default_factory=list)

    def path(self) -> Path:
        return STATE_DIR / f"pdfs_download_{self.run_ts_ms}.json"

    def save(self) -> None:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = self.path().with_suffix(".json.tmp")
        payload = {
            "run_ts_ms": self.run_ts_ms,
            "started_lima_iso": self.started_lima_iso,
            "total_expected": self.total_expected,
            "completed": sorted(self.completed),
            "failed": self.failed,
        }
        tmp.write_text(json.dumps(payload))
        tmp.replace(self.path())

    @classmethod
    def load(cls, run_ts_ms: int) -> PdfCheckpoint:
        data = json.loads((STATE_DIR / f"pdfs_download_{run_ts_ms}.json").read_text())
        return cls(
            run_ts_ms=data["run_ts_ms"],
            started_lima_iso=data["started_lima_iso"],
            total_expected=data["total_expected"],
            completed=set(data.get("completed", [])),
            failed=data.get("failed", []),
        )


def load_target_archivo_ids(
    limit: int | None = None, tipos: tuple[int, ...] | None = None
) -> list[str]:
    """Lista de archivoId de PDFs a descargar.

    Filtra a:
    - actas en estado C (terminal, no cambiarán más)
    - actas_archivos con archivoId no nulo + tipo en `tipos` si se especifica
    - skip duplicados (DataFrame.unique)
    """
    cab_path = DATA_DIR / "curated" / "actas_cabecera.parquet"
    arch_path = DATA_DIR / "curated" / "actas_archivos.parquet"
    if not cab_path.exists():
        raise SystemExit(f"falta {cab_path}. Correr pipeline antes.")
    if not arch_path.exists():
        raise SystemExit(
            f"falta {arch_path}. La tabla actas_archivos se pobla con re-snapshot full "
            "(task A4). Esperar a que termine antes de descargar PDFs."
        )

    c_actas = pl.scan_parquet(cab_path).filter(pl.col("codigoEstadoActa") == "C").select("idActa")
    lf = pl.scan_parquet(arch_path)
    if tipos:
        lf = lf.filter(pl.col("tipo").is_in(list(tipos)))
    archivo_ids = (
        lf.join(c_actas, on="idActa", how="inner")
        .select("archivoId")
        .unique()
        .drop_nulls()
        .collect()
        .to_series()
        .to_list()
    )
    if limit is not None:
        archivo_ids = archivo_ids[:limit]
    return archivo_ids


def _shard_filter(archivo_ids: list[str], shard_m: int, shard_n: int) -> list[str]:
    """Filtra a los archivoIds que corresponden a la shard M de N.

    Usa md5 (estable cross-platform y cross-Python) en lugar de hash() built-in
    que esta randomizado por proceso via PYTHONHASHSEED. Deterministico: el
    mismo archivoId siempre cae en la misma shard, permitiendo resumir desde
    cualquier worker sin coordinacion externa.
    """
    if not (0 <= shard_m < shard_n):
        raise SystemExit(f"shard invalido: {shard_m}/{shard_n} (debe ser 0 <= M < N)")
    return [
        aid
        for aid in archivo_ids
        if int(hashlib.md5(aid.encode("utf-8"), usedforsecurity=False).hexdigest(), 16) % shard_n
        == shard_m
    ]


async def _run(
    archivo_ids: list[str],
    rps: float,
    concurrency: int,
    resume_run_ts_ms: int | None,
    gcs_bucket_name: str | None,
) -> PdfCheckpoint:
    # Checkpoint: reanudar o arrancar fresh.
    if resume_run_ts_ms is not None:
        ck = PdfCheckpoint.load(resume_run_ts_ms)
        log.info(
            "reanudando run_ts_ms=%d, completados=%d/%d",
            ck.run_ts_ms,
            len(ck.completed),
            ck.total_expected,
        )
    else:
        now_ms = utc_now_ms()
        ck = PdfCheckpoint(
            run_ts_ms=now_ms,
            started_lima_iso=ms_to_lima_iso(now_ms),
            total_expected=len(archivo_ids),
        )
        ck.save()
        log.info("nuevo run_ts_ms=%d, target=%d PDFs", ck.run_ts_ms, ck.total_expected)

    pending = [aid for aid in archivo_ids if aid not in ck.completed]
    log.info("pendientes tras filtro checkpoint: %d", len(pending))

    # GCS bucket (lazy import: solo si se usa)
    gcs_bucket = None
    if gcs_bucket_name:
        from google.cloud import storage

        gcs_client = storage.Client()
        # Strip gs:// prefix si viene
        name = gcs_bucket_name.removeprefix("gs://").rstrip("/")
        gcs_bucket = gcs_client.bucket(name)
        log.info("destino: GCS bucket gs://%s", name)
    else:
        log.info("destino: local %s", PDFS_DIR)

    # Cliente ONPE para signed URLs. Cliente S3 separado.
    cfg_onpe = ClientConfig(max_concurrent=concurrency, rate_per_second=rps)
    s3_limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)

    async with (
        OnpeClient(cfg_onpe) as onpe,
        httpx.AsyncClient(timeout=60.0, limits=s3_limits) as s3,
    ):
        sem = asyncio.Semaphore(concurrency)

        async def one(aid: str) -> DownloadResult:
            async with sem:
                if gcs_bucket is not None:
                    return await download_pdf_to_gcs(onpe, s3, gcs_bucket, aid)
                return await download_pdf(onpe, s3, aid, PDFS_DIR)

        stats = {"downloaded": 0, "skipped_existing": 0, "failed": 0}
        t0 = time.perf_counter()
        for i in range(0, len(pending), BATCH):
            batch = pending[i : i + BATCH]
            results = await asyncio.gather(*(one(aid) for aid in batch))
            for r in results:
                stats[r.status] = stats.get(r.status, 0) + 1
                if r.status == "failed":
                    ck.failed.append({"archivo_id": r.archivo_id, "error": r.error})
                else:
                    ck.completed.add(r.archivo_id)
            ck.save()
            elapsed = time.perf_counter() - t0
            rate = (i + len(batch)) / max(elapsed, 1e-6)
            log.info(
                "progreso %d/%d (ok=%d skip=%d fail=%d) @ %.1f/s",
                len(ck.completed),
                ck.total_expected,
                stats["downloaded"],
                stats["skipped_existing"],
                stats["failed"],
                rate,
            )

    return ck


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="corta en N PDFs (smoke)")
    parser.add_argument("--resume", type=int, default=None, help="run_ts_ms a reanudar")
    parser.add_argument("--rps", type=float, default=3.0, help="rate-limit para signed URLs")
    parser.add_argument("--concurrency", type=int, default=5, help="descargas concurrentes")
    parser.add_argument(
        "--gcs-bucket",
        type=str,
        default=None,
        help="GCS bucket (gs://name o solo name). Streaming directo S3→GCS.",
    )
    parser.add_argument(
        "--tipos",
        type=str,
        default=None,
        help="comma-separated tipos a filtrar (1=escrutinio, 2=inst+sufragio combinada, 3=inst, 4=sufragio, 5=resolución)",
    )
    parser.add_argument(
        "--shard",
        type=str,
        default=None,
        metavar="M/N",
        help=(
            "particion distribuida: worker M de N. "
            "Filtra archivoIds via md5(aid) %% N == M. "
            "Ej: --shard 0/3 en Mac, --shard 1/3 en WinPC1, --shard 2/3 en WinPC2."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Disk precheck solo si destino es local.
    if args.gcs_bucket is None:
        check_disk_space(PDFS_DIR, required_gb=60.0)

    tipos = tuple(int(t) for t in args.tipos.split(",")) if args.tipos else None
    ids = load_target_archivo_ids(limit=args.limit, tipos=tipos)
    log.info(
        "universo de PDFs: %d archivoIds (tipos=%s)",
        len(ids),
        tipos or "todos",
    )

    if args.shard:
        try:
            shard_m, shard_n = (int(x) for x in args.shard.split("/", 1))
        except ValueError as e:
            raise SystemExit(f"--shard debe ser M/N (e.g. 1/3), se recibio {args.shard!r}") from e
        total_pre = len(ids)
        ids = _shard_filter(ids, shard_m, shard_n)
        log.info(
            "shard %d/%d aplicada: %d archivoIds (de %d universo, ~%.1f%% esperado)",
            shard_m,
            shard_n,
            len(ids),
            total_pre,
            100 / shard_n,
        )

    ck = asyncio.run(_run(ids, args.rps, args.concurrency, args.resume, args.gcs_bucket))
    log.info(
        "run_ts_ms=%d done: completados=%d/%d (failed=%d)",
        ck.run_ts_ms,
        len(ck.completed),
        ck.total_expected,
        len(ck.failed),
    )


if __name__ == "__main__":
    main()
