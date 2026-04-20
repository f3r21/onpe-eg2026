"""Ingesta del CSV oficial ONPE EG2026 desde datosabiertos.gob.pe.

Se ejecuta cuando `scripts/monitor_datosabiertos.py` detecta publicación
(flag file en `data/state/.eg2026_alert`). Descarga los recursos CSV,
normaliza al schema del curated ONPE, y persiste en
`data/curated/datosabiertos_eg2026.parquet` (schema paralelo, NO reemplaza
los actas scraped).

Shape esperado del CSV (basado en EG2021 reference en datosabiertos):
    CCODI_UBIGEO, ELECTORES_HABIL, TOT_CIUDADANOS_VOTARON, VOTOS_EMITIDOS,
    VOTOS_VALIDOS, VOTOS_BLANCO, VOTOS_NULO, VOTOS_IMPUGNADO, CANDIDATO/PARTIDO,
    VOTOS_PARTIDO, MESA_ID, DESC_MESA...

El schema exacto se confirma al momento de la publicación. Este script debe
ajustarse si la estructura cambia vs EG2021.

Uso:
    # Cuando hay flag de EG2026
    uv run python scripts/ingest_datosabiertos.py

    # Manual con URL específica
    uv run python scripts/ingest_datosabiertos.py --dataset-url https://www.datosabiertos.gob.pe/dataset/...
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import zipfile
from pathlib import Path

import httpx
import polars as pl

from onpe.datosabiertos import get_dataset_resources
from onpe.storage import DATA_DIR

log = logging.getLogger("ingest_datosabiertos")

STATE_DIR = DATA_DIR / "state"
OFICIAL_DIR = DATA_DIR / "oficial"
CURATED_DIR = DATA_DIR / "curated"
EG2026_ALERT_FILE = STATE_DIR / ".eg2026_alert"


def download_resource(url: str, dst: Path) -> Path:
    """Descarga un recurso (CSV/XLSX/ZIP) a disk."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    log.info("descargando %s → %s", url, dst)
    with httpx.stream("GET", url, timeout=300, follow_redirects=True) as r:
        r.raise_for_status()
        with dst.open("wb") as f:
            for chunk in r.iter_bytes(chunk_size=65536):
                f.write(chunk)
    size_mb = dst.stat().st_size / 1e6
    log.info("  %.1f MB", size_mb)
    return dst


def extract_zip(zip_path: Path, dst_dir: Path) -> list[Path]:
    """Unzip -> lista de archivos extraidos.

    Protege contra zip slip (CWE-22): rechaza miembros cuyo path resuelto
    escape de dst_dir. Necesario aunque datosabiertos.gob.pe sea una fuente
    oficial, porque es una dependencia externa fuera de nuestro control.
    """
    extracted: list[Path] = []
    dst_resolved = dst_dir.resolve()
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            if name.endswith("/"):
                continue  # skip dirs
            member_resolved = (dst_dir / name).resolve()
            try:
                member_resolved.relative_to(dst_resolved)
            except ValueError as e:
                raise ValueError(
                    f"zip slip detectado: {name!r} escaparia {dst_dir} en {zip_path}"
                ) from e
            z.extract(name, dst_dir)
            extracted.append(dst_dir / name)
    log.info("unzip %s: %d archivos", zip_path.name, len(extracted))
    return extracted


def normalize_to_parquet(csv_path: Path, dst_parquet: Path) -> int:
    """Convierte CSV → parquet preservando schema (sin forzar renames).

    Post-publicación, este step debe ajustarse para alinear columnas al
    schema del curated si hay compatibility útil.
    """
    # ONPE típicamente usa encoding latin-1 o utf-8 con BOM. Probar ambos.
    for enc in ("utf-8", "latin-1"):
        try:
            df = pl.read_csv(csv_path, encoding=enc, infer_schema_length=50000, ignore_errors=True)
            break
        except Exception as e:
            log.warning("read_csv con enc=%s falló: %s", enc, e)
    else:
        raise RuntimeError(f"no pude leer {csv_path} con ningún encoding")

    df.write_parquet(dst_parquet, compression="zstd")
    log.info(
        "%s → %s (%d filas, %d cols)",
        csv_path.name,
        dst_parquet.name,
        df.height,
        len(df.columns),
    )
    return df.height


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset-url",
        type=str,
        default=None,
        help="URL del dataset ONPE. Si no se pasa, lee el flag de monitor.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-ingesta aunque ya exista parquet.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    # Resolver URL del dataset
    dataset_url = args.dataset_url
    if not dataset_url:
        if not EG2026_ALERT_FILE.exists():
            log.error(
                "no hay flag de EG2026 en %s ni --dataset-url pasado. "
                "Correr scripts/monitor_datosabiertos.py primero.",
                EG2026_ALERT_FILE,
            )
            return 1
        alert = json.loads(EG2026_ALERT_FILE.read_text())
        dataset_url = alert.get("url")

    log.info("ingesta dataset: %s", dataset_url)
    OFICIAL_DIR.mkdir(parents=True, exist_ok=True)

    # Obtener recursos descargables
    resources = get_dataset_resources(dataset_url)
    log.info("recursos encontrados: %d", len(resources))
    for r in resources:
        log.info("  %s (%s) → %s", r["name"], r["format"], r["url"][:80])

    if not resources:
        log.error("no hay recursos descargables en %s — revisar parsing", dataset_url)
        return 1

    # Descargar + extraer + convertir
    parquets: list[Path] = []
    for r in resources:
        dst = OFICIAL_DIR / r["name"]
        download_resource(r["url"], dst)
        if r["format"] == "zip":
            extracted = extract_zip(dst, OFICIAL_DIR)
            for f in extracted:
                if f.suffix.lower() == ".csv":
                    pq = CURATED_DIR / f"datosabiertos_eg2026_{f.stem}.parquet"
                    normalize_to_parquet(f, pq)
                    parquets.append(pq)
        elif r["format"] == "csv":
            pq = CURATED_DIR / f"datosabiertos_eg2026_{dst.stem}.parquet"
            normalize_to_parquet(dst, pq)
            parquets.append(pq)
        elif r["format"] == "xlsx":
            log.warning("xlsx todavía no soportado: %s", dst)

    log.info("ingesta completa: %d parquets escritos", len(parquets))
    for p in parquets:
        log.info("  %s", p)
    log.info("")
    log.info("SIGUIENTE PASO: correr `uv run python scripts/dq_check.py --nivel 4`")
    return 0


if __name__ == "__main__":
    sys.exit(main())
