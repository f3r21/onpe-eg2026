"""Migración one-shot: chunks Parquet con columnas dtype=null -> cast a String.

Los chunks escritos antes del fix de `_coerce_null_columns` (task #28) pueden
tener columnas persistidas con Parquet `null` logical type (p.ej. `idMesa` en
el run nocturno del 2026-04-17). El reader de `build_curated.py` ya lo absorbe
via `how=diagonal_relaxed`, pero una migración deja el facts layer homogéneo y
evita el overhead por-archivo en reads futuros.

Idempotente: si un archivo ya tiene todas las columnas con dtype concreto, lo
omite. Atómico por archivo (escribe a .tmp, rename).

Uso:
    uv run python scripts/migrate_null_dtypes.py --dry-run
    uv run python scripts/migrate_null_dtypes.py
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from onpe.storage import DATA_DIR

FACT_DIRS: tuple[Path, ...] = (
    DATA_DIR / "facts" / "actas_cabecera",
    DATA_DIR / "facts" / "actas_votos",
)

log = logging.getLogger("migrate_null_dtypes")


def _null_columns(path: Path) -> list[str]:
    """Columnas del archivo con Parquet null logical type."""
    schema = pq.read_schema(path)
    return [f.name for f in schema if str(f.type) == "null"]


def _migrate_file(path: Path, null_cols: list[str]) -> None:
    """Reescribe castear columnas null -> String. Atómico via tmp + rename."""
    df = pl.read_parquet(path)
    df = df.with_columns([pl.col(c).cast(pl.String) for c in null_cols])
    tmp = path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    tmp.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="no reescribe")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    total = 0
    needs = 0
    for base in FACT_DIRS:
        if not base.exists():
            log.warning("no existe %s, skip", base)
            continue
        for path in sorted(base.glob("**/*.parquet")):
            total += 1
            null_cols = _null_columns(path)
            if not null_cols:
                continue
            needs += 1
            log.info(
                "%s%s: cols null=%s",
                "[dry-run] " if args.dry_run else "",
                path.relative_to(DATA_DIR),
                null_cols,
            )
            if not args.dry_run:
                _migrate_file(path, null_cols)

    log.info(
        "archivos revisados: %d | con cols null: %d | %s",
        total,
        needs,
        "dry-run (no se escribió)" if args.dry_run else "migrados",
    )


if __name__ == "__main__":
    main()
