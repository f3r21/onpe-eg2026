"""Enriquece curated/actas_cabecera con dimensiones materializadas.

Thin wrapper del modulo `onpe.enrich`. La logica vive en `src/onpe/enrich.py`
para poder ser consumida tambien por `scripts/build_curated.py` sin sys.path
manipulation.

Uso:
    uv run python scripts/enrich_curated.py              # in-place
    uv run python scripts/enrich_curated.py --dry-run    # reporta cambios

Integracion: build_curated.py invoca `enrich_cabecera` directamente cuando
se pasa --enrich (default en produccion, omitido en tests via flag).
"""

from __future__ import annotations

import argparse
import logging

import polars as pl

from onpe.enrich import enrich_cabecera, validate_integrity
from onpe.storage import DATA_DIR

log = logging.getLogger("enrich_curated")

CURATED_CAB = DATA_DIR / "curated" / "actas_cabecera.parquet"
DIM_MESAS = DATA_DIR / "dim" / "mesas.parquet"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="no reescribe, solo reporta")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not CURATED_CAB.exists():
        raise SystemExit(f"falta {CURATED_CAB}. Correr build_curated.py primero.")
    if not DIM_MESAS.exists():
        raise SystemExit(f"falta {DIM_MESAS}. Correr crawl_mesas.py primero.")

    df_cab = pl.read_parquet(CURATED_CAB)
    df_mesas = pl.read_parquet(DIM_MESAS)
    log.info("leido curated: %d actas; mesas: %d", df_cab.height, df_mesas.height)

    enriched = enrich_cabecera(df_cab, df_mesas)
    validate_integrity(enriched)

    if args.dry_run:
        log.info("dry-run: no se escribe")
        return

    # Escritura atomica: tmp + rename.
    tmp = CURATED_CAB.with_suffix(".parquet.tmp")
    enriched.write_parquet(tmp, compression="zstd")
    tmp.replace(CURATED_CAB)
    log.info("escrito: %s (%.1f MB)", CURATED_CAB, CURATED_CAB.stat().st_size / 1e6)


if __name__ == "__main__":
    main()
