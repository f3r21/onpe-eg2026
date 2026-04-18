"""Consolida los snapshots crudos de actas en una capa curada dedupada.

Lee todos los chunks Hive-particionados de `data/facts/actas_cabecera` y
`data/facts/actas_votos`, se queda con el run_ts_ms más reciente por idActa
(para cabecera; los votos heredan ese filtro por anti-join) y escribe un único
Parquet por tabla en `data/curated/`.

Uso:
    uv run python scripts/build_curated.py
    uv run python scripts/build_curated.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import polars as pl

from onpe.storage import DATA_DIR

CURATED_DIR = DATA_DIR / "curated"
FACTS_CAB = DATA_DIR / "facts" / "actas_cabecera"
FACTS_VOT = DATA_DIR / "facts" / "actas_votos"

logger = logging.getLogger("curated")


def _latest_run_per_idacta(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Para cada idActa, el run_ts_ms más reciente. Key semi-join eficiente."""
    return lf.group_by("idActa").agg(pl.col("run_ts_ms").max())


def build_cabecera(dry_run: bool) -> tuple[pl.DataFrame, pl.DataFrame]:
    lf = pl.scan_parquet(
        str(FACTS_CAB / "**/*.parquet"),
        hive_partitioning=True,
    )
    raw = lf.select(pl.len()).collect().item()

    latest = _latest_run_per_idacta(lf)
    df = (
        lf.join(latest, on=["idActa", "run_ts_ms"], how="semi")
        .drop("snapshot_date", "run_ts_ms")
        .collect()
    )
    logger.info("cabecera: %d filas crudas -> %d filas curadas", raw, df.height)

    if not dry_run:
        CURATED_DIR.mkdir(parents=True, exist_ok=True)
        out = CURATED_DIR / "actas_cabecera.parquet"
        df.write_parquet(out, compression="zstd")
        logger.info("escrito: %s (%.1f MB)", out, out.stat().st_size / 1e6)

    return df, latest.collect()


def build_votos(latest: pl.DataFrame, dry_run: bool) -> int:
    lf = pl.scan_parquet(
        str(FACTS_VOT / "**/*.parquet"),
        hive_partitioning=True,
    )
    raw = lf.select(pl.len()).collect().item()

    # Filtrar votos al run_ts_ms ganador de cada idActa (el más reciente que
    # vimos en cabecera). Evita sort+unique global sobre ~18M filas.
    plan = lf.join(latest.lazy(), on=["idActa", "run_ts_ms"], how="semi").drop(
        "snapshot_date", "run_ts_ms"
    )

    if dry_run:
        n = plan.select(pl.len()).collect().item()
        logger.info("votos: %d filas crudas -> %d filas curadas", raw, n)
        return n

    out = CURATED_DIR / "actas_votos.parquet"
    # sink_parquet: escritura streaming, no materializa todo en RAM
    plan.sink_parquet(out, compression="zstd")
    n = pl.scan_parquet(out).select(pl.len()).collect().item()
    logger.info("votos: %d filas crudas -> %d filas curadas", raw, n)
    logger.info("escrito: %s (%.1f MB)", out, out.stat().st_size / 1e6)
    return n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="no escribe archivos")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cab, latest = build_cabecera(args.dry_run)
    n_votos = build_votos(latest, args.dry_run)

    # Sanity (solo si ya tenemos votos escritos)
    if not args.dry_run:
        vot_ids = (
            pl.scan_parquet(CURATED_DIR / "actas_votos.parquet")
            .select(pl.col("idActa").n_unique())
            .collect()
            .item()
        )
        logger.info(
            "coherencia: cabecera=%d, votos_filas=%d, votos_actas_unicas=%d, cabecera_sin_votos=%d",
            cab.height,
            n_votos,
            vot_ids,
            cab.height - vot_ids,
        )


if __name__ == "__main__":
    main()
