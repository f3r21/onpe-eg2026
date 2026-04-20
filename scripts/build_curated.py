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
FACTS_LT = DATA_DIR / "facts" / "actas_linea_tiempo"
FACTS_ARCH = DATA_DIR / "facts" / "actas_archivos"

logger = logging.getLogger("curated")


def _latest_run_per_idacta(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Para cada idActa, el run_ts_ms más reciente. Key semi-join eficiente."""
    return lf.group_by("idActa").agg(pl.col("run_ts_ms").max())


def _scan_relaxed(base_dir: Path) -> pl.LazyFrame:
    """Escanea parquets Hive-particionados concatenando con schemas relajados.

    Algunos chunks terminan con columnas de dtype Null en Polars cuando un
    chunk entero no trae valores poblados (e.g. `cargo`/`sexo` ausente para
    Presidencial, o la columna legacy `idMesa` que el API nunca devuelve).
    Concatenar con el glob default falla al mezclarse con chunks donde la
    misma columna es String. `diagonal_relaxed` promueve Null al supertipo
    (String) en la unión.
    """
    paths = sorted(base_dir.glob("**/*.parquet"))
    if not paths:
        raise FileNotFoundError(f"sin parquets bajo {base_dir}")
    lfs = [pl.scan_parquet(str(p), hive_partitioning=True) for p in paths]
    return pl.concat(lfs, how="diagonal_relaxed")


# Columnas legacy que existen en chunks históricos pero no aportan información
# (100% null) y contaminan el schema del curated. Se droppean si aparecen.
_LEGACY_DROP_IF_PRESENT = ("idMesa",)


def _drop_legacy(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema_names = lf.collect_schema().names()
    to_drop = [c for c in _LEGACY_DROP_IF_PRESENT if c in schema_names]
    return lf.drop(to_drop) if to_drop else lf


def build_cabecera(dry_run: bool) -> tuple[pl.DataFrame, pl.DataFrame]:
    lf = _drop_legacy(_scan_relaxed(FACTS_CAB))
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
    lf = _scan_relaxed(FACTS_VOT)
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


def _build_aux(table_name: str, base_dir: Path, latest: pl.DataFrame, dry_run: bool) -> int:
    """Builder generico para tablas auxiliares (linea_tiempo, archivos).

    Mismo patron que build_votos: semi-join contra el run_ts_ms ganador por
    idActa. Si no hay chunks (p.ej. snapshot viejo sin estas tablas), skippea.
    """
    if not any(base_dir.glob("**/*.parquet")):
        logger.info("%s: sin chunks en %s (skip)", table_name, base_dir)
        return 0

    lf = _scan_relaxed(base_dir)
    raw = lf.select(pl.len()).collect().item()
    plan = lf.join(latest.lazy(), on=["idActa", "run_ts_ms"], how="semi").drop(
        "snapshot_date", "run_ts_ms"
    )

    if dry_run:
        n = plan.select(pl.len()).collect().item()
        logger.info("%s: %d filas crudas -> %d filas curadas", table_name, raw, n)
        return n

    out = CURATED_DIR / f"{table_name}.parquet"
    plan.sink_parquet(out, compression="zstd")
    n = pl.scan_parquet(out).select(pl.len()).collect().item()
    logger.info("%s: %d filas crudas -> %d filas curadas", table_name, raw, n)
    logger.info("escrito: %s (%.1f MB)", out, out.stat().st_size / 1e6)
    return n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="no escribe archivos")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--no-enrich",
        action="store_true",
        help="omitir el paso de enriquecimiento (idAmbitoGeografico + idDistritoElectoral)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cab, latest = build_cabecera(args.dry_run)
    n_votos = build_votos(latest, args.dry_run)
    _build_aux("actas_linea_tiempo", FACTS_LT, latest, args.dry_run)
    _build_aux("actas_archivos", FACTS_ARCH, latest, args.dry_run)

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

    # Enriquecimiento: agrega idAmbitoGeografico + idDistritoElectoral + ubigeoDepartamento/Provincia
    # + nombreDistrito desde dim/mesas. Idempotente (re-run dropa cols previas antes de join).
    if not args.dry_run and not args.no_enrich:
        # Import local: enrich_curated está en scripts/ (mismo dir), no en el pythonpath por defecto.
        import sys

        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from enrich_curated import _validate_integrity, enrich_cabecera

        logger.info("enriqueciendo curated con idAmbitoGeografico + idDistritoElectoral")
        df_cab = pl.read_parquet(CURATED_DIR / "actas_cabecera.parquet")
        df_mesas = pl.read_parquet(DATA_DIR / "dim" / "mesas.parquet")
        enriched = enrich_cabecera(df_cab, df_mesas)
        _validate_integrity(enriched)
        tmp = CURATED_DIR / "actas_cabecera.parquet.tmp"
        enriched.write_parquet(tmp, compression="zstd")
        tmp.replace(CURATED_DIR / "actas_cabecera.parquet")
        logger.info("curated enriquecido: +%d cols", len(enriched.columns) - len(df_cab.columns))


if __name__ == "__main__":
    main()
