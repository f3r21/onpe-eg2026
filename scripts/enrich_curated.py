"""Enriquece curated/actas_cabecera con dimensiones materializadas.

Columnas añadidas:
- idAmbitoGeografico (Int64): 1=Perú, 2=Exterior. Viene de dim/mesas.
- idDistritoElectoral (Int64): 1..27. Derivado del ubigeoDepartamento + idAmbito.
- ubigeoDepartamento (String): 6 dígitos, primeros 2 del ubigeoDistrito.
- ubigeoProvincia (String): 6 dígitos, primeros 4 del ubigeoDistrito.
- nombreDistrito (String): nombre oficial del distrito (de dim/mesas).

Estas columnas ya están implícitamente en dim/mesas, pero materializarlas en
el curated evita el join en cada query analítico y desbloquea DQ Nivel 3
(cruces por distrito electoral).

Uso:
    uv run python scripts/enrich_curated.py              # in-place
    uv run python scripts/enrich_curated.py --dry-run    # reporta cambios

Integración: build_curated.py lo invoca automáticamente si se pasa --enrich
(default en producción, omitido en tests via flag).
"""

from __future__ import annotations

import argparse
import logging

import polars as pl

from onpe.storage import DATA_DIR

log = logging.getLogger("enrich_curated")

CURATED_CAB = DATA_DIR / "curated" / "actas_cabecera.parquet"
DIM_MESAS = DATA_DIR / "dim" / "mesas.parquet"

# Columnas que enrich añade al curated. Si ya existen (re-run idempotente),
# se droppean antes de re-join para evitar columnas duplicadas en el output.
ENRICH_COLS: tuple[str, ...] = (
    "idAmbitoGeografico",
    "ubigeoDepartamento",
    "ubigeoProvincia",
    "nombreDistrito",
    "idDistritoElectoral",
)


def compute_distrito_electoral(
    ubigeo_departamento: str | None,
    ubigeo_provincia: str | None,
    id_ambito: int | None,
) -> int | None:
    """Mapea (ubigeoDepartamento, ubigeoProvincia, idAmbitoGeografico) a idDistritoElectoral.

    IMPORTANTE: ONPE usa su propio ubigeo (distinto de INEI).
    Los 24 departamentos ONPE van 01-23 alfabéticamente sin Callao,
    luego 24=Callao, 25=Ucayali. Los distritos electorales (27) ordenan
    alfabéticamente con Callao en 7 y Ucayali en 26.

    Mapeo ONPE_depto → DE (validado con dim/distritos.parquet):
    - 01-06 (Amazonas..Cajamarca) → DE 1-6 (mismo código)
    - 07-13 (Cusco..Lambayeque) → DE 8-14 (offset +1 por Callao insertado)
    - 14 (Lima):
        * Provincia 140100 (Lima cercado) → DE 15 Lima Metropolitana
        * Otras provincias de depto 14 → DE 16 Lima Provincias
    - 15-23 (Loreto..Tumbes) → DE 17-25 (offset +2 por Lima split)
    - 24 (Callao) → DE 7 (fuera de orden alfabético ONPE)
    - 25 (Ucayali) → DE 26
    - Exterior (idAmbito=2) → DE 27
    """
    if id_ambito == 2:
        return 27
    if not ubigeo_departamento:
        return None
    try:
        depto_code = int(ubigeo_departamento[:2])
    except (ValueError, TypeError):
        return None
    if 1 <= depto_code <= 6:
        return depto_code
    if 7 <= depto_code <= 13:
        return depto_code + 1
    if depto_code == 14:
        if ubigeo_provincia and ubigeo_provincia.startswith("1401"):
            return 15
        return 16
    if 15 <= depto_code <= 23:
        return depto_code + 2
    if depto_code == 24:
        return 7
    if depto_code == 25:
        return 26
    return None


def enrich_cabecera(
    df_cab: pl.DataFrame, df_mesas: pl.DataFrame
) -> pl.DataFrame:
    """Join curated cabecera con dim/mesas + cálculo de idDistritoElectoral.

    Idempotente: si las columnas ENRICH_COLS ya existen en df_cab, se droppean
    antes del join para evitar duplicados.
    """
    # Purga columnas de enriquecimiento previas (re-run idempotente).
    to_drop = [c for c in ENRICH_COLS if c in df_cab.columns]
    if to_drop:
        log.info("drop de cols previas de enrich: %s", to_drop)
        df_cab = df_cab.drop(to_drop)

    # Columnas a traer desde dim/mesas. idMesa de dim = idMesaRef de cabecera.
    mesas_slim = df_mesas.select(
        pl.col("idMesa"),
        pl.col("ubigeoDistrito"),
        pl.col("idAmbitoGeografico"),
        pl.col("ubigeoDepartamento"),
        pl.col("ubigeoProvincia"),
        pl.col("nombreDistrito"),
    )

    before = df_cab.height
    joined = df_cab.join(
        mesas_slim,
        left_on=["idMesaRef", "ubigeoDistrito"],
        right_on=["idMesa", "ubigeoDistrito"],
        how="left",
    )
    after = joined.height
    if after != before:
        raise ValueError(
            f"join introdujo duplicación: {before} → {after}. "
            "Investigar keys en dim/mesas."
        )

    # Derivar idDistritoElectoral fila-a-fila. Polars no tiene un when/then
    # flexible para la lógica Lima split, así que usamos map_rows.
    joined = joined.with_columns(
        pl.struct(
            "ubigeoDepartamento", "ubigeoProvincia", "idAmbitoGeografico"
        )
        .map_elements(
            lambda s: compute_distrito_electoral(
                s["ubigeoDepartamento"], s["ubigeoProvincia"], s["idAmbitoGeografico"]
            ),
            return_dtype=pl.Int64,
        )
        .alias("idDistritoElectoral")
    )

    return joined


def _validate_integrity(df: pl.DataFrame) -> None:
    """Chequeos post-enrich."""
    nulls_ambito = df.select(pl.col("idAmbitoGeografico").is_null().sum()).item()
    nulls_de = df.select(pl.col("idDistritoElectoral").is_null().sum()).item()
    log.info(
        "cardinalidades: n=%d, idAmbitoGeografico nulls=%d, idDistritoElectoral nulls=%d",
        df.height, nulls_ambito, nulls_de,
    )
    # Distribución de DE
    dist_de = (
        df.group_by("idDistritoElectoral")
        .agg(pl.len().alias("n"))
        .sort("idDistritoElectoral")
    )
    log.info("distribución idDistritoElectoral:\n%s", dist_de)
    if nulls_ambito > 0 or nulls_de > 0:
        log.warning(
            "HAY %d null en idAmbitoGeografico y %d null en idDistritoElectoral. "
            "Revisar mapeo de mesas.", nulls_ambito, nulls_de,
        )


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
    log.info("leído curated: %d actas; mesas: %d", df_cab.height, df_mesas.height)

    enriched = enrich_cabecera(df_cab, df_mesas)
    _validate_integrity(enriched)

    if args.dry_run:
        log.info("dry-run: no se escribe")
        return

    # Escritura atómica: tmp + rename.
    tmp = CURATED_CAB.with_suffix(".parquet.tmp")
    enriched.write_parquet(tmp, compression="zstd")
    tmp.replace(CURATED_CAB)
    log.info("escrito: %s (%.1f MB)", CURATED_CAB, CURATED_CAB.stat().st_size / 1e6)


if __name__ == "__main__":
    main()
