"""Ingesta del dataset histórico ONPE EG2021 (primera vuelta presidencial).

Fuente: https://github.com/jmcastagnetto/2021-elecciones-generales-peru-datos-de-onpe
(archivado; datos extraídos del API ONPE 2021 antes de ser publicados en
datosabiertos.gob.pe).

**IMPORTANTE**: el UBIGEO en los CSVs es RENIEC, NO INEI. Para convertir
usar https://github.com/jmcastagnetto/ubigeo-peru-aumentado — fuera de scope
de esta ingesta (se deja como columna adicional si el CSV la trae).

**Cobertura**:
- Distritos: 1,875 (universo Perú 2021)
- Actas: 86,494 (mesas × primera vuelta)
- Partidos: 18 (1ra vuelta)
- Segunda vuelta: incluida en actas_ronderos con prefijo v2_*

Uso:
    uv run python scripts/ingest_historico_eg2021.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from onpe.storage import DATA_DIR

log = logging.getLogger("ingest_eg2021")

RAW_DIR = DATA_DIR / "historico" / "eg2021" / "raw"
OUT_DIR = DATA_DIR / "historico" / "eg2021"


def _csv_to_parquet(src: Path, dst: Path, infer_schema_length: int = 10000) -> int:
    """Lee CSV → escribe parquet (zstd) + devuelve row count."""
    df = pl.read_csv(src, infer_schema_length=infer_schema_length, ignore_errors=True)
    df.write_parquet(dst, compression="zstd")
    return df.height


def ingest_distritos_generales() -> None:
    """1,875 distritos × (electores, actas, %, etc.)."""
    n = _csv_to_parquet(
        RAW_DIR / "presidencial-datos-generales.csv",
        OUT_DIR / "eg2021_distritos_generales.parquet",
    )
    log.info("eg2021_distritos_generales: %d filas", n)


def ingest_distritos_resumen() -> None:
    """1,875 distritos × (mesas instaladas/contabilizadas/impugnadas/etc.)."""
    n = _csv_to_parquet(
        RAW_DIR / "presidencial-datos-resumen.csv",
        OUT_DIR / "eg2021_distritos_resumen.parquet",
    )
    log.info("eg2021_distritos_resumen: %d filas", n)


def ingest_distritos_votos() -> None:
    """~7.5k filas: distrito × métrica (emitidos/blanco/nulo/impugnado)."""
    n = _csv_to_parquet(
        RAW_DIR / "presidencial-datos-votos.csv",
        OUT_DIR / "eg2021_distritos_votos.parquet",
    )
    log.info("eg2021_distritos_votos: %d filas", n)


def ingest_distritos_partidos() -> None:
    """~33k filas: distrito × partido con total_votos + %."""
    n = _csv_to_parquet(
        RAW_DIR / "presidencial-resultados-partidos.csv",
        OUT_DIR / "eg2021_distritos_partidos.parquet",
    )
    log.info("eg2021_distritos_partidos: %d filas", n)


def ingest_participacion() -> None:
    """1,875 distritos × participación (asistentes, ausentes, %)."""
    n = _csv_to_parquet(
        RAW_DIR / "resultados-participacion-por-distrito.csv",
        OUT_DIR / "eg2021_distritos_participacion.parquet",
    )
    log.info("eg2021_distritos_participacion: %d filas", n)


def ingest_actas_mesa() -> None:
    """86,494 mesas × v1/v2 votos por partido — la joya del dataset.

    Normaliza de wide (una columna por combinación) a long:
      mesa | vuelta (1|2) | metric | valor
    donde `metric` puede ser: TOT_CIUDADANOS_VOTARON, emitidos, validos,
    blanco, impugnados, nulos, fp (Fuerza Popular), perulibre, etc.
    """
    df = pl.read_csv(
        RAW_DIR / "presidencial-actas_ronderos.pe.csv",
        infer_schema_length=20000,
        ignore_errors=True,
    )
    # Shape wide: rowid, mesa, v1_TOT_CIUDADANOS_VOTARON, v1_fp, v1_perulibre, ..., v2_TOT_..., v2_fp, ...
    # Identificar cols base (no-prefijadas) y las con prefijo v1_/v2_
    base_cols = [c for c in df.columns if not c.startswith(("v1_", "v2_"))]
    v1_cols = [c for c in df.columns if c.startswith("v1_")]
    v2_cols = [c for c in df.columns if c.startswith("v2_")]
    log.info(
        "actas_mesa wide shape: %d filas, %d base + %d v1 + %d v2 cols",
        df.height, len(base_cols), len(v1_cols), len(v2_cols),
    )

    # Persistir ambos formatos:
    # 1. Wide (como viene) — útil para consumers que quieren joined view
    df.write_parquet(OUT_DIR / "eg2021_actas_mesa_wide.parquet", compression="zstd")
    log.info("eg2021_actas_mesa_wide: %d filas", df.height)

    # 2. Long — fácil para agregados por partido/vuelta
    def melt_vuelta(df: pl.DataFrame, vuelta_cols: list[str], vuelta_num: int) -> pl.DataFrame:
        if not vuelta_cols:
            return pl.DataFrame()
        sub = df.select(base_cols + vuelta_cols).unpivot(
            index=base_cols,
            on=vuelta_cols,
            variable_name="col",
            value_name="valor",
        )
        # "v1_fp" → "fp"
        sub = sub.with_columns(
            pl.col("col").str.replace(f"^v{vuelta_num}_", "").alias("metric"),
            pl.lit(vuelta_num).cast(pl.Int8).alias("vuelta"),
        ).drop("col")
        return sub

    long_v1 = melt_vuelta(df, v1_cols, 1)
    long_v2 = melt_vuelta(df, v2_cols, 2) if v2_cols else pl.DataFrame()
    long_all = pl.concat([long_v1, long_v2], how="diagonal_relaxed") if not long_v2.is_empty() else long_v1
    long_all.write_parquet(OUT_DIR / "eg2021_actas_mesa_long.parquet", compression="zstd")
    log.info(
        "eg2021_actas_mesa_long: %d filas (v1=%d, v2=%d)",
        long_all.height, long_v1.height, long_v2.height if not long_v2.is_empty() else 0,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if not RAW_DIR.exists():
        raise SystemExit(f"falta {RAW_DIR}. Descargar CSVs primero.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    ingest_distritos_generales()
    ingest_distritos_resumen()
    ingest_distritos_votos()
    ingest_distritos_partidos()
    ingest_participacion()
    ingest_actas_mesa()

    log.info("\n=== Outputs en %s ===", OUT_DIR)
    for p in sorted(OUT_DIR.glob("*.parquet")):
        size_mb = p.stat().st_size / 1e6
        n = pl.scan_parquet(p).select(pl.len()).collect().item()
        log.info("  %-42s %.1f MB  %d filas", p.name, size_mb, n)


if __name__ == "__main__":
    main()
