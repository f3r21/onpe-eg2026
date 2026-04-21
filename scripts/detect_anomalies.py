"""Detector baseline de anomalías contables y estadísticas en el dataset EG2026.

Ejecuta 6 detectores sobre `curated/*.parquet` + `dim/padron.parquet` (si está)
y escribe `data/analytics/anomalias.parquet` con una fila por (acta × tipo).
También genera un resumen markdown en `data/analytics/anomalias_resumen.md`.

Detectores:

1. **validos_gt_emitidos** (CRITICAL) — `totalVotosValidos > totalVotosEmitidos`.
   Matemáticamente imposible; marca error de captura o corrupción.

2. **asistentes_gt_habiles** (CRITICAL) — `totalAsistentes > totalElectoresHabiles`.
   Imposible; indica error o posible manipulación.

3. **participacion_gt_100** (CRITICAL) — `porcentajeParticipacionCiudadana > 100`.
   Redundante con #2 pero específico a la forma de reporte.

4. **suma_votos_mismatch** (HIGH) — en actas C, `sum(nvotos) ≠ totalVotosEmitidos`.
   Indicador de errores de conteo del acta individual (drift permitido ±1 por rounding).

5. **concentracion_extrema** (MEDIUM) — un partido con ≥ 95% de votos válidos.
   No necesariamente ilegítimo (hay distritos muy leales) pero señala revisión.

6. **participacion_outlier_distrito** (MEDIUM) — participación con z-score |z|>3 dentro
   del (idDistritoElectoral × idEleccion). Detecta actas que se alejan del patrón local.

7. **emitidos_gt_padron_reniec** (HIGH, solo si existe padron.parquet) — sum de emitidos
   por distrito > padrón RENIEC 2026. Cross-check contra fuente externa oficial.

Uso:
    uv run python scripts/detect_anomalies.py                 # default: todas las elecciones
    uv run python scripts/detect_anomalies.py --eleccion 10   # solo presidencial
    uv run python scripts/detect_anomalies.py --min-severity HIGH
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import polars as pl

from onpe.storage import DATA_DIR

log = logging.getLogger("detect_anomalies")

CURATED_DIR = DATA_DIR / "curated"
DIM_DIR = DATA_DIR / "dim"
OUT_DIR = DATA_DIR / "analytics"

SEVERITY_LEVELS = ("CRITICAL", "HIGH", "MEDIUM")
SEVERITY_RANK = {s: i for i, s in enumerate(SEVERITY_LEVELS)}

# Z-score para outlier distrital (~99.7% percentil, equivalente a 3-sigma).
OUTLIER_Z_THRESHOLD = 3.0

# Delta máximo tolerado entre sum(nvotos) y totalVotosEmitidos (rounding del API).
SUMA_VOTOS_TOLERANCIA = 1

# Un partido con ≥ 95% de votos válidos es marcado concentración extrema.
CONCENTRACION_THRESHOLD = 0.95

# Padrón RENIEC: flaggeamos si emitidos distritales exceden padrón por > 5% (margen de
# seguridad por caducidad DNI + migración interna no reflejada en el snapshot Q1 2026).
PADRON_MARGIN = 0.05


# ──────────────────────────────────────────────────────────────────────
#  Detectores
# ──────────────────────────────────────────────────────────────────────


def detect_validos_gt_emitidos(cab: pl.LazyFrame) -> pl.LazyFrame:
    return cab.filter(
        (pl.col("totalVotosValidos").is_not_null())
        & (pl.col("totalVotosEmitidos").is_not_null())
        & (pl.col("totalVotosValidos") > pl.col("totalVotosEmitidos"))
    ).select(
        [
            pl.lit("validos_gt_emitidos").alias("tipo"),
            pl.lit("CRITICAL").alias("severidad"),
            pl.col("idActa"),
            pl.col("idEleccion"),
            pl.col("codigoEstadoActa"),
            pl.col("ubigeoDistrito"),
            pl.col("idDistritoElectoral"),
            pl.col("totalVotosValidos").cast(pl.Float64).alias("valor_observado"),
            pl.col("totalVotosEmitidos").cast(pl.Float64).alias("valor_esperado_max"),
            pl.format(
                "validos={} > emitidos={} (imposible)",
                pl.col("totalVotosValidos"),
                pl.col("totalVotosEmitidos"),
            ).alias("mensaje"),
        ]
    )


def detect_asistentes_gt_habiles(cab: pl.LazyFrame) -> pl.LazyFrame:
    return cab.filter(
        (pl.col("totalAsistentes").is_not_null())
        & (pl.col("totalAsistentes") > pl.col("totalElectoresHabiles"))
    ).select(
        [
            pl.lit("asistentes_gt_habiles").alias("tipo"),
            pl.lit("CRITICAL").alias("severidad"),
            pl.col("idActa"),
            pl.col("idEleccion"),
            pl.col("codigoEstadoActa"),
            pl.col("ubigeoDistrito"),
            pl.col("idDistritoElectoral"),
            pl.col("totalAsistentes").cast(pl.Float64).alias("valor_observado"),
            pl.col("totalElectoresHabiles").cast(pl.Float64).alias("valor_esperado_max"),
            pl.format(
                "asistentes={} > hábiles={} (imposible)",
                pl.col("totalAsistentes"),
                pl.col("totalElectoresHabiles"),
            ).alias("mensaje"),
        ]
    )


def detect_participacion_gt_100(cab: pl.LazyFrame) -> pl.LazyFrame:
    return cab.filter(
        (pl.col("porcentajeParticipacionCiudadana").is_not_null())
        & (pl.col("porcentajeParticipacionCiudadana") > 100.0)
    ).select(
        [
            pl.lit("participacion_gt_100").alias("tipo"),
            pl.lit("CRITICAL").alias("severidad"),
            pl.col("idActa"),
            pl.col("idEleccion"),
            pl.col("codigoEstadoActa"),
            pl.col("ubigeoDistrito"),
            pl.col("idDistritoElectoral"),
            pl.col("porcentajeParticipacionCiudadana").alias("valor_observado"),
            pl.lit(100.0).alias("valor_esperado_max"),
            pl.format("participación={}% > 100%", pl.col("porcentajeParticipacionCiudadana")).alias(
                "mensaje"
            ),
        ]
    )


def detect_suma_votos_mismatch(cab: pl.LazyFrame, votos: pl.LazyFrame) -> pl.LazyFrame:
    """En actas C, sum(nvotos) debe == totalVotosEmitidos ± tolerancia."""
    suma_por_acta = votos.group_by("idActa").agg(pl.col("nvotos").sum().alias("suma_nvotos"))
    return (
        cab.filter(pl.col("codigoEstadoActa") == "C")
        .filter(pl.col("totalVotosEmitidos").is_not_null())
        .join(suma_por_acta, on="idActa", how="inner")
        .filter(
            (pl.col("suma_nvotos") - pl.col("totalVotosEmitidos")).abs() > SUMA_VOTOS_TOLERANCIA
        )
        .select(
            [
                pl.lit("suma_votos_mismatch").alias("tipo"),
                pl.lit("HIGH").alias("severidad"),
                pl.col("idActa"),
                pl.col("idEleccion"),
                pl.col("codigoEstadoActa"),
                pl.col("ubigeoDistrito"),
                pl.col("idDistritoElectoral"),
                pl.col("suma_nvotos").cast(pl.Float64).alias("valor_observado"),
                pl.col("totalVotosEmitidos").cast(pl.Float64).alias("valor_esperado_max"),
                pl.format(
                    "suma(nvotos)={} ≠ emitidos={} (delta > {})",
                    pl.col("suma_nvotos"),
                    pl.col("totalVotosEmitidos"),
                    pl.lit(SUMA_VOTOS_TOLERANCIA),
                ).alias("mensaje"),
            ]
        )
    )


def detect_concentracion_extrema(cab: pl.LazyFrame, votos: pl.LazyFrame) -> pl.LazyFrame:
    """Un partido (no especial) con ≥ 95% de votos válidos de la mesa."""
    partidos = votos.filter(~pl.col("es_especial"))
    max_partido = partidos.group_by("idActa").agg(
        pl.col("nvotos").max().alias("max_nvotos_partido")
    )
    return (
        cab.filter(pl.col("codigoEstadoActa") == "C")
        .filter(pl.col("totalVotosValidos") > 0)
        .join(max_partido, on="idActa", how="inner")
        .with_columns((pl.col("max_nvotos_partido") / pl.col("totalVotosValidos")).alias("ratio"))
        .filter(pl.col("ratio") >= CONCENTRACION_THRESHOLD)
        .select(
            [
                pl.lit("concentracion_extrema").alias("tipo"),
                pl.lit("MEDIUM").alias("severidad"),
                pl.col("idActa"),
                pl.col("idEleccion"),
                pl.col("codigoEstadoActa"),
                pl.col("ubigeoDistrito"),
                pl.col("idDistritoElectoral"),
                pl.col("ratio").alias("valor_observado"),
                pl.lit(CONCENTRACION_THRESHOLD).alias("valor_esperado_max"),
                pl.format(
                    "1 partido tomó {}% de válidos (>= {}%)",
                    (pl.col("ratio") * 100).round(1),
                    pl.lit(int(CONCENTRACION_THRESHOLD * 100)),
                ).alias("mensaje"),
            ]
        )
    )


def detect_participacion_outlier(cab: pl.LazyFrame) -> pl.LazyFrame:
    """Actas cuyo porcentaje de participación está a > 3 sigma de la media del DE×elección."""
    base = cab.filter(
        (pl.col("porcentajeParticipacionCiudadana").is_not_null())
        & (pl.col("porcentajeParticipacionCiudadana") > 0)
        & (pl.col("porcentajeParticipacionCiudadana") <= 100)
    )
    stats = (
        base.group_by(["idDistritoElectoral", "idEleccion"])
        .agg(
            [
                pl.col("porcentajeParticipacionCiudadana").mean().alias("mean_part"),
                pl.col("porcentajeParticipacionCiudadana").std().alias("std_part"),
            ]
        )
        .filter(pl.col("std_part") > 0)
    )
    with_z = base.join(stats, on=["idDistritoElectoral", "idEleccion"], how="inner").with_columns(
        (
            (pl.col("porcentajeParticipacionCiudadana") - pl.col("mean_part")) / pl.col("std_part")
        ).alias("z_score")
    )
    return with_z.filter(pl.col("z_score").abs() > OUTLIER_Z_THRESHOLD).select(
        [
            pl.lit("participacion_outlier_distrito").alias("tipo"),
            pl.lit("MEDIUM").alias("severidad"),
            pl.col("idActa"),
            pl.col("idEleccion"),
            pl.col("codigoEstadoActa"),
            pl.col("ubigeoDistrito"),
            pl.col("idDistritoElectoral"),
            pl.col("porcentajeParticipacionCiudadana").alias("valor_observado"),
            pl.col("mean_part").alias("valor_esperado_max"),
            pl.format(
                "participación={}% vs media DE={}% (z={})",
                pl.col("porcentajeParticipacionCiudadana").round(1),
                pl.col("mean_part").round(1),
                pl.col("z_score").round(2),
            ).alias("mensaje"),
        ]
    )


def detect_emitidos_gt_padron(cab: pl.LazyFrame, padron: pl.DataFrame) -> pl.LazyFrame:
    """Agregado distrital: sum(emitidos por distrito) vs padron RENIEC Q1 2026.

    Tolerancia PADRON_MARGIN (5% de caducidad/migración no reflejada en snapshot).
    Solo aplica para elección Presidencial (10) para evitar inflación por
    elecciones con doble voto (las otras tienen misma mesa = mismo elector).
    """
    per_distrito = (
        cab.filter(pl.col("idEleccion") == 10)
        .filter(pl.col("codigoEstadoActa") == "C")
        .filter(pl.col("idAmbitoGeografico") == 1)  # solo Perú — padron extranjero es por país
        .group_by("ubigeoDistrito")
        .agg(pl.col("totalVotosEmitidos").sum().alias("emitidos_distrito"))
        .collect()
    )
    padron_peru = padron.filter(pl.col("residencia") == "Nacional").select(
        ["ubigeo_reniec", "total_electores"]
    )
    joined = per_distrito.join(
        padron_peru, left_on="ubigeoDistrito", right_on="ubigeo_reniec", how="inner"
    )
    flagged = joined.filter(
        pl.col("emitidos_distrito") > pl.col("total_electores") * (1 + PADRON_MARGIN)
    )
    return flagged.select(
        [
            pl.lit("emitidos_gt_padron_reniec").alias("tipo"),
            pl.lit("HIGH").alias("severidad"),
            pl.lit(None).cast(pl.Int64).alias("idActa"),
            pl.lit(10).cast(pl.Int64).alias("idEleccion"),
            pl.lit("C").alias("codigoEstadoActa"),
            pl.col("ubigeoDistrito"),
            pl.lit(None).cast(pl.Int64).alias("idDistritoElectoral"),
            pl.col("emitidos_distrito").cast(pl.Float64).alias("valor_observado"),
            (pl.col("total_electores") * (1 + PADRON_MARGIN))
            .cast(pl.Float64)
            .alias("valor_esperado_max"),
            pl.format(
                "emitidos={} > padrón×{} = {} (padrón RENIEC Q1 2026)",
                pl.col("emitidos_distrito"),
                pl.lit(1 + PADRON_MARGIN),
                (pl.col("total_electores") * (1 + PADRON_MARGIN)).cast(pl.Int64),
            ).alias("mensaje"),
        ]
    ).lazy()


# ──────────────────────────────────────────────────────────────────────
#  Orquestación
# ──────────────────────────────────────────────────────────────────────


def run_all(eleccion: int | None, min_severity: str) -> pl.DataFrame:
    cab = pl.scan_parquet(CURATED_DIR / "actas_cabecera.parquet")
    votos = pl.scan_parquet(CURATED_DIR / "actas_votos.parquet")
    if eleccion is not None:
        cab = cab.filter(pl.col("idEleccion") == eleccion)
        votos = votos.filter(pl.col("idEleccion") == eleccion)

    detectors: list[pl.LazyFrame] = [
        detect_validos_gt_emitidos(cab),
        detect_asistentes_gt_habiles(cab),
        detect_participacion_gt_100(cab),
        detect_suma_votos_mismatch(cab, votos),
        detect_concentracion_extrema(cab, votos),
        detect_participacion_outlier(cab),
    ]

    padron_path = DIM_DIR / "padron.parquet"
    if padron_path.exists() and eleccion in (None, 10):
        log.info("padron RENIEC disponible — corriendo emitidos_gt_padron_reniec")
        padron_df = pl.read_parquet(padron_path)
        detectors.append(detect_emitidos_gt_padron(cab, padron_df))
    else:
        log.info("padron RENIEC no disponible, skip detector de padrón")

    combined = pl.concat(detectors, how="diagonal_relaxed").collect()

    # Filtro severidad mínima.
    min_rank = SEVERITY_RANK[min_severity]
    combined = combined.filter(
        pl.col("severidad").replace_strict(SEVERITY_RANK, default=99) <= min_rank
    )

    return combined.sort(
        ["severidad", "tipo", "idEleccion", "ubigeoDistrito"],
    )


def write_resumen_md(df: pl.DataFrame, out_md: Path) -> None:
    """Escribe un resumen markdown ejecutivo de las anomalías detectadas."""
    lines: list[str] = ["# Anomaly Report — onpe-eg2026 v1.0\n"]
    lines.append(f"**Total hallazgos**: {df.shape[0]:,}\n")

    if df.is_empty():
        lines.append("\n_Sin anomalías detectadas._\n")
        out_md.write_text("\n".join(lines))
        return

    por_tipo = (
        df.group_by(["severidad", "tipo"]).agg(pl.len().alias("n")).sort("n", descending=True)
    )
    lines.append("\n## Resumen por tipo\n")
    lines.append("| severidad | tipo | hallazgos |")
    lines.append("|---|---|---|")
    for row in por_tipo.iter_rows(named=True):
        lines.append(f"| {row['severidad']} | `{row['tipo']}` | {row['n']:,} |")

    por_eleccion = df.group_by("idEleccion").agg(pl.len().alias("n")).sort("n", descending=True)
    lines.append("\n## Por elección\n")
    lines.append("| idEleccion | hallazgos |")
    lines.append("|---|---|")
    for row in por_eleccion.iter_rows(named=True):
        lines.append(f"| {row['idEleccion']} | {row['n']:,} |")

    # Top 10 hallazgos CRITICAL
    top_critical = df.filter(pl.col("severidad") == "CRITICAL").head(10)
    if not top_critical.is_empty():
        lines.append("\n## Top 10 hallazgos CRITICAL\n")
        lines.append("| tipo | idActa | idEleccion | distrito | mensaje |")
        lines.append("|---|---|---|---|---|")
        for row in top_critical.iter_rows(named=True):
            lines.append(
                f"| `{row['tipo']}` | `{row['idActa']}` | {row['idEleccion']} | "
                f"`{row['ubigeoDistrito']}` | {row['mensaje']} |"
            )

    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--eleccion",
        type=int,
        default=None,
        choices=[10, 12, 13, 14, 15],
        help="filtrar a una elección (default: todas)",
    )
    parser.add_argument(
        "--min-severity",
        choices=list(SEVERITY_LEVELS),
        default="MEDIUM",
        help="severidad mínima a reportar",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("corriendo detectores (eleccion=%s, min_sev=%s)", args.eleccion, args.min_severity)
    df = run_all(args.eleccion, args.min_severity)

    out_parquet = OUT_DIR / "anomalias.parquet"
    df.write_parquet(out_parquet, compression="zstd")
    log.info("parquet: %s (%d filas)", out_parquet, df.shape[0])

    out_md = OUT_DIR / "anomalias_resumen.md"
    write_resumen_md(df, out_md)
    log.info("markdown: %s", out_md)

    # Resumen ejecutivo JSON (para dashboards/CI)
    summary = {
        "total": df.shape[0],
        "por_severidad": {
            k: v for k, v in df.group_by("severidad").agg(pl.len().alias("n")).iter_rows()
        },
        "por_tipo": {k: v for k, v in df.group_by("tipo").agg(pl.len().alias("n")).iter_rows()},
        "eleccion_filtro": args.eleccion,
        "min_severity": args.min_severity,
    }
    (OUT_DIR / "anomalias_resumen.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False)
    )

    print()
    print("Anomaly report")
    print(f"  total hallazgos     : {df.shape[0]:,}")
    for sev in SEVERITY_LEVELS:
        n = summary["por_severidad"].get(sev, 0)
        print(f"    {sev:9s}        : {n:>6,}")
    print(f"  parquet             : {out_parquet}")
    print(f"  markdown            : {out_md}")


if __name__ == "__main__":
    main()
