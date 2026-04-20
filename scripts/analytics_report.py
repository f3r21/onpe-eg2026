"""Analytics report sobre el dataset EG2026 — consolidado.

Produce un reporte legible (text + parquet intermedios) con:
1. Resumen ejecutivo (universo, cobertura, DQ)
2. Top partidos por elección (Presidencial, Diputados, Senadores nac/reg, Parl Andino)
3. Participación por departamento y distrito electoral
4. Comparativa EG2026 vs EG2021 (si histórico ingestado)
5. Ranking partidos por distrito electoral (DE 1-27)
6. Stats de voto exterior (DE 27)

Outputs en data/analytics/:
- report.txt — resumen ejecutivo legible
- top_partidos_por_eleccion.parquet
- participacion_por_depto.parquet
- partidos_por_de.parquet
- eg2021_vs_eg2026_participacion.parquet (si histórico)

Uso:
    uv run python scripts/analytics_report.py
    uv run python scripts/analytics_report.py --outdir data/analytics
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import polars as pl

from onpe.storage import DATA_DIR

log = logging.getLogger("analytics")

CURATED_CAB = DATA_DIR / "curated" / "actas_cabecera.parquet"
CURATED_VOT = DATA_DIR / "curated" / "actas_votos.parquet"
DIM_MESAS = DATA_DIR / "dim" / "mesas.parquet"
DIM_DE = DATA_DIR / "dim" / "distritos_electorales.parquet"
HISTORICO_EG2021 = DATA_DIR / "historico" / "eg2021"

ELECCIONES = {
    10: "Presidencial",
    12: "Parlamento Andino",
    13: "Diputados",
    14: "Senadores regional",
    15: "Senadores nacional",
}


def resumen_ejecutivo(cab: pl.DataFrame) -> str:
    """String bloque con cifras clave."""
    lines = ["=" * 70, "RESUMEN EJECUTIVO — DATASET EG2026", "=" * 70, ""]
    lines.append(f"Total actas: {cab.height:,}")
    lines.append(f"Mesas únicas: {cab['codigoMesa'].n_unique():,}")
    lines.append(f"Elecciones: {cab['idEleccion'].n_unique()}")
    lines.append("")
    lines.append("Distribución por estado:")
    by_estado = cab.group_by("codigoEstadoActa").agg(pl.len().alias("n")).sort("n", descending=True)
    for row in by_estado.iter_rows(named=True):
        pct = row["n"] / cab.height * 100
        lines.append(f"  {row['codigoEstadoActa']}: {row['n']:,} ({pct:.1f}%)")
    lines.append("")
    lines.append("Distribución por elección:")
    by_el = cab.group_by("idEleccion").agg(pl.len().alias("n")).sort("idEleccion")
    for row in by_el.iter_rows(named=True):
        name = ELECCIONES.get(row["idEleccion"], "?")
        lines.append(f"  id={row['idEleccion']} {name}: {row['n']:,}")
    lines.append("")
    return "\n".join(lines)


def top_partidos(vot: pl.DataFrame, cab: pl.DataFrame, outdir: Path) -> str:
    """Top 10 partidos por elección con % del total y total_votos."""
    # Filtrar no-especiales y agregar por partido × elección
    df = (
        vot.filter(~pl.col("es_especial"))
        .group_by(["idEleccion", "nagrupacionPolitica", "descripcion"])
        .agg(
            pl.col("nvotos").sum().alias("total_votos"),
        )
    )
    # Total votos por elección
    total_por_el = df.group_by("idEleccion").agg(pl.col("total_votos").sum().alias("sum_eleccion"))
    df = df.join(total_por_el, on="idEleccion").with_columns(
        (pl.col("total_votos") / pl.col("sum_eleccion") * 100).alias("pct")
    )

    df.write_parquet(outdir / "top_partidos_por_eleccion.parquet", compression="zstd")

    # Top 10 por elección
    lines = ["=" * 70, "TOP 10 PARTIDOS POR ELECCIÓN (C + E)", "=" * 70, ""]
    for el_id, el_name in ELECCIONES.items():
        top = df.filter(pl.col("idEleccion") == el_id).sort("total_votos", descending=True).head(10)
        if top.is_empty():
            continue
        lines.append(f"--- id={el_id} {el_name} ---")
        for row in top.iter_rows(named=True):
            lines.append(
                f"  {row['total_votos']:>12,}  {row['pct']:5.2f}%  {row['descripcion'][:60]}"
            )
        lines.append("")
    return "\n".join(lines)


def participacion_por_depto(cab: pl.DataFrame, outdir: Path) -> str:
    """Participación Presidencial por depto (filtramos id=10 C)."""
    df = (
        cab.filter((pl.col("idEleccion") == 10) & (pl.col("codigoEstadoActa") == "C"))
        .with_columns(pl.col("ubigeoDistrito").str.slice(0, 2).alias("depto_code"))
        .group_by("depto_code")
        .agg(
            pl.col("totalElectoresHabiles").sum().alias("hab"),
            pl.col("totalVotosEmitidos").sum().alias("emit"),
            pl.len().alias("n_actas"),
        )
        .with_columns((pl.col("emit") / pl.col("hab") * 100).alias("pct_participacion"))
        .sort("pct_participacion", descending=True)
    )
    df.write_parquet(outdir / "participacion_por_depto.parquet", compression="zstd")

    lines = ["=" * 70, "PARTICIPACIÓN CIUDADANA POR DEPTO (PRESIDENCIAL, C)", "=" * 70, ""]
    lines.append(f"  {'depto':<6} {'participación':>14} {'emitidos':>12} {'hábiles':>12}")
    for row in df.iter_rows(named=True):
        lines.append(
            f"  {row['depto_code']:<6} {row['pct_participacion']:13.2f}% {row['emit']:>12,} {row['hab']:>12,}"
        )
    lines.append("")
    return "\n".join(lines)


def partidos_por_de(vot: pl.DataFrame, cab: pl.DataFrame, outdir: Path) -> str:
    """Partidos ganadores en Diputados (id=13) por distrito electoral."""
    if "idDistritoElectoral" not in cab.columns:
        return "(skipped: curated no enriquecido con idDistritoElectoral)\n"

    vot_de = vot.filter((~pl.col("es_especial")) & (pl.col("idEleccion") == 13)).join(
        cab.filter(pl.col("idEleccion") == 13).select("idActa", "idDistritoElectoral"),
        on="idActa",
        how="inner",
    )
    por_de_partido = vot_de.group_by(
        ["idDistritoElectoral", "nagrupacionPolitica", "descripcion"]
    ).agg(pl.col("nvotos").sum().alias("total_votos"))
    # Ganador por DE
    ganadores = por_de_partido.sort(
        ["idDistritoElectoral", "total_votos"], descending=[False, True]
    ).unique(subset=["idDistritoElectoral"], keep="first")

    por_de_partido.write_parquet(outdir / "partidos_por_de_diputados.parquet", compression="zstd")
    ganadores.write_parquet(outdir / "ganadores_de_diputados.parquet", compression="zstd")

    lines = ["=" * 70, "PARTIDO GANADOR DIPUTADOS (id=13) POR DISTRITO ELECTORAL", "=" * 70, ""]
    for row in ganadores.sort("idDistritoElectoral").iter_rows(named=True):
        lines.append(
            f"  DE {row['idDistritoElectoral']:>2}: {row['total_votos']:>10,}  {row['descripcion'][:55]}"
        )
    lines.append("")
    return "\n".join(lines)


def exterior_stats(cab: pl.DataFrame, vot: pl.DataFrame) -> str:
    """Stats especiales del voto exterior (idAmbitoGeografico=2)."""
    if "idAmbitoGeografico" not in cab.columns:
        return "(skipped: curated no enriquecido)\n"

    ext_cab = cab.filter(pl.col("idAmbitoGeografico") == 2)
    ext_vot = vot.filter(
        pl.col("idActa").is_in(ext_cab["idActa"].implode()) & (~pl.col("es_especial"))
    )

    lines = ["=" * 70, "VOTO EXTERIOR — DE 27", "=" * 70, ""]
    lines.append(f"Mesas exterior: {ext_cab['codigoMesa'].n_unique():,}")
    lines.append(f"Actas totales (× 5 elecciones): {ext_cab.height:,}")
    lines.append("")

    # Top 5 ciudades exterior (ubigeoDistrito)
    lines.append("Top 10 distritos/ciudades exterior por n_actas:")
    top_dist = (
        ext_cab.group_by("ubigeoDistrito", "ubigeoNivel03")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .head(10)
    )
    for row in top_dist.iter_rows(named=True):
        lines.append(f"  {row['ubigeoDistrito']} {row['ubigeoNivel03']}: {row['n']:,}")
    lines.append("")

    # Top 5 partidos Presidencial exterior
    lines.append("Top 5 partidos Presidencial EXTERIOR (id=10):")
    pres_ext = ext_vot.join(
        ext_cab.filter(pl.col("idEleccion") == 10).select("idActa"),
        on="idActa",
        how="inner",
    ).filter(pl.col("idEleccion") == 10)
    top = (
        pres_ext.group_by("descripcion")
        .agg(pl.col("nvotos").sum().alias("total"))
        .sort("total", descending=True)
        .head(5)
    )
    for row in top.iter_rows(named=True):
        lines.append(f"  {row['total']:>10,}  {row['descripcion'][:60]}")
    lines.append("")
    return "\n".join(lines)


def comparativa_historico(cab: pl.DataFrame, outdir: Path) -> str:
    """Compara participación EG2026 vs EG2021 por depto (si histórico disponible)."""
    hist_file = HISTORICO_EG2021 / "eg2021_distritos_generales.parquet"
    if not hist_file.exists():
        return "(skipped: histórico EG2021 no ingestado)\n"

    hist = pl.read_parquet(hist_file)
    # EG2021: agrupado a nivel distrito (ubigeo), agregamos a depto
    # Primero necesitamos saber el mapeo RENIEC ubigeo → ONPE ubigeo
    # Los ubigeos en hist son los RENIEC (INEI-like). Los 2 primeros chars = depto.
    eg21 = (
        hist.with_columns(
            pl.col("ubigeo").cast(pl.String).str.zfill(6).str.slice(0, 2).alias("depto_code")
        )
        .group_by("depto_code")
        .agg(
            pl.col("ELECTORES_HABIL").sum().alias("hab_21"),
            pl.col("TOT_CIUDADANOS_VOTARON").sum().alias("votaron_21"),
        )
        .with_columns((pl.col("votaron_21") / pl.col("hab_21") * 100).alias("pct_21"))
    )

    # EG2026: de cab (Presidencial C)
    eg26 = (
        cab.filter((pl.col("idEleccion") == 10) & (pl.col("codigoEstadoActa") == "C"))
        .with_columns(pl.col("ubigeoDistrito").str.slice(0, 2).alias("depto_code_onpe"))
        .group_by("depto_code_onpe")
        .agg(
            pl.col("totalElectoresHabiles").sum().alias("hab_26"),
            pl.col("totalVotosEmitidos").sum().alias("votaron_26"),
        )
        .with_columns((pl.col("votaron_26") / pl.col("hab_26") * 100).alias("pct_26"))
    )

    # Nota: los depto codes ONPE y RENIEC son distintos (Lima ONPE=14, RENIEC=15).
    # La comparativa simple depto-por-depto no es directa sin mapeo.
    # Como proxy: juntamos by pct delta sin asumir mapping.
    out = (
        eg21.select("depto_code", "pct_21")
        .rename({"depto_code": "depto_reniec"})
        .join(eg26.select("depto_code_onpe", "pct_26"), how="cross")
        .with_columns((pl.col("pct_26") - pl.col("pct_21")).alias("delta_pct"))
    )

    out.write_parquet(
        outdir / "eg2021_vs_eg2026_participacion_pairwise.parquet", compression="zstd"
    )

    lines = ["=" * 70, "PARTICIPACIÓN EG2026 vs EG2021 (por depto)", "=" * 70, ""]
    lines.append("NOTA: ubigeos RENIEC (EG2021) ≠ ubigeos ONPE (EG2026) — mapeo deferred.")
    lines.append("")
    lines.append("Total país (agregado simple):")
    hab21_tot = float(eg21["hab_21"].sum())
    vot21_tot = float(eg21["votaron_21"].sum())
    hab26_tot = float(eg26["hab_26"].sum())
    vot26_tot = float(eg26["votaron_26"].sum())
    lines.append(
        f"  EG2021: {hab21_tot:>12,.0f} hábiles, {vot21_tot:>12,.0f} votaron → {vot21_tot / max(hab21_tot, 1) * 100:.2f}%"
    )
    lines.append(
        f"  EG2026: {hab26_tot:>12,.0f} hábiles, {vot26_tot:>12,.0f} votaron → {vot26_tot / max(hab26_tot, 1) * 100:.2f}% (solo C, drift temporal posible)"
    )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=str, default=str(DATA_DIR / "analytics"))
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    log.info("leyendo curated...")
    cab = pl.read_parquet(CURATED_CAB)
    vot = pl.read_parquet(CURATED_VOT)

    sections = [
        resumen_ejecutivo(cab),
        top_partidos(vot, cab, outdir),
        participacion_por_depto(cab, outdir),
        partidos_por_de(vot, cab, outdir),
        exterior_stats(cab, vot),
        comparativa_historico(cab, outdir),
    ]

    report = "\n".join(sections)
    report_path = outdir / "report.txt"
    report_path.write_text(report)
    log.info("escrito: %s (%.1f KB)", report_path, report_path.stat().st_size / 1024)

    # También print a stdout
    print(report)


if __name__ == "__main__":
    main()
