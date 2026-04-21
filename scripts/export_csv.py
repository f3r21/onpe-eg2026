"""Exportador CSV con filtros para consumers no-Python.

Permite extraer subconjuntos del dataset curated como CSV plano con columnas
amigables para periodistas, analistas no-Python (Excel/Stata/SPSS/R), o para
análisis focalizados (una región, una elección, un partido).

Uso típico:
    # Presidencial completo, todo el Perú:
    uv run python scripts/export_csv.py --eleccion 10 --output exports/pres.csv

    # Diputados por distrito electoral (Lima Metropolitana = DE 15):
    uv run python scripts/export_csv.py --eleccion 13 --de 15 --output exports/dip_lima.csv

    # Voto exterior presidencial:
    uv run python scripts/export_csv.py --eleccion 10 --de 27 --output exports/pres_exterior.csv

    # Solo partido específico (ccodigo):
    uv run python scripts/export_csv.py --partido 00000014 --output exports/partido14.csv

    # Formato agregado por distrito (resumen):
    uv run python scripts/export_csv.py --eleccion 10 --formato resumen-distrito \\
        --output exports/pres_por_distrito.csv

    # Comprimido:
    uv run python scripts/export_csv.py --eleccion 10 --output exports/pres.csv.gz --compression gzip
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import polars as pl

from onpe.storage import DATA_DIR

log = logging.getLogger("export_csv")

CURATED_DIR = DATA_DIR / "curated"

# Mapa legible de elecciones.
ELECCION_NOMBRES: dict[int, str] = {
    10: "Presidencial",
    12: "Parlamento Andino",
    13: "Diputados",
    14: "Senadores Regional",
    15: "Senadores Nacional",
}

FORMATOS = ("mesa-partido", "resumen-distrito", "cabecera")


def _parse_int_list(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    return [int(x) for x in raw.split(",") if x.strip()]


def _parse_str_list(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    return [x.strip() for x in raw.split(",") if x.strip()]


def build_filter_expressions(
    *,
    elecciones: list[int] | None,
    des: list[int] | None,
    deptos: list[str] | None,
    provincias: list[str] | None,
    distritos: list[str] | None,
    partidos: list[str] | None,
    ambito: int | None,
    estado: str | None,
) -> list[pl.Expr]:
    """Construye las expresiones Polars de filtro a partir de los args."""
    exprs: list[pl.Expr] = []
    if elecciones:
        exprs.append(pl.col("idEleccion").is_in(elecciones))
    if des:
        exprs.append(pl.col("idDistritoElectoral").is_in(des))
    if deptos:
        exprs.append(pl.col("ubigeoDepartamento").is_in(deptos))
    if provincias:
        exprs.append(pl.col("ubigeoProvincia").is_in(provincias))
    if distritos:
        exprs.append(pl.col("ubigeoDistrito").is_in(distritos))
    if ambito:
        exprs.append(pl.col("idAmbitoGeografico") == ambito)
    if estado:
        exprs.append(pl.col("codigoEstadoActa") == estado)
    if partidos:
        exprs.append(pl.col("ccodigo").is_in(partidos))
    return exprs


def export_mesa_partido(filters: list[pl.Expr]) -> pl.DataFrame:
    """Formato long: un row por (acta × partido) con contexto de cabecera.

    Salida ~9M filas si no se filtra (1 elección filtra a ~1.85M).
    """
    votos = pl.scan_parquet(CURATED_DIR / "actas_votos.parquet")
    cab = pl.scan_parquet(CURATED_DIR / "actas_cabecera.parquet").select(
        [
            "idActa",
            "codigoMesa",
            "nombreLocalVotacion",
            "totalElectoresHabiles",
            "totalVotosEmitidos",
            "totalVotosValidos",
            "totalAsistentes",
            "porcentajeParticipacionCiudadana",
            "codigoEstadoActa",
            "ubigeoDistrito",
            "ubigeoDepartamento",
            "ubigeoProvincia",
            "nombreDistrito",
            "idAmbitoGeografico",
            "idDistritoElectoral",
        ]
    )
    joined = votos.join(cab, on="idActa", how="inner", suffix="_cab").drop(
        "ubigeoDistrito_cab", strict=False
    )
    for f in filters:
        joined = joined.filter(f)
    return joined.select(
        [
            "idActa",
            "codigoMesa",
            "idEleccion",
            "idDistritoElectoral",
            "idAmbitoGeografico",
            "ubigeoDepartamento",
            "ubigeoProvincia",
            "ubigeoDistrito",
            "nombreDistrito",
            "nombreLocalVotacion",
            "codigoEstadoActa",
            "totalElectoresHabiles",
            "totalVotosEmitidos",
            "totalVotosValidos",
            "totalAsistentes",
            "porcentajeParticipacionCiudadana",
            pl.col("descripcion").alias("partido"),
            "ccodigo",
            "es_especial",
            "nvotos",
            "nporcentajeVotosValidos",
            "nporcentajeVotosEmitidos",
        ]
    ).collect()


def export_resumen_distrito(filters: list[pl.Expr]) -> pl.DataFrame:
    """Formato agregado: un row por (distrito × elección × partido) con sumas."""
    base = export_mesa_partido(filters).lazy()
    return (
        base.group_by(
            [
                "idEleccion",
                "idDistritoElectoral",
                "ubigeoDepartamento",
                "ubigeoProvincia",
                "ubigeoDistrito",
                "nombreDistrito",
                "partido",
                "ccodigo",
                "es_especial",
            ]
        )
        .agg(
            [
                pl.col("nvotos").sum().alias("nvotos"),
                pl.col("totalElectoresHabiles").sum().alias("padron_distrito"),
                pl.col("totalVotosEmitidos").sum().alias("emitidos_distrito"),
                pl.col("totalVotosValidos").sum().alias("validos_distrito"),
                pl.col("idActa").n_unique().alias("actas"),
            ]
        )
        .sort(
            ["idEleccion", "ubigeoDistrito", pl.col("nvotos").abs()],
            descending=[False, False, True],
        )
        .collect()
    )


def export_cabecera(filters: list[pl.Expr]) -> pl.DataFrame:
    """Formato compacto: una fila por acta, sin detalle de votos."""
    cab = pl.scan_parquet(CURATED_DIR / "actas_cabecera.parquet")
    for f in filters:
        cab = cab.filter(f)
    return cab.collect()


def write_csv(df: pl.DataFrame, out: Path, compression: str) -> int:
    """Escribe el DataFrame como CSV. Retorna bytes escritos."""
    out.parent.mkdir(parents=True, exist_ok=True)
    if compression == "gzip":
        import gzip

        csv_text = df.write_csv(None)  # to string
        data = csv_text.encode("utf-8")
        with gzip.open(out, "wb") as f:
            f.write(data)
        return out.stat().st_size
    df.write_csv(out)
    return out.stat().st_size


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--eleccion",
        type=str,
        default=None,
        help="idEleccion 10/12/13/14/15, coma-separados. Default: todas.",
    )
    parser.add_argument(
        "--de", type=str, default=None, help="idDistritoElectoral 1..27 coma-separados"
    )
    parser.add_argument("--depto", type=str, default=None, help="ubigeoDepartamento (ej. 140000)")
    parser.add_argument("--provincia", type=str, default=None, help="ubigeoProvincia (ej. 140100)")
    parser.add_argument("--distrito", type=str, default=None, help="ubigeoDistrito (ej. 140101)")
    parser.add_argument("--partido", type=str, default=None, help="ccodigo (ej. 00000014)")
    parser.add_argument(
        "--ambito", type=int, choices=[1, 2], default=None, help="1=Perú, 2=Exterior"
    )
    parser.add_argument(
        "--estado",
        type=str,
        choices=["C", "E", "P", "N"],
        default=None,
        help="codigoEstadoActa",
    )
    parser.add_argument(
        "--formato",
        choices=FORMATOS,
        default="mesa-partido",
        help="mesa-partido (long), resumen-distrito (agregado), cabecera (totales por acta)",
    )
    parser.add_argument("--output", type=Path, required=True, help="CSV de salida")
    parser.add_argument(
        "--compression",
        choices=["none", "gzip"],
        default="none",
        help="gzip si el archivo termina en .gz",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    filters = build_filter_expressions(
        elecciones=_parse_int_list(args.eleccion),
        des=_parse_int_list(args.de),
        deptos=_parse_str_list(args.depto),
        provincias=_parse_str_list(args.provincia),
        distritos=_parse_str_list(args.distrito),
        partidos=_parse_str_list(args.partido),
        ambito=args.ambito,
        estado=args.estado,
    )
    log.info("filtros: %d expresiones", len(filters))

    if args.formato == "mesa-partido":
        df = export_mesa_partido(filters)
    elif args.formato == "resumen-distrito":
        df = export_resumen_distrito(filters)
    else:
        df = export_cabecera(filters)

    log.info("resultado: %d filas × %d columnas", df.shape[0], df.shape[1])
    if df.is_empty():
        log.warning("resultado vacío — revisa los filtros")

    bytes_written = write_csv(df, args.output, args.compression)
    log.info("escrito %s (%s bytes)", args.output, f"{bytes_written:,}")

    print()
    print(f"formato        : {args.formato}")
    print(f"filas          : {df.shape[0]:,}")
    print(f"columnas       : {df.shape[1]}")
    print(f"tamaño archivo : {bytes_written / 1024:,.1f} KB")
    print(f"output         : {args.output}")


if __name__ == "__main__":
    main()
