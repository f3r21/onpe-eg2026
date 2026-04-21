"""Descarga y parse del padrón electoral de RENIEC desde datosabiertos.gob.pe.

RENIEC publica trimestralmente dos datasets CSV con la población identificada
con DNI (dataset OPP-16):

- **OPP 1** (`.01.csv`): un row por (ubigeo × tipo_dni × sexo × edad) con Cantidad.
- **OPP 4** (`.02.csv`): un row por (ubigeo × sexo × edad × caducado) con Cantidad.

Para reconstruir el padrón electoral del trimestre usamos OPP 1 como fuente
primaria (Vigente+Caducado = universo elegible >=18 que matchea 27.36M oficial
del JNE con delta ~0.5% por lag de snapshot) y OPP 4 como complemento para
derivar la columna de vigencia.

Portal WAF (Huawei CloudWAF) bloquea curl/httpx plano; requerimos User-Agent
real + Referer al dataset page correspondiente.

Fuentes:
- https://www.datosabiertos.gob.pe/dataset/reniec-poblaci%C3%B3n-identificada-con-dni-registro-nacional-de-identificaci%C3%B3n-y-estado-civil
- https://www.datosabiertos.gob.pe/dataset/reniec-poblaci%C3%B3n-identificada-con-dni-por-estado-de-vigencia-registro-nacional-de
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import httpx
import polars as pl

log = logging.getLogger(__name__)

BASE_URL = "https://www.datosabiertos.gob.pe"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
}

REFERER_BASE = (
    BASE_URL + "/dataset/reniec-poblaci%C3%B3n-identificada-con-dni-"
    "registro-nacional-de-identificaci%C3%B3n-y-estado-civil"
)
REFERER_VIGENCIA = (
    BASE_URL + "/dataset/reniec-poblaci%C3%B3n-identificada-con-dni-por-estado-"
    "de-vigencia-registro-nacional-de"
)

# Trimestres disponibles — filenames observados en datosabiertos (Apr 2026).
# Formato nuevo (2023-Q4 en adelante): BD_DAbiertos_16_OPP_YYYY_MM.NN.csv
# donde NN = 01 (base) | 02 (vigencia).
TRIMESTRES: dict[str, dict[str, str]] = {
    "2026_03": {
        "base": "/sites/default/files/BD_DAbiertos_16_OPP_2026_03.01.csv",
        "vigencia": "/sites/default/files/BD_DAbiertos_16_OPP_2026_03.02.csv",
    },
    "2025_12": {
        "base": "/sites/default/files/BD_DAbiertos_16_OPP_2025_12.01.csv",
        "vigencia": "/sites/default/files/BD_DAbiertos_16_OPP_2025_12.02.csv",
    },
}

# Edad mínima para votar en el Perú (Constitución art. 30).
EDAD_MINIMA_ELECTORAL = 18

# Schema overrides para ambos CSV — las columnas de ubigeo y código país deben
# ser String para preservar el zero-padding, aunque "parezcan" numéricas.
_SCHEMA_OVERRIDES = {
    "UBIGEO_RENIEC": pl.String,
    "UBIGEO_INEI": pl.String,
    "CodPais": pl.String,
    "Cod_continente": pl.String,
    "Edad": pl.Int64,
    "Cantidad": pl.Int64,
}


@dataclass(frozen=True)
class PadronSummary:
    """Snapshot a nivel país de la agregación del padrón."""

    trimestre: str
    total_electores: int
    total_peru: int
    total_extranjero: int
    distritos_peru: int
    paises_extranjero: int
    delta_vs_base: int


def download_csv(url_path: str, out_file: Path, referer: str) -> int:
    """Descarga un CSV del portal datosabiertos con headers browser-like.

    Retorna bytes descargados. Lanza httpx.HTTPError si falla.
    """
    out_file.parent.mkdir(parents=True, exist_ok=True)
    url = BASE_URL + url_path
    headers = {**DEFAULT_HEADERS, "Referer": referer}

    log.info("descargando %s -> %s", url, out_file)
    total = 0
    with httpx.stream("GET", url, headers=headers, timeout=120.0, follow_redirects=True) as r:
        r.raise_for_status()
        with out_file.open("wb") as f:
            for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                f.write(chunk)
                total += len(chunk)
    log.info("descargado %s bytes", f"{total:,}")
    return total


def download_trimestre(trimestre: str, out_dir: Path) -> tuple[Path, Path]:
    """Descarga los dos CSVs (base + vigencia) del trimestre especificado.

    Retorna (path_base, path_vigencia).
    """
    if trimestre not in TRIMESTRES:
        raise ValueError(f"trimestre desconocido {trimestre!r}; disponibles: {sorted(TRIMESTRES)}")
    urls = TRIMESTRES[trimestre]
    base_path = out_dir / f"padron_{trimestre}_base.csv"
    vig_path = out_dir / f"padron_{trimestre}_vigencia.csv"

    download_csv(urls["base"], base_path, REFERER_BASE)
    download_csv(urls["vigencia"], vig_path, REFERER_VIGENCIA)
    return base_path, vig_path


def read_base(csv_path: Path) -> pl.LazyFrame:
    """Lee el CSV OPP 1 (sin vigencia). Schema: ubigeo × tipo_dni × sexo × edad."""
    return pl.scan_csv(csv_path, schema_overrides=_SCHEMA_OVERRIDES)


def read_vigencia(csv_path: Path) -> pl.LazyFrame:
    """Lee el CSV OPP 4 (con vigencia). Schema: ubigeo × sexo × edad × caducado."""
    return pl.scan_csv(csv_path, schema_overrides=_SCHEMA_OVERRIDES)


def _edad_band_expr() -> pl.Expr:
    return (
        pl.when(pl.col("Edad") < EDAD_MINIMA_ELECTORAL)
        .then(pl.lit("<18"))
        .when(pl.col("Edad") < 26)
        .then(pl.lit("18_25"))
        .when(pl.col("Edad") < 36)
        .then(pl.lit("26_35"))
        .when(pl.col("Edad") < 46)
        .then(pl.lit("36_45"))
        .when(pl.col("Edad") < 61)
        .then(pl.lit("46_60"))
        .otherwise(pl.lit("61_plus"))
    )


def aggregate_base_by_ubigeo(lazy_base: pl.LazyFrame) -> pl.DataFrame:
    """Agrega el padrón electoral por ubigeo (Perú) y país (Extranjero).

    Filtra Edad >= 18 (mínimo constitucional para voto).
    Suma Vigente+Caducado porque el padrón electoral incluye ambos
    (un caducado sigue siendo elector, el DNI físico solo vence).
    """
    filtered = lazy_base.filter(pl.col("Edad") >= EDAD_MINIMA_ELECTORAL).with_columns(
        _edad_band_expr().alias("rango_etario")
    )

    peru_lazy = (
        filtered.filter(pl.col("Residencia") == "Nacional")
        .group_by(
            [
                "UBIGEO_RENIEC",
                "UBIGEO_INEI",
                "Departamento",
                "Provincia",
                "Distrito",
            ]
        )
        .agg(_agg_columns())
        .with_columns(
            [
                pl.lit("Nacional").alias("residencia"),
                pl.lit("").alias("pais_codigo"),
                pl.lit("Perú").alias("pais_nombre"),
            ]
        )
        .rename(
            {
                "UBIGEO_RENIEC": "ubigeo_reniec",
                "UBIGEO_INEI": "ubigeo_inei",
                "Departamento": "departamento",
                "Provincia": "provincia",
                "Distrito": "distrito",
            }
        )
    )

    # Extranjero: agrupar por país (no hay ubigeo per se).
    extranjero_lazy = (
        filtered.filter(pl.col("Residencia") == "Extranjero")
        .group_by(["CodPais", "Pais"])
        .agg(_agg_columns())
        .with_columns(
            [
                pl.lit("Extranjero").alias("residencia"),
                pl.lit("").alias("ubigeo_reniec"),
                pl.lit("").alias("ubigeo_inei"),
                pl.lit("").alias("departamento"),
                pl.lit("").alias("provincia"),
                pl.col("Pais").alias("distrito"),
            ]
        )
        .rename({"CodPais": "pais_codigo", "Pais": "pais_nombre"})
    )

    cols = [
        "ubigeo_reniec",
        "ubigeo_inei",
        "residencia",
        "pais_codigo",
        "pais_nombre",
        "departamento",
        "provincia",
        "distrito",
        "total_electores",
        "hombres",
        "mujeres",
        "dni_electronico",
        "dni_convencional",
        "rango_18_25",
        "rango_26_35",
        "rango_36_45",
        "rango_46_60",
        "rango_61_plus",
    ]
    combined = pl.concat(
        [peru_lazy.select(cols), extranjero_lazy.select(cols)],
        how="vertical",
    )
    return combined.collect()


def _agg_columns() -> list[pl.Expr]:
    """Expresiones de agregación comunes para Perú + Extranjero."""
    return [
        pl.col("Cantidad").sum().alias("total_electores"),
        pl.col("Cantidad").filter(pl.col("Sexo") == "Hombre").sum().alias("hombres"),
        pl.col("Cantidad").filter(pl.col("Sexo") == "Mujer").sum().alias("mujeres"),
        pl.col("Cantidad")
        .filter(pl.col("TipoDNI") == "DNI electrónico")
        .sum()
        .alias("dni_electronico"),
        pl.col("Cantidad")
        .filter(pl.col("TipoDNI") == "DNI convencional")
        .sum()
        .alias("dni_convencional"),
        pl.col("Cantidad").filter(pl.col("rango_etario") == "18_25").sum().alias("rango_18_25"),
        pl.col("Cantidad").filter(pl.col("rango_etario") == "26_35").sum().alias("rango_26_35"),
        pl.col("Cantidad").filter(pl.col("rango_etario") == "36_45").sum().alias("rango_36_45"),
        pl.col("Cantidad").filter(pl.col("rango_etario") == "46_60").sum().alias("rango_46_60"),
        pl.col("Cantidad").filter(pl.col("rango_etario") == "61_plus").sum().alias("rango_61_plus"),
    ]


def aggregate_vigencia_by_ubigeo(lazy_vig: pl.LazyFrame) -> pl.DataFrame:
    """Agrega el desglose de vigencia (Vigente/Caducado) por ubigeo."""
    filtered = lazy_vig.filter(pl.col("Edad") >= EDAD_MINIMA_ELECTORAL)

    peru = (
        filtered.filter(pl.col("Residencia") == "Nacional")
        .group_by(["UBIGEO_RENIEC"])
        .agg(
            [
                pl.col("Cantidad").filter(pl.col("Caducado") == "Vigente").sum().alias("vigentes"),
                pl.col("Cantidad")
                .filter(pl.col("Caducado") == "Caducado")
                .sum()
                .alias("caducados"),
            ]
        )
        .rename({"UBIGEO_RENIEC": "ubigeo_reniec"})
    )

    extranjero = (
        filtered.filter(pl.col("Residencia") == "Extranjero")
        .group_by(["CodPais"])
        .agg(
            [
                pl.col("Cantidad").filter(pl.col("Caducado") == "Vigente").sum().alias("vigentes"),
                pl.col("Cantidad")
                .filter(pl.col("Caducado") == "Caducado")
                .sum()
                .alias("caducados"),
            ]
        )
        .rename({"CodPais": "pais_codigo"})
        .with_columns(pl.lit("").alias("ubigeo_reniec"))
        .select(["ubigeo_reniec", "pais_codigo", "vigentes", "caducados"])
    )

    peru_final = peru.with_columns(pl.lit("").alias("pais_codigo")).select(
        ["ubigeo_reniec", "pais_codigo", "vigentes", "caducados"]
    )
    return pl.concat([peru_final, extranjero], how="vertical").collect()


def merge_base_with_vigencia(base_df: pl.DataFrame, vig_df: pl.DataFrame) -> pl.DataFrame:
    """Une la tabla base con el desglose de vigencia por ubigeo_reniec+pais_codigo.

    `left join + fill_null(0)` hace `vigentes=0, caducados=0` indistinguible de
    "distrito sin cobertura en OPP-4". Loggeamos el count para trazabilidad.
    """
    merged = base_df.join(
        vig_df,
        on=["ubigeo_reniec", "pais_codigo"],
        how="left",
    )
    sin_vigencia = merged.filter(pl.col("vigentes").is_null()).height
    if sin_vigencia > 0:
        log.warning(
            "%d distritos/países sin cobertura en OPP-4 (vigentes=0/caducados=0 "
            "no es necesariamente real — podría ser `left join` sin match)",
            sin_vigencia,
        )
    return merged.with_columns(
        [
            pl.col("vigentes").fill_null(0),
            pl.col("caducados").fill_null(0),
        ]
    )


def summarize(df: pl.DataFrame, trimestre: str, base_total: int) -> PadronSummary:
    """Resumen ejecutivo del padrón agregado."""
    peru_total = int(df.filter(pl.col("residencia") == "Nacional")["total_electores"].sum())
    ext_total = int(df.filter(pl.col("residencia") == "Extranjero")["total_electores"].sum())
    total = peru_total + ext_total
    return PadronSummary(
        trimestre=trimestre,
        total_electores=total,
        total_peru=peru_total,
        total_extranjero=ext_total,
        distritos_peru=df.filter(pl.col("residencia") == "Nacional").shape[0],
        paises_extranjero=df.filter(pl.col("residencia") == "Extranjero").shape[0],
        delta_vs_base=total - base_total,
    )
