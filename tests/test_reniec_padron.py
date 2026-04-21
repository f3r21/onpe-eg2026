"""Tests de onpe.reniec_padron: parse CSV + filtro edad + agregaciones."""

from __future__ import annotations

from pathlib import Path

import httpx
import polars as pl
import pytest

from onpe.reniec_padron import (
    EDAD_MINIMA_ELECTORAL,
    aggregate_base_by_ubigeo,
    aggregate_vigencia_by_ubigeo,
    merge_base_with_vigencia,
    read_base,
    read_vigencia,
    summarize,
)

# ──────────────────────────────────────────────────────────────────────
#  Fixtures de CSVs sintéticos — 2 ubigeos Perú + 1 país extranjero.
# ──────────────────────────────────────────────────────────────────────

_BASE_CSV = (
    "Residencia,Cod_continente,Continente,CodPais,Pais,UBIGEO_RENIEC,UBIGEO_INEI,"
    "Departamento,Provincia,Distrito,TipoDNI,Sexo,Edad,Cantidad\n"
    # Lima 140101: 3 hombres 18-25, 5 mujeres 26-35, 1 hombre 17 (excluir)
    "Nacional,92,América,9233,Perú,140101,150101,Lima,Lima,Lima,"
    "DNI electrónico,Hombre,20,3\n"
    "Nacional,92,América,9233,Perú,140101,150101,Lima,Lima,Lima,"
    "DNI convencional,Mujer,30,5\n"
    "Nacional,92,América,9233,Perú,140101,150101,Lima,Lima,Lima,"
    "DNI convencional,Hombre,17,1\n"
    # Amazonas 010101: 2 hombres 61+
    "Nacional,92,América,9233,Perú,010101,010101,Amazonas,Chachapoyas,Chachapoyas,"
    "DNI convencional,Hombre,70,2\n"
    # Extranjero Argentina 9301: 4 mujeres 36-45
    "Extranjero,92,América,9301,Argentina, , , , , ,"
    "DNI electrónico,Mujer,40,4\n"
    # Menor de edad: debe filtrarse
    "Nacional,92,América,9233,Perú,140101,150101,Lima,Lima,Lima,"
    "DNI convencional,Mujer,15,9\n"
)

_VIGENCIA_CSV = (
    "Residencia,Cod_continente,Continente,CodPais,Pais,UBIGEO_RENIEC,UBIGEO_INEI,"
    "Departamento,Provincia,Distrito,Sexo,Edad,Caducado,Cantidad\n"
    # Lima 140101: 6 vigente, 2 caducado (de los 8 totales >= 18 del base)
    "Nacional,92,América,9233,Perú,140101,150101,Lima,Lima,Lima,Hombre,20,Vigente,3\n"
    "Nacional,92,América,9233,Perú,140101,150101,Lima,Lima,Lima,Mujer,30,Vigente,3\n"
    "Nacional,92,América,9233,Perú,140101,150101,Lima,Lima,Lima,Mujer,30,Caducado,2\n"
    # Amazonas: 2 vigente
    "Nacional,92,América,9233,Perú,010101,010101,Amazonas,Chachapoyas,Chachapoyas,"
    "Hombre,70,Vigente,2\n"
    # Argentina: 4 vigente
    "Extranjero,92,América,9301,Argentina, , , , , ,Mujer,40,Vigente,4\n"
    # Ruido <18 (debe filtrarse)
    "Nacional,92,América,9233,Perú,140101,150101,Lima,Lima,Lima,Mujer,10,Vigente,99\n"
)


@pytest.fixture
def base_csv(tmp_path: Path) -> Path:
    p = tmp_path / "base.csv"
    p.write_text(_BASE_CSV, encoding="utf-8")
    return p


@pytest.fixture
def vigencia_csv(tmp_path: Path) -> Path:
    p = tmp_path / "vigencia.csv"
    p.write_text(_VIGENCIA_CSV, encoding="utf-8")
    return p


# ──────────────────────────────────────────────────────────────────────
#  read_base / read_vigencia — encoding + schema overrides
# ──────────────────────────────────────────────────────────────────────


def test_read_base_preserves_ubigeo_zero_padding(base_csv: Path):
    """UBIGEO_RENIEC debe ser String para mantener el leading zero."""
    df = read_base(base_csv).collect()
    assert df.schema["UBIGEO_RENIEC"] == pl.String
    assert df.schema["UBIGEO_INEI"] == pl.String
    # Amazonas comienza con 0 — si Polars lo inferiera como Int, se perdería.
    assert "010101" in df["UBIGEO_RENIEC"].to_list()


def test_read_vigencia_has_caducado_column(vigencia_csv: Path):
    df = read_vigencia(vigencia_csv).collect()
    assert "Caducado" in df.columns
    assert set(df["Caducado"].unique().to_list()) == {"Vigente", "Caducado"}


# ──────────────────────────────────────────────────────────────────────
#  aggregate_base_by_ubigeo — filtro edad + rollup correcto
# ──────────────────────────────────────────────────────────────────────


def test_aggregate_base_filtra_menores_edad(base_csv: Path):
    """Electores con Edad < 18 NO deben contar en total_electores."""
    df = aggregate_base_by_ubigeo(read_base(base_csv))
    lima = df.filter(pl.col("ubigeo_reniec") == "140101").row(0, named=True)
    # Expected: 3 (hombre 20) + 5 (mujer 30) = 8. Ignora 1 (17) y 9 (15).
    assert lima["total_electores"] == 8
    assert lima["hombres"] == 3
    assert lima["mujeres"] == 5


def test_aggregate_base_suma_bandas_etarias(base_csv: Path):
    """Suma(rangos) == total_electores por distrito."""
    df = aggregate_base_by_ubigeo(read_base(base_csv))
    for row in df.iter_rows(named=True):
        bands = (
            row["rango_18_25"]
            + row["rango_26_35"]
            + row["rango_36_45"]
            + row["rango_46_60"]
            + row["rango_61_plus"]
        )
        assert bands == row["total_electores"], (
            f"row {row} bands={bands} != total={row['total_electores']}"
        )


def test_aggregate_base_separa_peru_extranjero(base_csv: Path):
    df = aggregate_base_by_ubigeo(read_base(base_csv))
    peru = df.filter(pl.col("residencia") == "Nacional")
    ext = df.filter(pl.col("residencia") == "Extranjero")
    assert peru.shape[0] == 2  # Lima + Amazonas
    assert ext.shape[0] == 1  # Argentina
    # Extranjero tiene pais_codigo poblado y ubigeo_reniec vacío
    arg = ext.row(0, named=True)
    assert arg["pais_codigo"] == "9301"
    assert arg["pais_nombre"] == "Argentina"
    assert arg["ubigeo_reniec"] == ""
    assert arg["total_electores"] == 4


def test_aggregate_base_rollup_tipo_dni(base_csv: Path):
    """dni_electronico + dni_convencional = total_electores por distrito."""
    df = aggregate_base_by_ubigeo(read_base(base_csv))
    for row in df.iter_rows(named=True):
        assert row["dni_electronico"] + row["dni_convencional"] == row["total_electores"]


# ──────────────────────────────────────────────────────────────────────
#  aggregate_vigencia_by_ubigeo
# ──────────────────────────────────────────────────────────────────────


def test_aggregate_vigencia_respeta_filtro_edad(vigencia_csv: Path):
    """Suma vigente+caducado debe matchear el total de edad>=18 del CSV."""
    df = aggregate_vigencia_by_ubigeo(read_vigencia(vigencia_csv))
    lima = df.filter(pl.col("ubigeo_reniec") == "140101").row(0, named=True)
    # Lima >= 18: 3 + 3 + 2 = 8 (excluye el row Mujer 10 Vigente 99)
    assert lima["vigentes"] + lima["caducados"] == 8
    assert lima["vigentes"] == 6
    assert lima["caducados"] == 2


# ──────────────────────────────────────────────────────────────────────
#  merge_base_with_vigencia — left join preserva todos los rows base
# ──────────────────────────────────────────────────────────────────────


def test_merge_base_with_vigencia_join_ubigeo(base_csv: Path, vigencia_csv: Path):
    base_df = aggregate_base_by_ubigeo(read_base(base_csv))
    vig_df = aggregate_vigencia_by_ubigeo(read_vigencia(vigencia_csv))
    merged = merge_base_with_vigencia(base_df, vig_df)

    # Mismo numero de filas que base (left join).
    assert merged.shape[0] == base_df.shape[0]
    # Columnas de vigencia agregadas.
    assert "vigentes" in merged.columns
    assert "caducados" in merged.columns
    # Lima matchea.
    lima = merged.filter(pl.col("ubigeo_reniec") == "140101").row(0, named=True)
    assert lima["total_electores"] == 8
    assert lima["vigentes"] == 6
    assert lima["caducados"] == 2


def test_merge_preserva_extranjero_via_pais_codigo(base_csv: Path, vigencia_csv: Path):
    base_df = aggregate_base_by_ubigeo(read_base(base_csv))
    vig_df = aggregate_vigencia_by_ubigeo(read_vigencia(vigencia_csv))
    merged = merge_base_with_vigencia(base_df, vig_df)
    arg = merged.filter(pl.col("pais_codigo") == "9301").row(0, named=True)
    assert arg["total_electores"] == 4
    assert arg["vigentes"] == 4
    assert arg["caducados"] == 0


# ──────────────────────────────────────────────────────────────────────
#  summarize — snapshot ejecutivo
# ──────────────────────────────────────────────────────────────────────


def test_summarize_totals(base_csv: Path, vigencia_csv: Path):
    base_df = aggregate_base_by_ubigeo(read_base(base_csv))
    vig_df = aggregate_vigencia_by_ubigeo(read_vigencia(vigencia_csv))
    merged = merge_base_with_vigencia(base_df, vig_df)
    summary = summarize(merged, trimestre="2026_03", base_total=14)
    # 8 Lima + 2 Amazonas = 10 peru, 4 argentina
    assert summary.total_peru == 10
    assert summary.total_extranjero == 4
    assert summary.total_electores == 14
    assert summary.distritos_peru == 2
    assert summary.paises_extranjero == 1
    assert summary.trimestre == "2026_03"


# ──────────────────────────────────────────────────────────────────────
#  download_csv — MockTransport para evitar red real + verificar headers
# ──────────────────────────────────────────────────────────────────────


def test_download_csv_usa_browser_headers(tmp_path: Path, monkeypatch):
    """download_csv debe inyectar User-Agent real + Referer (CloudWAF)."""
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["user_agent"] = request.headers.get("user-agent", "")
        captured["referer"] = request.headers.get("referer", "")
        captured["url"] = str(request.url)
        return httpx.Response(200, content=b"col1,col2\n1,2\n")

    # Monkeypatch httpx.stream con un transport mockeado para evitar red real.
    import onpe.reniec_padron as rp

    def mock_stream(method, url, **kw):
        return httpx.Client(transport=httpx.MockTransport(handler)).stream(method, url, **kw)

    monkeypatch.setattr(rp.httpx, "stream", mock_stream)

    out = tmp_path / "mini.csv"
    rp.download_csv("/test/path.csv", out, referer="https://ref.test/dataset")

    assert out.exists()
    assert "Chrome" in captured["user_agent"]
    assert "Mozilla" in captured["user_agent"]
    assert captured["referer"] == "https://ref.test/dataset"
    assert captured["url"].endswith("/test/path.csv")


def test_edad_minima_is_18():
    """Regresión: no cambiar accidentalmente la constante."""
    assert EDAD_MINIMA_ELECTORAL == 18
