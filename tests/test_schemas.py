"""Tests de schema validation fail-fast."""

from __future__ import annotations

import polars as pl
import pytest

from onpe.schemas import SCHEMAS, SchemaDriftError, validate_chunk


def test_validate_ok_en_cabecera_limpia(sample_cabecera_df: pl.DataFrame):
    """DataFrame bien formado no produce violaciones."""
    violations = validate_chunk(sample_cabecera_df, "actas_cabecera")
    assert violations == []


def test_validate_detecta_type_drift(sample_cabecera_df: pl.DataFrame):
    """Cambiar idActa de Int64 a String dispara SchemaDriftError."""
    df = sample_cabecera_df.with_columns(pl.col("idActa").cast(pl.String))
    with pytest.raises(SchemaDriftError) as exc:
        validate_chunk(df, "actas_cabecera", strict=True)
    assert "idActa" in str(exc.value)
    assert "mismatch" in str(exc.value)


def test_validate_non_strict_no_lanza(sample_cabecera_df: pl.DataFrame):
    """strict=False devuelve lista de violaciones sin raise."""
    df = sample_cabecera_df.with_columns(pl.col("idActa").cast(pl.String))
    violations = validate_chunk(df, "actas_cabecera", strict=False)
    assert len(violations) == 1
    assert "idActa" in violations[0]


def test_validate_detecta_columna_faltante(sample_cabecera_df: pl.DataFrame):
    """Droppear una columna esperada dispara missing violation."""
    df = sample_cabecera_df.drop("codigoMesa")
    with pytest.raises(SchemaDriftError) as exc:
        validate_chunk(df, "actas_cabecera", strict=True)
    assert any("codigoMesa" in v and "missing" in v for v in exc.value.violations)


def test_validate_columna_extra_solo_warning(sample_cabecera_df: pl.DataFrame, caplog):
    """Columna nueva no esperada: WARNING, no error."""
    import logging

    caplog.set_level(logging.WARNING, logger="onpe.schemas")
    df = sample_cabecera_df.with_columns(pl.lit("nuevo").alias("campoNuevoONPE"))
    violations = validate_chunk(df, "actas_cabecera", strict=True)
    assert violations == []  # no raise, no violaciones bloqueantes
    assert any("campoNuevoONPE" in rec.message for rec in caplog.records)


def test_validate_allowed_null_cols_evita_mismatch():
    """Columnas en allowed_null_cols aceptan Null sin triggerar mismatch."""
    # DF con todas las cols esperadas + cargo forzado a Null.
    full = pl.DataFrame(
        {col: [None] for col in SCHEMAS["actas_votos"]},
        schema={col: dt for col, dt in SCHEMAS["actas_votos"].items()},
    ).with_columns(pl.col("cargo").cast(pl.Null))

    # Sin allowed_null_cols: cargo Null != String → mismatch.
    violations_default = validate_chunk(full, "actas_votos", strict=False)
    assert any("'cargo'" in v and "mismatch" in v for v in violations_default)

    # Con allowed_null_cols=["cargo"]: se exceptúa.
    violations_allowed = validate_chunk(
        full, "actas_votos", strict=False, allowed_null_cols=["cargo"]
    )
    assert not any("'cargo'" in v and "mismatch" in v for v in violations_allowed)


def test_validate_tabla_desconocida_raises_keyerror():
    df = pl.DataFrame({"x": [1]})
    with pytest.raises(KeyError):
        validate_chunk(df, "tabla_inexistente")


def test_validate_cuatro_tablas_del_curated_pasan():
    """Smoke: el curated actual debe validar sin drift (escenario golden-path)."""
    from pathlib import Path

    curated = Path("data/curated")
    for table in ("actas_cabecera", "actas_votos", "actas_linea_tiempo", "actas_archivos"):
        path = curated / f"{table}.parquet"
        if not path.exists():
            pytest.skip(f"{path} no existe — correr pipeline antes")
        df = pl.read_parquet(path, n_rows=50)
        # El curated puede tener String en nporcentaje* por diagonal_relaxed;
        # schemas.py apunta a la realidad de los CHUNKS (Float64), así que
        # el curated tiene String divergente. Marcar como non-strict en este smoke.
        violations = validate_chunk(df, table, strict=False)
        if table == "actas_votos":
            # Solo esperamos nporcentaje* divergentes en el curated legacy.
            unexpected = [v for v in violations if "nporcentaje" not in v]
            assert not unexpected, f"{table}: violaciones inesperadas: {unexpected}"
        else:
            assert violations == [], f"{table}: {violations}"
