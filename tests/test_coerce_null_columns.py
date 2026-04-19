"""Tests de _coerce_null_columns — promoción de Null a dtype correcto."""

from __future__ import annotations

import polars as pl

from onpe.actas import _coerce_null_columns


def test_coerce_null_a_string_default():
    """Columna pl.Null sin schema → cast a String (fallback seguro)."""
    df = pl.DataFrame({"x": [None, None], "y": [1, 2]}, schema={"x": pl.Null, "y": pl.Int64})
    result = _coerce_null_columns(df)
    assert result.schema["x"] == pl.String
    assert result.schema["y"] == pl.Int64


def test_coerce_null_a_int_via_schema():
    """Schema explícito {col: Int64} → cast a Int64."""
    df = pl.DataFrame({"x": [None, None]}, schema={"x": pl.Null})
    result = _coerce_null_columns(df, {"x": pl.Int64})
    assert result.schema["x"] == pl.Int64


def test_coerce_null_a_float_via_schema():
    """Float64 también se respeta."""
    df = pl.DataFrame({"p": [None, None]}, schema={"p": pl.Null})
    result = _coerce_null_columns(df, {"p": pl.Float64})
    assert result.schema["p"] == pl.Float64


def test_coerce_preserve_no_null_columns():
    """Columnas con dtype concreto no se tocan."""
    df = pl.DataFrame({"x": [1, 2], "s": ["a", "b"]}, schema={"x": pl.Int64, "s": pl.String})
    result = _coerce_null_columns(df, {"x": pl.Int64})
    assert result.equals(df)
    assert result.schema == df.schema


def test_coerce_sin_columnas_null_es_identidad():
    """Si no hay columnas Null, devuelve el df tal cual."""
    df = pl.DataFrame({"x": [1, 2]})
    result = _coerce_null_columns(df, {})
    assert result.equals(df)


def test_coerce_concat_diagonal_relaxed_int_y_null():
    """Caso real: concat de 2 chunks donde uno tiene Int64 y otro Null→Int64."""
    chunk_ok = pl.DataFrame({"idActa": [1, 2], "totalVotos": [100, 200]})
    chunk_con_null = pl.DataFrame(
        {"idActa": [3, 4], "totalVotos": [None, None]},
        schema={"idActa": pl.Int64, "totalVotos": pl.Null},
    )
    # Sin coerce, diagonal_relaxed promovería totalVotos a String.
    casted = _coerce_null_columns(chunk_con_null, {"totalVotos": pl.Int64})
    concat = pl.concat([chunk_ok, casted], how="diagonal_relaxed")
    assert concat.schema["totalVotos"] == pl.Int64
    assert concat.height == 4


def test_coerce_multiples_columnas_null():
    """Varias columnas Null se castean en una sola pasada."""
    df = pl.DataFrame(
        {"a": [None, None], "b": [None, None], "c": [1, 2]},
        schema={"a": pl.Null, "b": pl.Null, "c": pl.Int64},
    )
    result = _coerce_null_columns(df, {"a": pl.Int64, "b": pl.Float64})
    assert result.schema["a"] == pl.Int64
    assert result.schema["b"] == pl.Float64
    assert result.schema["c"] == pl.Int64
