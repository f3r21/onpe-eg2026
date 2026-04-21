"""Tests de scripts/export_csv: construcción de filtros + export formats."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import polars as pl


def _load_module():
    path = Path(__file__).resolve().parent.parent / "scripts" / "export_csv.py"
    spec = importlib.util.spec_from_file_location("export_csv", path)
    mod = importlib.util.module_from_spec(spec)
    sys.argv = ["export_csv"]
    spec.loader.exec_module(mod)
    return mod


def test_parse_int_list():
    mod = _load_module()
    assert mod._parse_int_list(None) is None
    assert mod._parse_int_list("") is None
    assert mod._parse_int_list("10") == [10]
    assert mod._parse_int_list("10,12,14") == [10, 12, 14]
    # Espacios alrededor deben tolerarse.
    assert mod._parse_int_list("10, 12 , 14") == [10, 12, 14]


def test_parse_str_list():
    mod = _load_module()
    assert mod._parse_str_list(None) is None
    assert mod._parse_str_list("") is None
    assert mod._parse_str_list("00000014") == ["00000014"]
    assert mod._parse_str_list("00000014,00000023") == ["00000014", "00000023"]


def test_build_filter_expressions_empty():
    mod = _load_module()
    exprs = mod.build_filter_expressions(
        elecciones=None,
        des=None,
        deptos=None,
        provincias=None,
        distritos=None,
        partidos=None,
        ambito=None,
        estado=None,
    )
    assert exprs == []


def test_build_filter_expressions_combinados():
    mod = _load_module()
    exprs = mod.build_filter_expressions(
        elecciones=[10],
        des=[15, 16],
        deptos=None,
        provincias=None,
        distritos=None,
        partidos=["00000014"],
        ambito=1,
        estado="C",
    )
    # Cada argumento poblado produce una expresión: elecciones, des, ambito, estado, partidos.
    assert len(exprs) == 5


def test_write_csv_sin_compresion(tmp_path):
    mod = _load_module()
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    out = tmp_path / "mini.csv"
    bytes_written = mod.write_csv(df, out, "none")
    assert out.exists()
    assert bytes_written > 0
    content = out.read_text()
    assert "a,b" in content
    assert "1,x" in content


def test_write_csv_gzip(tmp_path):
    mod = _load_module()
    import gzip

    df = pl.DataFrame({"a": list(range(100)), "b": ["x"] * 100})
    out = tmp_path / "mini.csv.gz"
    mod.write_csv(df, out, "gzip")
    assert out.exists()
    # Descompresión debe reproducir el CSV.
    with gzip.open(out, "rt") as f:
        content = f.read()
    assert "a,b" in content
    # 100 rows de datos + 1 header + trailing newline eventual.
    assert content.count("\n") >= 100
