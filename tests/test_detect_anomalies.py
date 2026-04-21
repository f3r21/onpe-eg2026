"""Tests de scripts/detect_anomalies: detectores sobre DataFrames sintéticos."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture(scope="module")
def anom_module():
    """Carga dinámicamente scripts/detect_anomalies.py como módulo."""
    path = Path(__file__).resolve().parent.parent / "scripts" / "detect_anomalies.py"
    spec = importlib.util.spec_from_file_location("detect_anomalies", path)
    mod = importlib.util.module_from_spec(spec)
    sys.argv = ["detect_anomalies"]
    spec.loader.exec_module(mod)
    return mod


def _make_cab(rows: list[dict]) -> pl.LazyFrame:
    """LazyFrame cabecera sintética con columnas mínimas usadas por detectores."""
    defaults = {
        "idActa": 0,
        "idEleccion": 10,
        "codigoEstadoActa": "C",
        "ubigeoDistrito": "140101",
        "idDistritoElectoral": 15,
        "idAmbitoGeografico": 1,
        "totalElectoresHabiles": 300,
        "totalVotosEmitidos": 200,
        "totalVotosValidos": 180,
        "totalAsistentes": 200,
        "porcentajeParticipacionCiudadana": 66.67,
    }
    full = [{**defaults, **r} for r in rows]
    return pl.DataFrame(full).lazy()


def _make_votos(rows: list[dict]) -> pl.LazyFrame:
    defaults = {
        "idActa": 0,
        "idEleccion": 10,
        "ubigeoDistrito": "140101",
        "es_especial": False,
        "nvotos": 0,
        "ccodigo": "00000001",
        "descripcion": "FOO",
    }
    return pl.DataFrame([{**defaults, **r} for r in rows]).lazy()


# ──────────────────────────────────────────────────────────────────────
#  validos_gt_emitidos (CRITICAL)
# ──────────────────────────────────────────────────────────────────────


def test_detect_validos_gt_emitidos_flags_imposible(anom_module):
    cab = _make_cab(
        [
            {"idActa": 1, "totalVotosValidos": 250, "totalVotosEmitidos": 200},
            {"idActa": 2, "totalVotosValidos": 180, "totalVotosEmitidos": 200},
        ]
    )
    df = anom_module.detect_validos_gt_emitidos(cab).collect()
    assert df.shape[0] == 1
    assert df["idActa"].item() == 1
    assert df["severidad"].item() == "CRITICAL"
    assert df["tipo"].item() == "validos_gt_emitidos"


def test_detect_validos_gt_emitidos_ignora_null(anom_module):
    cab = _make_cab([{"idActa": 3, "totalVotosValidos": None, "totalVotosEmitidos": 100}])
    df = anom_module.detect_validos_gt_emitidos(cab).collect()
    assert df.is_empty()


# ──────────────────────────────────────────────────────────────────────
#  asistentes_gt_habiles (CRITICAL)
# ──────────────────────────────────────────────────────────────────────


def test_detect_asistentes_gt_habiles_flags(anom_module):
    cab = _make_cab(
        [
            {"idActa": 10, "totalAsistentes": 350, "totalElectoresHabiles": 300},
            {"idActa": 11, "totalAsistentes": 150, "totalElectoresHabiles": 300},
        ]
    )
    df = anom_module.detect_asistentes_gt_habiles(cab).collect()
    assert df.shape[0] == 1
    assert df["idActa"].item() == 10


# ──────────────────────────────────────────────────────────────────────
#  participacion_gt_100 (CRITICAL)
# ──────────────────────────────────────────────────────────────────────


def test_detect_participacion_gt_100(anom_module):
    cab = _make_cab(
        [
            {"idActa": 20, "porcentajeParticipacionCiudadana": 105.3},
            {"idActa": 21, "porcentajeParticipacionCiudadana": 99.9},
            {"idActa": 22, "porcentajeParticipacionCiudadana": None},
        ]
    )
    df = anom_module.detect_participacion_gt_100(cab).collect()
    assert df.shape[0] == 1
    assert df["idActa"].item() == 20


# ──────────────────────────────────────────────────────────────────────
#  suma_votos_mismatch (HIGH)
# ──────────────────────────────────────────────────────────────────────


def test_detect_suma_votos_mismatch_flags_drift(anom_module):
    cab = _make_cab([{"idActa": 30, "totalVotosEmitidos": 200}])
    # suma(nvotos) = 50 + 50 = 100, delta vs 200 = 100 (> tolerancia 1)
    votos = _make_votos(
        [
            {"idActa": 30, "nvotos": 50, "ccodigo": "00000001"},
            {"idActa": 30, "nvotos": 50, "ccodigo": "00000002"},
        ]
    )
    df = anom_module.detect_suma_votos_mismatch(cab, votos).collect()
    assert df.shape[0] == 1
    assert df["idActa"].item() == 30
    assert df["severidad"].item() == "HIGH"


def test_detect_suma_votos_mismatch_tolera_rounding(anom_module):
    cab = _make_cab([{"idActa": 31, "totalVotosEmitidos": 200}])
    # suma(nvotos) = 199, delta = 1 (dentro de tolerancia)
    votos = _make_votos([{"idActa": 31, "nvotos": 199}])
    df = anom_module.detect_suma_votos_mismatch(cab, votos).collect()
    assert df.is_empty()


def test_detect_suma_votos_solo_actas_C(anom_module):
    # Mismatch en estado E → no flag (solo C tiene sum coherente)
    cab = _make_cab([{"idActa": 40, "codigoEstadoActa": "E", "totalVotosEmitidos": 200}])
    votos = _make_votos([{"idActa": 40, "nvotos": 50}])
    df = anom_module.detect_suma_votos_mismatch(cab, votos).collect()
    assert df.is_empty()


# ──────────────────────────────────────────────────────────────────────
#  concentracion_extrema (MEDIUM)
# ──────────────────────────────────────────────────────────────────────


def test_detect_concentracion_extrema_flags_lista_dominante(anom_module):
    cab = _make_cab([{"idActa": 50, "totalVotosValidos": 100}])
    # Partido A: 98 / 100 = 98% → flag
    votos = _make_votos(
        [
            {"idActa": 50, "nvotos": 98, "ccodigo": "00000001", "es_especial": False},
            {"idActa": 50, "nvotos": 2, "ccodigo": "00000002", "es_especial": False},
        ]
    )
    df = anom_module.detect_concentracion_extrema(cab, votos).collect()
    assert df.shape[0] == 1
    assert df["idActa"].item() == 50


def test_detect_concentracion_ignora_especiales(anom_module):
    """Los votos en blanco no cuentan como 'partido dominante'."""
    cab = _make_cab([{"idActa": 51, "totalVotosValidos": 100}])
    votos = _make_votos(
        [
            {"idActa": 51, "nvotos": 95, "ccodigo": "80", "es_especial": True},
            {"idActa": 51, "nvotos": 5, "ccodigo": "00000001", "es_especial": False},
        ]
    )
    df = anom_module.detect_concentracion_extrema(cab, votos).collect()
    assert df.is_empty()


# ──────────────────────────────────────────────────────────────────────
#  participacion_outlier_distrito (MEDIUM)
# ──────────────────────────────────────────────────────────────────────


def test_detect_outlier_participacion_tres_sigma(anom_module):
    """Actas con participación >3 sigma de la media DE×elección son flaggeadas."""
    rows = [
        # DE=15, idEleccion=10: media ~65%, std bajo
        {
            "idActa": i,
            "idDistritoElectoral": 15,
            "idEleccion": 10,
            "porcentajeParticipacionCiudadana": 65.0 + (i % 3 - 1),  # 64,65,66
        }
        for i in range(50)
    ]
    # Acta outlier: 95% en el mismo DE/elección
    rows.append(
        {
            "idActa": 999,
            "idDistritoElectoral": 15,
            "idEleccion": 10,
            "porcentajeParticipacionCiudadana": 95.0,
        }
    )
    cab = _make_cab(rows)
    df = anom_module.detect_participacion_outlier(cab).collect()
    # 999 debe aparecer; los 50 base pueden o no (depende del std).
    assert 999 in df["idActa"].to_list()


# ──────────────────────────────────────────────────────────────────────
#  emitidos_gt_padron_reniec (HIGH)
# ──────────────────────────────────────────────────────────────────────


def test_detect_emitidos_gt_padron(anom_module):
    cab = _make_cab(
        [
            {
                "idActa": 70,
                "idEleccion": 10,
                "codigoEstadoActa": "C",
                "ubigeoDistrito": "140101",
                "idAmbitoGeografico": 1,
                "totalVotosEmitidos": 2000,
            },
            {
                "idActa": 71,
                "idEleccion": 10,
                "codigoEstadoActa": "C",
                "ubigeoDistrito": "140101",
                "idAmbitoGeografico": 1,
                "totalVotosEmitidos": 500,
            },
        ]
    )
    # Sum emitidos 140101 = 2500. Padrón = 2000 → delta > 5%. Debe flaggear.
    padron = pl.DataFrame(
        {
            "ubigeo_reniec": ["140101"],
            "residencia": ["Nacional"],
            "total_electores": [2000],
        }
    )
    df = anom_module.detect_emitidos_gt_padron(cab, padron).collect()
    assert df.shape[0] == 1
    assert df["ubigeoDistrito"].item() == "140101"
    assert df["severidad"].item() == "HIGH"


def test_detect_emitidos_dentro_de_margen_no_flag(anom_module):
    """Si emitidos < padron×1.05, no debe flaggear."""
    cab = _make_cab(
        [
            {
                "idActa": 80,
                "idEleccion": 10,
                "codigoEstadoActa": "C",
                "ubigeoDistrito": "140102",
                "idAmbitoGeografico": 1,
                "totalVotosEmitidos": 2000,
            }
        ]
    )
    padron = pl.DataFrame(
        {
            "ubigeo_reniec": ["140102"],
            "residencia": ["Nacional"],
            "total_electores": [2100],  # emitidos < 2100×1.05 = 2205
        }
    )
    df = anom_module.detect_emitidos_gt_padron(cab, padron).collect()
    assert df.is_empty()


# ──────────────────────────────────────────────────────────────────────
#  write_resumen_md
# ──────────────────────────────────────────────────────────────────────


def test_write_resumen_md_empty(anom_module, tmp_path):
    out = tmp_path / "resumen.md"
    empty = pl.DataFrame(
        schema={
            "tipo": pl.String,
            "severidad": pl.String,
            "idActa": pl.Int64,
            "idEleccion": pl.Int64,
            "ubigeoDistrito": pl.String,
            "mensaje": pl.String,
        }
    )
    anom_module.write_resumen_md(empty, out)
    content = out.read_text()
    assert "Total hallazgos**: 0" in content
    assert "Sin anomalías detectadas" in content


def test_write_resumen_md_con_hallazgos(anom_module, tmp_path):
    out = tmp_path / "resumen.md"
    df = pl.DataFrame(
        [
            {
                "tipo": "validos_gt_emitidos",
                "severidad": "CRITICAL",
                "idActa": 1,
                "idEleccion": 10,
                "ubigeoDistrito": "140101",
                "mensaje": "validos=250 > emitidos=200",
            }
        ]
    )
    anom_module.write_resumen_md(df, out)
    content = out.read_text()
    assert "Total hallazgos**: 1" in content
    assert "CRITICAL" in content
    assert "validos_gt_emitidos" in content
