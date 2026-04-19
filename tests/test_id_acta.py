"""Tests de la fórmula determinística id_acta."""

from __future__ import annotations

import pytest

from onpe.endpoints import (
    ELECCION_DIPUTADOS,
    ELECCION_PARLAMENTO_ANDINO,
    ELECCION_PRESIDENCIAL,
    ELECCION_SENADORES_NACIONAL,
    ELECCION_SENADORES_REGIONAL,
    id_acta,
)


def test_id_acta_mesa_fixture_cayma():
    """Fixture canónico: mesa 5507 CAYMA Presidencial = 550704010210."""
    assert id_acta(5507, "040102", ELECCION_PRESIDENCIAL) == 550704010210


def test_id_acta_padding_mesa_leading_zeros():
    """idMesa < 1000 se padea a 4 dígitos."""
    # Mesa 1 en Lima-Cercado-Lima (150101), Presidencial (10).
    assert id_acta(1, "150101", 10) == 1_150101_10


def test_id_acta_ubigeo_con_leading_zero_string():
    """ubigeoDistrito como string '040102' (leading zero) se preserva."""
    assert id_acta(5507, "040102", 10) == 550704010210


def test_id_acta_ubigeo_sin_leading_zero_se_padea():
    """ubigeoDistrito como string '40102' sin leading zero se padea a 6 dígitos."""
    # int("40102") → 40102 → zfill(6) → "040102" → mismo resultado que "040102".
    assert id_acta(5507, "40102", 10) == id_acta(5507, "040102", 10)


@pytest.mark.parametrize(
    "id_eleccion",
    [
        ELECCION_PRESIDENCIAL,
        ELECCION_PARLAMENTO_ANDINO,
        ELECCION_DIPUTADOS,
        ELECCION_SENADORES_REGIONAL,
        ELECCION_SENADORES_NACIONAL,
    ],
)
def test_id_acta_longitud_estable_por_eleccion(id_eleccion: int):
    """Las 5 elecciones producen idActa de 12 dígitos con mesa 4-dig + ubigeo 6-dig."""
    acta_id = id_acta(5507, "040102", id_eleccion)
    assert len(str(acta_id)) == 12


def test_id_acta_senadores_nacional_mesa5507():
    """Cross-check con la asserción del smoke.py (mesa 5507 senado nacional)."""
    result = id_acta(5507, "040102", ELECCION_SENADORES_NACIONAL)
    assert str(result).endswith("15")
    assert str(result).startswith("5507")


def test_id_acta_ubigeo_exterior_91xxx():
    """Ubigeos del exterior (91-95xxxx) no colisionan con Perú."""
    # Buenos Aires fixture 920202, mesa 9001, Presidencial.
    acta_ext = id_acta(9001, "920202", 10)
    acta_peru = id_acta(9001, "040102", 10)
    assert acta_ext != acta_peru
    assert "920202" in str(acta_ext)


def test_id_acta_es_int():
    """El retorno es int (no string) — consumers hacen math con él."""
    assert isinstance(id_acta(5507, "040102", 10), int)
