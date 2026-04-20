"""Tests de enumerate_tasks — expansión (mesa × elección) → tuplas."""

from __future__ import annotations

import polars as pl

from onpe.actas import enumerate_tasks
from onpe.endpoints import (
    ELECCION_DIPUTADOS,
    ELECCION_PRESIDENCIAL,
    ELECCION_SENADORES_NACIONAL,
    id_acta,
)


def _make_mesas(rows: list[tuple[int, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {"idMesa": [r[0] for r in rows], "ubigeoDistrito": [r[1] for r in rows]},
        schema={"idMesa": pl.Int64, "ubigeoDistrito": pl.String},
    )


def test_enumerate_cross_product_basico():
    """2 mesas × 3 elecciones → 6 tasks."""
    mesas = _make_mesas([(5507, "040102"), (1, "150101")])
    tasks = enumerate_tasks(mesas, (10, 13, 15))
    assert len(tasks) == 6


def test_enumerate_tuple_shape():
    """Cada task es (acta_id:int, id_mesa:int, ubigeo:str, id_eleccion:int)."""
    mesas = _make_mesas([(5507, "040102")])
    tasks = enumerate_tasks(mesas, (10,))
    assert len(tasks) == 1
    acta_id, id_mesa, ubigeo, id_eleccion = tasks[0]
    assert isinstance(acta_id, int)
    assert isinstance(id_mesa, int) and id_mesa == 5507
    assert isinstance(ubigeo, str) and ubigeo == "040102"
    assert isinstance(id_eleccion, int) and id_eleccion == 10


def test_enumerate_coincide_con_id_acta_formula():
    """acta_id de enumerate_tasks == id_acta(id_mesa, ubigeo, id_eleccion)."""
    mesas = _make_mesas([(5507, "040102")])
    tasks = enumerate_tasks(
        mesas, (ELECCION_PRESIDENCIAL, ELECCION_DIPUTADOS, ELECCION_SENADORES_NACIONAL)
    )
    for acta_id, id_mesa, ubigeo, id_eleccion in tasks:
        assert acta_id == id_acta(id_mesa, ubigeo, id_eleccion)


def test_enumerate_preserva_ubigeo_leading_zero():
    """ubigeo '040102' se pasa tal cual, sin perder leading zero."""
    mesas = _make_mesas([(5507, "040102")])
    tasks = enumerate_tasks(mesas, (10,))
    assert tasks[0][2] == "040102"


def test_enumerate_iterable_elecciones():
    """Acepta cualquier iterable de elecciones (tupla, lista, generator)."""
    mesas = _make_mesas([(5507, "040102")])
    tasks_tuple = enumerate_tasks(mesas, (10, 13))
    tasks_list = enumerate_tasks(mesas, [10, 13])
    tasks_gen = enumerate_tasks(mesas, (e for e in (10, 13)))
    assert len(tasks_tuple) == len(tasks_list) == len(tasks_gen) == 2


def test_enumerate_mesas_vacias():
    """DataFrame vacío → 0 tasks, no error."""
    mesas = _make_mesas([])
    tasks = enumerate_tasks(mesas, (10, 13))
    assert tasks == []


def test_enumerate_5_elecciones_reales():
    """Universo completo: 1 mesa × 5 elecciones → 5 idActa distintos."""
    mesas = _make_mesas([(5507, "040102")])
    tasks = enumerate_tasks(mesas, (10, 12, 13, 14, 15))
    acta_ids = {t[0] for t in tasks}
    assert len(acta_ids) == 5  # todos distintos
