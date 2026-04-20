"""Tests de _all_candidatos + integracion con normalize_acta.

Verifica que la tabla actas_candidatos captura TODOS los candidatos del
array detalle.candidato[] (crucial para Senado/Diputados, que pueden tener
hasta 29 candidatos por lista vs el `_first_candidato` legacy).
"""

from __future__ import annotations

from typing import Any

from onpe.actas import _all_candidatos, normalize_acta


def test_all_candidatos_none_o_vacia_devuelve_lista_vacia():
    """cand_list None o [] -> [] (no rows)."""
    stamps = {"snapshot_ts_ms": 1, "snapshot_lima_iso": "x"}
    assert (
        _all_candidatos(
            None,
            acta_id=1,
            id_eleccion=10,
            ubigeo_distrito="040102",
            partido_ccodigo="00000001",
            stamps=stamps,
        )
        == []
    )
    assert (
        _all_candidatos(
            [],
            acta_id=1,
            id_eleccion=10,
            ubigeo_distrito="040102",
            partido_ccodigo="00000001",
            stamps=stamps,
        )
        == []
    )


def test_all_candidatos_enumera_array_completo():
    """Un array con N candidatos produce N filas con candidato_idx 0..N-1."""
    stamps = {"snapshot_ts_ms": 1, "snapshot_lima_iso": "x"}
    cands = [
        {
            "apellidoPaterno": "PEREZ",
            "apellidoMaterno": "GARCIA",
            "nombres": "JUAN",
            "cdocumentoIdentidad": "10000001",
        },
        {
            "apellidoPaterno": "LOPEZ",
            "apellidoMaterno": "MARTINEZ",
            "nombres": "MARIA",
            "cdocumentoIdentidad": "10000002",
        },
        {
            "apellidoPaterno": "SANCHEZ",
            "apellidoMaterno": "TORRES",
            "nombres": "CARLOS",
            "cdocumentoIdentidad": "10000003",
        },
    ]
    rows = _all_candidatos(
        cands,
        acta_id=550704010210,
        id_eleccion=15,
        ubigeo_distrito="040102",
        partido_ccodigo="00000014",
        stamps=stamps,
    )
    assert len(rows) == 3
    assert [r["candidato_idx"] for r in rows] == [0, 1, 2]
    assert rows[0]["apellidoPaterno"] == "PEREZ"
    assert rows[2]["cdocumentoIdentidad"] == "10000003"
    # Todas las filas heredan el acta padre + partido_ccodigo
    assert all(r["idActa"] == 550704010210 for r in rows)
    assert all(r["idEleccion"] == 15 for r in rows)
    assert all(r["ubigeoDistrito"] == "040102" for r in rows)
    assert all(r["partido_ccodigo"] == "00000014" for r in rows)
    assert all(r["snapshot_ts_ms"] == 1 for r in rows)


def test_all_candidatos_maneja_campos_ausentes():
    """Si un candidato del array no tiene apellidoPaterno/etc, queda None."""
    stamps = {"snapshot_ts_ms": 1, "snapshot_lima_iso": "x"}
    rows = _all_candidatos(
        [{"apellidoPaterno": "X"}],
        acta_id=1,
        id_eleccion=10,
        ubigeo_distrito="040102",
        partido_ccodigo="01",
        stamps=stamps,
    )
    assert len(rows) == 1
    assert rows[0]["apellidoPaterno"] == "X"
    assert rows[0]["apellidoMaterno"] is None
    assert rows[0]["nombres"] is None
    assert rows[0]["cdocumentoIdentidad"] is None


def test_normalize_acta_emite_candidatos_desde_detalle_presidencial(
    acta_c: dict[str, Any], snapshot_ts_ms: int
):
    """Presidencial: cada detalle.candidato[0] emite 1 fila en actas_candidatos.

    Nota: algunos partidos pueden tener candidato=null (no registrado o
    retirado); esos no producen filas en actas_candidatos pero si en votos.
    """
    _, votos, _, _, candidatos = normalize_acta(
        acta_c,
        acta_id_fallback=550704010210,
        id_mesa_fallback=5507,
        ubigeo_distrito="040102",
        snapshot_ts_ms=snapshot_ts_ms,
    )
    no_especiales = [v for v in votos if not v["es_especial"]]
    # Deben haber candidatos mayoritariamente (fixture real tiene 36 de 38 con candidato)
    assert len(candidatos) > 0
    assert len(candidatos) <= len(no_especiales)
    # Verificar que el partido_ccodigo enlaza actas_candidatos con actas_votos
    ccodigos_votos = {v["ccodigo"] for v in no_especiales}
    ccodigos_cands = {c["partido_ccodigo"] for c in candidatos}
    assert ccodigos_cands.issubset(ccodigos_votos)


def test_normalize_acta_especiales_no_generan_candidatos(
    acta_c: dict[str, Any], snapshot_ts_ms: int
):
    """VOTOS EN BLANCO/NULOS/IMPUGNADOS no tienen candidato asociado."""
    _, votos, _, _, candidatos = normalize_acta(
        acta_c,
        acta_id_fallback=550704010210,
        id_mesa_fallback=5507,
        ubigeo_distrito="040102",
        snapshot_ts_ms=snapshot_ts_ms,
    )
    # Candidatos solo viene de partidos no-especiales. Los ccodigos de especiales
    # no aparecen en la tabla candidatos.
    ccodigos_especiales = {v["ccodigo"] for v in votos if v["es_especial"]}
    ccodigos_cands = {c["partido_ccodigo"] for c in candidatos}
    assert ccodigos_especiales.isdisjoint(ccodigos_cands)


def test_normalize_acta_p_sin_detalle_candidatos_vacios(
    acta_p: dict[str, Any], snapshot_ts_ms: int
):
    """Acta P (pendiente) sin detalle[] -> candidatos = []."""
    _, _, _, _, candidatos = normalize_acta(
        acta_p,
        acta_id_fallback=550704010210,
        id_mesa_fallback=5507,
        ubigeo_distrito="040102",
        snapshot_ts_ms=snapshot_ts_ms,
    )
    assert candidatos == []
