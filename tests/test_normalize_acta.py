"""Tests de normalize_acta — rama por cada estado del acta."""

from __future__ import annotations

from typing import Any

from onpe.actas import normalize_acta


def test_normalize_mesa_inexistente_devuelve_none(acta_inexistente: dict[str, Any], snapshot_ts_ms: int):
    """codigoMesa=None → (None, [], [], []); no contamina el pipeline."""
    cab, votos, linea, archivos = normalize_acta(
        acta_inexistente, acta_id_fallback=123, id_mesa_fallback=1, ubigeo_distrito="040102",
        snapshot_ts_ms=snapshot_ts_ms,
    )
    assert cab is None
    assert votos == []
    assert linea == []
    assert archivos == []


def test_normalize_acta_c_cabecera_campos_criticos(acta_c: dict[str, Any], snapshot_ts_ms: int):
    """Acta C contabilizada: cabecera tiene id, codigoMesa, totales, estado."""
    cab, _, _, _ = normalize_acta(
        acta_c, acta_id_fallback=550704010210, id_mesa_fallback=5507,
        ubigeo_distrito="040102", snapshot_ts_ms=snapshot_ts_ms,
    )
    assert cab is not None
    assert cab["idActa"] == 550704010210
    assert cab["codigoMesa"] == "005507"
    assert cab["idEleccion"] == 10
    assert cab["codigoEstadoActa"] == "C"
    assert cab["totalElectoresHabiles"] == 274
    assert cab["totalVotosEmitidos"] == 251
    assert cab["idMesaRef"] == 5507
    assert cab["ubigeoDistrito"] == "040102"
    assert cab["snapshot_ts_ms"] == snapshot_ts_ms


def test_normalize_acta_c_votos_tienen_41_filas(acta_c: dict[str, Any], snapshot_ts_ms: int):
    """Acta C Presidencial debe producir 41 filas de detalle (38 partidos + 3 especiales)."""
    _, votos, _, _ = normalize_acta(
        acta_c, acta_id_fallback=550704010210, id_mesa_fallback=5507,
        ubigeo_distrito="040102", snapshot_ts_ms=snapshot_ts_ms,
    )
    assert len(votos) == 41


def test_normalize_especiales_flag(acta_c: dict[str, Any], snapshot_ts_ms: int):
    """'VOTOS EN BLANCO/NULOS/IMPUGNADOS' tienen es_especial=True, resto False."""
    _, votos, _, _ = normalize_acta(
        acta_c, acta_id_fallback=550704010210, id_mesa_fallback=5507,
        ubigeo_distrito="040102", snapshot_ts_ms=snapshot_ts_ms,
    )
    especiales = [v for v in votos if v["es_especial"]]
    normales = [v for v in votos if not v["es_especial"]]
    assert len(especiales) == 3
    descripciones = sorted(v["descripcion"] for v in especiales)
    assert descripciones == ["VOTOS EN BLANCO", "VOTOS IMPUGNADOS", "VOTOS NULOS"]
    # Especiales no tienen campos cand_* (_first_candidato devuelve {} para candidato vacío)
    assert all("cand_doc" not in v for v in especiales)
    assert all("cand_apellido_paterno" not in v for v in especiales)
    assert len(normales) == 38


def test_normalize_candidato_extraction(acta_c: dict[str, Any], snapshot_ts_ms: int):
    """Primer candidato del array se expande a cand_* fields."""
    _, votos, _, _ = normalize_acta(
        acta_c, acta_id_fallback=550704010210, id_mesa_fallback=5507,
        ubigeo_distrito="040102", snapshot_ts_ms=snapshot_ts_ms,
    )
    partidos = [v for v in votos if not v["es_especial"]]
    # Primer partido del fixture: RENOVACIÓN POPULAR con López Aliaga.
    rp = next(v for v in partidos if v["descripcion"] == "RENOVACIÓN POPULAR")
    assert rp["cand_apellido_paterno"] == "LÓPEZ ALIAGA"
    assert rp["cand_nombres"] == "RAFAEL BERNARDO"
    assert rp["nvotos"] == 102


def test_normalize_acta_p_sin_detalle(acta_p: dict[str, Any], snapshot_ts_ms: int):
    """Acta P pendiente: cabecera OK, votos vacíos, linea_tiempo vacía."""
    cab, votos, linea, archivos = normalize_acta(
        acta_p, acta_id_fallback=550704010210, id_mesa_fallback=5507,
        ubigeo_distrito="040102", snapshot_ts_ms=snapshot_ts_ms,
    )
    assert cab is not None
    assert cab["codigoEstadoActa"] == "P"
    assert cab["totalVotosEmitidos"] is None
    assert votos == []
    assert linea == []


def test_normalize_acta_e_tiene_detalle_pero_totales_null(acta_e: dict[str, Any], snapshot_ts_ms: int):
    """Acta E: detalle poblado, totales de cabecera null."""
    cab, votos, _, _ = normalize_acta(
        acta_e, acta_id_fallback=550704010210, id_mesa_fallback=5507,
        ubigeo_distrito="040102", snapshot_ts_ms=snapshot_ts_ms,
    )
    assert cab is not None
    assert cab["codigoEstadoActa"] == "E"
    assert cab["totalVotosEmitidos"] is None  # característico del bucket E
    assert len(votos) == 41  # el detalle persiste


def test_normalize_fallback_id_cuando_data_id_none(acta_c: dict[str, Any], snapshot_ts_ms: int):
    """Si data['id'] es None, usa acta_id_fallback."""
    acta_c["id"] = None
    cab, _, _, _ = normalize_acta(
        acta_c, acta_id_fallback=999999, id_mesa_fallback=5507,
        ubigeo_distrito="040102", snapshot_ts_ms=snapshot_ts_ms,
    )
    assert cab is not None
    assert cab["idActa"] == 999999


def test_normalize_linea_tiempo_con_evento_idx(acta_c: dict[str, Any], snapshot_ts_ms: int):
    """lineaTiempo del acta se enumera con evento_idx monotónico."""
    # Agregar linea tiempo sintética al fixture
    acta_c["lineaTiempo"] = [
        {"codigoEstadoActa": "P", "descripcionEstadoActa": "Pendiente", "fechaRegistro": 1744000000000},
        {"codigoEstadoActa": "C", "descripcionEstadoActa": "Contabilizada", "fechaRegistro": 1744100000000},
    ]
    _, _, linea, _ = normalize_acta(
        acta_c, acta_id_fallback=550704010210, id_mesa_fallback=5507,
        ubigeo_distrito="040102", snapshot_ts_ms=snapshot_ts_ms,
    )
    assert len(linea) == 2
    assert [ev["evento_idx"] for ev in linea] == [0, 1]
    assert linea[0]["codigoEstadoActa"] == "P"
    assert linea[1]["fechaRegistro"] == 1744100000000


def test_normalize_archivos_preserva_archivoId(acta_c: dict[str, Any], snapshot_ts_ms: int):
    """Campos de archivos[] mantienen archivoId (= data.id del sub-object)."""
    acta_c["archivos"] = [
        {"id": "abc-123", "tipo": 1, "nombre": "acta.pdf", "descripcion": "Acta sufragio", "daudFechaCreacion": 1744000000000},
    ]
    _, _, _, archivos = normalize_acta(
        acta_c, acta_id_fallback=550704010210, id_mesa_fallback=5507,
        ubigeo_distrito="040102", snapshot_ts_ms=snapshot_ts_ms,
    )
    assert len(archivos) == 1
    assert archivos[0]["archivoId"] == "abc-123"
    assert archivos[0]["tipo"] == 1
    assert archivos[0]["idActa"] == 550704010210


def test_normalize_ubigeo_distrito_en_todas_las_filas_hijas(acta_c: dict[str, Any], snapshot_ts_ms: int):
    """Todas las filas (votos/linea/archivos) heredan ubigeoDistrito del parent."""
    acta_c["archivos"] = [{"id": "a1", "tipo": 1, "nombre": "x", "descripcion": "y", "daudFechaCreacion": 0}]
    acta_c["lineaTiempo"] = [{"codigoEstadoActa": "C", "descripcionEstadoActa": "x", "fechaRegistro": 0}]
    _, votos, linea, archivos = normalize_acta(
        acta_c, acta_id_fallback=550704010210, id_mesa_fallback=5507,
        ubigeo_distrito="040102", snapshot_ts_ms=snapshot_ts_ms,
    )
    assert all(v["ubigeoDistrito"] == "040102" for v in votos)
    assert all(l["ubigeoDistrito"] == "040102" for l in linea)
    assert all(a["ubigeoDistrito"] == "040102" for a in archivos)
