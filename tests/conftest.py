"""Fixtures globales de la suite pytest.

Las fixtures de acta detalle se derivan de `data/smoke/acta_mesa5507_presi.json`
(respuesta real del API ONPE para mesa 5507 Cayma Presidencial, estado C).
Variantes sintéticas para estados E, P y mesa inexistente se construyen
mutando el fixture base — permiten cubrir las 4 ramas de normalize_acta
sin depender de más muestras en vivo.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest

SMOKE_DIR = Path(__file__).resolve().parent.parent / "data" / "smoke"


@pytest.fixture(scope="session")
def smoke_dir() -> Path:
    if not SMOKE_DIR.exists():
        pytest.skip("data/smoke/ no existe — correr `uv run python scripts/smoke.py` antes.")
    return SMOKE_DIR


@pytest.fixture(scope="session")
def acta_c_raw(smoke_dir: Path) -> dict[str, Any]:
    """Acta real de mesa 5507 Cayma Presidencial (estado C=Contabilizada)."""
    return json.loads((smoke_dir / "acta_mesa5507_presi.json").read_text())


@pytest.fixture
def acta_c(acta_c_raw: dict[str, Any]) -> dict[str, Any]:
    """Copia profunda del acta C para mutar sin contaminar otras fixtures."""
    return copy.deepcopy(acta_c_raw)


@pytest.fixture
def acta_e(acta_c_raw: dict[str, Any]) -> dict[str, Any]:
    """Sintético: acta estado E (Para envío al JEE) — detalle OK, totales null."""
    acta = copy.deepcopy(acta_c_raw)
    acta["codigoEstadoActa"] = "E"
    acta["descripcionEstadoActa"] = "Para envío al JEE"
    acta["totalVotosEmitidos"] = None
    acta["totalVotosValidos"] = None
    acta["totalAsistentes"] = None
    acta["porcentajeParticipacionCiudadana"] = None
    return acta


@pytest.fixture
def acta_p(acta_c_raw: dict[str, Any]) -> dict[str, Any]:
    """Sintético: acta estado P (Pendiente) — ni detalle ni totales."""
    acta = copy.deepcopy(acta_c_raw)
    acta["codigoEstadoActa"] = "P"
    acta["descripcionEstadoActa"] = "Pendiente"
    acta["totalVotosEmitidos"] = None
    acta["totalVotosValidos"] = None
    acta["totalAsistentes"] = None
    acta["porcentajeParticipacionCiudadana"] = None
    acta["detalle"] = []
    acta["lineaTiempo"] = []
    return acta


@pytest.fixture
def acta_inexistente() -> dict[str, Any]:
    """Respuesta 200 del API para un idActa que no corresponde a mesa real."""
    return {
        "id": None,
        "idMesa": None,
        "codigoMesa": None,  # señal de mesa inexistente
        "descripcionMesa": None,
        "idEleccion": None,
        "detalle": None,
        "lineaTiempo": None,
        "archivos": None,
    }


@pytest.fixture
def snapshot_ts_ms() -> int:
    """Timestamp determinístico para tests (2026-04-18 14:00:00 UTC)."""
    return 1744984800000


@pytest.fixture
def tmp_facts_dir(tmp_path: Path) -> Path:
    """Directorio temporal que simula la estructura data/facts/."""
    facts = tmp_path / "facts"
    facts.mkdir()
    return facts


@pytest.fixture
def sample_cabecera_df() -> pl.DataFrame:
    """DataFrame mínimo válido contra SCHEMAS['actas_cabecera']."""
    return pl.DataFrame(
        {
            "id": [550704010210],
            "codigoMesa": ["005507"],
            "descripcionMesa": ["005507"],
            "idEleccion": [10],
            "ubigeoNivel01": ["AREQUIPA"],
            "ubigeoNivel02": ["AREQUIPA"],
            "ubigeoNivel03": ["CAYMA"],
            "centroPoblado": [""],
            "nombreLocalVotacion": ["IEP NIÑO MAGISTRAL"],
            "totalElectoresHabiles": [274],
            "totalVotosEmitidos": [251],
            "totalVotosValidos": [246],
            "totalAsistentes": [251],
            "porcentajeParticipacionCiudadana": [91.606],
            "estadoActa": ["D"],
            "estadoComputo": ["N"],
            "codigoEstadoActa": ["C"],
            "descripcionEstadoActa": ["Contabilizada"],
            "estadoActaResolucion": [""],
            "estadoDescripcionActaResolucion": [None],
            "descripcionSubEstadoActa": [None],
            "codigoSolucionTecnologica": [1],
            "descripcionSolucionTecnologica": ["OCR"],
            "idActa": [550704010210],
            "idMesaRef": [5507],
            "ubigeoDistrito": ["040102"],
            "snapshot_ts_ms": [1744984800000],
            "snapshot_lima_iso": ["2026-04-18T09:00:00-05:00"],
        },
        schema={
            "id": pl.Int64,
            "codigoMesa": pl.String,
            "descripcionMesa": pl.String,
            "idEleccion": pl.Int64,
            "ubigeoNivel01": pl.String,
            "ubigeoNivel02": pl.String,
            "ubigeoNivel03": pl.String,
            "centroPoblado": pl.String,
            "nombreLocalVotacion": pl.String,
            "totalElectoresHabiles": pl.Int64,
            "totalVotosEmitidos": pl.Int64,
            "totalVotosValidos": pl.Int64,
            "totalAsistentes": pl.Int64,
            "porcentajeParticipacionCiudadana": pl.Float64,
            "estadoActa": pl.String,
            "estadoComputo": pl.String,
            "codigoEstadoActa": pl.String,
            "descripcionEstadoActa": pl.String,
            "estadoActaResolucion": pl.String,
            "estadoDescripcionActaResolucion": pl.String,
            "descripcionSubEstadoActa": pl.String,
            "codigoSolucionTecnologica": pl.Int64,
            "descripcionSolucionTecnologica": pl.String,
            "idActa": pl.Int64,
            "idMesaRef": pl.Int64,
            "ubigeoDistrito": pl.String,
            "snapshot_ts_ms": pl.Int64,
            "snapshot_lima_iso": pl.String,
        },
    )
