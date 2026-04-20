"""Ingestor de snapshots agregados.

Cada corrida captura una foto temporal del avance del conteo:

- fact_resumen_elecciones: una fila por idEleccion (5 filas/snapshot)
- fact_totales: una fila por idEleccion (nacional, 5 filas/snapshot)
- fact_totales_de: una fila por (idEleccion, idDistritoElectoral) (~135 filas)
- fact_participantes: una fila por (idEleccion, participante) nacional
- fact_participantes_de: una fila por (idEleccion, idDistritoElectoral, participante)
- fact_mapa_calor: una fila por (idEleccion, departamento)
- fact_mesa_totales: una fila/snapshot (nacional agregado de mesas)
- fact_ultima_fecha: una fila/snapshot (timestamp de la última sync del backend)

Todas incluyen `snapshot_ts_ms` y `snapshot_lima_iso`. Particionadas en
`data/facts/<tabla>/snapshot_date=YYYY-MM-DD/<ts_ms>.parquet`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import polars as pl

from onpe.client import OnpeClient
from onpe.endpoints import (
    ELECCIONES_NACIONALES,
    ELECCIONES_REGIONALES,
    distritos_electorales,
    mapa_calor_nacional,
    mesa_totales,
    participantes_distrito_electoral,
    participantes_nacional,
    resumen_elecciones,
    totales_distrito_electoral,
    totales_nacional,
    ultima_fecha,
)
from onpe.storage import ms_to_lima_iso, utc_now_ms, write_fact

log = logging.getLogger(__name__)

ALL_ELECCIONES: tuple[int, ...] = ELECCIONES_NACIONALES + ELECCIONES_REGIONALES


def _stamp(
    rows: list[dict[str, Any]] | dict[str, Any],
    snapshot_ts_ms: int,
    extras: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Añade columnas de snapshot y extras a un registro o lista de registros."""
    if isinstance(rows, dict):
        rows = [rows]
    meta = {
        "snapshot_ts_ms": snapshot_ts_ms,
        "snapshot_lima_iso": ms_to_lima_iso(snapshot_ts_ms),
    }
    if extras:
        meta.update(extras)
    return [{**r, **meta} for r in rows]


async def snapshot_aggregates(
    c: OnpeClient,
    id_proceso: int,
    elecciones: tuple[int, ...] = ALL_ELECCIONES,
) -> dict[str, pl.DataFrame]:
    """Captura una foto completa del avance en el momento actual."""
    snapshot_ts_ms = utc_now_ms()
    log.info("snapshot_ts_ms=%d (%s)", snapshot_ts_ms, ms_to_lima_iso(snapshot_ts_ms))

    # Fase 1: metadatos y lista de distritos electorales.
    res_elec, mt, uf, des = await asyncio.gather(
        resumen_elecciones(c, id_proceso),
        mesa_totales(c),
        ultima_fecha(c),
        distritos_electorales(c),
    )
    log.info("fase 1 ok: %d elecciones, %d distritos electorales", len(res_elec), len(des))

    # Fase 2: totales + participantes + mapa_calor por elección (nacional).
    # 3 calls × N elecciones en paralelo.
    tot_nac_tasks = [totales_nacional(c, e) for e in elecciones]
    part_nac_tasks = [participantes_nacional(c, e) for e in elecciones]
    mapa_nac_tasks = [mapa_calor_nacional(c, e) for e in elecciones]
    tot_nac, part_nac, mapa_nac = await asyncio.gather(
        asyncio.gather(*tot_nac_tasks),
        asyncio.gather(*part_nac_tasks),
        asyncio.gather(*mapa_nac_tasks),
    )
    log.info("fase 2 ok: totales/participantes/mapa-calor por %d elecciones", len(elecciones))

    # Fase 3: totales + participantes por (elección × distrito_electoral).
    # El endpoint /distrito-electoral/distritos devuelve {codigo, nombre}.
    # El param de query es idDistritoElectoral → se mapea desde `codigo`.
    de_ids = [d["codigo"] for d in des]
    de_pairs = [(e, d) for e in elecciones for d in de_ids]
    tot_de = await asyncio.gather(*(totales_distrito_electoral(c, e, d) for e, d in de_pairs))
    part_de = await asyncio.gather(
        *(participantes_distrito_electoral(c, e, d) for e, d in de_pairs)
    )
    log.info("fase 3 ok: totales/participantes por %d pares (eleccion × DE)", len(de_pairs))

    # Normalización a DataFrames.
    df_res = pl.DataFrame(_stamp(res_elec, snapshot_ts_ms))
    df_mt = pl.DataFrame(_stamp(mt, snapshot_ts_ms))
    df_uf = pl.DataFrame(_stamp(uf, snapshot_ts_ms))

    # Algunos endpoints devuelven None (HTTP 204) para combinaciones que no aplican
    # —p.ej. participantes_nacional con idEleccion=13/14 (regionales). Se filtran.
    tot_rows: list[dict[str, Any]] = []
    for e, row in zip(elecciones, tot_nac, strict=True):
        if row is None:
            continue
        tot_rows.extend(_stamp(row, snapshot_ts_ms, {"idEleccion": e}))
    df_tot = pl.DataFrame(tot_rows)

    part_rows: list[dict[str, Any]] = []
    for e, lst in zip(elecciones, part_nac, strict=True):
        if not lst:
            continue
        part_rows.extend(_stamp(lst, snapshot_ts_ms, {"idEleccion": e}))
    df_part = pl.DataFrame(part_rows)

    mapa_rows: list[dict[str, Any]] = []
    for e, lst in zip(elecciones, mapa_nac, strict=True):
        if not lst:
            continue
        mapa_rows.extend(_stamp(lst, snapshot_ts_ms, {"idEleccion": e}))
    df_mapa = pl.DataFrame(mapa_rows)

    tot_de_rows: list[dict[str, Any]] = []
    for (e, d), row in zip(de_pairs, tot_de, strict=True):
        if row is None:
            continue
        tot_de_rows.extend(_stamp(row, snapshot_ts_ms, {"idEleccion": e, "idDistritoElectoral": d}))
    df_tot_de = pl.DataFrame(tot_de_rows)

    part_de_rows: list[dict[str, Any]] = []
    for (e, d), lst in zip(de_pairs, part_de, strict=True):
        if not lst:
            continue
        part_de_rows.extend(
            _stamp(lst, snapshot_ts_ms, {"idEleccion": e, "idDistritoElectoral": d})
        )
    df_part_de = pl.DataFrame(part_de_rows)

    return {
        "resumen_elecciones": df_res,
        "mesa_totales": df_mt,
        "ultima_fecha": df_uf,
        "totales": df_tot,
        "totales_de": df_tot_de,
        "participantes": df_part,
        "participantes_de": df_part_de,
        "mapa_calor": df_mapa,
    }


async def snapshot_and_persist(
    c: OnpeClient,
    id_proceso: int,
    elecciones: tuple[int, ...] = ALL_ELECCIONES,
) -> tuple[int, dict[str, str]]:
    """Captura el snapshot y escribe cada tabla particionada."""
    snapshot_ts_ms = utc_now_ms()
    tables = await snapshot_aggregates(c, id_proceso, elecciones)
    paths: dict[str, str] = {}
    for name, df in tables.items():
        # Reutiliza el snapshot_ts_ms del primer registro si existe (consistencia).
        ts = df.get_column("snapshot_ts_ms").item(0) if len(df) else snapshot_ts_ms
        path = write_fact(name, df, int(ts))
        paths[name] = str(path)
    return snapshot_ts_ms, paths
