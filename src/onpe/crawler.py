"""Crawler jerárquico de dimensiones geográficas.

Jerarquía ONPE: distrito_electoral → departamento → provincia → distrito → local.

Cada nivel se escribe como un parquet en `data/dim/`. Las tablas hijas incluyen
los ubigeos de todos sus padres para facilitar joins posteriores:

- dim_distritos_electorales: catálogo plano (27 filas)
- dim_departamentos: ubigeo_depto, ...
- dim_provincias: ubigeo_prov, ubigeo_depto, ...
- dim_distritos: ubigeo_dist, ubigeo_prov, ubigeo_depto, ...
- dim_locales: codigo_local, ubigeo_dist, ubigeo_prov, ubigeo_depto, ...

Dentro de cada nivel se paraleliza con `asyncio.gather`; el cliente aplica
semáforo (max_concurrent) y rate-limit global.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import polars as pl

from onpe.client import OnpeClient
from onpe.endpoints import (
    ELECCION_PRESIDENCIAL,
    departamentos,
    distritos,
    distritos_electorales,
    locales,
    provincias,
)
from onpe.storage import write_dim

log = logging.getLogger(__name__)


def _ubigeo_key(row: dict[str, Any]) -> str:
    """Extrae el ubigeo de una fila normalizando a string de 6 dígitos."""
    for k in ("codigoUbigeo", "idUbigeo", "ubigeo"):
        v = row.get(k)
        if v is not None:
            return str(v).zfill(6)
    raise KeyError(f"fila sin campo ubigeo reconocido: keys={list(row.keys())}")


def _local_key(row: dict[str, Any]) -> int:
    for k in ("codigoLocalVotacion", "idLocalVotacion", "codigoLocal"):
        v = row.get(k)
        if v is not None:
            return int(v)
    raise KeyError(f"local sin codigo: keys={list(row.keys())}")


async def crawl_hierarchy(
    c: OnpeClient,
    id_eleccion: int = ELECCION_PRESIDENCIAL,
) -> dict[str, pl.DataFrame]:
    """Crawlea los 5 niveles en orden y devuelve un dict {tabla: DataFrame}.

    El id_eleccion afecta el filtrado de departamentos/provincias/distritos
    (solo devuelve los que tienen actas asignadas a esa elección). Para un
    catálogo completo usar la presidencial, que cubre todo el territorio.
    """
    de = await distritos_electorales(c)
    df_de = pl.DataFrame(de)
    log.info("distritos_electorales: %d", len(df_de))

    deptos = await departamentos(c, id_eleccion)
    df_deptos = pl.DataFrame(deptos)
    log.info("departamentos: %d", len(df_deptos))

    # Provincias en paralelo, una llamada por depto.
    depto_ubigeos = [_ubigeo_key(r) for r in deptos]
    provs_per_depto = await asyncio.gather(
        *(provincias(c, id_eleccion, u) for u in depto_ubigeos)
    )
    provs_flat: list[dict[str, Any]] = []
    for u_depto, lst in zip(depto_ubigeos, provs_per_depto, strict=True):
        for r in lst:
            provs_flat.append({**r, "ubigeoDepartamento": u_depto})
    df_provs = pl.DataFrame(provs_flat)
    log.info("provincias: %d", len(df_provs))

    # Distritos en paralelo, una llamada por provincia.
    prov_pairs = [
        (_ubigeo_key(r), r.get("ubigeoDepartamento")) for r in provs_flat
    ]
    dists_per_prov = await asyncio.gather(
        *(distritos(c, id_eleccion, u) for u, _ in prov_pairs)
    )
    dists_flat: list[dict[str, Any]] = []
    for (u_prov, u_depto), lst in zip(prov_pairs, dists_per_prov, strict=True):
        for r in lst:
            dists_flat.append(
                {**r, "ubigeoProvincia": u_prov, "ubigeoDepartamento": u_depto}
            )
    df_dists = pl.DataFrame(dists_flat)
    log.info("distritos: %d", len(df_dists))

    # Locales en paralelo, una llamada por distrito (~1874 requests).
    dist_triples = [
        (
            _ubigeo_key(r),
            r.get("ubigeoProvincia"),
            r.get("ubigeoDepartamento"),
        )
        for r in dists_flat
    ]
    locales_per_dist = await asyncio.gather(
        *(locales(c, u) for u, _, _ in dist_triples)
    )
    locales_flat: list[dict[str, Any]] = []
    for (u_dist, u_prov, u_depto), lst in zip(
        dist_triples, locales_per_dist, strict=True
    ):
        for r in lst:
            locales_flat.append(
                {
                    **r,
                    "ubigeoDistrito": u_dist,
                    "ubigeoProvincia": u_prov,
                    "ubigeoDepartamento": u_depto,
                }
            )
    df_locales = pl.DataFrame(locales_flat)
    log.info("locales: %d", len(df_locales))

    return {
        "distritos_electorales": df_de,
        "departamentos": df_deptos,
        "provincias": df_provs,
        "distritos": df_dists,
        "locales": df_locales,
    }


async def crawl_and_persist(
    c: OnpeClient,
    id_eleccion: int = ELECCION_PRESIDENCIAL,
) -> dict[str, str]:
    """Crawlea y escribe cada tabla a `data/dim/<tabla>.parquet`."""
    tables = await crawl_hierarchy(c, id_eleccion)
    paths: dict[str, str] = {}
    for name, df in tables.items():
        path = write_dim(name, df)
        paths[name] = str(path)
    return paths
