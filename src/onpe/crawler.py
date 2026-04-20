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
    AMBITOS_TODOS,
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


async def _crawl_one_ambito(
    c: OnpeClient,
    id_eleccion: int,
    id_ambito: int,
) -> dict[str, list[dict[str, Any]]]:
    """Crawlea los 4 niveles bajo un mismo idAmbitoGeografico.

    Para ambito=2 la semántica cambia (continente→pais→ciudad) pero los
    endpoints son los mismos y los ubigeos viven en un rango disjunto
    (91-95xxxx), así que al concatenar ambos ambitos no hay colisión.
    """
    deptos = await departamentos(c, id_eleccion, id_ambito)
    for r in deptos:
        r["idAmbitoGeografico"] = id_ambito
    log.info("ambito=%d departamentos: %d", id_ambito, len(deptos))

    depto_ubigeos = [_ubigeo_key(r) for r in deptos]
    provs_per_depto = await asyncio.gather(
        *(provincias(c, id_eleccion, u, id_ambito) for u in depto_ubigeos)
    )
    provs_flat: list[dict[str, Any]] = []
    for u_depto, lst in zip(depto_ubigeos, provs_per_depto, strict=True):
        for r in lst:
            provs_flat.append(
                {
                    **r,
                    "ubigeoDepartamento": u_depto,
                    "idAmbitoGeografico": id_ambito,
                }
            )
    log.info("ambito=%d provincias: %d", id_ambito, len(provs_flat))

    prov_pairs = [
        (_ubigeo_key(r), r.get("ubigeoDepartamento")) for r in provs_flat
    ]
    dists_per_prov = await asyncio.gather(
        *(distritos(c, id_eleccion, u, id_ambito) for u, _ in prov_pairs)
    )
    dists_flat: list[dict[str, Any]] = []
    for (u_prov, u_depto), lst in zip(prov_pairs, dists_per_prov, strict=True):
        for r in lst:
            dists_flat.append(
                {
                    **r,
                    "ubigeoProvincia": u_prov,
                    "ubigeoDepartamento": u_depto,
                    "idAmbitoGeografico": id_ambito,
                }
            )
    log.info("ambito=%d distritos: %d", id_ambito, len(dists_flat))

    dist_triples = [
        (
            _ubigeo_key(r),
            r.get("ubigeoProvincia"),
            r.get("ubigeoDepartamento"),
        )
        for r in dists_flat
    ]
    # `locales` no depende de ambito — acepta cualquier ubigeo.
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
                    "idAmbitoGeografico": id_ambito,
                }
            )
    log.info("ambito=%d locales: %d", id_ambito, len(locales_flat))

    return {
        "departamentos": deptos,
        "provincias": provs_flat,
        "distritos": dists_flat,
        "locales": locales_flat,
    }


async def crawl_hierarchy(
    c: OnpeClient,
    id_eleccion: int = ELECCION_PRESIDENCIAL,
    ambitos: tuple[int, ...] = AMBITOS_TODOS,
) -> dict[str, pl.DataFrame]:
    """Crawlea los 5 niveles sobre los ambitos indicados.

    El id_eleccion afecta el filtrado: solo devuelve niveles con actas
    asignadas a esa elección. Para un catálogo completo usar la presidencial.
    `ambitos` controla si se crawlea Perú, extranjero o ambos.
    """
    de = await distritos_electorales(c)
    df_de = pl.DataFrame(de)
    log.info("distritos_electorales: %d", len(df_de))

    merged: dict[str, list[dict[str, Any]]] = {
        "departamentos": [],
        "provincias": [],
        "distritos": [],
        "locales": [],
    }
    for amb in ambitos:
        one = await _crawl_one_ambito(c, id_eleccion, amb)
        for k, rows in one.items():
            merged[k].extend(rows)

    return {
        "distritos_electorales": df_de,
        "departamentos": pl.DataFrame(merged["departamentos"]),
        "provincias": pl.DataFrame(merged["provincias"]),
        "distritos": pl.DataFrame(merged["distritos"]),
        "locales": pl.DataFrame(merged["locales"]),
    }


async def crawl_and_persist(
    c: OnpeClient,
    id_eleccion: int = ELECCION_PRESIDENCIAL,
    ambitos: tuple[int, ...] = AMBITOS_TODOS,
) -> dict[str, str]:
    """Crawlea y escribe cada tabla a `data/dim/<tabla>.parquet`."""
    tables = await crawl_hierarchy(c, id_eleccion, ambitos)
    paths: dict[str, str] = {}
    for name, df in tables.items():
        path = write_dim(name, df)
        paths[name] = str(path)
    return paths
