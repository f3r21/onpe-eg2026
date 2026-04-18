"""Inventario de mesas físicas vía /actas listado.

El endpoint `/actas` paginado devuelve una fila por (mesa fisica × eleccion).
Como `idActa = pad(idMesa,4) + pad(ubigeoDistrito,6) + pad(idEleccion,2)`,
iterando una sola vez por (distrito × local-gatillo × eleccion=10) descubrimos
el universo de mesas; los idActa para las otras 4 elecciones se derivan
cambiando solamente los dos últimos dígitos.

Observación empírica (fuentes_datos.md §B.3): el listado es a nivel
**distrito** cuando los 6 params están presentes. `idLocalVotacion` actúa
como gatillo, no como filtro estricto. Por eso elegimos un local cualquiera
por distrito y paginamos hasta agotar `totalPaginas`.

Para un distrito sin mesas (p.ej. algún ubigeo de residentes extranjeros)
`totalPaginas` es 0 o `content` viene vacío; el flujo no rompe.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import polars as pl

from onpe.client import OnpeClient
from onpe.endpoints import ELECCION_PRESIDENCIAL, listar_actas
from onpe.storage import DIM_DIR, write_dim

log = logging.getLogger(__name__)

# 500 es buen balance: pocos distritos tienen > 500 mesas, así que la mayoría
# resuelve en una sola página. Tamaños más grandes no acelerarían nada.
PAGE_SIZE = 500


async def _paginate_distrito(
    c: OnpeClient,
    id_eleccion: int,
    ubigeo_dist: str,
    codigo_local_gatillo: int,
) -> list[dict[str, Any]]:
    """Paginación completa del listado para un distrito (local como gatillo)."""
    rows: list[dict[str, Any]] = []
    pagina = 0
    while True:
        data = await listar_actas(
            c,
            id_eleccion,
            ubigeo_dist,
            codigo_local_gatillo,
            pagina=pagina,
            tamanio=PAGE_SIZE,
        )
        # Cuando la API responde 204 (o algo que cayó al fallback del cliente),
        # data puede ser None. Tratamos como distrito sin mesas.
        if not data:
            return rows
        content = data.get("content") or []
        rows.extend(content)
        total_paginas = int(data.get("totalPaginas") or 0)
        if pagina + 1 >= total_paginas:
            return rows
        pagina += 1


async def crawl_mesas(
    c: OnpeClient,
    id_eleccion: int = ELECCION_PRESIDENCIAL,
    concurrency: int = 5,
) -> pl.DataFrame:
    """Descubre el universo de mesas iterando distrito × primer-local.

    Precondición: dim/distritos.parquet y dim/locales.parquet ya existen.
    Se generan con scripts/crawl_dims.py.
    """
    dist_path = DIM_DIR / "distritos.parquet"
    loc_path = DIM_DIR / "locales.parquet"
    if not dist_path.exists() or not loc_path.exists():
        raise FileNotFoundError(
            f"faltan dims en {DIM_DIR}. Correr scripts/crawl_dims.py antes."
        )

    df_dist = pl.read_parquet(dist_path)
    df_loc = pl.read_parquet(loc_path)

    # Un local "gatillo" por distrito: el primero en orden estable.
    triggers = (
        df_loc.sort(["ubigeoDistrito", "codigoLocalVotacion"])
        .group_by("ubigeoDistrito", maintain_order=True)
        .agg(pl.first("codigoLocalVotacion").alias("codigoLocalGatillo"))
    )

    # Join con distritos para conservar nombre y ubigeos padre en dim/mesas.
    pairs = triggers.join(
        df_dist.select(
            [
                pl.col("ubigeo").alias("ubigeoDistritoDim"),
                "ubigeoProvincia",
                "ubigeoDepartamento",
                pl.col("nombre").alias("nombreDistrito"),
            ]
        ),
        left_on="ubigeoDistrito",
        right_on="ubigeoDistritoDim",
        how="inner",
    )
    log.info("distritos a iterar: %d", len(pairs))

    sem = asyncio.Semaphore(concurrency)

    async def one(row: dict[str, Any]) -> list[dict[str, Any]]:
        async with sem:
            rows = await _paginate_distrito(
                c,
                id_eleccion,
                row["ubigeoDistrito"],
                int(row["codigoLocalGatillo"]),
            )
            extras = {
                "ubigeoDistrito": row["ubigeoDistrito"],
                "ubigeoProvincia": row["ubigeoProvincia"],
                "ubigeoDepartamento": row["ubigeoDepartamento"],
                "nombreDistrito": row["nombreDistrito"],
            }
            return [{**r, **extras} for r in rows]

    tasks = [one(r) for r in pairs.to_dicts()]
    nested = await asyncio.gather(*tasks)
    flat = [r for sub in nested for r in sub]
    log.info("mesas crudas (pre-dedup): %d", len(flat))

    if not flat:
        return pl.DataFrame()

    df = pl.DataFrame(flat, infer_schema_length=None)

    # El listado trae una fila por mesa-eleccion; como iteramos una sola
    # eleccion, debería ser 1:1 con mesa. Pero si un mismo idMesa apareciera
    # duplicado (p.ej. solapes entre locales), dedup por identidad física.
    key_cols = [c for c in ("idMesa", "ubigeoDistrito") if c in df.columns]
    if key_cols:
        df = df.unique(subset=key_cols, maintain_order=True)
    log.info("mesas únicas: %d", len(df))
    return df


async def crawl_and_persist(
    c: OnpeClient,
    id_eleccion: int = ELECCION_PRESIDENCIAL,
) -> tuple[int, str]:
    """Crawlea y persiste dim/mesas.parquet. Retorna (n_filas, path)."""
    df = await crawl_mesas(c, id_eleccion)
    path = write_dim("mesas", df)
    return len(df), str(path)
