"""Construye dim/mesas.parquet (universo de mesas físicas).

Itera distrito × primer-local sobre `/actas`. Por default cubre Perú y
exterior. Con `--solo-peru` reproduce el comportamiento previo.

Coste aprox:
- Perú: ~1874 distritos, páginas de 500, 5 rps -> ~7 min.
- Exterior: ~130 ciudades -> <1 min.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time

from onpe.client import ClientConfig, OnpeClient
from onpe.endpoints import AMBITO_EXTRANJERO, AMBITO_PERU
from onpe.mesas import crawl_and_persist


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--solo-peru", action="store_true", help="omite el exterior (idAmbitoGeografico=2)"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = ClientConfig(max_concurrent=5, rate_per_second=5.0)
    ambitos = (AMBITO_PERU,) if args.solo_peru else (AMBITO_PERU, AMBITO_EXTRANJERO)
    t0 = time.perf_counter()
    async with OnpeClient(config) as c:
        n, path = await crawl_and_persist(c, ambitos=ambitos)
    dt = time.perf_counter() - t0
    log = logging.getLogger("crawl_mesas")
    log.info("mesas: %d filas -> %s (%.1fs, ambitos=%s)", n, path, dt, ambitos)


if __name__ == "__main__":
    asyncio.run(main())
