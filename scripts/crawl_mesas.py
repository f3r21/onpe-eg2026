"""Construye dim/mesas.parquet (universo de mesas físicas).

Itera distrito × primer-local sobre `/actas` (eleccion=10 por default).
Coste aprox: ~1874 requests con páginas de 500, 5 rps → ~7 min.
"""

from __future__ import annotations

import asyncio
import logging
import time

from onpe.client import ClientConfig, OnpeClient
from onpe.mesas import crawl_and_persist


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = ClientConfig(max_concurrent=5, rate_per_second=5.0)
    t0 = time.perf_counter()
    async with OnpeClient(config) as c:
        n, path = await crawl_and_persist(c)
    dt = time.perf_counter() - t0
    log = logging.getLogger("crawl_mesas")
    log.info("mesas: %d filas → %s (%.1fs)", n, path, dt)


if __name__ == "__main__":
    asyncio.run(main())
