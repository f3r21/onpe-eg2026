"""Crawlea las dimensiones geográficas y escribe `data/dim/*.parquet`.

Por default crawlea ambos ambitos (Perú + extranjero). Con `--solo-peru`
reproduce el comportamiento previo.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time

from onpe.client import ClientConfig, OnpeClient
from onpe.crawler import crawl_and_persist
from onpe.endpoints import AMBITO_EXTRANJERO, AMBITO_PERU


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
        paths = await crawl_and_persist(c, ambitos=ambitos)
    dt = time.perf_counter() - t0
    log = logging.getLogger("crawl_dims")
    log.info("completado en %.1fs (ambitos=%s)", dt, ambitos)
    for name, p in paths.items():
        log.info("  %s -> %s", name, p)


if __name__ == "__main__":
    asyncio.run(main())
