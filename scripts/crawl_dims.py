"""Crawlea las dimensiones geográficas y escribe `data/dim/*.parquet`."""

from __future__ import annotations

import asyncio
import logging
import time

from onpe.client import ClientConfig, OnpeClient
from onpe.crawler import crawl_and_persist


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Conservador: 5 req/s, concurrencia 5. ~1874 distritos para locales ⇒ ~6-7 min.
    config = ClientConfig(max_concurrent=5, rate_per_second=5.0)
    t0 = time.perf_counter()
    async with OnpeClient(config) as c:
        paths = await crawl_and_persist(c)
    dt = time.perf_counter() - t0
    log = logging.getLogger("crawl_dims")
    log.info("completado en %.1fs", dt)
    for name, p in paths.items():
        log.info("  %s → %s", name, p)


if __name__ == "__main__":
    asyncio.run(main())
