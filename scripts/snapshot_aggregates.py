"""Captura una foto de agregados y escribe `data/facts/<tabla>/snapshot_date=.../...parquet`."""

from __future__ import annotations

import asyncio
import logging
import time

from onpe.aggregates import snapshot_and_persist
from onpe.client import ClientConfig, OnpeClient
from onpe.endpoints import proceso_activo


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = ClientConfig(max_concurrent=5, rate_per_second=5.0)
    t0 = time.perf_counter()
    async with OnpeClient(config) as c:
        proc = await proceso_activo(c)
        id_proceso = proc["id"]
        ts_ms, paths = await snapshot_and_persist(c, id_proceso)
    dt = time.perf_counter() - t0
    log = logging.getLogger("snapshot_aggregates")
    log.info("snapshot_ts_ms=%d completado en %.1fs", ts_ms, dt)
    for name, p in paths.items():
        log.info("  %s → %s", name, p)


if __name__ == "__main__":
    asyncio.run(main())
