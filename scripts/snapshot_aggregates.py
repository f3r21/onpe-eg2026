"""Captura una foto de agregados y escribe `data/facts/<tabla>/snapshot_date=.../...parquet`."""

from __future__ import annotations

import asyncio
import logging
import sys
import time

from onpe.aggregates import snapshot_and_persist
from onpe.client import ClientConfig, OnpeClient
from onpe.endpoints import proceso_activo

# Wall-clock cap del run. Un snapshot OK tarda ~30-60s; 300s cubre retries
# exponenciales + latencia ONPE. Si se excede, es deadlock o degradación severa:
# abortamos para que launchd recicle al próximo tick (StartInterval=900).
# Evita el failure mode observado 2026-04-19 donde un HTTP/2 500 dejó el
# proceso colgado 9h sin ningún snapshot.
_RUN_TIMEOUT_S = 300.0


async def main() -> None:
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
        log.info("  %s -> %s", name, p)


def _entrypoint() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("snapshot_aggregates")
    try:
        asyncio.run(asyncio.wait_for(main(), timeout=_RUN_TIMEOUT_S))
    except TimeoutError:
        log.error(
            "timeout %.0fs alcanzado. abortando para que launchd relance.",
            _RUN_TIMEOUT_S,
        )
        return 124  # convención: timeout
    except Exception:
        log.exception("snapshot_aggregates fallo. launchd relanzara en ~15min")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(_entrypoint())
