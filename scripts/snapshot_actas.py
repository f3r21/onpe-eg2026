"""Snapshot resumable de /actas/{idActa} para las 5 elecciones.

Primera corrida:
    uv run python scripts/snapshot_actas.py

Con límite (smoke test, 50 actas):
    uv run python scripts/snapshot_actas.py --limit 50

Reanudar un run previo:
    uv run python scripts/snapshot_actas.py --resume 1776492170298
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time

import polars as pl

from onpe.actas import SnapshotConfig, snapshot_actas
from onpe.client import ClientConfig, OnpeClient
from onpe.storage import DIM_DIR


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="corta en N actas (smoke)")
    parser.add_argument("--resume", type=int, default=None, help="run_ts_ms a reanudar")
    parser.add_argument("--rps", type=float, default=10.0, help="requests por segundo")
    parser.add_argument("--concurrency", type=int, default=10, help="concurrencia del cliente")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("snapshot_actas")

    mesas_path = DIM_DIR / "mesas.parquet"
    if not mesas_path.exists():
        raise SystemExit(f"falta {mesas_path}. Correr scripts/crawl_mesas.py antes.")
    df_mesas = pl.read_parquet(mesas_path)
    log.info("mesas cargadas: %d", len(df_mesas))

    cfg_http = ClientConfig(
        max_concurrent=args.concurrency, rate_per_second=args.rps
    )
    cfg_snap = SnapshotConfig()
    t0 = time.perf_counter()
    async with OnpeClient(cfg_http) as c:
        ck, stats = await snapshot_actas(
            c, df_mesas, cfg_snap, resume_run_ts_ms=args.resume, limit=args.limit
        )
    dt = time.perf_counter() - t0
    log.info(
        "run_ts_ms=%d done in %.1fs: ok=%d vacias=%d fallidas=%d (completados=%d/%d)",
        ck.run_ts_ms,
        dt,
        stats["ok"],
        stats["vacias"],
        stats["fallidas"],
        len(ck.completed_acta_ids),
        ck.total_expected,
    )


if __name__ == "__main__":
    asyncio.run(main())
