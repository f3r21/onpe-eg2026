"""Snapshot resumable de /actas/{idActa} para las 5 elecciones.

Primera corrida (Perú + exterior, todos los ámbitos):
    uv run python scripts/snapshot_actas.py

Solo exterior (idAmbitoGeografico=2, ~12.7k actas, ~15 min a 15 rps):
    uv run python scripts/snapshot_actas.py --ambitos 2 --rps 15 --concurrency 15

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
from onpe.endpoints import AMBITOS_TODOS
from onpe.locks import LockHeldError, PipelineLock
from onpe.storage import DIM_DIR


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="corta en N actas (smoke)")
    parser.add_argument("--resume", type=int, default=None, help="run_ts_ms a reanudar")
    parser.add_argument("--rps", type=float, default=10.0, help="requests por segundo")
    parser.add_argument("--concurrency", type=int, default=10, help="concurrencia del cliente")
    parser.add_argument(
        "--ambitos",
        type=str,
        default=",".join(str(a) for a in AMBITOS_TODOS),
        help="idAmbitoGeografico a incluir, comma-separated. Default: todos (1,2)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("snapshot_actas")

    ambitos: tuple[int, ...] = tuple(int(x) for x in args.ambitos.split(",") if x.strip())

    mesas_path = DIM_DIR / "mesas.parquet"
    if not mesas_path.exists():
        raise SystemExit(f"falta {mesas_path}. Correr scripts/crawl_mesas.py antes.")
    df_mesas = pl.read_parquet(mesas_path)
    log.info("mesas cargadas: %d", len(df_mesas))

    if "idAmbitoGeografico" in df_mesas.columns:
        df_mesas = df_mesas.filter(pl.col("idAmbitoGeografico").is_in(list(ambitos)))
        log.info("mesas tras filtro ambitos=%s: %d", ambitos, len(df_mesas))
    elif ambitos != AMBITOS_TODOS:
        log.warning(
            "mesas.parquet legacy sin idAmbitoGeografico; ignorando --ambitos=%s",
            ambitos,
        )

    cfg_http = ClientConfig(max_concurrent=args.concurrency, rate_per_second=args.rps)
    cfg_snap = SnapshotConfig()
    t0 = time.perf_counter()
    # Lock advisory: previene colisión de rate-limit con daily_refresh corriendo
    # en paralelo. El loop de aggregates es seguro paralelo (no toma este lock).
    try:
        with PipelineLock(metadata={"job": "snapshot_actas", "ambitos": list(ambitos)}):
            async with OnpeClient(cfg_http) as c:
                ck, stats = await snapshot_actas(
                    c, df_mesas, cfg_snap, resume_run_ts_ms=args.resume, limit=args.limit
                )
    except LockHeldError as e:
        log.error("abortando: %s", e)
        raise SystemExit(1) from e
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
