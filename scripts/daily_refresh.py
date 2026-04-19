"""Pipeline incremental: re-fetchea solo actas volátiles (estado P o E).

Lee `data/curated/actas_cabecera.parquet`, identifica las actas cuyo estado
aún puede cambiar (`codigoEstadoActa` en P=Pendiente o E=Para envío al JEE)
y re-fetchea *solo esas*, escribiendo un nuevo `run_ts_ms` en el data lake.

El curated siguiente (via `build_curated.py`) dedupa automáticamente al
`run_ts_ms` más reciente por idActa, por lo que las actas en C (Contabilizada)
mantienen su versión anterior sin costo de fetch.

Costo estimado (snapshot nacional 2026-04-18, día 6 post-elección):
    - 73,172 actas volátiles (7,760 P + 65,412 E) de 463,830 totales (15.8%)
    - A 10 rps → ~2h. A 15 rps → ~1h20m.
    - Decrece rápido: día 10 debería ser <30k, día 15 <10k.

Uso:
    uv run python scripts/daily_refresh.py                 # fetch + (separado) build_curated
    uv run python scripts/daily_refresh.py --dry-run       # solo reporta cuántas iría a refetchear
    uv run python scripts/daily_refresh.py --estados P     # solo Pendientes
    uv run python scripts/daily_refresh.py --estados P,E   # default
    uv run python scripts/daily_refresh.py --all           # fetch + build_curated + dq_check
    uv run python scripts/daily_refresh.py --rps 15 --concurrency 15

Flujo diario manual (equivalente a --all):
    uv run python scripts/daily_refresh.py
    uv run python scripts/build_curated.py
    uv run python scripts/dq_check.py
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import subprocess
import sys
import time
from pathlib import Path

import polars as pl

from onpe.actas import SnapshotConfig, snapshot_actas
from onpe.client import ClientConfig, OnpeClient
from onpe.locks import LockHeld, PipelineLock
from onpe.storage import DATA_DIR

CURATED_CAB = DATA_DIR / "curated" / "actas_cabecera.parquet"
# Estados que aún pueden cambiar: Pendiente y Para envío al JEE.
# C (Contabilizada) se considera terminal — no requiere refetch.
DEFAULT_ESTADOS_VOLATILES = ("P", "E")


def _load_volatile_tasks(estados: tuple[str, ...]) -> list[tuple[int, int, str, int]]:
    """Lee curated, filtra por estado y devuelve tasks listas para snapshot_actas.

    Formato de cada task: `(idActa, idMesaRef, ubigeoDistrito, idEleccion)`
    — mismo tuple que produce `enumerate_tasks`.
    """
    if not CURATED_CAB.exists():
        raise SystemExit(
            f"falta {CURATED_CAB}. Correr primero snapshot_actas.py + build_curated.py."
        )
    df = pl.read_parquet(CURATED_CAB)
    total = df.height
    volatiles = df.filter(pl.col("codigoEstadoActa").is_in(list(estados)))
    # Log distribución de estados para transparencia.
    dist = (
        df.group_by("codigoEstadoActa")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
    )
    log = logging.getLogger("daily_refresh")
    log.info("curated: %d filas totales. distribución de estados:", total)
    for row in dist.iter_rows(named=True):
        log.info("  %s: %d", row["codigoEstadoActa"], row["n"])
    log.info(
        "volátiles (estados=%s): %d (%.1f%%)",
        estados,
        volatiles.height,
        100 * volatiles.height / total if total else 0,
    )

    tasks = [
        (
            int(r["idActa"]),
            int(r["idMesaRef"]),
            str(r["ubigeoDistrito"]),
            int(r["idEleccion"]),
        )
        for r in volatiles.iter_rows(named=True)
    ]
    return tasks


def _run_subprocess(script: Path, log: logging.Logger) -> int:
    """Invoca un script auxiliar (build_curated, dq_check) como subprocess."""
    log.info("invocando %s", script.name)
    t0 = time.perf_counter()
    # Heredamos sys.executable; corre en el mismo entorno virtual que daily_refresh.
    result = subprocess.run([sys.executable, str(script)], check=False)
    dt = time.perf_counter() - t0
    if result.returncode == 0:
        log.info("%s OK en %.1fs", script.name, dt)
    else:
        log.error("%s falló (exit=%d, %.1fs)", script.name, result.returncode, dt)
    return result.returncode


async def _amain() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--estados",
        type=str,
        default=",".join(DEFAULT_ESTADOS_VOLATILES),
        help="codigoEstadoActa a refetchear, comma-separated. Default: P,E",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="no fetchea, solo reporta cuántas actas entrarían",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="tras el fetch, corre build_curated.py y dq_check.py",
    )
    parser.add_argument("--rps", type=float, default=10.0, help="requests por segundo")
    parser.add_argument(
        "--concurrency", type=int, default=10, help="concurrencia del cliente"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="corta en N actas (smoke test del refresh)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("daily_refresh")

    estados = tuple(s.strip().upper() for s in args.estados.split(",") if s.strip())
    if not estados:
        raise SystemExit("--estados vacío")

    tasks = _load_volatile_tasks(estados)
    if args.limit is not None:
        tasks = tasks[: args.limit]
        log.info("truncado a %d por --limit", len(tasks))

    if not tasks:
        log.info("no hay actas en estados %s; nada que refrescar", estados)
        return 0

    if args.dry_run:
        log.info("dry-run: se refetchearían %d actas", len(tasks))
        return 0

    cfg_http = ClientConfig(
        max_concurrent=args.concurrency, rate_per_second=args.rps
    )
    cfg_snap = SnapshotConfig()
    t0 = time.perf_counter()
    # Lock advisory: previene colisión con snapshot_actas corriendo en paralelo.
    try:
        with PipelineLock(metadata={"job": "daily_refresh", "estados": list(estados)}):
            async with OnpeClient(cfg_http) as c:
                ck, stats = await snapshot_actas(c, cfg=cfg_snap, tasks_override=tasks)
    except LockHeld as e:
        log.error("abortando: %s", e)
        return 1
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

    if args.all:
        scripts_dir = Path(__file__).resolve().parent
        rc = _run_subprocess(scripts_dir / "build_curated.py", log)
        if rc != 0:
            return rc
        rc = _run_subprocess(scripts_dir / "dq_check.py", log)
        return rc

    return 0


def main() -> None:
    sys.exit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
