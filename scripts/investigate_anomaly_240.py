"""Diagnóstico de las 240 actas C sin detalle — task #58.

Fenómeno observado al 2026-04-18 (pre-re-snapshot): 240 actas en estado
C (Contabilizada) pero con estadoActa=N, sin `detalle[]` en la respuesta
del API. Eran todas del voto exterior (48 mesas × 5 elecciones). La
hipótesis principal: mesas NO instaladas donde el estado terminal es
"Contabilizada" pero no hay detalle físico para reportar.

Este script:
1. Identifica las actas C sin detalle en curated
2. Re-fetchea cada una individualmente vía /actas/{idActa}
3. Clasifica la respuesta en 3 buckets:
   (a) tiene detalle ahora (se resolvió via A4 o update ONPE) → integrar
   (b) sigue sin detalle, estadoActa=N → mesa NO instalada confirmada
   (c) otro estado → cambio inesperado, flag para investigación
4. Escribe reporte en data/curated/actas_anomalia_240_investigacion.parquet
5. Si ≥90% son clase (b), actualizar CLAUDE.md con la clasificación definitiva
   (el script solo reporta; la actualización docs es manual).

Uso:
    uv run python scripts/investigate_anomaly_240.py
    uv run python scripts/investigate_anomaly_240.py --dry-run   # solo reporta el universo
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from onpe.client import ClientConfig, OnpeClient, OnpeError
from onpe.endpoints import acta_detalle
from onpe.storage import DATA_DIR, ms_to_lima_iso, utc_now_ms

log = logging.getLogger("anomaly240")

CURATED_CAB = DATA_DIR / "curated" / "actas_cabecera.parquet"
OUT_PATH = DATA_DIR / "curated" / "actas_anomalia_240_investigacion.parquet"


def find_anomaly_240() -> pl.DataFrame:
    """Identifica actas C sin votos en curated (fingerprint de la anomalía #58)."""
    cab = pl.scan_parquet(CURATED_CAB).filter(pl.col("codigoEstadoActa") == "C")
    votos_path = DATA_DIR / "curated" / "actas_votos.parquet"
    if not votos_path.exists():
        raise SystemExit(f"falta {votos_path}. Correr build_curated antes.")
    actas_con_votos = (
        pl.scan_parquet(votos_path).select(pl.col("idActa").unique()).collect()
    )
    return (
        cab.join(actas_con_votos.lazy(), on="idActa", how="anti")
        .select("idActa", "codigoMesa", "ubigeoDistrito", "idEleccion", "estadoActa")
        .collect()
    )


@dataclass(frozen=True)
class DiagnosticResult:
    idActa: int
    codigoMesa: str
    ubigeoDistrito: str
    idEleccion: int
    prev_estadoActa: str
    bucket: str  # "resolved" | "mesa_no_instalada" | "other"
    now_codigoEstadoActa: str | None
    now_estadoActa: str | None
    n_detalle: int
    descripcionSubEstadoActa: str | None


async def diagnose_one(
    c: OnpeClient, row: dict, snapshot_ts_ms: int
) -> DiagnosticResult:
    try:
        data = await acta_detalle(c, int(row["idActa"]))
    except OnpeError as e:
        return DiagnosticResult(
            idActa=int(row["idActa"]),
            codigoMesa=row["codigoMesa"],
            ubigeoDistrito=row["ubigeoDistrito"],
            idEleccion=int(row["idEleccion"]),
            prev_estadoActa=row["estadoActa"] or "",
            bucket="error",
            now_codigoEstadoActa=None,
            now_estadoActa=None,
            n_detalle=0,
            descripcionSubEstadoActa=str(e)[:120],
        )

    detalle = data.get("detalle") or []
    n_detalle = len(detalle)
    now_codigo = data.get("codigoEstadoActa")
    now_estado = data.get("estadoActa")
    sub = data.get("descripcionSubEstadoActa")

    if n_detalle > 0:
        bucket = "resolved"
    elif now_codigo == "C" and now_estado == "N":
        bucket = "mesa_no_instalada"
    else:
        bucket = "other"

    return DiagnosticResult(
        idActa=int(row["idActa"]),
        codigoMesa=row["codigoMesa"],
        ubigeoDistrito=row["ubigeoDistrito"],
        idEleccion=int(row["idEleccion"]),
        prev_estadoActa=row["estadoActa"] or "",
        bucket=bucket,
        now_codigoEstadoActa=now_codigo,
        now_estadoActa=now_estado,
        n_detalle=n_detalle,
        descripcionSubEstadoActa=sub,
    )


async def _run(df_target: pl.DataFrame, rps: float) -> list[DiagnosticResult]:
    cfg = ClientConfig(max_concurrent=3, rate_per_second=rps)
    snapshot_ts_ms = utc_now_ms()
    results: list[DiagnosticResult] = []
    async with OnpeClient(cfg) as c:
        rows = df_target.to_dicts()
        for i, row in enumerate(rows):
            res = await diagnose_one(c, row, snapshot_ts_ms)
            results.append(res)
            if (i + 1) % 50 == 0:
                log.info("progreso %d/%d", i + 1, len(rows))
    return results


def classify_and_save(results: list[DiagnosticResult]) -> dict[str, int]:
    """Persiste resultados en parquet y reporta bucket distribution."""
    df = pl.DataFrame([r.__dict__ for r in results])
    df.write_parquet(OUT_PATH, compression="zstd")
    log.info("reporte escrito: %s", OUT_PATH)
    dist = df.group_by("bucket").agg(pl.len().alias("n")).sort("bucket")
    log.info("\nDistribución de buckets:")
    for row in dist.iter_rows(named=True):
        log.info("  %s: %d", row["bucket"], row["n"])
    # Summary por bucket
    buckets = {row["bucket"]: int(row["n"]) for row in dist.iter_rows(named=True)}
    return buckets


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="solo reporta tamaño universo")
    parser.add_argument("--rps", type=float, default=3.0, help="rate limit suave")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    df = find_anomaly_240()
    log.info("anomalía #58 universo actual: %d actas C sin detalle en curated", df.height)
    if df.height > 0:
        dist = (
            df.group_by("idEleccion")
            .agg(pl.len().alias("n"))
            .sort("idEleccion")
        )
        log.info("distribución por idEleccion:\n%s", dist)
        dist_est = (
            df.group_by("estadoActa")
            .agg(pl.len().alias("n"))
            .sort("n", descending=True)
        )
        log.info("distribución por estadoActa:\n%s", dist_est)

    if args.dry_run:
        log.info("dry-run: no se consultan endpoints")
        return

    if df.height == 0:
        log.info("no hay actas C sin detalle — anomalía #58 está cerrada")
        OUT_PATH.unlink(missing_ok=True)
        return

    results = asyncio.run(_run(df, args.rps))
    buckets = classify_and_save(results)

    total = sum(buckets.values())
    log.info("\n=== CONCLUSIÓN ===")
    if buckets.get("resolved", 0) > 0:
        log.info("  %d actas se resolvieron (tienen detalle ahora) — próximo build_curated las integra",
                 buckets["resolved"])
    if buckets.get("mesa_no_instalada", 0) / max(total, 1) >= 0.9:
        log.info("  ≥90%% son mesa NO instalada (estadoActa=N) — anomalía explicada")
        log.info("  ACCIÓN: actualizar CLAUDE.md task #58 como closed con justificación")
    elif buckets.get("other", 0) > 0:
        log.info("  %d actas en bucket 'other' — requiere investigación case-by-case",
                 buckets["other"])


if __name__ == "__main__":
    main()
