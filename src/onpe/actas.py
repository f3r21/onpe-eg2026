"""Snapshot de actas individuales — fase B del pipeline de ingesta.

A partir de `dim/mesas.parquet` se derivan los idActa de las 5 elecciones
con la fórmula determinística `id_acta(idMesa, ubigeoDistrito, idEleccion)`
y se consulta cada uno vía `/actas/{idActa}`.

Se produce dos tablas fact:

- `actas_cabecera`: una fila por idActa con totales y estado.
- `actas_votos`: una fila por (idActa × descripcion) con nvotos, porcentajes
  y datos del primer candidato del array `candidato`. "VOTOS EN BLANCO",
  "VOTOS NULOS" y "VOTOS IMPUGNADOS" aparecen como filas propias con
  `es_especial=True` y candidato nulo.

Resumabilidad: checkpoint JSON en `data/state/actas_run_<run_ts_ms>.json`
con los idActa ya completados. Al reanudar se reutiliza `run_ts_ms` y se
saltean los idActa ya registrados. Los parquet se escriben chunked en
`data/facts/<tabla>/snapshot_date=<lima>/<run_ts_ms>-<chunk_idx>.parquet`.

Concurrencia: batches de `BATCH` tareas con `asyncio.gather`; el cliente
aplica semáforo + rate-limit global.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import polars as pl

from onpe.client import OnpeClient, OnpeError
from onpe.endpoints import acta_detalle, id_acta
from onpe.storage import (
    DATA_DIR,
    FACT_DIR,
    ms_to_lima_date,
    ms_to_lima_iso,
    utc_now_ms,
)

log = logging.getLogger(__name__)

STATE_DIR = DATA_DIR / "state"

# Tamaño del batch de tareas async concurrentes. El cliente tiene semaforo +
# rate-limit globales; esto solo limita la memoria del buffer in-flight.
BATCH = 500
# Cada cuántas actas completadas se flushea un chunk parquet y se guarda
# checkpoint. 5000 × 41 items/detalle = ~200k filas en votos; manejable.
CHUNK_SIZE = 5000


# --- Config y checkpoint ---------------------------------------------------


@dataclass
class SnapshotConfig:
    elecciones: tuple[int, ...] = (10, 12, 13, 14, 15)
    batch: int = BATCH
    chunk_size: int = CHUNK_SIZE


@dataclass
class Checkpoint:
    run_ts_ms: int
    started_lima_iso: str
    elecciones: tuple[int, ...]
    total_expected: int
    completed_acta_ids: set[int] = field(default_factory=set)
    next_chunk_idx: int = 0
    failed: list[dict[str, Any]] = field(default_factory=list)

    def path(self) -> Path:
        return STATE_DIR / f"actas_run_{self.run_ts_ms}.json"

    def save(self) -> None:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = self.path().with_suffix(".json.tmp")
        payload = {
            "run_ts_ms": self.run_ts_ms,
            "started_lima_iso": self.started_lima_iso,
            "elecciones": list(self.elecciones),
            "total_expected": self.total_expected,
            "completed_acta_ids": sorted(self.completed_acta_ids),
            "next_chunk_idx": self.next_chunk_idx,
            "failed": self.failed,
        }
        tmp.write_text(json.dumps(payload))
        tmp.replace(self.path())

    @classmethod
    def load(cls, run_ts_ms: int) -> "Checkpoint":
        path = STATE_DIR / f"actas_run_{run_ts_ms}.json"
        data = json.loads(path.read_text())
        return cls(
            run_ts_ms=data["run_ts_ms"],
            started_lima_iso=data["started_lima_iso"],
            elecciones=tuple(data["elecciones"]),
            total_expected=data["total_expected"],
            completed_acta_ids=set(data["completed_acta_ids"]),
            next_chunk_idx=data["next_chunk_idx"],
            failed=data.get("failed", []),
        )


# --- Enumeración de tareas -------------------------------------------------


def enumerate_tasks(
    df_mesas: pl.DataFrame,
    elecciones: Iterable[int],
) -> list[tuple[int, int, str, int]]:
    """Produce (acta_id, id_mesa, ubigeo_distrito, id_eleccion) por mesa × elección."""
    tasks: list[tuple[int, int, str, int]] = []
    for row in df_mesas.iter_rows(named=True):
        id_mesa = int(row["idMesa"])
        ubigeo = str(row["ubigeoDistrito"])
        for el in elecciones:
            tasks.append((id_acta(id_mesa, ubigeo, el), id_mesa, ubigeo, el))
    return tasks


# --- Normalización de la respuesta -----------------------------------------


_CABECERA_COLS = (
    "id",
    "idMesa",
    "codigoMesa",
    "descripcionMesa",
    "idEleccion",
    "ubigeoNivel01",
    "ubigeoNivel02",
    "ubigeoNivel03",
    "centroPoblado",
    "nombreLocalVotacion",
    "totalElectoresHabiles",
    "totalVotosEmitidos",
    "totalVotosValidos",
    "totalAsistentes",
    "porcentajeParticipacionCiudadana",
    "estadoActa",
    "estadoComputo",
    "codigoEstadoActa",
    "descripcionEstadoActa",
    "estadoActaResolucion",
    "estadoDescripcionActaResolucion",
    "descripcionSubEstadoActa",
)

_SPECIAL_DESCRIPTIONS = ("VOTOS EN BLANCO", "VOTOS NULOS", "VOTOS IMPUGNADOS")


def _first_candidato(cand: list[dict[str, Any]] | None) -> dict[str, Any]:
    if not cand:
        return {}
    c = cand[0]
    return {
        "cand_apellido_paterno": c.get("apellidoPaterno"),
        "cand_apellido_materno": c.get("apellidoMaterno"),
        "cand_nombres": c.get("nombres"),
        "cand_doc": c.get("cdocumentoIdentidad"),
    }


def normalize_acta(
    data: dict[str, Any],
    acta_id_fallback: int,
    id_mesa_fallback: int,
    ubigeo_distrito: str,
    snapshot_ts_ms: int,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Parte la respuesta en (cabecera_row, votos_rows).

    Si la mesa no existe (codigoMesa is None), devuelve (None, []).
    """
    if not data or data.get("codigoMesa") is None:
        return None, []

    acta_id = data.get("id") or acta_id_fallback
    id_eleccion = data.get("idEleccion")
    stamps = {
        "snapshot_ts_ms": snapshot_ts_ms,
        "snapshot_lima_iso": ms_to_lima_iso(snapshot_ts_ms),
    }

    cabecera = {c: data.get(c) for c in _CABECERA_COLS}
    cabecera["idActa"] = int(acta_id)
    cabecera["idMesaRef"] = id_mesa_fallback
    cabecera["ubigeoDistrito"] = ubigeo_distrito
    cabecera.update(stamps)

    votos: list[dict[str, Any]] = []
    for d in data.get("detalle") or []:
        desc = d.get("descripcion") or ""
        row = {
            "idActa": int(acta_id),
            "idEleccion": id_eleccion,
            "ubigeoDistrito": ubigeo_distrito,
            "descripcion": desc,
            "es_especial": desc in _SPECIAL_DESCRIPTIONS,
            "ccodigo": d.get("ccodigo"),
            "nposicion": d.get("nposicion"),
            "nvotos": d.get("nvotos"),
            "nagrupacionPolitica": d.get("nagrupacionPolitica"),
            "nporcentajeVotosValidos": d.get("nporcentajeVotosValidos"),
            "nporcentajeVotosEmitidos": d.get("nporcentajeVotosEmitidos"),
            "estado": d.get("estado"),
            "grafico": d.get("grafico"),
            "cargo": d.get("cargo"),
            "sexo": d.get("sexo"),
            "totalCandidatos": d.get("totalCandidatos"),
        }
        row.update(_first_candidato(d.get("candidato")))
        row.update(stamps)
        votos.append(row)

    return cabecera, votos


# --- Escritura de chunks ---------------------------------------------------


def _chunk_path(table: str, run_ts_ms: int, chunk_idx: int) -> Path:
    date = ms_to_lima_date(run_ts_ms)
    part = FACT_DIR / table / f"snapshot_date={date}"
    part.mkdir(parents=True, exist_ok=True)
    return part / f"{run_ts_ms}-{chunk_idx:05d}.parquet"


def _flush_chunk(
    run_ts_ms: int,
    chunk_idx: int,
    cabecera_buf: list[dict[str, Any]],
    votos_buf: list[dict[str, Any]],
) -> None:
    if cabecera_buf:
        df = pl.DataFrame(cabecera_buf, infer_schema_length=None)
        df.write_parquet(
            _chunk_path("actas_cabecera", run_ts_ms, chunk_idx), compression="zstd"
        )
    if votos_buf:
        df = pl.DataFrame(votos_buf, infer_schema_length=None)
        df.write_parquet(
            _chunk_path("actas_votos", run_ts_ms, chunk_idx), compression="zstd"
        )


# --- Loop principal --------------------------------------------------------


async def _fetch_one(
    c: OnpeClient,
    acta_id: int,
    id_mesa: int,
    ubigeo: str,
    snapshot_ts_ms: int,
) -> tuple[int, dict[str, Any] | None, list[dict[str, Any]], str | None]:
    """Fetch + normalize. Devuelve (acta_id, cabecera, votos, error_o_None)."""
    try:
        data = await acta_detalle(c, acta_id)
    except OnpeError as e:
        return acta_id, None, [], str(e)
    cab, votos = normalize_acta(data, acta_id, id_mesa, ubigeo, snapshot_ts_ms)
    return acta_id, cab, votos, None


async def snapshot_actas(
    c: OnpeClient,
    df_mesas: pl.DataFrame,
    cfg: SnapshotConfig | None = None,
    resume_run_ts_ms: int | None = None,
    limit: int | None = None,
) -> tuple[Checkpoint, dict[str, int]]:
    """Corre el snapshot completo (o hasta `limit` actas). Resumable."""
    cfg = cfg or SnapshotConfig()
    tasks = enumerate_tasks(df_mesas, cfg.elecciones)
    if limit is not None:
        tasks = tasks[:limit]

    # Checkpoint: reanudar o arrancar nuevo.
    if resume_run_ts_ms is not None:
        ck = Checkpoint.load(resume_run_ts_ms)
        log.info(
            "reanudando run_ts_ms=%d, completados=%d/%d, chunk_idx=%d",
            ck.run_ts_ms,
            len(ck.completed_acta_ids),
            ck.total_expected,
            ck.next_chunk_idx,
        )
    else:
        now = utc_now_ms()
        ck = Checkpoint(
            run_ts_ms=now,
            started_lima_iso=ms_to_lima_iso(now),
            elecciones=cfg.elecciones,
            total_expected=len(tasks),
        )
        ck.save()
        log.info("nuevo run_ts_ms=%d, total=%d", ck.run_ts_ms, ck.total_expected)

    # Filtrar tareas ya completadas.
    pending = [t for t in tasks if t[0] not in ck.completed_acta_ids]
    log.info("pendientes: %d", len(pending))

    cabecera_buf: list[dict[str, Any]] = []
    votos_buf: list[dict[str, Any]] = []
    stats = {"ok": 0, "vacias": 0, "fallidas": 0}

    # Cada cuántos batches logeamos progreso a INFO. 10 batches = 5000 actas.
    # Mantener bajo para no spamear; el checkpoint on-disk es la fuente de verdad.
    log_every_n_batches = 10

    for bidx, i in enumerate(range(0, len(pending), cfg.batch)):
        batch = pending[i : i + cfg.batch]
        results = await asyncio.gather(
            *(_fetch_one(c, aid, im, ub, ck.run_ts_ms) for aid, im, ub, _ in batch)
        )
        for acta_id, cab, votos, err in results:
            if err is not None:
                ck.failed.append({"acta_id": acta_id, "error": err})
                stats["fallidas"] += 1
                continue
            ck.completed_acta_ids.add(acta_id)
            if cab is None:
                stats["vacias"] += 1
                continue
            cabecera_buf.append(cab)
            votos_buf.extend(votos)
            stats["ok"] += 1

        # Checkpoint por batch: en caída perdemos a lo sumo `cfg.batch` actas.
        ck.save()

        if len(cabecera_buf) >= cfg.chunk_size:
            _flush_chunk(ck.run_ts_ms, ck.next_chunk_idx, cabecera_buf, votos_buf)
            log.info(
                "chunk %d flusheado (%d cab / %d votos), progreso %d/%d",
                ck.next_chunk_idx,
                len(cabecera_buf),
                len(votos_buf),
                len(ck.completed_acta_ids),
                ck.total_expected,
            )
            cabecera_buf = []
            votos_buf = []
            ck.next_chunk_idx += 1
            ck.save()
        elif (bidx + 1) % log_every_n_batches == 0:
            log.info(
                "progreso %d/%d (ok=%d vacias=%d fallidas=%d, buffer=%d)",
                len(ck.completed_acta_ids),
                ck.total_expected,
                stats["ok"],
                stats["vacias"],
                stats["fallidas"],
                len(cabecera_buf),
            )

    # Flush final de lo que quedó en buffer.
    if cabecera_buf or votos_buf:
        _flush_chunk(ck.run_ts_ms, ck.next_chunk_idx, cabecera_buf, votos_buf)
        log.info(
            "chunk final %d flusheado (%d cab / %d votos)",
            ck.next_chunk_idx,
            len(cabecera_buf),
            len(votos_buf),
        )
        ck.next_chunk_idx += 1
    ck.save()
    return ck, stats
