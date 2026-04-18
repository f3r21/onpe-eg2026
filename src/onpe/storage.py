"""Capa de almacenamiento Parquet.

Dos tipos de tablas:

- **dim/**: catálogos estáticos (departamentos, provincias, distritos, locales,
  distritos electorales). Un archivo por tabla, se sobreescribe en cada crawl.

- **facts/<tabla>/snapshot_date=YYYY-MM-DD/<snapshot_ts_ms>.parquet**: series
  temporales del avance. Particionado por fecha en hora Lima. DuckDB las lee
  con `SELECT * FROM 'data/facts/totales/**/*.parquet'`.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

TZ_LIMA = ZoneInfo("America/Lima")

_DEFAULT_ROOT = Path(__file__).resolve().parent.parent.parent / "data"
DATA_DIR = Path(os.environ.get("ONPE_DATA_DIR", _DEFAULT_ROOT))
DIM_DIR = DATA_DIR / "dim"
FACT_DIR = DATA_DIR / "facts"


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def ms_to_lima_date(ms: int) -> str:
    """Convierte epoch ms a YYYY-MM-DD en hora Lima (UTC-5)."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(TZ_LIMA).strftime("%Y-%m-%d")


def ms_to_lima_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(TZ_LIMA).isoformat()


def write_dim(name: str, df: pl.DataFrame) -> Path:
    """Escribe un catálogo estático (sobreescribe)."""
    DIM_DIR.mkdir(parents=True, exist_ok=True)
    path = DIM_DIR / f"{name}.parquet"
    df.write_parquet(path, compression="zstd")
    return path


def write_fact(table: str, df: pl.DataFrame, snapshot_ts_ms: int) -> Path:
    """Escribe un snapshot particionado por fecha Lima."""
    date = ms_to_lima_date(snapshot_ts_ms)
    part_dir = FACT_DIR / table / f"snapshot_date={date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    path = part_dir / f"{snapshot_ts_ms}.parquet"
    df.write_parquet(path, compression="zstd")
    return path
