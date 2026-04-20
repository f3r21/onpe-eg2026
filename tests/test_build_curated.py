"""Tests E2E de build_curated — dedup last-run-wins."""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


def _write_chunk(
    base: Path,
    table: str,
    run_ts_ms: int,
    chunk_idx: int,
    df: pl.DataFrame,
) -> Path:
    date = "2026-04-18"
    part = base / table / f"snapshot_date={date}" / f"run_ts_ms={run_ts_ms}"
    part.mkdir(parents=True, exist_ok=True)
    path = part / f"{chunk_idx:05d}.parquet"
    df.write_parquet(path, compression="zstd")
    return path


def _minimal_cabecera(idActa: int, run_ts_ms: int, estado: str = "C") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "idActa": [idActa],
            "idEleccion": [10],
            "idMesaRef": [5507],
            "ubigeoDistrito": ["040102"],
            "codigoMesa": ["005507"],
            "codigoEstadoActa": [estado],
            "totalVotosEmitidos": [100 + (run_ts_ms % 10)],  # distinto por run
            "totalVotosValidos": [95],
            "totalElectoresHabiles": [274],
            "totalAsistentes": [100],
            "porcentajeParticipacionCiudadana": [36.49],
            "codigoSolucionTecnologica": [1],
            "snapshot_ts_ms": [run_ts_ms],
            "snapshot_lima_iso": ["2026-04-18T09:00:00-05:00"],
        },
        schema={
            "idActa": pl.Int64,
            "idEleccion": pl.Int64,
            "idMesaRef": pl.Int64,
            "ubigeoDistrito": pl.String,
            "codigoMesa": pl.String,
            "codigoEstadoActa": pl.String,
            "totalVotosEmitidos": pl.Int64,
            "totalVotosValidos": pl.Int64,
            "totalElectoresHabiles": pl.Int64,
            "totalAsistentes": pl.Int64,
            "porcentajeParticipacionCiudadana": pl.Float64,
            "codigoSolucionTecnologica": pl.Int64,
            "snapshot_ts_ms": pl.Int64,
            "snapshot_lima_iso": pl.String,
        },
    )


def _minimal_votos(idActa: int, run_ts_ms: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "idActa": [idActa, idActa],
            "idEleccion": [10, 10],
            "ubigeoDistrito": ["040102", "040102"],
            "descripcion": ["PARTIDO X", "VOTOS EN BLANCO"],
            "es_especial": [False, True],
            "nvotos": [run_ts_ms % 100, 5],
            "snapshot_ts_ms": [run_ts_ms, run_ts_ms],
        },
        schema={
            "idActa": pl.Int64,
            "idEleccion": pl.Int64,
            "ubigeoDistrito": pl.String,
            "descripcion": pl.String,
            "es_especial": pl.Boolean,
            "nvotos": pl.Int64,
            "snapshot_ts_ms": pl.Int64,
        },
    )


@pytest.fixture
def tmp_data_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """DATA_DIR temporal; isola el test de data/ real."""
    monkeypatch.setenv("ONPE_DATA_DIR", str(tmp_path))
    # Re-importar módulos afectados para recoger el DATA_DIR nuevo.
    # build_curated calcula CURATED_DIR/FACTS_* al importar → purgar cache.
    for mod in list(sys.modules):
        if mod.startswith("onpe.") or mod == "onpe" or mod == "build_curated":
            del sys.modules[mod]
    return tmp_path


def test_build_curated_dedup_max_run_ts_ms(tmp_data_root: Path):
    """Mismo idActa en 2 runs → curated se queda con el run_ts_ms mayor."""
    from build_curated import build_cabecera, build_votos

    # Run 1 (más antiguo)
    _write_chunk(tmp_data_root / "facts", "actas_cabecera", 1000, 0, _minimal_cabecera(999, 1000))
    _write_chunk(tmp_data_root / "facts", "actas_votos", 1000, 0, _minimal_votos(999, 1000))

    # Run 2 (más reciente) — mismo idActa
    _write_chunk(tmp_data_root / "facts", "actas_cabecera", 2000, 0, _minimal_cabecera(999, 2000))
    _write_chunk(tmp_data_root / "facts", "actas_votos", 2000, 0, _minimal_votos(999, 2000))

    df_cab, latest = build_cabecera(dry_run=False)
    assert df_cab.height == 1, "dedup debe dejar 1 fila"
    assert df_cab["snapshot_ts_ms"][0] == 2000, "se queda con run_ts_ms mayor"

    n_votos = build_votos(latest, dry_run=False)
    assert n_votos == 2  # 2 filas del run 2


def test_build_curated_skip_tablas_vacias(tmp_data_root: Path):
    """Si facts/actas_linea_tiempo/ no tiene chunks, skip sin error."""
    from build_curated import _build_aux, build_cabecera

    _write_chunk(tmp_data_root / "facts", "actas_cabecera", 1000, 0, _minimal_cabecera(1, 1000))

    _, latest = build_cabecera(dry_run=False)
    # linea_tiempo directory no existe
    n = _build_aux(
        "actas_linea_tiempo", tmp_data_root / "facts" / "actas_linea_tiempo", latest, dry_run=False
    )
    assert n == 0


def test_build_curated_multiples_actas_multiples_runs(tmp_data_root: Path):
    """2 actas, 2 runs c/u — cada una dedupa a su max run."""
    from build_curated import build_cabecera

    # Acta 100: run 100 (antiguo) + run 500 (reciente)
    _write_chunk(tmp_data_root / "facts", "actas_cabecera", 100, 0, _minimal_cabecera(100, 100))
    _write_chunk(tmp_data_root / "facts", "actas_cabecera", 500, 0, _minimal_cabecera(100, 500))
    _write_chunk(tmp_data_root / "facts", "actas_votos", 100, 0, _minimal_votos(100, 100))
    _write_chunk(tmp_data_root / "facts", "actas_votos", 500, 0, _minimal_votos(100, 500))

    # Acta 200: solo run 300
    _write_chunk(tmp_data_root / "facts", "actas_cabecera", 300, 0, _minimal_cabecera(200, 300))
    _write_chunk(tmp_data_root / "facts", "actas_votos", 300, 0, _minimal_votos(200, 300))

    df_cab, _latest = build_cabecera(dry_run=False)
    assert df_cab.height == 2
    por_acta = {row["idActa"]: row["snapshot_ts_ms"] for row in df_cab.iter_rows(named=True)}
    assert por_acta[100] == 500  # max run para acta 100
    assert por_acta[200] == 300  # solo un run para acta 200
