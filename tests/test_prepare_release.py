"""Tests de scripts/prepare_release: helpers puros + plan invariants."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def release_module():
    path = Path(__file__).resolve().parent.parent / "scripts" / "prepare_release.py"
    spec = importlib.util.spec_from_file_location("prepare_release", path)
    mod = importlib.util.module_from_spec(spec)
    # Register en sys.modules antes de exec para que dataclass/__future__ annotations
    # puedan resolverse (de lo contrario dataclass __init__ falla con AttributeError).
    sys.modules["prepare_release"] = mod
    sys.argv = ["prepare_release"]
    spec.loader.exec_module(mod)
    return mod


def test_sha256_matches_known(release_module, tmp_path: Path):
    f = tmp_path / "mini.txt"
    payload = b"onpe-eg2026"
    f.write_bytes(payload)
    expected = hashlib.sha256(payload).hexdigest()
    assert release_module._sha256(f) == expected


def test_release_plan_dataclass(release_module, tmp_path: Path):
    plan = release_module.ReleasePlan(
        version="v1.0",
        out_dir=tmp_path,
        skip_csv=False,
        skip_sqlite=True,
        skip_geojson=False,
    )
    assert plan.version == "v1.0"
    assert plan.skip_sqlite is True
    assert plan.sqlite_full is False  # default


def test_summarize_agrupa_por_dir(release_module, tmp_path: Path):
    # Creamos archivos dummy en subcarpetas conocidas y verificamos el rollup.
    (tmp_path / "parquet").mkdir()
    (tmp_path / "parquet" / "a.parquet").write_bytes(b"x" * 1000)
    (tmp_path / "parquet" / "b.parquet").write_bytes(b"x" * 2000)
    (tmp_path / "csv").mkdir()
    (tmp_path / "csv" / "c.csv").write_bytes(b"y" * 500)

    plan = release_module.ReleasePlan(
        version="v1.0",
        out_dir=tmp_path,
        skip_csv=False,
        skip_sqlite=False,
        skip_geojson=False,
    )
    artifacts = [
        tmp_path / "parquet" / "a.parquet",
        tmp_path / "parquet" / "b.parquet",
        tmp_path / "csv" / "c.csv",
    ]
    summary = release_module.summarize(plan, artifacts)
    assert summary["version"] == "v1.0"
    assert summary["total_files"] == 3
    assert summary["total_bytes"] == 3500
    assert summary["by_dir"]["parquet"]["files"] == 2
    assert summary["by_dir"]["parquet"]["bytes"] == 3000
    assert summary["by_dir"]["csv"]["files"] == 1
    assert summary["by_dir"]["csv"]["bytes"] == 500


def test_generate_checksums_cubre_todos_archivos(release_module, tmp_path: Path):
    """El archivo CHECKSUMS.txt debe tener 1 línea por archivo (excepto él mismo)."""
    (tmp_path / "a.txt").write_bytes(b"hello")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_bytes(b"world")

    plan = release_module.ReleasePlan(
        version="v1.0",
        out_dir=tmp_path,
        skip_csv=False,
        skip_sqlite=False,
        skip_geojson=False,
    )
    path = release_module.generate_checksums(plan)
    content = path.read_text().strip().splitlines()
    # 2 archivos (a.txt + sub/b.txt), CHECKSUMS.txt es el que creó la función.
    assert len(content) == 2
    # Cada línea: <hash>  <rel_path>
    for line in content:
        digest, sep, rel = line.partition("  ")
        assert len(digest) == 64  # sha256 hex
        assert sep == "  "
        assert rel in {"a.txt", "sub/b.txt"}


def test_constants_listan_tablas_esperadas(release_module):
    """Regresión: no cambiar accidentalmente qué tablas entran al release."""
    assert "actas_cabecera" in release_module.CURATED_TABLES
    assert "actas_votos_tidy" in release_module.CURATED_TABLES
    assert "actas_candidatos" in release_module.CURATED_TABLES
    assert "padron" in release_module.DIM_TABLES
    assert "resoluciones" in release_module.DIM_TABLES
    assert "anomalias" in release_module.ANALYTICS_TABLES


def test_docs_files_existen(release_module):
    """Los docs que el script dice copiar deben existir en el repo."""
    repo_root = Path(__file__).resolve().parent.parent
    for rel in release_module.DOCS_FILES:
        assert (repo_root / rel).exists(), f"falta doc esperado: {rel}"
    for rel in release_module.ROOT_FILES:
        assert (repo_root / rel).exists(), f"falta root file esperado: {rel}"
