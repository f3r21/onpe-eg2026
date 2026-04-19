"""Tests del PipelineLock (fcntl advisory)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from onpe.locks import LockHeld, PipelineLock


@pytest.fixture
def tmp_lock_path(tmp_path: Path) -> Path:
    return tmp_path / ".test_lock"


def test_acquire_escribe_metadata(tmp_lock_path: Path):
    """Al adquirir, el archivo contiene {pid, started_ts_ms, started_iso}."""
    with PipelineLock(path=tmp_lock_path):
        assert tmp_lock_path.exists()
        data = json.loads(tmp_lock_path.read_text())
        assert data["pid"] == os.getpid()
        assert data["started_ts_ms"] > 0
        assert data["started_iso"]


def test_release_borra_archivo(tmp_lock_path: Path):
    """Al salir del context, el archivo se borra."""
    with PipelineLock(path=tmp_lock_path):
        pass
    assert not tmp_lock_path.exists()


def test_metadata_custom_se_merge(tmp_lock_path: Path):
    """metadata={job: 'x'} se añade al payload del lock file."""
    with PipelineLock(path=tmp_lock_path, metadata={"job": "snapshot_actas"}):
        data = json.loads(tmp_lock_path.read_text())
        assert data["job"] == "snapshot_actas"
        assert data["pid"] == os.getpid()


def test_reentrante_en_misma_instancia_libera_bien(tmp_lock_path: Path):
    """Adquirir+liberar dos veces en secuencia funciona (no stale fd)."""
    with PipelineLock(path=tmp_lock_path):
        pass
    with PipelineLock(path=tmp_lock_path):
        assert tmp_lock_path.exists()
    assert not tmp_lock_path.exists()


def test_segundo_holder_subprocess_levanta_lockheld(tmp_lock_path: Path, tmp_path: Path):
    """Un segundo proceso que intenta adquirir mientras estamos dentro → LockHeld."""
    with PipelineLock(path=tmp_lock_path):
        # Subprocess corre un snippet que intenta re-adquirir el mismo lock.
        code = f"""
import sys
sys.path.insert(0, {str(Path(__file__).resolve().parent.parent / 'src')!r})
from pathlib import Path
from onpe.locks import PipelineLock, LockHeld
try:
    with PipelineLock(path=Path({str(tmp_lock_path)!r})):
        sys.exit(99)
except LockHeld as e:
    print(f"HELD_BY={{e.pid}}")
    sys.exit(42)
"""
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONPATH": "src"},
        )
        assert result.returncode == 42, (
            f"retcode={result.returncode}; stdout={result.stdout!r}; stderr={result.stderr!r}"
        )
        assert f"HELD_BY={os.getpid()}" in result.stdout


def test_excepcion_dentro_del_context_libera_lock(tmp_lock_path: Path):
    """Si la sección crítica levanta, el lock se libera igual (__exit__ corre)."""
    with pytest.raises(RuntimeError):
        with PipelineLock(path=tmp_lock_path):
            assert tmp_lock_path.exists()
            raise RuntimeError("boom")
    assert not tmp_lock_path.exists()


def test_lock_held_error_tiene_pid_e_iso(tmp_lock_path: Path, tmp_path: Path):
    """LockHeld.pid y .started_iso son accesibles en el handler."""
    with PipelineLock(path=tmp_lock_path):
        second = PipelineLock(path=tmp_lock_path)
        try:
            with second:
                pytest.fail("debería haber raised LockHeld")
        except LockHeld as e:
            assert e.pid == os.getpid()
            assert e.path == tmp_lock_path
            assert e.started_iso
