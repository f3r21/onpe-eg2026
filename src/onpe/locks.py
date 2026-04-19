"""Advisory file lock para coordinar jobs del pipeline.

Previene que snapshot_actas y daily_refresh corran simultáneamente —
comparten el rate-limit de CloudFront ONPE (15 rps techo). El loop de
aggregates (1 req/15min) es seguro en paralelo y no usa este lock.

Implementación: fcntl.flock(LOCK_EX | LOCK_NB) sobre un archivo en
data/state/. Advisory lock — los cooperadores lo respetan; procesos que
no lo adquieran no son bloqueados a nivel OS. Suficiente para nuestros
scripts propios.

Uso:
    from onpe.locks import PipelineLock, LockHeld

    try:
        with PipelineLock():
            ...  # sección crítica
    except LockHeld as e:
        log.error("lock ocupado por PID=%d desde %s", e.pid, e.started_iso)
        raise SystemExit(1)
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from onpe.storage import DATA_DIR, ms_to_lima_iso, utc_now_ms

log = logging.getLogger(__name__)

DEFAULT_LOCK_PATH = DATA_DIR / "state" / ".pipeline_lock"


class LockHeld(Exception):
    """El lock ya está ocupado por otro proceso."""

    def __init__(self, pid: int, started_iso: str, path: Path) -> None:
        self.pid = pid
        self.started_iso = started_iso
        self.path = path
        super().__init__(
            f"pipeline_lock ocupado por PID={pid} desde {started_iso} (archivo: {path})"
        )


@dataclass
class PipelineLock:
    """Context manager para el lock advisory del pipeline.

    Al adquirir escribe metadata {pid, started_iso, started_ts_ms}.
    En salida (incluyendo excepción) libera flock y borra el archivo.
    Si otro proceso mantiene el lock, lanza LockHeld inmediatamente (no espera).
    """

    path: Path = DEFAULT_LOCK_PATH
    metadata: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        self._fd: int | None = None

    def _existing_holder(self) -> tuple[int, str] | None:
        """Lee el metadata del lock existente si hay. None si no se puede parsear."""
        try:
            data = json.loads(self.path.read_text())
            return int(data.get("pid", -1)), str(data.get("started_iso", "?"))
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            return None

    def __enter__(self) -> "PipelineLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # O_RDWR + O_CREAT: crea el archivo si no existe; no trunca si sí existe.
        # El truncado lo hace el write después del flock.
        fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            holder = self._existing_holder()
            if holder is None:
                raise LockHeld(pid=-1, started_iso="?", path=self.path) from None
            raise LockHeld(pid=holder[0], started_iso=holder[1], path=self.path) from None

        # Truncar + escribir metadata fresca.
        now_ms = utc_now_ms()
        payload: dict[str, Any] = {
            "pid": os.getpid(),
            "started_ts_ms": now_ms,
            "started_iso": ms_to_lima_iso(now_ms),
        }
        if self.metadata:
            payload.update(self.metadata)
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps(payload).encode("utf-8"))
        os.fsync(fd)
        self._fd = fd
        log.info("pipeline_lock adquirido (pid=%d, archivo=%s)", os.getpid(), self.path)
        return self

    def __exit__(self, *_exc: object) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            log.info("pipeline_lock liberado")
