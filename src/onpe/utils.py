"""Utilidades pequenas compartidas entre scripts del pipeline."""

from __future__ import annotations

import hashlib


def shard_of(key: str | int, shard_n: int) -> int:
    """Retorna el indice de shard (0..shard_n-1) para una key dada.

    Usa md5 (no el hash() built-in) para garantizar:
    - Determinismo cross-process (hash() esta randomizado por PYTHONHASHSEED).
    - Estabilidad cross-platform (mismo resultado en Mac y Windows).
    - Mismo resultado entre Python 3.x versions.

    Se usa para particionar el universo de ids entre multiples workers
    (download_pdfs.py, snapshot_actas.py) sin coordinacion central.
    """
    digest = hashlib.md5(str(key).encode("utf-8"), usedforsecurity=False).hexdigest()
    return int(digest, 16) % shard_n


def parse_shard_spec(spec: str) -> tuple[int, int]:
    """Parsea 'M/N' -> (M, N). Valida 0 <= M < N.

    Raises:
        SystemExit con mensaje explicativo si el formato es invalido.
    """
    try:
        m_str, n_str = spec.split("/", 1)
        m, n = int(m_str), int(n_str)
    except ValueError as e:
        raise SystemExit(f"--shard debe ser M/N (e.g. 1/3), se recibio {spec!r}") from e
    if not (0 <= m < n):
        raise SystemExit(f"shard invalido: {m}/{n} (debe ser 0 <= M < N)")
    return m, n
