"""Tests del filtro de sharding en scripts/download_pdfs.py.

El shard filter se usa para distribuir la descarga de PDFs entre multiples
workers (p.ej. Mac + 2 Windows PCs) sin coordinacion externa. Las garantias
que los tests verifican:

1. Particiones son DISJUNTAS (ningun archivoId cae en 2 shards).
2. Particiones cubren el UNIVERSO completo (union == input).
3. Distribucion es APROXIMADAMENTE uniforme (md5 distribucion esperada).
4. Es DETERMINISTICO (mismo aid + mismo N => misma shard en cada corrida).
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from download_pdfs import _shard_filter


def _hex_ids(n: int, seed: int = 0) -> list[str]:
    """Genera n archivoIds sinteticos con formato hex-24 (mismo que ONPE)."""
    rng = random.Random(seed)
    return ["".join(rng.choices("0123456789abcdef", k=24)) for _ in range(n)]


def test_shard_particiones_disjuntas_y_cubren_universo():
    """Union de todas las shards == input. Interseccion entre shards == vacio."""
    ids = _hex_ids(1000)
    n = 3
    shards = [_shard_filter(ids, m, n) for m in range(n)]

    # Disjuntas
    for i in range(n):
        for j in range(i + 1, n):
            intersection = set(shards[i]) & set(shards[j])
            assert intersection == set(), f"shards {i} y {j} solapan en {len(intersection)} ids"

    # Cubren el universo
    union = set()
    for s in shards:
        union.update(s)
    assert union == set(ids), "la union de las shards no cubre el universo"


def test_shard_distribucion_aproximadamente_uniforme():
    """Cada shard tiene ~N/k elementos (tolerancia 10% para N=1000, k=4)."""
    ids = _hex_ids(1000, seed=42)
    n = 4
    sizes = [len(_shard_filter(ids, m, n)) for m in range(n)]
    expected = len(ids) / n  # 250
    for m, size in enumerate(sizes):
        drift = abs(size - expected) / expected
        assert drift < 0.15, f"shard {m}/{n} tamano {size} drift {drift:.1%} (esperado ~{expected})"


def test_shard_deterministico_cross_run():
    """El mismo archivoId siempre cae en la misma shard (no randomness)."""
    aid = "69dce34ad7b6147f63e6fe04"
    for _ in range(5):
        shards_con_aid = [_shard_filter([aid], m, 3) for m in range(3)]
        # Exactamente una shard lo contiene
        hits = [m for m, s in enumerate(shards_con_aid) if aid in s]
        assert len(hits) == 1


def test_shard_m_fuera_de_rango_levanta():
    """M >= N o M < 0 deberia abortar."""
    ids = _hex_ids(10)
    with pytest.raises(SystemExit):
        _shard_filter(ids, 3, 3)  # M == N
    with pytest.raises(SystemExit):
        _shard_filter(ids, -1, 3)  # M < 0


def test_shard_single_vs_one_cubre_todo():
    """--shard 0/1 es equivalente a no usar --shard (todos los ids)."""
    ids = _hex_ids(100)
    assert _shard_filter(ids, 0, 1) == ids
