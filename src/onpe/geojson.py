"""Descarga y cache de GeoJSONs de ONPE SPA.

La SPA Angular usa amCharts5 con geodata estática. El país completo con sus
26 departamentos + Callao (PE-LKT es una geometry de Lago Titicaca) viene
en un solo archivo:

    /assets/lib/amcharts5/geodata/json/peruLow.json  (~38 KB, FeatureCollection)

Cada feature tiene:
- id: ubigeoDepartamento ONPE (ej. '140000' para Lima, '240000' para Callao)
- properties.name: nombre legible
- properties.id: código ISO (ej. 'PE-AMA')
- geometry: Polygon / MultiPolygon

NO se encontraron GeoJSONs separados por departamento ni por provincia en la
SPA pública. Para provincias/distritos se recomienda INEI o OpenStreetMap
en una task posterior.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

BASE_URL = "https://resultadoelectoral.onpe.gob.pe"
PERU_LOW_PATH = "/assets/lib/amcharts5/geodata/json/peruLow.json"

# Headers requeridos por CloudFront. Sin el Referer correcto devuelve el index.html
# del SPA (content-type text/html) en lugar del JSON.
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "Referer": f"{BASE_URL}/main/resumen",
}


def download_peru_low(dst: Path, force: bool = False) -> Path:
    """Descarga peruLow.json y valida content-type JSON + shape GeoJSON.

    Args:
        dst: path de destino (se crean directorios padre si no existen).
        force: si False y el archivo ya existe, skippea.

    Returns:
        path absoluto al archivo escrito (o ya existente si skippeado).

    Raises:
        httpx.HTTPError: si el request falla.
        ValueError: si la respuesta no es JSON válido o no tiene features.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and not force:
        log.info("skip: %s ya existe (usar force=True para re-descargar)", dst)
        return dst

    url = f"{BASE_URL}{PERU_LOW_PATH}"
    log.info("descargando %s", url)
    with httpx.Client(timeout=30, headers=DEFAULT_HEADERS) as c:
        r = c.get(url)
        r.raise_for_status()
        ct = r.headers.get("content-type", "")
        if "application/json" not in ct:
            raise ValueError(
                f"response content-type={ct!r} no es JSON. "
                "Probablemente CloudFront devolvió el fallback HTML del SPA — "
                "revisar headers Referer."
            )
        data = r.json()

    if data.get("type") != "FeatureCollection":
        raise ValueError(f"GeoJSON inválido: type={data.get('type')!r}")
    n_features = len(data.get("features", []))
    if n_features < 20:
        raise ValueError(f"GeoJSON sospechoso: solo {n_features} features (esperado ≥24)")

    # Atomic write: tmp + rename.
    tmp = dst.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False))
    tmp.replace(dst)
    log.info("escrito: %s (%d features, %.1f KB)", dst, n_features, dst.stat().st_size / 1024)
    return dst


def load_peru_low(path: Path) -> dict:
    """Carga el GeoJSON persistido. Raise si no existe."""
    if not path.exists():
        raise FileNotFoundError(f"{path} no existe. Correr download_peru_low() antes.")
    return json.loads(path.read_text())
