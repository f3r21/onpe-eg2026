"""Descarga y cache de GeoJSONs de ONPE SPA.

La SPA Angular usa amCharts5 con geodata estática. Hay 3 niveles de detalle:

1. País completo (`peruLow.json`, ~38 KB, FeatureCollection con 26 deptos + Callao)
2. Departamento individual (`departamentos/{ubigeo}.json`, una FeatureCollection
   con polígonos de las provincias del departamento)
3. Provincia individual (`provincias/{ubigeo}.json`, FeatureCollection con
   polígonos de los distritos de esa provincia)

Descubierto empíricamente 2026-04-19 via DevTools capturing de navegación por
mapa del SPA (/main/actas).

Paths:
    /assets/lib/amcharts5/geodata/json/peruLow.json
    /assets/lib/amcharts5/geodata/json/departamentos/{ubigeoDepartamento}.json
    /assets/lib/amcharts5/geodata/json/provincias/{ubigeoProvincia}.json

Cada feature tiene id=ubigeo, properties.name, geometry (Polygon/MultiPolygon).
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Iterable

import httpx

log = logging.getLogger(__name__)

BASE_URL = "https://resultadoelectoral.onpe.gob.pe"
PERU_LOW_PATH = "/assets/lib/amcharts5/geodata/json/peruLow.json"
DEPTO_PATH_TMPL = "/assets/lib/amcharts5/geodata/json/departamentos/{ubigeo}.json"
PROV_PATH_TMPL = "/assets/lib/amcharts5/geodata/json/provincias/{ubigeo}.json"

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


def _download_single_geojson(
    client: httpx.Client,
    url: str,
    dst: Path,
    force: bool = False,
) -> bool:
    """Descarga y valida un GeoJSON individual. Devuelve True si se descargó/existía OK."""
    if dst.exists() and not force:
        return True
    r = client.get(url)
    if r.status_code != 200:
        log.warning("skip %s: status=%d", url, r.status_code)
        return False
    ct = r.headers.get("content-type", "")
    if "application/json" not in ct:
        log.warning("skip %s: content-type=%s (no es JSON)", url, ct[:30])
        return False
    data = r.json()
    if data.get("type") not in ("FeatureCollection", "Feature", "GeometryCollection"):
        log.warning("skip %s: type=%r no es GeoJSON válido", url, data.get("type"))
        return False
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False))
    tmp.replace(dst)
    return True


def download_departamentos(
    dst_dir: Path,
    ubigeos: Iterable[str],
    rate_sleep: float = 0.3,
    force: bool = False,
) -> dict[str, bool]:
    """Descarga GeoJSONs de todos los ubigeos de departamento.

    Args:
        dst_dir: directorio destino (ej. data/geojson/departamentos/)
        ubigeos: lista de ubigeos de departamento ("010000", "020000", ...)
        rate_sleep: delay entre requests (respeta CloudFront)
        force: re-descargar aunque exista.

    Returns:
        dict {ubigeo: success_bool}
    """
    dst_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, bool] = {}
    with httpx.Client(timeout=30, headers=DEFAULT_HEADERS) as client:
        for ubigeo in ubigeos:
            url = f"{BASE_URL}{DEPTO_PATH_TMPL.format(ubigeo=ubigeo)}"
            dst = dst_dir / f"{ubigeo}.json"
            ok = _download_single_geojson(client, url, dst, force=force)
            results[ubigeo] = ok
            log.info("depto %s: %s", ubigeo, "OK" if ok else "SKIP")
            time.sleep(rate_sleep)
    return results


def download_provincias(
    dst_dir: Path,
    ubigeos: Iterable[str],
    rate_sleep: float = 0.3,
    force: bool = False,
) -> dict[str, bool]:
    """Descarga GeoJSONs de todos los ubigeos de provincia.

    Args:
        dst_dir: directorio destino (ej. data/geojson/provincias/)
        ubigeos: lista ubigeos provincia ("010100", "040100", ...)
        rate_sleep: delay entre requests
        force: re-descargar aunque exista.
    """
    dst_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, bool] = {}
    with httpx.Client(timeout=30, headers=DEFAULT_HEADERS) as client:
        for ubigeo in ubigeos:
            url = f"{BASE_URL}{PROV_PATH_TMPL.format(ubigeo=ubigeo)}"
            dst = dst_dir / f"{ubigeo}.json"
            ok = _download_single_geojson(client, url, dst, force=force)
            results[ubigeo] = ok
            time.sleep(rate_sleep)
    return results
