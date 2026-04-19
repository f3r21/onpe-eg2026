"""Descarga los GeoJSONs estáticos de la SPA ONPE y los persiste en data/geojson/.

Estado empírico al 2026-04-18:
- peruLow.json (país + 26 deptos + Callao): DISPONIBLE, ~38 KB
- Deptos individuales: NO expuestos en la SPA (cada feat ya está en peruLow)
- Provincias individuales: NO expuestos en la SPA — deferred a INEI/OSM

Uso:
    uv run python scripts/download_geojsons.py
    uv run python scripts/download_geojsons.py --force   # re-descarga aunque exista
"""

from __future__ import annotations

import argparse
import logging

from onpe.geojson import download_peru_low, load_peru_low
from onpe.storage import DATA_DIR

GEOJSON_DIR = DATA_DIR / "geojson"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="re-descargar aunque ya exista")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("download_geojsons")

    peru_low = GEOJSON_DIR / "peruLow.json"
    download_peru_low(peru_low, force=args.force)

    # Validación: cargar y reportar contenido
    data = load_peru_low(peru_low)
    feats = data["features"]
    deptos = sorted((f["id"], f["properties"].get("name")) for f in feats if str(f.get("id")).isdigit())
    otros = [(f["id"], f["properties"].get("name")) for f in feats if not str(f.get("id")).isdigit()]
    log.info("peruLow: %d features (%d deptos numéricos + %d otros)", len(feats), len(deptos), len(otros))
    for _id, name in deptos[:3]:
        log.info("  depto %s = %s", _id, name)
    log.info("  ...")
    for _id, name in deptos[-3:]:
        log.info("  depto %s = %s", _id, name)
    if otros:
        log.info("  otros: %s", otros)

    log.info("NOTA: provincias y distritos GeoJSON no están en la SPA ONPE.")
    log.info("      Para granularidad mayor usar INEI o OpenStreetMap (task futura).")


if __name__ == "__main__":
    main()
