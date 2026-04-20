"""Descarga los GeoJSONs estáticos de la SPA ONPE y los persiste en data/geojson/.

Estado al 2026-04-19 (paths descubiertos en DevTools navegando /main/actas):
- peruLow.json (país + 26 deptos + Callao): DISPONIBLE, ~38 KB
- departamentos/{ubigeo}.json (25 deptos Perú individuales): DISPONIBLE
- provincias/{ubigeo}.json (196 provincias Perú individuales): DISPONIBLE

Uso:
    uv run python scripts/download_geojsons.py                      # todo el set
    uv run python scripts/download_geojsons.py --only-country       # solo peruLow
    uv run python scripts/download_geojsons.py --force              # re-descargar
"""

from __future__ import annotations

import argparse
import logging

import polars as pl

from onpe.geojson import (
    download_departamentos,
    download_peru_low,
    download_provincias,
    load_peru_low,
)
from onpe.storage import DATA_DIR, DIM_DIR

GEOJSON_DIR = DATA_DIR / "geojson"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="re-descargar aunque ya exista")
    parser.add_argument(
        "--only-country", action="store_true",
        help="solo descargar peruLow.json (skip deptos + provs)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("download_geojsons")

    # 1. País completo
    peru_low = GEOJSON_DIR / "peruLow.json"
    download_peru_low(peru_low, force=args.force)
    data = load_peru_low(peru_low)
    log.info("peruLow: %d features", len(data["features"]))

    if args.only_country:
        return

    # 2. Departamentos individuales (25 ubigeos Perú, 01-25)
    log.info("")
    log.info("=== Descargando departamentos ===")
    depto_ubigeos = [f"{i:02d}0000" for i in range(1, 26)]
    depto_results = download_departamentos(
        GEOJSON_DIR / "departamentos", depto_ubigeos, force=args.force
    )
    ok_depto = sum(1 for v in depto_results.values() if v)
    log.info("deptos: %d/%d OK", ok_depto, len(depto_results))

    # 3. Provincias individuales (196 Perú)
    log.info("")
    log.info("=== Descargando provincias ===")
    provs_path = DIM_DIR / "provincias.parquet"
    if not provs_path.exists():
        log.warning("falta %s — correr scripts/crawl_dims.py primero", provs_path)
        return
    df_provs = pl.read_parquet(provs_path).filter(pl.col("idAmbitoGeografico") == 1)
    prov_ubigeos = df_provs.get_column("ubigeo").to_list()
    log.info("target: %d provincias", len(prov_ubigeos))
    prov_results = download_provincias(
        GEOJSON_DIR / "provincias", prov_ubigeos, force=args.force
    )
    ok_prov = sum(1 for v in prov_results.values() if v)
    log.info("provincias: %d/%d OK", ok_prov, len(prov_results))

    log.info("")
    log.info("=== Total descargas ===")
    log.info("  peruLow.json (país): 1")
    log.info("  departamentos: %d", ok_depto)
    log.info("  provincias: %d", ok_prov)


if __name__ == "__main__":
    main()
