"""Monitor weekly del catálogo ONPE en datosabiertos.gob.pe — detecta publicación EG2026.

Corre como one-shot (ideal cron semanal). Compara el listado actual contra
el snapshot previo guardado en `data/state/datosabiertos_catalog.json`. Si
detecta un dataset EG2026 nuevo, escribe alerta.

Output:
- data/state/datosabiertos_catalog.json — snapshot actual del catálogo ONPE
- data/state/.eg2026_alert — flag file si se detectó EG2026 (cron puede pollar)

Uso:
    # One-shot manual
    uv run python scripts/monitor_datosabiertos.py

    # Cron semanal (agregar a crontab):
    # 0 9 * * 1 cd /ruta/repo && uv run python scripts/monitor_datosabiertos.py

El exit code 0 = ok (publicado o no); 2 = detectó EG2026 nuevo (útil para cron wrappers).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict

from onpe.datosabiertos import Dataset, find_eg2026, list_onpe_datasets
from onpe.storage import DATA_DIR, ms_to_lima_iso, utc_now_ms

log = logging.getLogger("monitor_datosabiertos")

STATE_DIR = DATA_DIR / "state"
CATALOG_FILE = STATE_DIR / "datosabiertos_catalog.json"
EG2026_ALERT_FILE = STATE_DIR / ".eg2026_alert"


def load_previous_catalog() -> list[Dataset]:
    if not CATALOG_FILE.exists():
        return []
    data = json.loads(CATALOG_FILE.read_text())
    return [Dataset(**d) for d in data.get("datasets", [])]


def save_catalog(datasets: list[Dataset]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    now_ms = utc_now_ms()
    payload = {
        "last_check_ts_ms": now_ms,
        "last_check_iso": ms_to_lima_iso(now_ms),
        "n_datasets": len(datasets),
        "datasets": [asdict(d) for d in datasets],
    }
    tmp = CATALOG_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    tmp.replace(CATALOG_FILE)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-pages", type=int, default=5, help="páginas a escanear")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    log.info("consultando catálogo ONPE en datosabiertos.gob.pe...")
    try:
        datasets = list_onpe_datasets(max_pages=args.max_pages)
    except Exception as e:
        log.error("fallo al listar catálogo: %s", e)
        return 1

    log.info("catálogo: %d datasets encontrados", len(datasets))
    if not datasets:
        log.warning("catálogo vacío — posible bloqueo WAF")
        return 1

    previous = load_previous_catalog()
    previous_urls = {d.url for d in previous}
    new_datasets = [d for d in datasets if d.url not in previous_urls]
    if new_datasets:
        log.info("detectados %d dataset(s) nuevo(s) desde último check:", len(new_datasets))
        for d in new_datasets:
            log.info("  NUEVO: %s", d.title[:100])
            log.info("    %s", d.url)
    else:
        log.info("sin novedades desde último check")

    # Guardar snapshot actual
    save_catalog(datasets)

    # Detectar EG2026
    eg2026 = find_eg2026(datasets)
    if eg2026:
        log.warning("🎯 EG2026 DETECTADO en datosabiertos:")
        log.warning("  título: %s", eg2026.title)
        log.warning("  url: %s", eg2026.url)
        log.warning("  fecha: %s", eg2026.raw_date)
        # Flag file para downstream cron/scripts
        EG2026_ALERT_FILE.write_text(
            json.dumps(asdict(eg2026), ensure_ascii=False, indent=2)
        )
        log.warning("flag escrito: %s", EG2026_ALERT_FILE)
        log.warning("SIGUIENTE PASO: correr scripts/ingest_datosabiertos.py cuando esté listo")
        return 2  # exit code 2 = detected

    log.info("EG2026 NO publicado aún. Re-correr semanalmente.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
