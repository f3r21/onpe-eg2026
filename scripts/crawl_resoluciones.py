"""Hidrata el registry de resoluciones EG2026 contra El Peruano.

Flujo:
  1. Parsea `data/registry/resoluciones_eg2026.yaml`.
  2. Para cada `op` fetchea `busquedas.elperuano.pe/dispositivo/NL/{op}` y
     extrae `__NEXT_DATA__.pageProps.dispositivo`.
  3. Opcionalmente descarga el PDF a `data/raw/resoluciones/{op}.pdf`.
  4. Escribe `data/dim/resoluciones.parquet` con metadata normalizada.
  5. Escribe `data/dim/resoluciones_resumen.json` con totales por institución y categoría.

Uso:
    uv run python scripts/crawl_resoluciones.py
    uv run python scripts/crawl_resoluciones.py --no-pdf          # solo metadata
    uv run python scripts/crawl_resoluciones.py --registry custom.yaml
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import httpx
import polars as pl

from onpe.resoluciones import (
    DEFAULT_HEADERS,
    Resolucion,
    build_resolucion,
    download_pdf,
    fetch_dispositivo,
    parse_registry_yaml,
)
from onpe.storage import DATA_DIR, DIM_DIR, write_dim

log = logging.getLogger("crawl_resoluciones")

DEFAULT_REGISTRY = DATA_DIR.parent / "data" / "registry" / "resoluciones_eg2026.yaml"
DEFAULT_PDF_DIR = DATA_DIR / "raw" / "resoluciones"


def crawl(registry_path: Path, pdf_dir: Path, download: bool) -> list[Resolucion]:
    entries = parse_registry_yaml(registry_path)
    log.info("registry cargado: %d entradas", len(entries))

    resoluciones: list[Resolucion] = []
    with httpx.Client(headers=DEFAULT_HEADERS, timeout=30.0) as client:
        for i, entry in enumerate(entries, 1):
            op = entry["op"]
            log.info("[%d/%d] procesando op=%s", i, len(entries), op)
            raw = fetch_dispositivo(op, client=client)
            resolucion = build_resolucion(op, entry, raw)
            resoluciones.append(resolucion)

            if download and resolucion.url_ok and resolucion.url_pdf:
                pdf_path = pdf_dir / f"{op}.pdf"
                if pdf_path.exists() and pdf_path.stat().st_size > 0:
                    log.info("PDF existe, skip: %s", pdf_path.name)
                else:
                    try:
                        download_pdf(resolucion.url_pdf, pdf_path, client=client)
                    except httpx.HTTPError as e:
                        log.warning("op=%s PDF falló: %s", op, e)

            # Jitter suave entre requests para no tickear rate-limits del WAF.
            time.sleep(0.5)

    return resoluciones


def build_parquet(resoluciones: list[Resolucion]) -> pl.DataFrame:
    rows = [asdict(r) for r in resoluciones]
    return pl.DataFrame(rows)


def write_resumen(df: pl.DataFrame, path: Path) -> None:
    summary = {
        "total": df.shape[0],
        "url_ok": int(df["url_ok"].sum()),
        "por_institucion": {
            k: v for k, v in df.group_by("institucion").agg(pl.len().alias("n")).iter_rows()
        },
        "por_tag_categoria": {
            k: v for k, v in df.group_by("tag_categoria").agg(pl.len().alias("n")).iter_rows()
        },
        "por_tag_proceso": {
            k: v for k, v in df.group_by("tag_proceso").agg(pl.len().alias("n")).iter_rows()
        },
    }
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry",
        type=Path,
        default=DEFAULT_REGISTRY,
        help="YAML del registry (default data/registry/resoluciones_eg2026.yaml)",
    )
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=DEFAULT_PDF_DIR,
        help="dir de PDFs descargados (default data/raw/resoluciones)",
    )
    parser.add_argument(
        "--no-pdf",
        action="store_true",
        help="no descargar PDFs, solo metadata",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.registry.exists():
        log.critical("registry no encontrado: %s", args.registry)
        sys.exit(1)

    resoluciones = crawl(
        registry_path=args.registry,
        pdf_dir=args.pdf_dir,
        download=not args.no_pdf,
    )

    df = build_parquet(resoluciones)
    out_path = write_dim("resoluciones", df)
    log.info("escrito %s (%d filas, %d columnas)", out_path, df.shape[0], df.shape[1])

    summary_path = DIM_DIR / "resoluciones_resumen.json"
    write_resumen(df, summary_path)
    log.info("resumen escrito en %s", summary_path)

    ok = int(df["url_ok"].sum())
    print()
    print("Registry resoluciones EG2026")
    print(f"  total entradas       : {df.shape[0]:>4}")
    print(f"  hidratadas (url_ok)  : {ok:>4}")
    print(f"  fallback (sin fetch) : {df.shape[0] - ok:>4}")
    print(f"  parquet              : {out_path}")
    print(f"  resumen              : {summary_path}")
    if not args.no_pdf:
        pdfs = list(args.pdf_dir.glob("*.pdf"))
        total_bytes = sum(p.stat().st_size for p in pdfs)
        print(f"  PDFs descargados     : {len(pdfs):>4} ({total_bytes / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
