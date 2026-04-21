"""Descarga + normaliza el padrón RENIEC del trimestre más cercano a EG2026.

Flujo:
  1. Descargar los 2 CSV oficiales de datosabiertos.gob.pe (OPP 1 base + OPP 4 vigencia).
  2. Parse con Polars lazy, filtrar Edad >= 18.
  3. Agregar por ubigeo Perú + por país Extranjero.
  4. Unir base con desglose de vigencia.
  5. Escribir data/dim/padron.parquet + data/dim/padron_resumen.json.

Para EG2026 (12-abr-2026) el snapshot óptimo es Q1 2026 (publicado 2026-03-XX).
Output ~1,892 distritos Perú + 150 países extranjero ≈ 2,042 filas.

Uso:
    uv run python scripts/crawl_reniec_padron.py                        # Q1 2026 default
    uv run python scripts/crawl_reniec_padron.py --trimestre 2025_12    # otro trimestre
    uv run python scripts/crawl_reniec_padron.py --skip-download        # usa CSVs ya descargados
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import polars as pl

from onpe.reniec_padron import (
    TRIMESTRES,
    PadronSummary,
    aggregate_base_by_ubigeo,
    aggregate_vigencia_by_ubigeo,
    download_trimestre,
    merge_base_with_vigencia,
    read_base,
    read_vigencia,
    summarize,
)
from onpe.storage import DATA_DIR, DIM_DIR, write_dim

log = logging.getLogger("crawl_reniec_padron")


def _resolve_csv_paths(raw_dir: Path, trimestre: str) -> tuple[Path, Path]:
    """Localiza los CSV ya descargados. Intenta el naming canónico y luego legacy."""
    canonical_base = raw_dir / f"padron_{trimestre}_base.csv"
    canonical_vig = raw_dir / f"padron_{trimestre}_vigencia.csv"
    if canonical_base.exists() and canonical_vig.exists():
        return canonical_base, canonical_vig

    # Naming legacy de runs previos: `padron_YYYY_MM.01.csv` y `.02_vigencia.csv`.
    legacy_base = raw_dir / f"padron_{trimestre}.01.csv"
    legacy_vig = raw_dir / f"padron_{trimestre}.02_vigencia.csv"
    if legacy_base.exists() and legacy_vig.exists():
        return legacy_base, legacy_vig

    raise FileNotFoundError(
        f"no se encontraron CSVs para trimestre {trimestre} en {raw_dir}. "
        f"Ejecuta sin --skip-download para descargarlos."
    )


def build_padron(
    trimestre: str,
    raw_dir: Path,
    skip_download: bool,
) -> tuple[PadronSummary, Path]:
    if skip_download:
        base_path, vig_path = _resolve_csv_paths(raw_dir, trimestre)
        log.info("usando CSVs existentes base=%s vig=%s", base_path, vig_path)
    else:
        base_path, vig_path = download_trimestre(trimestre, raw_dir)

    log.info("parseando base CSV (%d MB)", base_path.stat().st_size // (1024 * 1024))
    lazy_base = read_base(base_path)

    log.info("parseando vigencia CSV (%d MB)", vig_path.stat().st_size // (1024 * 1024))
    lazy_vig = read_vigencia(vig_path)

    log.info("agregando base por ubigeo / país")
    base_df = aggregate_base_by_ubigeo(lazy_base)

    log.info("agregando vigencia por ubigeo / país")
    vig_df = aggregate_vigencia_by_ubigeo(lazy_vig)

    log.info("merge base + vigencia")
    merged = merge_base_with_vigencia(base_df, vig_df).with_columns(
        pl.lit(trimestre).alias("fuente_trimestre")
    )

    out_path = write_dim("padron", merged)
    log.info(
        "escrito %s (%d filas, %d columnas)",
        out_path,
        merged.shape[0],
        merged.shape[1],
    )

    base_total = int(merged["total_electores"].sum())
    summary = summarize(merged, trimestre=trimestre, base_total=base_total)
    return summary, out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--trimestre",
        default="2026_03",
        choices=sorted(TRIMESTRES),
        help="trimestre RENIEC (default 2026_03, el más reciente pre-EG2026)",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DATA_DIR / "raw" / "reniec",
        help="dir para CSVs raw (default data/raw/reniec)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="no redescargar, usar CSVs ya presentes en raw-dir",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    summary, out_path = build_padron(
        trimestre=args.trimestre,
        raw_dir=args.raw_dir,
        skip_download=args.skip_download,
    )

    summary_path = DIM_DIR / "padron_resumen.json"
    summary_path.write_text(
        json.dumps(
            {
                "trimestre": summary.trimestre,
                "total_electores": summary.total_electores,
                "total_peru": summary.total_peru,
                "total_extranjero": summary.total_extranjero,
                "distritos_peru": summary.distritos_peru,
                "paises_extranjero": summary.paises_extranjero,
                "parquet": str(out_path.relative_to(DATA_DIR)),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    log.info("resumen escrito en %s", summary_path)

    print()
    print(f"padrón RENIEC {summary.trimestre}")
    print(f"  total electores (Edad >= 18)     : {summary.total_electores:>12,}")
    print(f"    Perú                           : {summary.total_peru:>12,}")
    print(f"    Extranjero                     : {summary.total_extranjero:>12,}")
    print(f"  distritos Perú                   : {summary.distritos_peru:>12,}")
    print(f"  países con voto exterior         : {summary.paises_extranjero:>12,}")
    print(f"  parquet                          : {out_path}")


if __name__ == "__main__":
    main()
