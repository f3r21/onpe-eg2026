"""Empaqueta el dataset onpe-eg2026 para publicación multi-plataforma.

Genera `datasets/<version>/` listo para subir a Zenodo, Kaggle, HuggingFace, y
GitHub Release. El paquete contiene:

- `parquet/` — curated (5 tablas: cabecera, votos, votos_tidy, candidatos, linea_tiempo, archivos)
  + dim (mesas, distritos_electorales, distritos, departamentos, provincias, locales, padron, resoluciones)
  + analytics (anomalias, anomalias_resumen)
- `csv/` — CSV presidencial + resumen por distrito + top partidos + tidy voto-mesa-partido
- `sqlite/` — base onpe_eg2026.db con las 4 tablas curated principales
- `geojson/` — 222 GeoJSONs ONPE (país + deptos + provs)
- `docs/` — DATA_DICTIONARY, METHODOLOGY, LIMITATIONS, CITATION, README del release
- `notebooks/` — 5 Jupyter demo
- `CHECKSUMS.txt` — SHA256 por archivo para integridad
- `README.md` — landing page del release (resumen, quickstart)
- `CITATION.cff`, `dataset-metadata.json` — copiados del root

Uso:
    uv run python scripts/prepare_release.py --version v1.0
    uv run python scripts/prepare_release.py --version v1.0 --dry-run
    uv run python scripts/prepare_release.py --version v1.0 --skip-csv --skip-sqlite
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import polars as pl

from onpe.storage import DATA_DIR

log = logging.getLogger("prepare_release")

REPO_ROOT = Path(__file__).resolve().parent.parent

# Tablas curated que van en el release (todas las que están bajo data/curated/).
CURATED_TABLES = (
    "actas_cabecera",
    "actas_votos",
    "actas_votos_tidy",
    "actas_candidatos",
    "actas_linea_tiempo",
    "actas_archivos",
)

# Tablas dim que van en el release.
DIM_TABLES = (
    "mesas",
    "distritos_electorales",
    "distritos",
    "departamentos",
    "provincias",
    "locales",
    "padron",
    "resoluciones",
)

# Tablas analytics (output del detect_anomalies).
ANALYTICS_TABLES = ("anomalias",)

# Docs a copiar al release.
DOCS_FILES = (
    "docs/DATA_DICTIONARY.md",
    "docs/METHODOLOGY.md",
    "docs/LIMITATIONS.md",
    "docs/ARCHITECTURE.md",
)

# Archivos root a copiar.
ROOT_FILES = (
    "CITATION.cff",
    "dataset-metadata.json",
    "LICENSE",
)


@dataclass
class ReleasePlan:
    version: str
    out_dir: Path
    skip_csv: bool
    skip_sqlite: bool
    skip_geojson: bool
    sqlite_full: bool = False


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def copy_parquet(plan: ReleasePlan) -> list[Path]:
    """Copia las tablas parquet curated + dim + analytics."""
    dest = plan.out_dir / "parquet"
    dest.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []

    for name in CURATED_TABLES:
        src = DATA_DIR / "curated" / f"{name}.parquet"
        if not src.exists():
            log.warning("curated/%s.parquet no existe — skip", name)
            continue
        dst = dest / f"{name}.parquet"
        shutil.copy2(src, dst)
        copied.append(dst)
        log.info("parquet curated/%s.parquet -> %s", name, dst)

    for name in DIM_TABLES:
        src = DATA_DIR / "dim" / f"{name}.parquet"
        if not src.exists():
            log.warning("dim/%s.parquet no existe — skip", name)
            continue
        dst = dest / f"dim_{name}.parquet"
        shutil.copy2(src, dst)
        copied.append(dst)
        log.info("parquet dim/%s.parquet -> %s", name, dst)

    for name in ANALYTICS_TABLES:
        src = DATA_DIR / "analytics" / f"{name}.parquet"
        if not src.exists():
            log.info("analytics/%s.parquet no existe — skip", name)
            continue
        dst = dest / f"analytics_{name}.parquet"
        shutil.copy2(src, dst)
        copied.append(dst)
        log.info("parquet analytics/%s.parquet -> %s", name, dst)

    return copied


def export_csv(plan: ReleasePlan) -> list[Path]:
    """Exporta 4 CSVs representativos del dataset.

    Mantiene tamaño razonable (Kaggle free tier < 20 GB) exportando vistas
    útiles, no el detalle completo de 18M votos.
    """
    if plan.skip_csv:
        return []
    dest = plan.out_dir / "csv"
    dest.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []

    # 1. Cabecera completa (463k × 33 cols) — ~50 MB
    cab_path = DATA_DIR / "curated" / "actas_cabecera.parquet"
    if cab_path.exists():
        df = pl.read_parquet(cab_path)
        out = dest / "actas_cabecera.csv"
        df.write_csv(out)
        copied.append(out)
        log.info(
            "csv actas_cabecera: %d filas -> %s (%.1f MB)",
            df.shape[0],
            out,
            out.stat().st_size / 1e6,
        )

    # 2. Presidencial agregado por distrito × partido (pequeño, útil)
    tidy_path = DATA_DIR / "curated" / "actas_votos_tidy.parquet"
    if tidy_path.exists():
        df = (
            pl.scan_parquet(tidy_path)
            .filter(pl.col("idEleccion") == 10)
            .group_by(
                [
                    "idDistritoElectoral",
                    "ubigeoDepartamento",
                    "ubigeoDistrito",
                    "nombreDistrito",
                    "partido",
                    "ccodigo",
                    "es_especial",
                ]
            )
            .agg(pl.col("nvotos").sum().alias("nvotos"))
            .sort(
                ["idDistritoElectoral", "ubigeoDistrito", pl.col("nvotos")],
                descending=[False, False, True],
            )
            .collect()
        )
        out = dest / "presidencial_por_distrito_x_partido.csv"
        df.write_csv(out)
        copied.append(out)
        log.info(
            "csv presidencial distrito×partido: %d filas -> %s (%.1f MB)",
            df.shape[0],
            out,
            out.stat().st_size / 1e6,
        )

    # 3. Totales nacionales por partido × elección
    if tidy_path.exists():
        df = (
            pl.scan_parquet(tidy_path)
            .group_by(["idEleccion", "partido", "ccodigo", "es_especial"])
            .agg(pl.col("nvotos").sum().alias("nvotos"))
            .sort(["idEleccion", pl.col("nvotos")], descending=[False, True])
            .collect()
        )
        out = dest / "totales_nacional_por_partido.csv"
        df.write_csv(out)
        copied.append(out)
        log.info(
            "csv totales nacional: %d filas -> %s (%.1f MB)",
            df.shape[0],
            out,
            out.stat().st_size / 1e6,
        )

    # 4. Resoluciones (lightweight)
    res_path = DATA_DIR / "dim" / "resoluciones.parquet"
    if res_path.exists():
        out = dest / "resoluciones.csv"
        pl.read_parquet(res_path).write_csv(out)
        copied.append(out)
        log.info("csv resoluciones -> %s", out)

    # 5. Padron por distrito
    padron_path = DATA_DIR / "dim" / "padron.parquet"
    if padron_path.exists():
        out = dest / "padron_reniec_por_distrito.csv"
        pl.read_parquet(padron_path).write_csv(out)
        copied.append(out)
        log.info("csv padron -> %s (%.1f MB)", out, out.stat().st_size / 1e6)

    return copied


def export_sqlite(plan: ReleasePlan) -> list[Path]:
    """Exporta las tablas principales a un archivo SQLite único.

    Por default excluye `actas_votos_tidy` (18.6M rows, ~1.8 GB) para mantener
    el archivo en ~30 MB. Usar `--sqlite-full` para incluirla.
    """
    if plan.skip_sqlite:
        return []
    dest = plan.out_dir / "sqlite"
    dest.mkdir(parents=True, exist_ok=True)
    db_path = dest / "onpe_eg2026.db"
    if db_path.exists():
        db_path.unlink()

    con = sqlite3.connect(db_path)
    try:
        tables = {
            "actas_cabecera": DATA_DIR / "curated" / "actas_cabecera.parquet",
            "dim_padron": DATA_DIR / "dim" / "padron.parquet",
            "dim_resoluciones": DATA_DIR / "dim" / "resoluciones.parquet",
            "dim_mesas": DATA_DIR / "dim" / "mesas.parquet",
        }
        if plan.sqlite_full:
            tables["actas_votos_tidy"] = DATA_DIR / "curated" / "actas_votos_tidy.parquet"
        for tbl, path in tables.items():
            if not path.exists():
                log.warning("skip sqlite tabla %s: fuente no existe", tbl)
                continue
            df = pl.read_parquet(path)
            # Polars.to_pandas requiere pandas; usamos write_database_raw path via sqlite3 bulk insert.
            _bulk_insert(con, tbl, df)
            log.info("sqlite %s: %d filas insertadas", tbl, df.shape[0])
    finally:
        con.commit()
        con.close()

    log.info("sqlite escrito %s (%.1f MB)", db_path, db_path.stat().st_size / 1e6)
    return [db_path]


def _bulk_insert(con: sqlite3.Connection, tbl: str, df: pl.DataFrame) -> None:
    """Inserta un DataFrame de Polars en SQLite sin pasar por pandas."""
    cols = df.columns
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    con.execute(f'CREATE TABLE "{tbl}" ({", ".join(f""" "{c}" """ for c in cols)})')
    con.executemany(
        f'INSERT INTO "{tbl}" ({cols_sql}) VALUES ({placeholders})',
        df.iter_rows(),
    )


def copy_geojson(plan: ReleasePlan) -> list[Path]:
    if plan.skip_geojson:
        return []
    src_dir = DATA_DIR / "geojson"
    if not src_dir.exists():
        log.warning("geojson/ no existe — skip")
        return []
    dest = plan.out_dir / "geojson"
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src_dir, dest)
    paths = sorted(dest.rglob("*.json"))
    total_bytes = sum(p.stat().st_size for p in paths)
    log.info("geojson: %d archivos copiados (%.1f MB)", len(paths), total_bytes / 1e6)
    return paths


def copy_docs(plan: ReleasePlan) -> list[Path]:
    dest = plan.out_dir / "docs"
    dest.mkdir(parents=True, exist_ok=True)
    copied: list[Path] = []
    for rel_path in DOCS_FILES:
        src = REPO_ROOT / rel_path
        if not src.exists():
            log.warning("doc no existe: %s", rel_path)
            continue
        dst = dest / src.name
        shutil.copy2(src, dst)
        copied.append(dst)
    return copied


def copy_notebooks(plan: ReleasePlan) -> list[Path]:
    src_dir = REPO_ROOT / "notebooks"
    if not src_dir.exists():
        log.warning("notebooks/ no existe — skip")
        return []
    dest = plan.out_dir / "notebooks"
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src_dir, dest, ignore=shutil.ignore_patterns(".ipynb_checkpoints"))
    paths = sorted(dest.rglob("*.ipynb"))
    log.info("notebooks: %d copiados", len(paths))
    return paths


def copy_root_files(plan: ReleasePlan) -> list[Path]:
    copied: list[Path] = []
    for rel in ROOT_FILES:
        src = REPO_ROOT / rel
        if not src.exists():
            log.warning("root file no existe: %s", rel)
            continue
        dst = plan.out_dir / src.name
        shutil.copy2(src, dst)
        copied.append(dst)
    return copied


def write_readme(plan: ReleasePlan) -> Path:
    """Escribe README.md del release con resumen ejecutivo + quickstart."""
    md = f"""# onpe-eg2026 — Dataset Release {plan.version}

Dataset estructurado de las **Elecciones Generales Perú 2026** (primera vuelta, 12-abr-2026).
Scrape oficial del portal ONPE + fuentes gubernamentales peruanas (RENIEC padrón,
El Peruano resoluciones, GeoJSONs ONPE).

**Fuente canónica (código + docs)**: https://github.com/f3r21/onpe-eg2026

## Contenido del paquete

| Directorio | Contenido |
|---|---|
| `parquet/` | Tablas curated (`actas_cabecera`, `actas_votos`, `actas_votos_tidy`, `actas_candidatos`, `actas_linea_tiempo`, `actas_archivos`) + dims (`dim_mesas`, `dim_padron`, `dim_resoluciones`, …) + `analytics_anomalias` |
| `csv/` | `actas_cabecera.csv`, `presidencial_por_distrito_x_partido.csv`, `totales_nacional_por_partido.csv`, `padron_reniec_por_distrito.csv`, `resoluciones.csv` |
| `sqlite/` | `onpe_eg2026.db` con `actas_cabecera`, `actas_votos_tidy`, `dim_padron`, `dim_resoluciones`, `dim_mesas` |
| `geojson/` | 222 archivos GeoJSON ONPE (país + 25 deptos + 196 provs) |
| `docs/` | `DATA_DICTIONARY.md`, `METHODOLOGY.md`, `LIMITATIONS.md`, `ARCHITECTURE.md` |
| `notebooks/` | 5 Jupyter demo (getting-started, participación, resultados, exterior, anomalías) |
| `CHECKSUMS.txt` | SHA256 de cada archivo |
| `CITATION.cff`, `dataset-metadata.json` | Metadata estándar |

## Quickstart

```python
import polars as pl

# Totales por partido en Presidencial
votos = pl.read_parquet("parquet/actas_votos_tidy.parquet")
votos.filter(pl.col("idEleccion") == 10).group_by("partido").agg(
    pl.col("nvotos").sum()
).sort("nvotos", descending=True)
```

## Licencias

- **Datos**: CC-BY-4.0 — atribución a ONPE + este dataset (ver `CITATION.cff`).
- **Código** del pipeline (disponible en GitHub): MIT.

## Limitaciones

Ver `docs/LIMITATIONS.md` para la lista completa (estado pre-proclamación, voto
preferencial no capturado, PDFs binarios no incluidos, etc.).

## Citar este dataset

Ver `CITATION.cff` en formato Citation File Format (CFF 1.2.0) y BibTeX equivalente.

## Reportar issues

https://github.com/f3r21/onpe-eg2026/issues
"""
    out = plan.out_dir / "README.md"
    out.write_text(md, encoding="utf-8")
    log.info("README: %s", out)
    return out


def generate_checksums(plan: ReleasePlan) -> Path:
    """Calcula SHA256 de cada archivo del release. Escribe CHECKSUMS.txt."""
    checksums_path = plan.out_dir / "CHECKSUMS.txt"
    # Iterar todos los archivos excepto el checksums mismo.
    lines: list[str] = []
    for path in sorted(plan.out_dir.rglob("*")):
        if path.is_file() and path.name != "CHECKSUMS.txt":
            rel = path.relative_to(plan.out_dir).as_posix()
            digest = _sha256(path)
            lines.append(f"{digest}  {rel}")
    checksums_path.write_text("\n".join(lines) + "\n")
    log.info("checksums: %d archivos -> %s", len(lines), checksums_path)
    return checksums_path


def summarize(plan: ReleasePlan, artifacts: list[Path]) -> dict:
    by_dir: dict[str, dict] = {}
    for p in artifacts:
        rel = p.relative_to(plan.out_dir)
        parent = rel.parts[0] if len(rel.parts) > 1 else "."
        by_dir.setdefault(parent, {"files": 0, "bytes": 0})
        by_dir[parent]["files"] += 1
        by_dir[parent]["bytes"] += p.stat().st_size
    total_bytes = sum(e["bytes"] for e in by_dir.values())
    return {
        "version": plan.version,
        "total_files": sum(e["files"] for e in by_dir.values()),
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / 1e6, 2),
        "by_dir": by_dir,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", default="v1.0", help="versión del release (default v1.0)")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="dir de salida (default datasets/<version>)",
    )
    parser.add_argument("--dry-run", action="store_true", help="no escribir, solo listar")
    parser.add_argument("--skip-csv", action="store_true", help="omitir export CSV")
    parser.add_argument("--skip-sqlite", action="store_true", help="omitir export SQLite")
    parser.add_argument("--skip-geojson", action="store_true", help="omitir copia de GeoJSONs")
    parser.add_argument(
        "--sqlite-full",
        action="store_true",
        help="incluir actas_votos_tidy (18.6M rows, ~2 GB) en SQLite (default excluida)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    out_dir = args.out_dir or (REPO_ROOT / "datasets" / args.version)
    plan = ReleasePlan(
        version=args.version,
        out_dir=out_dir,
        skip_csv=args.skip_csv,
        skip_sqlite=args.skip_sqlite,
        skip_geojson=args.skip_geojson,
        sqlite_full=args.sqlite_full,
    )

    if args.dry_run:
        print(f"[DRY-RUN] empaquetaría {plan.version} en {plan.out_dir}")
        print(f"[DRY-RUN] curated: {CURATED_TABLES}")
        print(f"[DRY-RUN] dim: {DIM_TABLES}")
        print(f"[DRY-RUN] analytics: {ANALYTICS_TABLES}")
        print(
            f"[DRY-RUN] skip-csv={plan.skip_csv} skip-sqlite={plan.skip_sqlite} skip-geojson={plan.skip_geojson}"
        )
        return

    if plan.out_dir.exists():
        log.warning("limpiando directorio existente: %s", plan.out_dir)
        shutil.rmtree(plan.out_dir)
    plan.out_dir.mkdir(parents=True)

    artifacts: list[Path] = []
    artifacts.extend(copy_parquet(plan))
    artifacts.extend(export_csv(plan))
    artifacts.extend(export_sqlite(plan))
    artifacts.extend(copy_geojson(plan))
    artifacts.extend(copy_docs(plan))
    artifacts.extend(copy_notebooks(plan))
    artifacts.extend(copy_root_files(plan))
    artifacts.append(write_readme(plan))
    artifacts.append(generate_checksums(plan))

    summary = summarize(plan, artifacts)
    summary_path = plan.out_dir / "release_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False))

    print()
    print(f"Release {plan.version} empaquetado en {plan.out_dir}")
    print(f"  total archivos : {summary['total_files']:>4}")
    print(f"  total tamaño   : {summary['total_mb']:>8.1f} MB")
    print("  por directorio :")
    for dirname, info in sorted(summary["by_dir"].items()):
        print(f"    {dirname:12s}: {info['files']:>3} archivos, {info['bytes'] / 1e6:>7.1f} MB")


if __name__ == "__main__":
    main()
