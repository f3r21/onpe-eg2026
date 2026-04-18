"""Validación rápida de la segregación Perú/Exterior en dim/."""

from __future__ import annotations

import polars as pl

from onpe.storage import DIM_DIR


def main() -> None:
    tablas = ["departamentos", "provincias", "distritos", "locales", "mesas"]
    print(f"{'tabla':<16} {'total':>8} {'peru':>8} {'exterior':>10}")
    print("-" * 48)
    for t in tablas:
        df = pl.read_parquet(DIM_DIR / f"{t}.parquet")
        if "idAmbitoGeografico" not in df.columns:
            print(f"{t:<16} {len(df):>8} {'sin col':>8} {'sin col':>10}")
            continue
        c = df.group_by("idAmbitoGeografico").agg(pl.len().alias("n"))
        peru = c.filter(pl.col("idAmbitoGeografico") == 1)["n"].sum()
        ext = c.filter(pl.col("idAmbitoGeografico") == 2)["n"].sum()
        print(f"{t:<16} {len(df):>8} {peru:>8} {ext:>10}")

    mesas = pl.read_parquet(DIM_DIR / "mesas.parquet")
    ext = mesas.filter(pl.col("idAmbitoGeografico") == 2)
    print()
    print("top 5 ciudades del exterior por nro de mesas:")
    print(
        ext.group_by(["ubigeoDistrito", "nombreDistrito"])
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .head(5)
    )


if __name__ == "__main__":
    main()
