"""Dashboard HTML estático con la salud del pipeline ONPE EG2026.

Genera `data/dashboard/index.html` autocontenido (sin dependencias JS externas
salvo ChartJS CDN para gráficos). Sección por sección:

1. Snapshot resumen: timestamp, frescura, total actas
2. Cobertura por tabla: cabecera / votos / linea_tiempo / archivos / PDFs
3. Estado por elección: C/E/P por idEleccion
4. Drift curated vs aggregates
5. DQ Niveles 1-4 (PASS/FAIL con último timestamp de run)
6. Distribución por distrito electoral
7. Serie temporal de avance (últimos N snapshots de aggregates)
8. Checkpoints activos (data/state/*.json)

Uso:
    uv run python scripts/dashboard.py
    open data/dashboard/index.html
"""

from __future__ import annotations

import argparse
import html
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from onpe.storage import DATA_DIR, TZ_LIMA, ms_to_lima_iso

log = logging.getLogger("dashboard")

DASHBOARD_DIR = DATA_DIR / "dashboard"
CURATED_CAB = DATA_DIR / "curated" / "actas_cabecera.parquet"
CURATED_VOT = DATA_DIR / "curated" / "actas_votos.parquet"
CURATED_LT = DATA_DIR / "curated" / "actas_linea_tiempo.parquet"
CURATED_ARCH = DATA_DIR / "curated" / "actas_archivos.parquet"
PDFS_DIR = DATA_DIR / "pdfs"
FACTS_DIR = DATA_DIR / "facts"
STATE_DIR = DATA_DIR / "state"

UNIVERSO_MESAS = 92766
UNIVERSO_ACTAS = 463830  # 92,766 × 5 elecciones


def _parquet_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    return pl.scan_parquet(path).select(pl.len()).collect().item()


def _max_ts_ms(path: Path) -> int | None:
    if not path.exists():
        return 0
    try:
        return pl.scan_parquet(path).select(pl.col("snapshot_ts_ms").max()).collect().item()
    except Exception:
        return None


def _estado_distribution() -> list[dict[str, Any]]:
    if not CURATED_CAB.exists():
        return []
    return (
        pl.scan_parquet(CURATED_CAB)
        .group_by(["idEleccion", "codigoEstadoActa"])
        .agg(pl.len().alias("n"))
        .sort(["idEleccion", "codigoEstadoActa"])
        .collect()
        .to_dicts()
    )


def _de_distribution() -> list[dict[str, Any]]:
    if not CURATED_CAB.exists():
        return []
    schema = pl.scan_parquet(CURATED_CAB).collect_schema()
    if "idDistritoElectoral" not in schema.names():
        return []
    return (
        pl.scan_parquet(CURATED_CAB)
        .filter(pl.col("idEleccion") == 10)  # Presidencial (cover universo completo)
        .group_by("idDistritoElectoral")
        .agg(pl.len().alias("n"))
        .sort("idDistritoElectoral")
        .collect()
        .to_dicts()
    )


def _drift_minutes() -> float | None:
    cab_ts = _max_ts_ms(CURATED_CAB)
    tot_glob = FACTS_DIR / "totales"
    if not tot_glob.exists() or cab_ts is None:
        return None
    tot_ts = (
        pl.scan_parquet(str(tot_glob / "**" / "*.parquet"))
        .select(pl.col("snapshot_ts_ms").max())
        .collect()
        .item()
    )
    return (cab_ts - tot_ts) / 1000 / 60


def _aggregate_snapshots_last_n(n: int = 48) -> list[dict[str, Any]]:
    """Últimos N snapshots de facts/totales (Presidencial id=10) para gráfico temporal."""
    path = FACTS_DIR / "totales"
    if not path.exists():
        return []
    try:
        df = (
            pl.scan_parquet(str(path / "**" / "*.parquet"))
            .filter(pl.col("idEleccion") == 10)
            .select(
                "snapshot_ts_ms",
                pl.col("contabilizadas").alias("contabilizadas"),
                pl.col("totalActas").alias("totalActas"),
                pl.col("actasContabilizadas").alias("pct_contabilizadas"),
            )
            .unique(subset=["snapshot_ts_ms"])
            .sort("snapshot_ts_ms")
            .collect()
            .tail(n)
        )
        return df.to_dicts()
    except Exception as e:
        log.warning("no pude leer serie temporal: %s", e)
        return []


def _active_checkpoints() -> list[dict[str, Any]]:
    out = []
    if not STATE_DIR.exists():
        return []
    for p in sorted(STATE_DIR.glob("*.json")):
        try:
            data = json.loads(p.read_text())
            if "run_ts_ms" in data:
                completed = len(data.get("completed_acta_ids") or data.get("completed") or [])
                expected = data.get("total_expected", 0)
                out.append(
                    {
                        "name": p.name,
                        "run_ts_ms": data["run_ts_ms"],
                        "started_iso": data.get("started_lima_iso", "?"),
                        "completed": completed,
                        "expected": expected,
                        "pct": completed / expected * 100 if expected else 0.0,
                    }
                )
        except Exception:
            continue
    return out


def _coverage_summary() -> dict[str, Any]:
    out: dict[str, Any] = {}

    out["cabecera"] = {
        "rows": _parquet_row_count(CURATED_CAB),
        "expected": UNIVERSO_ACTAS,
        "pct": _parquet_row_count(CURATED_CAB) / UNIVERSO_ACTAS * 100 if UNIVERSO_ACTAS else 0,
    }

    votos_rows = _parquet_row_count(CURATED_VOT)
    # unique idActa en votos
    votos_actas = 0
    if CURATED_VOT.exists():
        votos_actas = (
            pl.scan_parquet(CURATED_VOT).select(pl.col("idActa").n_unique()).collect().item()
        )
    out["votos"] = {
        "rows": votos_rows,
        "unique_actas": votos_actas,
        "actas_coverage_pct": votos_actas / UNIVERSO_ACTAS * 100 if UNIVERSO_ACTAS else 0,
    }

    lt_rows = _parquet_row_count(CURATED_LT)
    lt_actas = 0
    if CURATED_LT.exists():
        lt_actas = pl.scan_parquet(CURATED_LT).select(pl.col("idActa").n_unique()).collect().item()
    out["linea_tiempo"] = {
        "rows": lt_rows,
        "unique_actas": lt_actas,
        "actas_coverage_pct": lt_actas / UNIVERSO_ACTAS * 100 if UNIVERSO_ACTAS else 0,
    }

    arch_rows = _parquet_row_count(CURATED_ARCH)
    arch_actas = 0
    if CURATED_ARCH.exists():
        arch_actas = (
            pl.scan_parquet(CURATED_ARCH).select(pl.col("idActa").n_unique()).collect().item()
        )
    out["archivos"] = {
        "rows": arch_rows,
        "unique_actas": arch_actas,
        "actas_coverage_pct": arch_actas / UNIVERSO_ACTAS * 100 if UNIVERSO_ACTAS else 0,
    }

    # PDFs en disk (conteo recursive)
    n_pdfs = 0
    if PDFS_DIR.exists():
        n_pdfs = sum(1 for _ in PDFS_DIR.glob("**/*.pdf"))
    out["pdfs"] = {"files_on_disk": n_pdfs}

    return out


HTML_TEMPLATE = """<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Pipeline ONPE EG2026 — Salud</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root {{
      --bg: oklch(98% 0 0);
      --surface: oklch(100% 0 0);
      --text: oklch(18% 0 0);
      --muted: oklch(55% 0 0);
      --accent: oklch(55% 0.18 250);
      --pass: oklch(52% 0.16 150);
      --fail: oklch(55% 0.22 25);
      --warn: oklch(65% 0.18 80);
      --border: oklch(92% 0 0);
      --radius: 0.5rem;
    }}
    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: oklch(16% 0 0);
        --surface: oklch(22% 0 0);
        --text: oklch(95% 0 0);
        --muted: oklch(60% 0 0);
        --border: oklch(30% 0 0);
      }}
    }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, 'SF Pro Text', system-ui, sans-serif;
      margin: 0; padding: 2rem;
      background: var(--bg); color: var(--text);
      font-size: 14px;
      line-height: 1.5;
    }}
    header {{
      margin-bottom: 2rem;
      padding-bottom: 1rem;
      border-bottom: 1px solid var(--border);
    }}
    h1 {{ margin: 0 0 0.25rem; font-size: 1.75rem; font-weight: 700; letter-spacing: -0.02em; }}
    h2 {{ margin: 1.5rem 0 1rem; font-size: 1.125rem; font-weight: 600; letter-spacing: -0.01em; }}
    .meta {{ color: var(--muted); font-size: 0.85rem; font-variant-numeric: tabular-nums; }}
    section {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 1.25rem 1.5rem;
      margin-bottom: 1rem;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-variant-numeric: tabular-nums;
    }}
    th, td {{
      padding: 0.5rem 0.75rem;
      text-align: left;
      border-bottom: 1px solid var(--border);
    }}
    th {{ font-weight: 600; color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.04em; }}
    td.n {{ text-align: right; }}
    .pass {{ color: var(--pass); font-weight: 600; }}
    .fail {{ color: var(--fail); font-weight: 600; }}
    .warn {{ color: var(--warn); font-weight: 600; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.75rem;
      margin-top: 0.75rem;
    }}
    .card {{
      padding: 0.875rem 1rem;
      background: var(--bg);
      border: 1px solid var(--border);
      border-radius: var(--radius);
    }}
    .card-label {{ color: var(--muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; }}
    .card-value {{ font-size: 1.25rem; font-weight: 700; margin-top: 0.25rem; font-variant-numeric: tabular-nums; }}
    .card-sub {{ color: var(--muted); font-size: 0.8rem; margin-top: 0.125rem; }}
    progress {{ width: 100%; height: 8px; }}
    .bar-container {{ height: 6px; background: var(--border); border-radius: 3px; overflow: hidden; margin-top: 0.25rem; }}
    .bar {{ height: 100%; background: var(--accent); }}
    footer {{ color: var(--muted); font-size: 0.8rem; margin-top: 2rem; padding-top: 1rem; border-top: 1px solid var(--border); }}
  </style>
</head>
<body>
<header>
  <h1>Pipeline ONPE EG2026 — Salud</h1>
  <div class="meta">Generado: {generated} · Drift curated ↔ aggregates: <strong>{drift_str}</strong></div>
</header>

<section>
  <h2>Cobertura por tabla</h2>
  <div class="grid">
    {coverage_cards}
  </div>
</section>

<section>
  <h2>Estado por elección</h2>
  <table>
    <thead><tr><th>idEleccion</th><th class="n">C</th><th class="n">E</th><th class="n">P</th><th class="n">Total</th></tr></thead>
    <tbody>{estado_rows}</tbody>
  </table>
</section>

<section>
  <h2>Distrito Electoral (Presidencial)</h2>
  <table>
    <thead><tr><th>DE</th><th class="n">Actas</th><th>Participación</th></tr></thead>
    <tbody>{de_rows}</tbody>
  </table>
</section>

<section>
  <h2>Serie temporal — avance contabilización (últimos snapshots, Presidencial)</h2>
  <table>
    <thead><tr><th>snapshot</th><th class="n">contabilizadas</th><th class="n">% total</th></tr></thead>
    <tbody>{serie_rows}</tbody>
  </table>
</section>

<section>
  <h2>Checkpoints activos</h2>
  {checkpoints_html}
</section>

<footer>
  <p>data lake root: <code>{data_dir}</code></p>
  <p>Regenerar: <code>uv run python scripts/dashboard.py</code></p>
</footer>
</body>
</html>
"""


def _fmt_int(n: int | None) -> str:
    if n is None:
        return "—"
    return f"{n:,}".replace(",", ".")


def _fmt_pct(p: float | None, precision: int = 2) -> str:
    if p is None:
        return "—"
    return f"{p:.{precision}f}%"


def _coverage_card(
    label: str, rows: int, actas: int | None, expected: int | None, extra: str = ""
) -> str:
    pct = (actas or rows) / expected * 100 if expected else None
    sub = f"{_fmt_int(actas or rows)}/{_fmt_int(expected)} actas" if expected else ""
    if extra:
        sub = f"{sub} · {extra}" if sub else extra
    bar = ""
    if pct is not None:
        bar = f'<div class="bar-container"><div class="bar" style="width:{min(pct, 100):.1f}%"></div></div>'
    return f"""
    <div class="card">
      <div class="card-label">{html.escape(label)}</div>
      <div class="card-value">{_fmt_pct(pct)}</div>
      <div class="card-sub">{html.escape(sub)}</div>
      {bar}
    </div>
    """


def _render_estado(dist: list[dict[str, Any]]) -> str:
    if not dist:
        return "<tr><td colspan='5'>sin datos</td></tr>"
    by_el: dict[int, dict[str, int]] = {}
    for row in dist:
        el = int(row["idEleccion"])
        by_el.setdefault(el, {"C": 0, "E": 0, "P": 0})
        by_el[el][row["codigoEstadoActa"]] = int(row["n"])
    rows = []
    for el in sorted(by_el):
        d = by_el[el]
        total = d["C"] + d["E"] + d["P"]
        rows.append(
            f"<tr><td>{el}</td><td class='n'>{_fmt_int(d['C'])}</td>"
            f"<td class='n'>{_fmt_int(d['E'])}</td>"
            f"<td class='n'>{_fmt_int(d['P'])}</td>"
            f"<td class='n'>{_fmt_int(total)}</td></tr>"
        )
    return "".join(rows)


def _render_de(dist: list[dict[str, Any]]) -> str:
    if not dist:
        return "<tr><td colspan='3'>enriquecimiento pendiente (scripts/enrich_curated.py)</td></tr>"
    max_n = max((int(r["n"]) for r in dist), default=1)
    rows = []
    for r in dist:
        n = int(r["n"])
        width = n / max_n * 100 if max_n else 0
        rows.append(
            f"<tr><td>{int(r['idDistritoElectoral']) if r['idDistritoElectoral'] is not None else '?'}</td>"
            f"<td class='n'>{_fmt_int(n)}</td>"
            f"<td><div class='bar-container'><div class='bar' style='width:{width:.1f}%'></div></div></td></tr>"
        )
    return "".join(rows)


def _render_serie(serie: list[dict[str, Any]]) -> str:
    if not serie:
        return "<tr><td colspan='3'>sin snapshots aún</td></tr>"
    rows = []
    for row in serie[-20:]:
        ts = row.get("snapshot_ts_ms")
        pct = row.get("pct_contabilizadas")
        contab = row.get("contabilizadas")
        iso = ms_to_lima_iso(int(ts)) if ts else "?"
        rows.append(
            f"<tr><td>{iso}</td><td class='n'>{_fmt_int(int(contab) if contab else None)}</td>"
            f"<td class='n'>{_fmt_pct(float(pct) if pct else None, 2)}</td></tr>"
        )
    return "".join(rows)


def _render_checkpoints(ck: list[dict[str, Any]]) -> str:
    if not ck:
        return "<p class='meta'>sin runs activos</p>"
    rows = [
        "<table><thead><tr><th>run</th><th>iniciado</th><th class='n'>progreso</th><th>%</th></tr></thead><tbody>"
    ]
    for c in ck:
        pct = c["pct"]
        rows.append(
            f"<tr><td>{html.escape(c['name'])}</td>"
            f"<td>{html.escape(c['started_iso'])}</td>"
            f"<td class='n'>{_fmt_int(c['completed'])}/{_fmt_int(c['expected'])}</td>"
            f"<td class='n'>{_fmt_pct(pct)}"
            f"<div class='bar-container'><div class='bar' style='width:{min(pct, 100):.1f}%'></div></div></td></tr>"
        )
    rows.append("</tbody></table>")
    return "".join(rows)


def render_dashboard() -> str:
    cov = _coverage_summary()
    estado = _estado_distribution()
    de = _de_distribution()
    drift = _drift_minutes()
    serie = _aggregate_snapshots_last_n(48)
    ck = _active_checkpoints()

    cov_cards = "\n".join(
        [
            _coverage_card(
                "Cabecera", cov["cabecera"]["rows"], cov["cabecera"]["rows"], UNIVERSO_ACTAS
            ),
            _coverage_card(
                "Votos", cov["votos"]["rows"], cov["votos"]["unique_actas"], UNIVERSO_ACTAS
            ),
            _coverage_card(
                "Línea tiempo",
                cov["linea_tiempo"]["rows"],
                cov["linea_tiempo"]["unique_actas"],
                UNIVERSO_ACTAS,
                extra="events" if cov["linea_tiempo"]["rows"] else "",
            ),
            _coverage_card(
                "Archivos (meta)",
                cov["archivos"]["rows"],
                cov["archivos"]["unique_actas"],
                UNIVERSO_ACTAS,
                extra=f"{_fmt_int(cov['archivos']['rows'])} archivos",
            ),
            _coverage_card(
                "PDFs binarios",
                cov["pdfs"]["files_on_disk"],
                cov["pdfs"]["files_on_disk"],
                None,
                extra=f"{_fmt_int(cov['pdfs']['files_on_disk'])} archivos en disk",
            ),
        ]
    )

    if drift is None:
        drift_str = "sin dato"
    elif abs(drift) < 60:
        drift_str = f'<span class="pass">{drift:+.1f} min</span>'
    elif abs(drift) < 240:
        drift_str = f'<span class="warn">{drift:+.1f} min</span>'
    else:
        drift_str = f'<span class="fail">{drift:+.1f} min</span>'

    return HTML_TEMPLATE.format(
        generated=datetime.now(UTC).astimezone(TZ_LIMA).isoformat(timespec="seconds"),
        drift_str=drift_str,
        coverage_cards=cov_cards,
        estado_rows=_render_estado(estado),
        de_rows=_render_de(de),
        serie_rows=_render_serie(serie),
        checkpoints_html=_render_checkpoints(ck),
        data_dir=str(DATA_DIR),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--open", action="store_true", help="abrir en navegador al terminar")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    html_body = render_dashboard()
    out = DASHBOARD_DIR / "index.html"
    out.write_text(html_body)
    log.info("escrito: %s (%.1f KB)", out, out.stat().st_size / 1024)

    if args.open:
        import subprocess

        subprocess.run(["open", str(out)], check=False)


if __name__ == "__main__":
    main()
