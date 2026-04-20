"""Genera mapa coroplético HTML interactivo del dataset EG2026.

Produce `data/dashboard/mapa_eg2026.html`: autocontenido (Leaflet desde CDN)
con 3 vistas switcheables:
  1. Presidencial — ganador por departamento
  2. Diputados — ganador por distrito electoral (overlay con provincias)
  3. Participación ciudadana — % por departamento (gradiente)

Uso:
    uv run python scripts/build_choropleth.py
    uv run python scripts/build_choropleth.py --open  # abre en browser
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

import polars as pl

from onpe.storage import DATA_DIR

log = logging.getLogger("choropleth")

CURATED_CAB = DATA_DIR / "curated" / "actas_cabecera.parquet"
CURATED_VOT = DATA_DIR / "curated" / "actas_votos.parquet"
GEOJSON_DIR = DATA_DIR / "geojson"
DASHBOARD_DIR = DATA_DIR / "dashboard"

# Paleta por partido (top 10) — semantic colors distinguibles.
PARTY_COLORS = {
    "FUERZA POPULAR": "#ff6b35",
    "JUNTOS POR EL PERÚ": "#e63946",
    "RENOVACIÓN POPULAR": "#118ab2",
    "PARTIDO DEL BUEN GOBIERNO": "#06a77d",
    "PARTIDO CÍVICO OBRAS": "#ffd166",
    "PARTIDO PAÍS PARA TODOS": "#8338ec",
    "AHORA NACIÓN - AN": "#a78bfa",
    "PRIMERO LA GENTE": "#ef476f",
    "PARTIDO SICREO": "#264653",
    "PARTIDO FRENTE DE LA ESPERANZA 2021": "#2a9d8f",
}
DEFAULT_COLOR = "#95a5a6"


def compute_ganadores_por_depto(cab: pl.DataFrame, vot: pl.DataFrame) -> dict[str, dict]:
    """Para idEleccion=10 (Presidencial): devuelve {ubigeoDepto: {partido, votos, pct, total_depto}}."""
    # cab tiene idAmbitoGeografico ya enriquecido; filtrar Perú (ambito=1) para mapa
    cab_peru = cab.filter(
        (pl.col("idEleccion") == 10) & (pl.col("idAmbitoGeografico") == 1)
    )
    # Extraer depto code from ubigeoDistrito (2 chars) → convertir a XX0000 formato dim
    votos_peru = (
        vot.filter((~pl.col("es_especial")) & (pl.col("idEleccion") == 10))
        .join(cab_peru.select("idActa", "ubigeoDistrito"), on="idActa", how="inner")
        .with_columns(
            (pl.col("ubigeoDistrito").str.slice(0, 2) + "0000").alias("depto_code")
        )
        .group_by(["depto_code", "descripcion"])
        .agg(pl.col("nvotos").sum().alias("total"))
    )
    # Total por depto
    total_depto = votos_peru.group_by("depto_code").agg(
        pl.col("total").sum().alias("sum_depto")
    )
    votos_peru = votos_peru.join(total_depto, on="depto_code").with_columns(
        (pl.col("total") / pl.col("sum_depto") * 100).alias("pct")
    )
    # Ganador por depto
    winners = votos_peru.sort(
        ["depto_code", "total"], descending=[False, True]
    ).unique(subset="depto_code", keep="first")

    out = {}
    for row in winners.iter_rows(named=True):
        out[row["depto_code"]] = {
            "partido": row["descripcion"],
            "votos": int(row["total"]),
            "pct": round(row["pct"], 2),
            "total_depto": int(row["sum_depto"]),
        }
    return out


def compute_participacion_por_depto(cab: pl.DataFrame) -> dict[str, dict]:
    """% participación por depto (Presidencial C)."""
    df = (
        cab.filter(
            (pl.col("idEleccion") == 10)
            & (pl.col("codigoEstadoActa") == "C")
            & (pl.col("idAmbitoGeografico") == 1)
        )
        .with_columns(
            (pl.col("ubigeoDistrito").str.slice(0, 2) + "0000").alias("depto_code")
        )
        .group_by("depto_code")
        .agg(
            pl.col("totalElectoresHabiles").sum().alias("hab"),
            pl.col("totalVotosEmitidos").sum().alias("emit"),
            pl.len().alias("n_actas"),
        )
        .with_columns((pl.col("emit") / pl.col("hab") * 100).alias("pct"))
    )
    return {
        row["depto_code"]: {
            "pct": round(row["pct"], 2),
            "emit": int(row["emit"]),
            "hab": int(row["hab"]),
            "n_actas": int(row["n_actas"]),
        }
        for row in df.iter_rows(named=True)
    }


def color_for_participation(pct: float) -> str:
    """Gradient verde para participación (65-85% típica)."""
    if pct >= 80:
        return "#1a5e3f"
    elif pct >= 75:
        return "#4a9d5b"
    elif pct >= 70:
        return "#8ecb94"
    elif pct >= 65:
        return "#c7e4c1"
    elif pct >= 60:
        return "#ede7a9"
    elif pct >= 50:
        return "#e8a07a"
    else:
        return "#c55a4a"


def render_html(
    peru_low: dict,
    ganadores_presidencial: dict[str, dict],
    participacion: dict[str, dict],
    meta: dict,
) -> str:
    """Genera HTML autocontenido con Leaflet + 3 vistas."""
    # Preparar GeoJSON con properties extendidas para cada vista
    features_enriched = []
    for feat in peru_low["features"]:
        feat_id = str(feat.get("id", ""))
        ganador = ganadores_presidencial.get(feat_id, {})
        part = participacion.get(feat_id, {})
        enriched = {
            **feat,
            "properties": {
                **feat["properties"],
                "depto_id": feat_id,
                "presidencial_partido": ganador.get("partido", "s/d"),
                "presidencial_votos": ganador.get("votos", 0),
                "presidencial_pct": ganador.get("pct", 0),
                "participacion_pct": part.get("pct", 0),
                "participacion_emit": part.get("emit", 0),
                "participacion_hab": part.get("hab", 0),
            },
        }
        features_enriched.append(enriched)

    geojson_str = json.dumps({
        "type": "FeatureCollection",
        "features": features_enriched,
    }, ensure_ascii=False)
    party_colors_str = json.dumps(PARTY_COLORS, ensure_ascii=False)
    meta_str = json.dumps(meta, ensure_ascii=False)

    return """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>ONPE EG2026 — Mapa Coroplético</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
  :root {
    --bg: #0d1117; --surface: #161b22; --text: #e6edf3;
    --muted: #7d8590; --accent: #3b82f6; --border: #30363d;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, 'SF Pro Display', system-ui, sans-serif;
         background: var(--bg); color: var(--text); overflow: hidden; }
  header { padding: 1rem 1.5rem; border-bottom: 1px solid var(--border);
           display: flex; justify-content: space-between; align-items: center; gap: 1rem; }
  h1 { font-size: 1.25rem; font-weight: 700; letter-spacing: -0.02em; }
  .controls { display: flex; gap: 0.5rem; }
  .btn { background: var(--surface); color: var(--text); border: 1px solid var(--border);
         padding: 0.5rem 1rem; border-radius: 0.375rem; cursor: pointer;
         font: inherit; font-size: 0.875rem; }
  .btn:hover { border-color: var(--accent); }
  .btn.active { background: var(--accent); border-color: var(--accent); color: #fff; }
  #map { height: calc(100vh - 60px); width: 100%; background: var(--bg); }
  .leaflet-container { background: var(--bg) !important; }
  .leaflet-tooltip { background: var(--surface) !important; color: var(--text) !important;
                     border: 1px solid var(--border) !important; border-radius: 0.375rem !important;
                     padding: 0.5rem 0.75rem !important; font-size: 0.85rem !important; }
  .leaflet-tooltip-top:before { border-top-color: var(--border) !important; }
  .legend { position: absolute; bottom: 1rem; right: 1rem; background: var(--surface);
            border: 1px solid var(--border); border-radius: 0.375rem; padding: 0.75rem;
            font-size: 0.8rem; max-width: 300px; z-index: 1000; }
  .legend-item { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.25rem; }
  .legend-swatch { width: 14px; height: 14px; border-radius: 2px; }
  footer { position: absolute; bottom: 1rem; left: 1rem; font-size: 0.75rem; color: var(--muted); }
  code { background: var(--surface); padding: 0.1rem 0.3rem; border-radius: 3px; font-size: 0.8em; }
</style>
</head>
<body>
<header>
  <h1>🇵🇪 ONPE EG2026 — Mapa coroplético</h1>
  <div class="controls">
    <button class="btn active" data-view="presidencial">Presidencial</button>
    <button class="btn" data-view="participacion">Participación</button>
  </div>
</header>
<div id="map"></div>
<div class="legend" id="legend"></div>
<footer>Dataset EG2026 · N=463,830 actas · Build META_BUILT_AT · <code>f3r21/onpe-eg2026</code></footer>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const GEOJSON = GEOJSON_DATA;
const PARTY_COLORS = PARTY_COLORS_DATA;
const META = META_DATA;
const DEFAULT_COLOR = "DEFAULT_COLOR_STR";

function colorForParticipation(pct) {
  if (pct >= 80) return '#1a5e3f';
  if (pct >= 75) return '#4a9d5b';
  if (pct >= 70) return '#8ecb94';
  if (pct >= 65) return '#c7e4c1';
  if (pct >= 60) return '#ede7a9';
  if (pct >= 50) return '#e8a07a';
  return '#c55a4a';
}

function colorForParty(partyName) {
  for (const [k, v] of Object.entries(PARTY_COLORS)) {
    if (partyName.startsWith(k)) return v;
  }
  return DEFAULT_COLOR;
}

const map = L.map('map', { zoomControl: true, attributionControl: false })
  .setView([-9.5, -75], 5.5);

let layer = null;
let currentView = 'presidencial';

function styleFor(feature) {
  const p = feature.properties;
  if (currentView === 'presidencial') {
    return { fillColor: colorForParty(p.presidencial_partido), color: '#30363d',
             weight: 1, fillOpacity: 0.75, opacity: 1 };
  } else {
    return { fillColor: colorForParticipation(p.participacion_pct), color: '#30363d',
             weight: 1, fillOpacity: 0.75, opacity: 1 };
  }
}

function tooltipFor(feature) {
  const p = feature.properties;
  const name = p.name || p.depto_id;
  if (currentView === 'presidencial') {
    return '<strong>' + name + '</strong><br>' +
           'Ganador: ' + p.presidencial_partido + '<br>' +
           'Votos: ' + p.presidencial_votos.toLocaleString() + '<br>' +
           '% del depto: ' + p.presidencial_pct + '%';
  } else {
    return '<strong>' + name + '</strong><br>' +
           'Participación: ' + p.participacion_pct + '%<br>' +
           'Emitidos: ' + p.participacion_emit.toLocaleString() + '<br>' +
           'Hábiles: ' + p.participacion_hab.toLocaleString();
  }
}

function renderLegend() {
  const el = document.getElementById('legend');
  if (currentView === 'presidencial') {
    let html = '<strong>Partido ganador</strong><br>';
    // Listar solo partidos que aparecen como ganadores
    const winners = new Set();
    GEOJSON.features.forEach(f => { if (f.properties.presidencial_partido) winners.add(f.properties.presidencial_partido); });
    const ordered = [...winners].sort();
    for (const p of ordered) {
      html += '<div class="legend-item">' +
              '<span class="legend-swatch" style="background:' + colorForParty(p) + '"></span>' +
              p.length > 30 ? p.substring(0, 30) + '…' : p +
              '</div>';
    }
    el.innerHTML = html.replace(/undefined/g, '');
  } else {
    el.innerHTML = '<strong>Participación %</strong>' +
                   '<div class="legend-item"><span class="legend-swatch" style="background:#1a5e3f"></span>≥ 80%</div>' +
                   '<div class="legend-item"><span class="legend-swatch" style="background:#4a9d5b"></span>75-80%</div>' +
                   '<div class="legend-item"><span class="legend-swatch" style="background:#8ecb94"></span>70-75%</div>' +
                   '<div class="legend-item"><span class="legend-swatch" style="background:#c7e4c1"></span>65-70%</div>' +
                   '<div class="legend-item"><span class="legend-swatch" style="background:#ede7a9"></span>60-65%</div>' +
                   '<div class="legend-item"><span class="legend-swatch" style="background:#e8a07a"></span>50-60%</div>' +
                   '<div class="legend-item"><span class="legend-swatch" style="background:#c55a4a"></span>< 50%</div>';
  }
}

function render() {
  if (layer) map.removeLayer(layer);
  layer = L.geoJSON(GEOJSON, {
    style: styleFor,
    onEachFeature: (feature, lyr) => {
      lyr.bindTooltip(tooltipFor(feature), { sticky: true });
      lyr.on('mouseover', e => e.target.setStyle({ weight: 3, fillOpacity: 0.9 }));
      lyr.on('mouseout', e => layer.resetStyle(e.target));
    }
  }).addTo(map);
  renderLegend();
}

document.querySelectorAll('.btn').forEach(b => {
  b.addEventListener('click', () => {
    document.querySelectorAll('.btn').forEach(x => x.classList.remove('active'));
    b.classList.add('active');
    currentView = b.dataset.view;
    render();
  });
});

render();
</script>
</body>
</html>
""".replace("GEOJSON_DATA", geojson_str).replace("PARTY_COLORS_DATA", party_colors_str).replace("META_DATA", meta_str).replace("DEFAULT_COLOR_STR", DEFAULT_COLOR).replace("META_BUILT_AT", meta.get("built_at", "?"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--open", action="store_true", help="abrir en browser al terminar")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    log.info("leyendo curated + geojson...")
    cab = pl.read_parquet(CURATED_CAB)
    vot = pl.read_parquet(CURATED_VOT)
    peru_low = json.loads((GEOJSON_DIR / "peruLow.json").read_text())

    log.info("calculando ganadores Presidencial por depto...")
    ganadores = compute_ganadores_por_depto(cab, vot)
    log.info("  deptos con data: %d", len(ganadores))

    log.info("calculando participación por depto...")
    participacion = compute_participacion_por_depto(cab)

    from datetime import datetime, timezone
    from onpe.storage import TZ_LIMA
    built_at = datetime.now(timezone.utc).astimezone(TZ_LIMA).isoformat(timespec="seconds")
    meta = {"built_at": built_at, "n_actas": cab.height}

    html = render_html(peru_low, ganadores, participacion, meta)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    out = DASHBOARD_DIR / "mapa_eg2026.html"
    out.write_text(html)
    log.info("escrito: %s (%.1f KB)", out, out.stat().st_size / 1024)

    if args.open:
        import subprocess
        subprocess.run(["open", str(out)], check=False)


if __name__ == "__main__":
    main()
