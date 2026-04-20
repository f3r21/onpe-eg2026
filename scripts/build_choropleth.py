"""Mapa coroplético HTML interactivo multi-nivel del dataset EG2026.

Produce `data/dashboard/mapa_eg2026.html`: autocontenido (Leaflet desde CDN)
con drilldown 2 niveles:

  Nivel 1 (país): peruLow + ganador por departamento
  Nivel 2 (departamento): click en depto → provincias del depto + ganador por provincia

Switcher superior: Presidencial (ganador partido) / Participación (% gradiente).

Uso:
    uv run python scripts/build_choropleth.py
    uv run python scripts/build_choropleth.py --open
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC

import polars as pl

from onpe.storage import DATA_DIR

log = logging.getLogger("choropleth")

CURATED_CAB = DATA_DIR / "curated" / "actas_cabecera.parquet"
CURATED_VOT = DATA_DIR / "curated" / "actas_votos.parquet"
GEOJSON_DIR = DATA_DIR / "geojson"
DASHBOARD_DIR = DATA_DIR / "dashboard"

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
    "VOTEMOS PERÚ": "#f77f00",
    "ALIANZA PARA EL PROGRESO": "#fca311",
    "ACCIÓN POPULAR": "#d62828",
}
DEFAULT_COLOR = "#95a5a6"


def ganadores_por_nivel(
    cab: pl.DataFrame,
    vot: pl.DataFrame,
    level: str,  # 'depto' | 'provincia' | 'distrito'
) -> dict[str, dict]:
    """Devuelve {ubigeo_nivel: {partido, votos, pct, total}} para Presidencial Perú."""
    if level == "depto":
        slice_ = (0, 2)
        suffix = "0000"
    elif level == "provincia":
        slice_ = (0, 4)
        suffix = "00"
    elif level == "distrito":
        slice_ = (0, 6)
        suffix = ""
    else:
        raise ValueError(f"level inválido: {level}")

    cab_peru = cab.filter((pl.col("idEleccion") == 10) & (pl.col("idAmbitoGeografico") == 1))
    votos = (
        vot.filter((~pl.col("es_especial")) & (pl.col("idEleccion") == 10))
        .join(cab_peru.select("idActa", "ubigeoDistrito"), on="idActa", how="inner")
        .with_columns(
            (pl.col("ubigeoDistrito").str.slice(slice_[0], slice_[1]) + suffix).alias("key")
        )
        .group_by(["key", "descripcion"])
        .agg(pl.col("nvotos").sum().alias("total"))
    )
    totales = votos.group_by("key").agg(pl.col("total").sum().alias("sum_key"))
    votos = votos.join(totales, on="key").with_columns(
        (pl.col("total") / pl.col("sum_key") * 100).alias("pct")
    )
    winners = votos.sort(["key", "total"], descending=[False, True]).unique(
        subset="key", keep="first"
    )
    out = {}
    for row in winners.iter_rows(named=True):
        out[row["key"]] = {
            "partido": row["descripcion"],
            "votos": int(row["total"]),
            "pct": round(row["pct"], 2),
            "total_key": int(row["sum_key"]),
        }
    return out


def participacion_por_nivel(cab: pl.DataFrame, level: str) -> dict[str, dict]:
    """% participación Presidencial C por depto, provincia o distrito."""
    if level == "depto":
        slice_ = (0, 2)
        suffix = "0000"
    elif level == "provincia":
        slice_ = (0, 4)
        suffix = "00"
    elif level == "distrito":
        slice_ = (0, 6)
        suffix = ""
    else:
        raise ValueError(f"level inválido: {level}")

    df = (
        cab.filter(
            (pl.col("idEleccion") == 10)
            & (pl.col("codigoEstadoActa") == "C")
            & (pl.col("idAmbitoGeografico") == 1)
        )
        .with_columns(
            (pl.col("ubigeoDistrito").str.slice(slice_[0], slice_[1]) + suffix).alias("key")
        )
        .group_by("key")
        .agg(
            pl.col("totalElectoresHabiles").sum().alias("hab"),
            pl.col("totalVotosEmitidos").sum().alias("emit"),
            pl.len().alias("n_actas"),
        )
        .with_columns((pl.col("emit") / pl.col("hab") * 100).alias("pct"))
    )
    return {
        row["key"]: {
            "pct": round(row["pct"], 2),
            "emit": int(row["emit"]),
            "hab": int(row["hab"]),
            "n_actas": int(row["n_actas"]),
        }
        for row in df.iter_rows(named=True)
    }


def enrich_feature(feat: dict, feat_id: str, ganadores: dict, participacion: dict) -> dict:
    """Añade properties coroplet-relevant al feature."""
    g = ganadores.get(feat_id, {})
    p = participacion.get(feat_id, {})
    props = {
        **feat.get("properties", {}),
        "id": feat_id,
        "presidencial_partido": g.get("partido", "s/d"),
        "presidencial_votos": g.get("votos", 0),
        "presidencial_pct": g.get("pct", 0),
        "participacion_pct": p.get("pct", 0),
        "participacion_emit": p.get("emit", 0),
        "participacion_hab": p.get("hab", 0),
    }
    return {**feat, "properties": props}


def build_provincias_by_depto(ganadores_prov: dict, participacion_prov: dict) -> dict[str, dict]:
    """Devuelve {ubigeoDepartamento: FeatureCollection_provincias}."""
    by_depto: dict[str, dict] = {}
    depto_dir = GEOJSON_DIR / "departamentos"
    for depto_path in sorted(depto_dir.glob("*.json")):
        depto_id = depto_path.stem
        data = json.loads(depto_path.read_text())
        enriched_feats = []
        for feat in data.get("features", []):
            props = feat.get("properties", {})
            prov_id = str(props.get("ID") or props.get("id") or "")
            if not prov_id:
                prov_id = str(feat.get("id", ""))
            if len(prov_id) == 4:
                prov_id = prov_id + "00"
            enriched = enrich_feature(feat, prov_id, ganadores_prov, participacion_prov)
            enriched_feats.append(enriched)
        by_depto[depto_id] = {
            "type": "FeatureCollection",
            "features": enriched_feats,
        }
    return by_depto


def build_distritos_by_provincia(ganadores_dist: dict, participacion_dist: dict) -> dict[str, dict]:
    """Devuelve {ubigeoProvincia: FeatureCollection_distritos}.

    Cada provincia geojson tiene features=distritos; el id del distrito viene
    en properties.ID (ej "040112") o como id top-level.
    """
    by_prov: dict[str, dict] = {}
    prov_dir = GEOJSON_DIR / "provincias"
    for prov_path in sorted(prov_dir.glob("*.json")):
        prov_id = prov_path.stem
        data = json.loads(prov_path.read_text())
        enriched_feats = []
        for feat in data.get("features", []):
            props = feat.get("properties", {})
            dist_id = str(props.get("ID") or props.get("id") or feat.get("id") or "")
            if not dist_id:
                continue
            # normalizar a 6 dígitos (ej "40112" → "040112")
            if len(dist_id) == 5:
                dist_id = "0" + dist_id
            enriched = enrich_feature(feat, dist_id, ganadores_dist, participacion_dist)
            enriched_feats.append(enriched)
        by_prov[prov_id] = {
            "type": "FeatureCollection",
            "features": enriched_feats,
        }
    return by_prov


def render_html(
    peru_low: dict,
    provincias_by_depto: dict[str, dict],
    distritos_by_provincia: dict[str, dict],
    meta: dict,
) -> str:
    """HTML autocontenido con 3 niveles + drilldown."""
    peru_geojson = json.dumps(peru_low, ensure_ascii=False)
    provincias_json = json.dumps(provincias_by_depto, ensure_ascii=False)
    distritos_json = json.dumps(distritos_by_provincia, ensure_ascii=False)
    party_colors_json = json.dumps(PARTY_COLORS, ensure_ascii=False)

    template = """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>ONPE EG2026 — Mapa Coroplético</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
  :root { --bg:#0d1117; --surface:#161b22; --text:#e6edf3; --muted:#7d8590;
          --accent:#3b82f6; --border:#30363d; }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family:-apple-system,'SF Pro Display',system-ui,sans-serif;
         background:var(--bg); color:var(--text); overflow:hidden; }
  header { padding:0.75rem 1.25rem; border-bottom:1px solid var(--border);
           display:flex; justify-content:space-between; align-items:center; gap:1rem;
           flex-wrap:wrap; }
  h1 { font-size:1.15rem; font-weight:700; letter-spacing:-0.02em; }
  .sub { color:var(--muted); font-size:0.8rem; margin-top:0.15rem; }
  .loading { position:fixed; top:50%; left:50%; transform:translate(-50%,-50%);
             background:var(--surface); padding:1rem 2rem; border-radius:0.5rem;
             border:1px solid var(--border); z-index:2000; font-size:0.9rem; }
  .controls { display:flex; gap:0.5rem; align-items:center; }
  .btn { background:var(--surface); color:var(--text); border:1px solid var(--border);
         padding:0.4rem 0.9rem; border-radius:0.375rem; cursor:pointer;
         font:inherit; font-size:0.85rem; transition:all 150ms; }
  .btn:hover { border-color:var(--accent); }
  .btn.active { background:var(--accent); border-color:var(--accent); color:#fff; }
  .btn:disabled { opacity:0.4; cursor:not-allowed; }
  .breadcrumb { color:var(--muted); font-size:0.85rem; padding:0.4rem 0.8rem;
                background:var(--surface); border:1px solid var(--border);
                border-radius:0.375rem; }
  .breadcrumb .current { color:var(--text); font-weight:600; }
  #map { height:calc(100vh - 64px); width:100%; background:var(--bg); }
  .leaflet-container { background:var(--bg) !important; }
  .leaflet-tooltip { background:var(--surface) !important; color:var(--text) !important;
                     border:1px solid var(--border) !important; border-radius:0.375rem !important;
                     padding:0.5rem 0.75rem !important; font-size:0.85rem !important;
                     box-shadow:0 4px 12px rgba(0,0,0,0.4) !important; }
  .leaflet-tooltip-top:before { border-top-color:var(--border) !important; }
  .legend { position:absolute; bottom:1rem; right:1rem; background:var(--surface);
            border:1px solid var(--border); border-radius:0.375rem; padding:0.7rem 0.9rem;
            font-size:0.78rem; max-width:280px; z-index:1000; }
  .legend strong { display:block; margin-bottom:0.4rem; }
  .legend-item { display:flex; align-items:center; gap:0.4rem; margin-bottom:0.2rem; }
  .legend-swatch { width:12px; height:12px; border-radius:2px; flex-shrink:0; }
  footer { position:absolute; bottom:1rem; left:1rem; font-size:0.72rem; color:var(--muted); }
  code { background:var(--surface); padding:0.1rem 0.3rem; border-radius:3px; font-size:0.85em; }
</style>
</head>
<body>
<header>
  <div>
    <h1>🇵🇪 ONPE EG2026 — Mapa Coroplético</h1>
    <div class="sub">Click depto → provincias · Click provincia → distritos · Botón ◂ Volver</div>
  </div>
  <div class="controls">
    <span class="breadcrumb" id="breadcrumb">Perú</span>
    <button class="btn active" data-view="presidencial">Presidencial</button>
    <button class="btn" data-view="participacion">Participación</button>
    <button class="btn" id="backBtn" disabled>◂ Volver</button>
  </div>
</header>
<div id="map"></div>
<div class="legend" id="legend"></div>
<footer>Dataset EG2026 · N=NACTAS_PLACEHOLDER actas · BUILT_AT_PLACEHOLDER · <code>f3r21/onpe-eg2026</code></footer>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const PERU_GEOJSON = PERU_GEOJSON_PLACEHOLDER;
const PROVINCIAS_BY_DEPTO = PROVINCIAS_PLACEHOLDER;
const DISTRITOS_BY_PROVINCIA = DISTRITOS_PLACEHOLDER;
const PARTY_COLORS = PARTY_COLORS_PLACEHOLDER;
const DEFAULT_COLOR = "DEFAULT_COLOR_PLACEHOLDER";

const PERU_BOUNDS = [[-18.5, -81.5], [-0.5, -68.5]];

function colorForParty(partyName) {
  if (!partyName || partyName === 's/d') return DEFAULT_COLOR;
  for (const [k, v] of Object.entries(PARTY_COLORS)) {
    if (partyName.startsWith(k)) return v;
  }
  return DEFAULT_COLOR;
}
function colorForParticipation(pct) {
  if (pct >= 80) return '#1a5e3f';
  if (pct >= 75) return '#4a9d5b';
  if (pct >= 70) return '#8ecb94';
  if (pct >= 65) return '#c7e4c1';
  if (pct >= 60) return '#ede7a9';
  if (pct >= 50) return '#e8a07a';
  return '#c55a4a';
}

const map = L.map('map', { zoomControl: true, attributionControl: false });
map.fitBounds(PERU_BOUNDS);

let currentLayer = null;
let currentView = 'presidencial';
let currentLevel = 'nacional';  // 'nacional' | 'provincia' | 'distrito'
let currentDeptoId = null;
let currentProvinciaId = null;

function styleFor(feature) {
  const p = feature.properties;
  if (currentView === 'presidencial') {
    return { fillColor: colorForParty(p.presidencial_partido), color: '#30363d',
             weight: 1, fillOpacity: 0.78, opacity: 1 };
  } else {
    return { fillColor: colorForParticipation(p.participacion_pct), color: '#30363d',
             weight: 1, fillOpacity: 0.78, opacity: 1 };
  }
}

function tooltipFor(feature) {
  const p = feature.properties;
  const name = p.name || p.DISTRITO || p.id;
  if (currentView === 'presidencial') {
    return '<strong>' + name + '</strong><br>' +
           '<span style="color:' + colorForParty(p.presidencial_partido) + '">●</span> ' +
           p.presidencial_partido + '<br>' +
           '<strong>' + p.presidencial_votos.toLocaleString() + '</strong> votos · ' +
           p.presidencial_pct + '%';
  } else {
    return '<strong>' + name + '</strong><br>' +
           'Participación: <strong>' + p.participacion_pct + '%</strong><br>' +
           p.participacion_emit.toLocaleString() + ' / ' + p.participacion_hab.toLocaleString() + ' hábiles';
  }
}

function renderLegend(geojson) {
  const el = document.getElementById('legend');
  if (currentView === 'presidencial') {
    const winners = new Set();
    geojson.features.forEach(f => {
      if (f.properties.presidencial_partido && f.properties.presidencial_partido !== 's/d') {
        winners.add(f.properties.presidencial_partido);
      }
    });
    let html = '<strong>Partido ganador</strong>';
    for (const p of [...winners].sort()) {
      const short = p.length > 28 ? p.substring(0, 28) + '…' : p;
      html += '<div class="legend-item"><span class="legend-swatch" style="background:' +
              colorForParty(p) + '"></span>' + short + '</div>';
    }
    el.innerHTML = html;
  } else {
    el.innerHTML = '<strong>Participación %</strong>' +
      '<div class="legend-item"><span class="legend-swatch" style="background:#1a5e3f"></span>≥ 80%</div>' +
      '<div class="legend-item"><span class="legend-swatch" style="background:#4a9d5b"></span>75-80%</div>' +
      '<div class="legend-item"><span class="legend-swatch" style="background:#8ecb94"></span>70-75%</div>' +
      '<div class="legend-item"><span class="legend-swatch" style="background:#c7e4c1"></span>65-70%</div>' +
      '<div class="legend-item"><span class="legend-swatch" style="background:#ede7a9"></span>60-65%</div>' +
      '<div class="legend-item"><span class="legend-swatch" style="background:#e8a07a"></span>50-60%</div>' +
      '<div class="legend-item"><span class="legend-swatch" style="background:#c55a4a"></span>&lt; 50%</div>';
  }
}

function renderBreadcrumb() {
  const el = document.getElementById('breadcrumb');
  const backBtn = document.getElementById('backBtn');
  if (currentLevel === 'nacional') {
    el.innerHTML = '<span class="current">Perú</span>';
    backBtn.disabled = true;
  } else if (currentLevel === 'provincia') {
    el.innerHTML = 'Perú › <span class="current">Depto ' + currentDeptoId + '</span>';
    backBtn.disabled = false;
  } else {
    el.innerHTML = 'Perú › Depto ' + currentDeptoId +
                   ' › <span class="current">Prov ' + currentProvinciaId + '</span>';
    backBtn.disabled = false;
  }
}

function drillToProvincia(deptoId) {
  const provGeojson = PROVINCIAS_BY_DEPTO[deptoId];
  if (!provGeojson) { console.warn('no provincias para depto', deptoId); return; }
  currentLevel = 'provincia';
  currentDeptoId = deptoId;
  currentProvinciaId = null;
  renderBreadcrumb();
  if (currentLayer) map.removeLayer(currentLayer);
  currentLayer = L.geoJSON(provGeojson, {
    style: styleFor,
    onEachFeature: (feat, lyr) => {
      lyr.bindTooltip(tooltipFor(feat), { sticky: true });
      lyr.on('mouseover', e => e.target.setStyle({ weight: 3, fillOpacity: 0.92 }));
      lyr.on('mouseout', e => currentLayer.resetStyle(e.target));
      lyr.on('click', e => {
        const provId = e.target.feature.properties.id;
        drillToDistrito(provId);
      });
    }
  }).addTo(map);
  map.fitBounds(currentLayer.getBounds(), { padding: [20, 20] });
  renderLegend(provGeojson);
}

function drillToDistrito(provinciaId) {
  const distGeojson = DISTRITOS_BY_PROVINCIA[provinciaId];
  if (!distGeojson || !distGeojson.features.length) {
    console.warn('no distritos para provincia', provinciaId);
    return;
  }
  currentLevel = 'distrito';
  currentProvinciaId = provinciaId;
  renderBreadcrumb();
  if (currentLayer) map.removeLayer(currentLayer);
  currentLayer = L.geoJSON(distGeojson, {
    style: styleFor,
    onEachFeature: (feat, lyr) => {
      lyr.bindTooltip(tooltipFor(feat), { sticky: true });
      lyr.on('mouseover', e => e.target.setStyle({ weight: 3, fillOpacity: 0.92 }));
      lyr.on('mouseout', e => currentLayer.resetStyle(e.target));
    }
  }).addTo(map);
  map.fitBounds(currentLayer.getBounds(), { padding: [20, 20] });
  renderLegend(distGeojson);
}

function showNacional() {
  currentLevel = 'nacional';
  currentDeptoId = null;
  currentProvinciaId = null;
  renderBreadcrumb();
  if (currentLayer) map.removeLayer(currentLayer);
  currentLayer = L.geoJSON(PERU_GEOJSON, {
    style: styleFor,
    onEachFeature: (feat, lyr) => {
      lyr.bindTooltip(tooltipFor(feat), { sticky: true });
      lyr.on('mouseover', e => e.target.setStyle({ weight: 3, fillOpacity: 0.92 }));
      lyr.on('mouseout', e => currentLayer.resetStyle(e.target));
      lyr.on('click', e => {
        const deptoId = e.target.feature.properties.id;
        drillToProvincia(deptoId);
      });
    }
  }).addTo(map);
  map.fitBounds(PERU_BOUNDS);
  renderLegend(PERU_GEOJSON);
}

function goBack() {
  if (currentLevel === 'distrito') drillToProvincia(currentDeptoId);
  else if (currentLevel === 'provincia') showNacional();
}

document.querySelectorAll('.btn[data-view]').forEach(b => {
  b.addEventListener('click', () => {
    document.querySelectorAll('.btn[data-view]').forEach(x => x.classList.remove('active'));
    b.classList.add('active');
    currentView = b.dataset.view;
    // Re-render current level
    if (currentLevel === 'nacional') showNacional();
    else if (currentLevel === 'provincia') drillToProvincia(currentDeptoId);
    else if (currentLevel === 'distrito') drillToDistrito(currentProvinciaId);
  });
});

document.getElementById('backBtn').addEventListener('click', goBack);

showNacional();
</script>
</body>
</html>
"""
    return (
        template.replace("PERU_GEOJSON_PLACEHOLDER", peru_geojson)
        .replace("PROVINCIAS_PLACEHOLDER", provincias_json)
        .replace("DISTRITOS_PLACEHOLDER", distritos_json)
        .replace("PARTY_COLORS_PLACEHOLDER", party_colors_json)
        .replace("DEFAULT_COLOR_PLACEHOLDER", DEFAULT_COLOR)
        .replace("NACTAS_PLACEHOLDER", f"{meta['n_actas']:,}")
        .replace("BUILT_AT_PLACEHOLDER", meta["built_at"])
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--open", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    log.info("leyendo curated + geojsons...")
    cab = pl.read_parquet(CURATED_CAB)
    vot = pl.read_parquet(CURATED_VOT)

    log.info("calculando ganadores por nivel...")
    ganadores_depto = ganadores_por_nivel(cab, vot, "depto")
    ganadores_prov = ganadores_por_nivel(cab, vot, "provincia")
    ganadores_dist = ganadores_por_nivel(cab, vot, "distrito")
    log.info("  deptos con ganador: %d", len(ganadores_depto))
    log.info("  provincias con ganador: %d", len(ganadores_prov))
    log.info("  distritos con ganador: %d", len(ganadores_dist))

    log.info("calculando participación por nivel...")
    part_depto = participacion_por_nivel(cab, "depto")
    part_prov = participacion_por_nivel(cab, "provincia")
    part_dist = participacion_por_nivel(cab, "distrito")

    # País: peruLow enriquecido
    peru_low = json.loads((GEOJSON_DIR / "peruLow.json").read_text())
    peru_low["features"] = [
        enrich_feature(f, str(f.get("id", "")), ganadores_depto, part_depto)
        for f in peru_low["features"]
    ]

    # Provincias agrupadas por depto + distritos agrupados por provincia
    prov_by_depto = build_provincias_by_depto(ganadores_prov, part_prov)
    dist_by_prov = build_distritos_by_provincia(ganadores_dist, part_dist)
    log.info("  deptos con provincias geojson: %d", len(prov_by_depto))
    log.info("  provincias con distritos geojson: %d", len(dist_by_prov))
    total_dist = sum(len(v["features"]) for v in dist_by_prov.values())
    log.info("  distritos total embebidos: %d", total_dist)

    from datetime import datetime

    from onpe.storage import TZ_LIMA

    built_at = datetime.now(UTC).astimezone(TZ_LIMA).isoformat(timespec="seconds")
    meta = {"built_at": built_at, "n_actas": cab.height}

    html = render_html(peru_low, prov_by_depto, dist_by_prov, meta)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    out = DASHBOARD_DIR / "mapa_eg2026.html"
    out.write_text(html)
    size_mb = out.stat().st_size / 1e6
    log.info("escrito: %s (%.2f MB)", out, size_mb)

    if args.open:
        import subprocess

        subprocess.run(["open", str(out)], check=False)


if __name__ == "__main__":
    main()
