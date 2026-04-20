# onpe-eg2026 v1.0.0 — Release Notes

**Release date**: 2026-04-19

**TL;DR**: Primera release pública del dataset completo de las **Elecciones Generales Perú 2026** (EG2026) + tooling open source para ingesta desde el API reverse-engineered de ONPE.

---

## 🎯 Qué es

Un data lake en Parquet con partición Hive que consolida:

- **463,830 actas** × 5 elecciones simultáneas (Presidencial, Parlamento Andino, Diputados, Senadores nacional/regional)
- **18.6M filas de votos** por partido × mesa × elección
- **1.27M eventos** de línea de tiempo (transiciones de estado por acta)
- **811k metadatos** de PDFs escaneados
- **222 GeoJSONs** (país + 25 deptos + 196 provincias)
- **Histórico EG2021** (86k actas 1ra vuelta) como baseline

**Ready-to-analyze** con Polars, DuckDB, o pandas. 4 niveles de Data Quality validados (14/14 PASS en N1+N2+N3). 87 tests pytest. Schema validation fail-fast.

## 🔥 Highlights

### Pipeline robusto
- httpx async + HTTP/2 + tenacity + rate-limit 15 rps con jitter ±10%
- Advisory file locks para coordinar jobs
- Chunked Parquet writes resumables con checkpoint JSON
- Schema validation fail-fast ante type drift de ONPE

### Observabilidad
- Dashboard HTML autocontenido (`data/dashboard/index.html`)
- **Mapa coroplético interactivo 3 niveles** (`data/dashboard/mapa_eg2026.html`): país → depto → provincia → distrito con drilldown Leaflet
- `analytics_report.py` con top partidos, participación por depto, ganador por DE, stats voto exterior, comparativa EG2021

### Automation
- LaunchAgents macOS para shim aggregates + monitor semanal datosabiertos
- Scripts idempotentes con `skip_existing` en GCS
- Lock file previniendo colisiones rate-limit

### Data Quality
- **Nivel 1** (5 checks): integridad interna — universo, cardinalidad detalle=41, identidades contables, rangos, padrón coherente
- **Nivel 2** (5 checks): cruce vs aggregates ONPE — universo mesas, totalActas, votos id=10 drift ≤0.1%, regiones, partidos
- **Nivel 3** (4 checks): cruces regionales — coherencia ámbito×DE, exterior universo, totales_de, partidos por DE
- **Nivel 4** (placeholder): reconciliación vs datosabiertos.gob.pe oficial, activo cuando ONPE publique

## 📊 Insights preliminares (snapshot 2026-04-19)

### Presidencial — top 5 partidos

| Partido | Votos | % |
|---|---:|---:|
| Fuerza Popular | 2,687,621 | 17.06% |
| Juntos Por El Perú | 1,891,906 | 12.01% |
| Renovación Popular | 1,878,493 | 11.92% |
| Partido del Buen Gobierno | 1,743,316 | 11.06% |
| Partido Cívico Obras | 1,602,041 | 10.17% |

### Diputados (27 DEs)
- **Fuerza Popular** gana en 14 DEs
- **Juntos Por El Perú** en 9 DEs
- **Renovación Popular** domina DE 15 (Lima Metropolitana, 806k votos) + DE 27 (exterior)

### Participación nacional
- **EG2026: 74.35%** (solo C — drift temporal posible, se acerca al oficial cuando cierre)
- EG2021: 71.98%
- **+2.37 puntos** vs elección anterior

### Voto exterior (2,543 mesas, 210 ciudades)
- Top ciudades: Buenos Aires (1,105 actas), Santiago (1,090), Madrid (1,060), Nueva Jersey, Barcelona, Milán
- Ganador Presidencial exterior: **Renovación Popular** con 55,526 votos

## 🚀 Getting started

```bash
git clone https://github.com/f3r21/onpe-eg2026
cd onpe-eg2026
brew install uv
uv sync

# Smoke end-to-end
uv run python scripts/smoke.py

# Descargar GeoJSONs para mapas
uv run python scripts/download_geojsons.py

# Generar analytics + mapa
uv run python scripts/analytics_report.py
uv run python scripts/build_choropleth.py
open data/dashboard/mapa_eg2026.html
```

Ver [README.md](https://github.com/f3r21/onpe-eg2026/blob/main/README.md#quickstart) para flujo completo de ingesta.

## 📦 Assets incluidos en esta release

- `mapa_eg2026.html` — mapa coroplético interactivo 3 niveles (22 MB, autocontenido con Leaflet desde CDN)
- `analytics_report.txt` — reporte consolidado en texto
- `source code (zip/tar)` — generado automático por GitHub

## 🧪 Qué hay bajo el capó

### Stack técnico
- Python 3.12+
- Polars streaming (scan_parquet + sink_parquet)
- httpx async + HTTP/2 + tenacity
- Parquet zstd con partición Hive
- BeautifulSoup4 para scraping de datosabiertos.gob.pe
- google-cloud-storage para archivado de PDFs binarios
- pytest + pytest-asyncio + pytest-cov

### Arquitectura del data lake

```
data/
├── dim/                         # Catálogos estáticos (ubigeos, DE, mesas)
├── facts/                       # Snapshots raw Hive-particionados
│   ├── totales/snapshot_date=YYYY-MM-DD/<ts>.parquet
│   ├── actas_cabecera/snapshot_date=.../run_ts_ms=.../N.parquet
│   └── actas_votos/...
├── curated/                     # Dedup max(run_ts_ms), enriquecido
│   ├── actas_cabecera.parquet   # 463,830 × 30 cols
│   ├── actas_votos.parquet      # 18,612,565 × 22 cols
│   ├── actas_linea_tiempo.parquet
│   └── actas_archivos.parquet
├── geojson/                     # 222 GeoJSONs (país + 25 deptos + 196 provs)
├── historico/eg2021/            # Baseline EG2021 (86k actas 1ra vuelta)
├── analytics/                   # Reports + agregados
└── dashboard/                   # HTMLs generados
```

## ⚠️ Gaps conocidos

- **Voto preferencial**: sin endpoint API en ONPE. Disponible solo en PDFs de escrutinio. Excluido del 100% actual; opción OCR deferred.
- **DQ Nivel 4 real**: pendiente publicación ONPE en datosabiertos (~4 semanas post-JNE). Monitor semanal activo detectará automáticamente.
- **PDFs binarios**: descarga en GCS (`gs://onpe-eg2026-pdfs-v2`) en curso; ~1 TB, ETA ~2 días. No bloquea el dataset estructurado.
- **Mapeo RENIEC ↔ ONPE ubigeos**: la comparativa EG2021 vs EG2026 a nivel distrito individual requiere tabla cross-walk. Agregado nacional funciona OK.

## 🙏 Agradecimientos

- **ONPE** por publicar los resultados via SPA (aunque sin API oficial).
- **jmcastagnetto** por el repo `2021-elecciones-generales-peru-datos-de-onpe` usado como baseline histórico.
- Comunidad del scraping electoral peruano que ha documentado patterns del API ONPE año tras año.

## 📄 Licencia

MIT. Usá, modificá, compartí. Una cita al repo es apreciada pero no obligatoria.

---

**Próxima release (v1.1 esperada ~junio 2026)**:
- DQ Nivel 4 implementado con schema datosabiertos oficial
- PDFs binarios completos en bucket GCS público (o mirror)
- Mapeo RENIEC ↔ ONPE ubigeos
- (Posible) voto preferencial vía OCR si se aprueba esfuerzo

Issues y PRs bienvenidos: https://github.com/f3r21/onpe-eg2026/issues
