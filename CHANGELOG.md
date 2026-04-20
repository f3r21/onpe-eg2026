# Changelog

Formato basado en [Keep a Changelog](https://keepachangelog.com/es/1.1.0/). Proyecto sigue [SemVer](https://semver.org/).

## [1.0.0] — 2026-04-19

Primera release pública del dataset electoral EG2026 + tooling open source.

### Agregado

#### Dataset
- **463,830 actas** × 5 elecciones (Presidencial, Parlamento Andino, Diputados, Senadores nacional/regional).
- **18.6M filas de votos** individuales en `curated/actas_votos.parquet`.
- **1.27M eventos de línea de tiempo** (transiciones de estado por acta).
- **811,984 metadatos de archivos PDF** (archivoId + nombre UUID + tipo).
- **222 GeoJSONs** ONPE: país + 25 deptos + 196 provincias.
- **Histórico EG2021** como baseline (86k actas 1ra vuelta + 5 tablas distritales, fuente jmcastagnetto).
- **Serie temporal de aggregates** (snapshot cada 15 min durante conteo).

#### Pipeline
- `src/onpe/client.py` — HTTP async httpx + HTTP/2 + tenacity + rate-limit con jitter ±10% anti-detección.
- `src/onpe/actas.py` — snapshot resumable de `/actas/{idActa}` con chunked Parquet writes.
- `src/onpe/aggregates.py` — snapshot totales + mapa-calor + participantes por elección × distrito electoral.
- `src/onpe/crawler.py` + `mesas.py` — inventario de universe (92,766 mesas).
- `src/onpe/locks.py` — advisory file lock (fcntl) para coordinar jobs.
- `src/onpe/schemas.py` — SCHEMAS canónico + `validate_chunk()` fail-fast ante type drift.
- `src/onpe/pdfs.py` — descarga PDFs binarios streaming S3→GCS (endpoint `/actas/file?id=`).
- `src/onpe/geojson.py` — descarga peruLow + deptos + provincias.
- `src/onpe/datosabiertos.py` — scraper BS4 del catálogo Drupal ONPE.

#### Scripts
- Pipeline: `crawl_dims`, `crawl_mesas`, `snapshot_aggregates`, `snapshot_actas`, `daily_refresh`, `build_curated`.
- Enriquecimiento: `enrich_curated` (idAmbitoGeografico + idDistritoElectoral).
- Data Quality: `dq_check` con **4 niveles** (integridad interna, cruce aggregates, cruces regionales, reconciliación oficial).
- Externos: `download_geojsons`, `download_pdfs`, `ingest_datosabiertos`, `ingest_historico_eg2021`.
- Ops: `smoke`, `investigate_anomaly_240`, `monitor_datosabiertos`, `validate_ambitos`, `migrate_null_dtypes`.
- Visualización: `dashboard` (HTML de salud), `build_choropleth` (mapa drilldown 3 niveles), `analytics_report`.

#### Calidad
- **87 tests pytest** (src/onpe schemas 100% / locks 90% / client 83% / geojson 97% / datosabiertos 75%).
- **14/14 DQ checks PASS** en Niveles 1+2+3 (5+5+4).
- Schema validation fail-fast integrado en `_flush_chunk`.
- Advisory file locks previniendo colisiones rate-limit entre jobs.

#### Automation
- `ops/launchd/com.onpe.aggregates.plist` — shim aggregates cada 15 min (nativo macOS).
- `ops/launchd/com.onpe.datosabiertos.plist` — monitor semanal (lunes 9am).
- `ops/launchd/install.sh` — instalar/stop/status con un comando.

#### Documentación
- `README.md` con Quickstart, flujo diario, pipeline por etapas, DQ niveles, layout, troubleshooting.
- `CLAUDE.md` con convenciones operacionales + estado empírico del pipeline.
- `fuentes_datos.md` con mapa completo de endpoints ONPE, fórmula determinística `idActa`, peculiaridades CloudFront.
- `docs/linkedin_post_draft.md` con 3 variantes para anuncio + notas timing/assets.
- `CHANGELOG.md`, `CONTRIBUTING.md`.

### Corregido

- **Schema drift**: chunks con columnas `pl.Null` ya no contaminan el curated vía `diagonal_relaxed` → String promotion. Resuelto con `_NUMERIC_SCHEMAS` mapping y `validate_chunk` fail-fast.
- **Bug #42** (post-smoke): `totalVotosEmitidos` all-null → String → contaminación del curated entero.
- **Anomalía 240 actas C sin detalle** (task #58): clasificadas como "mesa no instalada" (100%, 48 mesas × 5 elecciones voto exterior). Documentadas y excluidas del denominador DQ.
- **Race condition daily_refresh ↔ snapshot_actas**: resuelto con `PipelineLock` (fcntl advisory) en `data/state/.pipeline_lock`.
- **Drift curated vs aggregates >17h** inicial → ≤90 min tras re-snapshot full y aggregates loop.

### Conocido

- **Voto preferencial** (D5): sin endpoint API ONPE. Disponible solo en PDFs de acta de escrutinio (tipo 1). Excluido del 100% actual, opción OCR deferred.
- **DQ Nivel 4** (reconciliación oficial): placeholder hasta publicación ONPE en datosabiertos.gob.pe (~4 semanas post-proclamación JNE). Monitor semanal activo.
- **Mapeo RENIEC ↔ ONPE ubigeos**: EG2021 usa RENIEC (INEI-like), EG2026 usa ONPE propio. Comparativa a nivel distrito individual requiere tabla mapping (deferred).
- **PDFs binarios en GCS**: descarga en curso (~725k PDFs, ~1 TB) en `gs://onpe-eg2026-pdfs-v2`. ETA ~2-3 días post-release. No bloquea el dataset estructurado.

---

## [0.x] — 2026-04-17 → 2026-04-18

Iteraciones internas pre-release. Ver `git log --oneline` para detalle.
