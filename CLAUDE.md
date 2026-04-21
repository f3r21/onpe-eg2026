# CLAUDE.md — contexto para Claude Code

Este archivo consolida el contexto operativo del proyecto. Leerlo al inicio de cada sesión antes de tocar código o ejecutar scripts.

## Proyecto

Ingesta de resultados de las **Elecciones Generales Perú 2026** (EG2026, 2026-04-12) desde el API reverse-engineered de ONPE (`resultadoelectoral.onpe.gob.pe/presentacion-backend/`). Produce un data lake en Parquet con particiones Hive, consumible con Polars o DuckDB.

**Tres objetivos de negocio:**
1. Predictor electoral (Presidente/Diputados/Senadores) basado en datos de mesa + overlay de candidatos.
2. Detección de anomalías/fraude vía análisis de actas, transiciones de estado, outliers.
3. LinkedIn post + open-source del tooling ONPE.

**Scope del trabajo** (acordado con el usuario, feedback explícito 2026-04-18):
- **En scope**: pipeline ONPE completo (actas / votos / mesas / agregados / timeline / archivos / DQ). También fuentes electorales oficiales: datosabiertos.gob.pe, RENIEC padrón, archivo histórico ONPE, GeoJSONs.
- **Fuera de scope** (el usuario los consigue): INFOgob, DECLARA, hojas de vida JNE, ingresos/bienes/antecedentes, partido político. Esos datos son capa de enriquecimiento que él inyecta; no dedicar ciclos a ellos.

## Estado al 2026-04-19 (post-A4 re-snapshot full)

| Activo | Filas | Cobertura | Nota |
|---|---|---|---|
| `dim/mesas.parquet` | 92,766 | 100% (90,223 Perú + 2,543 exterior) | OK |
| `dim/distritos_electorales.parquet` | 27 | — | 1..26 + 27 (extranjero) |
| `dim/geojson/peruLow.json` | 26 features | — | 25 deptos + Callao + Lago Titicaca |
| `curated/actas_cabecera.parquet` | 463,830 | 100% universo (92,766×5) | +5 cols enrich (idAmbitoGeografico, idDistritoElectoral, ubigeoDepartamento/Provincia, nombreDistrito) |
| `curated/actas_votos.parquet` | 18,612,565 | **97.87% actas** (453,965 / 463,830) | 9,865 sin votos: 240 mesa-no-instalada + 9,625 Pendientes |
| `curated/actas_linea_tiempo.parquet` | **1,270,390** | **274% row ratio** (1M+ eventos × 463k actas) | Cerrado gap (eran 439) |
| `curated/actas_archivos.parquet` | **811,984** | **~95% actas** (metadata 2 PDFs/acta) | Cerrado gap (eran 60) |
| `curated/actas_anomalia_240_investigacion.parquet` | 240 | — | 100% mesa_no_instalada (anomalia cerrada) |
| `facts/totales,mapa_calor,...` (aggregates) | N snapshots | — | Series temporales cada 15min (shim aggregate_loop) |

**DQ al 2026-04-19 08:11:** **14/14 PASS** (5 N1 + 5 N2 + 4 N3). Identidades contables EXACTAS en 390,827 actas C con detalle. Drifts todos en µ% (≤0.001% presidencial, ≤2.16% Diputados por DE).

**Snapshot full completado en 8h 53m 50s** (2026-04-18 21:07 → 2026-04-19 07:43). ok=463830 vacias=0 fallidas=0.

## Arquitectura

```
src/onpe/
  client.py       # httpx async + HTTP/2 + tenacity + rate-limit + jitter ±10%. 204 → {"data": None}
  endpoints.py    # wrappers tipados de cada endpoint ONPE + fórmula idActa
  storage.py      # helpers Parquet + Lima tz + DATA_DIR (override con env ONPE_DATA_DIR)
  crawler.py      # crawl jerárquico ubigeos (fase 1) — soporta Perú + exterior
  mesas.py        # inventario de mesas vía /actas listado (fase 2)
  aggregates.py   # snapshot de totales / mapa-calor / resumen / participantes
  actas.py        # snapshot resumable de /actas/{idActa} con chunked writes + schema validation
  locks.py        # advisory file lock (fcntl) para coordinar snapshot_actas vs daily_refresh
  schemas.py      # SCHEMAS + validate_chunk fail-fast ante drift de ONPE
  pdfs.py         # framework descarga PDFs binarios (pending: _SIGNED_URL_ENDPOINT vía DevTools)
  geojson.py     # descarga peruLow.json (26 deptos + Callao) desde la SPA

scripts/
  crawl_dims.py                   # fase 1 — ubigeos (--solo-peru opcional)
  crawl_mesas.py                  # fase 2 — inventario 92,766 mesas (--solo-peru opcional)
  snapshot_aggregates.py          # fase 3a — barato, repetible
  snapshot_actas.py               # fase 3b — 463k actas, ~8h @ 15 rps, resumable, toma lock
  daily_refresh.py                # pipeline incremental solo P/E (~1h20m @ 15 rps), toma lock
  build_curated.py                # dedup max(run_ts_ms), streaming sink_parquet, auto-enrich (--no-enrich opt), genera actas_votos_tidy.parquet
  enrich_curated.py               # join con dim/mesas → idAmbitoGeografico + idDistritoElectoral
  crawl_reniec_padron.py          # scraper RENIEC padrón Q1 2026 → data/dim/padron.parquet
  crawl_resoluciones.py           # scraper El Peruano resoluciones EG2026 → data/dim/resoluciones.parquet + PDFs
  export_csv.py                   # exporter CSV con filtros (eleccion/DE/depto/partido) — periodistas/analistas
  detect_anomalies.py             # detector baseline 7 reglas → data/analytics/anomalias.parquet + resumen .md/.json
  prepare_release.py              # empaqueta datasets/<version>/ (parquet+csv+sqlite+geojson+docs+notebooks+CHECKSUMS)
  generate_cover.py               # render docs/cover.png 1200x600 para cards Kaggle/Zenodo/HF
  dq_check.py                     # DQ Nivel 1 + 2 + 3 (--nivel 1|2|3|0)
  smoke.py                        # end-to-end validation — golden path del API
  dashboard.py                    # HTML estático salud del pipeline → data/dashboard/index.html
  investigate_anomaly_240.py      # diagnostico re-fetch + clasificar 240 C sin detalle
  download_geojsons.py            # descarga peruLow.json
  download_pdfs.py                # descarga masiva PDFs a GCS, soporta --shard M/N distribuido
  migrate_null_dtypes.py          # one-shot: castear columnas Parquet null a String
  validate_ambitos.py             # sanity check de la segregación Perú/Exterior en dim/

tests/                            # pytest (62 tests, cov: schemas 100% / locks 90% / client 83%)
  conftest.py                     # fixtures (acta_c/e/p, snapshot_ts_ms, tmp_facts_dir)
  test_id_acta.py                 # fórmula determinística + exterior + padding
  test_normalize_acta.py          # C/E/P/N, especiales, linea_tiempo, archivos, fallbacks
  test_enumerate_tasks.py         # mesas × elecciones → tuples
  test_coerce_null_columns.py     # Null → String/Int64/Float64 + diagonal_relaxed concat
  test_schemas.py                 # validate_chunk + drift detection
  test_locks.py                   # PipelineLock acquire/release/concurrent/exception-safe
  test_build_curated.py           # E2E fixture con 2 runs dedup last-wins
  test_client.py                  # httpx MockTransport: 204, 429, 500, HTML fallback
```

Referencia canónica de endpoints, reverse engineering del API y decisiones de arquitectura: `fuentes_datos.md` (raíz del repo).

## Comandos clave

```bash
# Setup (Python 3.12+)
brew install uv
uv sync

# Smoke end-to-end: ejercita todos los endpoints, valida fórmula idActa
uv run python scripts/smoke.py

# Dims (fase 1) — ubigeos Perú + exterior
uv run python scripts/crawl_dims.py               # ambos ámbitos (default)
uv run python scripts/crawl_dims.py --solo-peru   # omite idAmbitoGeografico=2

# Mesas (fase 2) — inventario físico
uv run python scripts/crawl_mesas.py              # ambos ámbitos (default)

# Snapshot completo (primera vez o re-snapshot nocturno)
uv run python scripts/snapshot_actas.py --rps 15 --concurrency 15

# Subconjunto por ámbito (voto exterior ~12.7k actas, ~15 min)
uv run python scripts/snapshot_actas.py --ambitos 2 --rps 15 --concurrency 15

# Reanudar un run caído (usa run_ts_ms del checkpoint data/state/actas_run_*.json)
uv run python scripts/snapshot_actas.py --resume <run_ts_ms>

# Smoke test del snapshot (50 actas)
uv run python scripts/snapshot_actas.py --limit 50

# Daily refresh (solo volátiles P/E) — --all ejecuta también build_curated + dq_check
uv run python scripts/daily_refresh.py --all --rps 15
uv run python scripts/daily_refresh.py --dry-run            # reporta cuántas entrarían
uv run python scripts/daily_refresh.py --estados P          # solo Pendientes

# Agregados (cada 15-30 min durante conteo activo — series temporales)
uv run python scripts/snapshot_aggregates.py

# Curated + DQ después de cualquier fetch
uv run python scripts/build_curated.py
uv run python scripts/build_curated.py --dry-run
uv run python scripts/dq_check.py                  # todos los niveles (default)
uv run python scripts/dq_check.py --nivel 1        # solo integridad interna

# Analytics + mapa interactivo (requiere curated + dim/geojson)
uv run python scripts/analytics_report.py          # top partidos, participacion, ganadores DE
uv run python scripts/build_choropleth.py          # mapa Leaflet 3 niveles pais->provincia->distrito

# Para runs largos en background en el Mac
nohup caffeinate -dims uv run python scripts/snapshot_actas.py \
  --rps 15 --concurrency 15 > snapshot.log 2>&1 &!
```

## Lint / tests

```bash
# Lint (ruff, configurado en pyproject.toml: line-length=100, selects=E,F,I,UP,B,SIM,N,RUF)
uv run ruff check src scripts
uv run ruff format src scripts

# Tests (87 pytest, ejecutar con --cov=src/onpe para ver cobertura)
uv run pytest
uv run pytest --cov=src/onpe --cov-report=term-missing
```

## Override del DATA_DIR

```bash
# Por default data/ cuelga de la raíz del repo. Override:
ONPE_DATA_DIR=/tmp/onpe_test uv run python scripts/smoke.py
```

## API ONPE — peculiaridades críticas

**idActa** es determinístico: `pad(idMesa, 4) ++ pad(ubigeoDistrito, 6) ++ pad(idEleccion, 2)`. Ejemplo: `550704010210` = mesa 5507 + CAYMA (040102) + Presidencial (10).

**5 elecciones simultáneas:**

| idEleccion | Elección | tipoFiltro |
|---|---|---|
| 10 | Presidencial | eleccion (nacional) |
| 12 | Parlamento Andino | eleccion (nacional) |
| 13 | Diputados | distrito_electoral (27 distritos) |
| 14 | Senadores regional | distrito_electoral |
| 15 | Senadores nacional | eleccion (nacional) |

**Distrito electoral 27 = peruanos en el extranjero.** 1.21M electores, 2,543 mesas, 12,715 actas. `idAmbitoGeografico=2` y ubigeos en rango 91-95xxxx.

**Estados del acta (`codigoEstadoActa`) — 3 buckets:**

- **C** (Contabilizada, 84.2%): terminal. Identidades contables exactas. Excepto 240 con `estadoActa=N` que son "Mesa no instalada" sin detalle.
- **E** (Para envío al JEE, 13.1%): volátil. Tiene `detalle[]` con votos pero totales de cabecera NULL.
- **P** (Pendiente, 2.7%): volátil. Ni detalle ni totales.

Para totales "oficiales" filtrar `codigoEstadoActa == "C"`. Para universo máximo, derivar totales desde `sum(nvotos)` en `actas_votos`.

**Tipos del API (ojo con columnas que parecen numéricas pero son String):**

- String zero-padded: `ccodigo` ("00000014"), `codigoMesa` ("005956"), `ubigeoDistrito` ("040102").
- String formateado: `nporcentajeVotosValidos` ("23.78"), `nporcentajeVotosEmitidos`.
- Int64 real: `idActa`, `idEleccion`, `idMesaRef`, `totalElectoresHabiles`, `totalVotosEmitidos/Validos`, `totalAsistentes`, `nposicion`, `nvotos`, `totalCandidatos`, `codigoSolucionTecnologica`, `fechaRegistro`, `daudFechaCreacion`, `snapshot_ts_ms`.
- Float64 real: `porcentajeParticipacionCiudadana` (cabecera).
- Boolean: `es_especial` (derivado por `normalize_acta`).

Antes de añadir una columna al `_NUMERIC_SCHEMAS` de `src/onpe/actas.py`, verificar empíricamente el tipo con un fetch — si es categórica aunque sea numérica, dejarla como String (default). Se disparó un bug pasado en el que un smoke con totalVotosEmitidos all-null castó a String y contagió el curated entero.

**204 No Content** en combinaciones no aplicables (p. ej. `participantes_nacional` para id=13 Diputados). El cliente ya lo trata como `{"data": None}`.

## Anomalías conocidas

- **240 actas C sin `detalle[]`** — **CERRADA 2026-04-19**. Diagnóstico definitivo via `scripts/investigate_anomaly_240.py` post-A4: 240/240 (100%) clasificadas como **mesa NO instalada** (`codigoEstadoActa=C` + `estadoActa=N` + `detalle=[]`). Son 48 mesas × 5 elecciones del voto exterior que nunca se instalaron físicamente. Excluidas del denominador de DQ identidades contables (el check nivel1_identidades_contables ya hace `inner join` con votos y se queda solo con las que tienen detalle). Reporte persistido en `data/curated/actas_anomalia_240_investigacion.parquet`.
- **`idMesa` NULL en cabecera** — columna legacy droppeada en build_curated; usar `codigoMesa` (String) o `idMesaRef` (Int64).
- **19 actas no-Contabilizadas con totales no-null** — ruido, no bloqueante.
- **`idAmbitoGeografico` + `idDistritoElectoral` en curated** — desde 2026-04-18 se materializan via enrich_curated.py (invocado automáticamente por build_curated salvo --no-enrich).

## Coordinación de jobs (lock file)

`data/state/.pipeline_lock` es un advisory lock (fcntl) que previene colisión de rate-limit entre:
- `snapshot_actas.py` (fetch masivo, 15 rps, ~8h)
- `daily_refresh.py` (subset P/E, 15 rps, ~1h20m)

Ambos scripts toman el lock al arrancar y liberan al salir (incluso en excepción). El snapshot_aggregates loop NO toma el lock (1 req/15 min es negligible).

Si un proceso muere sin limpiar el lock, el archivo queda pero `fcntl.flock(LOCK_EX | LOCK_NB)` detecta que nadie lo tiene reservado y lo re-adquiere al siguiente intento. Para caso patológico (archivo huérfano con `fcntl` inconsistente): `rm data/state/.pipeline_lock`.

## Schema validation (fail-fast contra drift ONPE)

Desde 2026-04-18: `src/onpe/schemas.py` define `SCHEMAS` canónico por tabla. `src/onpe/actas._flush_chunk` invoca `validate_chunk(df, name, strict=True)` antes de cada `write_parquet`. Si ONPE cambia tipos (p.ej. Int → String), el chunk aborta con `SchemaDriftError` antes de contaminar facts.

Mantenimiento: si ONPE añade una columna nueva, validate_chunk emite WARNING pero no bloquea — extraer la columna en `normalize_acta` y añadirla al SCHEMAS correspondiente. Si ONPE REMUEVE una columna esperada o cambia su tipo, se levanta error con violaciones específicas; actualizar SCHEMAS tras verificar en vivo.

## Convenciones de código (instrucciones del usuario)

- **NUNCA usar emojis en código.** Ni en strings, ni en logs, ni en comments.
- **Prints mínimos.** Solo los críticos. Preferir `logging`.
- **Español** para comentarios, nombres técnicos, mensajes de log (el proyecto está en español).
- **uv** para gestión de venv y ejecución. Python 3.12+.
- **Polars** sobre pandas. Usar `scan_parquet` + streaming cuando los datasets son grandes (el usuario tiene Mac M2 24GB — `actas_votos.parquet` ya son 87MB / 18M filas).
- **Hive partitions** para snapshots raw: `snapshot_date=YYYY-MM-DD/run_ts_ms=<epoch_ms>/<chunk_idx>.parquet`.
- **Último-run-gana** en `build_curated` vía semi-join por `max(run_ts_ms)` por idActa.

## Entorno

- Python 3.12+, uv para deps, Polars + httpx + tenacity.
- Los LaunchAgents en `ops/launchd/` asumen macOS; en otros OS adaptar a systemd/cron.

## Plan de trabajo hacia el 100%

Estado al 2026-04-20:

**Completadas 2026-04-18/19**:
- A1 locks.py + integración (fcntl advisory en `data/state/.pipeline_lock`, 90% cov)
- A2 shim aggregates cada 15min (nohup bash loop + caffeinate) — captura serie temporal irrecuperable
- A4 re-snapshot FULL: 8h 53m 50s, 463,830 actas ok=100%, linea_tiempo y archivos poblados al ~100%
- B1 enrich_curated.py + integración en build_curated. Mapeo ONPE-depto → DE: Callao=7, Lima split 15/16 (por provincia 1401), offsets +1/+2 validados
- B2 DQ Nivel 3 (4 checks, 14/14 PASS post-A4)
- B3 investigate_anomaly_240 ejecutado: **240/240 = mesa_no_instalada** (anomalia cerrada)
- C1 tests pytest (77 passing, cov schemas=100%, geojson=97%, locks=90%, client=83%, pdfs=65%)
- C2 schemas.py + validate_chunk en _flush_chunk (fail-fast drift) + jitter ±10% en _throttle
- D2 GeoJSONs peruLow.json (26 deptos + Callao + Lago Titicaca). Provs no expuestas en SPA
- F1 Dashboard `data/dashboard/index.html` autocontenido

**En curso — descarga PDFs**:
- D1 download_pdfs → GCS: running. Endpoint descubierto 2026-04-19 via DevTools: `GET /presentacion-backend/actas/file?id={archivoId}` → `{success, data: signed_s3_url}`. Destino: bucket GCS configurable vía `--gcs-bucket gs://<tu-bucket>` (proyecto y org del usuario). Framework streaming S3→GCS con `asyncio.to_thread` para upload sync (fix perf 0.6→3.2 PDFs/s). ETA ~2.6 días para 725,782 PDFs (~1 TB).

**Blocked — esperan input externo**:
- D5 voto preferencial: sin endpoint API descubierto. Decisión user pendiente: (a) DevTools, (b) excluir, (c) OCR PDFs, (d) esperar datosabiertos.

**Completado 2026-04-19 adicional**:
- D3 datosabiertos: monitor + ingest skeleton + DQ Nivel 4 (2 checks). Flag `--nivel 4` en dq_check.py. Baseline 50 datasets guardados. Deferred hasta publicación ONPE real (~2-4 sem post-JNE).
- D4 histórico EG2021: **REMOVIDO del scope (2026-04-20)**. Dataset v1.0 queda laser-focused en EG2026 primera vuelta con fuentes oficiales solamente. Histórico podrá publicarse aparte como dataset complementario `onpe-eg2021` si se retoma.

**Completado 2026-04-20 (PR #6)**:
- Padrón RENIEC integrado via datosabiertos.gob.pe. `src/onpe/reniec_padron.py` + `scripts/crawl_reniec_padron.py` + 12 tests. Output `data/dim/padron.parquet` (2,039 filas, 27.23M electores Q1 2026 delta 0.46% vs oficial JNE). Columnas: ubigeo_reniec/inei, demografía (sexo, rangos etarios), DNI vigencia.
- **Finding ubigeo**: ONPE.ubigeoDistrito ≡ RENIEC.UBIGEO_RENIEC (100% match). Quien diverge es INEI. Join ONPE ↔ RENIEC es directo vía `ubigeoDistrito ↔ ubigeo_reniec`. Corrige la suposición previa que agrupaba ONPE con INEI/RENIEC.

**Completado 2026-04-20 (PR #7)**:
- El Peruano resoluciones integrado vía `busquedas.elperuano.pe/dispositivo/NL/{op}`. `src/onpe/resoluciones.py` + `scripts/crawl_resoluciones.py` + `data/registry/resoluciones_eg2026.yaml` (7 landmarks: cronograma, reglamentos primarias, padrón, voto digital, miembros mesa exterior) + 26 tests. Output `data/dim/resoluciones.parquet` + PDFs descargados. **GraphQL paginado roto server-side** (error `'hits'`), usamos páginas detalle con `__NEXT_DATA__`. Registry expandible — agregar op IDs al YAML y re-correr.
- ONPE POE: deferred (sitio `www.onpe.gob.pe/elecciones-generales-2026/` retorna 403 a scrapers plano).

**Completado 2026-04-20 (PR #8)**:
- `scripts/export_csv.py`: exporter CSV con filtros (eleccion/DE/depto/provincia/distrito/partido/ambito/estado). 3 formatos: `mesa-partido` (long), `resumen-distrito` (agregado), `cabecera` (compacto). Soporta gzip.
- `scripts/build_curated.py` extendido: genera `data/curated/actas_votos_tidy.parquet` (18.6M filas × 17 cols consumer-friendly) como paso final automático post-enrich.
- `scripts/detect_anomalies.py`: detector baseline con 7 reglas (3 CRITICAL + 2 HIGH + 2 MEDIUM). Cross-check contra padrón RENIEC. Baseline dataset 2026-04-20: **0 CRITICAL, 0 HIGH, 4,600 MEDIUM** (outliers estadísticos, DQ total sano).
- 20 tests nuevos. Full suite 142/142 PASS.

**Completado 2026-04-20 (PR #9)**:
- `scripts/prepare_release.py`: empaqueta `datasets/<version>/` con `parquet/` (14 archivos 158 MB) + `csv/` (5 archivos útiles 131 MB) + `sqlite/onpe_eg2026.db` (5 tablas 128 MB, sin `actas_votos_tidy` por default para mantener tamaño) + `geojson/` (222 archivos 23 MB) + `docs/` + `notebooks/` + `CHECKSUMS.txt` + `README.md`. Total ~441 MB, dentro de límites Kaggle/Zenodo/HF.
- `scripts/generate_cover.py`: render `docs/cover.png` 1200×600 matplotlib.
- 5 notebooks demo: getting-started, participación, resultados presidencial, voto exterior, anomaly detection.
- 6 tests nuevos. Full suite 174/174 PASS.

**Deferred (fuera de scope del 100%)**:
- A0 daemon formal aggregate_loop.py (shim actual cumple SLI)

**Pendiente**:
- G1 finalizar README.md (Quickstart + troubleshooting)

## Recordatorios

- Antes de correr cualquier fetch, verificar que el API responde (headers `referer` + `sec-ch-ua` + `user-agent` exactos del SPA — ver `src/onpe/client.py`).
- Rate-limit: CloudFront de ONPE aguanta 5-10 req/s sin problema; 15 rps es techo seguro.
- Si un run queda colgado, el checkpoint en `data/state/actas_run_<run_ts_ms>.json` permite resume con costo máximo de 500 actas.
- Todos los scripts son idempotentes. `build_curated.py` regenera el curated desde los hive partitions en ~2 min.
