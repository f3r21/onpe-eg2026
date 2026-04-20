# onpe-eg2026

[![CI](https://github.com/f3r21/onpe-eg2026/actions/workflows/ci.yml/badge.svg)](https://github.com/f3r21/onpe-eg2026/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![uv](https://img.shields.io/badge/packaging-uv-DE5FE9)](https://github.com/astral-sh/uv)
[![Polars](https://img.shields.io/badge/data-Polars-CD792C)](https://pola.rs/)
[![Tests](https://img.shields.io/badge/tests-87%20passing-brightgreen)](tests/)
[![DQ](https://img.shields.io/badge/DQ-14%2F14%20PASS-brightgreen)](scripts/dq_check.py)
[![Release](https://img.shields.io/github/v/release/f3r21/onpe-eg2026?include_prereleases)](https://github.com/f3r21/onpe-eg2026/releases)

Ingesta de resultados de las **Elecciones Generales Perú 2026** desde el API reverse-engineered de ONPE (`resultadoelectoral.onpe.gob.pe/presentacion-backend/`). Produce un data lake en Parquet con particiones Hive, listo para consumir con Polars, DuckDB o pandas.

**Dataset**: 463,830 actas × 5 elecciones · 18.6M filas de votos · 222 GeoJSONs · 4 niveles de Data Quality. **Scope**: Elecciones Generales Perú 2026 — **primera vuelta** (12-abr-2026). Solo fuentes oficiales del gobierno peruano (ONPE principalmente; JNE, RENIEC y El Peruano en roadmap).

**Docs**: [CHANGELOG](CHANGELOG.md) · [CONTRIBUTING](CONTRIBUTING.md) · [ARCHITECTURE](docs/ARCHITECTURE.md) · [SECURITY](SECURITY.md) · [Release notes v1.0](docs/RELEASE_NOTES_v1.0.md) · [fuentes_datos.md](fuentes_datos.md)

Cubre las 5 elecciones simultáneas:

| idEleccion | Elección                                    |
|-----------:|---------------------------------------------|
| 10         | Presidencial                                |
| 12         | Parlamento Andino                           |
| 13         | Diputados                                   |
| 14         | Senadores — distrito electoral múltiple     |
| 15         | Senadores — distrito electoral único        |

Ver `fuentes_datos.md` para el mapa completo de endpoints, la fórmula determinística del `idActa`, decisiones arquitectónicas y peculiaridades del API. Ver `CLAUDE.md` para convenciones operacionales.

## Setup

```bash
brew install uv
uv sync             # instala runtime
uv sync --extra dev # runtime + dev deps (pytest, ruff)

# Opcional: configurar variables de entorno (solo si usás descarga de PDFs a GCS)
cp .env.example .env  # editá .env con tus valores
```

Requiere Python 3.12+.

## Quickstart (primera vez en un Mac fresh)

```bash
# 1. Validar conectividad contra ONPE (smoke end-to-end, ~1 min)
uv run python scripts/smoke.py

# 2. Dimensiones geográficas (Perú + exterior, ~10 min)
uv run python scripts/crawl_dims.py
uv run python scripts/crawl_mesas.py

# 3. Snapshot aggregates (~1 min, barato, repetible)
uv run python scripts/snapshot_aggregates.py

# 4. Snapshot completo de actas (~8-9h @ 15 rps)
nohup caffeinate -dims uv run python scripts/snapshot_actas.py \
  --rps 15 --concurrency 15 > logs/snapshot_full.log 2>&1 & disown

# 5. Consolidar + enriquecer + validar
uv run python scripts/build_curated.py     # auto-enrich
uv run python scripts/dq_check.py          # 14/14 esperado

# 6. GeoJSONs para visualizaciones (peruLow, ~40 KB)
uv run python scripts/download_geojsons.py

# 7. Dashboard HTML
uv run python scripts/dashboard.py && open data/dashboard/index.html

# 8. (Opcional, ~1 TB) Descargar PDFs binarios de actas a GCS
#    Requiere: proyecto GCP + bucket + gcloud auth application-default login
#    Endpoint ONPE: GET /presentacion-backend/actas/file?id={archivoId}
gcloud auth application-default login
gcloud storage buckets create gs://<tu-bucket> --location=us-central1 --default-storage-class=STANDARD
nohup caffeinate -dims uv run python scripts/download_pdfs.py \
  --gcs-bucket gs://<tu-bucket> --rps 12 --concurrency 20 \
  > logs/pdfs.log 2>&1 & disown
# 725k PDFs ≈ 1 TB ≈ 2-3 días a 3 PDFs/s. Idempotente (skip_existing en GCS).
#
# Descarga distribuida en varios hosts (Mac + Windows, etc.) — reduce ETA 3x.
# Cada worker corre con `--shard M/N`; particiones disjuntas via md5.
# Ver docs/DISTRIBUTED_DOWNLOAD.md para el setup completo.
```

## Flujo diario (durante conteo activo)

Las series temporales del avance de conteo son **irrecuperables** — cada 15 min sin snapshot es data perdida. Mantener el loop de aggregates corriendo en background:

```bash
# Loop permanente (shim bash hasta que se reemplace por daemon formal)
nohup caffeinate -dims bash -c '
  while true; do
    uv run python scripts/snapshot_aggregates.py >> logs/aggregates_shim.log 2>&1
    sleep 900
  done
' >/dev/null 2>&1 & disown
```

Para refrescar los ~73k volátiles P+E (~1h20m a 15 rps):

```bash
uv run python scripts/daily_refresh.py --all --rps 15
```

`--all` encadena `daily_refresh → build_curated → dq_check`.

## Pipeline — etapas

| Fase | Script | Output | Coste |
|---|---|---|---|
| 1 | `crawl_dims.py` | `data/dim/{departamentos,provincias,distritos,locales}.parquet` | ~8 min |
| 2 | `crawl_mesas.py` | `data/dim/mesas.parquet` | ~8 min (92,766 mesas) |
| 3a | `snapshot_aggregates.py` | `data/facts/{totales,mapa_calor,...}/snapshot_date=.../<ts>.parquet` | ~1 min |
| 3b | `snapshot_actas.py` | `data/facts/{actas_cabecera,actas_votos,actas_linea_tiempo,actas_archivos}/snapshot_date=.../run_ts_ms=.../N.parquet` | ~8h (463,830 actas) |
| 3b' | `daily_refresh.py` | ídem 3b pero solo P+E | ~1h20m (~73k volátiles) |
| 4 | `build_curated.py` | `data/curated/*.parquet` (dedup max run_ts_ms + enrich) | ~1 min |
| 5 | `enrich_curated.py` | (in-place) agrega idAmbitoGeografico + idDistritoElectoral | ~5s |
| 6 | `dq_check.py` | report console (Niveles 1+2+3) | ~10s |
| 7 | `download_geojsons.py` | `data/geojson/peruLow.json` | ~5s |
| 8 | `dashboard.py` | `data/dashboard/index.html` | ~10s |

## Niveles de DQ (Data Quality)

`uv run python scripts/dq_check.py` ejecuta 14 chequeos en 3 niveles:

- **Nivel 1 (integridad interna, 5 checks)**: universo por elección, cardinalidad detalle=41, identidades contables `sum(nvotos) == totales` en C, rangos/signos, padrón coherente por mesa.
- **Nivel 2 (cruce aggregates ONPE, 5 checks)**: universo mesas ONPE vs curated, totalActas por elección, votos id=10 drift ≤0.1%, regiones mapa_calor, partidos id=10 estructural + drift ≤0.5%.
- **Nivel 3 (cruces regionales, 4 checks)**: coherencia ámbito × DE, exterior DE=27 universo=2543, totales_de Diputados vs curated, partidos Diputados por DE con drift ≤5%.
- **Nivel 4 (reconciliación post-proclamación, futuro)**: cruce contra datosabiertos.gob.pe CSV oficial cuando ONPE lo publique.

## Schema contract (fail-fast drift detection)

Desde 2026-04-18, `src/onpe/schemas.py` define `SCHEMAS` canónico por tabla. Cada `_flush_chunk` ejecuta `validate_chunk(df, table, strict=True)` antes de `write_parquet`. Si ONPE cambia el tipo de una columna (p.ej. Int → String), el chunk aborta con `SchemaDriftError` antes de contaminar facts.

Si ONPE agrega una columna: warning, pero no bloquea. Para ingerirla, extraer en `normalize_acta` y añadir a SCHEMAS.

## Coordinación de jobs (lock file)

`data/state/.pipeline_lock` es un advisory lock (fcntl) que previene colisión de rate-limit entre `snapshot_actas.py` y `daily_refresh.py` (ambos @ 15 rps). El loop de aggregates (1 req / 15 min) NO toma el lock — es seguro en paralelo.

## Smoke + tests

```bash
uv run python scripts/smoke.py   # E2E contra API real
uv run pytest --cov=src/onpe     # 77 tests, cov 51% overall (schemas=100%, geojson=97%, locks=90%, client=83%)
```

Los módulos en 0% de cov (`aggregates.py`, `crawler.py`, `mesas.py`) son orquestación async que se valida via `smoke.py`.

## Layout de datos

```
data/
├── dim/                          # catálogos estáticos
│   ├── distritos.parquet
│   ├── locales.parquet
│   ├── mesas.parquet             # 92,766 mesas (90,223 Perú + 2,543 exterior)
│   └── distritos_electorales.parquet  # 27 DE (1-26 + 27 extranjero)
├── geojson/
│   └── peruLow.json              # 26 deptos + Callao + decorativo
├── facts/                        # snapshots versionados (Hive)
│   ├── totales/                  # serie temporal, una fila/elección/snapshot
│   ├── totales_de/               # por distrito electoral
│   ├── mapa_calor/               # nivel departamento
│   ├── participantes/
│   ├── participantes_de/
│   ├── resumen_elecciones/
│   ├── mesa_totales/
│   ├── ultima_fecha/
│   ├── actas_cabecera/           # una fila por idActa × run_ts_ms
│   ├── actas_votos/              # una fila por (idActa × descripcion) × run_ts_ms
│   ├── actas_linea_tiempo/       # eventos de cambio de estado por acta
│   └── actas_archivos/           # metadata de PDFs (nombre UUID.pdf + tipo)
├── curated/                      # dedupado, último run (consumo directo)
│   ├── actas_cabecera.parquet    # 463,830 filas, enriquecido con DE+ambito
│   ├── actas_votos.parquet       # 18.6M filas
│   ├── actas_linea_tiempo.parquet
│   ├── actas_archivos.parquet    # metadata; binarios PDF en data/pdfs/
│   └── actas_anomalia_240_investigacion.parquet  # 240 mesas no-instaladas
├── pdfs/                         # binarios PDF (sharded XX/YY/archivoId.pdf)
├── dashboard/                    # HTML salud pipeline
│   └── index.html
├── state/                        # checkpoints JSON runs resumables + lock file
└── smoke/                        # muestras del smoke test
```

## Estructura del código

Ver `CLAUDE.md` para arquitectura detallada. Resumen:

```
src/onpe/
  client.py     # HTTP async httpx + HTTP/2 + tenacity + rate-limit + jitter
  endpoints.py  # wrappers tipados de cada endpoint + fórmula idActa
  storage.py    # helpers Parquet + Lima tz + DATA_DIR
  crawler.py    # crawl jerárquico ubigeos (fase 1)
  mesas.py      # inventario de mesas vía /actas listado (fase 2)
  aggregates.py # snapshot de totales/mapa-calor/resumen/participantes
  actas.py      # snapshot resumable de /actas/{idActa} + chunked writes + schema validation
  locks.py      # advisory file lock (fcntl) para coordinar jobs
  schemas.py    # SCHEMAS + validate_chunk fail-fast drift
  pdfs.py       # descarga PDFs binarios streaming S3→GCS (endpoint /actas/file?id=)
  geojson.py    # descarga peruLow.json

scripts/
  # Ingesta
  crawl_dims.py, crawl_mesas.py, snapshot_aggregates.py, snapshot_actas.py, daily_refresh.py
  # Consolidación + enriquecimiento
  build_curated.py, enrich_curated.py
  # Calidad
  dq_check.py, smoke.py, investigate_anomaly_240.py, validate_ambitos.py
  # Fuentes externas / visualización
  download_geojsons.py, download_pdfs.py, dashboard.py
  # Mantenimiento
  migrate_null_dtypes.py
```

## Troubleshooting

| Síntoma | Causa probable | Fix |
|---|---|---|
| `OnpeError: no-JSON en /xxx (content-type=text/html)` | CloudFront rechazó el request (headers incorrectos o patrón de scraping). | Verificar `DEFAULT_HEADERS` en `src/onpe/client.py`. No aumentar rps > 15. |
| `LockHeldError: pipeline_lock ocupado por PID=N` | Otro `snapshot_actas` o `daily_refresh` corriendo. | `ps -ef \| grep snapshot_actas` — esperar o kill si zombie. |
| `SchemaDriftError en _flush_chunk` | ONPE cambió el tipo de una columna. | Inspeccionar el error, ajustar `SCHEMAS` en `src/onpe/schemas.py` tras verificar en vivo. |
| `build_curated OOM` | Chunk lf demasiado grande. | Ya usa `sink_parquet` streaming; bajar `CHUNK_SIZE` en `SnapshotConfig` si persiste. |
| `actas_linea_tiempo.parquet` casi vacío | Snapshot pre-task-#45 (features agregados post-ingesta). | Correr un re-snapshot full (`snapshot_actas.py --rps 15`). |
| Disk full en `data/pdfs/` | 50GB de PDFs binarios. | `du -sh data/pdfs/ && df -h .` — rotar si no se necesitan. |
| Respuesta 204 No Content en logs | Combinación no aplicable (p.ej. `participantes_nacional` con id=13 Diputados). | Esperado. Cliente ya lo normaliza a `{"data": None}`. |
| Checkpoint "corrupto" al reanudar | Escritura atómica falló en un crash muy temprano. | `jq . data/state/actas_run_<ts>.json` para verificar. Si ilegible, borrar y empezar fresh. |

## Referencias

- `fuentes_datos.md` — documentación exhaustiva de fuentes, endpoints y decisiones
- `CLAUDE.md` — convenciones operacionales + estado empírico del pipeline
- Portal oficial: https://resultadoelectoral.onpe.gob.pe/
- Datos abiertos ONPE: https://www.onpe.gob.pe/
- Histórico: https://resultadoshistorico.onpe.gob.pe/
