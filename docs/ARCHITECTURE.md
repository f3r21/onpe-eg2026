# Arquitectura — onpe-eg2026

Documento técnico de referencia del pipeline. Target audience: contributors que quieran entender el flujo end-to-end antes de modificar código.

## Flujo de datos

```
ONPE SPA backend API (reverse-engineered)
  │
  ├── /proceso                     → proceso activo (idProcesoElectoral)
  ├── /resumen-general/*           → agregados nacionales (snapshot cada 15 min)
  ├── /distrito-electoral/*        → catálogo de 27 DEs + totales
  ├── /actas                       → listado paginado por ubigeo
  ├── /actas/{idActa}              → detalle de acta con votos por partido
  └── /actas/file?id=<archivoId>   → signed URL S3 para PDFs escaneados (TTL 660s)

  │
  ▼
src/onpe/
  ├── client.py          httpx async + HTTP/2 + tenacity + rate-limit + jitter 10%
  ├── endpoints.py       wrappers tipados por endpoint + fórmula idActa
  ├── storage.py         helpers Parquet + Lima tz + DATA_DIR override
  ├── crawler.py         crawl jerárquico de ubigeos (fase 1)
  ├── mesas.py           inventario de mesas vía /actas listado (fase 2)
  ├── aggregates.py      snapshots temporales de totales (fase 3a)
  ├── actas.py           snapshot resumable de /actas/{id} con chunked writes
  ├── pdfs.py            descarga PDFs S3→GCS en streaming
  ├── schemas.py         SCHEMAS canónico + validate_chunk fail-fast
  ├── locks.py           fcntl advisory lock (coordinación de jobs)
  ├── geojson.py         descarga de peruLow + deptos + provincias
  └── datosabiertos.py   scraper BS4 para catálogo ONPE datosabiertos

  │
  ▼
data/
  ├── facts/             Hive-particionado por snapshot_date + run_ts_ms
  │   ├── totales/snapshot_date=YYYY-MM-DD/<ts>.parquet
  │   ├── actas_cabecera/.../run_ts_ms=.../N.parquet
  │   └── actas_votos/...
  ├── dim/               catálogos estáticos
  │   ├── ubigeos.parquet
  │   ├── mesas.parquet                (92,766 mesas)
  │   ├── distritos_electorales.parquet (27 DEs)
  │   └── geojson/                     (222 archivos: país + 25 deptos + 196 provs)
  ├── curated/           dedup max(run_ts_ms) por idActa + enrich
  │   ├── actas_cabecera.parquet       (463,830 × 30 cols)
  │   ├── actas_votos.parquet          (18.6M × 22 cols)
  │   ├── actas_linea_tiempo.parquet   (1.27M eventos)
  │   └── actas_archivos.parquet       (811k metadata de PDFs)
  ├── historico/         baseline EG2021
  ├── analytics/         reports + agregados downstream
  └── dashboard/         HTMLs generados

  │
  ▼
scripts/
  ├── smoke.py                     golden path end-to-end del API
  ├── crawl_dims.py                fase 1: ubigeos
  ├── crawl_mesas.py               fase 2: inventario de mesas
  ├── snapshot_aggregates.py       captura agregados (15 min cadence)
  ├── snapshot_actas.py            snapshot full resumable (~8h, 15 rps)
  ├── daily_refresh.py             subset P/E (solo volátiles, ~1h20m)
  ├── build_curated.py             dedup last-run-wins + auto-enrich
  ├── enrich_curated.py            join dim/mesas → idAmbitoGeografico, idDistritoElectoral
  ├── dq_check.py                  4 niveles de Data Quality (N1..N4)
  ├── download_pdfs.py             descarga masiva a disk local o GCS
  ├── dashboard.py                 HTML salud del pipeline
  ├── analytics_report.py          top partidos, participación, DEs, voto exterior
  └── build_choropleth.py          mapa interactivo Leaflet 3 niveles
```

## Coordinación de jobs

`data/state/.pipeline_lock` (fcntl advisory) previene colisión de rate-limit entre:

- `snapshot_actas.py` — fetch masivo 15 rps, ~8h.
- `daily_refresh.py` — subset P/E 15 rps, ~1h20m.

Ambos scripts toman el lock al arrancar y liberan al salir (incluso en excepción). `snapshot_aggregates.py` NO toma el lock (1 req/15 min es negligible).

LaunchAgents (`com.onpe.aggregates`, `com.onpe.datosabiertos`) gestionan cadencia en macOS. Templates en `ops/launchd/*.plist.template` se renderizan via `install.sh` con variables del entorno local.

## Schema validation fail-fast

`src/onpe/schemas.py` define `SCHEMAS` canónico por tabla. `src/onpe/actas._flush_chunk` invoca `validate_chunk(df, name, strict=True)` antes de cada `write_parquet`. Si ONPE cambia tipos (p.ej. Int → String), el chunk aborta con `SchemaDriftError` antes de contaminar `facts/`.

Mantenimiento: si ONPE añade una columna nueva, `validate_chunk` emite WARNING pero no bloquea — extraer la columna en `normalize_acta` y añadirla al SCHEMAS correspondiente.

## Data Quality (4 niveles)

| Nivel | Qué chequea | Implementado |
|---|---|---|
| N1 | Integridad interna: universo, cardinalidad detalle=41, identidades contables, rangos, padrón | ✓ |
| N2 | Cruce vs aggregates ONPE: universo mesas, totalActas, votos idEleccion=10, regiones, partidos | ✓ |
| N3 | Cruces regionales: coherencia ámbito×DE, exterior universo, totales_de, partidos por DE | ✓ |
| N4 | Reconciliación vs datosabiertos.gob.pe CSV oficial | placeholder (espera publicación ONPE) |

## Diccionario de columnas (curated)

### `curated/actas_cabecera.parquet` (463,830 × 30 cols)

| Columna | Tipo | Descripción |
|---|---|---|
| `idActa` | Int64 | PK determinística: `pad(idMesa,4) + pad(ubigeoDistrito,6) + pad(idEleccion,2)` |
| `idEleccion` | Int64 | 10=Presidencial, 12=Parlamento Andino, 13=Diputados, 14=Senadores regional, 15=Senadores nacional |
| `idMesaRef` | Int64 | FK a `dim/mesas.idMesa` |
| `codigoMesa` | String | código zero-padded (6 dígitos) |
| `codigoEstadoActa` | String | C=Contabilizada (84%), E=Envío JEE (13%), P=Pendiente (3%) |
| `estadoActa` | String | descripción textual del estado |
| `idAmbitoGeografico` | Int64 | 1=Perú, 2=Exterior |
| `idDistritoElectoral` | Int64 | 1..27 (27=extranjero) |
| `ubigeoDistrito` | String | ubigeo ONPE 6 dígitos |
| `ubigeoDepartamento` | String | `ubigeoDistrito[:2] + "0000"` |
| `ubigeoProvincia` | String | `ubigeoDistrito[:4] + "00"` |
| `nombreDistrito` | String | nombre oficial ONPE |
| `totalElectoresHabiles` | Int64 | electores asignados a la mesa |
| `totalAsistentes` | Int64 | asistentes al acto |
| `totalVotosEmitidos` | Int64 | solo no-null en estado C |
| `totalVotosValidos` | Int64 | = emitidos - nulos - blancos |
| `totalCandidatos` | Int64 | cantidad de partidos en la boleta |
| `porcentajeParticipacionCiudadana` | Float64 | computado por ONPE |
| `es_especial` | Boolean | flag para mesas especiales (CPI, hospitales, cárceles) |
| `fechaRegistro` | Int64 | epoch ms |
| `daudFechaCreacion` | Int64 | epoch ms (creación en data audit) |
| `snapshot_ts_ms` | Int64 | timestamp del snapshot que ganó (dedup last-wins) |
| `codigoSolucionTecnologica` | Int64 | método de escrutinio |

### `curated/actas_votos.parquet` (18.6M × 22 cols)

Una fila por `(idActa, partido)`. Campos clave:

| Columna | Tipo | Descripción |
|---|---|---|
| `idActa` | Int64 | FK a cabecera |
| `idPartidoPolitico` | Int64 | identificador del partido |
| `nombrePartido` | String | nombre comercial |
| `siglas` | String | acrónimo |
| `nposicion` | Int64 | orden en la boleta |
| `nvotos` | Int64 | votos crudos (campo principal) |
| `nporcentajeVotosValidos` | String | formateado "XX.XX" |
| `snapshot_ts_ms` | Int64 | dedup |

### `curated/actas_linea_tiempo.parquet` (1.27M)

Eventos de transición de estado por acta. Permite reconstruir el historial del conteo.

### `curated/actas_archivos.parquet` (811k)

Metadata de PDFs escaneados (~2 por acta: escrutinio + resolución).

| Columna | Descripción |
|---|---|
| `archivoId` | PK para fetchar PDF |
| `tipo` | 1=escrutinio, 2=inst+sufragio combinada, 3=inst, 4=sufragio, 5=resolución |
| `fechaRegistro` | epoch ms |

## Determinística `idActa`

```
idActa = pad(idMesa, 4) ++ pad(ubigeoDistrito, 6) ++ pad(idEleccion, 2)
```

Ejemplo: mesa 5507 + distrito CAYMA (040102) + Presidencial (10) →
`idActa = 550704010210`

Esto permite cruzar actas sin depender de IDs opacos de ONPE.

## Anomalías documentadas

- **240 actas C sin `detalle[]`**: 100% clasificadas como mesa NO instalada (`codigoEstadoActa=C` + `estadoActa=N` + `detalle=[]`). Son 48 mesas × 5 elecciones del voto exterior que nunca se instalaron. Reporte en `data/curated/actas_anomalia_240_investigacion.parquet`.

- **19 actas no-C con totales no-null**: ruido documentado, no bloqueante.

- **`idMesa` NULL en cabecera legacy**: columna droppeada en `build_curated.py`. Usar `codigoMesa` (String) o `idMesaRef` (Int64).

## Convenciones de código

- **NUNCA emojis** en código, logs, comments.
- **Español** para comentarios y mensajes.
- **Polars** sobre pandas; `scan_parquet` + streaming para datasets grandes.
- **Hive partitions** para facts raw; **last-run-wins** en curated vía `max(run_ts_ms)`.
- **Idempotencia obligatoria**: todos los scripts deben reanudar desde checkpoint JSON.
- **uv** para deps y venv; Python 3.12+.

## Referencias adicionales

- `CLAUDE.md` — contexto operativo completo (estado actual, comandos, anomalías).
- `fuentes_datos.md` — reverse engineering del API ONPE, decisiones de arquitectura, apéndice A con headers CloudFront.
- `CHANGELOG.md` — historial de releases.
- `CONTRIBUTING.md` — guía para contributors externos.
