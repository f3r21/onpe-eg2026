# Methodology — onpe-eg2026 v1.0

Documento de metodología de recolección, curación y validación del dataset de las Elecciones Generales Perú 2026 primera vuelta. Para contributors, investigadores y usuarios que necesiten entender cómo se construye el dataset.

## 1. Scope

- **Elección**: Elecciones Generales del Perú 2026, **primera vuelta** celebrada el 12 de abril de 2026.
- **Cobertura electoral**: 5 elecciones simultáneas en una misma jornada:
  - Presidencial (`idEleccion=10`)
  - Parlamento Andino (`idEleccion=12`)
  - Diputados (`idEleccion=13`)
  - Senadores regional (`idEleccion=14`)
  - Senadores nacional (`idEleccion=15`)
- **Geografía**: Perú (ámbito 1) + peruanos residentes en el extranjero (ámbito 2, 210 ciudades / 2,543 mesas).
- **Universo**: 92,766 mesas × 5 elecciones = **463,830 actas**.
- **Fuera de scope**: segunda vuelta (si ocurre post-proclamación JNE), elecciones anteriores (EG2021 etc.), elecciones regionales/municipales (ERM).

## 2. Fuentes oficiales

### 2.1 ONPE — Oficina Nacional de Procesos Electorales (fuente primaria)

**Entidad**: órgano autónomo del Estado peruano encargado de ejecutar los procesos electorales.

**Endpoints capturados**:

| Endpoint | Uso | Frecuencia |
|---|---|---|
| `/proceso/proceso-electoral-activo` | metadata del proceso (idProcesoElectoral) | una vez |
| `/resumen-general/totales` | totales nacionales y por distrito electoral | cada 15 min durante conteo |
| `/resumen-general/participantes` | agrupaciones políticas + candidatos destacados | cada 15 min |
| `/resumen-general/mapa-calor` | avance de conteo por departamento | cada 15 min |
| `/distrito-electoral/distritos` | catálogo 27 DEs | una vez |
| `/ubigeos/{nivel}` | catálogo geográfico (depto→prov→dist→local) | una vez |
| `/mesa/totales` | agregado nacional de mesas | cada 15 min |
| `/fecha/listarFecha` | timestamp de última sync ONPE | cada 15 min |
| `/actas/{idActa}` | detalle acta-mesa (votos por partido + línea de tiempo + metadata PDFs) | snapshot inicial + refresh de P/E |
| `/actas/file?id={archivoId}` | signed URL S3 con TTL 660s para descargar PDF | una vez por archivoId |

**Rate limit aplicado**: 15 rps por IP (techo CloudFront observado empíricamente). Jitter ±10% para anti-detección de patrones.

### 2.2 ONPE SPA assets — GeoJSONs

`https://resultadoelectoral.onpe.gob.pe/assets/lib/amcharts5/geodata/json/`

222 archivos estáticos (1 nacional + 25 deptos + 196 provs) listos para visualizaciones choropléticas.

### 2.3 ONPE S3 — PDFs escaneados

Los signed URLs retornados por `/actas/file?id=<archivoId>` apuntan al bucket S3 interno de ONPE (`pr-ciudadanos-prd-actas-bucket`). Streaming directo S3→GCS sin pasar por disco local.

**Volumen**: ~811,984 archivoIds únicos, ~1 TB total.

### 2.4 Fuentes oficiales adicionales (roadmap)

Documentadas en `fuentes_datos.md`, pendientes de scraper:

- **JNE Plataforma Electoral** — candidatos EG2026 (DNI, nombre, partido oficial)
- **RENIEC Padrón Electoral 2026** — 27.36M electores por distrito
- **datosabiertos.gob.pe** — CSV oficial post-proclamación (~4 semanas post-JNE)
- **El Peruano** — resoluciones oficiales del proceso
- **ONPE comunicados/POE** — Plan Operativo Electoral

## 3. Pipeline de ingesta

### 3.1 Fase 1 — Catálogos geográficos (`crawl_dims.py`)

Crawl jerárquico de ubigeos: país → departamento → provincia → distrito → local de votación. Genera `data/dim/{departamentos,provincias,distritos,locales}.parquet`.

**Costo**: ~8 minutos a 15 rps.

### 3.2 Fase 2 — Inventario de mesas (`crawl_mesas.py`)

Paginación sobre `/actas` por ubigeo para enumerar las 92,766 mesas físicas. Genera `data/dim/mesas.parquet`.

**Costo**: ~8 minutos.

### 3.3 Fase 3a — Aggregates (`snapshot_aggregates.py`)

Captura de totales/participantes/mapa_calor por elección y distrito electoral en un single snapshot. Repetible cada 15 min para capturar **series temporales** del conteo.

**Costo**: ~1 minuto por snapshot. **Crítico**: las series temporales del conteo son **irrecuperables** — si no se snapshotean en vivo, se pierden para siempre.

### 3.4 Fase 3b — Snapshot actas (`snapshot_actas.py`)

Fetch de `/actas/{idActa}` para las 463,830 actas. Chunked Parquet writes con checkpoint JSON resumable. Shardable `--shard M/N` para distribución en múltiples workers.

**Costo**: ~8 horas a 15 rps con 1 worker. ~3 horas con 3 workers paralelos.

### 3.5 Fase 3b' — Daily refresh (`daily_refresh.py`)

Variante incremental: solo re-fetch de las actas en estados volátiles P (pendiente) y E (envío a JEE). Universo típico: ~73k actas.

**Costo**: ~1h20m a 15 rps.

### 3.6 Fase 4 — Build curated (`build_curated.py`)

Dedupe `max(run_ts_ms)` por `idActa` sobre los facts raw Hive-particionados. Genera `data/curated/*.parquet` (actas_cabecera, actas_votos, actas_candidatos, actas_linea_tiempo, actas_archivos).

Patrón: **last-run-wins**. Si un acta tiene varios snapshots, se queda con el más reciente (`run_ts_ms` máximo). El resto queda disponible en `data/facts/` para análisis histórico granular.

**Costo**: ~1 minuto (streaming Polars `sink_parquet`).

### 3.7 Fase 5 — Enriquecimiento (`enrich_curated.py`)

Join con `dim/mesas.parquet` para agregar columnas derivadas:
- `idAmbitoGeografico` (1=Perú, 2=Exterior)
- `idDistritoElectoral` (1..27) — derivado via mapa ONPE deptos (Callao=7 insertado, Lima split 15/16 por provincia 140100)
- `ubigeoDepartamento`, `ubigeoProvincia` — slices del `ubigeoDistrito`
- `nombreDistrito`

**Costo**: ~5 segundos.

### 3.8 Fase 6 — Validación DQ (`dq_check.py`)

4 niveles de Data Quality (ver sección 4).

### 3.9 Fase 7 — PDFs binarios (`download_pdfs.py`, opcional)

Streaming S3→GCS de los 811k PDFs escaneados. Shardable por `--shard M/N` + filtrable por `--estados C,E,P` y `--tipos 1,2,3,4,5`.

**Costo**: ~2-3 días para universo completo (~1 TB @ 3-8 PDFs/s combinado con 3 workers).

## 4. Data Quality (4 niveles)

`scripts/dq_check.py` ejecuta 14 checks en 4 niveles jerárquicos. El dataset v1.0 pasa 14/14 en N1+N2+N3.

### 4.1 Nivel 1 — Integridad interna (5 checks)

Verifica consistencia dentro del dataset sin fuentes externas:

1. **Universo por elección**: 92,766 mesas × 5 elecciones = 463,830 actas, sin duplicados.
2. **Cardinalidad detalle = 41**: cada acta tiene 38 agrupaciones + 3 especiales (BLANCOS, NULOS, IMPUGNADOS).
3. **Identidades contables**: en actas en estado C, `sum(nvotos) == totalVotosValidos + blancos + nulos + impugnados`. **390,827 actas C con detalle — todas exactas (0 mismatches)**.
4. **Rangos y signos**: emitidos/válidos >= 0; hábiles > 0; emitidos <= hábiles; válidos <= emitidos.
5. **Padrón coherente por mesa**: cada mesa tiene exactamente un valor de `totalElectoresHabiles` consistente entre las 5 elecciones.

### 4.2 Nivel 2 — Cruce contra aggregates ONPE (5 checks)

Compara el dataset curado (derivado de `/actas/{id}` por mesa) contra los aggregates oficiales ONPE (`/resumen-general/totales`):

6. **Universo mesas**: oficial = curated = 92,766.
7. **Total actas por elección**: oficial = curated para las 5 elecciones.
8. **Votos Presidencial drift ≤ 0.1%**: drift típico 0.08%.
9. **Regiones mapa_calor**: 25 deptos match, max delta ≤ 33 actas.
10. **Partidos Presidencial estructural + drift ≤ 0.5%**: 36/36 pares match.

### 4.3 Nivel 3 — Cruces regionales (4 checks)

Coherencia entre enriquecimiento y catálogos:

11. **Coherencia ámbito × DE**: `idDistritoElectoral=27 ↔ idAmbitoGeografico=2`.
12. **Exterior DE=27 universo**: 2,543 mesas.
13. **Totales_de Diputados vs curated**: 27/27 DEs match, max_delta=0.
14. **Partidos por DE Diputados**: 825/825 pares match, drift ≤2.16% por DE.

### 4.4 Nivel 4 — Reconciliación oficial (placeholder)

Pendiente publicación ONPE en `datosabiertos.gob.pe` (~4 semanas post-proclamación JNE). Monitor semanal activo via LaunchAgent `com.onpe.datosabiertos`. Cuando ONPE publique:

- Ingesta automática vía `scripts/ingest_datosabiertos.py`.
- Reconciliación votos por (partido × distrito × elección).
- Delta esperado: ≤1 voto por grupo (el oficial de datosabiertos es el estado post-JNE con correcciones de impugnaciones).

## 5. Schema validation fail-fast

`src/onpe/schemas.py::SCHEMAS` define el contrato canónico. En cada `_flush_chunk` antes de `write_parquet`:

```python
validate_chunk(df, table_name, strict=True)
# Si hay drift → raise SchemaDriftError → chunk abortado, checkpoint guardado
```

Tipos de drift detectables:
- `mismatch`: columna existe pero con dtype distinto (típico: Int → String por promoción de Null en chunk vacío).
- `missing`: columna esperada ausente (ONPE removió un campo).
- `extra`: columna nueva en df (ONPE agregó — solo warning, no bloquea).

Mantenimiento: si ONPE cambia algo, actualizar `SCHEMAS` y correr tests.

## 6. Coordinación de jobs concurrentes

`data/state/.pipeline_lock` (fcntl advisory) previene colisión de rate-limit entre:
- `snapshot_actas.py` (fetch masivo, 15 rps, ~8h)
- `daily_refresh.py` (subset P/E, 15 rps, ~1h20m)

`snapshot_aggregates.py` **NO toma el lock** (1 req / 15 min es negligible, seguro en paralelo).

## 7. Convenciones de curación

Decisiones específicas que pueden sorprender:

### 7.1 `codigoMesa` como String zero-padded

El API ONPE devuelve `"005507"` (string con padding), NO `5507` (int). Se preserva el String para no perder el padding. `idMesaRef` sí es Int (clave numérica pura).

### 7.2 `nporcentajeVotosValidos` como String formateado

El campo viene como `"23.78"` (string con 2 decimales). Se preserva así para no perder la precisión visual. Cast a Float es responsabilidad del consumidor.

### 7.3 Anomalía 240 actas C sin detalle

240 actas con `codigoEstadoActa=C` + `estadoActa=N` + `detalle=[]`. **Clasificadas como mesa_no_instalada** (48 mesas del voto exterior × 5 elecciones que nunca se instalaron físicamente). **Excluidas del denominador** en el check de identidades contables (el inner join con votos automáticamente las descarta). Reporte en `data/curated/actas_anomalia_240_investigacion.parquet`.

### 7.4 Ubigeo ONPE ≠ Ubigeo INEI/RENIEC

ONPE usa su propio esquema de ubigeos, distinto del INEI/RENIEC. Ejemplo:
- **Lima en ONPE**: depto 14 (orden alfabético ignorando Callao)
- **Lima en INEI**: depto 15

Para joins contra padrón RENIEC se necesita tabla cross-walk (pendiente v1.1).

### 7.5 Distrito Electoral mapping

Los 27 distritos electorales ordenan alfabéticamente con particularidades:
- Callao = DE 7 (insertado fuera del orden alfabético ONPE)
- Lima se splitea: `provincia 1401 → DE 15 (Lima Metropolitana)`, resto dep 14 → DE 16 (Lima Provincias)
- Ucayali = DE 26 (último antes de exterior)
- Exterior = DE 27

Ver `src/onpe/enrich.py::compute_distrito_electoral` para la lógica exacta.

## 8. Reproducibilidad

Versiones exactas de deps en `uv.lock` y `pyproject.toml`. Entorno:
- Python 3.12+
- uv como gestor de deps
- Polars streaming (no cargar todo en RAM)
- httpx async + HTTP/2

Para reproducir desde cero:

```bash
brew install uv  # o winget install astral-sh.uv
git clone https://github.com/f3r21/onpe-eg2026
cd onpe-eg2026
uv sync
uv run python scripts/smoke.py   # validar acceso ONPE
uv run python scripts/crawl_dims.py
uv run python scripts/crawl_mesas.py
uv run python scripts/snapshot_actas.py
uv run python scripts/build_curated.py
uv run python scripts/dq_check.py   # esperado: 14/14 PASS
```

## 9. Versionado

SemVer. El dataset sigue la evolución del proceso electoral:

- **v1.0.0** — publicación inicial pre-proclamación JNE (2026-04-19).
- **v1.1.0** (planned, ~4 semanas) — post-proclamación + reconciliación contra datosabiertos.gob.pe + fuentes JNE/RENIEC.
- **v2.0.0** (hipotético) — si ONPE publica voto preferencial o si incluimos segunda vuelta.

## 10. Integridad de archivos

Cada release incluye `checksums.txt` con SHA256 de todos los archivos. Verificar:

```bash
sha256sum -c checksums.txt
```

## Referencias

- [Pipeline architecture](ARCHITECTURE.md) — flujo de datos end-to-end.
- [Data dictionary](DATA_DICTIONARY.md) — contratos de columna.
- [Limitations](LIMITATIONS.md) — gaps conocidos y sesgos.
- [Audit report v1.0](AUDIT_v1.0.md) — findings de seguridad y calidad aplicados.
- [Citation info](../CITATION.cff) — cómo citar este dataset.
