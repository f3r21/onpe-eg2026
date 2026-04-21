# Data Dictionary — onpe-eg2026 v1.0

Referencia canónica de cada columna en las tablas del dataset curated. Los tipos de Polars indicados matchean los declarados en `src/onpe/schemas.py::SCHEMAS` y son validados fail-fast en cada flush de chunk via `validate_chunk()`.

Convenciones de nombres:
- **camelCase**: columnas que matchean 1:1 el JSON del API ONPE (intencional, no se normalizan a snake_case).
- **snake_case**: columnas derivadas o stamps (`snapshot_ts_ms`, `snapshot_lima_iso`, `es_especial`).

---

## `actas_cabecera.parquet`

Una fila por (mesa × elección) = 92,766 mesas × 5 elecciones = **463,830 filas**. Contiene los totales oficiales del acta y los metadatos de la mesa.

| Columna | Tipo | Unidad / formato | Descripción |
|---|---|---|---|
| `idActa` | Int64 | `pad(idMesa,4) + pad(ubigeoDistrito,6) + pad(idEleccion,2)` | PK determinística. Ejemplo: mesa 5507, distrito `040102`, elección 10 → `550704010210`. |
| `idEleccion` | Int64 | 10, 12, 13, 14, 15 | 10=Presidencial, 12=Parlamento Andino, 13=Diputados, 14=Senadores regional, 15=Senadores nacional. |
| `idMesaRef` | Int64 | — | FK a `dim/mesas.idMesa`. |
| `codigoMesa` | String | zero-padded 6 chars | Código físico de la mesa (ej. `"005507"`). |
| `descripcionMesa` | String | libre | Descripción textual, habitualmente el mismo código sin padding o una etiqueta del local. |
| `codigoEstadoActa` | String | `C`, `E`, `P`, `N` | C=Contabilizada (terminal, 84.2%), E=Envío a JEE (volátil, 13.1%), P=Pendiente (volátil, 2.7%), N=Anomalía mesa no instalada (240 casos). |
| `estadoActa` | String | libre | Descripción textual del estado (matchea `codigoEstadoActa`). |
| `estadoComputo` | String | libre | Estado del cómputo electoral para la mesa. |
| `descripcionEstadoActa` | String | libre | Descripción larga del estado. |
| `estadoActaResolucion` | String (nullable) | — | Estado de resolución si ha pasado por JEE. |
| `estadoDescripcionActaResolucion` | String (nullable) | — | Descripción de la resolución. |
| `descripcionSubEstadoActa` | String (nullable) | — | Subestado interno (rara vez poblado). |
| `codigoSolucionTecnologica` | Int64 | categórico | Método de escrutinio/digitalización (OCR, manual, etc.). |
| `descripcionSolucionTecnologica` | String | — | Nombre humano del método de escrutinio. |
| `ubigeoDistrito` | String | 6 dígitos | Ubigeo ONPE (**no INEI**). Ejemplo: `"040102"` = Cayma, Arequipa. |
| `ubigeoNivel01` | String | 2 dígitos | Departamento (ej. `"04"`). |
| `ubigeoNivel02` | String | 4 dígitos | Provincia (ej. `"0401"`). |
| `ubigeoNivel03` | String | 6 dígitos | Distrito (equivalente a `ubigeoDistrito`). |
| `centroPoblado` | String | libre | Nombre del centro poblado donde se ubica la mesa. |
| `nombreLocalVotacion` | String | libre | Nombre del local de votación (ej. "I.E. CORONEL BOLOGNESI"). |
| `totalElectoresHabiles` | Int64 | personas | Electores asignados a la mesa (~200-300 típico, máximo 300). |
| `totalAsistentes` | Int64 (nullable) | personas | Asistentes al acto electoral (se llena post-cómputo). |
| `totalVotosEmitidos` | Int64 (nullable) | votos | Solo no-null en estado C. Total emitidos = válidos + nulos + blancos + impugnados. |
| `totalVotosValidos` | Int64 (nullable) | votos | = `totalVotosEmitidos - nulos - blancos - impugnados`. |
| `porcentajeParticipacionCiudadana` | Float64 (nullable) | 0-100 | `totalVotosEmitidos / totalElectoresHabiles * 100`. Computado por ONPE. |
| `snapshot_ts_ms` | Int64 | epoch milliseconds (UTC) | Timestamp del snapshot que ganó el dedup last-wins. |
| `snapshot_lima_iso` | String | ISO 8601 con TZ Lima | Equivalente humano del snapshot_ts_ms. |

**Columnas agregadas en enrich** (`src/onpe/enrich.py`, después del build_curated):

| Columna | Tipo | Descripción |
|---|---|---|
| `idAmbitoGeografico` | Int64 | 1=Perú, 2=Exterior. Desde dim/mesas. |
| `idDistritoElectoral` | Int64 | 1..27 (26 deptos + 27=Exterior). Derivado de ubigeo + ámbito via mapa ONPE. |
| `ubigeoDepartamento` | String | 6 dígitos: `ubigeoDistrito[:2] + "0000"`. |
| `ubigeoProvincia` | String | 6 dígitos: `ubigeoDistrito[:4] + "00"`. |
| `nombreDistrito` | String | Nombre oficial ONPE del distrito. |

---

## `actas_votos.parquet`

Una fila por (acta × partido/especial). Total: **~18.6M filas** en el snapshot actual. Incluye tanto partidos políticos como votos especiales (blancos, nulos, impugnados).

| Columna | Tipo | Descripción |
|---|---|---|
| `idActa` | Int64 | FK a `actas_cabecera.idActa`. |
| `idEleccion` | Int64 | FK al código de elección (ver arriba). |
| `ubigeoDistrito` | String | Redundante con cabecera para join sin re-fetch. |
| `descripcion` | String | Nombre del partido o del voto especial ("VOTOS EN BLANCO", "VOTOS NULOS", "VOTOS IMPUGNADOS"). |
| `es_especial` | Boolean | True si la `descripcion` es especial (no partido). Derivado. |
| `ccodigo` | String | Código zero-padded del partido (ej. `"00000014"`). String intencional, no Int. |
| `nposicion` | Int64 | Orden dentro del detalle (típico 1..N agrupaciones + 3 especiales). |
| `nvotos` | Int64 | **Votos recibidos** por esta agrupación/especial en esta mesa. Campo principal. |
| `nagrupacionPolitica` | Int64 (nullable) | Id numérico de la agrupación (distinto de ccodigo). |
| `nporcentajeVotosValidos` | Float64 | `nvotos / totalVotosValidos * 100`. Computado por ONPE. |
| `nporcentajeVotosEmitidos` | Float64 | `nvotos / totalVotosEmitidos * 100`. Computado por ONPE. |
| `estado` | Int64 | Flag interno de validación/renderizado ONPE. |
| `grafico` | Int64 | Flag interno de renderizado. |
| `cargo` | String (nullable) | Cargo del candidato (típico null en agrupaciones Presidenciales). |
| `sexo` | String (nullable) | Sexo del candidato principal. |
| `totalCandidatos` | Int64 | Total de candidatos en la lista del partido (relevante Senado/Diputados). |
| `cand_apellido_paterno` | String (nullable) | Apellido paterno del **primer** candidato. |
| `cand_apellido_materno` | String (nullable) | Apellido materno del primer candidato. |
| `cand_nombres` | String (nullable) | Nombres del primer candidato. |
| `cand_doc` | String (nullable) | DNI del primer candidato. |
| `snapshot_ts_ms` | Int64 | Timestamp dedup. |
| `snapshot_lima_iso` | String | ISO 8601 Lima. |

**Nota**: las columnas `cand_*` capturan **solo el primer candidato**. Para Senado/Diputados cada lista tiene hasta 29 candidatos — usar la tabla `actas_candidatos` para el array completo.

---

## `actas_candidatos.parquet`

Una fila por (acta × partido × candidato_idx). Expande el array `detalle.candidato[]` del JSON ONPE. Crítico para análisis a nivel candidato individual en Senado (14, 15) y Diputados (13).

| Columna | Tipo | Descripción |
|---|---|---|
| `idActa` | Int64 | FK a cabecera. |
| `idEleccion` | Int64 | FK al código de elección. |
| `ubigeoDistrito` | String | Redundante para join. |
| `partido_ccodigo` | String | FK a `actas_votos.ccodigo` — identifica el partido al que pertenece el candidato. |
| `candidato_idx` | Int64 | Orden del candidato dentro de la lista (0-based). |
| `apellidoPaterno` | String (nullable) | — |
| `apellidoMaterno` | String (nullable) | — |
| `nombres` | String (nullable) | — |
| `cdocumentoIdentidad` | String (nullable) | DNI del candidato. |
| `snapshot_ts_ms` | Int64 | Dedup. |
| `snapshot_lima_iso` | String | ISO 8601 Lima. |

---

## `actas_linea_tiempo.parquet`

Eventos de transición de estado por acta. Permite reconstruir el historial temporal del conteo para cada mesa.

| Columna | Tipo | Descripción |
|---|---|---|
| `idActa` | Int64 | FK a cabecera. |
| `idEleccion` | Int64 | FK al código de elección. |
| `ubigeoDistrito` | String | Redundante para join. |
| `evento_idx` | Int64 | Orden del evento dentro del historial de la mesa (0-based). |
| `codigoEstadoActa` | String | Estado en este punto del tiempo. |
| `descripcionEstadoActa` | String | Descripción del estado. |
| `descripcionEstadoActaResolucion` | String (nullable) | Si el evento involucra JEE. |
| `fechaRegistro` | Int64 | epoch ms (UTC) del evento. |
| `snapshot_ts_ms` | Int64 | Dedup. |
| `snapshot_lima_iso` | String | ISO 8601 Lima. |

---

## `actas_votos_tidy.parquet` (~18.6M filas)

Vista consumer-friendly de `actas_votos` ya enriquecida con el contexto geográfico desde `actas_cabecera`. Pensada para análisis académico/ML y visualización directa sin join manual. Columna `descripcion` renombrada a `partido` para semántica más obvia.

| Columna | Tipo | Descripción |
|---|---|---|
| `idActa` | Int64 | FK a cabecera. |
| `codigoMesa` | String | Código mesa 6-dig zero-padded. |
| `idEleccion` | Int64 | 10/12/13/14/15. |
| `idAmbitoGeografico` | Int64 | 1=Perú, 2=Exterior. |
| `idDistritoElectoral` | Int64 | 1..27. |
| `ubigeoDepartamento`, `ubigeoProvincia`, `ubigeoDistrito` | String | Ubigeos zero-padded. |
| `nombreDistrito` | String | Nombre oficial ONPE. |
| `codigoEstadoActa` | String | C/E/P/N. |
| `partido` | String | Renamed desde `descripcion`. |
| `ccodigo` | String | zero-padded 8-dig. |
| `es_especial` | Boolean | True para BLANCOS/NULOS/IMPUGNADOS. |
| `nvotos` | Int64 | Votos de la agrupación en la mesa. |
| `totalVotosEmitidos`, `totalVotosValidos`, `totalElectoresHabiles` | Int64 | Totales de la mesa (redundantes para análisis directo). |

Ejemplo:
```python
import polars as pl
tidy = pl.scan_parquet("data/curated/actas_votos_tidy.parquet")
# Top 5 partidos nacional en Presidencial
tidy.filter(pl.col("idEleccion") == 10).group_by("partido").agg(
    pl.col("nvotos").sum()
).sort("nvotos", descending=True).head(5).collect()
```

---

## `actas_archivos.parquet`

Metadata de los PDFs escaneados adjuntos a cada acta (típicamente 2 por mesa: acta de escrutinio + acta de sufragio/resolución). ~811k filas.

| Columna | Tipo | Descripción |
|---|---|---|
| `idActa` | Int64 | FK a cabecera. |
| `idEleccion` | Int64 | FK al código de elección. |
| `ubigeoDistrito` | String | Redundante para join. |
| `archivoId` | String | 24 chars hex. PK para descargar el PDF via `/actas/file?id=<archivoId>`. |
| `tipo` | Int64 | 1=escrutinio, 2=instalación+sufragio combinada, 3=instalación, 4=sufragio, 5=resolución. |
| `nombre` | String | Nombre del archivo (UUID-like). |
| `descripcion` | String | Descripción humana del archivo. |
| `daudFechaCreacion` | Int64 | epoch ms de creación en el data audit. |
| `snapshot_ts_ms` | Int64 | Dedup. |
| `snapshot_lima_iso` | String | ISO 8601 Lima. |

---

## Dimensiones (`dim/`)

Catálogos estáticos referenciados por las tablas fact.

### `dim/mesas.parquet` (92,766 filas)

Inventario físico de mesas: idMesa, codigoMesa, ubigeoDistrito, idAmbitoGeografico, nombre del local, etc.

### `dim/distritos_electorales.parquet` (27 filas)

Los 27 distritos electorales del Perú: 26 departamentos + 27=Residentes en el extranjero (idDistritoElectoral=27, idAmbitoGeografico=2).

### `dim/distritos.parquet` (2,102 filas)

Catálogo ONPE de distritos geográficos.

### `dim/departamentos.parquet` (30 filas)

30 departamentos ONPE (incluye Callao y Lima separados).

### `dim/provincias.parquet` (273 filas)

Catálogo de provincias.

### `dim/locales.parquet` (10,553 filas)

Locales de votación físicos (colegios, universidades, centros comunales).

### `dim/padron.parquet` (2,039 filas)

Padrón electoral RENIEC Q1 2026 agregado por distrito (Perú) + país (voto exterior). Fuente oficial: [datosabiertos.gob.pe OPP-16](https://www.datosabiertos.gob.pe/dataset/reniec-poblaci%C3%B3n-identificada-con-dni-registro-nacional-de-identificaci%C3%B3n-y-estado-civil). Filtrado Edad ≥ 18. Total 27,230,711 electores (delta 0.46% vs 27,356,578 oficiales JNE).

| columna | tipo | descripción |
|---|---|---|
| `ubigeo_reniec` | String | Ubigeo RENIEC 6-dígit zero-padded. **Coincide con ONPE.ubigeoDistrito** (join directo). Vacío para rows Extranjero. |
| `ubigeo_inei` | String | Ubigeo INEI 6-dígit (diverge de RENIEC en Lima=15 vs 14). Vacío para Extranjero. |
| `residencia` | String | `Nacional` (Perú) o `Extranjero` |
| `pais_codigo` | String | Código país RENIEC 4-dígit. Solo pobla cuando `residencia=Extranjero`. |
| `pais_nombre` | String | Nombre del país (Perú para nacional, nombre específico para extranjero). |
| `departamento`, `provincia`, `distrito` | String | Nombres geográficos oficiales (solo nacional). |
| `total_electores` | Int64 | Total electores ≥18 años del ubigeo (Vigente + Caducado). |
| `hombres`, `mujeres` | Int64 | Desglose por sexo. Suma = `total_electores`. |
| `dni_electronico`, `dni_convencional` | Int64 | Desglose por tipo de DNI. Suma = `total_electores`. |
| `rango_18_25`, `rango_26_35`, `rango_36_45`, `rango_46_60`, `rango_61_plus` | Int64 | Bandas etarias. Suma = `total_electores`. |
| `vigentes`, `caducados` | Int64 | Del dataset OPP-4 (vigencia DNI). Suma = `total_electores`. |
| `fuente_trimestre` | String | Trimestre RENIEC del snapshot (ej. `"2026_03"`). |

Join con actas ONPE:
```python
import polars as pl
actas = pl.read_parquet("data/curated/actas_cabecera.parquet")
padron = pl.read_parquet("data/dim/padron.parquet")
cobertura = actas.join(padron, left_on="ubigeoDistrito", right_on="ubigeo_reniec", how="left")
```

### `dim/resoluciones.parquet` (registry curado EG2026)

Registro estructurado de resoluciones oficiales landmark del proceso EG2026 (cronograma, reglamentos primarias, padrón, voto digital, miembros de mesa exterior). Fuente: Diario Oficial El Peruano — páginas `busquedas.elperuano.pe/dispositivo/NL/{op}`. PDFs descargados a `data/raw/resoluciones/{op}.pdf`. Editable en `data/registry/resoluciones_eg2026.yaml`: añadir el `op` del buscador El Peruano + tags curados y correr `scripts/crawl_resoluciones.py` para hidratar metadata en vivo.

| Columna | Tipo | Descripción |
|---|---|---|
| `op_id` | String | ID del dispositivo en El Peruano (ej. `"2388220-1"`). PK. |
| `fecha_publicacion` | String | ISO `YYYY-MM-DD` (normalizado desde `YYYYMMDD` del API). |
| `institucion` | String | `JNE`, `ONPE`, `RENIEC`, `JEE`, …. Campo directo del API o inferido del `nombreDispositivo`. |
| `tipo_dispositivo` | String | `RESOLUCION`, `RESOLUCION JEFATURAL`, …. |
| `nombre_dispositivo` | String | Ej. `"N° 0126-2025-JNE"` o `"N° 000063-2025-JN/ONPE"`. |
| `sumilla` | String | Descripción corta (título oficial del documento). |
| `url_html` | String | URL a la página de detalle en `busquedas.elperuano.pe`. |
| `url_pdf` | String | URL directa del PDF oficial. |
| `sector`, `rubro` | String (nullable) | Categorización interna de El Peruano. |
| `tag_proceso` | String | Etiqueta curada: `EG2026`, `EG2026_PRIMARIAS`, `EG2026_EXTERIOR`. |
| `tag_categoria` | String | Etiqueta curada: `cronograma`, `padron`, `reglamento_primarias`, `voto_digital`, `miembros_mesa`, `observacion`, `cedula`. |
| `url_ok` | Boolean | True si el scraper pudo hidratar metadata en vivo. False → data del YAML fallback. |
| `notas` | String | Notas del curador explicando relevancia de la resolución. |

---

## GeoJSONs (`geojson/`)

222 archivos GeoJSON descargados desde el SPA oficial ONPE:
- `peruLow.json` (1 archivo, país con 26 features: 25 deptos + Callao)
- `departamentos/{ubigeo6}.json` (25 archivos, uno por departamento)
- `provincias/{ubigeo6}.json` (196 archivos, uno por provincia)

Todos en formato GeoJSON FeatureCollection estándar. Polígonos vectoriales listos para Leaflet / folium / kepler.gl.

---

## Convenciones transversales

- **Timestamps**: `snapshot_ts_ms` siempre en UTC epoch milliseconds. `snapshot_lima_iso` equivalente humano con TZ Lima (UTC-5).
- **Null handling**: columnas con `(nullable)` arriba pueden ser `pl.Null`. Las columnas numéricas tienen coerción explícita via `_NUMERIC_SCHEMAS` para evitar contaminación del curated.
- **Enteros vs strings**: Los códigos zero-padded (`codigoMesa`, `ccodigo`, `ubigeoDistrito`) son String **intencionalmente** para preservar el padding. NO convertir a Int.
- **Dedup semántico**: `curated/` tiene una sola fila por `idActa` — el ganador del `max(run_ts_ms)` entre todos los snapshots raw en `facts/`. Versiones históricas disponibles en `facts/<tabla>/snapshot_date=YYYY-MM-DD/run_ts_ms=<epoch_ms>/N.parquet`.

---

## Referencia cruzada

- **Schema canónico en código**: `src/onpe/schemas.py::SCHEMAS` (fail-fast ante drift).
- **Arquitectura end-to-end**: `docs/ARCHITECTURE.md`.
- **Metodología de curación**: `docs/METHODOLOGY.md`.
- **Limitaciones conocidas**: `docs/LIMITATIONS.md`.
- **Citar el dataset**: `CITATION.cff`.
