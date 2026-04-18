# Fuentes de datos — Elecciones Generales Perú 2026

Inventario de fuentes para construir un dataset nacional + regional de votos y actas. Prioriza velocidad de implementación, completitud y estabilidad a 6 meses. Cada fuente trae método de acceso, granularidad real, formato, limitaciones y —donde se pudo validar— URLs concretas probadas.

## 1. Portal de resultados ONPE (la fuente autoritativa viva)

**URL:** https://resultadoelectoral.onpe.gob.pe/
**Contexto:** SPA Angular 17, servida por CloudFront + S3 (us-east-1). Es la fuente oficial en tiempo real mientras se digitalizan actas.

**Arquitectura descubierta** (extraída del `Content-Security-Policy` del servidor):

- Backend API: **mismo host** (`connect-src 'self'`) → los endpoints JSON cuelgan de `https://resultadoelectoral.onpe.gob.pe/...`. No hay subdominio `api.*` como en 2021.
- Actas escaneadas (imágenes de las actas físicas firmadas): `https://pr-ciudadanos-prd-actas-bucket.s3.amazonaws.com/`
- Bucket de disaster recovery: `https://pr-ciudadanos-prd-dr-actas-bucket.s3.amazonaws.com/`
- Assets estáticos: `https://publiconpepage.s3.amazonaws.com/`
- CDN: CloudFront (POP LIM50-P4). Los `.js` exigen cabecera `Referer: https://resultadoelectoral.onpe.gob.pe/...` o devuelven el `index.html` de fallback (anti-scraping light).

**Endpoints del API:** no pude enumerarlos sin ejecutar la SPA en un navegador real. El bundle principal (`main-*.js`, ~2.9 MB) los contiene, pero CloudFront activa protección tras el primer request. La forma fiable de obtenerlos es **abrir el sitio en el navegador, DevTools → Network → filtro XHR/Fetch**, y navegar las vistas (resumen / presidencial / senadores nacional / senadores regional / diputados / parlamento andino) tomando nota de cada endpoint.

**Granularidad esperada** (equivalente a 2021, cuya arquitectura siguen):

- Tipo de elección: presidencial, senado nacional, senado regional, diputados, parlamento andino
- Ámbito geográfico: Perú, Perú+Extranjero, Extranjero
- Jerarquía: nacional → departamento (26) → provincia (196) → distrito (1874) → local de votación → mesa de sufragio (~87k en total)
- Por mesa: votos por organización política, blancos, nulos, impugnados, electores hábiles, total de votantes
- Estado de cada mesa: contabilizada / no contabilizada / en proceso / impugnada

**Imágenes de actas:** descargables una a una vía URL firmada emitida por el backend. El bucket no permite listing (`ListBucket` denegado, 403). Se deben obtener los keys desde el API por mesa.

**Limitaciones:**

- No hay documentación oficial del API, todo es reverse engineering.
- CloudFront detecta patrones de scraping agresivo — conviene rate-limit (5–10 req/s máximo) y rotación de User-Agent.
- Los conteos son "en avance", no son resultados proclamados. Lo oficial final lo proclama el JNE.
- No hay websockets: el frontend poll-ea el backend (probablemente 15–30 s).

**Estrategia:** scraping del API REST una vez identificados los endpoints en DevTools. Guardar snapshots con timestamp para series temporales del avance del conteo (muy valioso analíticamente).

## 2. Plataforma Nacional de Datos Abiertos

**URL:** https://www.datosabiertos.gob.pe/group/oficina-nacional-de-procesos-electorales-onpe

**Hallazgo:** el portal NO es CKAN estándar. Es Drupal con catálogo propio y detrás de un WAF de Huawei Cloud (`HWWAFSESID` cookie, responde HTTP 418 a requests automatizados). Los endpoints `api/3/action/*` de CKAN devuelven 404.

**Qué hay:** históricamente, ONPE publica aquí los resultados definitivos por mesa tras la proclamación, en formato CSV/XLSX/ZIP. Para 2021 (referencia):

- "Resultados por mesa – Elecciones Presidenciales 2021 Segunda Vuelta"
- Archivos CSV con una fila por mesa × organización política, codificados con UBIGEO (6 dígitos: departamento 2 + provincia 2 + distrito 2)
- ZIP con imágenes de actas

**Estado para EG2026** (hoy 18-abr-2026): todavía no publicado. Típicamente aparece 2–4 semanas después de la proclamación oficial del JNE.

**Método de acceso:**

1. Scraping HTML del catálogo (respeta el WAF, pocas requests) para enumerar datasets.
2. Descarga directa de los archivos CSV/XLSX (no pasan por el WAF del portal, van a S3/almacenamiento directo).
3. Alternativa: monitor RSS/Atom de actualizaciones del grupo ONPE.

**Ventajas:** datos canónicos, limpios, sin necesidad de reverse engineering. Es el formato ideal para análisis histórico.
**Desventajas:** llega tarde (post proclamación) y es un snapshot único, no series temporales.

## 3. Sitio informativo EG2026 ONPE

**URL:** https://eg2026.onpe.gob.pe/
**Qué es:** sitio estático con info para electores y miembros de mesa. Tiene consulta de local de votación por DNI. No expone datos de resultados.
**Utilidad para el proyecto:** **baja**, pero sirve para validar padrón por DNI si hiciera falta cruzar una mesa concreta.

## 4. JNE — Jurado Nacional de Elecciones (complemento obligatorio)

ONPE cuenta los votos. JNE **proclama** los resultados y concentra la info de candidatos, organizaciones políticas e inscripción de listas. Para un dataset completo se necesitan ambos.

- **Plataforma Electoral:** https://plataformaelectoral.jne.gob.pe/
  Candidatos, listas por región, hojas de vida (PDF), organizaciones políticas inscritas. Scraping HTML.
- **INFOgob:** https://infogob.jne.gob.pe/
  Base histórica de resultados y autoridades desde 1931. Tiene descarga previo registro por correo. Es la fuente para series históricas comparables.
- **DECLARA:** https://declara.jne.gob.pe/
  Declaraciones juradas patrimoniales de candidatos. Útil si se quiere añadir análisis patrimonial.

## 5. RENIEC — Padrón electoral

**URL:** https://sisbi.reniec.gob.pe/PLPI

Total del padrón 2026: ~27.36 M electores. Permite consultas individuales por DNI y descargas de listas agregadas. No expone API. Para cruzar participación (votos emitidos / padrón hábil) por distrito es la única fuente.

## 6. Archivo histórico ONPE

**URL:** https://resultadoshistorico.onpe.gob.pe/
Presentaciones equivalentes al portal vivo, pero de elecciones pasadas (EG2021, ERM2022, etc.). Misma estructura técnica, datos ya consolidados. Útil si se quiere comparar contra un histórico reciente sin esperar a datosabiertos.gob.pe.

## 7. Scrapers previos (GitHub) — referencia de arquitectura

Aunque los dominios de 2021 (`api.resultados.eleccionesgenerales2021.pe`, etc.) ya no responden en 2026, el código muestra **patrones de paginación, identificadores de mesa y shape del JSON** que ONPE reutiliza año tras año.

- **joseluisq/peru-elecciones-generales-2021-scraper** — scraper JSON, recomendado de lectura.
- **pablodz/datos-abiertos-onpe-2021** — dos modos (API + web), muy bien documentado. Tiene los paths de mesa individual.
- **jmcastagnetto/2021-elecciones-generales-peru-datos-de-onpe** — datos ya extraídos en CSV/parquet.
- **yachaycode/api-resultados-onpe-2021** — 86.484 actas extraídas en segunda vuelta.
- **tabo/elecciones_peru_2021** — export a SQLite.

**No reutilizables directo**, pero ahorran 60–70% del trabajo de ingeniería de la ingesta.

## Dimensiones que se pueden categorizar con estas fuentes

Combinando ONPE + JNE + RENIEC + INEI:

- **Geográfica:** país, departamento, provincia, distrito, local de votación, mesa, urbano/rural (vía UBIGEO + INEI), zonas altoandinas / amazonia / costa.
- **Elección:** presidencial, senado nacional, senado regional, diputados (distrito electoral múltiple), parlamento andino.
- **Voto:** válido por organización política, blanco, nulo, impugnado, no emitido.
- **Candidato:** nombre, organización política, sexo, edad, ocupación, antecedentes (desde JNE), patrimonio (desde DECLARA).
- **Acta/mesa:** estado (contabilizada / observada / impugnada), local, miembros de mesa, incidencias.
- **Elector:** sexo y rango etario agregados por mesa (a partir del padrón RENIEC), no a nivel individual.
- **Temporal:** snapshots del avance del conteo (si se hace polling del API durante el escrutinio).

## Recomendación — plan en 3 fases

**Fase 1 — rápida (esta semana):**

1. Abrir `resultadoelectoral.onpe.gob.pe` en el navegador con DevTools abiertos. Navegar resumen, presidenciales, cada cámara y cada región. Documentar cada endpoint XHR (método, URL, params, respuesta). Esto es **bloqueante** para todo lo demás y son 60–90 minutos.
2. En paralelo: scrapear candidatos desde `plataformaelectoral.jne.gob.pe` (no depende del avance del conteo, datos ya finalizados).

**Fase 2 — ingesta masiva (2–3 semanas):**

3. Construir el scraper del API de ONPE con polling configurable (sugiero cada 10–15 min durante escrutinio activo, 1 h después). Guardar cada snapshot completo con timestamp en Parquet particionado por fecha. Esto da series temporales del avance que nadie más tendrá.
4. Descarga de imágenes de actas por mesa (URL firmada → archivo). Paralelizar respetando rate limit.

**Fase 3 — dataset canónico (1–2 meses):**

5. Cuando datosabiertos.gob.pe publique los CSV oficiales de EG2026, ingerirlos como **verdad de referencia** y hacer reconciliación contra los datos scrapeados del API para detectar divergencias.
6. Cruzar con RENIEC (padrón por distrito) e INFOgob (histórico) para enriquecimiento analítico.

## Notas operativas

- Almacenamiento: una mesa × 15 organizaciones × 5 elecciones × 87k mesas ≈ 65 M filas para el snapshot final. En Parquet con compresión zstd son ~300–500 MB. Con series temporales del avance (cada 15 min × 1 semana) se va a ~5–10 GB. Para tu Mac M2 con 24 GB, trabajable con DuckDB sin problema.
- No hay que pegarle al portal más fuerte de lo necesario: el CloudFront de ONPE ha tenido caídas históricas bajo carga (2021), y scrapers abusivos son parte del problema. Con 5 req/s distribuidas está de sobra.
- Todo es público y está amparado por Ley 27806 (Acceso a la información pública). Scrapear resultados oficiales no viola términos — pero sí conviene identificarse con un User-Agent honesto tipo `Elecciones2026-Research/1.0 (contacto@tuemail)`.

## URLs validadas en esta investigación

- https://resultadoelectoral.onpe.gob.pe/ (SPA, confirmada arquitectura S3+CloudFront)
- https://eg2026.onpe.gob.pe/ (sitio informativo activo)
- https://pr-ciudadanos-prd-actas-bucket.s3.amazonaws.com/ (existe, 403 listing)
- https://pr-ciudadanos-prd-dr-actas-bucket.s3.amazonaws.com/ (existe, DR)
- https://publiconpepage.s3.amazonaws.com/ (existe)
- https://www.datosabiertos.gob.pe/group/oficina-nacional-de-procesos-electorales-onpe (alcanzable, WAF para bots)
- https://plataformaelectoral.jne.gob.pe/ (JNE plataforma electoral)
- https://infogob.jne.gob.pe/ (JNE histórico)
- https://declara.jne.gob.pe/ (declaraciones juradas)
- https://resultadoshistorico.onpe.gob.pe/ (archivo histórico ONPE)
- https://sisbi.reniec.gob.pe/PLPI (padrón electoral RENIEC)

## Descartadas / no disponibles

- `api.resultados.eleccionesgenerales2026.pe`, `api.eleccionesgenerales2026.pe`: **no resuelven DNS**. El patrón 2021 (subdominio API dedicado) NO se reutilizó.
- `api.onpe.gob.pe`: **no resuelve**. No hay API institucional pública.
- Endpoints CKAN (`api/3/action/*`) en datosabiertos.gob.pe: 404, el portal no es CKAN.

## Apéndice A — Mapa del API ONPE 2026 (validado en vivo, 18-abr-2026)

Tras la inspección en DevTools y pruebas con `curl` replicando los headers del navegador, se confirmó la estructura completa del API. Toda la data cuelga de un único base path, **sin subdominio dedicado** (contrario a 2021).

**Base URL:** `https://resultadoelectoral.onpe.gob.pe/presentacion-backend/`

**Headers requeridos** (CloudFront rechaza requests que no parezcan originados del SPA):

```
accept: */*
content-type: application/json
referer: https://resultadoelectoral.onpe.gob.pe/main/resumen
sec-ch-ua: "Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "macOS"
sec-fetch-dest: empty
sec-fetch-mode: cors
sec-fetch-site: same-origin
user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36
```

**Endpoints confirmados:**

| Propósito | Path | Query |
|---|---|---|
| Proceso activo | `/proceso/proceso-electoral-activo` | — |
| Catálogo de elecciones | `/proceso/elecciones` | — |
| Distritos electorales (27) | `/distrito-electoral/distritos` | — |
| Totales agregados | `/resumen-general/totales` | `idEleccion=<id>&tipoFiltro=eleccion` |
| Totales por distrito electoral | `/resumen-general/totales` | `idAmbitoGeografico=1&idEleccion=<id>&tipoFiltro=distrito_electoral&idDistritoElectoral=<1-27>` |
| Participantes (candidatos/agrupaciones) | `/resumen-general/participantes` | mismos filtros que totales |

**Matriz de combinaciones por tipo de elección:**

| `idEleccion` | Nombre | `tipoFiltro` correcto | Nota |
|---|---|---|---|
| 10 | Presidencial | `eleccion` | Distrito único nacional |
| 12 | Parlamento Andino | `eleccion` | Distrito único nacional |
| 13 | Diputados | `distrito_electoral` | Por cada uno de los 27 distritos |
| 14 | Senadores (regional) | `distrito_electoral` | Por cada uno de los 27 distritos |
| 15 | Senadores (nacional) | `eleccion` | Distrito único nacional |

**Catálogo de distritos electorales** (extraído de `/distrito-electoral/distritos`):

- codigo 1-26: un distrito por departamento (AMAZONAS...UCAYALI), con codigo 15 = LIMA METROPOLITANA (separada del resto de Lima)
- codigo 27: PERUANOS RESIDENTES EN EL EXTRANJERO

**Shape de `/resumen-general/totales`** (ejemplo real, Presidencial nacional, 18-abr-2026):

```json
{
  "actasContabilizadas": 93.414,
  "contabilizadas": 86656,
  "totalActas": 92766,
  "participacionCiudadana": 69.162,
  "actasEnviadasJee": 6.147,
  "enviadasJee": 5702,
  "actasPendientesJee": 0.439,
  "pendientesJee": 408,
  "fechaActualizacion": 1776488709960,
  "totalVotosEmitidos": 18898830,
  "totalVotosValidos": 15754714,
  "porcentajeVotosEmitidos": 100,
  "porcentajeVotosValidos": 100,
  "idUbigeoDepartamento": 140000,
  "idUbigeoProvincia": 140100,
  "idUbigeoDistrito": 140101,
  "idUbigeoDistritoElectoral": 15
}
```

`fechaActualizacion` es epoch en **milisegundos**. Hacer `pd.to_datetime(ts, unit='ms', utc=True).tz_convert('America/Lima')`.

**Shape de `/resumen-general/participantes`** (array; 36 entradas en Presidencial):

```json
{
  "nombreAgrupacionPolitica": "...",
  "codigoAgrupacionPolitica": "...",
  "nombreCandidato": "...",
  "dniCandidato": "...",
  "totalVotosValidos": 0,
  "porcentajeVotosValidos": 0.0,
  "porcentajeVotosEmitidos": 0.0
}
```

**Confirmación de tracking anti-bot:** CloudFront devuelve el `index.html` de fallback si falta `referer` correcto o los `sec-fetch-*`. No hay rate-limit visible en requests moderadas (5-10/s), pero conviene mantenerse bajo ese umbral.

## Apéndice B — Drill-down jerárquico (validado en vivo desde `/main/actas`)

Capturados y probados los endpoints que alimentan el explorador de actas. Todo cuelga del mismo `presentacion-backend/` y usa los mismos headers del Apéndice A, cambiando el `referer` a `https://resultadoelectoral.onpe.gob.pe/main/actas`.

### B.1 — Jerarquía geográfica

| Propósito | Path | Query | Respuesta |
|---|---|---|---|
| Lista de departamentos | `/ubigeos/departamentos` | `idEleccion=<id>&idAmbitoGeografico=1` | 25 ítems `{ubigeo: "010000".."260000", nombre}` |
| Provincias de un depto | `/ubigeos/provincias` | `...&idUbigeoDepartamento=<ubigeo6>` | `{ubigeo, nombre}`, ~8 por depto |
| Distritos de una provincia | `/ubigeos/distritos` | `...&idUbigeoProvincia=<ubigeo6>` | `{ubigeo, nombre}`, 29 en Arequipa-Arequipa p. ej. |
| Locales de votación de un distrito | `/ubigeos/locales` | `idUbigeo=<ubigeo6>` | `{codigoLocalVotacion, nombreLocalVotacion}` |

**Formato UBIGEO:** 6 dígitos con leading zeros: `040102` = AREQUIPA dpto(04) + AREQUIPA prov(01) + CAYMA dist(02). Los endpoints `/ubigeos/*` lo aceptan con los 6 dígitos. El endpoint `/actas` lo acepta como integer (`idUbigeo=40102`, 5 dígitos sin leading zero) O como string `040102`; ambas formas funcionan.

### B.2 — Mapa de calor (avance del conteo por ubigeo)

Endpoint: `/resumen-general/mapa-calor`

| tipoFiltro | Query | Qué devuelve |
|---|---|---|
| `ambito_geografico` | `idAmbitoGeografico=1&idEleccion=<id>` | Una fila por **departamento** (26) |
| `ubigeo_nivel_01` | `...&ubigeoNivel01=<dpto6>` | Una fila por **provincia** del depto |
| `ubigeo_nivel_02` | `...&ubigeoNivel01=<d6>&ubigeoNivel02=<p6>` | Una fila por **distrito** de la provincia |

Shape de cada fila:
```json
{
  "ambitoGeografico": 1,
  "ubigeoNivel01": 40000,
  "ubigeoNivel02": 40100,
  "ubigeoNivel03": null,
  "distritoElectoral": null,
  "porcentajeActasContabilizadas": 98.297,
  "actasContabilizadas": 3175
}
```

No trae votos, solo avance de conteo. Para votos hay que pegarle a `/resumen-general/totales` o bajar a nivel mesa.

### B.3 — Lista paginada de actas por local de votación

Endpoint: `/actas`

**Query requerida** (todos obligatorios para que devuelva data — si falta alguno, devuelve `contenido: []`):

```
pagina=<int>        # zero-indexed
tamanio=<int>       # sugerido 100-500 para bulk; el portal usa 15
idAmbitoGeografico=1
idEleccion=<10|12|13|14|15>
idUbigeo=<ubigeo distrito, p. ej. 040102>
idLocalVotacion=<codigoLocalVotacion>
```

**Respuesta** (ejemplo real, CAYMA - IE 40046, Presidencial):
```json
{
  "paginaActual": 0,
  "totalRegistros": 1375,
  "totalPaginas": 92,
  "content": [
    {
      "id": 550704010210,
      "idMesa": 5507,
      "codigoMesa": "005507",
      "numeroCopia": "02",
      "idEleccion": 10,
      "idAmbitoGeografico": 1,
      "idUbigeo": 40102,
      ...
    }
  ]
}
```

**Ojo:** `totalRegistros=1375` para un solo local parece alto — probablemente el listado es a nivel **distrito** (todas las mesas de CAYMA) cuando se pasan los filtros completos, no solo del local. `idLocalVotacion` parece actuar como "gatillo" del filtro más que filtro estricto. Para uso masivo, basta con iterar por (distrito, eleccion).

### B.4 — Acta individual (el dato de oro)

Endpoint: `/actas/{idActa}`

**Fórmula del `idActa`** (deducida y confirmada experimentalmente):

```
idActa = idMesa(4 digitos) ++ idUbigeoDistrito(6) ++ idEleccion(2)
```

Ejemplos confirmados:
- `550704010210` = mesa 5507 + ubigeo 040102 (CAYMA) + elección 10 (Presidencial) ✓
- `550704010215` = misma mesa 5507, mismo distrito, **elección 15 (Senado nacional)** → devuelve acta con 41 detalle[] ✓

Si la mesa no existe (p. ej. `550004010210`), el endpoint devuelve 200 con todos los campos `null`. **Para validar existencia, chequear `codigoMesa != null`.**

**Respuesta** — cabecera del acta:
```json
{
  "id": 550704010210,
  "idMesa": null,
  "codigoMesa": "005507",
  "descripcionMesa": "005507",
  "idEleccion": 10,
  "ubigeoNivel01": "AREQUIPA",       // string, nombre (NO ubigeo)
  "ubigeoNivel02": "AREQUIPA",
  "ubigeoNivel03": "CAYMA",
  "centroPoblado": "",
  "nombreLocalVotacion": "IEP NIÑO MAGISTRAL SECUNDARIA",
  "totalElectoresHabiles": 274,
  "totalVotosEmitidos": 251,
  "totalVotosValidos": 246,
  "totalAsistentes": 251,
  "porcentajeParticipacionCiudadana": 91.606,
  "estadoActa": "D",                  // tentativo: D = Digitalizada
  "estadoComputo": "N",
  "codigoEstadoActa": "C",            // C = Contabilizada
  "descripcionEstadoActa": "Contabilizada",
  "estadoActaResolucion": "",
  "estadoDescripcionActaResolucion": null,
  "descripcionSubEstadoActa": null,
  "detalle": [ ... ]
}
```

**`detalle[]`** — 41 ítems en Presidencial (36 agrupaciones + blancos + nulos + impugnados + otras filas auxiliares):
```json
{
  "descripcion": "RENOVACIÓN POPULAR",       // también aparecen "VOTOS EN BLANCO", "VOTOS NULOS", "VOTOS IMPUGNADOS"
  "estado": 1,
  "grafico": 1,
  "cargo": null,
  "sexo": null,
  "totalCandidatos": 0,
  "candidato": [                              // lista; null para blancos/nulos/impugnados
    {
      "apellidoPaterno": "LÓPEZ ALIAGA",
      "apellidoMaterno": "CAZORLA",
      "nombres": "RAFAEL BERNARDO",
      "cdocumentoIdentidad": "07845838"
    }
  ],
  "ccodigo": "00000035",
  "nposicion": 33,
  "nvotos": 102,
  "nagrupacionPolitica": 35,
  "nporcentajeVotosValidos": 41.463,
  "nporcentajeVotosEmitidos": 40.637
}
```

**Así se obtienen blancos / nulos / impugnados:** vienen embebidos como entradas del mismo `detalle[]`, con `descripcion` in `{"VOTOS EN BLANCO", "VOTOS NULOS", "VOTOS IMPUGNADOS"}` y sin `candidato`. Listo.

**El idActa sigue siendo estable entre elecciones** cambiando solo los últimos 2 dígitos → para una misma mesa física se consulta 5 elecciones con solo cambiar el sufijo.

### B.5 — Agregados auxiliares

| Endpoint | Query | Qué devuelve |
|---|---|---|
| `/mesa/totales` | `tipoFiltro=eleccion` | `{mesasInstaladas: 92489, mesasNoInstaladas: 0, mesasPendientes: 277}` (nacional) |
| `/fecha/listarFecha` | — | `{fechaProceso: <epoch ms>, servicioFirma: true, cdescripcion: "Ultima Fecha Migración"}` — última sincronización del backend |
| `/resumen-general/elecciones` | `activo=1&idProceso=2&tipoFiltro=eleccion` | Array con una fila **por tipo de elección** (5 filas): `{id, nombre, actasContabilizadas, porcentajeActasContabilizadas, actasObservadasEnviadas, actasPendientes, ...}` — ideal para el dashboard de avance comparado |
| `/proceso/2/elecciones` | — | Menú de navegación del SPA: `{id, nombre, url, idEleccion, esPrincipal, icono, orden, padre, hijos}`. Útil para decodificar qué `idEleccion` corresponde a qué ruta del frontend. |

### B.6 — Assets estáticos útiles (GeoJSON)

| URL | Contenido |
|---|---|
| `/assets/lib/amcharts5/geodata/json/peruLow.json` | Mapa nacional de Perú (amCharts) |
| `/assets/lib/amcharts5/geodata/json/departamentos/{ubigeo6}.json` | Mapa de un departamento (provincias) |
| `/assets/lib/amcharts5/geodata/json/provincias/{ubigeo6}.json` | Mapa de una provincia (distritos) |

Útiles si queremos renderizar mapas sin buscar los shapefiles de INEI aparte.

### B.7 — Mapa API completo (resumen)

```
/presentacion-backend/
├── proceso/
│   ├── proceso-electoral-activo
│   ├── elecciones                        # catálogo global
│   └── {idProceso}/elecciones            # menú del SPA
├── distrito-electoral/
│   └── distritos                         # 27 distritos electorales
├── ubigeos/
│   ├── departamentos?idEleccion&idAmbitoGeografico
│   ├── provincias?...&idUbigeoDepartamento
│   ├── distritos?...&idUbigeoProvincia
│   └── locales?idUbigeo
├── resumen-general/
│   ├── totales?idEleccion&tipoFiltro=eleccion|distrito_electoral|...
│   ├── participantes?mismos filtros
│   ├── elecciones?activo=1&idProceso=2
│   └── mapa-calor?idAmbitoGeografico&idEleccion&tipoFiltro=ambito_geografico|ubigeo_nivel_01|ubigeo_nivel_02[&ubigeoNivel01[&ubigeoNivel02]]
├── mesa/
│   └── totales?tipoFiltro=eleccion
├── actas?pagina&tamanio&idAmbitoGeografico&idEleccion&idUbigeo&idLocalVotacion
├── actas/{idActa}                        # idActa = idMesa(4) + ubigeoDistrito(6) + idEleccion(2)
└── fecha/listarFecha
```

### B.8 — Implicaciones para la ingesta

- **Ya no se necesita DevTools para cerrar el scraper**; todos los shapes están validados.
- **Cobertura total**: 5 elecciones × 92,766 mesas = **~463,830 llamadas a `/actas/{id}`** para un snapshot completo de mesa-nivel. A 10 req/s son ~13 horas. A 5 req/s son ~26 horas. Para snapshots frecuentes (cada 15-30 min durante escrutinio) conviene hacer solo mesas con cambios → usar `/resumen-general/mapa-calor` primero para detectar distritos con nuevas actas y bajar solo esas.
- **Iteración jerárquica recomendada** (un crawl completo, no incremental):
  1. `/ubigeos/departamentos` → 25 deptos
  2. por depto → `/ubigeos/provincias` → ~196 provincias
  3. por provincia → `/ubigeos/distritos` → ~1874 distritos
  4. por distrito → `/ubigeos/locales` → ~87k locales
  5. por (distrito, eleccion) → `/actas?...` paginado → lista de IDs de acta
  6. por idActa → `/actas/{id}` → voto detallado
- **Alternativa más eficiente**: saltarse `/actas?...` (listado) y construir directamente los idActa por iteración cartesiana `mesa × distrito × eleccion`. Pero requiere saber los `idMesa` válidos por distrito, que es más seguro obtenerlos del listado.
- **El listado paginado `/actas` requiere los 6 params**: si falta cualquiera (`idEleccion`, `idAmbitoGeografico`, `idUbigeo`, `idLocalVotacion`) devuelve `content: []`. No es bug, es diseño del endpoint.
- **Imágenes de las actas (PNG/PDF firmado):** no encontramos el endpoint que devuelve la URL firmada del bucket en las requests capturadas. Probablemente esté bajo otra ruta dentro de `/main/actas` (botón "Ver acta imagen"). Es la única pieza que falta. No bloquea la ingesta de datos.
