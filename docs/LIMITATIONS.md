# Limitations — onpe-eg2026 v1.0

Documento explícito de **gaps conocidos, sesgos y restricciones** del dataset v1.0. Honestidad metodológica: este dataset NO es la fuente oficial canónica (esa es ONPE post-proclamación JNE vía datosabiertos.gob.pe); es un scrape reverse-engineered del portal web de resultados ONPE.

## 1. Estado temporal: pre-proclamación

**Crítico**: el dataset v1.0 representa el **estado del conteo en curso** al 19 de abril de 2026, **antes** de la proclamación oficial del JNE (Jurado Nacional de Elecciones).

Implicaciones:
- **Actas en estado E** (13.1% del universo): en proceso de revisión por JEE (Jurado Electoral Especial). Sus votos pueden cambiar post-JEE (actas rechazadas, votos reasignados, etc.).
- **Actas en estado P** (2.7%): pendientes de escrutinio. Muchas tendrán detalle poblado post-proclamación.
- **Drift contra oficial JNE**: esperamos ≤1 voto de delta por (partido × distrito × elección) una vez ONPE publique en datosabiertos.gob.pe. Hasta ese momento, considerar este dataset como **snapshot intermedio fiel al estado del conteo ONPE**, NO como resultado oficial definitivo.

**Recomendación**: para análisis posteriores a la proclamación (~4 semanas), usar el dataset v1.1 (con reconciliación contra datosabiertos) cuando esté disponible.

## 2. Voto preferencial NO capturado

**Gap**: el API ONPE no expone endpoint para voto preferencial por candidato. Solo están disponibles los votos agregados por agrupación política (partido).

Para Senado (idEleccion 14, 15) y Diputados (idEleccion 13), donde cada lista tiene hasta 29 candidatos, el **ranking individual de candidatos por mesa queda invisible**.

**Alternativas para obtenerlo**:
1. **OCR de PDFs escaneados** de escrutinio (tipo 1): posible pero costoso (~725k PDFs × OCR). Research spike de >1 semana, accuracy incierta.
2. **Esperar datosabiertos.gob.pe**: ONPE podría publicar voto preferencial en el CSV oficial post-proclamación. Probable pero no garantizado.
3. **JNE Plataforma Electoral (roadmap v1.1)**: candidatos registrados oficialmente se podrán joinear pero los votos individuales siguen sin capturar.

**Marcadores del gap en el dataset**: la tabla `actas_votos` tiene columnas `cand_apellido_paterno` etc. que capturan **solo el primer candidato** de la lista. La tabla `actas_candidatos` expande el array completo de candidatos pero solo sus nombres/DNI — **no votos por candidato individual**.

## 3. Padrón RENIEC no integrado

**Gap**: el cálculo de participación usa `totalElectoresHabiles` reportado por ONPE en cada acta. Esto es un número consistente mesa-por-mesa pero **no permite validar contra el padrón oficial RENIEC** ni desglosar por demografía (sexo, rango etario).

**Implicaciones**:
- No podemos calcular participación exacta por distrito = `votos_emitidos_distrito / padrón_hábil_distrito_RENIEC`.
- No podemos segmentar voto por demografía.

**Roadmap v1.1**: scraper RENIEC (`data/dim/padron.parquet`) + tabla cross-walk ubigeos ONPE ↔ INEI/RENIEC.

## 4. Ubigeos ONPE ≠ INEI/RENIEC (mapping deferred)

ONPE usa su propio esquema de ubigeos (ej. Lima = depto 14), distinto del INEI/RENIEC (Lima = depto 15). Para joins con cualquier dataset externo (censos INEI, padrón RENIEC, histórico pre-2020), hace falta tabla de mapeo.

**Estado**: la lógica para derivar `idDistritoElectoral` desde ubigeo ONPE está implementada (`src/onpe/enrich.py::compute_distrito_electoral`). Pero la tabla bidirectional ONPE ↔ INEI a nivel distrito NO existe en el repo.

## 5. Anomalía 240 actas C sin detalle

240 actas con `codigoEstadoActa=C` + `estadoActa=N` + `detalle=[]`. Análisis exhaustivo (ver `scripts/investigate_anomaly_240.py` + `data/curated/actas_anomalia_240_investigacion.parquet`): **100% clasificadas como mesa no instalada** (48 mesas del voto exterior × 5 elecciones que nunca se instalaron físicamente).

**Excluidas** del denominador en el check de identidades contables (inner join con votos las descarta automáticamente). NO contaminan el dataset pero hay que mencionar su existencia para transparency.

## 6. PDFs binarios: opcionales, no parte del dataset curated

Los 811,984 PDFs escaneados están en un bucket GCS (`gs://onpe-eg2026-pdfs-v2/eg2026/`). **NO se publican** como parte del dataset v1.0 porque:
- Volumen de ~1 TB excede límites de Kaggle y es pesado para Zenodo.
- La metadata (`archivoId`, tipo, fechas) SÍ está en `actas_archivos.parquet` para que consumers los descarguen bajo demanda via `scripts/download_pdfs.py`.

**Alternativas para acceder**:
- Correr el pipeline + downloader con credenciales GCP propias.
- Si se hace público un mirror (Archive.org, IPFS, etc.) en el futuro, referenciar aquí.

## 7. Serie temporal de aggregates: irrecuperable

Los snapshots de aggregates cada 15 min durante el conteo capturan **data irrecuperable**: ONPE no tiene API histórico de estos snapshots. Si el loop se caía, esos minutos se perdían.

**Estado en v1.0**: ~1000 snapshots capturados en los días 18-20 de abril 2026. Disponible en `data/facts/totales/`, `data/facts/totales_de/`, `data/facts/participantes/`, `data/facts/mapa_calor/`.

**Limitación**: la cobertura temporal depende de cuándo se inició el loop. Los primeros minutos del conteo (12-13 abril noche) NO están en el dataset porque el pipeline aún no corría.

## 8. Fuentes oficiales adicionales: pendientes

El v1.0 captura solo ONPE (fuente primaria). **Pendientes** para v1.1+:

| Fuente | Estado | Prioridad |
|---|---|---|
| JNE Plataforma Electoral (candidatos registrados) | scraper en roadmap | Alta |
| RENIEC Padrón 2026 | scraper en roadmap | Alta |
| El Peruano (resoluciones JNE) | scraper en roadmap | Media |
| ONPE POE (Plan Operativo Electoral) | scraper en roadmap | Baja |
| JNE INFOgob (histórico) | excluido (requiere registro) | N/A |
| JEE resoluciones por circunscripción | excluido (complejidad alta) | N/A |

## 9. Sesgos metodológicos

- **Sesgo temporal**: el dataset refleja el estado del conteo al 19-abr-2026. Analizar como "estado intermedio", no como "resultado definitivo".
- **Sesgo tecnológico**: las actas con `codigoSolucionTecnologica` OCR pueden tener mayor tasa de errores de reconocimiento que las procesadas manualmente. Sin indicador directo de "calidad por acta".
- **Sesgo geográfico**: el voto exterior (2,543 mesas en 210 ciudades) tiene su propia dinámica (infraestructura, tasa de instalación). El DE 27 puede no ser directamente comparable con los 26 deptos peruanos.
- **Sesgo del scraping**: reverse-engineered del portal web, no del backend oficial. Si ONPE cambia el schema del JSON sin aviso, el pipeline detecta el drift (fail-fast) pero puede perderse un window de captura.

## 10. Alcance geográfico del voto exterior

Los 2,543 locales del ámbito 2 (exterior) no están GeoJSON-mapeados (no existen polígonos oficiales ONPE para consulados/embajadas). El choropleth los muestra solo como agregado DE=27, no con granularidad ciudad-por-ciudad.

## 11. Cobertura aggregates nacional/DE pero NO depto/prov

El loop de aggregates captura `totales`, `totales_de`, `participantes`, `participantes_de`, `mapa_calor` nacional. **No capturamos** `mapa_calor_departamento` ni `mapa_calor_provincia` (ONPE los expone pero sería 25x más llamadas; el mismo detalle está en `actas_cabecera` agrupable post-hoc).

## 12. Licencia y uso

- **Código**: MIT License (`LICENSE`).
- **Datos**: origen público ONPE. Para uso académico/periodístico proponemos **CC-BY 4.0** (atribución requerida) — se define en el release v1.0 del dataset.
- **PDFs escaneados**: propiedad de ONPE. Republicar requiere evaluación legal (son documentos oficiales con información personal de miembros de mesa).

## 13. Manténgase actualizado

Los gaps de esta lista se irán cerrando en versiones subsecuentes:
- `v1.1` (~semana 20-25 post-proclamación): JNE candidatos + RENIEC padrón + reconciliación datosabiertos.
- `v1.2` (futuro): OCR spike voto preferencial si se prueba viable.
- `v2.0` (si aplica): segunda vuelta presidencial.

Ver `CHANGELOG.md` + roadmap de issues en GitHub.

---

**Contacto para reportar limitaciones adicionales**: [Issue tracker](https://github.com/f3r21/onpe-eg2026/issues).
