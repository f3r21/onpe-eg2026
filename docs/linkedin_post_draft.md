# LinkedIn post draft — ONPE EG2026 dataset

## Opción A — Técnico + data-driven (recomendado)

---

🇵🇪 En las últimas dos semanas construí un dataset completo de las **Elecciones Generales 2026** desde cero.

463,830 actas × 5 elecciones simultáneas (Presidencial, Parlamento Andino, Senadores nacional/regional, Diputados) → **18.6 millones de filas de votos** + series temporales + metadata de 725k PDFs escaneados.

Todo extraído directamente del API reverse-engineered de ONPE, validado contra 4 niveles de Data Quality, y documentado para uso open source.

**Algunos hallazgos del snapshot actual** 📊

🏆 Presidencial (top 5):
• Fuerza Popular: 17.06%
• Juntos Por El Perú: 12.01%
• Renovación Popular: 11.92%
• Partido del Buen Gobierno: 11.06%
• Partido Cívico Obras: 10.17%

🗳️ Participación nacional: **74.35%** en EG2026 vs 71.98% en EG2021 → **+2.37 puntos**.

🌎 Voto exterior (2,543 mesas en 210 ciudades, desde Buenos Aires hasta Seúl): Renovación Popular lidera Presidencial con 55k votos.

🏛️ Diputados: Fuerza Popular gana en 14 de los 27 distritos electorales; Juntos Por El Perú en 9; Renovación Popular domina Lima Metropolitana (806k votos) y voto exterior.

**Stack técnico** 🛠️

• Python 3.12 + uv + Polars streaming
• httpx async + HTTP/2 + tenacity para el scraping
• Parquet + Hive partitions (snapshot_date / run_ts_ms)
• 79 tests pytest con schema validation fail-fast
• GCS streaming para los 725k PDFs escaneados (~1 TB)
• Advisory file locks para coordinar jobs
• Dashboard HTML coroplético con Leaflet

**¿Por qué lo hice?** 🎯

1. Las elecciones son un bien público → los datos también deberían serlo
2. ONPE no publica API oficial — hay que reverse-engineerear cada elección
3. El scraper + dataset queda abierto para analistas, periodistas, investigadores

El código está en GitHub: **github.com/f3r21/onpe-eg2026** (MIT).

Próximos pasos:
• OCR de PDFs escaneados para extraer voto preferencial
• Cruce con datosabiertos.gob.pe cuando publique la versión oficial (~4 semanas post-proclamación)
• Predictor electoral baseline EG2026 vs EG2021

¿Preguntas o quieres construir algo sobre el dataset? 👇

#Perú #EG2026 #DataEngineering #OpenSource #Elecciones2026 #Python #Polars #ONPE #DataQuality

---

## Opción B — Corto, para engagement rápido

---

🇵🇪 Acabo de terminar de construir el dataset electoral EG2026 más completo en GitHub:

• **463,830 actas** × 5 elecciones
• **18.6M filas de votos** individuales
• Participación **74.35%** (+2.37 puntos vs 2021)
• 725k PDFs escaneados backed-up en GCS
• 4 niveles de Data Quality validados

Fuerza Popular lidera Presidencial con 17.06%. Renovación Popular domina Lima Metro + voto exterior.

Todo open source en **github.com/f3r21/onpe-eg2026** 🔓

Stack: Python + Polars + Parquet + Hive partitions. 79 tests. Schema validation fail-fast. Advisory locks.

Listo para que analistas, periodistas, investigadores lo usen.

#EG2026 #DataEngineering #OpenSource #Perú

---

## Opción C — Narrativa, focus en el problema

---

¿Sabías que ONPE publica los resultados electorales de Perú solo a través de una web app Angular que **no tiene API oficial**? 🤔

Para que cualquier analista, periodista o investigador pueda usar los datos, hay que hacer reverse engineering de la SPA cada elección. Así que esta vez lo hice — y lo abrí.

**onpe-eg2026** en GitHub: un pipeline completo que extrae los 463,830 actas de las Elecciones Generales 2026 con 18.6M filas de votos, los valida con 4 niveles de Data Quality, y los pone en Parquet listos para Polars/DuckDB.

Algunos hallazgos:

🏆 Presidencial: Fuerza Popular 17% / Juntos Por El Perú 12% / Renovación Popular 12%
🗳️ Participación nacional: 74.35% (+2.37 puntos vs 2021)
🌎 Voto exterior (2543 mesas): Renovación Popular gana Presidencial
🏛️ Diputados: Fuerza Popular gana 14/27 distritos electorales

Técnicamente:
• Python + Polars streaming
• httpx async con rate-limit 15 rps + jitter anti-detección
• Schema validation fail-fast contra type drift de ONPE
• Advisory file locks para coordinar jobs paralelos
• Dataset backed-up en GCS (~1 TB incluyendo PDFs escaneados)

El dataset es MIT. El código también. Que alguien lo use.

GitHub: **github.com/f3r21/onpe-eg2026**

#EG2026 #OpenData #DataEngineering #Perú #Elecciones

---

## Notas sobre timing y assets

**Mejor momento para postear**:
- Martes/miércoles 10-12am (horario Perú)
- Evitar viernes/lunes (bajo engagement LinkedIn)
- No postear el mismo día que proclamación JNE (ruido)

**Assets sugeridos a adjuntar**:
1. Screenshot del mapa coroplético (`data/dashboard/mapa_eg2026.html`)
2. Tabla top 5 partidos Presidencial
3. Logo/banner simple con "ONPE EG2026" + cifras clave
4. Gráfico participación 2021 vs 2026

**Para ajustar antes de postear**:
- Actualizar cifras de participación % con último snapshot
- Verificar ranking partidos (puede cambiar en próximas semanas)
- Incluir link al archivo Releases v1.0 de GitHub (crear release)
