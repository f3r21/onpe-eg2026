# onpe-eg2026

Ingesta de resultados de las Elecciones Generales Perú 2026 desde el API reverse-engineered de ONPE (`resultadoelectoral.onpe.gob.pe/presentacion-backend/`). Produce un data lake en Parquet con particiones Hive, listo para consumir con Polars, DuckDB o pandas.

Cubre las 5 elecciones simultáneas:

| idEleccion | Elección                                    |
|-----------:|---------------------------------------------|
| 10         | Presidencial                                |
| 12         | Parlamento Andino                           |
| 13         | Diputados                                   |
| 14         | Senadores — distrito electoral múltiple     |
| 15         | Senadores — distrito electoral único        |

Ver `fuentes_datos.md` para el mapa completo de endpoints, la fórmula determinística del `idActa`, decisiones arquitectónicas y peculiaridades del API.

## Setup

```bash
brew install uv
uv sync
```

Requiere Python 3.12+.

## Pipeline

El flujo tiene tres etapas independientes. Cada una produce datasets versionados por `snapshot_date` y `run_ts_ms`.

**1. Dimensiones geográficas** — pobla `data/dim/` con distritos, locales y departamentos.

```bash
uv run python scripts/crawl_dims.py
```

**2. Inventario de mesas** — descubre las ~92k mesas físicas iterando el listado `/actas` por distrito. Produce `data/dim/mesas.parquet`.

```bash
uv run python scripts/crawl_mesas.py
```

Coste: ~1900 requests a 5 rps → ~7 min.

**3a. Agregados por snapshot** — descarga los totales, mapa de calor y resúmenes pre-computados por ONPE. Barato, ideal para correr en loop cada X minutos durante el conteo.

```bash
uv run python scripts/snapshot_aggregates.py
```

**3b. Snapshot de actas individuales** — itera las ~451k actas (90k mesas × 5 elecciones) golpeando `/actas/{idActa}`. Checkpoint resumable, chunked Parquet writes.

```bash
# Primera corrida (run completo)
uv run python scripts/snapshot_actas.py --rps 15 --concurrency 15

# Smoke test (50 actas)
uv run python scripts/snapshot_actas.py --limit 50

# Reanudar después de una caída
uv run python scripts/snapshot_actas.py --resume <run_ts_ms>
```

Coste: ~8h a 15 req/s. El checkpoint se guarda por batch (cada 500 actas) en `data/state/actas_run_<run_ts_ms>.json`, así que cualquier caída cuesta como mucho 500 actas de re-fetch.

Para runs nocturnos bulletproof:

```bash
nohup caffeinate -dims uv run python scripts/snapshot_actas.py \
  --rps 15 --concurrency 15 > snapshot.log 2>&1 &!
```

`caffeinate -dims` previene sleep; `&!` en zsh backgroundea + disown en un paso.

## Smoke test

Valida todos los endpoints principales y la fórmula del `idActa`:

```bash
uv run python scripts/smoke.py
```

Guarda muestras de respuesta en `data/smoke/`.

## Layout de datos

```
data/
├── dim/                          # dimensiones (estables entre elecciones)
│   ├── distritos.parquet
│   ├── locales.parquet
│   └── mesas.parquet
├── facts/                        # hechos con snapshot versionado
│   ├── totales/
│   │   └── snapshot_date=2026-04-18/<run_ts_ms>.parquet
│   ├── mapa_calor/
│   ├── resumen_elecciones/
│   ├── actas_cabecera/           # una fila por idActa
│   │   └── snapshot_date=2026-04-18/<run_ts_ms>-<chunk_idx>.parquet
│   └── actas_votos/              # una fila por (idActa × opción)
├── state/                        # checkpoints JSON para runs resumables
└── smoke/                        # muestras del smoke test
```

## Estructura del código

```
src/onpe/
├── client.py       # HTTP client async (httpx + HTTP/2 + tenacity + rate limit)
├── endpoints.py    # wrappers tipados de cada endpoint ONPE
├── storage.py      # helpers Parquet + time helpers (Lima tz)
├── crawler.py      # crawl jerárquico de dimensiones geográficas
├── mesas.py        # inventario de mesas vía /actas listado
├── aggregates.py   # snapshot de endpoints agregados
└── actas.py        # snapshot resumable de /actas/{idActa}

scripts/
├── crawl_dims.py            # Fase 1
├── crawl_mesas.py           # Fase 2
├── snapshot_aggregates.py   # Fase 3a (barato, repetible)
├── snapshot_actas.py        # Fase 3b (caro, resumable)
└── smoke.py                 # validación end-to-end
```

## Referencias

- `fuentes_datos.md` — documentación exhaustiva de fuentes, endpoints y decisiones.
- Portal oficial: https://resultadoelectoral.onpe.gob.pe/
- Datos abiertos ONPE: https://www.onpe.gob.pe/
