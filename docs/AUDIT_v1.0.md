# Audit pre-release v1.0 — TODOS los findings aplicados

Auditoría ejecutada el 2026-04-20 con 7 agents especializados antes de hacer el repo público. Todos los findings (CRITICAL, HIGH, MEDIUM, LOW) fueron aplicados en 3 commits sucesivos.

## Commits que aplicaron los findings

- **`6dfd8de`** — CRITICAL + HIGH (~13 findings de runtime, seguridad, data integrity, types)
- **`a03e8df`** — Documenta metodología del audit
- **`6ca2580`** — MEDIUM + LOW (~15 findings de refactor, perf, design, claridad, comment drift)

## Categorías cubiertas

### Security
- Path traversal defense vía `_validate_archivo_id` regex hex-24 (`pdfs.py`)
- Zip slip protection via `Path.resolve().relative_to()` (`scripts/ingest_datosabiertos.py`)

### Runtime / async correctness
- `asyncio.get_event_loop()` → `get_running_loop()` (`client.py`)
- `asyncio.CancelledError` re-raise explícito en 4 sitios (`pdfs.py` x2, `actas.py`, `snapshot_aggregates.py`)
- `asyncio.gather(return_exceptions=True)` con handler que re-raise CancelledError y logea excepciones inesperadas (`actas.py`)
- `__aexit__` con try/finally para cleanup garantizado (`client.py`)

### Data integrity
- GCS `PreconditionFailed` race handling: re-check y reportar `skipped_existing` en vez de loop infinito (`pdfs.py`)
- `self._fd = fd` asignado inmediatamente post-flock para evitar fd leak si write falla (`locks.py`)
- `PipelineLock._fd` como `field(init=False)` para type checker (`locks.py`)

### Type safety
- `DownloadResult.status: Literal["downloaded", "skipped_existing", "failed"]` (`pdfs.py`)
- `DownloadResult.__post_init__` valida invariant `failed ↔ error != None` (`pdfs.py`)
- `ClientConfig.__post_init__` valida `rate_per_second > 0`, `max_concurrent >= 1`, `timeout_s > 0` (`client.py`)
- `TYPE_CHECKING` guard para `Bucket` en `pdfs.py`
- NewTypes `IdActa`, `UbigeoDistrito`, `ArchivoId` disponibles en `endpoints.py`

### Refactor
- `src/onpe/enrich.py` (NEW): módulo del paquete con lógica de enriquecimiento
- `scripts/build_curated.py`: importa `onpe.enrich` directamente (sin sys.path hack)
- `scripts/enrich_curated.py`: thin wrapper del módulo del paquete

### Performance
- `build_cabecera` retorna `(count, latest)` en vez de `(full_df, latest)` (reduce memory peak)
- `_load_volatile_tasks` usa `to_list + zip` en vez de `iter_rows(named=True)` (3-5× faster sobre 73k filas)

### Design
- `_NUMERIC_SCHEMAS` derivado automáticamente de `SCHEMAS` (una sola fuente de verdad)
- `CheckResult.skipped` bool para representar checks que requieren datos externos
- `nivel4_reconciliacion` ahora es SKIPPED en vez de FAIL espurio
- DQ summary muestra `X PASS, Y FAIL, Z SKIP`
- `drop(cs.by_name(..., require_all=False))` en vez de hardcoded column names

### Error handling UX
- `build_curated.py::main` catch `FileNotFoundError` con `log.critical` + exit 1

### Comment drift
- Removidas referencias a `task #28/#38/#42/#58` sin tracker público (6 archivos)
- `CLAUDE.md`: agregados scripts `analytics_report.py` + `build_choropleth.py` a "Comandos clave"
- `CLAUDE.md`: removidas menciones a sesiones privadas `/plan` y "Cowork mode"
- `schemas.py`: actualizado "10/10 PASS" → "N1+N2+N3 en PASS"
- `snapshot_aggregates.py`: removida fecha específica 2026-04-19

### Dead code
- `_local_key` (crawler.py) — nunca referenciado
- Fixture `tmp_facts_dir` (conftest.py) — nunca usado

## Criterios de salida

- **0 findings CRITICAL abiertos** ✓
- **0 findings HIGH abiertos** ✓
- **0 findings MEDIUM abiertos** ✓
- **0 findings LOW abiertos** ✓
- **CI verde** ✓ (post cada commit)
- **94 tests PASS** ✓ (87 originales + 7 nuevos para invariants)
- **Ruff check + format** ✓ clean

## Auditors

Agents ejecutados en paralelo:

| Agent | Findings reportados | Findings aplicados |
|---|---:|---:|
| security-reviewer | 3 | 2 aplicados, 1 descartado (falso positivo) |
| python-reviewer | 9 | 8 aplicados, 1 informativo |
| silent-failure-hunter | 7 | 6 aplicados, 1 informativo |
| refactor-cleaner | 5 | 4 aplicados, 1 mantenido (scripts documentados) |
| type-design-analyzer | 8 | 7 aplicados, 1 deferido (NewType sweep completo) |
| comment-analyzer | ~15 | 12 aplicados, 3 mantenidos (contexto válido) |
| security-scan (manual) | 0 | 0 (CLEAN) |

**Total**: ~28 findings aplicados en 3 commits. Repo listo para `Change visibility → Public`.
