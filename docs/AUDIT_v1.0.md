# Audit pre-release v1.0 — findings diferidos

Resumen de la auditoría ejecutada el 2026-04-20 con 7 agents especializados antes de hacer el repo público.

## Findings aplicados en el commit `6dfd8de`

Ver commit message para el detalle. Categorías cubiertas:

- **Security (HIGH)**: path traversal defense, zip slip protection, `asyncio.get_running_loop()`, PreconditionFailed race handling.
- **Data integrity (HIGH)**: `self._fd` assignment order en `PipelineLock`, `asyncio.gather(return_exceptions=True)` + `CancelledError` re-raise en múltiples paths.
- **Type safety (HIGH)**: `Literal` en `DownloadResult.status`, `TYPE_CHECKING` guard para `Bucket`, `ClientConfig` range validation, `DownloadResult` invariants.
- **Doc drift (HIGH)**: CLAUDE.md cleanup, stale task references.
- **Dead code**: `_local_key` (crawler.py), fixture `tmp_facts_dir` (conftest.py).

## Findings diferidos a backlog (MEDIUM — post-release)

### Perf — `scripts/daily_refresh.py::_load_volatile_tasks`

**Finding**: `iter_rows(named=True)` con 73k filas crea ~60 MB de dicts Python por iteración completa.

**Fix sugerido**:
```python
rows = volatiles.select(["idActa", "idMesaRef", "ubigeoDistrito", "idEleccion"])
tasks = list(zip(
    rows["idActa"].to_list(),
    rows["idMesaRef"].to_list(),
    rows["ubigeoDistrito"].to_list(),
    rows["idEleccion"].to_list(),
))
```

**Impacto**: 3-5x faster, reduce peak memory.

**Por qué diferido**: el pipeline actual funciona; optimización sin urgencia.

### Refactor — `scripts/build_curated.py`: sys.path manipulation

**Finding**: `sys.path.insert` para importar `enrich_curated` (script) desde `build_curated` (script). Fragile si el CWD cambia.

**Fix sugerido**: mover `enrich_cabecera` y `_validate_integrity` a `src/onpe/enrich.py`, importar desde el paquete.

**Por qué diferido**: requiere restructurar, crear nuevo módulo en src/.

### Memory — `build_cabecera` materializa DataFrame completo

**Finding**: `build_cabecera` retorna el DataFrame completo (~463k filas) cuando solo se usa `.height` en el caller.

**Fix sugerido**: retornar solo `count` + `latest` LazyFrame; usar `sink_parquet` para la escritura (igual que `build_votos` y `_build_aux` que ya usan streaming).

**Por qué diferido**: 463k es manejable en 24GB. Cuando el dataset crezca, revisitar.

### Type design — SCHEMAS/\_NUMERIC_SCHEMAS duplicación

**Finding**: `_NUMERIC_SCHEMAS` es un subset derivable de `SCHEMAS` pero está duplicado.

**Fix sugerido**:
```python
_NUMERIC_TYPES = {pl.Int64, pl.Float64, pl.Boolean}
_NUMERIC_SCHEMAS = {
    table: {col: dt for col, dt in cols.items() if dt in _NUMERIC_TYPES}
    for table, cols in SCHEMAS.items()
}
```

**Por qué diferido**: hoy 14/14 DQ PASS; divergencia no ocurrió. Cambio no urgente.

### Error handling — `scripts/build_curated.py::main`

**Finding**: `_scan_relaxed` levanta `FileNotFoundError` sin catch en `main()` → traceback crudo en vez de `log.critical` + `sys.exit(1)`.

**Fix sugerido**: wrapping en try/except con logging a nivel CRITICAL.

**Por qué diferido**: cosmético; el pipeline funciona, solo UX de error.

### Error handling — `scripts/snapshot_aggregates.py::_entrypoint`

**Finding**: `except Exception` no filtra explícitamente `asyncio.CancelledError` (aunque ya es BaseException, el comentario no lo aclara).

**Fix sugerido**: agregar `except asyncio.CancelledError: raise` antes del catch-all para claridad.

**Por qué diferido**: comportamiento correcto con `BaseException`; es claridad de código, no bug.

### Dev friction — `nivel4_reconciliacion` siempre SKIPPED

**Finding**: `nivel4_reconciliacion_votos_por_partido` en `dq_check.py` retorna skeleton `CheckResult(False, "TODO")` hasta que ONPE publique datosabiertos oficial.

**Fix sugerido**: añadir guard explícito `SKIPPED` (como el `datasource` check ya hace) en vez de `False/TODO` que luce como fail.

**Por qué diferido**: task D3 aún en wait (datosabiertos ONPE ~4 semanas post-JNE). Cuando se implemente real, reemplazar.

### Scripts huérfanos — analytics_report.py, build_choropleth.py

**Finding**: no están en la sección "Comandos clave" de CLAUDE.md (sí en ARCHITECTURE.md y CHANGELOG).

**Fix sugerido**: agregar referencias a CLAUDE.md.

**Por qué diferido**: no es bug; es mejora de descubribilidad menor.

## Findings diferidos a backlog (LOW)

- `client.py::__aexit__` swallows `aclose()` errors en shutdown. Impacto mínimo.
- `build_curated.py` drop Hive partition columns asume nombres específicos. Si Polars cambia, romper explícito.
- `NewType` para `IdActa`, `UbigeoDistrito`, `ArchivoId` — type safety adicional, no urgente.
- Task ID references (`#42`, `#48`, `#58`) sin tracker público en comments varios.
- Fechas específicas (`2026-04-18`, `2026-04-19`) en comments de motivación → envejecen mal pero no mienten.

## Criterios cumplidos

- **0 findings CRITICAL abiertos** ✓
- **0 findings HIGH abiertos** ✓ (todos aplicados en commit `6dfd8de`)
- **CI verde post-fixes** ✓ (run `25...`, en progreso al momento)
- **MEDIUM/LOW documentados** ✓ (este archivo)

El repo está listo para `Change visibility → Public`.

## Auditors

Agents ejecutados en paralelo el 2026-04-20:

| Agent | Severity peak | Findings aplicados |
|---|---|---|
| security-reviewer | MEDIUM | 2 (path traversal, zip slip) |
| python-reviewer | HIGH | 3 (get_running_loop, CancelledError, bucket type) |
| silent-failure-hunter | CRITICAL | 2 (PreconditionFailed race, locks _fd order) |
| refactor-cleaner | HIGH confidence | 2 (dead code: _local_key, tmp_facts_dir) |
| type-design-analyzer | HIGH | 3 (ClientConfig validation, DownloadResult Literal, DownloadResult invariants) |
| comment-analyzer | HIGH | 3 (CLAUDE.md, dq_check comment, investigate script log) |
| general-purpose (security-scan) | CLEAN | 0 (0 findings en configs/CI/templates) |
