# Contribuir a onpe-eg2026

Gracias por interesarte en contribuir. Este proyecto busca datos electorales limpios, verificables y útiles para analistas, periodistas e investigadores.

## Cómo reportar bugs

Abrí un issue en [GitHub](https://github.com/f3r21/onpe-eg2026/issues) con:

1. **Versión del repo** (`git rev-parse HEAD`)
2. **Pasos para reproducir** exactos (qué script corriste, qué flags usaste)
3. **Output esperado vs obtenido** (pegá stderr/stdout; evitá screenshots salvo para UI)
4. **Environment** (macOS/Linux, Python version, `uv --version`)

Si el bug afecta la integridad del dataset (DQ failure, identidades contables, conteos incorrectos), **marcá el issue como `data-quality` y proveé el `snapshot_ts_ms`** del curated que lo disparó.

## Cómo proponer cambios

1. **Fork + branch** desde `main`.
2. **Mantené un solo propósito por PR** (bug fix, feature, refactor, docs — no mezcles).
3. **Tests**: los cambios en `src/onpe/` requieren tests en `tests/` cuando el comportamiento cambia. Para bugs, escribí el test que falla antes del fix.
4. **Conventional commits** (`feat:`, `fix:`, `chore:`, `docs:`, `test:`).
5. **Sin emojis en código** (ni en strings, ni logs, ni comments). Los docs pueden usarlos con medida.
6. **Español** para comments, nombres técnicos, mensajes de log (el proyecto es en español).
7. **Ruff passing**: `uv run ruff check src scripts tests && uv run ruff format --check src scripts tests`.
8. **Tests passing**: `uv run pytest`.

## Dev setup

```bash
git clone https://github.com/f3r21/onpe-eg2026
cd onpe-eg2026
brew install uv         # o https://astral.sh/uv/install
uv sync --extra dev     # runtime + tests
uv run pytest           # 87 tests
uv run python scripts/smoke.py    # validación end-to-end contra API real
```

## Estilo de código

- **Polars** sobre pandas. Usar `scan_parquet` + streaming para datasets grandes.
- **asyncio** con semáforos para I/O concurrente; rate-limit global en el cliente.
- **Hive partitions** para snapshots raw.
- **Atomic writes** (tmp + rename) para archivos importantes.
- **Logging** sobre `print()`. Prints solo para CLI output crítico.
- **Type hints en signatures** (PEP 8 + PEP 604 union types con `|`).
- **dataclass(frozen=True)** para DTOs inmutables donde aplique.

## Áreas que necesitan contribuciones

Ver el [CHANGELOG.md](CHANGELOG.md) sección "Conocido" para gaps actuales:

- **Mapeo RENIEC ↔ ONPE ubigeos** — tabla cruzada necesaria para joins con padrón electoral RENIEC 2026.
- **OCR de PDFs de escrutinio** para extraer voto preferencial.
- **DQ Nivel 4** implementación específica cuando ONPE publique en datosabiertos.
- **Dashboards** con gráficos temporales del conteo (más allá del mapa actual).
- **Scrapers JNE / El Peruano** para enriquecer candidatos y capturar resoluciones oficiales.
- **Detección de anomalías** (z-score outliers por partido × distrito, sospecha de fraude).

## Código de conducta

Sé amable. No toleramos:

- Ataques personales, doxing, acoso.
- Manipulación política (este dataset debe servir a análisis objetivo, no a sesgar resultados).
- Introducir código malicioso o backdoors.
- Compartir credenciales, secrets o data privada de ONPE/RENIEC/JNE.

Los mantainers nos reservamos el derecho de rechazar PRs o banear contribuyentes que violen estas normas.

## Licencia

Al contribuir aceptás que tu código se publica bajo [MIT License](LICENSE).
