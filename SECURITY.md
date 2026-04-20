# Security Policy

## Versiones soportadas

Mantenemos security support solo para la release más reciente (`v1.x`). Versiones anteriores reciben fixes solo si un issue es crítico y reportado.

## Reportar una vulnerabilidad

**No abras un issue público** para vulnerabilidades. En su lugar:

1. Enviá un report privado vía [GitHub Private Vulnerability Reporting](https://github.com/f3r21/onpe-eg2026/security/advisories/new).
2. Incluí: descripción, pasos para reproducir, impacto estimado.
3. Respondo en un plazo de **7 días** con acknowledgment y plan de fix.

## Scope

Dentro de scope:
- Scraping en condiciones que violen ToS de ONPE no documentadas aquí
- Inyección de código vía inputs externos (schemas, ubigeos, archivoIds)
- Bypass de rate-limit que afecte la disponibilidad del API público
- Fuga de credenciales o paths privados en commits recientes

Fuera de scope:
- Vulnerabilidades en ONPE mismo (reportar a la entidad)
- Denial of service generado por uso legítimo del pipeline
- Issues en dependencias externas (reportar upstream a httpx, polars, etc.)
- Hallazgos en data pública ya publicada por ONPE
