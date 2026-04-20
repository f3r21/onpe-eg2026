"""Schemas esperados por tabla — fail-fast contra type drift de ONPE.

Los schemas fueron derivados empíricamente del curated 2026-04-18 (snapshot
post-elección, 463,830 actas × 5 elecciones; 10/10 PASS en DQ Nivel 1+2).
Si ONPE cambia tipos sin aviso, `validate_chunk()` detecta el drift antes
de escribir el chunk al parquet y propaga `SchemaDriftError` para que
el loop lo atrape y salte el chunk sin contaminar el facts layer.

Tres tipos de inconsistencia detectados:
- `mismatch`: columna existe en df pero con dtype distinto al esperado
- `missing`: columna esperada no aparece en df (ONPE removió un campo)
- `extra`: columna en df no esperada (ONPE agregó un campo nuevo — solo warning)

Cuando ONPE cambia algo intencionalmente, actualizar SCHEMAS aquí y correr
tests. Referencia cruzada con `_NUMERIC_SCHEMAS` en `onpe.actas` (subset
numérico usado para coerción de columnas pl.Null).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

import polars as pl

log = logging.getLogger(__name__)


class SchemaDriftError(Exception):
    """Se detectó drift de schema que requiere abortar el chunk."""

    def __init__(self, table: str, violations: list[str]) -> None:
        self.table = table
        self.violations = violations
        super().__init__(
            f"schema drift en tabla '{table}': {len(violations)} violación(es); "
            + "; ".join(violations)
        )


# Schema canónico por tabla. Derivado del curated 2026-04-18 (PASS DQ N1+N2).
# Incluye columnas de stamp (snapshot_ts_ms, snapshot_lima_iso).
SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "actas_cabecera": {
        "id": pl.Int64,
        "codigoMesa": pl.String,
        "descripcionMesa": pl.String,
        "idEleccion": pl.Int64,
        "ubigeoNivel01": pl.String,
        "ubigeoNivel02": pl.String,
        "ubigeoNivel03": pl.String,
        "centroPoblado": pl.String,
        "nombreLocalVotacion": pl.String,
        "totalElectoresHabiles": pl.Int64,
        "totalVotosEmitidos": pl.Int64,
        "totalVotosValidos": pl.Int64,
        "totalAsistentes": pl.Int64,
        "porcentajeParticipacionCiudadana": pl.Float64,
        "estadoActa": pl.String,
        "estadoComputo": pl.String,
        "codigoEstadoActa": pl.String,
        "descripcionEstadoActa": pl.String,
        "estadoActaResolucion": pl.String,
        "estadoDescripcionActaResolucion": pl.String,
        "descripcionSubEstadoActa": pl.String,
        "codigoSolucionTecnologica": pl.Int64,
        "descripcionSolucionTecnologica": pl.String,
        "idActa": pl.Int64,
        "idMesaRef": pl.Int64,
        "ubigeoDistrito": pl.String,
        "snapshot_ts_ms": pl.Int64,
        "snapshot_lima_iso": pl.String,
    },
    "actas_votos": {
        "idActa": pl.Int64,
        "idEleccion": pl.Int64,
        "ubigeoDistrito": pl.String,
        "descripcion": pl.String,
        "es_especial": pl.Boolean,
        # ccodigo es String zero-padded ("00000014"), no Int — ver CLAUDE.md.
        "ccodigo": pl.String,
        "nposicion": pl.Int64,
        "nvotos": pl.Int64,
        "nagrupacionPolitica": pl.Int64,
        # nporcentaje*: el API ONPE devuelve Float en el JSON (ej. 41.463 sin
        # comillas). Polars infiere Float64 al serializar el chunk. En el curated
        # puede aparecer como String por la promoción Null→String via
        # diagonal_relaxed (ver build_curated._scan_relaxed); esto es un artefacto
        # del layer de curación, no del contrato de la tabla fact. schemas.py
        # valida CHUNKS pre-write, así que el target correcto es Float64.
        "nporcentajeVotosValidos": pl.Float64,
        "nporcentajeVotosEmitidos": pl.Float64,
        "estado": pl.Int64,
        "grafico": pl.Int64,
        "cargo": pl.String,
        "sexo": pl.String,
        "totalCandidatos": pl.Int64,
        "cand_apellido_paterno": pl.String,
        "cand_apellido_materno": pl.String,
        "cand_nombres": pl.String,
        "cand_doc": pl.String,
        "snapshot_ts_ms": pl.Int64,
        "snapshot_lima_iso": pl.String,
    },
    "actas_linea_tiempo": {
        "idActa": pl.Int64,
        "idEleccion": pl.Int64,
        "ubigeoDistrito": pl.String,
        "evento_idx": pl.Int64,
        "codigoEstadoActa": pl.String,
        "descripcionEstadoActa": pl.String,
        "descripcionEstadoActaResolucion": pl.String,
        "fechaRegistro": pl.Int64,
        "snapshot_ts_ms": pl.Int64,
        "snapshot_lima_iso": pl.String,
    },
    "actas_archivos": {
        "idActa": pl.Int64,
        "idEleccion": pl.Int64,
        "ubigeoDistrito": pl.String,
        # archivoId es String (UUID-like formatted), aunque el campo `id` del
        # sub-object en ONPE puede ser Int. Se preserva como String porque
        # el upstream (normalize_acta) toma a.get("id") que viene así.
        "archivoId": pl.String,
        "tipo": pl.Int64,
        "nombre": pl.String,
        "descripcion": pl.String,
        "daudFechaCreacion": pl.Int64,
        "snapshot_ts_ms": pl.Int64,
        "snapshot_lima_iso": pl.String,
    },
}


def _dtype_name(dt: pl.DataType) -> str:
    """Nombre estable para comparar dtypes en mensajes de error."""
    return str(dt)


def validate_chunk(
    df: pl.DataFrame,
    table: str,
    *,
    strict: bool = True,
    allowed_null_cols: Iterable[str] | None = None,
) -> list[str]:
    """Valida el schema del DataFrame contra SCHEMAS[table].

    Args:
        df: DataFrame a validar.
        table: nombre de la tabla (clave de SCHEMAS).
        strict: si True, lanza SchemaDriftError ante violaciones (missing +
            mismatch). Columnas extra generan solo warning en log.
        allowed_null_cols: columnas que pueden ser pl.Null sin gatillar
            mismatch. Útil para chunks chicos donde una columna
            opcional puede no tener valores y _coerce_null_columns aún no
            casteó (caso poco común pero válido en tests).

    Returns:
        Lista de strings describiendo cada violación. Vacía si todo OK.

    Raises:
        SchemaDriftError: si strict=True y hay violaciones.
        KeyError: si `table` no existe en SCHEMAS.
    """
    expected = SCHEMAS[table]
    allowed_nulls = set(allowed_null_cols or ())
    actual = df.schema
    violations: list[str] = []

    # 1. Columnas esperadas faltantes.
    for col in expected:
        if col not in actual:
            violations.append(f"missing: columna '{col}' esperada pero ausente")

    # 2. Dtype mismatch en columnas presentes.
    for col, expected_dt in expected.items():
        if col not in actual:
            continue
        actual_dt = actual[col]
        if actual_dt == pl.Null and col in allowed_nulls:
            continue
        if actual_dt != expected_dt:
            violations.append(
                f"mismatch: '{col}' esperado={_dtype_name(expected_dt)} "
                f"obtenido={_dtype_name(actual_dt)}"
            )

    # 3. Columnas extra (solo warning — puede ser feature nueva de ONPE).
    extras = [c for c in actual if c not in expected]
    if extras:
        log.warning(
            "tabla %s: %d columna(s) extra no esperadas (posible feature nueva ONPE): %s",
            table,
            len(extras),
            extras,
        )

    if violations and strict:
        raise SchemaDriftError(table, violations)

    return violations
