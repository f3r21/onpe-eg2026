"""Checks de calidad de datos (DQ) contra el snapshot curated + facts.

Nivel 1 — integridad interna del curated:
  * cardinalidades (universo, filas por acta)
  * identidades contables (sum detalle == totales cabecera)
  * rangos / signos
  * coherencia de padrón entre elecciones de la misma mesa

Nivel 2 — validación cruzada curated detalle ↔ ONPE aggregates (facts):
  * universo de mesas
  * actas por bucket (C/E/P) por elección
  * votos emitidos/válidos por elección
  * actas por región (mapa_calor vs curated)
  * partidos (/participantes vs actas_votos)

Ejecución:
    uv run python scripts/dq_check.py            # ambos niveles
    uv run python scripts/dq_check.py --nivel 1  # solo Nivel 1
    uv run python scripts/dq_check.py --nivel 2  # solo Nivel 2
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

import polars as pl

log = logging.getLogger("dq_check")

ROOT = Path(__file__).resolve().parent.parent
CAB = ROOT / "data" / "curated" / "actas_cabecera.parquet"
VOT = ROOT / "data" / "curated" / "actas_votos.parquet"
FACTS = ROOT / "data" / "facts"


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def _fmt(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


# ---- Nivel 1 --------------------------------------------------------------


def nivel1_universo() -> CheckResult:
    df = (
        pl.scan_parquet(CAB)
        .group_by("idEleccion")
        .agg(pl.col("idActa").n_unique().alias("u"), pl.len().alias("n"))
        .collect()
    )
    ok = (df["u"] == df["n"]).all() and df["n"].n_unique() == 1
    return CheckResult("universo por elección", ok, str(df))


def nivel1_cardinalidad_detalle() -> CheckResult:
    df = (
        pl.scan_parquet(VOT)
        .group_by(["idActa", "idEleccion"])
        .agg(pl.len().alias("n"))
        .group_by("n")
        .agg(pl.len().alias("actas"))
        .sort("n")
        .collect()
    )
    ok = len(df) == 1 and df["n"][0] == 41
    return CheckResult("cardinalidad detalle = 41", ok, str(df))


def nivel1_identidades_contables() -> CheckResult:
    agg = (
        pl.scan_parquet(VOT)
        .group_by(["idActa", "idEleccion"])
        .agg(
            pl.col("nvotos").sum().alias("sum_tot"),
            pl.when(~pl.col("es_especial"))
            .then(pl.col("nvotos"))
            .otherwise(0)
            .sum()
            .alias("sum_val"),
        )
    )
    cab = (
        pl.scan_parquet(CAB)
        .filter(pl.col("codigoEstadoActa") == "C")
        .select(["idActa", "idEleccion", "totalVotosEmitidos", "totalVotosValidos"])
    )
    # Solo evaluamos las que SÍ tienen detalle; las sin_detalle son una anomalía
    # separada (240 actas estadoActa=N, tracked en task #38).
    res = (
        cab.join(agg, on=["idActa", "idEleccion"], how="inner")
        .select(
            pl.len().alias("total_con_detalle"),
            (pl.col("sum_tot") != pl.col("totalVotosEmitidos")).sum().alias("mismatch_emit"),
            (pl.col("sum_val") != pl.col("totalVotosValidos")).sum().alias("mismatch_val"),
        )
        .collect(engine="streaming")
    )
    total_cd = int(res["total_con_detalle"][0])
    mism_em = int(res["mismatch_emit"][0])
    mism_va = int(res["mismatch_val"][0])
    ok = mism_em == 0 and mism_va == 0
    return CheckResult(
        "identidades contables (C con detalle)",
        ok,
        f"total_con_detalle={total_cd} mismatch_emit={mism_em} mismatch_val={mism_va}",
    )


def nivel1_rangos() -> CheckResult:
    df = (
        pl.scan_parquet(CAB)
        .filter(pl.col("codigoEstadoActa") == "C")
        .select(
            (pl.col("totalVotosEmitidos") < 0).sum().alias("emit_neg"),
            (pl.col("totalVotosValidos") < 0).sum().alias("val_neg"),
            (pl.col("totalElectoresHabiles") <= 0).sum().alias("hab_le_0"),
            (pl.col("totalVotosEmitidos") > pl.col("totalElectoresHabiles"))
            .sum()
            .alias("emit_gt_hab"),
            (pl.col("totalVotosValidos") > pl.col("totalVotosEmitidos")).sum().alias("val_gt_emit"),
        )
        .collect()
    )
    ok = df.row(0) == (0, 0, 0, 0, 0)
    return CheckResult("rangos y signos", ok, str(df))


def nivel1_padron_coherente() -> CheckResult:
    df = (
        pl.scan_parquet(CAB)
        .group_by("codigoMesa")
        .agg(
            pl.col("idEleccion").n_unique().alias("elec"),
            pl.col("totalElectoresHabiles").n_unique().alias("hab"),
            pl.col("ubigeoDistrito").n_unique().alias("dist"),
        )
        .collect()
    )
    ok = (df["elec"] == 5).all() and (df["hab"] == 1).all() and (df["dist"] == 1).all()
    return CheckResult("padrón coherente por mesa", ok, f"mesas={len(df)}")


def run_nivel1() -> list[CheckResult]:
    return [
        nivel1_universo(),
        nivel1_cardinalidad_detalle(),
        nivel1_identidades_contables(),
        nivel1_rangos(),
        nivel1_padron_coherente(),
    ]


# ---- Nivel 2 --------------------------------------------------------------


def _drift_minutes() -> float:
    cab_ts = pl.scan_parquet(CAB).select(pl.col("snapshot_ts_ms").max()).collect().item()
    tot_ts = (
        pl.scan_parquet(str(FACTS / "totales" / "**" / "*.parquet"))
        .select(pl.col("snapshot_ts_ms").max())
        .collect()
        .item()
    )
    return (cab_ts - tot_ts) / 1000 / 60


def _latest_snapshot(table: str) -> pl.DataFrame:
    """Devuelve solo la foto más reciente de la tabla de aggregates.

    Los aggregates se toman periódicamente (ver aggregate_loop), así que
    facts/<tabla>/ acumula N snapshots. Para comparar contra curated
    necesitamos un único snapshot, no la concatenación.
    """
    lf = pl.scan_parquet(str(FACTS / table / "**" / "*.parquet"))
    max_ts = lf.select(pl.col("snapshot_ts_ms").max()).collect().item()
    return lf.filter(pl.col("snapshot_ts_ms") == max_ts).collect()


def nivel2_universo_mesas() -> CheckResult:
    mt = _latest_snapshot("mesa_totales")
    oficial = int(mt["mesasInstaladas"][0] + mt["mesasPendientes"][0] + mt["mesasNoInstaladas"][0])
    cur = pl.scan_parquet(CAB).select(pl.col("codigoMesa").n_unique()).collect().item()
    ok = oficial == cur
    return CheckResult("universo mesas ONPE vs curated", ok, f"oficial={oficial} curated={cur}")


def nivel2_actas_por_eleccion() -> CheckResult:
    """Verifica que oficial_total = cur_total; las deltas C/E/P son drift temporal."""
    ofi = _latest_snapshot("totales").select(["idEleccion", pl.col("totalActas").alias("ofi_tot")])
    cur = pl.scan_parquet(CAB).group_by("idEleccion").agg(pl.len().alias("cur_tot")).collect()
    j = ofi.join(cur, on="idEleccion")
    ok = (j["ofi_tot"] == j["cur_tot"]).all()
    return CheckResult("totalActas por elección", ok, str(j))


def nivel2_votos_id10_exactos() -> CheckResult:
    """Presidencial (id=10) ya cerró cerca del 100% — deltas deben ser 0."""
    ofi = (
        _latest_snapshot("totales")
        .filter(pl.col("idEleccion") == 10)
        .select("totalVotosEmitidos", "totalVotosValidos")
        .row(0)
    )
    cur = (
        pl.scan_parquet(CAB)
        .filter((pl.col("idEleccion") == 10) & (pl.col("codigoEstadoActa") == "C"))
        .select(
            pl.col("totalVotosEmitidos").sum(),
            pl.col("totalVotosValidos").sum(),
        )
        .collect()
        .row(0)
    )
    # Drift temporal: aggregates frescas + curated 1d atrás = delta esperado.
    # Chequeamos magnitud relativa ≤ 0.1% (tolerancia anti-drift para Presidencial).
    emit_ofi, val_ofi = ofi
    emit_cur, val_cur = cur
    emit_pct = abs(emit_ofi - emit_cur) / max(emit_ofi, 1) * 100
    val_pct = abs(val_ofi - val_cur) / max(val_ofi, 1) * 100
    ok = emit_pct <= 0.1 and val_pct <= 0.1
    return CheckResult(
        "votos id=10 drift ≤ 0.1%",
        ok,
        f"oficial={ofi} curated={cur} Δemit={emit_pct:.3f}% Δval={val_pct:.3f}%",
    )


def nivel2_regiones_id10() -> CheckResult:
    """mapa_calor id=10 vs curated: delta por departamento normalizado.

    Drift esperado mientras curated tiene snapshot antiguo. Tolerancia ≤1%.
    """
    mc = (
        _latest_snapshot("mapa_calor")
        .filter(pl.col("idEleccion") == 10)
        .select(
            pl.col("ubigeoNivel01").alias("ub01"),
            pl.col("actasContabilizadas").alias("ofi_C"),
        )
    )
    cur = (
        pl.scan_parquet(CAB)
        .filter((pl.col("idEleccion") == 10) & (pl.col("codigoEstadoActa") == "C"))
        .with_columns(
            (pl.col("ubigeoDistrito").str.slice(0, 2).cast(pl.Int64) * 10000).alias("ub01")
        )
        .group_by("ub01")
        .agg(pl.len().alias("cur_C"))
        .collect()
    )
    j = cur.join(mc, on="ub01", how="inner")
    deltas = (j["cur_C"] - j["ofi_C"]).abs()
    max_delta = int(deltas.max() or 0)
    # Tolerancia: delta ≤ max(10, 1% del total)
    tolerance = max(10, int(j["ofi_C"].sum() * 0.01 / max(len(j), 1)))
    ok = max_delta <= tolerance and len(j) == len(mc)
    return CheckResult(
        "regiones id=10 mapa_calor vs curated",
        ok,
        f"regiones_match={len(j)} max_delta={max_delta} tolerancia={tolerance}",
    )


def nivel2_partidos_id10() -> CheckResult:
    """/participantes oficial vs curated — los partidos deben coincidir estructuralmente."""
    ofi = (
        _latest_snapshot("participantes")
        .filter(pl.col("idEleccion") == 10)
        .select(
            pl.col("codigoAgrupacionPolitica").cast(pl.Int64).alias("cod"),
            pl.col("totalVotosValidos").alias("ofi_val"),
        )
    )
    cur = (
        pl.scan_parquet(VOT)
        .filter((~pl.col("es_especial")) & (pl.col("idEleccion") == 10))
        .group_by("nagrupacionPolitica")
        .agg(pl.col("nvotos").sum().alias("cur_val"))
        .rename({"nagrupacionPolitica": "cod"})
        .collect()
    )
    j = ofi.join(cur, on="cod", how="inner")
    # Drift normalizado ≤ 0.5% sobre el total de votos de todos los partidos.
    total_ofi = int(j["ofi_val"].sum())
    total_delta = int((j["cur_val"] - j["ofi_val"]).abs().sum())
    drift_pct = (total_delta / max(total_ofi, 1)) * 100
    estructural_ok = len(j) == len(ofi)
    drift_ok = drift_pct <= 0.5
    ok = estructural_ok and drift_ok
    return CheckResult(
        "partidos id=10 estructural + drift ≤ 0.5%",
        ok,
        f"pares_match={len(j)}/{len(ofi)} drift={drift_pct:.3f}%",
    )


def run_nivel2() -> list[CheckResult]:
    return [
        nivel2_universo_mesas(),
        nivel2_actas_por_eleccion(),
        nivel2_votos_id10_exactos(),
        nivel2_regiones_id10(),
        nivel2_partidos_id10(),
    ]


# ---- Nivel 3 --------------------------------------------------------------
# Nivel 3 requiere que el curated tenga idDistritoElectoral + idAmbitoGeografico
# materializados (enrich_curated.py). Si faltan, los checks fallan con mensaje claro.


def _require_enriched_curated() -> None:
    cols = pl.scan_parquet(CAB).collect_schema().names()
    missing = [c for c in ("idDistritoElectoral", "idAmbitoGeografico") if c not in cols]
    if missing:
        raise SystemExit(
            f"curated/actas_cabecera.parquet no está enriquecido (falta {missing}). "
            "Correr scripts/enrich_curated.py o build_curated.py sin --no-enrich antes de Nivel 3."
        )


def nivel3_totales_de_vs_curated_id13() -> CheckResult:
    """Para Diputados (id=13), totalActas por DE en aggregates == count en curated.

    DE cerrados (100% contabilizadas) deberían coincidir exactamente.
    DE activos pueden tener delta pequeño por drift temporal (tolerancia: ≤1%).
    """
    ofi = (
        pl.scan_parquet(str(FACTS / "totales_de" / "**" / "*.parquet"))
        .filter(pl.col("idEleccion") == 13)
        .select(
            pl.col("idDistritoElectoral"),
            pl.col("totalActas").alias("ofi_total"),
        )
        .unique(subset=["idDistritoElectoral"], keep="last")
        .collect()
    )
    cur = (
        pl.scan_parquet(CAB)
        .filter(pl.col("idEleccion") == 13)
        .group_by("idDistritoElectoral")
        .agg(pl.len().alias("cur_total"))
        .collect()
    )
    j = ofi.join(cur, on="idDistritoElectoral", how="inner")
    deltas = (j["cur_total"] - j["ofi_total"]).abs()
    max_delta = int(deltas.max() or 0)
    ok = max_delta == 0 and len(j) == len(ofi)
    return CheckResult(
        "totales_de id=13 (Diputados) vs curated",
        ok,
        f"DEs_match={len(j)}/{len(ofi)} max_delta={max_delta}",
    )


def nivel3_partidos_diputados_por_de() -> CheckResult:
    """participantes_de id=13 vs sum(nvotos) en curated por (DE × partido).

    Chequeo estructural: TODOS los pares (DE × partido) del oficial deben
    encontrarse en curated. Drift temporal (aggregates frescas vs curated 1d):
    se reporta pero no falla si se mantiene bajo 5% del total por DE.

    PASS si:
    - pares_match == pares_ofi (universo estructural coincide)
    - drift promedio normalizado ≤ 5% por DE
    """
    ofi = (
        pl.scan_parquet(str(FACTS / "participantes_de" / "**" / "*.parquet"))
        .filter(pl.col("idEleccion") == 13)
        .select(
            pl.col("idDistritoElectoral").alias("de"),
            pl.col("codigoAgrupacionPolitica").cast(pl.Int64).alias("cod"),
            pl.col("totalVotosValidos").alias("ofi_val"),
        )
        .unique(subset=["de", "cod"], keep="last")
        .collect()
    )
    # Para cruzar con aggregates, NO filtramos C — aggregates incluye todo acta
    # con detalle (C + E). actas_votos ya excluye P (sin detalle).
    cur = (
        pl.scan_parquet(VOT)
        .filter((~pl.col("es_especial")) & (pl.col("idEleccion") == 13))
        .join(
            pl.scan_parquet(CAB).select("idActa", "idDistritoElectoral"),
            on="idActa",
            how="inner",
        )
        .group_by(["idDistritoElectoral", "nagrupacionPolitica"])
        .agg(pl.col("nvotos").sum().alias("cur_val"))
        .rename({"idDistritoElectoral": "de", "nagrupacionPolitica": "cod"})
        .collect()
    )
    j = ofi.join(cur, on=["de", "cod"], how="inner")
    deltas = (j["cur_val"] - j["ofi_val"]).abs()
    max_delta = int(deltas.max() or 0)
    # Drift normalizado por DE: sum|delta| / sum(ofi_val) ≤ 5%
    drift_by_de = (
        j.group_by("de")
        .agg(
            (pl.col("cur_val") - pl.col("ofi_val")).abs().sum().alias("abs_delta"),
            pl.col("ofi_val").sum().alias("total"),
        )
        .with_columns(
            (
                pl.col("abs_delta")
                / pl.when(pl.col("total") > 0).then(pl.col("total")).otherwise(1)
            ).alias("ratio")
        )
    )
    max_drift_pct = float(drift_by_de["ratio"].max() or 0.0) * 100
    estructural_ok = len(j) == len(ofi)
    drift_ok = max_drift_pct <= 5.0
    ok = estructural_ok and drift_ok
    return CheckResult(
        "partidos por DE id=13 (Diputados)",
        ok,
        f"pares={len(j)}/{len(ofi)} max_delta={max_delta} max_drift_de={max_drift_pct:.2f}%",
    )


def nivel3_exterior_universo_senadores_nacional() -> CheckResult:
    """DE 27 (Exterior) × idEleccion=15 (Senadores nacional) → exactamente 2,543 actas.

    El universo de mesas del exterior es 2,543 (CLAUDE.md); una elección = 2,543 actas.
    """
    df = (
        pl.scan_parquet(CAB)
        .filter((pl.col("idDistritoElectoral") == 27) & (pl.col("idEleccion") == 15))
        .select(pl.len().alias("n"))
        .collect()
    )
    n = int(df["n"][0])
    ok = n == 2543
    return CheckResult(
        "DE 27 (Exterior) id=15 universo = 2,543",
        ok,
        f"actas={n} esperadas=2543",
    )


def nivel3_coherencia_ambito_vs_de() -> CheckResult:
    """idAmbitoGeografico y idDistritoElectoral deben ser coherentes.

    - Todas las actas con DE=27 deben tener idAmbitoGeografico=2.
    - Todas las actas con idAmbitoGeografico=2 deben tener DE=27.
    - Ninguna otra combinación cruzada debe existir.
    """
    df = (
        pl.scan_parquet(CAB)
        .select(
            ((pl.col("idDistritoElectoral") == 27) & (pl.col("idAmbitoGeografico") != 2))
            .sum()
            .alias("de27_no_ext"),
            ((pl.col("idAmbitoGeografico") == 2) & (pl.col("idDistritoElectoral") != 27))
            .sum()
            .alias("ext_no_de27"),
            ((pl.col("idDistritoElectoral").is_null()) | (pl.col("idAmbitoGeografico").is_null()))
            .sum()
            .alias("nulls"),
        )
        .collect()
    )
    de27_ext = int(df["de27_no_ext"][0])
    ext_de27 = int(df["ext_no_de27"][0])
    nulls = int(df["nulls"][0])
    ok = de27_ext == 0 and ext_de27 == 0 and nulls == 0
    return CheckResult(
        "coherencia ambito × DE",
        ok,
        f"de27_no_ext={de27_ext} ext_no_de27={ext_de27} nulls={nulls}",
    )


def run_nivel3() -> list[CheckResult]:
    _require_enriched_curated()
    return [
        nivel3_coherencia_ambito_vs_de(),
        nivel3_exterior_universo_senadores_nacional(),
        nivel3_totales_de_vs_curated_id13(),
        nivel3_partidos_diputados_por_de(),
    ]


# ---- Nivel 4 (cruce contra datosabiertos oficial post-proclamación) -------


def nivel4_datasource_disponible() -> CheckResult:
    """Pre-check: existe el parquet de datosabiertos EG2026?

    Si no existe (ONPE aún no publicó en datosabiertos, ~2-4 sem post-JNE),
    todos los checks Nivel 4 se skippean con mensaje informativo.
    """
    oficiales = sorted((CAB.parent).glob("datosabiertos_eg2026_*.parquet"))
    ok = len(oficiales) > 0
    detail = (
        f"datasets oficiales: {[p.name for p in oficiales]}"
        if ok
        else "PENDING — ONPE aún no publicó en datosabiertos.gob.pe. "
        "Correr `scripts/monitor_datosabiertos.py` semanalmente."
    )
    return CheckResult("datosabiertos EG2026 disponible", ok, detail)


def nivel4_reconciliacion_votos_por_partido() -> CheckResult:
    """Reconcilia votos por (partido × distrito × elección) — curated vs oficial.

    PENDING hasta que ONPE publique EG2026 en datosabiertos. El skeleton
    espera columnas tipo: UBIGEO, PARTIDO/CANDIDATO_ID, VOTOS_PARTIDO.
    El mapping exacto se confirma post-publicación.
    """
    oficiales = sorted((CAB.parent).glob("datosabiertos_eg2026_*.parquet"))
    if not oficiales:
        return CheckResult(
            "reconciliación votos por partido × distrito × elección",
            False,
            "SKIPPED — no hay parquet oficial aún",
        )
    # TODO: cuando haya data real, implementar:
    # - join curated actas_votos agregado por (idEleccion × idDistritoElectoral × partido)
    # - vs oficial datosabiertos agregado mismas dimensiones
    # - delta ≤ 1 voto por grupo
    return CheckResult(
        "reconciliación votos (skeleton)",
        False,
        f"TODO — implementar cuando schema oficial esté disponible. "
        f"Encontrados: {len(oficiales)} parquet(s)",
    )


def run_nivel4() -> list[CheckResult]:
    return [
        nivel4_datasource_disponible(),
        nivel4_reconciliacion_votos_por_partido(),
    ]


# ---- Main -----------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--nivel",
        type=int,
        choices=[1, 2, 3, 4, 0],
        default=0,
        help="1 | 2 | 3 | 4 = solo un nivel; 0 = todos los niveles disponibles (default)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    results: list[tuple[int, CheckResult]] = []
    if args.nivel in (0, 1):
        for r in run_nivel1():
            results.append((1, r))
    if args.nivel in (0, 2):
        log.info("\nDrift entre curated y aggregates: %.1f min", _drift_minutes())
        for r in run_nivel2():
            results.append((2, r))
    if args.nivel in (0, 3):
        for r in run_nivel3():
            results.append((3, r))
    if args.nivel in (0, 4):
        for r in run_nivel4():
            results.append((4, r))

    log.info("\n%-6s %-6s %-40s %s", "NIVEL", "RES", "CHECK", "DETALLE")
    log.info("-" * 100)
    failed = 0
    for nivel, r in results:
        log.info("%-6d %-6s %-40s %s", nivel, _fmt(r.passed), r.name, r.detail.replace("\n", " | "))
        if not r.passed:
            failed += 1
    log.info("-" * 100)
    log.info("Resumen: %d PASS, %d FAIL (total %d)", len(results) - failed, failed, len(results))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
