"""Smoke test — ejercita todos los endpoints principales contra el API real.

Verifica además la fórmula determinística del idActa con una mesa conocida
(mesa 5507 de CAYMA, distrito 040102, Presidencial → idActa=550704010210).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from onpe.client import ClientConfig, OnpeClient
from onpe.endpoints import (
    ELECCION_PRESIDENCIAL,
    ELECCION_SENADORES_NACIONAL,
    acta_detalle,
    departamentos,
    distritos,
    distritos_electorales,
    id_acta,
    listar_actas,
    locales,
    mapa_calor_nacional,
    mesa_totales,
    participantes_nacional,
    proceso_activo,
    provincias,
    resumen_elecciones,
    totales_nacional,
    ultima_fecha,
)

OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "smoke"

# Fixture conocido: mesa 5507 en CAYMA (Arequipa-Arequipa), confirmado el 18-abr-2026.
UBIGEO_AREQUIPA = "040000"
UBIGEO_AREQUIPA_AREQUIPA = "040100"
UBIGEO_CAYMA = "040102"
MESA_FIXTURE = 5507
ID_ACTA_FIXTURE = 550704010210


async def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    config = ClientConfig(max_concurrent=3, rate_per_second=3.0)

    async with OnpeClient(config) as c:
        proc = await proceso_activo(c)
        _dump("proceso_activo.json", proc)
        id_proceso = proc["id"]

        res_elec = await resumen_elecciones(c, id_proceso)
        _dump("resumen_elecciones.json", res_elec)

        de = await distritos_electorales(c)
        _dump("distritos_electorales.json", de)

        totals = await totales_nacional(c, ELECCION_PRESIDENCIAL)
        _dump("totales_presidencial.json", totals)

        parts = await participantes_nacional(c, ELECCION_PRESIDENCIAL)
        _dump("participantes_presidencial.json", parts[:5])

        deptos = await departamentos(c, ELECCION_PRESIDENCIAL)
        _dump("deptos.json", deptos)

        provs = await provincias(c, ELECCION_PRESIDENCIAL, UBIGEO_AREQUIPA)
        _dump("provs_arequipa.json", provs)

        dists = await distritos(c, ELECCION_PRESIDENCIAL, UBIGEO_AREQUIPA_AREQUIPA)
        _dump("dists_arequipa_arequipa.json", dists)

        locs = await locales(c, UBIGEO_CAYMA)
        _dump("locales_cayma.json", locs[:5])

        mapa = await mapa_calor_nacional(c, ELECCION_PRESIDENCIAL)
        _dump("mapa_calor_nacional.json", mapa)

        mt = await mesa_totales(c)
        _dump("mesa_totales.json", mt)

        uf = await ultima_fecha(c)
        _dump("ultima_fecha.json", uf)

        # Listado paginado de actas — requiere los 6 filtros.
        codigo_local = locs[0]["codigoLocalVotacion"]
        actas_page = await listar_actas(
            c,
            id_eleccion=ELECCION_PRESIDENCIAL,
            ubigeo_dist=UBIGEO_CAYMA,
            codigo_local_votacion=codigo_local,
            pagina=0,
            tamanio=5,
        )
        _dump("actas_listado.json", actas_page)

        # Fórmula del idActa y detalle de la mesa-fixture en 2 elecciones.
        computed = id_acta(MESA_FIXTURE, UBIGEO_CAYMA, ELECCION_PRESIDENCIAL)
        assert computed == ID_ACTA_FIXTURE, (
            f"fórmula idActa rota: esperado {ID_ACTA_FIXTURE}, obtenido {computed}"
        )

        acta_presi = await acta_detalle(c, computed)
        _dump("acta_mesa5507_presi.json", acta_presi)
        assert acta_presi["codigoMesa"] == "005507", "idActa no devolvió la mesa esperada"

        acta_senado = await acta_detalle(
            c, id_acta(MESA_FIXTURE, UBIGEO_CAYMA, ELECCION_SENADORES_NACIONAL)
        )
        _dump("acta_mesa5507_senado_nac.json", acta_senado)
        assert acta_senado["idEleccion"] == ELECCION_SENADORES_NACIONAL

        summary = {
            "proceso": proc["acronimo"],
            "presidencial_pct_contabilizadas": totals["actasContabilizadas"],
            "mesas_instaladas": mt["mesasInstaladas"],
            "deptos_count": len(deptos),
            "arequipa_provincias": len(provs),
            "cayma_locales": len(locs),
            "acta_fixture_estado": acta_presi["descripcionEstadoActa"],
            "acta_fixture_votos_validos": acta_presi["totalVotosValidos"],
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))


def _dump(name: str, data: Any) -> None:
    (OUT_DIR / name).write_text(json.dumps(data, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
