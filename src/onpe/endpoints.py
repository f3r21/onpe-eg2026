"""Wrappers tipados de los endpoints ONPE.

Referencia: fuentes_datos.md, apéndices A y B. Todos los paths son relativos
a BASE_URL del OnpeClient.
"""

from __future__ import annotations

from typing import Any

from onpe.client import OnpeClient

# idEleccion confirmados en vivo (18-abr-2026) vía /proceso/2/elecciones
ELECCION_PRESIDENCIAL = 10
ELECCION_PARLAMENTO_ANDINO = 12
ELECCION_DIPUTADOS = 13
ELECCION_SENADORES_REGIONAL = 14
ELECCION_SENADORES_NACIONAL = 15

ELECCIONES_NACIONALES: tuple[int, ...] = (
    ELECCION_PRESIDENCIAL,
    ELECCION_PARLAMENTO_ANDINO,
    ELECCION_SENADORES_NACIONAL,
)
ELECCIONES_REGIONALES: tuple[int, ...] = (
    ELECCION_DIPUTADOS,
    ELECCION_SENADORES_REGIONAL,
)

AMBITO_PERU = 1  # idAmbitoGeografico


# --- Catálogos y jerarquía geográfica ---------------------------------------


async def proceso_activo(c: OnpeClient) -> dict[str, Any]:
    return (await c.get_json("/proceso/proceso-electoral-activo"))["data"]


async def elecciones_menu(c: OnpeClient, id_proceso: int) -> list[dict[str, Any]]:
    """Menú de navegación del SPA (mapea idEleccion a la ruta del frontend)."""
    return (await c.get_json(f"/proceso/{id_proceso}/elecciones"))["data"]


async def distritos_electorales(c: OnpeClient) -> list[dict[str, Any]]:
    """27 distritos electorales (26 departamentos + 27 = residentes en el extranjero)."""
    return (await c.get_json("/distrito-electoral/distritos"))["data"]


async def departamentos(c: OnpeClient, id_eleccion: int) -> list[dict[str, Any]]:
    return (
        await c.get_json(
            "/ubigeos/departamentos",
            params={"idEleccion": id_eleccion, "idAmbitoGeografico": AMBITO_PERU},
        )
    )["data"]


async def provincias(
    c: OnpeClient, id_eleccion: int, ubigeo_depto: str
) -> list[dict[str, Any]]:
    return (
        await c.get_json(
            "/ubigeos/provincias",
            params={
                "idEleccion": id_eleccion,
                "idAmbitoGeografico": AMBITO_PERU,
                "idUbigeoDepartamento": ubigeo_depto,
            },
        )
    )["data"]


async def distritos(
    c: OnpeClient, id_eleccion: int, ubigeo_prov: str
) -> list[dict[str, Any]]:
    return (
        await c.get_json(
            "/ubigeos/distritos",
            params={
                "idEleccion": id_eleccion,
                "idAmbitoGeografico": AMBITO_PERU,
                "idUbigeoProvincia": ubigeo_prov,
            },
        )
    )["data"]


async def locales(c: OnpeClient, ubigeo_dist: str) -> list[dict[str, Any]]:
    return (
        await c.get_json("/ubigeos/locales", params={"idUbigeo": ubigeo_dist})
    )["data"]


# --- Totales y participantes ------------------------------------------------


async def totales_nacional(c: OnpeClient, id_eleccion: int) -> dict[str, Any]:
    return (
        await c.get_json(
            "/resumen-general/totales",
            params={"idEleccion": id_eleccion, "tipoFiltro": "eleccion"},
        )
    )["data"]


async def totales_distrito_electoral(
    c: OnpeClient, id_eleccion: int, id_distrito_electoral: int
) -> dict[str, Any]:
    return (
        await c.get_json(
            "/resumen-general/totales",
            params={
                "idEleccion": id_eleccion,
                "idAmbitoGeografico": AMBITO_PERU,
                "tipoFiltro": "distrito_electoral",
                "idDistritoElectoral": id_distrito_electoral,
            },
        )
    )["data"]


async def participantes_nacional(
    c: OnpeClient, id_eleccion: int
) -> list[dict[str, Any]]:
    return (
        await c.get_json(
            "/resumen-general/participantes",
            params={"idEleccion": id_eleccion, "tipoFiltro": "eleccion"},
        )
    )["data"]


async def participantes_distrito_electoral(
    c: OnpeClient, id_eleccion: int, id_distrito_electoral: int
) -> list[dict[str, Any]]:
    return (
        await c.get_json(
            "/resumen-general/participantes",
            params={
                "idEleccion": id_eleccion,
                "idAmbitoGeografico": AMBITO_PERU,
                "tipoFiltro": "distrito_electoral",
                "idDistritoElectoral": id_distrito_electoral,
            },
        )
    )["data"]


async def resumen_elecciones(
    c: OnpeClient, id_proceso: int
) -> list[dict[str, Any]]:
    """Avance agregado por tipo de elección (una fila por cada uno de los 5 idEleccion)."""
    return (
        await c.get_json(
            "/resumen-general/elecciones",
            params={
                "activo": 1,
                "idProceso": id_proceso,
                "tipoFiltro": "eleccion",
            },
        )
    )["data"]


# --- Mapa de calor (avance del conteo jerárquico) --------------------------


async def mapa_calor_nacional(
    c: OnpeClient, id_eleccion: int
) -> list[dict[str, Any]]:
    """Una fila por departamento."""
    return (
        await c.get_json(
            "/resumen-general/mapa-calor",
            params={
                "idAmbitoGeografico": AMBITO_PERU,
                "idEleccion": id_eleccion,
                "tipoFiltro": "ambito_geografico",
            },
        )
    )["data"]


async def mapa_calor_departamento(
    c: OnpeClient, id_eleccion: int, ubigeo_depto: str
) -> list[dict[str, Any]]:
    """Una fila por provincia dentro del departamento."""
    return (
        await c.get_json(
            "/resumen-general/mapa-calor",
            params={
                "idAmbitoGeografico": AMBITO_PERU,
                "idEleccion": id_eleccion,
                "ubigeoNivel01": ubigeo_depto,
                "tipoFiltro": "ubigeo_nivel_01",
            },
        )
    )["data"]


async def mapa_calor_provincia(
    c: OnpeClient,
    id_eleccion: int,
    ubigeo_depto: str,
    ubigeo_prov: str,
) -> list[dict[str, Any]]:
    """Una fila por distrito dentro de la provincia."""
    return (
        await c.get_json(
            "/resumen-general/mapa-calor",
            params={
                "idAmbitoGeografico": AMBITO_PERU,
                "idEleccion": id_eleccion,
                "ubigeoNivel01": ubigeo_depto,
                "ubigeoNivel02": ubigeo_prov,
                "tipoFiltro": "ubigeo_nivel_02",
            },
        )
    )["data"]


# --- Agregados auxiliares ---------------------------------------------------


async def mesa_totales(c: OnpeClient) -> dict[str, Any]:
    """Conteo nacional de mesas instaladas / no instaladas / pendientes."""
    return (
        await c.get_json("/mesa/totales", params={"tipoFiltro": "eleccion"})
    )["data"]


async def ultima_fecha(c: OnpeClient) -> dict[str, Any]:
    """Última sincronización del backend (epoch ms en data.fechaProceso)."""
    return (await c.get_json("/fecha/listarFecha"))["data"]


# --- Actas -------------------------------------------------------------------


async def listar_actas(
    c: OnpeClient,
    id_eleccion: int,
    ubigeo_dist: str,
    codigo_local_votacion: int,
    pagina: int = 0,
    tamanio: int = 100,
) -> dict[str, Any]:
    """Listado paginado de actas.

    Todos los params son obligatorios: si falta alguno la API devuelve
    `content: []` silenciosamente (no es error, es diseño del endpoint).
    """
    return (
        await c.get_json(
            "/actas",
            params={
                "pagina": pagina,
                "tamanio": tamanio,
                "idAmbitoGeografico": AMBITO_PERU,
                "idEleccion": id_eleccion,
                "idUbigeo": ubigeo_dist,
                "idLocalVotacion": codigo_local_votacion,
            },
        )
    )["data"]


def id_acta(id_mesa: int, ubigeo_distrito: str, id_eleccion: int) -> int:
    """Construye el idActa determinísticamente. Ver fuentes_datos.md §B.4.

    Fórmula: pad(idMesa, 4) ++ pad(ubigeoDistrito, 6) ++ pad(idEleccion, 2).
    Ejemplo: id_acta(5507, "040102", 10) == 550704010210.
    """
    return int(f"{id_mesa:04d}{int(ubigeo_distrito):06d}{id_eleccion:02d}")


async def acta_detalle(c: OnpeClient, acta_id: int) -> dict[str, Any]:
    """Detalle completo de una mesa-elección.

    Si el idActa no corresponde a una mesa existente, devuelve 200 OK con
    todos los campos en null. Validar con `data["codigoMesa"] is not None`.
    """
    return (await c.get_json(f"/actas/{acta_id}"))["data"]
