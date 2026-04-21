"""Scraper de resoluciones oficiales del proceso EG2026 desde El Peruano.

Fuente: ``busquedas.elperuano.pe`` — buscador de Normas Legales del Diario
Oficial El Peruano. El endpoint GraphQL expuesto (`/api/graphql`) está
introspectable pero las queries paginadas (`getSearchNormasPaginated`,
`getNormasPaginated`, etc.) retornan error `'hits'` consistentemente — bug
server-side que no deja listar corpus por fechas.

**Alternativa funcional**: las páginas de detalle `/dispositivo/NL/{op}` sí
responden correctamente y embeden `__NEXT_DATA__` con el record completo:

- `fechaPublicacion` (YYYYMMDD)
- `tipoDispositivo` (RESOLUCION / LEY / DECRETO …)
- `nombreDispositivo` (ej. "N° 0126-2025-JNE")
- `sumilla` (descripción corta)
- `urlPDF`, `urlPortada`
- `sector`, `rubro` (categorización interna)

Para el dataset EG2026 v1.0 mantenemos un **registry YAML curado** en
`data/registry/resoluciones_eg2026.yaml` con los op-IDs landmark del proceso
(cronograma, reglamentos, cierre padrón, etc.). El scraper hidrata cada op
contra el endpoint detalle, descarga el PDF, y escribe `data/dim/resoluciones.parquet`.

Fallback: si el endpoint está caído o el op-ID no existe, se preserva la
metadata del YAML (con flag `url_ok=False`) para transparencia.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

BASE_URL = "https://busquedas.elperuano.pe"
DISPOSITIVO_PATH = "/dispositivo/NL/{op}"

# Validación del op_id (anti path-traversal): sólo alfanumérico + `.` + `-`.
# Ejemplos válidos: "2388220-1", "2501055-1". Inválidos: "../../evil", "op with space".
_OP_ID_RE = re.compile(r"^[\w.\-]+$")

# Tamaño mínimo razonable para PDF válido. Por debajo asumimos error de servidor.
_MIN_PDF_BYTES = 1024

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    "Referer": "https://busquedas.elperuano.pe/",
}

# Regex para extraer __NEXT_DATA__ del HTML — JSON embebido por Next.js.
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.+?)</script>',
    re.DOTALL,
)


@dataclass(frozen=True)
class Resolucion:
    """Registro normalizado de una resolución EG2026."""

    op_id: str  # ID del dispositivo en El Peruano (ej. "2388220-1")
    fecha_publicacion: str  # ISO YYYY-MM-DD
    institucion: str  # "JNE" | "ONPE" | "RENIEC" | "JEE" | …
    tipo_dispositivo: str  # "RESOLUCION" | "RESOLUCION JEFATURAL" | …
    nombre_dispositivo: str  # ej. "N° 0126-2025-JNE"
    sumilla: str  # descripción corta
    url_html: str  # página dispositivo en busquedas.elperuano.pe
    url_pdf: str  # PDF directo
    sector: str | None  # categorización interna
    rubro: str | None  # sub-categorización
    tag_proceso: str  # "EG2026" | "EG2026_PRIMARIAS" | …
    tag_categoria: str  # "cronograma" | "padron" | "reglamento" | …
    url_ok: bool  # True si el fetch respondió 200 y tenía metadata
    notas: str  # descripción del curador (del YAML)


def _safe_https_url(raw: object) -> str:
    """Retorna la URL sólo si comienza con https://, sino cadena vacía.

    Defensa contra SSRF: si El Peruano devolviera un esquema file:// u otro
    no-HTTP, el downloader podría leer archivos locales o hacer requests internos.
    """
    s = str(raw or "")
    return s if s.startswith("https://") else ""


def _yyyymmdd_to_iso(raw: str | None) -> str:
    """Convierte YYYYMMDD → YYYY-MM-DD; devuelve "" si malformado."""
    if not raw or len(raw) != 8 or not raw.isdigit():
        return ""
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def _infer_institucion(nombre_dispositivo: str) -> str:
    """Deriva la institución del nombre cuando el campo `institucion` viene vacío.

    `busquedas.elperuano.pe` a veces no pobla el campo institucion en el JSON;
    lo inferimos del sufijo del nombreDispositivo (ej. `N° 0126-2025-JNE`).
    """
    nombre_upper = nombre_dispositivo.upper()
    if "/ONPE" in nombre_upper or nombre_upper.endswith("ONPE"):
        return "ONPE"
    if "/JNE" in nombre_upper or nombre_upper.endswith("JNE"):
        return "JNE"
    if "RENIEC" in nombre_upper:
        return "RENIEC"
    if "/JEE" in nombre_upper:
        return "JEE"
    return ""


def fetch_dispositivo(
    op_id: str, *, client: httpx.Client | None = None, timeout: float = 30.0
) -> dict | None:
    """Fetch del HTML `/dispositivo/NL/{op}` y parse de `__NEXT_DATA__`.

    Retorna el dict `pageProps.dispositivo` o None si no se encuentra. Loggea
    cada rama de fallo con el motivo específico (status, estructura ausente,
    error de red) para facilitar diagnóstico sin tener que correr con debug.

    Valida `op_id` contra path traversal.
    """
    if not _OP_ID_RE.match(op_id):
        raise ValueError(f"op_id inválido (path traversal?): {op_id!r}")

    close_after = False
    if client is None:
        client = httpx.Client(headers=DEFAULT_HEADERS, timeout=timeout)
        close_after = True
    try:
        url = BASE_URL + DISPOSITIVO_PATH.format(op=op_id)
        log.info("GET %s", url)
        try:
            r = client.get(url)
        except httpx.HTTPError as e:
            log.warning("op=%s error de red (%s): %s", op_id, type(e).__name__, e)
            return None
        if r.status_code != 200:
            log.warning("op=%s status=%s", op_id, r.status_code)
            return None
        html = r.text
        m = _NEXT_DATA_RE.search(html)
        if not m:
            log.warning("op=%s sin __NEXT_DATA__", op_id)
            return None
        try:
            payload = json.loads(m.group(1))
        except json.JSONDecodeError as e:
            log.warning("op=%s __NEXT_DATA__ JSON inválido: %s", op_id, e)
            return None
        dispositivo = payload.get("props", {}).get("pageProps", {}).get("dispositivo")
        if dispositivo is None:
            # Estructura Next.js cambió (actualización del sitio). Log con keys
            # disponibles para diagnosticar qué migró.
            page_keys = list(payload.get("props", {}).get("pageProps", {}).keys())
            log.warning(
                "op=%s __NEXT_DATA__ parseado pero sin props.pageProps.dispositivo "
                "(estructura Next.js cambió?). Keys disponibles: %s",
                op_id,
                page_keys,
            )
        return dispositivo
    finally:
        if close_after:
            client.close()


def build_resolucion(
    op_id: str,
    entry_meta: dict,
    raw: dict | None,
) -> Resolucion:
    """Construye un `Resolucion` desde el dict del API + metadata del YAML.

    Si `raw` es None (fetch falló), rellena con la data del YAML y flaggea
    `url_ok=False`.
    """
    if raw is None:
        return Resolucion(
            op_id=op_id,
            fecha_publicacion=entry_meta.get("fecha_publicacion", ""),
            institucion=entry_meta.get("institucion", ""),
            tipo_dispositivo=entry_meta.get("tipo_dispositivo", ""),
            nombre_dispositivo=entry_meta.get("nombre_dispositivo", ""),
            sumilla=entry_meta.get("sumilla", ""),
            url_html=BASE_URL + DISPOSITIVO_PATH.format(op=op_id),
            url_pdf="",
            sector=None,
            rubro=None,
            tag_proceso=entry_meta.get("tag_proceso", "EG2026"),
            tag_categoria=entry_meta.get("tag_categoria", ""),
            url_ok=False,
            notas=entry_meta.get("notas", ""),
        )

    nombre = str(raw.get("nombreDispositivo") or "") or entry_meta.get("nombre_dispositivo", "")
    institucion = (
        (raw.get("institucion") or "").strip()
        or _infer_institucion(nombre)
        or entry_meta.get("institucion", "")
    )

    return Resolucion(
        op_id=op_id,
        fecha_publicacion=_yyyymmdd_to_iso(raw.get("fechaPublicacion"))
        or entry_meta.get("fecha_publicacion", ""),
        institucion=institucion,
        tipo_dispositivo=str(raw.get("tipoDispositivo") or "")
        or entry_meta.get("tipo_dispositivo", ""),
        nombre_dispositivo=nombre,
        sumilla=str(raw.get("sumilla") or "") or entry_meta.get("sumilla", ""),
        url_html=BASE_URL + DISPOSITIVO_PATH.format(op=op_id),
        # Sólo aceptamos url_pdf https:// para evitar SSRF si el API devolviera
        # un esquema file:// o similar.
        url_pdf=_safe_https_url(raw.get("urlPDF")),
        sector=str(raw.get("sector") or "") or None,
        rubro=str(raw.get("rubro") or "") or None,
        tag_proceso=entry_meta.get("tag_proceso", "EG2026"),
        tag_categoria=entry_meta.get("tag_categoria", ""),
        url_ok=True,
        notas=entry_meta.get("notas", ""),
    )


def download_pdf(
    url: str, out_file: Path, *, client: httpx.Client | None = None, timeout: float = 60.0
) -> int:
    """Descarga un PDF y lo guarda en `out_file`. Retorna bytes escritos.

    Rechaza URLs no-HTTPS (anti SSRF) y PDFs sospechosamente pequeños que
    típicamente indican un error de servidor disfrazado como 200 OK (página
    de error en vez del PDF real).
    """
    if not url.startswith("https://"):
        raise ValueError(f"url_pdf no https:// (rechazada): {url[:80]!r}")
    out_file.parent.mkdir(parents=True, exist_ok=True)
    close_after = False
    if client is None:
        client = httpx.Client(headers=DEFAULT_HEADERS, timeout=timeout, follow_redirects=True)
        close_after = True
    try:
        total = 0
        with client.stream("GET", url) as r:
            r.raise_for_status()
            with out_file.open("wb") as f:
                for chunk in r.iter_bytes(chunk_size=256 * 1024):
                    f.write(chunk)
                    total += len(chunk)
        if total < _MIN_PDF_BYTES:
            # Servidor devolvió 200 pero body muy pequeño → error disfrazado.
            out_file.unlink(missing_ok=True)
            raise ValueError(
                f"PDF sospechosamente pequeño ({total} bytes < {_MIN_PDF_BYTES}): {url}"
            )
        log.info("PDF descargado %s (%s bytes)", out_file.name, f"{total:,}")
        return total
    finally:
        if close_after:
            client.close()


def parse_registry_yaml(registry_path: Path) -> list[dict]:
    """Parsea el YAML curado del registry. Retorna lista de entradas."""
    try:
        import yaml
    except ImportError as e:  # pragma: no cover
        raise ImportError("pyyaml requerido. Instalar con `uv add pyyaml` o similar.") from e
    raw = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or "resoluciones" not in raw:
        raise ValueError(
            f"{registry_path} debe tener key raíz `resoluciones:` con lista de entradas"
        )
    entries = raw["resoluciones"]
    if not isinstance(entries, list):
        raise ValueError("`resoluciones` debe ser lista")
    for i, e in enumerate(entries):
        if not isinstance(e, dict) or "op" not in e:
            raise ValueError(f"entrada {i} sin key obligatoria `op`")
    return entries
