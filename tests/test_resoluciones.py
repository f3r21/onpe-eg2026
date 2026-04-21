"""Tests de onpe.resoluciones: parser registry + normalization + fallback."""

from __future__ import annotations

import textwrap
from pathlib import Path

import httpx
import pytest

from onpe.resoluciones import (
    BASE_URL,
    DISPOSITIVO_PATH,
    Resolucion,
    _infer_institucion,
    _safe_https_url,
    _yyyymmdd_to_iso,
    build_resolucion,
    download_pdf,
    fetch_dispositivo,
    parse_registry_yaml,
)

# ──────────────────────────────────────────────────────────────────────
#  _yyyymmdd_to_iso
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("20250407", "2025-04-07"),
        ("20260412", "2026-04-12"),
        ("", ""),
        (None, ""),
        ("2025-04-07", ""),  # ya formateado → rejecta
        ("invalid", ""),
        ("202504", ""),  # longitud incorrecta
    ],
)
def test_yyyymmdd_to_iso(raw, expected):
    assert _yyyymmdd_to_iso(raw) == expected


# ──────────────────────────────────────────────────────────────────────
#  _infer_institucion
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "nombre,expected",
    [
        ("N° 0126-2025-JNE", "JNE"),
        ("N° 000063-2025-JN/ONPE", "ONPE"),
        ("N° 000029-2026-JN/ONPE", "ONPE"),
        ("N° 000039-2026/JNAC/RENIEC", "RENIEC"),
        ("Res. ADM-0001-2026/JEE-LIMA", "JEE"),
        ("LEY N° 32264", ""),
        ("", ""),
    ],
)
def test_infer_institucion(nombre, expected):
    assert _infer_institucion(nombre) == expected


# ──────────────────────────────────────────────────────────────────────
#  parse_registry_yaml
# ──────────────────────────────────────────────────────────────────────


def test_parse_registry_valid(tmp_path: Path):
    yaml_content = textwrap.dedent(
        """\
        resoluciones:
          - op: "2388220-1"
            tag_proceso: "EG2026"
            tag_categoria: "cronograma"
            institucion: "JNE"
            notas: "landmark"
          - op: "2391947-1"
            tag_proceso: "EG2026_PRIMARIAS"
            tag_categoria: "reglamento_primarias"
        """
    )
    registry = tmp_path / "registry.yaml"
    registry.write_text(yaml_content)
    entries = parse_registry_yaml(registry)
    assert len(entries) == 2
    assert entries[0]["op"] == "2388220-1"
    assert entries[0]["institucion"] == "JNE"
    assert entries[1]["tag_categoria"] == "reglamento_primarias"


def test_parse_registry_rejects_missing_op(tmp_path: Path):
    yaml_content = textwrap.dedent(
        """\
        resoluciones:
          - tag_proceso: "EG2026"
        """
    )
    registry = tmp_path / "registry.yaml"
    registry.write_text(yaml_content)
    with pytest.raises(ValueError, match="op"):
        parse_registry_yaml(registry)


def test_parse_registry_rejects_missing_root_key(tmp_path: Path):
    registry = tmp_path / "registry.yaml"
    registry.write_text("normas: []")
    with pytest.raises(ValueError, match="resoluciones"):
        parse_registry_yaml(registry)


def test_parse_registry_rejects_non_list(tmp_path: Path):
    registry = tmp_path / "registry.yaml"
    registry.write_text("resoluciones: foo")
    with pytest.raises(ValueError, match="lista"):
        parse_registry_yaml(registry)


# ──────────────────────────────────────────────────────────────────────
#  build_resolucion — merge raw (API) + entry_meta (YAML)
# ──────────────────────────────────────────────────────────────────────


def _make_raw(**overrides):
    base = {
        "fechaPublicacion": "20250407",
        "tipoDispositivo": "RESOLUCION",
        "nombreDispositivo": "N° 0126-2025-JNE",
        "sumilla": "Aprueban el cronograma electoral",
        "urlPDF": "https://busquedas.elperuano.pe/api/media/x.PDF",
        "institucion": "JNE",
        "sector": "1986",
        "rubro": "606",
    }
    base.update(overrides)
    return base


def test_build_resolucion_hidratada():
    entry = {
        "op": "2388220-1",
        "tag_proceso": "EG2026",
        "tag_categoria": "cronograma",
        "notas": "landmark",
    }
    r = build_resolucion("2388220-1", entry, _make_raw())
    assert r.op_id == "2388220-1"
    assert r.fecha_publicacion == "2025-04-07"
    assert r.institucion == "JNE"
    assert r.nombre_dispositivo == "N° 0126-2025-JNE"
    assert r.tag_proceso == "EG2026"
    assert r.tag_categoria == "cronograma"
    assert r.url_ok is True
    assert r.url_html == BASE_URL + DISPOSITIVO_PATH.format(op="2388220-1")
    assert r.url_pdf.endswith(".PDF")


def test_build_resolucion_fallback_institucion_desde_nombre():
    """Si raw['institucion']=='', inferir del nombreDispositivo."""
    entry = {"op": "2391947-1", "tag_proceso": "EG2026_PRIMARIAS", "tag_categoria": "x"}
    r = build_resolucion(
        "2391947-1",
        entry,
        _make_raw(institucion="", nombreDispositivo="N° 000063-2025-JN/ONPE"),
    )
    assert r.institucion == "ONPE"


def test_build_resolucion_fallback_institucion_desde_yaml():
    """Si raw y nombre no dan institucion, caer al YAML."""
    entry = {
        "op": "2447328-1",
        "tag_proceso": "EG2026",
        "tag_categoria": "padron",
        "institucion": "RENIEC",
    }
    r = build_resolucion("2447328-1", entry, _make_raw(institucion="", nombreDispositivo=""))
    assert r.institucion == "RENIEC"


def test_build_resolucion_raw_none_preserva_metadata_yaml():
    """Si fetch falla, retorna Resolucion con fallback y url_ok=False."""
    entry = {
        "op": "9999999-1",
        "tag_proceso": "EG2026",
        "tag_categoria": "otros",
        "fecha_publicacion": "2026-01-01",
        "institucion": "ONPE",
        "nombre_dispositivo": "RJ fallback",
        "sumilla": "resolución de prueba",
        "notas": "no existe en El Peruano",
    }
    r = build_resolucion("9999999-1", entry, None)
    assert r.op_id == "9999999-1"
    assert r.url_ok is False
    assert r.institucion == "ONPE"
    assert r.nombre_dispositivo == "RJ fallback"
    assert r.sumilla == "resolución de prueba"
    assert r.url_pdf == ""
    assert r.url_html == BASE_URL + DISPOSITIVO_PATH.format(op="9999999-1")


# ──────────────────────────────────────────────────────────────────────
#  fetch_dispositivo — MockTransport (sin red real)
# ──────────────────────────────────────────────────────────────────────


def _make_html_with_next_data(payload: dict) -> str:
    import json as _json

    body = _json.dumps({"props": {"pageProps": {"dispositivo": payload}}})
    return (
        f"<!DOCTYPE html><html><head></head><body>"
        f'<script id="__NEXT_DATA__" type="application/json">{body}</script>'
        f"</body></html>"
    )


def test_fetch_dispositivo_extrae_next_data():
    payload = _make_raw()
    html = _make_html_with_next_data(payload)

    def handler(request: httpx.Request) -> httpx.Response:
        assert "2388220-1" in str(request.url)
        return httpx.Response(200, content=html.encode(), headers={"content-type": "text/html"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        raw = fetch_dispositivo("2388220-1", client=client)

    assert raw is not None
    assert raw["nombreDispositivo"] == "N° 0126-2025-JNE"
    assert raw["institucion"] == "JNE"


def test_fetch_dispositivo_status_non_200_retorna_none():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, content=b"not found")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        raw = fetch_dispositivo("bad-op", client=client)
    assert raw is None


def test_fetch_dispositivo_html_sin_next_data_retorna_none():
    html = "<html><body>no next data here</body></html>"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=html.encode())

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        raw = fetch_dispositivo("op", client=client)
    assert raw is None


# ──────────────────────────────────────────────────────────────────────
#  Resolucion dataclass — frozen / immutable
# ──────────────────────────────────────────────────────────────────────


def test_resolucion_es_inmutable():
    r = Resolucion(
        op_id="x",
        fecha_publicacion="2026-04-12",
        institucion="ONPE",
        tipo_dispositivo="RJ",
        nombre_dispositivo="x",
        sumilla="x",
        url_html="x",
        url_pdf="x",
        sector=None,
        rubro=None,
        tag_proceso="EG2026",
        tag_categoria="x",
        url_ok=True,
        notas="x",
    )
    with pytest.raises(AttributeError):
        r.institucion = "JNE"  # type: ignore[misc]


# ──────────────────────────────────────────────────────────────────────
#  Validaciones de seguridad (audit pre-release)
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "bad_op",
    [
        "../../evil",
        "op with space",
        "op/slash",
        "op?query=1",
        "op#frag",
        "",
    ],
)
def test_fetch_dispositivo_rechaza_op_id_path_traversal(bad_op):
    """op_id con caracteres peligrosos debe abortar antes del GET."""
    with pytest.raises(ValueError, match="op_id inválido"):
        fetch_dispositivo(bad_op)


@pytest.mark.parametrize(
    "raw,expected",
    [
        (
            "https://busquedas.elperuano.pe/api/file.PDF",
            "https://busquedas.elperuano.pe/api/file.PDF",
        ),
        ("http://insecure.example.com/file.pdf", ""),
        ("file:///etc/passwd", ""),
        ("", ""),
        (None, ""),
        ("javascript:alert(1)", ""),
    ],
)
def test_safe_https_url(raw, expected):
    assert _safe_https_url(raw) == expected


def test_download_pdf_rechaza_url_no_https(tmp_path):
    with pytest.raises(ValueError, match="no https://"):
        download_pdf("http://insecure.example.com/x.pdf", tmp_path / "out.pdf")


def test_download_pdf_rechaza_pdf_sospechosamente_pequeno(tmp_path):
    """Servidor devuelve 200 con body <1024 bytes → abort + archivo borrado."""

    def handler(request: httpx.Request) -> httpx.Response:
        # 500 bytes — debajo del mínimo 1024.
        return httpx.Response(200, content=b"x" * 500)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        out = tmp_path / "mini.pdf"
        with pytest.raises(ValueError, match="sospechosamente pequeño"):
            download_pdf("https://mock.test/x.pdf", out, client=client)
    # El archivo parcial debió borrarse.
    assert not out.exists()
