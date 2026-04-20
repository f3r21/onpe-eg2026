"""Tests de onpe.geojson usando httpx.MockTransport."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from onpe.geojson import download_peru_low, load_peru_low


def _fake_peru_low_json() -> dict:
    return {
        "type": "FeatureCollection",
        "features": [
            {"id": f"{i:02d}0000", "type": "Feature", "properties": {"name": f"dept{i}"}}
            for i in range(1, 27)  # 26 deptos
        ],
    }


def _mock_transport(
    status_code: int, body: bytes, ct: str = "application/json"
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, content=body, headers={"content-type": ct})

    return httpx.MockTransport(handler)


def test_download_peru_low_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Descarga valida y persiste el archivo atómicamente."""
    dst = tmp_path / "peruLow.json"
    body = json.dumps(_fake_peru_low_json()).encode()

    # Monkeypatch httpx.Client para devolver nuestro transport
    original_client = httpx.Client

    def make_client(*args, **kwargs):
        kwargs["transport"] = _mock_transport(200, body)
        return original_client(*args, **kwargs)

    monkeypatch.setattr("onpe.geojson.httpx.Client", make_client)

    result = download_peru_low(dst)
    assert result == dst
    assert dst.exists()
    data = json.loads(dst.read_text())
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) == 26


def test_download_peru_low_skip_existing(tmp_path: Path):
    """Si el archivo ya existe y force=False, skippea sin request."""
    dst = tmp_path / "peruLow.json"
    dst.write_text('{"existing": true}')
    result = download_peru_low(dst, force=False)
    assert result == dst
    # No se sobrescribió
    assert json.loads(dst.read_text()) == {"existing": True}


def test_download_peru_low_html_fallback_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Content-type text/html → ValueError (CloudFront SPA fallback)."""
    dst = tmp_path / "peruLow.json"
    original_client = httpx.Client

    def make_client(*args, **kwargs):
        kwargs["transport"] = _mock_transport(200, b"<html></html>", ct="text/html")
        return original_client(*args, **kwargs)

    monkeypatch.setattr("onpe.geojson.httpx.Client", make_client)

    with pytest.raises(ValueError, match="no es JSON"):
        download_peru_low(dst, force=True)


def test_download_peru_low_too_few_features_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """GeoJSON con <20 features es sospechoso → ValueError."""
    dst = tmp_path / "peruLow.json"
    bad = {"type": "FeatureCollection", "features": [{"id": "x"} for _ in range(5)]}
    original_client = httpx.Client

    def make_client(*args, **kwargs):
        kwargs["transport"] = _mock_transport(200, json.dumps(bad).encode())
        return original_client(*args, **kwargs)

    monkeypatch.setattr("onpe.geojson.httpx.Client", make_client)

    with pytest.raises(ValueError, match="sospechoso"):
        download_peru_low(dst, force=True)


def test_load_peru_low_missing_raises(tmp_path: Path):
    """load_peru_low falla si el archivo no existe."""
    with pytest.raises(FileNotFoundError):
        load_peru_low(tmp_path / "no_existe.json")


def test_load_peru_low_ok(tmp_path: Path):
    """load_peru_low deserializa JSON válido."""
    path = tmp_path / "peruLow.json"
    path.write_text(json.dumps(_fake_peru_low_json()))
    data = load_peru_low(path)
    assert data["type"] == "FeatureCollection"
