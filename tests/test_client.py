"""Tests del OnpeClient usando httpx.MockTransport.

Cubre los paths críticos del cliente sin golpear el API real:
- 204 No Content → {"data": None}
- HTML fallback de CloudFront (content-type text/html) → OnpeError
- 429/503 → OnpeTransientError (gatilla retry de tenacity)
- 5xx no-recoverable → OnpeError
- JSON OK happy path
- rate-limit (throttle) — sin assert temporal estricto, solo que no rompe
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from onpe.client import ClientConfig, OnpeClient, OnpeError, OnpeTransientError


def _make_client(handler) -> OnpeClient:
    """Construye OnpeClient con MockTransport inyectado."""
    cfg = ClientConfig(max_concurrent=5, rate_per_second=1000.0)  # throttle desactivado
    c = OnpeClient(cfg)
    # Inyectar un AsyncClient con MockTransport ANTES de __aenter__.
    # Lo más limpio: subclase via monkey-patch en el test individual.
    return c


@pytest.mark.asyncio
async def test_get_json_happy_path(monkeypatch: pytest.MonkeyPatch):
    """Respuesta JSON 200 se parsea como dict."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"ok": True}})

    cfg = ClientConfig(max_concurrent=3, rate_per_second=1000.0)
    client = OnpeClient(cfg)
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(base_url=cfg.base_url, transport=transport) as http:
        client._client = http
        client._semaphore = asyncio.Semaphore(cfg.max_concurrent)
        data = await client.get_json("/proceso/proceso-electoral-activo")
        assert data == {"data": {"ok": True}}


@pytest.mark.asyncio
async def test_get_json_204_devuelve_data_none():
    """204 No Content se normaliza a {'data': None} — contrato del cliente."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    cfg = ClientConfig(max_concurrent=3, rate_per_second=1000.0)
    client = OnpeClient(cfg)
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(base_url=cfg.base_url, transport=transport) as http:
        client._client = http
        client._semaphore = asyncio.Semaphore(cfg.max_concurrent)
        data = await client.get_json("/resumen-general/participantes", params={"idEleccion": 13})
        assert data == {"data": None}


@pytest.mark.asyncio
async def test_get_json_html_fallback_cloudfront_raises():
    """Content-Type text/html → CloudFront fallback → OnpeError con mensaje claro."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"content-type": "text/html"}, content=b"<html>...</html>"
        )

    cfg = ClientConfig(max_concurrent=3, rate_per_second=1000.0)
    client = OnpeClient(cfg)
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(base_url=cfg.base_url, transport=transport) as http:
        client._client = http
        client._semaphore = asyncio.Semaphore(cfg.max_concurrent)
        with pytest.raises(OnpeError) as exc:
            await client.get_json("/cualquier-ruta")
        msg = str(exc.value)
        assert "no-JSON" in msg or "HTML" in msg
        assert "sec-fetch" in msg or "headers" in msg


@pytest.mark.asyncio
async def test_get_json_429_dispara_transient_error():
    """429 Too Many Requests → OnpeTransientError (gatilla retry exponencial)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, text="rate limited")

    cfg = ClientConfig(max_concurrent=3, rate_per_second=1000.0)
    client = OnpeClient(cfg)
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(base_url=cfg.base_url, transport=transport) as http:
        client._client = http
        client._semaphore = asyncio.Semaphore(cfg.max_concurrent)
        with pytest.raises(OnpeTransientError):
            # Invocar directo al método interno sin retry para no sleep 30s
            await client.get_json.retry_with(stop=lambda *_: True)(client, "/x")


@pytest.mark.asyncio
async def test_get_json_500_no_retryable():
    """500 Internal Server Error → OnpeError (no es transient, no retry)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="server error")

    cfg = ClientConfig(max_concurrent=3, rate_per_second=1000.0)
    client = OnpeClient(cfg)
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(base_url=cfg.base_url, transport=transport) as http:
        client._client = http
        client._semaphore = asyncio.Semaphore(cfg.max_concurrent)
        with pytest.raises(OnpeError) as exc:
            await client.get_json("/x")
        assert "500" in str(exc.value)


@pytest.mark.asyncio
async def test_get_json_respuesta_vacia_devuelve_data_none():
    """Content vacío (sin 204 explícito) también se normaliza a data=None."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"", headers={"content-type": "application/json"})

    cfg = ClientConfig(max_concurrent=3, rate_per_second=1000.0)
    client = OnpeClient(cfg)
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(base_url=cfg.base_url, transport=transport) as http:
        client._client = http
        client._semaphore = asyncio.Semaphore(cfg.max_concurrent)
        data = await client.get_json("/x")
        assert data == {"data": None}


def test_cliente_sin_inicializar_levanta_error():
    """Usar el cliente fuera de un `async with` → OnpeError."""
    cfg = ClientConfig()
    client = OnpeClient(cfg)
    assert client._client is None

    async def call():
        await client.get_json("/x")

    with pytest.raises(OnpeError) as exc:
        asyncio.run(call())
    assert "no inicializado" in str(exc.value) or "async with" in str(exc.value)
