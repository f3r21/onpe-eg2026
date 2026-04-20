"""Cliente HTTP async para el API reverse-engineered de ONPE EG2026.

CloudFront rechaza cualquier request que no parezca originada del SPA:
devuelve el index.html del frontend como fallback silencioso (HTTP 200,
content-type text/html). Por eso replicamos headers exactos del navegador
y validamos el content-type de la respuesta.

Referencia: fuentes_datos.md, apéndice A.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

BASE_URL = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"

DEFAULT_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "referer": "https://resultadoelectoral.onpe.gob.pe/main/resumen",
    "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
}


class OnpeError(Exception):
    """Error no recuperable del cliente ONPE."""


class OnpeTransientError(OnpeError):
    """Error recuperable: timeout, 5xx, 429. Disparará retry exponencial."""


@dataclass(slots=True)
class ClientConfig:
    base_url: str = BASE_URL
    max_concurrent: int = 5
    rate_per_second: float = 5.0
    timeout_s: float = 30.0
    http2: bool = True


class OnpeClient:
    """HTTP client async con concurrencia controlada y rate-limit global.

    Uso:
        async with OnpeClient() as c:
            data = await c.get_json("/proceso/proceso-electoral-activo")
    """

    def __init__(self, config: ClientConfig | None = None) -> None:
        self.config = config or ClientConfig()
        self._client: httpx.AsyncClient | None = None
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._min_interval = 1.0 / self.config.rate_per_second
        self._last_request_at: float = 0.0
        self._rate_lock = asyncio.Lock()

    async def __aenter__(self) -> OnpeClient:
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            headers=DEFAULT_HEADERS,
            http2=self.config.http2,
            timeout=self.config.timeout_s,
        )
        return self

    async def __aexit__(self, *_exc: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _throttle(self) -> None:
        async with self._rate_lock:
            loop = asyncio.get_event_loop()
            delta = loop.time() - self._last_request_at
            if delta < self._min_interval:
                # Jitter ±10% del intervalo: suaviza el patrón temporal para
                # reducir chance de que CloudFront detecte comportamiento de
                # scraper regular (hardening R2). Rate efectivo promedio no
                # cambia apreciablemente.
                jitter = random.uniform(-0.1, 0.1) * self._min_interval
                await asyncio.sleep(max(0.0, self._min_interval - delta + jitter))
            self._last_request_at = loop.time()

    @retry(
        reraise=True,
        retry=retry_if_exception_type(OnpeTransientError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=30),
    )
    async def get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        if self._client is None:
            raise OnpeError("cliente no inicializado (usar 'async with OnpeClient()')")

        await self._throttle()
        async with self._semaphore:
            try:
                resp = await self._client.get(path, params=params)
            except httpx.TimeoutException as e:
                raise OnpeTransientError(f"timeout en {path}") from e
            except httpx.TransportError as e:
                raise OnpeTransientError(f"transport error en {path}: {e}") from e

        if resp.status_code in {429, 500, 502, 503, 504}:
            raise OnpeTransientError(f"{resp.status_code} en {path}")
        if resp.status_code >= 400:
            raise OnpeError(f"{resp.status_code} en {path}: {resp.text[:200]}")

        # 204 No Content: respuesta legítima y vacía del backend. Típicamente ocurre
        # en endpoints que no aplican al idEleccion solicitado (p.ej. participantes
        # nacionales para elecciones regionales). Mantenemos la forma {data: None}
        # para no romper el contrato de los wrappers.
        if resp.status_code == 204 or not resp.content:
            return {"data": None}

        ct = resp.headers.get("content-type", "")
        if "application/json" not in ct:
            raise OnpeError(
                f"respuesta no-JSON en {path} (content-type={ct!r}). "
                "CloudFront devolvió el fallback HTML del SPA — "
                "revisar headers sec-fetch-*, referer y user-agent."
            )

        return resp.json()
