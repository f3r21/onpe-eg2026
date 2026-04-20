"""Monitor del catálogo ONPE en datosabiertos.gob.pe.

ONPE publica los resultados oficiales por mesa post-proclamación JNE
(típicamente 2-4 semanas después de la elección). La página es Drupal
con WAF Huawei Cloud — scraping ligero respetuoso (1 req cada vez,
User-Agent real) funciona; scraping agresivo dispara bloqueos.

**Uso**:
    from onpe.datosabiertos import list_onpe_datasets, find_eg2026

    datasets = list_onpe_datasets()
    eg2026 = find_eg2026(datasets)
    if eg2026:
        print(f"EG2026 publicado: {eg2026.url}")
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass

import httpx
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

BASE_URL = "https://www.datosabiertos.gob.pe"
ONPE_GROUP_PATH = "/group/oficina-nacional-de-procesos-electorales-onpe"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
}

# Patrones para reconocer datasets de EG2026 (Elecciones Generales 2026).
# Incluye variaciones comunes observadas en ONPE 2021 (vuelta1/vuelta2, presidenciales, etc.).
EG2026_PATTERNS = [
    re.compile(r"elecci\w+\s+generales?\s+20\s*26", re.IGNORECASE),
    re.compile(r"\beg\s*2026\b", re.IGNORECASE),
    re.compile(r"resultados?\s+.*\s+2026", re.IGNORECASE),
    re.compile(r"presidenciales?\s+2026", re.IGNORECASE),
    re.compile(r"congresales?\s+2026", re.IGNORECASE),
    re.compile(r"diputados?\s+2026", re.IGNORECASE),
    re.compile(r"senadores?\s+2026", re.IGNORECASE),
]


@dataclass(frozen=True)
class Dataset:
    """Metadata de un dataset del catálogo."""

    title: str
    url: str
    raw_date: str = ""

    def matches_eg2026(self) -> bool:
        """True si el título sugiere dataset de EG2026."""
        return any(p.search(self.title) for p in EG2026_PATTERNS)


def list_onpe_datasets(
    max_pages: int = 5,
    client: httpx.Client | None = None,
) -> list[Dataset]:
    """Lista los últimos N páginas de datasets del catálogo ONPE.

    El listado default muestra los 10 datasets más recientes por página.
    Con max_pages=5 cubre ~50 datasets, suficiente para ventana EG2026 cuando
    ONPE publique (típicamente aparece primero en page 1 por ser lo más nuevo).

    Raises:
        httpx.HTTPError si la request falla.
    """
    _client = client or httpx.Client(
        timeout=30, headers=DEFAULT_HEADERS, follow_redirects=True
    )
    results: list[Dataset] = []
    try:
        for page in range(max_pages):
            path = f"{ONPE_GROUP_PATH}?sort_by=changed"
            if page > 0:
                path += f"&page={page}"
            r = _client.get(f"{BASE_URL}{path}")
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            articles = soup.find_all("article")
            if not articles:
                break  # paginación agotada

            for art in articles:
                title_tag = art.find(["h2", "h3", "h4"])
                if not title_tag:
                    continue
                link = title_tag.find("a")
                if not link:
                    continue
                title = link.get_text(strip=True)
                href = link.get("href", "")
                date_tag = art.find(class_=lambda c: c and "date" in str(c).lower())
                date_str = date_tag.get_text(strip=True) if date_tag else ""
                if href.startswith("/"):
                    href = f"{BASE_URL}{href}"
                results.append(Dataset(title=title, url=href, raw_date=date_str))
    finally:
        if client is None:
            _client.close()
    return results


def find_eg2026(datasets: Iterable[Dataset]) -> Dataset | None:
    """Devuelve el primer dataset cuyo título matchea patrones de EG2026."""
    for d in datasets:
        if d.matches_eg2026():
            return d
    return None


def get_dataset_resources(url: str, client: httpx.Client | None = None) -> list[dict]:
    """Extrae los recursos descargables (CSVs, XLSX, ZIPs) de una página de dataset.

    Returns:
        Lista de dicts con keys: name, url, format, size.
    """
    _client = client or httpx.Client(
        timeout=30, headers=DEFAULT_HEADERS, follow_redirects=True
    )
    try:
        r = _client.get(url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        resources: list[dict] = []
        # Drupal patrón: <a class="data-link" ...> o .resource-item
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if any(href.lower().endswith(ext) for ext in (".csv", ".xlsx", ".zip", ".csv.gz")):
                resources.append({
                    "name": link.get_text(strip=True) or href.rsplit("/", 1)[-1],
                    "url": href if href.startswith("http") else f"{BASE_URL}{href}",
                    "format": href.rsplit(".", 1)[-1].lower(),
                })
        return resources
    finally:
        if client is None:
            _client.close()
