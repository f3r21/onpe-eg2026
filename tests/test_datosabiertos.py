"""Tests de onpe.datosabiertos scraper (usando httpx.MockTransport)."""

from __future__ import annotations

import httpx

from onpe.datosabiertos import (
    Dataset,
    find_eg2026,
    list_onpe_datasets,
)


def _make_listing_html(datasets: list[tuple[str, str]]) -> bytes:
    """Construye HTML Drupal-like con una lista de artículos."""
    articles = "".join(
        f'<article><h2><a href="{href}">{title}</a></h2>'
        f'<div class="date-display-single">2026-01-15</div></article>'
        for title, href in datasets
    )
    return f"""<!DOCTYPE html><html><body>
        <div class="view-content">
        {articles}
        </div></body></html>""".encode()


def _make_mock_client(body: bytes, status: int = 200) -> httpx.Client:
    """httpx.Client con MockTransport que devuelve body fijo."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status, content=body, headers={"content-type": "text/html; charset=utf-8"}
        )
    return httpx.Client(transport=httpx.MockTransport(handler), timeout=5)


def test_dataset_matches_eg2026_positive():
    d = Dataset(title="Resultados por mesa Elecciones Generales 2026", url="/x")
    assert d.matches_eg2026()


def test_dataset_matches_eg2026_negative():
    d = Dataset(title="Consulta Popular Revocatoria 2025", url="/x")
    assert not d.matches_eg2026()


def test_dataset_matches_eg2026_variations():
    """Diversos patrones que deben matchear."""
    for title in [
        "Resultados mesa EG 2026 Primera vuelta",
        "Presidenciales 2026 ONPE",
        "Diputados 2026 — Oficina Nacional",
        "Senadores 2026",
        "Elecciones Generales 2026 — Primera Vuelta",
    ]:
        d = Dataset(title=title, url="/x")
        assert d.matches_eg2026(), f"should match: {title}"


def test_dataset_matches_eg2026_no_match_otros_años():
    for title in ["Elecciones 2021", "Resultados 2023", "Consulta 2028"]:
        d = Dataset(title=title, url="/x")
        assert not d.matches_eg2026(), f"should NOT match: {title}"


def test_list_onpe_datasets_parses_articles():
    html = _make_listing_html([
        ("Resultados por mesa EG2026", "/dataset/eg2026"),
        ("Consulta Popular 2025", "/dataset/consulta-2025"),
    ])
    client = _make_mock_client(html)
    # Single page to avoid pagination complexity
    datasets = list_onpe_datasets(max_pages=1, client=client)
    assert len(datasets) == 2
    titles = [d.title for d in datasets]
    assert "Resultados por mesa EG2026" in titles
    assert datasets[0].url.startswith("https://www.datosabiertos.gob.pe/")


def test_list_onpe_datasets_empty_cataloga_breaks_pagination():
    """Si una página no tiene artículos, el loop termina (pagination agotada)."""
    client = _make_mock_client(b"<html><body></body></html>")
    datasets = list_onpe_datasets(max_pages=3, client=client)
    assert datasets == []


def test_find_eg2026_returns_first_match():
    datasets = [
        Dataset(title="Consulta 2025", url="/a"),
        Dataset(title="Elecciones Generales 2026 Presidencial", url="/b"),
        Dataset(title="Elecciones Generales 2026 Diputados", url="/c"),
    ]
    found = find_eg2026(datasets)
    assert found is not None
    assert found.url == "/b"


def test_find_eg2026_returns_none_if_no_match():
    datasets = [Dataset(title="Otra cosa", url="/x")]
    assert find_eg2026(datasets) is None
