"""Genera `docs/cover.png` — imagen de portada para Kaggle/Zenodo/HuggingFace.

Usa matplotlib para componer un cover 1200×600 px con:
- Título del dataset
- Subtítulo (fecha + scope)
- 4 métricas clave destacadas
- Línea temporal del conteo (mini gráfico inspiracional)

Uso:
    uv run python scripts/generate_cover.py
    uv run python scripts/generate_cover.py --output custom.png
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

log = logging.getLogger("generate_cover")

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = REPO_ROOT / "docs" / "cover.png"


def render_cover(out_path: Path) -> None:
    """Render del cover usando matplotlib + rectangles/text."""
    try:
        import matplotlib.pyplot as plt
        from matplotlib.patches import FancyBboxPatch
    except ImportError as e:
        raise ImportError("matplotlib requerido. `uv add matplotlib` o equivalente.") from e

    # 1200×600 px → figura 12×6 pulgadas @ 100 dpi.
    fig = plt.figure(figsize=(12, 6), dpi=100)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 6)
    ax.axis("off")

    # Fondo gradiente simple (rectángulo + overlay semi-transparente).
    bg = FancyBboxPatch(
        (0, 0),
        12,
        6,
        boxstyle="round,pad=0",
        linewidth=0,
        facecolor="#0B1F3A",  # navy oscuro
        zorder=0,
    )
    ax.add_patch(bg)

    # Banda accent izquierda
    ax.add_patch(
        FancyBboxPatch(
            (0, 0),
            0.15,
            6,
            boxstyle="round,pad=0",
            linewidth=0,
            facecolor="#C6262E",  # rojo peruano
            zorder=1,
        )
    )

    # Título
    ax.text(
        0.5,
        5.0,
        "onpe-eg2026",
        fontsize=52,
        fontweight="bold",
        color="white",
        va="center",
        family="monospace",
    )
    ax.text(
        0.5,
        4.3,
        "Elecciones Generales Perú 2026 · Primera Vuelta",
        fontsize=22,
        color="#EAEAEA",
        va="center",
    )
    ax.text(
        0.5,
        3.85,
        "Dataset oficial reverse-engineered desde ONPE + RENIEC + El Peruano",
        fontsize=13,
        color="#A8B5C5",
        va="center",
        style="italic",
    )

    # Métricas clave (4 cajas)
    metricas = [
        ("463,830", "actas"),
        ("18.6M", "filas de votos"),
        ("27.23M", "electores (RENIEC)"),
        ("14/14", "DQ checks PASS"),
    ]
    box_w = 2.5
    gap = 0.35
    start_x = 0.5
    box_y = 1.4
    box_h = 1.8

    for i, (valor, label) in enumerate(metricas):
        x = start_x + i * (box_w + gap)
        ax.add_patch(
            FancyBboxPatch(
                (x, box_y),
                box_w,
                box_h,
                boxstyle="round,pad=0.02,rounding_size=0.15",
                linewidth=1.5,
                edgecolor="#3B5879",
                facecolor="#14304F",
            )
        )
        ax.text(
            x + box_w / 2,
            box_y + box_h * 0.60,
            valor,
            fontsize=30,
            fontweight="bold",
            color="white",
            ha="center",
            va="center",
        )
        ax.text(
            x + box_w / 2,
            box_y + box_h * 0.22,
            label,
            fontsize=13,
            color="#A8B5C5",
            ha="center",
            va="center",
        )

    # Footer
    ax.text(
        0.5,
        0.5,
        "github.com/f3r21/onpe-eg2026",
        fontsize=12,
        color="#8FA3BF",
        family="monospace",
        va="center",
    )
    ax.text(
        11.5,
        0.5,
        "CC-BY-4.0 · MIT (code)",
        fontsize=11,
        color="#8FA3BF",
        ha="right",
        va="center",
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=100, bbox_inches=None, pad_inches=0, facecolor="#0B1F3A")
    plt.close(fig)
    log.info("cover escrito %s (%.1f KB)", out_path, out_path.stat().st_size / 1024)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT, help="archivo PNG de salida")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    render_cover(args.output)
    print(f"cover: {args.output}")


if __name__ == "__main__":
    main()
