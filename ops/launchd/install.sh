#!/usr/bin/env bash
# Instala los LaunchAgents ONPE en ~/Library/LaunchAgents/ y los carga.
# Reemplaza el shim `while true; do ... sleep 900; done` por launchd nativo.
#
# Los plists se generan desde .template renderizando 2 variables:
#   __REPO_ROOT__  → ruta absoluta del repo (detectada automáticamente)
#   __UV_BIN__     → ruta absoluta a `uv` (detectada con `which uv`)
#   __UV_DIR__     → dirname de __UV_BIN__ (para agregar al PATH)
#
# Uso:
#   bash ops/launchd/install.sh       # instalar + cargar
#   bash ops/launchd/install.sh stop  # unload
#   bash ops/launchd/install.sh status

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
LABELS=(com.onpe.aggregates com.onpe.datosabiertos)

cmd="${1:-install}"

detect_uv() {
  local uv_bin
  uv_bin="$(command -v uv 2>/dev/null)" || {
    echo "ERROR: no se encontró 'uv' en PATH. Instalar: brew install uv" >&2
    exit 1
  }
  echo "$uv_bin"
}

render_plist() {
  local template="$1"
  local dst="$2"
  local uv_bin="$3"
  local uv_dir
  uv_dir="$(dirname "$uv_bin")"
  sed \
    -e "s|__REPO_ROOT__|$REPO_ROOT|g" \
    -e "s|__UV_BIN__|$uv_bin|g" \
    -e "s|__UV_DIR__|$uv_dir|g" \
    "$template" > "$dst"
}

case "$cmd" in
  install)
    mkdir -p "$LAUNCH_AGENTS_DIR" "$REPO_ROOT/logs"
    uv_bin="$(detect_uv)"
    echo "Rendering con REPO_ROOT=$REPO_ROOT UV=$uv_bin"
    for label in "${LABELS[@]}"; do
      template="$SCRIPT_DIR/$label.plist.template"
      dst="$LAUNCH_AGENTS_DIR/$label.plist"
      if [ ! -f "$template" ]; then
        echo "ERROR: template no encontrado: $template" >&2
        exit 1
      fi
      render_plist "$template" "$dst" "$uv_bin"
      # Si ya está cargado, unload primero (idempotencia)
      launchctl unload "$dst" 2>/dev/null || true
      launchctl load "$dst"
      echo "loaded: $label"
    done
    echo ""
    echo "--- status ---"
    launchctl list | grep onpe || echo "(no jobs todavía)"
    ;;
  stop|unload)
    for label in "${LABELS[@]}"; do
      dst="$LAUNCH_AGENTS_DIR/$label.plist"
      if [ -f "$dst" ]; then
        launchctl unload "$dst" 2>/dev/null && echo "unloaded: $label"
      fi
    done
    ;;
  status)
    echo "=== LaunchAgents loaded ==="
    launchctl list | grep onpe || echo "(no hay onpe LaunchAgents cargados)"
    echo ""
    echo "=== Plists instalados ==="
    ls -la "$LAUNCH_AGENTS_DIR"/com.onpe.*.plist 2>/dev/null || echo "(no instalados)"
    ;;
  *)
    echo "uso: $0 [install|stop|status]"
    exit 1
    ;;
esac
