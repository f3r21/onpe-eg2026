#!/usr/bin/env bash
# Instala los LaunchAgents ONPE en ~/Library/LaunchAgents/ y los carga.
# Reemplaza el shim `while true; do ... sleep 900; done` por launchd nativo.
#
# Uso:
#   bash ops/launchd/install.sh       # instalar + cargar
#   bash ops/launchd/install.sh stop  # unload
#   bash ops/launchd/install.sh status

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
LABELS=(com.onpe.aggregates com.onpe.datosabiertos)

cmd="${1:-install}"

case "$cmd" in
  install)
    mkdir -p "$LAUNCH_AGENTS_DIR"
    for label in "${LABELS[@]}"; do
      src="$SCRIPT_DIR/$label.plist"
      dst="$LAUNCH_AGENTS_DIR/$label.plist"
      cp "$src" "$dst"
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
