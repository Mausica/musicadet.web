#!/usr/bin/env bash
# MusicaDet HUD launcher — reads port from config.json
set -euo pipefail

INSTALL_DIR="/opt/musicadet"
cd "$INSTALL_DIR"

if ! python3 -c "from fastapi import FastAPI; import uvicorn" 2>/dev/null; then
  echo "ERROR: FastAPI/Uvicorn broken. Run setup.sh again or:" >&2
  echo "  pip3 install --upgrade --ignore-installed -r /opt/musicadet/requirements.txt --break-system-packages" >&2
  exit 1
fi

PORT="$(python3 -c "import json; print(json.load(open('config.json')).get('hud_port', 8800))")"

# Release stale bind on the HUD port (e.g. old music-sync-hud process)
if command -v fuser >/dev/null 2>&1; then
  fuser -k "${PORT}/tcp" 2>/dev/null || true
  sleep 1
fi

exec python3 -m uvicorn hud:app --host 0.0.0.0 --port "$PORT" --log-level info
