#!/usr/bin/env bash
# Pull ultima versiune din GitHub și re-rulează setup-ul (idempotent).
set -euo pipefail

REPO_DIR="/opt/musicadet"
APP_DIR="/opt/music-sync"

echo "==> git pull în $REPO_DIR"
git -C "$REPO_DIR" pull --ff-only

echo "==> Re-rulez setup.sh (deps + systemd reload + HUD restart)"
cd "$APP_DIR"
chmod +x setup.sh
bash setup.sh

echo "✅ Update complet."
