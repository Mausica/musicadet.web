#!/usr/bin/env bash
# One-shot installer pentru MusicaDet.web (Spotify -> Jellyfin sync + HUD web).
# Rulează ca root pe serverul Jellyfin (LXC):
#   bash <(curl -fsSL https://raw.githubusercontent.com/Mausica/musicadet.web/main/install.sh)
#
# Ce face:
#   1. instalează git/curl (dacă lipsesc)
#   2. clonează / actualizează repo în /opt/musicadet
#   3. linkează /opt/music-sync -> /opt/musicadet
#   4. rulează setup.sh (deps + systemd + HUD pe port 8800)

set -euo pipefail

REPO_URL="https://github.com/Mausica/musicadet.web.git"
REPO_DIR="/opt/musicadet"
APP_DIR="/opt/music-sync"

if [[ $EUID -ne 0 ]]; then
  echo "Rulează ca root (sudo -i)." >&2
  exit 1
fi

echo "==> Instalez prerechizite (git, curl)…"
apt-get update -qq
apt-get install -y -qq git curl ca-certificates >/dev/null

if [[ -d "$REPO_DIR/.git" ]]; then
  echo "==> Repo există, fac git pull…"
  git -C "$REPO_DIR" pull --ff-only
else
  echo "==> Clonez $REPO_URL în $REPO_DIR…"
  git clone --depth 1 "$REPO_URL" "$REPO_DIR"
fi

echo "==> Linkez $APP_DIR -> $REPO_DIR"
rm -f "$APP_DIR" 2>/dev/null || true
if [[ -d "$APP_DIR" && ! -L "$APP_DIR" ]]; then
  echo "    $APP_DIR există ca director real — îl mut în ${APP_DIR}.bak-$(date +%s)"
  mv "$APP_DIR" "${APP_DIR}.bak-$(date +%s)"
fi
ln -sfn "$REPO_DIR" "$APP_DIR"

echo "==> Rulez setup.sh…"
cd "$APP_DIR"
chmod +x setup.sh
bash setup.sh

echo ""
echo "✅ Gata. Update ulterior: bash $APP_DIR/update.sh"
