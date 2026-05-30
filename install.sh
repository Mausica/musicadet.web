#!/usr/bin/env bash
# One-shot install / update for MusicaDet.web
# Run as root on the Jellyfin LXC:
#   bash <(curl -fsSL https://raw.githubusercontent.com/Mausica/musicadet.web/main/install.sh)

set -euo pipefail

REPO_URL="https://github.com/Mausica/musicadet.web.git"
REPO_DIR="/opt/musicadet"
APP_DIR="/opt/music-sync"

if [[ $EUID -ne 0 ]]; then
  echo "Run as root (sudo -i)." >&2
  exit 1
fi

echo "==> Installing prerequisites (git, curl)..."
apt-get update -qq
apt-get install -y -qq git curl ca-certificates >/dev/null

if [[ -d "$REPO_DIR/.git" ]]; then
  echo "==> Updating repo (git pull)..."
  git -C "$REPO_DIR" pull --ff-only
else
  echo "==> Cloning $REPO_URL into $REPO_DIR..."
  git clone --depth 1 "$REPO_URL" "$REPO_DIR"
fi

echo "==> Linking $APP_DIR -> $REPO_DIR"
rm -f "$APP_DIR" 2>/dev/null || true
if [[ -d "$APP_DIR" && ! -L "$APP_DIR" ]]; then
  echo "    Moving existing $APP_DIR to ${APP_DIR}.bak-$(date +%s)"
  mv "$APP_DIR" "${APP_DIR}.bak-$(date +%s)"
fi
ln -sfn "$REPO_DIR" "$APP_DIR"

echo "==> Running setup.sh..."
cd "$APP_DIR"
chmod +x setup.sh
bash setup.sh

echo ""
echo "Done. Use 'music-sync' from anywhere, or open the HUD (see setup output above)."
