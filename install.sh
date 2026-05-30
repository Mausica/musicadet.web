#!/usr/bin/env bash
# One-shot install / update for MusicaDet.web
# Run as root on the Jellyfin LXC:
#   bash <(curl -fsSL https://raw.githubusercontent.com/Mausica/musicadet.web/main/install.sh)

set -euo pipefail

REPO_URL="https://github.com/Mausica/musicadet.web.git"
INSTALL_DIR="/opt/musicadet"

if [[ $EUID -ne 0 ]]; then
  echo "Run as root (sudo -i)." >&2
  exit 1
fi

echo "==> Installing prerequisites (git, curl)..."
apt-get update -qq
apt-get install -y -qq git curl ca-certificates >/dev/null

update_repo() {
  echo "==> Updating repo in $INSTALL_DIR (discarding local changes)..."
  git -C "$INSTALL_DIR" fetch origin main
  git -C "$INSTALL_DIR" reset --hard origin/main
  git -C "$INSTALL_DIR" clean -fd
}

fresh_clone() {
  echo "==> Cloning fresh copy into $INSTALL_DIR..."
  rm -rf "$INSTALL_DIR"
  git clone --depth 1 --branch main "$REPO_URL" "$INSTALL_DIR"
}

if [[ -d "$INSTALL_DIR/.git" ]]; then
  if ! update_repo; then
    echo "==> Update failed — re-cloning..."
    fresh_clone
  fi
else
  fresh_clone
fi

# Backward-compatible symlink for older configs that reference /opt/music-sync
ln -sfn "$INSTALL_DIR" /opt/music-sync

echo "==> Running setup..."
cd "$INSTALL_DIR"
chmod +x setup.sh
bash setup.sh

echo ""
echo "Done. Run 'musicadet' from anywhere, or open the HUD (see setup output above)."
