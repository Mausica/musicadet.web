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

# Back up user data before updating
BAK_DIR="/tmp/musicadet_bak_$(date +%s)"
mkdir -p "$BAK_DIR"

if [[ -d "$INSTALL_DIR" ]]; then
  echo "==> Backing up config and cookies..."
  [[ -f "$INSTALL_DIR/config.json" ]] && cp "$INSTALL_DIR/config.json" "$BAK_DIR/" || true
  [[ -f "$INSTALL_DIR/music.db" ]] && cp "$INSTALL_DIR/music.db" "$BAK_DIR/" || true
  [[ -f "$INSTALL_DIR/music.db-wal" ]] && cp "$INSTALL_DIR/music.db-wal" "$BAK_DIR/" || true
  [[ -f "$INSTALL_DIR/music.db-shm" ]] && cp "$INSTALL_DIR/music.db-shm" "$BAK_DIR/" || true
  [[ -f "$INSTALL_DIR/sync-data/youtube-cookies.txt" ]] && cp "$INSTALL_DIR/sync-data/youtube-cookies.txt" "$BAK_DIR/" || true
fi

restore_bak() {
  if [[ -d "$BAK_DIR" ]]; then
    echo "==> Restoring config and cookies..."
    mkdir -p "$INSTALL_DIR/sync-data"
    [[ -f "$BAK_DIR/config.json" ]] && cp "$BAK_DIR/config.json" "$INSTALL_DIR/" || true
    [[ -f "$BAK_DIR/music.db" ]] && cp "$BAK_DIR/music.db" "$INSTALL_DIR/" || true
    [[ -f "$BAK_DIR/music.db-wal" ]] && cp "$BAK_DIR/music.db-wal" "$INSTALL_DIR/" || true
    [[ -f "$BAK_DIR/music.db-shm" ]] && cp "$BAK_DIR/music.db-shm" "$INSTALL_DIR/" || true
    [[ -f "$BAK_DIR/youtube-cookies.txt" ]] && cp "$BAK_DIR/youtube-cookies.txt" "$INSTALL_DIR/sync-data/" || true
    rm -rf "$BAK_DIR"
  fi
}

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

# Restore backed up configurations and cookies
restore_bak

# Backward-compatible symlink for older configs that reference /opt/music-sync
ln -sfn "$INSTALL_DIR" /opt/music-sync

echo "==> Running setup..."
cd "$INSTALL_DIR"
chmod +x setup.sh
bash setup.sh

echo ""
echo "Done. Run 'musicadet' from anywhere, or open the HUD (see setup output above)."
