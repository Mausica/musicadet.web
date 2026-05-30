#!/usr/bin/env bash
# MusicaDet setup — run from /opt/musicadet
set -euo pipefail

INSTALL_DIR="/opt/musicadet"
cd "$INSTALL_DIR"

echo "==> Installing system dependencies (ffmpeg, python3, pip)..."
apt-get update -qq
apt-get install -y ffmpeg python3 python3-pip curl ca-certificates

echo "==> Installing / upgrading spotDL..."
pip3 install --upgrade --ignore-installed spotdl --break-system-packages

echo "==> Installing Python deps (FastAPI, Uvicorn, mutagen)..."
pip3 install --upgrade fastapi "uvicorn[standard]" mutagen --break-system-packages

echo "==> Reading config..."
# Migrate legacy paths in config.json if present
if [[ -f "$INSTALL_DIR/config.json" ]]; then
  sed -i \
    -e 's|/opt/music-sync|/opt/musicadet|g' \
    -e 's|/var/log/music-sync|/var/log/musicadet|g' \
    "$INSTALL_DIR/config.json" 2>/dev/null || true
fi
MUSIC_DIR="$(python3 -c "import json; print(json.load(open('$INSTALL_DIR/config.json')).get('music_dir','/mnt/storage_jellyfin/media/music'))" 2>/dev/null || echo /mnt/storage_jellyfin/media/music)"
SYNC_DIR="$INSTALL_DIR/sync-data"
LOG_DIR="/var/log/musicadet"

echo "==> Creating directories..."
mkdir -p "$INSTALL_DIR" "$SYNC_DIR" "$MUSIC_DIR" "$LOG_DIR"
chmod 755 "$INSTALL_DIR"

echo "==> Checking required files..."
for f in music_sync.py config.json musicadet.service musicadet.timer hud.py musicadet-hud.service; do
  if [[ ! -f "$INSTALL_DIR/$f" ]]; then
    echo "ERROR: missing $INSTALL_DIR/$f" >&2
    exit 1
  fi
done
chmod +x "$INSTALL_DIR/music_sync.py" || true

echo "==> Installing global CLI: musicadet"
cat > /usr/local/bin/musicadet <<'WRAPPER'
#!/bin/sh
exec python3 /opt/musicadet/music_sync.py "$@"
WRAPPER
chmod +x /usr/local/bin/musicadet
rm -f /usr/local/bin/music-sync

echo "==> Initializing database..."
python3 "$INSTALL_DIR/music_sync.py" list >/dev/null || true

echo "==> Removing legacy systemd units (if any)..."
for old in music-sync.timer music-sync-hud.service music-sync.service; do
  systemctl disable --now "$old" 2>/dev/null || true
  rm -f "/etc/systemd/system/$old"
done

echo "==> Installing systemd units..."
install -m 644 "$INSTALL_DIR/musicadet.service"     /etc/systemd/system/musicadet.service
install -m 644 "$INSTALL_DIR/musicadet.timer"       /etc/systemd/system/musicadet.timer
install -m 644 "$INSTALL_DIR/musicadet-hud.service" /etc/systemd/system/musicadet-hud.service
systemctl daemon-reload
systemctl enable --now musicadet.timer
systemctl enable musicadet-hud.service
systemctl restart musicadet-hud.service

HUD_PORT="$(grep -oP '"hud_port"\s*:\s*\K[0-9]+' "$INSTALL_DIR/config.json" 2>/dev/null || echo 8800)"
SERVER_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
[[ -z "$SERVER_IP" ]] && SERVER_IP="<server-ip>"

echo ""
echo "===================================================="
echo " Setup complete."
echo "===================================================="
echo " Music output : $MUSIC_DIR"
echo " Database     : $INSTALL_DIR/music.db"
echo " Logs         : $LOG_DIR"
echo ""
echo " HUD dashboard: http://$SERVER_IP:$HUD_PORT"
echo ""
echo " Global CLI (works from anywhere):"
echo "   musicadet                              # full sync"
echo "   musicadet scan                         # discover artists from playlists"
echo "   musicadet scan-artists                 # scan albums into DB"
echo "   musicadet artists-sync                 # download discographies"
echo "   musicadet artists-sync --new-only"
echo "   musicadet reconcile"
echo "   musicadet fix-metadata"
echo "   musicadet list-albums"
echo "   musicadet add \"Artist Name\""
echo "   musicadet list"
echo ""
echo " Update anytime:"
echo "   bash <(curl -fsSL https://raw.githubusercontent.com/Mausica/musicadet.web/main/install.sh)"
echo ""
echo " Timer:  systemctl status musicadet.timer"
echo " HUD:    systemctl status musicadet-hud.service"
echo "===================================================="
