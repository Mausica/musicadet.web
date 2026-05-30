#!/usr/bin/env bash
# /opt/music-sync/setup.sh
set -euo pipefail

INSTALL_DIR="/opt/music-sync"
cd "$INSTALL_DIR"

echo "==> Installing system dependencies (ffmpeg, python3, pip)..."
apt-get update -qq
apt-get install -y ffmpeg python3 python3-pip curl ca-certificates

echo "==> Installing / upgrading spotDL..."
pip3 install --upgrade --ignore-installed spotdl --break-system-packages

echo "==> Installing Python deps (FastAPI, Uvicorn, mutagen)..."
pip3 install --upgrade fastapi "uvicorn[standard]" mutagen --break-system-packages

echo "==> Reading config..."
MUSIC_DIR="$(python3 -c "import json; print(json.load(open('$INSTALL_DIR/config.json')).get('music_dir','/mnt/storage_jellyfin/media/music'))" 2>/dev/null || echo /mnt/storage_jellyfin/media/music)"
SYNC_DIR="$INSTALL_DIR/sync-data"
LOG_DIR="/var/log/music-sync"

echo "==> Creating directories..."
mkdir -p "$INSTALL_DIR" "$SYNC_DIR" "$MUSIC_DIR" "$LOG_DIR"
chmod 755 "$INSTALL_DIR"

echo "==> Checking required files..."
for f in music_sync.py config.json music-sync.service music-sync.timer hud.py music-sync-hud.service; do
  if [[ ! -f "$INSTALL_DIR/$f" ]]; then
    echo "ERROR: missing $INSTALL_DIR/$f" >&2
    exit 1
  fi
done
chmod +x "$INSTALL_DIR/music_sync.py" || true

echo "==> Installing global CLI: music-sync"
cat > /usr/local/bin/music-sync <<'WRAPPER'
#!/bin/sh
exec python3 /opt/music-sync/music_sync.py "$@"
WRAPPER
chmod +x /usr/local/bin/music-sync

echo "==> Initializing database..."
python3 "$INSTALL_DIR/music_sync.py" list >/dev/null || true

echo "==> Installing systemd units..."
install -m 644 "$INSTALL_DIR/music-sync.service" /etc/systemd/system/music-sync.service
install -m 644 "$INSTALL_DIR/music-sync.timer"   /etc/systemd/system/music-sync.timer
install -m 644 "$INSTALL_DIR/music-sync-hud.service" /etc/systemd/system/music-sync-hud.service
systemctl daemon-reload
systemctl enable --now music-sync.timer
systemctl enable music-sync-hud.service
systemctl restart music-sync-hud.service

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
echo "   music-sync                              # full sync"
echo "   music-sync scan                         # discover artists from playlists"
echo "   music-sync scan-artists                 # scan albums into DB"
echo "   music-sync artists-sync                 # download discographies"
echo "   music-sync artists-sync --new-only"
echo "   music-sync reconcile"
echo "   music-sync fix-metadata"
echo "   music-sync list-albums"
echo "   music-sync add \"Artist Name\""
echo "   music-sync list"
echo ""
echo " Update anytime:"
echo "   bash <(curl -fsSL https://raw.githubusercontent.com/Mausica/musicadet.web/main/install.sh)"
echo ""
echo " Timer:  systemctl status music-sync.timer"
echo " HUD:    systemctl status music-sync-hud.service"
echo "===================================================="
