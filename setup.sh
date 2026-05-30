#!/usr/bin/env bash
# /opt/music-sync/setup.sh
# Clean installer for the Spotify -> Jellyfin auto-sync stack.
# Run ONCE as root on the Jellyfin LXC:
#     bash setup.sh
#
# Assumes the package files (music_sync.py, config.json, music-sync.service,
# music-sync.timer, this setup.sh) are already in /opt/music-sync/.

set -euo pipefail

INSTALL_DIR="/opt/music-sync"
MUSIC_DIR="/mnt/storage_jellyfin/media/music/spotify"
SYNC_DIR="$INSTALL_DIR/sync-data"
LOG_DIR="/var/log/music-sync"

cd "$INSTALL_DIR"

echo "==> Installing system dependencies (ffmpeg, python3, pip)..."
apt-get update -qq
apt-get install -y ffmpeg python3 python3-pip curl ca-certificates

echo "==> Installing / upgrading spotDL..."
# --ignore-installed: ocolește pachetele instalate via apt (ex: python3-rich) pe care pip nu le poate dezinstala
pip3 install --upgrade --ignore-installed spotdl --break-system-packages

echo "==> Installing HUD dependencies (FastAPI + Uvicorn)..."
pip3 install --upgrade fastapi "uvicorn[standard]" --break-system-packages

echo "==> Creating directories..."
mkdir -p "$INSTALL_DIR" "$SYNC_DIR" "$MUSIC_DIR" "$LOG_DIR"
chmod 755 "$INSTALL_DIR"

echo "==> Checking required files..."
for f in music_sync.py config.json music-sync.service music-sync.timer hud.py music-sync-hud.service; do
  if [[ ! -f "$INSTALL_DIR/$f" ]]; then
    echo "ERROR: missing $INSTALL_DIR/$f - copy the whole package first." >&2
    exit 1
  fi
done
chmod +x "$INSTALL_DIR/music_sync.py" || true

echo "==> Initializing database (no downloads yet)..."
python3 "$INSTALL_DIR/music_sync.py" list >/dev/null || true

echo "==> Installing systemd units..."
install -m 644 "$INSTALL_DIR/music-sync.service" /etc/systemd/system/music-sync.service
install -m 644 "$INSTALL_DIR/music-sync.timer"   /etc/systemd/system/music-sync.timer
install -m 644 "$INSTALL_DIR/music-sync-hud.service" /etc/systemd/system/music-sync-hud.service
systemctl daemon-reload
systemctl enable --now music-sync.timer
systemctl enable music-sync-hud.service
systemctl restart music-sync-hud.service

# Detect a LAN IP for the final hint (best-effort)
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
echo " Useful commands:"
echo "   python3 $INSTALL_DIR/music_sync.py add \"THE MOTANS\""
echo "   python3 $INSTALL_DIR/music_sync.py add https://open.spotify.com/artist/XXXX"
echo "   python3 $INSTALL_DIR/music_sync.py scan          # only discover from playlists"
echo "   python3 $INSTALL_DIR/music_sync.py artists-sync  # download discographies"
echo "   python3 $INSTALL_DIR/music_sync.py               # FULL sync (scan + download)"
echo "   python3 $INSTALL_DIR/music_sync.py list"
echo ""
echo " Timer:"
echo "   systemctl status music-sync.timer"
echo "   systemctl list-timers music-sync.timer"
echo "   systemctl start music-sync.service   # run now"
echo "   journalctl -u music-sync.service -f  # live logs"
echo ""
echo " HUD:"
echo "   systemctl status music-sync-hud.service"
echo "   journalctl -u music-sync-hud.service -f"
echo "===================================================="
