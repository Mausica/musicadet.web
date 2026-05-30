# MusicaDet.web

Self-hosted Spotify → Jellyfin music sync cu dashboard web (HUD) live.

Descarcă discografii de artiști și playlist-uri Spotify cu **spotDL**, le ține
ordonate pentru Jellyfin, evită descărcările duplicate (DB SQLite) și oferă un
**HUD web** cu loguri live, statistici și editare bază de date artiști/playlist-uri.

## Instalare (one-liner, ca root)

```bash
bash <(curl -fsSL https://raw.githubusercontent.com/Mausica/musicadet.web/main/install.sh)
```

Ce face:
1. clonează `Mausica/musicadet.web` în `/opt/musicadet`
2. linkează `/opt/music-sync` -> `/opt/musicadet`
3. instalează deps (ffmpeg, spotDL, FastAPI, Uvicorn)
4. pornește timer-ul de sync + serviciul HUD

## HUD web

După instalare: **http://IP_SERVER:8800**

- loguri live (WebSocket) din `music_sync.py` și spotDL
- statistici (artiști, playlist-uri, melodii)
- editare artiști & playlist-uri (persistă în DB + `config.json`)
- declanșare manuală: scan / artists-sync / full sync

## Update

```bash
bash /opt/music-sync/update.sh
```

## Comenzi utile

```bash
python3 /opt/music-sync/music_sync.py add "THE MOTANS"
python3 /opt/music-sync/music_sync.py scan
python3 /opt/music-sync/music_sync.py artists-sync
python3 /opt/music-sync/music_sync.py            # full sync
python3 /opt/music-sync/music_sync.py list

systemctl status music-sync.timer
systemctl status music-sync-hud.service
journalctl -u music-sync-hud.service -f
```

## Config

`config.json`:
- `music_dir` — unde se salvează muzica (montată în Jellyfin)
- `hud_port` — portul HUD (default 8800)
- `playlists` — lista de playlist-uri urmărite
