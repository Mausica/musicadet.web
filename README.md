# MusicaDet

Self-hosted Spotify → Jellyfin music sync with a web dashboard (HUD).

Discovers artists from Spotify playlists, scans their full album catalogs into SQLite,
downloads discographies with **spotDL** as **MP3 320 kbps** with embedded cover art,
lyrics, and ID3 tags, and organizes files for Jellyfin:

```
/mnt/storage_jellyfin/media/music/ARTIST/ALBUM/track.mp3
```

## Install / update (one-liner, as root)

```bash
bash <(curl -fsSL https://raw.githubusercontent.com/Mausica/musicadet.web/main/install.sh)
```

This clones or updates the repo (overwriting any local changes in `/opt/musicadet`),
installs dependencies, and registers the global `musicadet` CLI.

## HUD web dashboard

After install: **http://SERVER_IP:8800**

- Dark minimal UI with live console (WebSocket)
- Dashboard stats: artists, albums, songs, metadata gaps
- **Library** tab: album download progress, per-song cover/lyrics status
- **Settings** tab: music folder, format, bitrate, lyrics providers, timeouts
- Manage artists & playlists; trigger scan / sync / reconcile / fix-metadata

## Global CLI

Works from anywhere after install:

```bash
musicadet                              # full sync (playlists → scan albums → download)
musicadet scan                         # discover artists from playlists
musicadet scan-artists                 # scan artist albums into DB
musicadet scan-artists --new-only
musicadet artists-sync                 # download all active artists
musicadet artists-sync --new-only
musicadet reconcile                    # match files ↔ database
musicadet fix-metadata                 # re-embed tags/cover/lyrics
musicadet fix-metadata --artist "Name"
musicadet list-albums
musicadet add "Artist Name"
musicadet add "https://open.spotify.com/artist/..."
musicadet list
```

## Config (`/opt/musicadet/config.json`)

| Key | Default | Description |
|-----|---------|-------------|
| `music_dir` | `/mnt/storage_jellyfin/media/music` | Download root (editable in HUD Settings) |
| `format` | `mp3` | Audio format |
| `bitrate` | `320k` | MP3 bitrate |
| `output_template` | `{artist}/{album}/{track-number} - {title}.{output-ext}` | Folder layout |
| `lyrics_providers` | genius, musixmatch, azlyrics | Lyrics fallback chain |
| `playlist_save_timeout` | `600` | Seconds for large playlist metadata fetch |
| `hud_port` | `8800` | Web dashboard port |

## Systemd

```bash
systemctl status musicadet.timer       # daily auto sync
systemctl start musicadet.service      # run sync now
systemctl status musicadet-hud.service
journalctl -u musicadet-hud.service -f
```

## Migrating from older installs

If you previously used `/opt/music-sync` or `/mnt/storage_jellyfin/media/music/spotify/`:

- `/opt/music-sync` is kept as a symlink to `/opt/musicadet` for compatibility
- Old `music-sync` CLI is removed; use `musicadet` instead
- Old systemd units (`music-sync.*`) are replaced by `musicadet.*`
- Either keep your old `music_dir` in Settings, or move files and run `musicadet reconcile`
