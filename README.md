# MusicaDet.web

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

This clones or updates the repo, installs dependencies, and registers the global `music-sync` CLI.

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
music-sync                              # full sync (playlists → scan albums → download)
music-sync scan                         # discover artists from playlists
music-sync scan-artists                 # scan artist albums into DB
music-sync scan-artists --new-only
music-sync artists-sync                 # download all active artists
music-sync artists-sync --new-only
music-sync reconcile                    # match files ↔ database
music-sync fix-metadata                 # re-embed tags/cover/lyrics
music-sync fix-metadata --artist "Name"
music-sync list-albums
music-sync add "Artist Name"
music-sync add "https://open.spotify.com/artist/..."
music-sync list
```

## Config (`config.json`)

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
systemctl status music-sync.timer       # daily auto sync
systemctl start music-sync.service      # run sync now
systemctl status music-sync-hud.service
journalctl -u music-sync-hud.service -f
```

## Migrating from old `spotify/` subfolder

If you previously used `/mnt/storage_jellyfin/media/music/spotify/`, either:

1. Change `music_dir` in Settings back to the old path, or
2. Move files to the new root and run `music-sync reconcile`
