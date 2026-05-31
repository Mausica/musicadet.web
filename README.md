# Musicadet 🎵

A fully automated, self-hosted music aggregator. It discovers artists from Spotify, rips their discographies from YouTube Music at blazing speeds, and injects beautiful high-res metadata. 

---

## 🏗 How It Works

```mermaid
graph TD
    subgraph UI [📱 Premium Web UI]
    A[Dark Mode Dashboard] -->|WebSockets| B[Live Download Console]
    A -->|API| C[FastAPI Backend]
    end

    subgraph Engine [⚙️ Sync Engine]
    E[Spotify Playlist] -->|Extract Artists| F[SQLite WAL Database]
    F -->|4-Worker Thread Pool| G[yt-dlp Downloader]
    G -->|Concurrent Ripper| H[Sequential per-artist]
    end

    subgraph PostProcessing [🎨 Post-Processing]
    H -->|Saves .opus / .mp3| I[Mutagen Tag Engine]
    J[iTunes API] -.->|600x600 Cover, Year, Genre| I
    I --> D[Jellyfin Music Folder]
    end
```

---

## 🚀 One-Line Install

```bash
bash <(curl -fsSL https://raw.githubusercontent.com/Mausica/musicadet.web/main/install.sh)
```

## ✨ Highlights

* **Blazing Fast:** 4 concurrent `yt-dlp` background workers.
* **Smart Metadata:** Automatically embeds gorgeous **600x600 iTunes Cover Art**, Release Year, and Genre directly into your files.
* **Bulletproof Database:** SQLite runs in `WAL` mode with 60-second timeouts — zero database locks.
* **Instant Inspection:** Click **Info** on any file in the Web UI to see its embedded cover and metadata in a sleek modal!

## 📱 Web Dashboard (`http://SERVER_IP:8800`)

* **Library & Artists:** Browse your entire downloaded catalog.
* **Files:** Inspect physical disk files and their embedded tags.
* **Console:** Watch the 4-worker thread pool rip tracks in real-time.
* **Settings:** Configure Jellyfin paths, preferred format (`opus` or `mp3`), and bitrate.

## 💻 CLI Commands

```bash
musicadet                              # Run a full automated sync
musicadet download-pending             # Trigger the 4-worker downloader
musicadet fix-metadata                 # Re-fetch covers & tags from iTunes
musicadet add "Artist Name"            # Force-add an artist
```
