#!/usr/bin/env python3
"""
custom_dl.py — yt-dlp native downloader + Mutagen metadata embedder
─────────────────────────────────────────────────────────────────────
Pipeline per-track:
  1. Search YouTube Music for "artist - title" and pick the best hit.
  2. Download to  music_dir/{artist}/{album}/{track:02d} - {title}.opus
  3. Embed Vorbis tags: title, artist (primary only), album, tracknumber.
  4. After a track is saved, check if the primary artist has any missing
     albums in the DB and queue a full yt-dlp album download if needed.

Album completeness check:
  - For each artist found in a playlist sync, query all DB albums.
  - If an album has downloaded_count < track_count, try to download it
    from YouTube Music using a "ytsearch" album query, all tracks, sequentially.

No Spotify client-ID, no YouTube login, no cookies needed.
"""

import logging
import os
import re
import shutil
import sqlite3
from pathlib import Path
from typing import Optional

log = logging.getLogger("musicadet.custom_dl")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)

try:
    import yt_dlp
except ImportError:
    yt_dlp = None

try:
    from mutagen.oggopus import OggOpus
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, TRCK
    import mutagen as _mutagen_mod
except ImportError:
    OggOpus = None
    _mutagen_mod = None


# ─────────────────────────────────────────────────────────────────────────────
# Filename sanitation
# ─────────────────────────────────────────────────────────────────────────────

def _clean_filename(name: str) -> str:
    """Remove characters illegal on Linux/Windows filesystems."""
    return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip().strip(".")


# ─────────────────────────────────────────────────────────────────────────────
# Metadata embedding (Mutagen)
# ─────────────────────────────────────────────────────────────────────────────

def enforce_primary_artist(
    file_path: Path,
    primary_artist: str,
    title: str,
    album: str,
    track_number: Optional[int],
    cover_url: str = None,
) -> bool:
    """
    Write Vorbis / ID3 tags to an audio file.
    Always sets artist to only the primary (first) artist — no feat. clutter.
    """
    if _mutagen_mod is None or not file_path.exists():
        return False

    ext = file_path.suffix.lower()
    try:
        if ext == ".opus":
            audio = OggOpus(file_path)
            audio["title"] = title
            audio["artist"] = primary_artist
            audio["albumartist"] = primary_artist
            if album:
                audio["album"] = album
            if track_number:
                audio["tracknumber"] = str(track_number)
            audio.save()
            return True

        elif ext == ".mp3":
            try:
                audio = ID3(file_path)
            except Exception:
                from mutagen.id3 import ID3NoHeaderError
                audio = ID3()
            audio.add(TIT2(encoding=3, text=title))
            audio.add(TPE1(encoding=3, text=primary_artist))
            if album:
                audio.add(TALB(encoding=3, text=album))
            if track_number:
                audio.add(TRCK(encoding=3, text=str(track_number)))
            audio.save(file_path)
            return True

        else:
            # Generic mutagen fallback (flac, m4a, ogg, …)
            from mutagen import File as MutagenFile
            audio = MutagenFile(file_path, easy=True)
            if audio is not None:
                audio["title"] = title
                audio["artist"] = primary_artist
                audio["albumartist"] = primary_artist
                if album:
                    audio["album"] = album
                if track_number:
                    audio["tracknumber"] = str(track_number)
                audio.save()
                return True

    except Exception as e:
        log.error("Failed to tag %s: %s", file_path, e)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# yt-dlp downloader
# ─────────────────────────────────────────────────────────────────────────────

class YtDlpDownloader:
    """
    Thin wrapper around yt-dlp that:
      - Searches YouTube Music for a track by "artist - title" query.
      - Downloads the best audio stream as Opus.
      - Optionally downloads a full album by searching "artist album full album".
    No authentication required.
    """

    def __init__(self, music_dir: Path, fmt: str = "opus"):
        self.music_dir = music_dir
        self.fmt = fmt

    # ── internal: build ydl_opts ────────────────────────────────────────────

    def _ydl_opts(self, out_template: str, quiet: bool = True) -> dict:
        return {
            "format": "bestaudio/best",
            "outtmpl": out_template,
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": self.fmt,
                    "preferredquality": "0",   # best VBR
                }
            ],
            "quiet": quiet,
            "no_warnings": True,
            "ignoreerrors": True,
            "retries": 3,
            "fragment_retries": 5,
            "extractor_args": {
                "youtube": {"skip": ["dash", "hls"]},
            },
            # Prefer YouTube Music results when using ytsearch
            "default_search": "https://music.youtube.com/search?q=",
        }

    # ── search → first result URL ────────────────────────────────────────────

    def _search_url(self, query: str) -> Optional[str]:
        """Return the YouTube URL of the first search hit, or None."""
        if yt_dlp is None:
            return None
        opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,       # don't download, just extract info
            "default_search": "ytsearch1",
        }
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(f"ytsearch1:{query}", download=False)
                if info and info.get("entries"):
                    entry = info["entries"][0]
                    return entry.get("url") or entry.get("webpage_url") or f"https://www.youtube.com/watch?v={entry['id']}"
        except Exception as e:
            log.debug("Search failed for '%s': %s", query, e)
        return None

    # ── download single track ────────────────────────────────────────────────

    def download_track(
        self,
        artist: str,
        title: str,
        album: str,
        track_number: Optional[int],
        yt_url: Optional[str] = None,
    ) -> Optional[Path]:
        """
        Download one track.
        If yt_url is not provided (spotdl didn't give us one) we search YT Music.
        Returns the Path to the downloaded file, or None on failure.
        """
        if yt_dlp is None:
            log.error("yt-dlp not installed — cannot download")
            return None

        # Build the output path
        artist_folder = self.music_dir / _clean_filename(artist)
        album_folder = artist_folder / _clean_filename(album) if album else artist_folder
        album_folder.mkdir(parents=True, exist_ok=True)

        trk = str(track_number).zfill(2) if track_number else "00"
        safe_title = _clean_filename(title)
        out_path = album_folder / f"{trk} - {safe_title}.{self.fmt}"

        if out_path.exists():
            log.info("    ↳ Already exists: %s", out_path.name)
            return out_path

        # Resolve URL if not given
        if not yt_url:
            query = f"{artist} - {title}"
            log.info("    ↳ Searching: %s", query)
            yt_url = self._search_url(query)
            if not yt_url:
                log.warning("    ✗ No YouTube result for: %s", query)
                return None

        log.info("    ↳ Downloading: %s → %s", title, out_path.name)

        # yt-dlp writes {outtmpl}.{ext}; we give it the path without extension
        out_tpl = str(out_path.with_suffix("")) + ".%(ext)s"
        opts = self._ydl_opts(out_tpl)

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([yt_url])
        except Exception as e:
            log.error("    ✗ yt-dlp error for %s: %s", title, e)
            return None

        # yt-dlp might have written a slightly different extension; locate the file
        actual = self._find_downloaded(album_folder, safe_title, trk)
        if actual is None:
            log.warning("    ✗ File not found after download: %s", out_path)
            return None

        # Rename to canonical path if needed
        if actual != out_path:
            actual.rename(out_path)

        return out_path

    def _find_downloaded(self, folder: Path, safe_title: str, trk: str) -> Optional[Path]:
        """Locate the file yt-dlp wrote (may be .webm, .m4a before conversion)."""
        audio_exts = {".opus", ".webm", ".m4a", ".mp3", ".ogg"}
        for f in folder.iterdir():
            if f.suffix.lower() in audio_exts and safe_title.lower() in f.stem.lower():
                return f
        # Try by track prefix
        for f in folder.iterdir():
            if f.stem.startswith(trk) and f.suffix.lower() in audio_exts:
                return f
        return None

    # ── download whole album from YouTube Music ──────────────────────────────

    def download_album(self, artist: str, album: str) -> int:
        """
        Search YouTube Music for the full album playlist and download all tracks.
        Returns the number of successfully downloaded tracks.
        """
        if yt_dlp is None:
            return 0

        query = f"ytsearch1:{artist} {album} full album"
        log.info("  ↳ Album search: %s — %s", artist, album)

        # First, search for a playlist/album result
        search_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
        }
        playlist_url = None
        try:
            with yt_dlp.YoutubeDL(search_opts) as ydl:
                info = ydl.extract_info(query, download=False)
                if info and info.get("entries"):
                    entry = info["entries"][0]
                    url = entry.get("url") or entry.get("webpage_url") or ""
                    # Accept playlist or single video
                    if "list=" in url or "playlist" in url:
                        playlist_url = url
                    else:
                        # Fall back: use it as a single video
                        playlist_url = url
        except Exception as e:
            log.debug("Album search error: %s", e)

        if not playlist_url:
            log.warning("  ✗ No result for album: %s — %s", artist, album)
            return 0

        # Build output template for album tracks
        artist_folder = self.music_dir / _clean_filename(artist)
        album_folder = artist_folder / _clean_filename(album)
        album_folder.mkdir(parents=True, exist_ok=True)

        out_tpl = str(album_folder / "%(playlist_index)02d - %(title)s.%(ext)s")
        opts = self._ydl_opts(out_tpl, quiet=False)

        downloaded = 0
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                result = ydl.download([playlist_url])
                if result == 0:
                    downloaded = len(list(album_folder.glob(f"*.{self.fmt}")))
        except Exception as e:
            log.error("  ✗ Album download error (%s — %s): %s", artist, album, e)

        log.info("  ✓ Album downloaded %d tracks: %s — %s", downloaded, artist, album)
        return downloaded


# ─────────────────────────────────────────────────────────────────────────────
# Album completeness check
# ─────────────────────────────────────────────────────────────────────────────

def check_and_complete_artist_albums(
    db_path: Path,
    music_dir: Path,
    artist_id: str,
    artist_name: str,
    downloader: YtDlpDownloader,
) -> int:
    """
    For a given artist, look at all their albums in the DB.
    If an album has fewer downloaded tracks than expected, queue a full album
    download via yt-dlp.
    Returns the number of albums that were (re-)downloaded.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    albums = conn.execute(
        """
        SELECT spotify_id, name, track_count, downloaded_count
        FROM albums
        WHERE artist_id = ?
        """,
        (artist_id,),
    ).fetchall()

    fixed = 0
    for album in albums:
        total = album["track_count"] or 0
        done = album["downloaded_count"] or 0
        if total > 0 and done < total:
            missing = total - done
            log.info(
                "  → Album '%s' by %s: %d/%d tracks — downloading %d missing",
                album["name"], artist_name, done, total, missing,
            )
            n = downloader.download_album(artist_name, album["name"])
            if n > 0:
                fixed += 1
                # Update downloaded_count
                conn.execute(
                    "UPDATE albums SET downloaded_count=? WHERE spotify_id=?",
                    (done + n, album["spotify_id"]),
                )
        else:
            log.debug("  ✓ Album complete: %s (%d/%d)", album["name"], done, total)

    conn.commit()
    conn.close()
    return fixed


# ─────────────────────────────────────────────────────────────────────────────
# Single-track download (called from music_sync cmd_artists_sync)
# ─────────────────────────────────────────────────────────────────────────────

def download_track(download_url: str, output_path: Path, format_codec: str = "opus") -> bool:
    """
    Compatibility wrapper used by the legacy sync path.
    Preferred: use YtDlpDownloader.download_track() directly.
    """
    if yt_dlp is None:
        log.error("yt_dlp not installed")
        return False

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": str(output_path.with_suffix("")) + ".%(ext)s",
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": format_codec,
                "preferredquality": "0",
            }
        ],
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([download_url])
        return output_path.exists()
    except Exception as e:
        log.error("Failed to download %s: %s", download_url, e)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Flat-to-album structure migration
# ─────────────────────────────────────────────────────────────────────────────

def migrate_structure(db_path: Path, music_dir: Path):
    """
    Scans the DB for songs that are either:
      - listed as 'downloaded' with a flat file_path ({artist}/{title}.ext)
      - physically present as flat files in the artist folder

    Moves them to {artist}/{album}/{track:02d} - {title}.ext
    and re-embeds metadata via enforce_primary_artist.
    """
    if not db_path.exists() or not music_dir.exists():
        log.error("DB or music dir not found")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    songs = conn.execute(
        """
        SELECT s.spotify_id, s.title, s.track_number, s.status, s.file_path,
               al.name AS album_name, ar.name AS artist_name
        FROM songs s
        JOIN albums al ON s.album_id = al.spotify_id
        JOIN artists ar ON s.artist_id = ar.spotify_id
        WHERE s.status IN ('downloaded', 'pending')
        """
    ).fetchall()

    migrated = 0
    for s in songs:
        artist_folder = music_dir / _clean_filename(s["artist_name"])

        # Try known flat paths for this track
        candidates = [
            artist_folder / f"{_clean_filename(s['title'])}.opus",
            artist_folder / f"{_clean_filename(s['title'])}.mp3",
            artist_folder / f"{_clean_filename(s['title'])}.m4a",
        ]
        if s["file_path"]:
            fp = Path(s["file_path"])
            if not fp.is_absolute():
                fp = music_dir / fp
            candidates.insert(0, fp)

        src = next((p for p in candidates if p.exists()), None)
        if not src:
            continue

        # Build destination
        album_folder = artist_folder / _clean_filename(s["album_name"])
        album_folder.mkdir(parents=True, exist_ok=True)
        trk = str(s["track_number"]).zfill(2) if s["track_number"] else "00"
        dst = album_folder / f"{trk} - {_clean_filename(s['title'])}{src.suffix}"

        if src != dst:
            shutil.move(str(src), str(dst))
            log.info("Moved: %s → %s", src.relative_to(music_dir), dst.relative_to(music_dir))
            migrated += 1

        enforce_primary_artist(dst, s["artist_name"], s["title"], s["album_name"], s["track_number"])

        rel = str(dst.relative_to(music_dir))
        conn.execute(
            "UPDATE songs SET file_path=?, status='downloaded' WHERE spotify_id=?",
            (rel, s["spotify_id"]),
        )

    conn.commit()
    conn.close()
    log.info("migrate_structure: moved %d files into album folders.", migrated)
