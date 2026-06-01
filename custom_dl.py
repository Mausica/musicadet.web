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

Optional YouTube cookies (config: youtube_cookies_file or youtube_cookies_from_browser)
help when YouTube returns “Sign in to confirm you're not a bot”.
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
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, TRCK, APIC
    import mutagen as _mutagen_mod
except ImportError:
    OggOpus = None
    _mutagen_mod = None


# ─────────────────────────────────────────────────────────────────────────────
# Rate limiting
# ─────────────────────────────────────────────────────────────────────────────

import time
RATE_LIMITED_UNTIL = 0.0  # Unix timestamp when rate limit expires (0 = not rate-limited)

def is_rate_limited() -> bool:
    """Check if we are currently rate-limited by YouTube."""
    return time.time() < RATE_LIMITED_UNTIL

def check_rate_limit() -> Optional[int]:
    """
    If rate-limited, return seconds remaining. Otherwise return None.
    """
    if is_rate_limited():
        return int(RATE_LIMITED_UNTIL - time.time()) + 1
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Filename sanitation
# ─────────────────────────────────────────────────────────────────────────────

def _clean_filename(name: str) -> str:
    """Remove characters illegal on Linux/Windows filesystems."""
    return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip().strip(".")


def detect_singles(album: Optional[str], title: str) -> str:
    if not album or album == "Unknown Album":
        return "Singles"
    album_clean = _clean_filename(album).lower().split(" (feat")[0].replace(" - single", "").replace(" - ep", "").strip()
    title_clean = _clean_filename(title).lower().split(" (feat")[0].split(" - ")[0].strip()
    if album_clean == title_clean:
        return "Singles"
    return album


# ─────────────────────────────────────────────────────────────────────────────
# Metadata embedding (Mutagen)
# ─────────────────────────────────────────────────────────────────────────────

# Garbage genre values that should be replaced with proper genres or removed
GARBAGE_GENRES = {
    "-", "Music", "Music Video", "Unknown", "Other", "Video", "",
    "music", "music video", "unknown", "other", "video",
    "Entertainment", "entertainment", "People & Blogs", "people & blogs",
    "Howto & Style", "Comedy", "News & Politics", "Film & Animation",
    "music videos", "unknown genre", "various", "various artists"
}

def _is_bad_genre(g: str) -> bool:
    if not g:
        return True
    g_clean = str(g).strip().lower()
    return g_clean in {x.lower() for x in GARBAGE_GENRES} or not g_clean


def _fetch_itunes_cover(artist: str, title: str) -> tuple[Optional[bytes], Optional[str], Optional[str]]:
    import urllib.request, urllib.parse, json
    query = urllib.parse.quote(f"{artist} {title}")
    url = f"https://itunes.apple.com/search?term={query}&limit=1&media=music"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.loads(r.read())
            if data.get("resultCount", 0) > 0:
                result = data["results"][0]
                img_url = result.get("artworkUrl100", "").replace("100x100bb.jpg", "600x600bb.jpg")
                year = result.get("releaseDate", "")[:4] if "releaseDate" in result else None
                genre = result.get("primaryGenreName")
                if _is_bad_genre(genre):
                    genre = None
                if img_url:
                    with urllib.request.urlopen(img_url, timeout=5) as ir:
                        return ir.read(), year, genre
                return None, year, genre
    except Exception as e:
        log.debug("iTunes metadata fetch failed for %s - %s: %s", artist, title, e)
    return None, None, None

def _fetch_artist_genre(artist: str) -> Optional[str]:
    """Query iTunes for the artist alone to get their real genre."""
    import urllib.request, urllib.parse, json
    query = urllib.parse.quote(artist)
    url = f"https://itunes.apple.com/search?term={query}&limit=5&media=music&entity=song"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            data = json.loads(r.read())
            for result in data.get("results", []):
                genre = result.get("primaryGenreName", "")
                if not _is_bad_genre(genre):
                    return genre
    except Exception:
        pass
    return None

def _fetch_youtube_metadata(artist: str, title: str) -> tuple[Optional[bytes], Optional[str], Optional[str]]:
    if yt_dlp is None:
        return None, None, None
    opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            query = f"ytsearch1:{artist} {title} audio"
            info = ydl.extract_info(query, download=False)
            if info and info.get("entries"):
                entry = info["entries"][0]
                img_data = None
                thumbnails = entry.get("thumbnails", [])
                if thumbnails:
                    best = sorted(thumbnails, key=lambda t: t.get("width", 0) or 0, reverse=True)[0]
                    img_url = best.get("url")
                    if img_url:
                        import urllib.request
                        try:
                            req = urllib.request.Request(img_url, headers={'User-Agent': 'Mozilla/5.0'})
                            with urllib.request.urlopen(req, timeout=5) as ir:
                                img_data = ir.read()
                        except: pass
                
                # YouTube stores upload_date as YYYYMMDD
                year = entry.get("upload_date", "")[:4] if entry.get("upload_date") else None
                genre = None # extract_flat doesn't usually provide reliable genres
                return img_data, year, genre
    except Exception as e:
        log.debug("YouTube metadata fetch failed for %s - %s: %s", artist, title, e)
    return None, None, None

def enforce_primary_artist(
    file_path: Path,
    primary_artist: str,
    title: str,
    album: str,
    track_number: Optional[int],
    fetch_cover: bool = True,
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
            audio["title"] = [title]
            audio["artist"] = [primary_artist]
            audio["albumartist"] = [primary_artist]
            if album:
                audio["album"] = [album]
            if track_number:
                audio["tracknumber"] = [str(track_number)]
            
            # Always fix bad/missing genre
            existing_genre = audio.get("genre", [""])[0] if "genre" in audio else ""
            need_cover = fetch_cover and "metadata_block_picture" not in audio
            need_genre = _is_bad_genre(existing_genre)
            
            if need_cover or need_genre:
                img_data, year, genre = _fetch_itunes_cover(primary_artist, title)
                if not img_data:
                    y_img, y_year, y_genre = _fetch_youtube_metadata(primary_artist, title)
                    img_data = img_data or y_img
                    year = year or y_year
                    genre = genre or y_genre
                # If still no genre, search by artist name alone
                if _is_bad_genre(genre):
                    genre = _fetch_artist_genre(primary_artist)
                if year:
                    audio["date"] = [str(year)]
                if genre and not _is_bad_genre(genre):
                    audio["genre"] = [genre]
                elif _is_bad_genre(existing_genre) and "genre" in audio:
                    del audio["genre"]
                if need_cover and img_data:
                    from mutagen.flac import Picture
                    import base64
                    pic = Picture()
                    pic.type = 3 # front cover
                    pic.mime = "image/jpeg"
                    pic.desc = "Cover"
                    pic.data = img_data
                    audio["metadata_block_picture"] = [base64.b64encode(pic.write()).decode("ascii")]
            
            audio.save()
            return True

        elif ext == ".mp3":
            from mutagen.mp3 import MP3
            from mutagen.id3 import ID3, TIT2, TPE1, TALB, TRCK, APIC, TCON, TDRC, ID3NoHeaderError
            try:
                audio = MP3(file_path, ID3=ID3)
                if audio.tags is None:
                    audio.add_tags()
            except Exception as e:
                log.error("Failed to read MP3 %s: %s", file_path, e)
                return False
                
            tags = audio.tags
            tags.add(TIT2(encoding=3, text=title))
            tags.add(TPE1(encoding=3, text=primary_artist))
            if album:
                tags.add(TALB(encoding=3, text=album))
            if track_number:
                tags.add(TRCK(encoding=3, text=str(track_number)))
            
            # Always fix bad/missing genre for mp3
            existing_genre = str(tags.get("TCON", ""))
            need_cover = fetch_cover and not tags.getall("APIC")
            need_genre = _is_bad_genre(existing_genre)
            
            if need_cover or need_genre:
                img_data, year, genre = _fetch_itunes_cover(primary_artist, title)
                if not img_data:
                    y_img, y_year, y_genre = _fetch_youtube_metadata(primary_artist, title)
                    img_data = img_data or y_img
                    year = year or y_year
                    genre = genre or y_genre
                if _is_bad_genre(genre):
                    genre = _fetch_artist_genre(primary_artist)
                if year:
                    tags.add(TDRC(encoding=3, text=str(year)))
                if genre and not _is_bad_genre(genre):
                    tags.add(TCON(encoding=3, text=genre))
                elif _is_bad_genre(existing_genre) and "TCON" in tags:
                    tags.pop("TCON", None)
                if need_cover and img_data:
                    tags.add(APIC(
                        encoding=3,
                        mime="image/jpeg",
                        type=3,
                        desc="Cover",
                        data=img_data
                    ))

            audio.save(v2_version=3)
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
                
                # Genre check/fix for fallback
                existing_genre = audio.get("genre", [""])[0] if "genre" in audio else ""
                if _is_bad_genre(existing_genre):
                    img_data, year, genre = _fetch_itunes_cover(primary_artist, title)
                    if _is_bad_genre(genre):
                        genre = _fetch_artist_genre(primary_artist)
                    if genre and not _is_bad_genre(genre):
                        audio["genre"] = [genre]
                    elif "genre" in audio:
                        del audio["genre"]
                        
                audio.save()
                return True

    except Exception as e:
        log.error("Failed to tag %s: %s", file_path, e)
    return False


# ─────────────────────────────────────────────────────────────────────────────
# yt-dlp downloader
# ─────────────────────────────────────────────────────────────────────────────

def _is_valid_netscape_cookies(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size == 0:
        return False
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line_strip = line.strip()
                if not line_strip:
                    continue
                # Netscape cookie file header comments
                if line_strip.startswith("# Netscape HTTP Cookie File") or line_strip.startswith("# HTTP Cookie File"):
                    return True
                # Or standard tab-separated cookie line
                if len(line_strip.split("\t")) >= 4:
                    return True
                break
    except Exception:
        pass
    return False


def _build_cookie_opts(
    cookies_file: Optional[str] = None,
    cookies_from_browser: Optional[str] = None,
) -> dict:
    """yt-dlp cookie options from config (file path and/or browser name)."""
    opts: dict = {}
    if cookies_file:
        path = Path(cookies_file).expanduser()
        if path.is_file():
            if _is_valid_netscape_cookies(path):
                opts["cookiefile"] = str(path)
            else:
                log.warning("youtube_cookies_file '%s' is not a valid Netscape format cookies file — skipping it to prevent download failure.", path)
        else:
            log.warning("youtube_cookies_file not found: %s", path)
    browser = (cookies_from_browser or "").strip()
    if browser:
        opts["cookiesfrombrowser"] = (browser,)
    return opts


class YtDlpDownloader:
    """
    Thin wrapper around yt-dlp that:
      - Searches YouTube Music for a track by "artist - title" query.
      - Downloads the best audio stream as Opus.
      - Optionally downloads a full album by searching "artist album full album".
    """

    def __init__(
        self,
        music_dir: Path,
        fmt: str = "opus",
        cookies_file: Optional[str] = None,
        cookies_from_browser: Optional[str] = None,
    ):
        self.music_dir = music_dir
        self.fmt = fmt
        self._cookie_opts = _build_cookie_opts(cookies_file, cookies_from_browser)

    # ── internal: build ydl_opts ────────────────────────────────────────────

    def _ydl_opts(self, out_template: str, quiet: bool = True) -> dict:
        opts = {
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
            "noprogress": True,
            "ignoreerrors": True,
            "retries": 3,
            "fragment_retries": 5,
            "extractor_args": {
                "youtube": {"skip": ["dash", "hls"]},
            },
            # Prefer YouTube Music results when using ytsearch
            "default_search": "https://music.youtube.com/search?q=",
        }
        opts.update(self._cookie_opts)
        return opts

    def _base_opts(self, quiet: bool = True) -> dict:
        """Search/extract-only options (includes cookies when configured)."""
        opts = {
            "quiet": quiet,
            "no_warnings": True,
            "ignoreerrors": True,
            "retries": 3,
            "fragment_retries": 5,
        }
        opts.update(self._cookie_opts)
        return opts

    # ── search → first result URL ────────────────────────────────────────────

    def _search_url(self, query: str) -> Optional[str]:
        """Return the YouTube URL of the first search hit, or None."""
        if yt_dlp is None:
            return None
        opts = {
            **self._base_opts(),
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

        # Check if we're rate-limited
        rate_wait = check_rate_limit()
        if rate_wait is not None:
            log.error("    ✗ YouTube rate-limited. Wait %d seconds before retrying.", rate_wait)
            return None

        # Build the output path
        resolved_album = detect_singles(album, title)
        artist_folder = self.music_dir / _clean_filename(artist)
        safe_album = _clean_filename(resolved_album) if resolved_album else ""
        album_folder = artist_folder / safe_album if safe_album else artist_folder
        album_folder.mkdir(parents=True, exist_ok=True)

        safe_title = _clean_filename(title)
        if track_number and safe_album != "Singles":
            trk = str(track_number).zfill(2)
            out_path = album_folder / f"{trk} - {safe_title}.{self.fmt}"
        else:
            out_path = album_folder / f"{safe_title}.{self.fmt}"

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

        rel = out_path.relative_to(self.music_dir)
        log.info("    ↳ Downloading: %s → %s", title, rel)

        # yt-dlp writes {outtmpl}.{ext}; we give it the path without extension
        out_tpl = str(out_path.with_suffix("")) + ".%(ext)s"
        opts = self._ydl_opts(out_tpl)

        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([yt_url])
        except Exception as e:
            # Detect YouTube rate limiting
            err_msg = str(e).lower()
            if "rate-limited" in err_msg or "video unavailable" in err_msg:
                global RATE_LIMITED_UNTIL
                RATE_LIMITED_UNTIL = time.time() + 3600  # 1 hour
                log.error("    ✗ yt-dlp rate limit encountered, pausing downloads for 1 hour")
            else:
                log.error("    ✗ yt-dlp error for %s: %s", title, e)
            return None

        # yt-dlp might have written a slightly different extension; locate the file
        actual = self._find_downloaded(album_folder, safe_title)
        if actual is None:
            log.warning("    ✗ File not found after download: %s", out_path)
            return None

        # Rename to canonical path if needed
        if actual != out_path:
            actual.rename(out_path)

        return out_path

    def _find_downloaded(self, folder: Path, safe_title: str) -> Optional[Path]:
        """Locate the file yt-dlp wrote (may be .webm, .m4a before conversion)."""
        audio_exts = {".opus", ".webm", ".m4a", ".mp3", ".ogg"}
        for f in folder.iterdir():
            if f.suffix.lower() in audio_exts and safe_title.lower() in f.stem.lower():
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

        # Check if we're rate-limited
        rate_wait = check_rate_limit()
        if rate_wait is not None:
            log.error("  ✗ YouTube rate-limited. Wait %d seconds before retrying.", rate_wait)
            return 0

        query = f"ytsearch1:{artist} {album} full album"
        log.info("  ↳ Album search: %s — %s", artist, album)

        # First, search for a playlist/album result
        search_opts = {
            **self._base_opts(),
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
    enabled: bool = True,
) -> int:
    """
    For a given artist, look at all their albums in the DB.
    If an album has fewer downloaded tracks than expected, queue a full album
    download via yt-dlp.
    Returns the number of albums that were (re-)downloaded.
    Set enabled=False when the artist has a per-track download cap (album mode
    would fetch entire albums and bypass the limit).
    """
    if not enabled:
        return 0
    conn = sqlite3.connect(db_path, timeout=60.0)
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
    Scans the music directory for flat files (e.g. {artist}/{title}.ext),
    reads their embedded tags, and moves them to {artist}/{album}/{title}.ext
    """
    import mutagen
    import shutil
    
    if not music_dir.exists():
        log.error("Music dir not found")
        return

    migrated = 0
    audio_exts = {".opus", ".mp3", ".m4a", ".flac", ".ogg", ".webm"}

    for artist_folder in music_dir.iterdir():
        if not artist_folder.is_dir():
            continue

        # Find ALL audio files inside artist_folder recursively
        for f in artist_folder.rglob("*"):
            if not f.is_file() or f.suffix.lower() not in audio_exts:
                continue

            try:
                # Fallbacks in case tags are missing
                album = "Unknown Album"
                title = f.stem

                # Read tags
                try:
                    audio = mutagen.File(f, easy=True)
                    if audio:
                        if audio.get("album"):
                            album = audio.get("album")[0]
                        if audio.get("title"):
                            title = audio.get("title")[0]
                except Exception as e:
                    log.warning("Could not read tags for %s: %s", f.name, e)

                safe_album = _clean_filename(album)
                safe_title = _clean_filename(title)

                # Group singles/unknowns together to prevent thousands of 1-song album folders
                if safe_album.lower() == safe_title.lower() or safe_album in ("Unknown Album", ""):
                    safe_album = "Singles"
                    
                    # Update the tag inside the file so other players know it's a Single
                    try:
                        audio = mutagen.File(f, easy=True)
                        if audio:
                            audio["album"] = "Singles"
                            audio.save()
                    except Exception:
                        pass

                album_folder = artist_folder / safe_album
                album_folder.mkdir(parents=True, exist_ok=True)
                
                dst = album_folder / f"{safe_title}{f.suffix}"

                if f != dst:
                    shutil.move(str(f), str(dst))
                    log.info("Moved: %s → %s", f.name, dst.relative_to(music_dir))
                    migrated += 1
                    
                    # Clean up old empty folder if this was a 1-song album folder
                    old_parent = f.parent
                    if old_parent != artist_folder and not any(old_parent.iterdir()):
                        try:
                            old_parent.rmdir()
                        except OSError:
                            pass

            except Exception as e:
                log.error("Failed to move %s: %s", f.name, e)

    log.info("migrate_structure: moved %d flat files into album folders.", migrated)
