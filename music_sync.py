#!/usr/bin/env python3
"""
music_sync.py — Automated music library manager for Jellyfin
─────────────────────────────────────────────────────────────
- Scans Spotify playlists → discovers artists
- Scans each artist's albums/songs into SQLite
- Downloads discographies via spotDL (MP3 320k, full metadata)
- Tracks per-album / per-song download status
─────────────────────────────────────────────────────────────
Usage:
  musicadet                              # Full sync (default)
  musicadet scan                         # Scan playlists only
  musicadet scan-artists                 # Scan artist albums into DB
  musicadet artists-sync                 # Download/sync discographies
  musicadet reconcile                    # Match files ↔ DB
  musicadet fix-metadata [--artist NAME] # Re-embed tags/cover/lyrics
  musicadet list-albums [artist]
  musicadet add "Artist Name"
"""

import hashlib
import json
import logging
import argparse
import os
import re
import sqlite3
import subprocess
import concurrent.futures
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import custom_dl
except ImportError:
    custom_dl = None

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).resolve().parent


def _detect_base() -> Path:
    for candidate in (Path("/opt/musicadet"), Path("/opt/music-sync"), _SCRIPT_DIR):
        if candidate.exists() and (candidate / "config.json").exists():
            return candidate
    return _SCRIPT_DIR


BASE = _detect_base()
CFG_FILE = BASE / "config.json"

DEFAULTS: dict = {
    "music_dir": "/mnt/storage_jellyfin/media/music",
    "sync_dir": str(BASE / "sync-data"),
    "db_path": str(BASE / "music.db"),
    "log_dir": "/var/log/musicadet",
    "format": "opus",
    "bitrate": "320k",
    "threads": 4,
    # Legacy flat template kept for reference; new downloads use album structure
    "output_template": "{artist}/{title}.{output-ext}",
    # New: album-structured output  →  artist/album/NN - title.ext
    "album_output_template": "{artist}/{album}/{track-number:02d} - {title}.{output-ext}",
    "download_concurrency": 1,          # sequential — no rate-limit issues
    "playlist_save_timeout": 600,
    "playlist_save_retries": 3,
    "artist_save_timeout": 900,
    "lyrics_providers": ["genius", "musixmatch", "azlyrics"],
    "generate_lrc": False,
    "scan_concurrency": 4,
    "sync_concurrency": 1,              # sequential artist sync
    "verified_artists": [],             # populated from config.json
    "playlists": [
        {"name": "Today's Top Hits", "url": "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"},
        {"name": "Top 50 Romania", "url": "https://open.spotify.com/playlist/37i9dQZEVXbNZbJ6TZelCq"},
        {"name": "Top 50 Global", "url": "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"},
        {"name": "Top melodii Romania", "url": "https://open.spotify.com/playlist/37i9dQZEVXbMeCoUmQDLUW"},
    ],
}

AUDIO_EXTS = {".mp3", ".opus", ".m4a", ".flac", ".ogg", ".wav", ".aac", ".webm"}


def load_cfg() -> dict:
    cfg = DEFAULTS.copy()
    if CFG_FILE.exists():
        try:
            cfg.update(json.loads(CFG_FILE.read_text(encoding="utf-8")))
        except json.JSONDecodeError as e:
            print(f"Warning: config.json parse error: {e}")
    return cfg


CFG = load_cfg()


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

Path(CFG["log_dir"]).mkdir(parents=True, exist_ok=True)
_log_file = Path(CFG["log_dir"]) / f"sync-{datetime.now():%Y-%m-%d}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(_log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("musicadet")


# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(CFG["db_path"], timeout=60.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _migrate_columns(db: sqlite3.Connection) -> None:
    """Add columns to existing tables if missing."""
    artist_cols = {r[1] for r in db.execute("PRAGMA table_info(artists)")}
    if "albums_scanned_at" not in artist_cols:
        db.execute("ALTER TABLE artists ADD COLUMN albums_scanned_at TEXT")


def db_init():
    """Create / migrate the database schema."""
    with db_connect() as db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS artists (
                spotify_id   TEXT PRIMARY KEY,
                name         TEXT NOT NULL,
                source       TEXT DEFAULT 'manual',
                active       INTEGER DEFAULT 1,
                sync_done    INTEGER DEFAULT 0,
                last_synced  TEXT,
                albums_scanned_at TEXT,
                added_at     TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS playlists (
                spotify_id   TEXT PRIMARY KEY,
                name         TEXT NOT NULL,
                url          TEXT NOT NULL,
                active       INTEGER DEFAULT 1,
                last_synced  TEXT
            );

            CREATE TABLE IF NOT EXISTS playlist_artists (
                playlist_id  TEXT NOT NULL REFERENCES playlists(spotify_id) ON DELETE CASCADE,
                artist_id    TEXT NOT NULL REFERENCES artists(spotify_id)   ON DELETE CASCADE,
                PRIMARY KEY  (playlist_id, artist_id)
            );

            CREATE TABLE IF NOT EXISTS albums (
                spotify_id       TEXT PRIMARY KEY,
                artist_id        TEXT NOT NULL REFERENCES artists(spotify_id) ON DELETE CASCADE,
                name             TEXT NOT NULL,
                release_year     TEXT,
                track_count      INTEGER DEFAULT 0,
                downloaded_count INTEGER DEFAULT 0,
                last_scanned     TEXT,
                UNIQUE(artist_id, name)
            );

            CREATE TABLE IF NOT EXISTS songs (
                spotify_id          TEXT PRIMARY KEY,
                album_id            TEXT NOT NULL REFERENCES albums(spotify_id) ON DELETE CASCADE,
                artist_id           TEXT NOT NULL REFERENCES artists(spotify_id) ON DELETE CASCADE,
                title               TEXT NOT NULL,
                track_number        INTEGER,
                status              TEXT DEFAULT 'pending',
                file_path           TEXT,
                has_cover           INTEGER DEFAULT 0,
                has_lyrics          INTEGER DEFAULT 0,
                has_core_tags       INTEGER DEFAULT 0,
                metadata_checked_at TEXT,
                last_error          TEXT,
                updated_at          TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_artists_active ON artists(active, sync_done);
            CREATE INDEX IF NOT EXISTS idx_songs_status ON songs(status);
            CREATE INDEX IF NOT EXISTS idx_albums_artist ON albums(artist_id);
        """)
        _migrate_columns(db)

        for pl in CFG.get("playlists", []):
            pid = _extract_id_from_url(pl["url"], "playlist")
            if pid:
                db.execute("""
                    INSERT INTO playlists (spotify_id, name, url) VALUES (?,?,?)
                    ON CONFLICT(spotify_id) DO UPDATE
                        SET name=excluded.name, url=excluded.url
                """, (pid, pl["name"], pl["url"]))

    log.info("DB ready: %s", CFG["db_path"])


def _extract_id_from_url(url: str, kind: str) -> Optional[str]:
    marker = f"/{kind}/"
    if marker in url:
        return url.split(marker)[1].split("?")[0].split("/")[0]
    if url.startswith(f"spotify:{kind}:"):
        return url.split(":")[-1]
    return None


def _artist_key(entry: str) -> tuple[str, str]:
    if "open.spotify.com/artist/" in entry:
        sid = _extract_id_from_url(entry, "artist")
        return sid, f"artist:{sid}"
    if entry.startswith("spotify:artist:"):
        sid = entry.split(":")[-1]
        return sid, f"artist:{sid}"
    safe = entry.lower().replace(" ", "_")[:60]
    return f"q:{safe}", entry


def _normalize_spotify_id(raw) -> Optional[str]:
    if not raw:
        return None
    s = str(raw)
    if "/" in s:
        s = s.split("/")[-1].split("?")[0]
    return s or None


def _song_id_from_dict(song: dict) -> Optional[str]:
    for key in ("song_id", "track_id", "id", "spotify_id"):
        sid = _normalize_spotify_id(song.get(key))
        if sid:
            return sid
    url = song.get("url") or song.get("spotify_url") or ""
    if "/track/" in url:
        return _extract_id_from_url(url, "track")
    return None


def _album_id_from_song(song: dict, artist_id: str) -> str:
    aid = _normalize_spotify_id(song.get("album_id") or song.get("albumId"))
    if aid:
        return aid
    album = (song.get("album") or song.get("album_name") or "Unknown").strip()
    digest = hashlib.md5(f"{artist_id}:{album}".encode(), usedforsecurity=False).hexdigest()[:16]
    return f"alb:{digest}"


def _album_name_from_song(song: dict) -> str:
    return (song.get("album") or song.get("album_name") or "Unknown").strip()


def _track_number_from_song(song: dict) -> Optional[int]:
    for key in ("track_number", "track", "trackNumber"):
        val = song.get(key)
        if val is None:
            continue
        try:
            return int(str(val).split("/")[0].strip())
        except (ValueError, TypeError):
            pass
    return None


def _title_from_song(song: dict) -> str:
    return (song.get("name") or song.get("title") or "Unknown").strip()


def _artist_target(spotify_id: str, name: str) -> str:
    if spotify_id.startswith("q:"):
        return name
    return f"https://open.spotify.com/artist/{spotify_id}"


# ─────────────────────────────────────────────────────────────────────────────
# Metadata verification (mutagen)
# ─────────────────────────────────────────────────────────────────────────────

def verify_song_metadata(path: Path) -> dict:
    """Return has_cover, has_lyrics, has_core_tags for an audio file."""
    result = {"has_cover": 0, "has_lyrics": 0, "has_core_tags": 0}
    if not path.exists():
        return result
    try:
        from mutagen.mp3 import MP3
        from mutagen.id3 import ID3
    except ImportError:
        log.debug("mutagen not installed — skipping metadata verify for %s", path)
        result["has_core_tags"] = 1
        return result

    try:
        ext = path.suffix.lower()
        if ext == ".mp3":
            audio = MP3(path, ID3=ID3)
            tags = audio.tags or {}
            result["has_cover"] = int(any(k.startswith("APIC") for k in tags))
            result["has_lyrics"] = int(any(k.startswith("USLT") or k.startswith("SYLT") for k in tags))
            result["has_core_tags"] = int(bool(
                tags.get("TIT2") and tags.get("TPE1") and tags.get("TALB")
            ))
        else:
            from mutagen import File as MutagenFile
            audio = MutagenFile(path)
            if audio is not None and audio.tags:
                tags = audio.tags
                result["has_core_tags"] = int(bool(
                    tags.get("title") and tags.get("artist") and tags.get("album")
                ))
                has_pic = bool(getattr(audio, "pictures", None)) or "metadata_block_picture" in tags
                result["has_cover"] = int(has_pic)
    except Exception as e:
        log.debug("metadata verify failed for %s: %s", path, e)
    return result


def _parse_title_from_filename(filename: str) -> str:
    name = Path(filename).stem
    m = re.match(r"^\d+\s*-\s*(.+)$", name)
    return (m.group(1) if m else name).strip().lower()


# ─────────────────────────────────────────────────────────────────────────────
# spotDL helpers
# ─────────────────────────────────────────────────────────────────────────────

def spotdl_save(url: str, timeout: Optional[int] = None) -> list:
    """Run `spotdl save URL` with retries. Returns song dicts or []."""
    timeout = timeout or int(CFG.get("playlist_save_timeout", 600))
    retries = int(CFG.get("playlist_save_retries", 3))
    backoff = [30, 60, 120]

    for attempt in range(retries):
        with tempfile.NamedTemporaryFile(suffix=".spotdl", delete=False) as f:
            tmp = Path(f.name)
        try:
            cmd = ["spotdl", "save", url, "--save-file", str(tmp)]
            r = subprocess.run(cmd, capture_output=True, timeout=timeout)
            if r.returncode != 0:
                log.debug("spotdl save stderr: %s", r.stderr.decode(errors="replace"))

            if tmp.exists() and tmp.stat().st_size >= 5:
                raw = json.loads(tmp.read_text(encoding="utf-8"))
                songs = raw if isinstance(raw, list) else raw.get("songs", [])
                if songs:
                    if attempt > 0:
                        log.info("  → succeeded on attempt %d", attempt + 1)
                    return songs
        except subprocess.TimeoutExpired:
            log.warning("Timeout fetching metadata (attempt %d/%d): %s", attempt + 1, retries, url)
        except json.JSONDecodeError as e:
            log.warning("JSON parse error from spotdl save: %s", e)
        except Exception as e:
            log.warning("spotdl save failed: %s", e)
        finally:
            tmp.unlink(missing_ok=True)

        if attempt < retries - 1:
            wait = backoff[min(attempt, len(backoff) - 1)]
            log.info("  → retrying in %ds...", wait)
            time.sleep(wait)

    return []


def _extract_artist_from_song(song: dict) -> Optional[tuple[str, str]]:
    sid = (
        song.get("artist_id")
        or song.get("main_artist_id")
        or (song.get("artist_ids") or [None])[0]
    )
    artists_field = song.get("artists") or song.get("artist") or []
    if isinstance(artists_field, list):
        name = artists_field[0] if artists_field else None
    else:
        name = str(artists_field) or None

    sid = _normalize_spotify_id(sid)
    if not sid or not name:
        return None
    return sid, str(name)


def _spotdl_flags(out_tpl: str) -> list:
    """Return spotdl option flags (no command prefix, no --lyrics).
    Callers must place positional args BEFORE --lyrics to avoid nargs='+' conflict."""
    args = [
        "--output", out_tpl,
        "--format", str(CFG["format"]),
        "--bitrate", str(CFG["bitrate"]),
        "--threads", str(CFG["threads"]),
        "--log-level", "WARNING",
        "--overwrite", "metadata",
        "--force-update-metadata",
    ]
    if CFG.get("generate_lrc"):
        args.append("--generate-lrc")
    return args


def _spotdl_lyrics_args() -> list:
    """Return --lyrics flag + values. MUST be the LAST args in the command
    since --lyrics uses nargs='+' and greedily consumes non-flag tokens."""
    lyrics = CFG.get("lyrics_providers") or ["genius", "musixmatch", "azlyrics"]
    return ["--lyrics"] + lyrics


def spotdl_sync_artist(spotify_id: str, name: str, target: str) -> bool:
    sync_file = Path(CFG["sync_dir"]) / f"{spotify_id.replace(':', '_')}.spotdl"
    out_tpl = str(Path(CFG["music_dir"]) / CFG["output_template"])
    flags = _spotdl_flags(out_tpl)
    lyrics_args = _spotdl_lyrics_args()

    is_new = not sync_file.exists()
    if is_new:
        # Positional target MUST come right after 'sync' and BEFORE --lyrics
        # because --lyrics uses nargs='+' and would consume the URL otherwise.
        cmd = ["spotdl", "sync", target, "--save-file", str(sync_file)] + flags + lyrics_args
        log.info("    ↳ First run — downloading full discography")
    else:
        cmd = ["spotdl", "sync", str(sync_file)] + flags + lyrics_args
        log.info("    ↳ Checking for new releases (incremental)")

    try:
        r = subprocess.run(cmd, timeout=7200)
        if r.returncode != 0 and is_new:
            if sync_file.exists() and sync_file.stat().st_size < 20:
                sync_file.unlink()
        return r.returncode == 0
    except subprocess.TimeoutExpired:
        log.error("    ✗ Timed out after 2 hours: %s", name)
        return False
    except Exception as e:
        log.error("    ✗ Error: %s", e)
        return False


def spotdl_fix_artist_metadata(spotify_id: str, name: str, target: str) -> bool:
    """Re-embed metadata for an artist via spotdl sync on existing .spotdl file."""
    sync_file = Path(CFG["sync_dir"]) / f"{spotify_id.replace(':', '_')}.spotdl"
    out_tpl = str(Path(CFG["music_dir"]) / CFG["output_template"])
    flags = _spotdl_flags(out_tpl)
    lyrics_args = _spotdl_lyrics_args()

    if sync_file.exists():
        cmd = ["spotdl", "sync", str(sync_file)] + flags + lyrics_args
    else:
        cmd = ["spotdl", "sync", target, "--save-file", str(sync_file)] + flags + lyrics_args

    try:
        r = subprocess.run(cmd, timeout=7200)
        return r.returncode == 0
    except Exception as e:
        log.error("    ✗ fix-metadata error: %s", e)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Album / song scanning & reconcile
# ─────────────────────────────────────────────────────────────────────────────

def _upsert_artist_catalog(db: sqlite3.Connection, artist_id: str, songs: list) -> tuple[int, int]:
    """Upsert albums/songs from spotdl save output. Returns (new_albums, new_songs)."""
    albums: dict[str, dict] = {}
    for song in songs:
        album_id = _album_id_from_song(song, artist_id)
        if album_id not in albums:
            year = song.get("year") or (str(song.get("release_date") or "")[:4] or None)
            albums[album_id] = {
                "name": _album_name_from_song(song),
                "year": year,
                "tracks": [],
            }
        albums[album_id]["tracks"].append(song)

    new_albums = new_songs = 0
    now = datetime.now().isoformat(timespec="seconds")

    for album_id, info in albums.items():
        existing_album = db.execute(
            "SELECT spotify_id FROM albums WHERE artist_id=? AND name=?",
            (artist_id, info["name"])
        ).fetchone()

        final_album_id = existing_album["spotify_id"] if existing_album else album_id

        if not existing_album:
            cur = db.execute("""
                INSERT INTO albums (spotify_id, artist_id, name, release_year, track_count, last_scanned)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(spotify_id) DO UPDATE SET
                    name=excluded.name,
                    release_year=COALESCE(excluded.release_year, albums.release_year),
                    track_count=excluded.track_count,
                    last_scanned=excluded.last_scanned
            """, (final_album_id, artist_id, info["name"], info["year"], len(info["tracks"]), now))
            if cur.rowcount == 1:
                new_albums += 1
        else:
            db.execute("""
                UPDATE albums SET 
                    release_year=COALESCE(?, release_year),
                    track_count=?, 
                    last_scanned=?
                WHERE spotify_id=?
            """, (info["year"], len(info["tracks"]), now, final_album_id))

        for song in info["tracks"]:
            song_id = _song_id_from_dict(song)
            if not song_id:
                digest = hashlib.md5(
                    f"{final_album_id}:{_title_from_song(song)}".encode(), usedforsecurity=False
                ).hexdigest()[:16]
                song_id = f"trk:{digest}"

            title = _title_from_song(song)
            track_num = _track_number_from_song(song)

            existing = db.execute(
                "SELECT status, file_path FROM songs WHERE spotify_id=?", (song_id,)
            ).fetchone()

            if existing and existing["status"] == "downloaded" and existing["file_path"]:
                fp = Path(existing["file_path"])
                if not fp.is_absolute():
                    fp = Path(CFG["music_dir"]) / fp
                if fp.exists():
                    db.execute("""
                        UPDATE songs SET album_id=?, artist_id=?, title=?, track_number=?, updated_at=?
                        WHERE spotify_id=?
                    """, (final_album_id, artist_id, title, track_num, now, song_id))
                    continue

            cur = db.execute("""
                INSERT INTO songs (spotify_id, album_id, artist_id, title, track_number, status, updated_at)
                VALUES (?,?,?,?,?,'pending',?)
                ON CONFLICT(spotify_id) DO UPDATE SET
                    album_id=excluded.album_id,
                    title=excluded.title,
                    track_number=excluded.track_number,
                    updated_at=excluded.updated_at
            """, (song_id, final_album_id, artist_id, title, track_num, now))
            if cur.rowcount == 1:
                new_songs += 1
            elif existing and existing["status"] != "downloaded":
                db.execute(
                    "UPDATE songs SET status='pending' WHERE spotify_id=? AND status='failed'",
                    (song_id,),
                )

    return new_albums, new_songs


def _build_file_index(music_dir: Path) -> tuple[dict, dict]:
    """Index by (artist, album, title) and (album, title) keys."""
    by_full: dict[tuple[str, str, str], Path] = {}
    by_album_title: dict[tuple[str, str], Path] = {}
    if not music_dir.exists():
        return by_full, by_album_title
    for root, _dirs, files in os.walk(music_dir):
        for fname in files:
            if Path(fname).suffix.lower() not in AUDIO_EXTS:
                continue
            full = Path(root) / fname
            rel = full.relative_to(music_dir)
            parts = rel.parts
            if len(parts) < 2:
                continue
            artist = parts[0].lower()
            album = parts[1].lower() if len(parts) > 2 else ""
            title = _parse_title_from_filename(fname)
            by_full[(artist, album, title)] = full
            if album:
                by_album_title[(album, title)] = full
    return by_full, by_album_title


def reconcile_artist_downloads(artist_id: str, artist_name: str, *, file_index=None) -> dict:
    """Match filesystem files to DB songs. Returns stats dict."""
    music_dir = Path(CFG["music_dir"])
    if file_index is not None:
        by_full, by_album_title = file_index
    else:
        by_full, by_album_title = _build_file_index(music_dir)
    stats = {"downloaded": 0, "cover": 0, "lyrics": 0, "pending": 0}

    with db_connect() as db:
        row = db.execute("SELECT name FROM artists WHERE spotify_id=?", (artist_id,)).fetchone()
        display_name = row["name"] if row else artist_name

        albums = db.execute(
            "SELECT spotify_id, name FROM albums WHERE artist_id=?", (artist_id,)
        ).fetchall()
        album_names = {r["spotify_id"]: r["name"] for r in albums}
        songs = db.execute("SELECT * FROM songs WHERE artist_id=?", (artist_id,)).fetchall()

        for song in songs:
            album_name = album_names.get(song["album_id"], "")
            title_key = song["title"].strip().lower()
            album_key = album_name.lower()
            
            resolved_album_key = ""
            if custom_dl:
                resolved_album_key = custom_dl._clean_filename(custom_dl.detect_singles(album_name, song["title"])).lower()

            found: Optional[Path] = None
            if song["file_path"]:
                fp = Path(song["file_path"])
                if not fp.is_absolute():
                    fp = music_dir / fp
                if fp.exists():
                    found = fp

            if not found:
                for key in [
                    (display_name.lower(), album_key, title_key),
                    (display_name.lower(), resolved_album_key, title_key),
                    (display_name.lower(), "", title_key),
                    (artist_id.lower(), album_key, title_key),
                    (artist_id.lower(), resolved_album_key, title_key),
                    (artist_id.lower(), "", title_key),
                ]:
                    if key in by_full:
                        found = by_full[key]
                        break

            if not found and album_key:
                found = by_album_title.get((album_key, title_key))
            if not found and resolved_album_key:
                found = by_album_title.get((resolved_album_key, title_key))

            now = datetime.now().isoformat(timespec="seconds")
            if found:
                rel = str(found.relative_to(music_dir))
                meta = verify_song_metadata(found)
                db.execute("""
                    UPDATE songs SET status='downloaded', file_path=?,
                        has_cover=?, has_lyrics=?, has_core_tags=?,
                        metadata_checked_at=?, updated_at=?
                    WHERE spotify_id=?
                """, (rel, meta["has_cover"], meta["has_lyrics"], meta["has_core_tags"], now, now, song["spotify_id"]))
                stats["downloaded"] += 1
                stats["cover"] += meta["has_cover"]
                stats["lyrics"] += meta["has_lyrics"]
            else:
                if song["status"] == "downloaded":
                    db.execute("""
                        UPDATE songs SET status='pending', file_path=NULL, updated_at=?
                        WHERE spotify_id=?
                    """, (now, song["spotify_id"]))
                stats["pending"] += 1

        for album_id, name in album_names.items():
            row = db.execute("""
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN status='downloaded' THEN 1 ELSE 0 END) AS done
                FROM songs WHERE album_id=?
            """, (album_id,)).fetchone()
            db.execute(
                "UPDATE albums SET track_count=?, downloaded_count=? WHERE spotify_id=?",
                (row["total"], row["done"] or 0, album_id),
            )

    return stats


def scan_artist_catalog(artist_row: sqlite3.Row) -> tuple[int, int]:
    """Scan one artist's discography into albums/songs tables."""
    sid = artist_row["spotify_id"]
    name = artist_row["name"]
    
    if sid.startswith("local:"):
        log.info("  → Skipping local artist catalog scan: %s", name)
        return 0, 0

    import ytm_scanner
    songs = ytm_scanner.scan_artist(name)
    
    if not songs:
        log.warning("  → No songs returned for %s", name)
        return 0, 0

    with db_connect() as db:
        new_albums, new_songs = _upsert_artist_catalog(db, sid, songs)
        db.execute(
            "UPDATE artists SET albums_scanned_at=datetime('now') WHERE spotify_id=?",
            (sid,),
        )

    album_count = len({ _album_id_from_song(s, sid) for s in songs })
    log.info("  → %d albums, %d tracks (%d new albums, %d new tracks)",
             album_count, len(songs), new_albums, new_songs)
    return new_albums, new_songs


# ─────────────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_artist_display_name(spotify_id: str, target: str, fallback: str) -> str:
    if not spotify_id.startswith("q:"):
        songs = spotdl_save(target, timeout=min(120, int(CFG.get("artist_save_timeout", 900))))
        if songs:
            result = _extract_artist_from_song(songs[0])
            if result:
                return result[1]
    return fallback


def cmd_add(args):
    entry = args.artist.strip()
    sid, name = _artist_key(entry)
    if not sid.startswith("q:") and name.startswith("artist:"):
        name = _resolve_artist_display_name(sid, _artist_target(sid, name), name)
    with db_connect() as db:
        cur = db.execute("""
            INSERT INTO artists (spotify_id, name, source) VALUES (?,?,'manual')
            ON CONFLICT(spotify_id) DO UPDATE SET active=1, name=excluded.name
        """, (sid, name))
    action = "Added" if cur.rowcount else "Already exists (re-enabled)"
    log.info("%s: %s  [%s]", action, name, sid)


def cmd_import(args):
    path = Path(args.file)
    if not path.exists():
        log.error("File not found: %s", args.file)
        return
    added = 0
    with db_connect() as db:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            sid, name = _artist_key(line)
            cur = db.execute("""
                INSERT INTO artists (spotify_id, name, source) VALUES (?,?,'manual')
                ON CONFLICT DO NOTHING
            """, (sid, name))
            if cur.rowcount:
                added += 1
                log.info("  + %s", name)
    log.info("Imported %d new artists from %s", added, args.file)


def cmd_list(args):
    with db_connect() as db:
        rows = db.execute("""
            SELECT name, spotify_id, source, active, sync_done, last_synced, albums_scanned_at
            FROM artists ORDER BY name COLLATE NOCASE
        """).fetchall()
        pl_count = db.execute("SELECT COUNT(*) FROM playlists WHERE active=1").fetchone()[0]
        album_count = db.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
        song_dl = db.execute("SELECT COUNT(*) FROM songs WHERE status='downloaded'").fetchone()[0]
        song_total = db.execute("SELECT COUNT(*) FROM songs").fetchone()[0]

    total = len(rows)
    synced = sum(1 for r in rows if r["sync_done"])
    disabled = sum(1 for r in rows if not r["active"])

    print(f"\n{'Artist':<42} {'Source':<26} {'Sync':>4}  Last synced")
    print("─" * 88)
    for r in rows:
        done = "✓" if r["sync_done"] else "·"
        last = (r["last_synced"] or "never")[:10]
        flag = " [off]" if not r["active"] else ""
        print(f"{r['name']:<42} {r['source']:<26} {done:>4}  {last}{flag}")

    print(f"\n{total} artists ({synced} synced, {total - synced - disabled} pending, {disabled} disabled)")
    print(f"{pl_count} playlists | {album_count} albums | {song_dl}/{song_total} songs downloaded")


def cmd_scan(args):
    with db_connect() as db:
        playlists = db.execute("SELECT * FROM playlists WHERE active=1").fetchall()

    if not playlists:
        log.warning("No playlists in DB. Edit config.json and re-run.")
        return

    grand_total = 0
    for pl in playlists:
        log.info("Scanning playlist: %s", pl["name"])
        songs = spotdl_save(pl["url"])
        if not songs:
            log.warning("  → No songs returned (network issue or invalid URL?)")
            continue

        log.info("  → %d songs found", len(songs))
        new_artists = 0
        with db_connect() as db:
            for song in songs:
                result = _extract_artist_from_song(song)
                if not result:
                    continue
                sid, name = result
                cur = db.execute("""
                    INSERT INTO artists (spotify_id, name, source) VALUES (?,?,?)
                    ON CONFLICT DO NOTHING
                """, (sid, name, f"playlist:{pl['name']}"))
                if cur.rowcount:
                    new_artists += 1
                    log.info("  ✦ New artist: %s", name)
                db.execute(
                    "INSERT INTO playlist_artists VALUES (?,?) ON CONFLICT DO NOTHING",
                    (pl["spotify_id"], sid),
                )
            db.execute(
                "UPDATE playlists SET last_synced=datetime('now') WHERE spotify_id=?",
                (pl["spotify_id"],),
            )
        log.info("  → %d new artists discovered", new_artists)
        grand_total += new_artists

    log.info("Playlist scan complete — %d new artists total", grand_total)


def cmd_scan_artists(args):
    new_only = getattr(args, "new_only", False)
    with db_connect() as db:
        query = "SELECT * FROM artists WHERE active=1"
        if new_only:
            query += " AND albums_scanned_at IS NULL"
        artists = db.execute(query + " ORDER BY name COLLATE NOCASE").fetchall()

    tag = " (new only)" if new_only else ""
    workers = min(int(CFG.get("scan_concurrency", 4)), max(len(artists), 1))
    log.info("Scanning artist catalogs%s: %d (×%d workers)", tag, len(artists), workers)
    total = len(artists)

    def _scan(i_a):
        i, a = i_a
        log.info("[%d/%d] %s", i, total, a["name"])
        return scan_artist_catalog(a)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(_scan, enumerate(artists, 1)))
    log.info("Artist catalog scan complete")


def _sync_artist_with_ytdlp(sid: str, name: str, songs: list, index: int, total: int) -> bool:
    """
    Core sequential download loop for one artist.

    Pipeline per-track:
      1. SpotDL gave us song metadata (album, track_number, yt_url if present).
      2. Upsert into DB.
      3. For each pending track → YtDlpDownloader.download_track().
      4. Embed metadata via enforce_primary_artist.
      5. After all tracks → check_and_complete_artist_albums (album completeness).
    """
    if not custom_dl:
        log.error("custom_dl module not available — cannot download")
        return False

    music_dir = Path(CFG["music_dir"])
    db_path = Path(CFG["db_path"])
    fmt = CFG.get("format", "opus")

    downloader = custom_dl.YtDlpDownloader(music_dir, fmt=fmt)

    # Build a lookup: spotify_id → yt_url from spotdl JSON
    # spotdl save stores the YouTube URL in the 'download_url' field
    yt_url_map: dict = {}
    for song in songs:
        song_id = _song_id_from_dict(song)
        yt_url = (
            song.get("download_url")
            or song.get("youtube_url")
            or song.get("url")
            or None
        )
        if song_id and yt_url and "youtube" in str(yt_url):
            yt_url_map[song_id] = yt_url

    # Fetch pending songs from DB for this artist
    with db_connect() as db:
        pending_rows = db.execute(
            """
            SELECT s.spotify_id, s.title, s.track_number, s.status, s.file_path,
                   al.name AS album_name
            FROM songs s
            JOIN albums al ON s.album_id = al.spotify_id
            WHERE s.artist_id = ? AND s.status != 'downloaded'
            ORDER BY al.name, s.track_number
            """,
            (sid,),
        ).fetchall()

    log.info("[%d/%d] %s — %d tracks to download", index, total, name, len(pending_rows))

    success = True
    for s in pending_rows:
        yt_url = yt_url_map.get(s["spotify_id"])
        track_num = s["track_number"]
        album_name = s["album_name"] or "Unknown Album"
        
        # If spotify_id is 11 chars (YouTube videoId) and doesn't contain colon, use it directly
        s_id = s["spotify_id"]
        if len(s_id) == 11 and ":" not in s_id:
            yt_url = f"https://music.youtube.com/watch?v={s_id}"

        # Check if file already exists on disk
        safe_artist = custom_dl._clean_filename(name)
        resolved_album = custom_dl.detect_singles(album_name, s["title"])
        safe_album = custom_dl._clean_filename(resolved_album)
        safe_title = custom_dl._clean_filename(s["title"])
        if track_num and safe_album != "Singles":
            trk = str(track_num).zfill(2)
            expected = music_dir / safe_artist / safe_album / f"{trk} - {safe_title}.{fmt}"
        else:
            expected = music_dir / safe_artist / safe_album / f"{safe_title}.{fmt}"

        if expected.exists():
            with db_connect() as db:
                rel = str(expected.relative_to(music_dir))
                db.execute(
                    "UPDATE songs SET status='downloaded', file_path=? WHERE spotify_id=?",
                    (rel, s["spotify_id"]),
                )
            log.info("    ✓ Already on disk: %s", expected.name)
            continue

        # Download
        result = downloader.download_track(
            artist=name,
            title=s["title"],
            album=album_name,
            track_number=track_num,
            yt_url=yt_url,
        )

        with db_connect() as db:
            if result and result.exists():
                # Embed metadata
                custom_dl.enforce_primary_artist(
                    result, name, s["title"], album_name, track_num
                )
                rel = str(result.relative_to(music_dir))
                db.execute(
                    "UPDATE songs SET status='downloaded', file_path=?, updated_at=datetime('now') WHERE spotify_id=?",
                    (rel, s["spotify_id"]),
                )
                log.info("    ✓ Downloaded: %s", result.name)
            else:
                db.execute(
                    "UPDATE songs SET status='failed', last_error='yt-dlp search returned no result', updated_at=datetime('now') WHERE spotify_id=?",
                    (s["spotify_id"],),
                )
                success = False

    # ── Album completeness check ─────────────────────────────────────────────
    log.info("  ↳ Checking album completeness for: %s", name)
    reconcile_artist_downloads(sid, name)
    fixed = custom_dl.check_and_complete_artist_albums(
        db_path, music_dir, sid, name, downloader
    )
    if fixed:
        log.info("  ↳ Downloaded %d missing album(s) for %s", fixed, name)
        reconcile_artist_downloads(sid, name)

    # Mark artist as synced if no failures
    with db_connect() as db:
        if success:
            db.execute(
                "UPDATE artists SET sync_done=1, last_synced=datetime('now') WHERE spotify_id=?",
                (sid,),
            )

    return success


def cmd_artists_sync(args):
    """Sequential artist sync: SpotDL fetch → yt-dlp download → album completeness."""
    new_only = getattr(args, "new_only", False)
    with db_connect() as db:
        query = "SELECT * FROM artists WHERE active=1"
        if new_only:
            query += " AND sync_done=0"
        artists = db.execute(query + " ORDER BY name COLLATE NOCASE").fetchall()

    tag = " (new only)" if new_only else ""
    log.info("Artists to sync%s: %d (sequential)", tag, len(artists))
    total = len(artists)

    ok = failed = 0
    for i, a in enumerate(artists, 1):
        sid, name = a["spotify_id"], a["name"]

        # Step 2: Download pending tracks + album completeness check
        result = _sync_artist_with_ytdlp(sid, name, [], i, total)
        if result:
            ok += 1
        else:
            failed += 1

    log.info("Artists sync done — ✓ %d  ✗ %d", ok, failed)

def cmd_download_pending(args):
    """Bypasses SpotDL entirely and just downloads pending songs currently in the DB."""
    with db_connect() as db:
        # Get artists that actually have pending songs
        artists = db.execute("""
            SELECT DISTINCT a.spotify_id, a.name 
            FROM artists a 
            JOIN songs s ON s.artist_id = a.spotify_id 
            WHERE a.active=1 AND s.status != 'downloaded'
            ORDER BY a.name COLLATE NOCASE
        """).fetchall()

    workers = min(int(CFG.get("scan_concurrency", 4)), max(len(artists), 1))
    log.info("Artists with pending tracks: %d (×%d workers)", len(artists), workers)
    total = len(artists)

    def _dl_worker(i_a):
        i, a = i_a
        sid, name = a["spotify_id"], a["name"]
        return _sync_artist_with_ytdlp(sid, name, [], i, total)

    ok = failed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_dl_worker, enumerate(artists, 1)))
        
    for res in results:
        if res: ok += 1
        else: failed += 1

    log.info("Download pending complete — ✓ %d  ✗ %d", ok, failed)

def cmd_sync_playlist(args):
    """
    Sync a single Spotify playlist URL end-to-end:
      1. SpotDL saves the playlist → JSON (26 tracks, N artists).
      2. For each track:  yt-dlp downloads it sequentially.
      3. For each unique primary artist: album completeness check.

    This is the "playlist-first" entry point — ideal for ad-hoc syncs
    triggered from the web UI or CLI.
    """
    url = args.url.strip()
    log.info("━" * 60)
    log.info("  SYNC PLAYLIST — %s", url)
    log.info("━" * 60)

    if not custom_dl:
        log.error("custom_dl module not available — cannot download")
        return

    music_dir = Path(CFG["music_dir"])
    db_path = Path(CFG["db_path"])
    fmt = CFG.get("format", "opus")
    downloader = custom_dl.YtDlpDownloader(music_dir, fmt=fmt)

    # ── Step 1: SpotDL fetch ─────────────────────────────────────────────────
    log.info("\n▶ Step 1 — Fetching playlist metadata via SpotDL")
    songs = spotdl_save(url, timeout=int(CFG.get("playlist_save_timeout", 600)))
    if not songs:
        log.error("SpotDL returned no tracks. Check the URL or network connection.")
        return
    log.info("  → %d tracks found in playlist", len(songs))

    # ── Step 2: Upsert artists + albums + songs into DB ──────────────────────
    log.info("\n▶ Step 2 — Updating database")
    pl_id = _extract_id_from_url(url, "playlist")
    artist_ids_found: set[tuple[str, str]] = set()   # (spotify_id, name)

    with db_connect() as db:
        for song in songs:
            result = _extract_artist_from_song(song)
            if not result:
                continue
            sid, name = result
            db.execute(
                """
                INSERT INTO artists (spotify_id, name, source) VALUES (?,?,'playlist')
                ON CONFLICT(spotify_id) DO UPDATE SET
                    name = excluded.name,
                    active = 1
                """,
                (sid, name),
            )
            artist_ids_found.add((sid, name))
            if pl_id:
                db.execute(
                    "INSERT INTO playlist_artists VALUES (?,?) ON CONFLICT DO NOTHING",
                    (pl_id, sid),
                )
            _upsert_artist_catalog(db, sid, songs)

    # ── Step 3: Download tracks sequentially ─────────────────────────────────
    log.info("\n▶ Step 3 — Downloading %d tracks (sequential)", len(songs))
    ok = failed = 0
    with db_connect() as db:
        for idx, song in enumerate(songs, 1):
            result = _extract_artist_from_song(song)
            primary_artist = result[1] if result else "Unknown Artist"
            title = _title_from_song(song)
            album = _album_name_from_song(song)
            track_num = _track_number_from_song(song)
            song_id = _song_id_from_dict(song)

            yt_url = (
                song.get("download_url")
                or song.get("youtube_url")
                or None
            )
            if yt_url and "youtube" not in str(yt_url):
                yt_url = None

            log.info("[%d/%d] %s — %s", idx, len(songs), primary_artist, title)

            # Detect Singles
            album = custom_dl.detect_singles(album, title)

            # Check if already downloaded
            if song_id:
                row = db.execute(
                    "SELECT status, file_path FROM songs WHERE spotify_id=?", (song_id,)
                ).fetchone()
                if row and row["status"] == "downloaded" and row["file_path"]:
                    fp = Path(row["file_path"])
                    if not fp.is_absolute():
                        fp = Path(CFG["music_dir"]) / fp
                    if fp.exists():
                        log.info("    ✓ Already downloaded")
                        ok += 1
                        continue

            log.info("[%d/%d] %s — %s", idx, len(songs), primary_artist, title)

            path = downloader.download_track(
                artist=primary_artist,
                title=title,
                album=album,
                track_number=track_num,
                yt_url=yt_url,
            )

            if path and path.exists():
                custom_dl.enforce_primary_artist(path, primary_artist, title, album, track_num)
                if song_id:
                    rel = str(path.relative_to(music_dir))
                    db.execute(
                        "UPDATE songs SET status='downloaded', file_path=?, updated_at=datetime('now') WHERE spotify_id=?",
                        (rel, song_id),
                    )
                ok += 1
            else:
                if song_id:
                    db.execute(
                        "UPDATE songs SET status='failed', last_error='yt-dlp no result', updated_at=datetime('now') WHERE spotify_id=?",
                        (song_id,),
                    )
                failed += 1

    log.info("  → Downloads: ✓ %d  ✗ %d", ok, failed)

    # ── Step 4: Album completeness for each unique artist ────────────────────
    log.info("\n▶ Step 4 — Checking album completeness for %d artist(s)", len(artist_ids_found))
    for artist_sid, artist_name in sorted(artist_ids_found, key=lambda x: x[1]):
        log.info("  Artist: %s", artist_name)
        fixed = custom_dl.check_and_complete_artist_albums(
            db_path, music_dir, artist_sid, artist_name, downloader
        )
        if fixed:
            reconcile_artist_downloads(artist_sid, artist_name)

    log.info("\n━" * 60)
    log.info("  PLAYLIST SYNC COMPLETE — ✓ %d downloaded, ✗ %d failed", ok, failed)
    log.info("━" * 60)


def cmd_download_direct(args):
    """
    Directly download a Spotify or YT Music playlist/track into the folder structure.
    Bypasses all database checks and album completion loops.
    """
    url = args.url.strip()
    log.info("━" * 60)
    log.info("  DIRECT DOWNLOAD — %s", url)
    log.info("━" * 60)

    if not custom_dl:
        log.error("custom_dl module not available — cannot download")
        return

    music_dir = Path(CFG["music_dir"])
    fmt = CFG.get("format", "opus")
    downloader = custom_dl.YtDlpDownloader(music_dir, fmt=fmt)

    # Auto-detect source
    if "spotify.com" in url:
        log.info("▶ Source: Spotify")
        songs = spotdl_save(url, timeout=int(CFG.get("playlist_save_timeout", 600)))
        if not songs:
            log.error("SpotDL returned no tracks.")
            return
        
        log.info("  → Found %d tracks. Downloading directly...", len(songs))
        ok = failed = 0
        for idx, song in enumerate(songs, 1):
            result = _extract_artist_from_song(song)
            primary_artist = result[1] if result else "Unknown Artist"
            title = _title_from_song(song)
            album = _album_name_from_song(song)
            track_num = _track_number_from_song(song)
            yt_url = song.get("download_url") or song.get("youtube_url")

            album = custom_dl.detect_singles(album, title)

            log.info("[%d/%d] %s — %s", idx, len(songs), primary_artist, title)
            path = downloader.download_track(
                artist=primary_artist, title=title, album=album, track_number=track_num, yt_url=yt_url
            )
            if path and path.exists():
                custom_dl.enforce_primary_artist(path, primary_artist, title, album, track_num)
                ok += 1
            else:
                failed += 1
        log.info("  → Downloads: ✓ %d  ✗ %d", ok, failed)

    elif "youtube.com" in url or "youtu.be" in url:
        log.info("▶ Source: YouTube / YouTube Music")
        import yt_dlp
        opts = {"extract_flat": True, "quiet": True, "no_warnings": True}
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                entries = info.get("entries", [info]) if info else []
        except Exception as e:
            log.error("Failed to extract YouTube info: %s", e)
            return

        entries = [e for e in entries if e]
        log.info("  → Found %d tracks. Downloading directly...", len(entries))
        
        ok = failed = 0
        for idx, entry in enumerate(entries, 1):
            title = entry.get("track") or entry.get("title", "Unknown Title")
            artist = entry.get("artist") or entry.get("uploader", "Unknown Artist")
            album = entry.get("album") or "Unknown Album"
            track_num = None  # flat extraction usually doesn't give track numbers

            album = custom_dl.detect_singles(album, title)

            # Strip "- Topic" from YouTube artist names
            if artist.endswith(" - Topic"):
                artist = artist.replace(" - Topic", "")

            entry_url = entry.get("url") or entry.get("webpage_url")
            if not entry_url and entry.get("id"):
                entry_url = f"https://www.youtube.com/watch?v={entry['id']}"

            log.info("[%d/%d] %s — %s", idx, len(entries), artist, title)
            path = downloader.download_track(
                artist=artist, title=title, album=album, track_number=track_num, yt_url=entry_url
            )
            if path and path.exists():
                custom_dl.enforce_primary_artist(path, artist, title, album, track_num)
                ok += 1
            else:
                failed += 1
        log.info("  → Downloads: ✓ %d  ✗ %d", ok, failed)
    else:
        log.error("Unsupported URL format. Please provide a Spotify or YouTube link.")

    log.info("━" * 60)


def cmd_migrate_structure(args):
    log.info("Migrating existing flat files to album structure...")
    
    if custom_dl:
        custom_dl.migrate_structure(Path(CFG["db_path"]), Path(CFG["music_dir"]))
    else:
        log.error("custom_dl module not available.")
        return

    log.info("Scanning all local folders and registering them into the database...")
    music_dir = Path(CFG["music_dir"])
    if music_dir.exists():
        import mutagen
        audio_exts = {".opus", ".mp3", ".m4a", ".flac", ".ogg", ".webm"}
        with db_connect() as db:
            for artist_dir in music_dir.iterdir():
                if not artist_dir.is_dir() or artist_dir.name.startswith("."): continue
                
                # Register artist
                ar_row = db.execute("SELECT spotify_id FROM artists WHERE name COLLATE NOCASE = ?", (artist_dir.name,)).fetchone()
                if ar_row:
                    artist_id = ar_row["spotify_id"]
                else:
                    artist_id = f"local:ar:{artist_dir.name}"
                    db.execute("INSERT INTO artists (spotify_id, name, source, active) VALUES (?, ?, 'local', 1)", (artist_id, artist_dir.name))
                    log.info("Auto-added local artist: %s", artist_dir.name)

                # Iterate albums
                for album_dir in artist_dir.iterdir():
                    if not album_dir.is_dir(): continue
                    
                    # Register album
                    al_row = db.execute("SELECT spotify_id FROM albums WHERE artist_id=? AND name COLLATE NOCASE = ?", (artist_id, album_dir.name)).fetchone()
                    if al_row:
                        album_id = al_row["spotify_id"]
                    else:
                        album_id = f"local:al:{artist_id}:{album_dir.name}"
                        db.execute("INSERT INTO albums (spotify_id, artist_id, name) VALUES (?, ?, ?)", (album_id, artist_id, album_dir.name))

                    # Iterate songs
                    for song_file in album_dir.iterdir():
                        if not song_file.is_file() or song_file.suffix.lower() not in audio_exts: continue
                        
                        # Use strictly relative paths using forward slashes for the DB
                        rel_path = song_file.relative_to(music_dir).as_posix()
                        
                        s_row = db.execute("SELECT spotify_id FROM songs WHERE file_path=?", (rel_path,)).fetchone()
                        if not s_row:
                            title = song_file.stem
                            try:
                                audio = mutagen.File(song_file, easy=True)
                                if audio and audio.get("title"):
                                    title = audio.get("title")[0]
                            except Exception:
                                pass
                            
                            song_id = f"local:tr:{album_id}:{song_file.name}"
                            db.execute(
                                "INSERT OR IGNORE INTO songs (spotify_id, album_id, artist_id, title, status, file_path) VALUES (?, ?, ?, ?, 'downloaded', ?)",
                                (song_id, album_id, artist_id, title, rel_path)
                            )

            # Update album counts
            db.execute("""
                UPDATE albums SET 
                track_count = (SELECT COUNT(*) FROM songs WHERE album_id=albums.spotify_id),
                downloaded_count = (SELECT COUNT(*) FROM songs WHERE album_id=albums.spotify_id AND status='downloaded')
            """)
            db.commit()

    cmd_reconcile(args)


def cmd_reconcile(args):
    artist_filter = getattr(args, "artist", None)
    with db_connect() as db:
        if artist_filter:
            artists = db.execute(
                "SELECT * FROM artists WHERE active=1 AND (name LIKE ? OR spotify_id=?)",
                (f"%{artist_filter}%", artist_filter),
            ).fetchall()
        else:
            artists = db.execute("SELECT * FROM artists WHERE active=1").fetchall()

    file_index = _build_file_index(Path(CFG["music_dir"]))
    for a in artists:
        log.info("Reconciling: %s", a["name"])
        stats = reconcile_artist_downloads(a["spotify_id"], a["name"], file_index=file_index)
        log.info("  → %d downloaded, %d pending", stats["downloaded"], stats["pending"])


def cmd_fix_metadata(args):
    artist_filter = getattr(args, "artist", None)
    with db_connect() as db:
        if artist_filter:
            artists = db.execute(
                "SELECT * FROM artists WHERE active=1 AND (name LIKE ? OR spotify_id=?)",
                (f"%{artist_filter}%", artist_filter),
            ).fetchall()
        else:
            artists = db.execute("""
                SELECT DISTINCT a.* FROM artists a
                JOIN songs s ON s.artist_id = a.spotify_id
                WHERE a.active=1 AND s.status='downloaded'
            """).fetchall()

    if not artists:
        log.info("No artists need metadata fixes")
        return

    for a in artists:
        log.info("Fixing metadata: %s", a["name"])
        
        # 1. Fallback / Local tagging: run enforce_primary_artist on all downloaded songs
        with db_connect() as db:
            songs = db.execute(
                "SELECT * FROM songs WHERE artist_id=? AND status='downloaded' AND file_path IS NOT NULL",
                (a["spotify_id"],)
            ).fetchall()
        
        music_dir = Path(CFG["music_dir"])
        for song in songs:
            fp = Path(song["file_path"])
            if not fp.is_absolute():
                fp = music_dir / fp
            if fp.exists():
                # We already know artist/title/album from DB; embed them along with iTunes cover
                album_name = "Unknown Album"
                with db_connect() as db:
                    al = db.execute("SELECT name FROM albums WHERE spotify_id=?", (song["album_id"],)).fetchone()
                    if al: album_name = al["name"]
                
                custom_dl.enforce_primary_artist(fp, a["name"], song["title"], album_name, song["track_number"], fetch_cover=True)
        
        # 2. Spotify tagging (SpotDL): only for real Spotify artists
        if not a["spotify_id"].startswith("local:"):
            target = _artist_target(a["spotify_id"], a["name"])
            if spotdl_fix_artist_metadata(a["spotify_id"], a["name"], target):
                pass # spotdl succeeded

        # 3. Reconcile to update has_cover / has_lyrics
        stats = reconcile_artist_downloads(a["spotify_id"], a["name"])
        log.info("  → Metadata: %d/%d cover, %d/%d lyrics",
                 stats["cover"], stats["downloaded"],
                 stats["lyrics"], stats["downloaded"])


def cmd_list_albums(args):
    artist_filter = getattr(args, "artist", None)
    with db_connect() as db:
        if artist_filter:
            rows = db.execute("""
                SELECT al.name AS album, al.downloaded_count, al.track_count,
                       ar.name AS artist, al.last_scanned
                FROM albums al
                JOIN artists ar ON ar.spotify_id = al.artist_id
                WHERE ar.name LIKE ? OR ar.spotify_id = ?
                ORDER BY ar.name, al.name
            """, (f"%{artist_filter}%", artist_filter)).fetchall()
        else:
            rows = db.execute("""
                SELECT al.name AS album, al.downloaded_count, al.track_count,
                       ar.name AS artist, al.last_scanned
                FROM albums al
                JOIN artists ar ON ar.spotify_id = al.artist_id
                ORDER BY ar.name, al.name
                LIMIT 500
            """).fetchall()

    print(f"\n{'Artist':<30} {'Album':<35} {'Progress':>12}")
    print("─" * 80)
    for r in rows:
        prog = f"{r['downloaded_count']}/{r['track_count']}"
        print(f"{r['artist']:<30} {r['album']:<35} {prog:>12}")
    print(f"\n{len(rows)} albums shown")


def cmd_disable(args):
    with db_connect() as db:
        cur = db.execute(
            "UPDATE artists SET active=0 WHERE name LIKE ? OR spotify_id=?",
            (f"%{args.artist}%", args.artist),
        )
    log.info("Disabled %d artist(s) matching '%s'", cur.rowcount, args.artist)


def cmd_enable(args):
    with db_connect() as db:
        cur = db.execute(
            "UPDATE artists SET active=1 WHERE name LIKE ? OR spotify_id=?",
            (f"%{args.artist}%", args.artist),
        )
    log.info("Enabled %d artist(s) matching '%s'", cur.rowcount, args.artist)


def cmd_full_sync(args):
    log.info("━" * 60)
    log.info("  FULL SYNC  —  %s", datetime.now().strftime("%Y-%m-%d %H:%M"))
    log.info("━" * 60)

    log.info("\n▶ Step 1 / 3 — Scanning playlists")
    cmd_scan(argparse.Namespace())

    log.info("\n▶ Step 2 / 3 — Scanning artist albums")
    cmd_scan_artists(argparse.Namespace(new_only=False))

    log.info("\n▶ Step 3 / 3 — Syncing artist discographies")
    cmd_artists_sync(argparse.Namespace(new_only=False))

    with db_connect() as db:
        albums = db.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
        dl = db.execute("SELECT COUNT(*) FROM songs WHERE status='downloaded'").fetchone()[0]
        pending = db.execute("SELECT COUNT(*) FROM songs WHERE status='pending'").fetchone()[0]
        no_cover = db.execute(
            "SELECT COUNT(*) FROM songs WHERE status='downloaded' AND has_cover=0"
        ).fetchone()[0]

    log.info("\n━" * 60)
    log.info("  SYNC COMPLETE — %d albums, %d songs downloaded, %d pending", albums, dl, pending)
    if no_cover:
        log.info("  %d songs missing cover — run: musicadet fix-metadata", no_cover)
    log.info("━" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    for d in (CFG["sync_dir"], CFG["music_dir"], CFG["log_dir"]):
        Path(d).mkdir(parents=True, exist_ok=True)

    db_init()

    p = argparse.ArgumentParser(
        prog="musicadet",
        description="Automated Spotify→Jellyfin music library manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  musicadet                                        Full sync (scan + catalog + download)
  musicadet scan                                   Discover artists from playlists
  musicadet scan-artists                           Scan albums/songs into DB
  musicadet artists-sync                           Download all artist discographies (sequential)
  musicadet artists-sync --new-only
  musicadet sync-playlist <spotify_playlist_url>   Fetch playlist → download tracks → complete albums
  musicadet download <url>                         Direct download from Spotify/YT (no DB check)
  musicadet reconcile                              Match files to DB
  musicadet migrate-structure                      Move flat files into album folders
  musicadet fix-metadata --artist NAME             Re-embed tags
  musicadet list-albums
  musicadet add "THE MOTANS"
        """,
    )

    sub = p.add_subparsers(dest="cmd")
    sub.add_parser("sync", help="Full pipeline — default command")
    sub.add_parser("scan", help="Scan playlists, discover artists")

    sa = sub.add_parser("scan-artists", help="Scan artist albums into DB")
    sa.add_argument("--new-only", action="store_true", help="Only artists not yet scanned")

    as_ = sub.add_parser("artists-sync", help="Sync artist discographies (sequential yt-dlp)")
    as_.add_argument("--new-only", action="store_true", help="Only sync artists not yet downloaded")

    sub.add_parser("download-pending", help="Directly download all pending tracks without SpotDL fetches")

    # New: playlist-first sync command
    sp = sub.add_parser(
        "sync-playlist",
        help="Fetch a Spotify playlist via SpotDL then download all tracks + check artist albums",
    )
    sp.add_argument("url", help="Spotify playlist URL")

    # New: direct download command
    dd = sub.add_parser(
        "download",
        help="Directly download a Spotify/YT playlist to local folders (no DB/album checks)",
    )
    dd.add_argument("url", help="Spotify or YouTube URL")

    rec = sub.add_parser("reconcile", help="Match filesystem files to DB")
    rec.add_argument("--artist", help="Limit to artist name/id")

    sub.add_parser("migrate-structure", help="Move existing flat files to album folders and restrict to primary artist")

    fix = sub.add_parser("fix-metadata", help="Re-embed metadata for incomplete songs")
    fix.add_argument("--artist", help="Limit to artist name/id")

    la = sub.add_parser("list-albums", help="Show album download progress")
    la.add_argument("artist", nargs="?", help="Filter by artist name")

    add_ = sub.add_parser("add", help="Add an artist by URL or name")
    add_.add_argument("artist")

    imp_ = sub.add_parser("import", help="Bulk import artists from a text file")
    imp_.add_argument("file")

    sub.add_parser("list", help="List all artists in the DB")

    dis_ = sub.add_parser("disable", help="Disable an artist")
    dis_.add_argument("artist")

    en_ = sub.add_parser("enable", help="Re-enable a disabled artist")
    en_.add_argument("artist")

    args = p.parse_args()

    routes = {
        None:                cmd_full_sync,
        "sync":              cmd_full_sync,
        "scan":              cmd_scan,
        "scan-artists":      cmd_scan_artists,
        "artists-sync":      cmd_artists_sync,
        "download-pending":  cmd_download_pending,
        "sync-playlist":     cmd_sync_playlist,
        "download":          cmd_download_direct,
        "reconcile":         cmd_reconcile,
        "migrate-structure": cmd_migrate_structure,
        "fix-metadata":      cmd_fix_metadata,
        "list-albums":       cmd_list_albums,
        "add":               cmd_add,
        "import":            cmd_import,
        "list":              cmd_list,
        "disable":           cmd_disable,
        "enable":            cmd_enable,
    }

    fn = routes.get(args.cmd)
    if fn:
        fn(args)
    else:
        p.print_help()


if __name__ == "__main__":
    main()
