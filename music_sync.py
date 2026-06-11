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
    # New: album-structured output  →  artist/album/title.ext
    "album_output_template": "{artist}/{album}/{title}.{output-ext}",
    "download_concurrency": 1,          # sequential — no rate-limit issues
    "playlist_save_timeout": 600,
    "playlist_save_retries": 3,
    "artist_save_timeout": 900,
    "lyrics_providers": ["genius", "musixmatch", "azlyrics"],
    "generate_lrc": False,
    "artist_scanner": "ytmusic",
    "scan_concurrency": 4,
    "sync_concurrency": 1,              # sequential artist sync
    "verified_artists": [],             # populated from config.json
    "max_downloads_per_artist": 0,      # 0 = unlimited (per-artist override in DB)
    "youtube_cookies_file": "",         # path to Netscape cookies.txt (optional)
    "youtube_cookies_from_browser": "", # e.g. chrome, firefox, edge (optional)
    "audiomuse": {
        "enabled": False,
        "postgres_container": "audiomuse-postgres",
        "postgres_user": "audiomuse",
        "postgres_db": "audiomusedb",
        "music_dir": "",                # defaults to music_dir
        "prune_unlisted_scores": False, # drop AudioMuse rows not in MusicaDet verified set
    },
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
    if "max_downloads" not in artist_cols:
        db.execute("ALTER TABLE artists ADD COLUMN max_downloads INTEGER")
    if "is_romanian" not in artist_cols:
        db.execute("ALTER TABLE artists ADD COLUMN is_romanian INTEGER DEFAULT 0")
    if "romanian_manual" not in artist_cols:
        db.execute("ALTER TABLE artists ADD COLUMN romanian_manual INTEGER DEFAULT 0")
    if "ytmusic_name" not in artist_cols:
        db.execute("ALTER TABLE artists ADD COLUMN ytmusic_name TEXT")
    if "ytmusic_browse_id" not in artist_cols:
        db.execute("ALTER TABLE artists ADD COLUMN ytmusic_browse_id TEXT")
    if "ytmusic_searched_at" not in artist_cols:
        db.execute("ALTER TABLE artists ADD COLUMN ytmusic_searched_at TEXT")
    if "ytmusic_status" not in artist_cols:
        db.execute("ALTER TABLE artists ADD COLUMN ytmusic_status TEXT DEFAULT 'unknown'")
        # Status values: 'found', 'not_found', 'manually_mapped', 'duplicate', 'unknown'
    if "ytmusic_notes" not in artist_cols:
        db.execute("ALTER TABLE artists ADD COLUMN ytmusic_notes TEXT")
    if "ytmusic_url" not in artist_cols:
        db.execute("ALTER TABLE artists ADD COLUMN ytmusic_url TEXT")

    song_cols = {r[1] for r in db.execute("PRAGMA table_info(songs)")}
    if "youtube_url" not in song_cols:
        db.execute("ALTER TABLE songs ADD COLUMN youtube_url TEXT")


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
                max_downloads INTEGER,
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


# Comprehensive list of known Romanian artists (normalized, lowercase, no punctuation)
_ROMANIAN_ARTISTS = {
    # Mainstream / Pop / Hip-Hop
    "smiley", "babasha", "themotans", "carlascreams", "irina rimes", "irinarimes",
    "nosfe", "morometzii", "bodo", "connect r", "connectr", "akcent", "morandi",
    "voltaj", "holograf", "phoenix", "iris", "cargo", "proconsul", "taxi",
    "tranda", "maximtb", "delia", "inna", "alexandra stan", "alexandrastan",
    "elena gheorghe", "elenagheorghe", "antonia", "jessie j", "dr alban",
    "lala band", "lalaband", "loredana", "loredanagroza", "nicoleta guta", "nicoletabogda",
    "florin salam", "florinsalam", "nicolae guta", "nicolaeguta", "cristi dules", "cristidules",
    "claudia ionas", "claudiaionas", "jador", "dorian popa", "dorianpopa",
    "sore", "what s up", "whatsup", "the motans", "subcarpati", "hie",
    "grasu xxl", "grasuxxl", "lino golden", "linogolden", "alex velea", "alexvelea",
    "mario fresh", "mariofresh", "matteo", "edward sanda", "edwardsanda",
    "emilian", "iamreal", "bvcovia", "la familia", "lafamilia",
    "nane", "dragos becker", "dragosbecker", "b o r k", "bork",
    "guess who", "guesswho", "cheloo", "parazitiitraditionali", "parazitii",
    "mahia beldo", "mahiabeldo", "ami", "andreea banica", "andreeabanica",
    "andreea antonescu", "andreeaantonescu", "andreea ignat", "andreeaignat",
    "cleopatra stratan", "cleopatrastratan", "abi talent", "abitalent",
    "catalin josan", "catalinjosan", "mr juve", "mrjuve", "mr ghita",
    "nicolae botgros", "nicolaebotgros", "silvia dumitrescu", "silviadumitrescu",
    "benone sinulescu", "benonesinulescu", "gheorghe dinca", "gheorghedinca",
    "stefan banica jr", "stefanbanicajr", "mihai margineanu", "mihaimargineanu",
    "mihai eminescu", "cristi minculescu", "cristiminculescu",
    "what s up", "whatsup", "sisu tudor", "sisutudor",
    # Rap / Trap / Urban
    "azteca", "amuly", "tzanca uraganu", "tzancauraganu", "tzanca uraganul",
    "florin peste", "florinpeste", "johnny romano", "johnnyromano",
    "ionut cercel", "ionutcercel", "guta", "nicolae guta",
    "tata vlad", "tatavlad", "daz dillinger", "dj project", "djproject",
    "keed", "lvbel c5", "lvbelc5", "petre stefan", "petrestefan",
    "robert toma", "roberttoma", "bogdan dragos", "bogdandragos",
    "vunk", "mirela petrean", "mirela retegan", "mirelapetrean",
    "alina eremia", "alinaeremia", "antonia", "corina", "madalina ghenea",
    # Rock / Metal / Alternative  
    "byron", "partizan", "timpuri noi", "timpurinoi", "cerbul de aur",
    "directia 5", "directia5", "robin and the backstabbers", "robinandthebackstabbers",
    "cargo", "phoenix", "compact", "metropolitan", "celelalte cuvinte",
    "implant pentru refuz", "implantpentrurefuz", "goodbye to gravity",
    "goodbye gravity", "the mono jacks", "themonojacks",
    "jurjak", "luna amara", "lunaamara", "ro ala", "roala",
    # Folk / Ethno  
    "maria tanase", "mariatanase", "nicu alifantis", "nicuaifantis",
    "grigore lese", "grigorelese", "mircea vintila", "mirceaintila",
    "pasarea colibri", "pasareacolibri", "fanfare ciocarlia", "fanfareciocarlia",
    "taraf de haidouks", "tarafddehaidouks",
    # Electronic / Dance
    "edward maya", "edwardmaya", "dj project", "dj sava", "djsava",
    "dj fly", "djfly", "dj dark", "djdark", "dj gigi", "djgigi",
    "dj paul", "djpaul", "dj rynno", "djrynno", "sylvio", "play aj",
    "playaj", "frissco", "dj dan", "djdan",
    # Manele / Etno
    "florin salam", "florinsalam", "nicolae guta", "nicolaeguta",
    "liviu pustiu", "liviupustiu", "mr juve", "mrjuve", "sorinel pustiu",
    "sorinelpustiu", "denisa", "vali vijelie", "valivijelie",
    "costi ionita", "costiionita", "adi de vito", "adidevito",
    "bianca de la tulcea", "biancadelatulcea", "sandu ciorba", "sanduciorba",
    "copilul de aur", "copiluldeaur", "geo de la timisoara",
    # Recent / New Gen
    "renvtø", "renvto", "two feet", "el nino", "elnino",
    "kapushon", "carlisstyle", "vlad babos", "vladbabos",
    "alex fitzu", "alexfitzu", "stres", "bug mafia", "bugmafia",
    "AG Remix", "agremix", "al rafaelo", "alrafaelo",
    "hm", "radu sirbu", "radusirbu", "ala bala portocala",
}


def _normalize_artist_name(name: str) -> tuple[str, str]:
    """Strip YouTube/Spotify noise before Romanian matching."""
    n = name.lower()
    for cut in (
        " - topic", " - vevo", " - official", " official", " topic",
        " (official", " [official",
    ):
        if cut in n:
            n = n.split(cut)[0]
    n = re.sub(r"\s*-\s*official.*$", "", n, flags=re.I)
    n = re.sub(r"#\s*\d+.*$", "", n)
    n = re.sub(r"\s+\d+\s*$", "", n)
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    ns = re.sub(r"[^a-z0-9]", "", n)
    return n, ns


def _matches_romanian_list(norm: str, norm_nospace: str) -> bool:
    if norm in _ROMANIAN_ARTISTS or norm_nospace in _ROMANIAN_ARTISTS:
        return True
    for ro_name in _ROMANIAN_ARTISTS:
        if len(ro_name) < 4:
            continue
        if ro_name in norm or ro_name in norm_nospace:
            return True
        if norm and (norm in ro_name or norm_nospace in ro_name):
            if len(norm) >= 4:
                return True
    return _is_romanian_fuzzy(norm) or _is_romanian_fuzzy(norm_nospace)


def _is_romanian_fuzzy(norm: str) -> bool:
    """Check if a normalized artist name fuzzy-matches any known Romanian artist."""
    if not norm or len(norm) < 3:
        return False
    from difflib import SequenceMatcher
    for ro_name in _ROMANIAN_ARTISTS:
        if abs(len(norm) - len(ro_name)) <= 4:
            ratio = SequenceMatcher(None, norm, ro_name).ratio()
            if ratio >= 0.85:
                return True
    return False


_mb_cache: dict = {}  # Cache MusicBrainz results to avoid repeated calls

def _check_musicbrainz(artist_name: str) -> bool:
    """Query MusicBrainz API to check if artist is from Romania. Cached."""
    import urllib.request, json, time
    if artist_name in _mb_cache:
        return _mb_cache[artist_name]
    try:
        query = urllib.parse.quote(artist_name)
        url = f"https://musicbrainz.org/ws/2/artist/?query=artist:{query}&fmt=json&limit=3"
        req = urllib.request.Request(url, headers={"User-Agent": "Musicadet/1.0 (musicadet@local)"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        artists = data.get("artists", [])
        for a in artists:
            if int(a.get("score", 0)) >= 70:
                country = (a.get("country") or "").upper()
                area = (a.get("area") or {}).get("name", "").lower()
                if country == "RO" or "romania" in area:
                    _mb_cache[artist_name] = True
                    time.sleep(0.35)
                    return True
                begin = (a.get("begin-area") or {}).get("name", "").lower()
                if "romania" in begin:
                    _mb_cache[artist_name] = True
                    time.sleep(0.35)
                    return True
        _mb_cache[artist_name] = False
        time.sleep(0.3)
    except Exception:
        _mb_cache[artist_name] = False
    return False


def _auto_mark_romanian_artists():
    """Automatically mark Romanian artists using 3-tier detection:
    1. Exact match against curated list
    2. Fuzzy match (typos, diacritics, suffixes like ' - Topic')
    3. MusicBrainz API lookup (for unknowns not in our list)
    """
    import urllib.parse
    try:
        with db_connect() as db:
            # Only process artists not yet manually set (is_romanian IS NULL means never checked)
            # We use is_romanian=0 as "not marked" — but we don't re-check already marked ones
            artists = db.execute(
                """SELECT spotify_id, name FROM artists
                   WHERE active >= 0 AND is_romanian = 0 AND COALESCE(romanian_manual, 0) = 0"""
            ).fetchall()

            marked = 0
            mb_checked = 0
            mb_cap = int(CFG.get("romanian_mb_cap", 0))  # 0 = no cap
            for art in artists:
                name = art["name"]
                norm, norm_nospace = _normalize_artist_name(name)

                is_ro = _matches_romanian_list(norm, norm_nospace)
                if is_ro:
                    log.debug("RO (list): %s", name)

                if not is_ro and (mb_cap == 0 or mb_checked < mb_cap):
                    is_ro = _check_musicbrainz(name)
                    mb_checked += 1
                    if is_ro:
                        log.debug("RO (MusicBrainz): %s", name)

                if is_ro:
                    db.execute(
                        "UPDATE artists SET is_romanian=1 WHERE spotify_id=?",
                        (art["spotify_id"],),
                    )
                    marked += 1

            log.info(
                "Romanian detection: marked %d / %d artists (%d MusicBrainz lookups)",
                marked, len(artists), mb_checked,
            )
    except Exception as e:
        log.warning("Auto-mark Romanian artists failed: %s", e)


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


def cmd_clean_ytm(args):
    """Removes all pending tracks and albums that were added by the YouTube Music scanner."""
    with db_connect() as db:
        # Delete albums that came entirely from YTM (their IDs start with MPREb_)
        cur1 = db.execute("DELETE FROM albums WHERE spotify_id LIKE 'MPREb_%'")
        # Delete songs added by YTM that attached to existing Spotify albums. 
        # YTM songs have exactly 11 chars (videoId) or start with 'trk:'
        cur2 = db.execute(
            "DELETE FROM songs WHERE status != 'downloaded' AND "
            "(length(spotify_id) = 11 OR spotify_id LIKE 'trk:%')"
        )
        
        # Reset the albums_scanned_at so Spotify scanner can rescan them properly
        db.execute("UPDATE artists SET albums_scanned_at = NULL WHERE active = 1")
        
        db.commit()
        
    log.info("Cleanup complete:")
    log.info("  - Removed %d YouTube Music albums (and their tracks)", cur1.rowcount)
    log.info("  - Removed %d pending YouTube Music tracks from Spotify albums", cur2.rowcount)
    log.info("  - Reset artist scan status. You can now re-run 'Scan artist albums' with the Spotify scanner.")


def _normalize_artist_name(name: str) -> str:
    s = name.lower()
    if s.endswith(" - topic"):
        s = s[:-8]
    elif s.endswith(" topic"):
        s = s[:-6]
    s = re.sub(r'[^a-z0-9]', '', s)
    return s.strip()


def _normalize_song_title(title: str) -> str:
    s = title.lower()
    s = re.sub(r'\([^)]*\)', '', s)
    s = re.sub(r'\[[^\]]*\]', '', s)
    s = re.split(r'\b(feat|featuring|ft|with)\b', s)[0]
    s = re.sub(r'[^a-z0-9]', '', s)
    return s.strip()


def _clean_artist_name_for_ytm(artist_name: str) -> str:
    """Strip auto-channel suffixes so YT Music search finds the real artist."""
    n = (artist_name or "").strip()
    for suffix in (" - Topic", " Topic", " - topic"):
        if n.endswith(suffix):
            n = n[: -len(suffix)].strip()
    return n or artist_name


def _song_youtube_video_id(row) -> Optional[str]:
    if hasattr(row, "keys"):
        keys = row.keys()
        sid = row["spotify_id"] if "spotify_id" in keys else ""
    else:
        sid = row.get("spotify_id", "")
    if sid and len(sid) == 11 and ":" not in sid:
        return sid
    if hasattr(row, "keys"):
        keys = row.keys()
        url = row["youtube_url"] if "youtube_url" in keys else None
    else:
        url = row.get("youtube_url")
    if url:
        m = re.search(r'(?:v=|youtu\.be/)([a-zA-Z0-9_-]{11})', str(url))
        if m:
            return m.group(1)
    return None



def _song_matches_top_entry(row, entry: dict) -> bool:
    vid = entry.get("videoId")
    if vid and _song_youtube_video_id(row) == vid:
        return True
    norm_row = _normalize_song_title(row["title"])
    norm_top = entry.get("norm") or _normalize_song_title(entry.get("title", ""))
    return bool(norm_row and norm_top and norm_row == norm_top)


def _merge_folders(src: Path, dst: Path):
    import shutil
    if not src.exists() or src == dst:
        return
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            if target.exists() and target.is_dir():
                _merge_folders(item, target)
            else:
                shutil.move(str(item), str(target))
        else:
            if target.exists():
                if item.stat().st_size > target.stat().st_size:
                    try:
                        target.unlink()
                        shutil.move(str(item), str(target))
                    except Exception:
                        pass
                else:
                    try:
                        item.unlink()
                    except Exception:
                        pass
            else:
                shutil.move(str(item), str(target))
    try:
        src.rmdir()
    except OSError:
        pass


def cmd_deduplicate(args):
    """
    Merge duplicate artists, deduplicate tracks under canonical artists,
    and clean up 1-track albums that are duplicates of tracks in full albums.
    """
    from collections import defaultdict
    import shutil
    
    log.info("Starting Deduplication Engine...")
    
    with db_connect() as db:
        # Avoid thread lock issues by using Row factory locally
        db.row_factory = sqlite3.Row
        
        # 1. ARTIST DEDUPLICATION & MERGING
        artists = db.execute("SELECT spotify_id, name, active, last_synced, added_at FROM artists").fetchall()
        
        # Group by normalized name
        groups = defaultdict(list)
        for art in artists:
            norm = _normalize_artist_name(art["name"])
            if norm:
                groups[norm].append(art)
                
        for norm_name, group_artists in groups.items():
            if len(group_artists) > 1:
                # Determine canonical
                def artist_sort_key(art):
                    aid = art["spotify_id"]
                    if aid.startswith("local:"):
                        rank = 1
                    elif aid.startswith("q:"):
                        rank = 2
                    else:
                        rank = 3
                    return (rank, art["active"] or 0, aid)
                    
                sorted_group = sorted(group_artists, key=artist_sort_key, reverse=True)
                canonical_artist = sorted_group[0]
                canonical_id = canonical_artist["spotify_id"]
                
                # Choose the cleanest canonical name (preferring one that doesn't end with " - Topic")
                names = [a["name"] for a in sorted_group]
                clean_names = [n for n in names if not n.lower().endswith(" - topic") and not n.lower().endswith(" topic")]
                if clean_names:
                    canonical_name = clean_names[0]
                else:
                    canonical_name = canonical_artist["name"]
                    if canonical_name.lower().endswith(" - topic"):
                        canonical_name = canonical_name[:-8]
                    elif canonical_name.lower().endswith(" topic"):
                        canonical_name = canonical_name[:-6]
                
                log.info("Artist Group '%s': canonical is '%s' (%s)", norm_name, canonical_name, canonical_id)
                
                duplicate_artists = sorted_group[1:]
                for dup in duplicate_artists:
                    dup_id = dup["spotify_id"]
                    dup_name = dup["name"]
                    log.info("  Merging duplicate artist '%s' (%s) -> '%s' (%s)", dup_name, dup_id, canonical_name, canonical_id)
                    
                    # Merge playlist_artists
                    playlist_ids = [r[0] for r in db.execute("SELECT playlist_id FROM playlist_artists WHERE artist_id = ?", (dup_id,)).fetchall()]
                    for pl_id in playlist_ids:
                        exists = db.execute("SELECT 1 FROM playlist_artists WHERE playlist_id = ? AND artist_id = ?", (pl_id, canonical_id)).fetchone()
                        if not exists:
                            db.execute("INSERT INTO playlist_artists (playlist_id, artist_id) VALUES (?, ?)", (pl_id, canonical_id))
                    db.execute("DELETE FROM playlist_artists WHERE artist_id = ?", (dup_id,))
                    
                    # Merge albums
                    dup_albums = db.execute("SELECT spotify_id, name, release_year, track_count, downloaded_count, last_scanned FROM albums WHERE artist_id = ?", (dup_id,)).fetchall()
                    for da in dup_albums:
                        da_id = da["spotify_id"]
                        da_name = da["name"]
                        
                        canonical_match = db.execute(
                            "SELECT spotify_id, track_count, downloaded_count FROM albums WHERE artist_id = ? AND LOWER(TRIM(name)) = LOWER(TRIM(?))",
                            (canonical_id, da_name)
                        ).fetchone()
                        
                        if canonical_match:
                            cm_id = canonical_match["spotify_id"]
                            log.info("    Merging duplicate album '%s' (%s) -> canonical album (%s)", da_name, da_id, cm_id)
                            da_songs = db.execute("SELECT spotify_id, title, track_number, status, file_path, youtube_url FROM songs WHERE album_id = ?", (da_id,)).fetchall()
                            for ds in da_songs:
                                ds_id = ds["spotify_id"]
                                song_by_id = db.execute("SELECT spotify_id, status, file_path FROM songs WHERE spotify_id = ?", (ds_id,)).fetchone()
                                if song_by_id:
                                    if ds["status"] == "downloaded" and song_by_id["status"] != "downloaded":
                                        db.execute("UPDATE songs SET status='downloaded', file_path=?, youtube_url=? WHERE spotify_id=?",
                                                   (ds["file_path"], ds["youtube_url"], ds_id))
                                    elif ds["status"] == "downloaded" and song_by_id["status"] == "downloaded":
                                        if ds["file_path"] and song_by_id["file_path"] and ds["file_path"] != song_by_id["file_path"]:
                                            try:
                                                Path(ds["file_path"]).unlink(missing_ok=True)
                                            except Exception:
                                                pass
                                    if ds_id != song_by_id["spotify_id"]:
                                        db.execute("DELETE FROM songs WHERE spotify_id = ?", (ds_id,))
                                else:
                                    db.execute("UPDATE songs SET artist_id = ?, album_id = ? WHERE spotify_id = ?",
                                               (canonical_id, cm_id, ds_id))
                            
                            # Count tracks dynamically
                            tot_count = db.execute("SELECT COUNT(*) FROM songs WHERE album_id = ?", (cm_id,)).fetchone()[0]
                            dl_count = db.execute("SELECT COUNT(*) FROM songs WHERE album_id = ? AND status='downloaded'", (cm_id,)).fetchone()[0]
                            db.execute("UPDATE albums SET track_count = ?, downloaded_count = ? WHERE spotify_id = ?",
                                       (max(tot_count, canonical_match["track_count"] or 0), dl_count, cm_id))
                            
                            db.execute("DELETE FROM albums WHERE spotify_id = ?", (da_id,))
                        else:
                            log.info("    Moving album '%s' (%s) to canonical artist", da_name, da_id)
                            db.execute("UPDATE albums SET artist_id = ? WHERE spotify_id = ?", (canonical_id, da_id))
                            db.execute("UPDATE songs SET artist_id = ? WHERE album_id = ?", (canonical_id, da_id))
                    
                    # Move physical files
                    dup_folder = Path(CFG["music_dir"]) / custom_dl._clean_filename(dup_name)
                    canonical_folder = Path(CFG["music_dir"]) / custom_dl._clean_filename(canonical_name)
                    if dup_folder.exists() and dup_folder.is_dir() and dup_folder.resolve() != canonical_folder.resolve():
                        log.info("    Moving files from '%s' to '%s'", dup_folder.name, canonical_folder.name)
                        _merge_folders(dup_folder, canonical_folder)
                        
                    # Soft-delete duplicate artist so it is permanently blacklisted
                    db.execute("UPDATE artists SET active=-1 WHERE spotify_id = ?", (dup_id,))
                    
                # Update canonical name
                db.execute("UPDATE artists SET name = ?, active = 1 WHERE spotify_id = ?", (canonical_name, canonical_id))
        
        # 2. TRACK DEDUPLICATION
        active_artists = db.execute("SELECT spotify_id, name FROM artists WHERE active = 1").fetchall()
        for art in active_artists:
            art_id = art["spotify_id"]
            art_name = art["name"]
            
            songs = db.execute("""
                SELECT s.spotify_id, s.album_id, s.title, s.track_number, s.status, s.file_path, s.youtube_url,
                       al.name as album_name
                FROM songs s
                JOIN albums al ON s.album_id = al.spotify_id
                WHERE s.artist_id = ?
            """, (art_id,)).fetchall()
            
            by_title = defaultdict(list)
            for s in songs:
                by_title[_normalize_song_title(s["title"])].append(s)
                
            for title_norm, group_songs in by_title.items():
                if len(group_songs) > 1:
                    def song_sort_key(s):
                        is_dl = 1 if s["status"] == "downloaded" else 0
                        is_real = 1 if (s["album_name"] and s["album_name"].lower() not in ("singles", "unknown album", "")) else 0
                        has_file = 1 if (s["file_path"] and os.path.exists(s["file_path"])) else 0
                        return (is_dl, is_real, has_file, s["spotify_id"])
                        
                    sorted_songs = sorted(group_songs, key=song_sort_key, reverse=True)
                    canonical_song = sorted_songs[0]
                    discarded_songs = sorted_songs[1:]
                    
                    log.info("Deduplicating tracks for %s: keeping '%s' (%s, status: %s)",
                             art_name, canonical_song["title"], canonical_song["spotify_id"], canonical_song["status"])
                             
                    for ds in discarded_songs:
                        if ds["file_path"] and os.path.exists(ds["file_path"]):
                            if canonical_song["status"] != "downloaded":
                                src_p = Path(ds["file_path"])
                                res_alb = custom_dl.detect_singles(canonical_song["album_name"], canonical_song["title"]) if custom_dl else canonical_song["album_name"]
                                dest_dir = Path(CFG["music_dir"]) / _clean_filename(art_name) / _clean_filename(res_alb)
                                dest_dir.mkdir(parents=True, exist_ok=True)
                                dest_p = dest_dir / src_p.name
                                try:
                                    shutil.move(str(src_p), str(dest_p))
                                    db.execute("UPDATE songs SET file_path = ?, status = 'downloaded' WHERE spotify_id = ?",
                                               (str(dest_p), canonical_song["spotify_id"]))
                                    canonical_song["status"] = "downloaded"
                                    canonical_song["file_path"] = str(dest_p)
                                except Exception as e:
                                    log.warning("    Failed to move file %s -> canonical: %s", src_p, e)
                            else:
                                src_p = Path(ds["file_path"])
                                can_p = Path(canonical_song["file_path"]) if canonical_song["file_path"] else None
                                if can_p and src_p.exists() and can_p.exists() and src_p.resolve() != can_p.resolve():
                                    try:
                                        src_p.unlink()
                                    except Exception as e:
                                        log.warning("    Failed to delete duplicate file %s: %s", src_p, e)
                                        
                        db.execute("DELETE FROM songs WHERE spotify_id = ?", (ds["spotify_id"],))
                        
        # 3. 1-TRACK ALBUM CLEANUP
        for art in active_artists:
            art_id = art["spotify_id"]
            art_name = art["name"]
            albums = db.execute("SELECT spotify_id, name FROM albums WHERE artist_id = ?", (art_id,)).fetchall()
            for alb in albums:
                alb_id = alb["spotify_id"]
                alb_name = alb["name"]
                
                songs_in_alb = db.execute("SELECT spotify_id, title, status, file_path FROM songs WHERE album_id = ?", (alb_id,)).fetchall()
                if len(songs_in_alb) == 1:
                    song = songs_in_alb[0]
                    norm_title = _normalize_song_title(song["title"])
                    
                    other_songs = db.execute("""
                        SELECT s.spotify_id, s.album_id, s.title, s.status, s.file_path, al.name as album_name
                        FROM songs s
                        JOIN albums al ON s.album_id = al.spotify_id
                        WHERE s.artist_id = ? AND s.album_id != ?
                    """, (art_id, alb_id)).fetchall()
                    
                    other_match = None
                    for os_row in other_songs:
                        if _normalize_song_title(os_row["title"]) == norm_title:
                            other_match = os_row
                            break
                            
                    if other_match:
                        log.info("1-Track Album Cleanup for %s: removing '%s' (Album: %s) because it exists in '%s'",
                                 art_name, song["title"], alb_name, other_match["album_name"])
                                 
                        if song["file_path"] and os.path.exists(song["file_path"]):
                            if other_match["status"] != "downloaded":
                                src_p = Path(song["file_path"])
                                res_alb = custom_dl.detect_singles(other_match["album_name"], other_match["title"]) if custom_dl else other_match["album_name"]
                                dest_dir = Path(CFG["music_dir"]) / _clean_filename(art_name) / _clean_filename(res_alb)
                                dest_dir.mkdir(parents=True, exist_ok=True)
                                dest_p = dest_dir / src_p.name
                                try:
                                    shutil.move(str(src_p), str(dest_p))
                                    db.execute("UPDATE songs SET file_path = ?, status = 'downloaded' WHERE spotify_id = ?",
                                               (str(dest_p), other_match["spotify_id"]))
                                except Exception as e:
                                    log.warning("    Failed to move 1-track album file to canonical album: %s", e)
                            else:
                                src_p = Path(song["file_path"])
                                can_p = Path(other_match["file_path"]) if other_match["file_path"] else None
                                if can_p and src_p.exists() and can_p.exists() and src_p.resolve() != can_p.resolve():
                                    try:
                                        src_p.unlink()
                                    except Exception as e:
                                        log.warning("    Failed to delete duplicate 1-track file: %s", e)
                                        
                        db.execute("DELETE FROM songs WHERE spotify_id = ?", (song["spotify_id"],))
                        db.execute("DELETE FROM albums WHERE spotify_id = ?", (alb_id,))
                        
        # 4. RECALCULATE ALBUM STATS
        log.info("Recalculating album track and download stats...")
        all_albums = db.execute("SELECT spotify_id FROM albums").fetchall()
        for alb in all_albums:
            alb_id = alb["spotify_id"]
            tot_count = db.execute("SELECT COUNT(*) FROM songs WHERE album_id = ?", (alb_id,)).fetchone()[0]
            dl_count = db.execute("SELECT COUNT(*) FROM songs WHERE album_id = ? AND status='downloaded'", (alb_id,)).fetchone()[0]
            db.execute("UPDATE albums SET track_count = ?, downloaded_count = ? WHERE spotify_id = ?", (tot_count, dl_count, alb_id))
            
        db.commit()
        
    log.info("Deduplication complete.")


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
            yt_url = song.get("download_url") or song.get("youtube_url") or song.get("url")
            if yt_url and "youtube" not in str(yt_url):
                yt_url = None

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
                INSERT INTO songs (spotify_id, album_id, artist_id, title, track_number, status, youtube_url, updated_at)
                VALUES (?,?,?,?,?,'pending',?,?)
                ON CONFLICT(spotify_id) DO UPDATE SET
                    album_id=excluded.album_id,
                    title=excluded.title,
                    track_number=excluded.track_number,
                    youtube_url=COALESCE(excluded.youtube_url, songs.youtube_url),
                    updated_at=excluded.updated_at
            """, (song_id, final_album_id, artist_id, title, track_num, yt_url, now))
            if cur.rowcount == 1:
                new_songs += 1
            elif existing and existing["status"] != "downloaded":
                db.execute(
                    "UPDATE songs SET status='pending' WHERE spotify_id=? AND status='failed'",
                    (song_id,),
                )

    return new_albums, new_songs


def remove_track_number_prefixes(music_dir: Path):
    """Scan the music directory and rename any files like '04 - Title.ext' to 'Title.ext'."""
    if not music_dir.exists():
        return
    log.info("Scanning for files with track number prefixes to strip...")
    renamed_count = 0
    for root, _dirs, files in os.walk(music_dir):
        for fname in files:
            if Path(fname).suffix.lower() not in AUDIO_EXTS:
                continue
            m = re.match(r"^\d+\s*-\s*(.+)$", fname)
            if m:
                new_fname = m.group(1).strip()
                old_path = Path(root) / fname
                new_path = Path(root) / new_fname
                if old_path != new_path:
                    try:
                        if new_path.exists():
                            if old_path.stat().st_size >= new_path.stat().st_size:
                                new_path.unlink()
                                old_path.rename(new_path)
                            else:
                                old_path.unlink()
                        else:
                            old_path.rename(new_path)
                        renamed_count += 1
                        log.debug("Renamed: %s -> %s", fname, new_fname)
                    except Exception as e:
                        log.warning("Failed to rename prefix for %s: %s", fname, e)
    if renamed_count > 0:
        log.info("Renamed %d files to remove track number prefixes.", renamed_count)


def _build_file_index(music_dir: Path) -> tuple[dict, dict]:
    """Index by (artist, album, title) and (album, title) keys."""
    by_full: dict[tuple[str, str, str], Path] = {}
    by_album_title: dict[tuple[str, str], list[Path]] = {}
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
                key = (album, title)
                if key not in by_album_title:
                    by_album_title[key] = []
                by_album_title[key].append(full)
    return by_full, by_album_title


def _is_matching_artist(disk_artist: str, artist_name: str, artist_id: str) -> bool:
    """Check if the artist name on disk is case-insensitively/fuzzily matching the artist we want."""
    def normalize(s: str) -> str:
        import unicodedata
        s_norm = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('utf-8').lower()
        return re.sub(r'[^a-z0-9]', '', s_norm)

    disk_norm = normalize(disk_artist)
    name_norm = normalize(artist_name)
    if disk_norm == name_norm:
        return True
    
    # Check for substring match (e.g. "Trinix & Friends" vs "Trinix")
    if len(disk_norm) >= 3 and len(name_norm) >= 3:
        if disk_norm in name_norm or name_norm in disk_norm:
            return True

    # Check against clean artist_id
    clean_id = artist_id
    for prefix in ["spotify:artist:", "local:", "q:"]:
        if clean_id.lower().startswith(prefix):
            clean_id = clean_id[len(prefix):]
    id_norm = normalize(clean_id)
    if disk_norm == id_norm:
        return True
    if len(disk_norm) >= 3 and len(id_norm) >= 3:
        if disk_norm in id_norm or id_norm in disk_norm:
            return True

    return False


def reconcile_artist_downloads(artist_id: str, artist_name: str, *, file_index=None) -> dict:
    """Match filesystem files to DB songs. Returns stats dict."""
    music_dir = Path(CFG["music_dir"])
    if file_index is not None:
        by_full, by_album_title = file_index
    else:
        by_full, by_album_title = _build_file_index(music_dir)
    stats = {"downloaded": 0, "cover": 0, "lyrics": 0, "pending": 0, "moved_paths": []}

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
                resolved_album_name = get_resolved_album_name(db, song["album_id"], album_name, song["title"])
                resolved_album_key = custom_dl._clean_filename(resolved_album_name).lower()

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
                candidates = by_album_title.get((album_key, title_key)) or []
                for cand in candidates:
                    disk_artist = cand.relative_to(music_dir).parts[0]
                    if _is_matching_artist(disk_artist, display_name, artist_id):
                        found = cand
                        break
            if not found and resolved_album_key:
                candidates = by_album_title.get((resolved_album_key, title_key)) or []
                for cand in candidates:
                    disk_artist = cand.relative_to(music_dir).parts[0]
                    if _is_matching_artist(disk_artist, display_name, artist_id):
                        found = cand
                        break

            if found and not found.exists():
                found = None

            now = datetime.now().isoformat(timespec="seconds")
            old_rel = str(song["file_path"]).replace("\\", "/") if song["file_path"] else ""
            if found:
                rel = str(found.relative_to(music_dir)).replace("\\", "/")
                expected_artist = display_name if custom_dl is None else custom_dl._clean_filename(display_name)
                
                # Auto-rename / move if it's in the wrong folder structure
                if custom_dl:
                    resolved_album = get_resolved_album_name(db, song["album_id"], album_name, song["title"])
                    safe_album = custom_dl._clean_filename(resolved_album)
                    expected_folder = music_dir / expected_artist / safe_album
                    old_parent = found.parent
                    
                    if str(old_parent.resolve()).lower() != str(expected_folder.resolve()).lower():
                        expected_folder.mkdir(parents=True, exist_ok=True)
                        new_path = expected_folder / found.name
                        if str(found.resolve()).lower() != str(new_path.resolve()).lower():
                            import shutil
                            try:
                                if new_path.exists() and str(new_path.resolve()).lower() != str(found.resolve()).lower():
                                    if found.stat().st_size <= new_path.stat().st_size:
                                        found.unlink()
                                        log.info("  [Auto-Clean] Duplicate found at destination: unlinked smaller %s", found.name)
                                        found = new_path
                                    else:
                                        new_path.unlink()
                                        shutil.move(str(found), str(new_path))
                                        log.info("  [Auto-Move] Overwrote %s with %s", new_path.name, found.name)
                                        found = new_path
                                else:
                                    shutil.move(str(found), str(new_path))
                                    log.info("  [Auto-Move] Moved %s from %s to %s", found.name, old_parent.name, expected_folder.name)
                                    found = new_path
                                
                                # Clean up old parent
                                try:
                                    if old_parent.exists() and not any(old_parent.iterdir()):
                                        old_parent.rmdir()
                                        log.info("  [Auto-Clean] Removed empty directory: %s", old_parent.name)
                                        grandparent = old_parent.parent
                                        if grandparent.exists() and grandparent.resolve() != music_dir.resolve() and not any(grandparent.iterdir()):
                                            grandparent.rmdir()
                                            log.info("  [Auto-Clean] Removed empty artist directory: %s", grandparent.name)
                                except Exception as e:
                                    log.debug("Failed to remove empty folder %s: %s", old_parent, e)

                                rel = str(found.relative_to(music_dir)).replace("\\", "/")
                            except Exception as e:
                                log.warning("Could not auto-move %s: %s", found, e)

                if old_rel and old_rel != rel:
                    stats["moved_paths"].append((old_rel, rel))
                elif not old_rel and rel:
                    pass

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


MIN_ALBUM_TRACKS = 5


def consolidate_small_albums(db: sqlite3.Connection, artist_id: Optional[str] = None) -> int:
    """
    Move tracks from albums with fewer than MIN_ALBUM_TRACKS into a virtual 'Singles' album.
    Returns number of albums merged.
    """
    clause = (
        "WHERE LOWER(TRIM(al.name)) != 'singles' "
        "AND (SELECT COUNT(*) FROM songs s WHERE s.album_id = al.spotify_id) > 0 "
        "AND (SELECT COUNT(*) FROM songs s WHERE s.album_id = al.spotify_id) < ?"
    )
    params: list = [MIN_ALBUM_TRACKS]
    if artist_id:
        clause += " AND al.artist_id = ?"
        params.append(artist_id)

    small = db.execute(
        f"""
        SELECT al.spotify_id, al.artist_id, al.name, al.track_count
        FROM albums al
        {clause}
        ORDER BY al.artist_id, al.name COLLATE NOCASE
        """,
        params,
    ).fetchall()
    if not small:
        return 0

    merged = 0
    for alb in small:
        aid = alb["artist_id"]
        singles = db.execute(
            "SELECT spotify_id FROM albums WHERE artist_id=? AND LOWER(TRIM(name))='singles'",
            (aid,),
        ).fetchone()
        if singles:
            singles_id = singles["spotify_id"]
        else:
            singles_id = f"singles:{aid}"
            db.execute(
                "INSERT OR IGNORE INTO albums (spotify_id, artist_id, name, track_count, downloaded_count) VALUES (?,?,?,0,0)",
                (singles_id, aid, "Singles"),
            )

        next_num = db.execute(
            "SELECT COALESCE(MAX(track_number), 0) FROM songs WHERE album_id=?",
            (singles_id,),
        ).fetchone()[0]

        songs = db.execute(
            "SELECT spotify_id FROM songs WHERE album_id=? ORDER BY track_number, title",
            (alb["spotify_id"],),
        ).fetchall()
        for i, song in enumerate(songs, start=next_num + 1):
            db.execute(
                "UPDATE songs SET album_id=?, track_number=? WHERE spotify_id=?",
                (singles_id, i, song["spotify_id"]),
            )

        db.execute("DELETE FROM albums WHERE spotify_id=?", (alb["spotify_id"],))
        row = db.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN status='downloaded' THEN 1 ELSE 0 END) AS done
            FROM songs WHERE album_id=?
            """,
            (singles_id,),
        ).fetchone()
        db.execute(
            "UPDATE albums SET track_count=?, downloaded_count=? WHERE spotify_id=?",
            (row["total"], row["done"] or 0, singles_id),
        )
        merged += 1

    return merged


def scan_artist_catalog(artist_row: sqlite3.Row) -> tuple[int, int]:
    """Scan one artist's discography into albums/songs tables."""
    sid = artist_row["spotify_id"]
    name = artist_row["name"]
    scanner_type = CFG.get("artist_scanner", "ytmusic")
    
    if sid.startswith("local:") and scanner_type != "ytmusic":
        log.info("  → Skipping local artist catalog scan: %s", name)
        return 0, 0
    
    log.info("  → Scanning %s using %s...", name, scanner_type.upper())
    
    matched_name = None
    browse_id = None
    
    if scanner_type == "ytmusic":
        import ytm_scanner
        
        # Check if we have cached YouTube Music artist data
        cached_ytmusic_name = artist_row["ytmusic_name"]
        cached_ytmusic_browse_id = artist_row["ytmusic_browse_id"]
        
        if cached_ytmusic_browse_id:
            log.info("  → Using cached YT Music ID for %s", name)
        
        # Get songs and save the matched name + browse_id
        songs, matched_name, browse_id = ytm_scanner.scan_artist_with_metadata(
            name,
            cached_browse_id=cached_ytmusic_browse_id
        )
        
        # If we got new metadata (from search), cache it
        if matched_name and browse_id and (not cached_ytmusic_name or not cached_ytmusic_browse_id):
            log.info("  → Caching YT Music mapping: '%s' → '%s' (ID: %s)", name, matched_name, browse_id)
            with db_connect() as db:
                db.execute(
                    "UPDATE artists SET ytmusic_name=?, ytmusic_browse_id=?, ytmusic_searched_at=datetime('now'), ytmusic_status='found' WHERE spotify_id=?",
                    (matched_name, browse_id, sid),
                )
            # If the matched name differs from search name, log it prominently
            if matched_name.lower().replace(" ", "").replace("-", "") != name.lower().replace(" ", "").replace("-", ""):
                log.warning("  ⚠️  Auto-detected name: '%s' → '%s' on YT Music", name, matched_name)
    else:
        target = _artist_target(sid, name)
        timeout = int(CFG.get("artist_save_timeout", 900))
        songs = spotdl_save(target, timeout=timeout)
    
    if not songs:
        log.warning("  → No songs returned for %s", name)
        # Mark as not found on YT Music if using ytmusic scanner
        if scanner_type == "ytmusic":
            with db_connect() as db:
                db.execute(
                    "UPDATE artists SET ytmusic_status='not_found', ytmusic_notes='Failed to find artist on YouTube Music' WHERE spotify_id=?",
                    (sid,),
                )
        return 0, 0

    with db_connect() as db:
        new_albums, new_songs = _upsert_artist_catalog(db, sid, songs)
        merged = consolidate_small_albums(db, sid)
        if merged:
            log.info("  → Merged %d small album(s) into Singles", merged)
        db.execute(
            "UPDATE artists SET albums_scanned_at=datetime('now'), "
            "ytmusic_status=CASE WHEN ytmusic_status!='manually_mapped' THEN 'found' ELSE ytmusic_status END, "
            "ytmusic_notes=CASE WHEN ytmusic_status!='manually_mapped' THEN NULL ELSE ytmusic_notes END "
            "WHERE spotify_id=?",
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
        blocked = db.execute(
            "SELECT active FROM artists WHERE spotify_id=?", (sid,)
        ).fetchone()
        if blocked and blocked["active"] == -1:
            log.error("Artist %s was permanently removed — cannot re-add", name)
            return
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


def cmd_mark_romanian(args):
    log.info("Marking Romanian artists (list + MusicBrainz)...")
    _auto_mark_romanian_artists()


def cmd_list(args):
    with db_connect() as db:
        rows = db.execute("""
            SELECT name, spotify_id, source, active, sync_done, last_synced, albums_scanned_at, ytmusic_status
            FROM artists ORDER BY name COLLATE NOCASE
        """).fetchall()
        pl_count = db.execute("SELECT COUNT(*) FROM playlists WHERE active=1").fetchone()[0]
        album_count = db.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
        song_dl = db.execute("SELECT COUNT(*) FROM songs WHERE status='downloaded'").fetchone()[0]
        song_total = db.execute("SELECT COUNT(*) FROM songs").fetchone()[0]

    total = len(rows)
    synced = sum(1 for r in rows if r["sync_done"])
    disabled = sum(1 for r in rows if not r["active"])
    manually_mapped = sum(1 for r in rows if r["ytmusic_status"] == "manually_mapped")

    print(f"\n{'Artist':<42} {'Source':<15} {'YTM Status':<18} {'Sync':>4}  Last synced")
    print("─" * 105)
    for r in rows:
        done = "✓" if r["sync_done"] else "·"
        last = (r["last_synced"] or "never")[:10]
        flag = " [off]" if not r["active"] else ""
        status = r["ytmusic_status"] or "—"
        status_icon = "🔧" if status == "manually_mapped" else ("⚠️" if status == "not_found" else " ")
        print(f"{status_icon} {r['name']:<40} {r['source']:<15} {status:<18} {done:>4}  {last}{flag}")

    print(f"\n{total} artists ({synced} synced, {total - synced - disabled} pending, {disabled} disabled, {manually_mapped} manually mapped)")
    print(f"{pl_count} playlists | {album_count} albums | {song_dl}/{song_total} songs downloaded")


def cmd_artists_issues(args):
    """List and manage artists with YouTube Music lookup issues."""
    # Handle --fix flag
    if getattr(args, "fix", None):
        parts = args.fix.split(":", 1)
        if len(parts) != 2:
            log.error("--fix format: artist_id:new_ytmusic_name")
            return
        artist_id, new_name = parts
        new_name = new_name.strip()
        with db_connect() as db:
            artist = db.execute("SELECT name FROM artists WHERE spotify_id=?", (artist_id,)).fetchone()
            if not artist:
                log.error("Artist not found: %s", artist_id)
                return
            db.execute(
                "UPDATE artists SET ytmusic_name=?, ytmusic_status='manually_mapped', ytmusic_notes='Manually mapped by user', sync_done=1, last_synced=datetime('now') WHERE spotify_id=?",
                (new_name, artist_id),
            )
        log.info("✓ Marked '%s' as manually mapped to '%s' (skips downloads, marked as synced)", artist["name"], new_name)
        return
    
    # Handle --remove flag
    if getattr(args, "remove", None):
        artist_id = args.remove
        with db_connect() as db:
            artist = db.execute("SELECT name FROM artists WHERE spotify_id=?", (artist_id,)).fetchone()
            if not artist:
                log.error("Artist not found: %s", artist_id)
                return
            db.execute("UPDATE artists SET active=-1 WHERE spotify_id=?", (artist_id,))
        log.info("✓ Removed artist: %s", artist["name"])
        return
    
    # List artists with issues
    with db_connect() as db:
        issues = db.execute("""
            SELECT spotify_id, name, ytmusic_status, ytmusic_name, ytmusic_notes
            FROM artists
            WHERE active=1 AND (ytmusic_status IN ('not_found', 'unknown') OR ytmusic_status IS NULL)
            ORDER BY name COLLATE NOCASE
        """).fetchall()
    
    if not issues:
        log.info("✓ No YouTube Music lookup issues found!")
        return
    
    print(f"\n{'⚠️  YouTube Music Lookup Issues'}:")
    print(f"{'─' * 120}")
    print(f"{'Artist':<35} {'Spotify ID':<25} {'Status':<15} {'YT Music Name':<30}")
    print("─" * 120)
    
    for issue in issues:
        status = issue["ytmusic_status"] or "unknown"
        ytm_name = issue["ytmusic_name"] or "—"
        icon = "✗" if status == "not_found" else "?"
        print(f"{icon} {issue['name']:<33} {issue['spotify_id']:<25} {status:<15} {ytm_name:<30}")
        if issue["ytmusic_notes"]:
            print(f"   → {issue['ytmusic_notes']}")
    
    print(f"\n{len(issues)} artist(s) with lookup issues")
    print(f"\nUsage:")
    print(f"  Mark with correct YT Music name (skips downloads):")
    print(f"    musicadet artists-issues --fix SPOTIFY_ID:NewName")
    print(f"\n  Remove artist entirely:")
    print(f"    musicadet artists-issues --remove SPOTIFY_ID")


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
                blocked = db.execute(
                    "SELECT active FROM artists WHERE spotify_id=?", (sid,)
                ).fetchone()
                if blocked and blocked["active"] == -1:
                    continue
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


def _verify_artist_ytmusic_reachable(sid: str, name: str, index: int, total: int) -> bool:
    """Ensure the artist has a verified YT Music mapping before downloading."""
    if CFG.get("artist_scanner", "ytmusic") != "ytmusic":
        return True

    with db_connect() as db:
        row = db.execute(
            "SELECT ytmusic_status FROM artists WHERE spotify_id=?",
            (sid,),
        ).fetchone()
    status = (row["ytmusic_status"] if row else None) or "unknown"
    status = status.lower()
    if status in ("found", "manually_mapped"):
        return True
    if status == "not_found":
        log.info("[%d/%d] %s — retrying YT Music lookup (was not_found)", index, total, name)
        with db_connect() as db:
            db.execute(
                "UPDATE artists SET ytmusic_status='unknown', ytmusic_notes='Auto-retry after previous failure' WHERE spotify_id=?",
                (sid,),
            )
        status = "unknown"

    log.info("[%d/%d] %s — verifying YT Music reachability before download", index, total, name)
    with db_connect() as db:
        row = db.execute("SELECT * FROM artists WHERE spotify_id=?", (sid,)).fetchone()
    if not row:
        log.warning("[%d/%d] %s — artist record missing from DB", index, total, name)
        return False

    scan_artist_catalog(row)

    with db_connect() as db:
        row = db.execute(
            "SELECT ytmusic_status FROM artists WHERE spotify_id=?",
            (sid,),
        ).fetchone()
    new_status = (row["ytmusic_status"] if row else None) or "unknown"
    new_status = new_status.lower()
    if new_status in ("found", "manually_mapped"):
        log.info("[%d/%d] %s — YT Music verification succeeded", index, total, name)
        return True

    log.warning(
        "[%d/%d] %s — YT Music still unreachable after verification (%s)",
        index, total, name, new_status,
    )
    return False


def _sync_artist_with_ytdlp(
    sid: str,
    name: str,
    songs: list,
    index: int,
    total: int,
    allowed_song_ids: set = None,
    skip_album_completeness: bool = False,
) -> bool:
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

    # Check if we're rate-limited before starting
    rate_wait = custom_dl.check_rate_limit()
    if rate_wait is not None:
        log.error("[%d/%d] %s — RATE-LIMITED. Wait %d seconds before retrying.", 
                  index, total, name, rate_wait)
        return False

    music_dir = Path(CFG["music_dir"])
    db_path = Path(CFG["db_path"])
    fmt = CFG.get("format", "opus")

    downloader = _make_ytdlp_downloader(music_dir, fmt)

    # Check if artist is marked as manually_mapped (user-corrected) — skip download phase
    with db_connect() as db:
        artist_row = db.execute(
            "SELECT ytmusic_status, ytmusic_name FROM artists WHERE spotify_id=?",
            (sid,),
        ).fetchone()
    
    if artist_row and artist_row["ytmusic_status"] == "manually_mapped":
        log.info("[%d/%d] %s — marked as manually mapped, skipping download phase", index, total, name)
        with db_connect() as db:
            db.execute(
                "UPDATE artists SET sync_done=1, last_synced=datetime('now') WHERE spotify_id=?",
                (sid,),
            )
        return True

    if not _verify_artist_ytmusic_reachable(sid, name, index, total):
        return False

    if allowed_song_ids is not None and len(allowed_song_ids) == 0:
        log.info("[%d/%d] %s — at download cap, skipping", index, total, name)
        return True

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
            SELECT s.spotify_id, s.album_id, s.title, s.track_number, s.status, s.file_path, s.youtube_url,
                   al.name AS album_name
            FROM songs s
            JOIN albums al ON s.album_id = al.spotify_id
            WHERE s.artist_id = ? AND s.status != 'downloaded'
            ORDER BY al.name, s.track_number
            """,
            (sid,),
        ).fetchall()

    if allowed_song_ids is not None:
        pending_rows = [r for r in pending_rows if r["spotify_id"] in allowed_song_ids]

    log.info("[%d/%d] %s — %d tracks to download", index, total, name, len(pending_rows))

    success = True
    for s in pending_rows:
        # Check if rate-limited before each track
        rate_wait = custom_dl.check_rate_limit()
        if rate_wait is not None:
            log.error("    ✗ YouTube rate-limited mid-download. Pausing. Wait %d seconds.", rate_wait)
            success = False
            break
        
        track_num = s["track_number"]
        album_name = s["album_name"] or "Unknown Album"
        
        # If spotify_id is 11 chars (YouTube videoId) and doesn't contain colon, use it directly
        s_id = s["spotify_id"]
        if len(s_id) == 11 and ":" not in s_id:
            yt_url = f"https://music.youtube.com/watch?v={s_id}"
        else:
            yt_url = s["youtube_url"]

        # Check if file already exists on disk
        safe_artist = custom_dl._clean_filename(name)
        with db_connect() as db:
            resolved_album = get_resolved_album_name(db, s["album_id"], album_name, s["title"])
        safe_album = custom_dl._clean_filename(resolved_album)
        safe_title = custom_dl._clean_filename(s["title"])
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
            album=resolved_album,
            track_number=track_num,
            yt_url=yt_url,
        )

        with db_connect() as db:
            if result and result.exists():
                # Embed metadata
                custom_dl.enforce_primary_artist(
                    result, name, s["title"], resolved_album, track_num
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
        db_path,
        music_dir,
        sid,
        name,
        downloader,
        enabled=not skip_album_completeness,
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
    artist_filter = getattr(args, "artist", None)
    remove_track_number_prefixes(Path(CFG["music_dir"]))
    with db_connect() as db:
        if artist_filter:
            # If a specific artist is requested, allow active >= 0
            query = "SELECT * FROM artists WHERE active >= 0 AND (name LIKE ? OR spotify_id=?)"
            artists = db.execute(query + " ORDER BY name COLLATE NOCASE", (f"%{artist_filter}%", artist_filter)).fetchall()
        else:
            query = "SELECT * FROM artists WHERE active=1"
            if new_only:
                query += " AND sync_done=0"
            artists = db.execute(query + " ORDER BY name COLLATE NOCASE").fetchall()

    tag = " (new only)" if new_only else ""
    log.info("Artists to sync%s: %d (sequential)", tag, len(artists))
    total = len(artists)

    max_dl = int(CFG.get("max_downloads_per_artist", 0))
    log.info("▶ Enforcing caps before artist sync")
    enforce_all_download_caps(artist_filter, quiet_if_none=True)
    ok = failed = 0
    for i, a in enumerate(artists, 1):
        # Check if we're rate-limited before syncing this artist
        rate_wait = custom_dl.check_rate_limit()
        if rate_wait is not None:
            log.error("RATE-LIMIT DETECTED. Pausing downloads. Wait %d seconds before retrying.", rate_wait)
            break
        
        sid, name = a["spotify_id"], a["name"]
        with db_connect() as db_conn:
            keep_ids, skip_album = _apply_artist_download_cap(
                db_conn, sid, name, a["max_downloads"], max_dl, prune_skipped=True
            )

        # Download pending tracks (+ album completeness only when uncapped)
        result = _sync_artist_with_ytdlp(
            sid, name, [], i, total,
            allowed_song_ids=keep_ids,
            skip_album_completeness=skip_album,
        )
        if result:
            ok += 1
        else:
            failed += 1

    log.info("Artists sync done — ✓ %d  ✗ %d", ok, failed)
    log.info("▶ Final cap enforcement")
    enforce_all_download_caps(artist_filter, quiet_if_none=True)


def get_resolved_album_name(db_conn, album_id: str, album_name: str, song_title: str) -> str:
    """
    Resolve album name. If the album has <= 5 songs in the database, return 'Singles'.
    Otherwise, fall back to detect_singles.
    """
    if not album_name or album_name == "Unknown Album":
        return "Singles"

    row = db_conn.execute(
        "SELECT track_count FROM albums WHERE spotify_id=?", (album_id,)
    ).fetchone()
    if row and row["track_count"] is not None and row["track_count"] < 5:
        return "Singles"

    if custom_dl:
        return custom_dl.detect_singles(album_name, song_title)
    return album_name


def _artist_effective_limit(artist_max_downloads, global_max: int) -> int:
    """Per-artist cap; NULL in DB falls back to global config (0 = unlimited)."""
    if artist_max_downloads is not None:
        return int(artist_max_downloads or 0)
    return int(global_max or 0)


def _pending_songs_for_artist(db_conn, sid: str) -> list:
    return db_conn.execute(
        """
        SELECT s.spotify_id, s.title, s.youtube_url
        FROM songs s
        JOIN albums al ON s.album_id = al.spotify_id
        WHERE s.artist_id = ? AND s.status != 'downloaded'
        ORDER BY al.release_year DESC, al.name, s.track_number
        """,
        (sid,),
    ).fetchall()


def _apply_artist_download_cap(
    db_conn,
    sid: str,
    name: str,
    artist_max_downloads,
    global_max: int,
    *,
    prune_skipped: bool = False,
) -> tuple[set | None, bool]:
    """
    Choose which pending song IDs may be downloaded under max_downloads.

    Returns (allowed_song_ids, skip_album_completeness).
      - allowed_song_ids None → no cap
      - empty set → already at cap, nothing to download
    """
    limit = _artist_effective_limit(artist_max_downloads, global_max)
    if limit <= 0:
        return None, False

    artist_row = db_conn.execute(
        "SELECT ytmusic_status, ytmusic_browse_id FROM artists WHERE spotify_id=?", (sid,)
    ).fetchone()
    ytmusic_status = (artist_row["ytmusic_status"] if artist_row else None) or "unknown"
    ytm_browse_id = artist_row["ytmusic_browse_id"] if artist_row else None

    downloaded = db_conn.execute(
        "SELECT COUNT(*) FROM songs WHERE artist_id=? AND status='downloaded'",
        (sid,),
    ).fetchone()[0]

    pending_songs = _pending_songs_for_artist(db_conn, sid)
    if downloaded >= limit:
        if prune_skipped and pending_songs:
            skip_ids = [ps["spotify_id"] for ps in pending_songs]
            db_conn.execute(
                f"DELETE FROM songs WHERE spotify_id IN ({','.join(['?'] * len(skip_ids))})",
                skip_ids,
            )
            log.info(
                "  -> %s: at cap (%d downloaded); removed %d pending tracks from queue",
                name, limit, len(skip_ids),
            )
        return set(), True

    remaining = limit - downloaded
    if not pending_songs:
        return set(), True

    if len(pending_songs) <= remaining:
        return {ps["spotify_id"] for ps in pending_songs}, True

    if ytmusic_status == "not_found":
        log.warning("  -> %s: ytmusic_status is 'not_found' — skipping downloads.", name)
        keep_ids = set()
        skip_ids = [ps["spotify_id"] for ps in pending_songs]
        if prune_skipped and skip_ids:
            db_conn.execute(
                f"DELETE FROM songs WHERE spotify_id IN ({','.join(['?'] * len(skip_ids))})",
                skip_ids,
            )
            log.info("  -> Deleted %d skipped pending songs for %s", len(skip_ids), name)
        return keep_ids, True

    top_tracks = _get_top_tracks_ordered(name, limit, browse_id=ytm_browse_id)
    if top_tracks:
        # Fetch all songs for this artist (downloaded + pending) to align ranking perfectly
        all_songs = db_conn.execute(
            """
            SELECT s.spotify_id, s.title, s.status, s.youtube_url
            FROM songs s
            WHERE s.artist_id = ?
            """,
            (sid,),
        ).fetchall()

        keep, _ = _rank_songs_for_cap(list(all_songs), top_tracks, limit)

        # Select pending songs from the ranked keep list up to the remaining quota
        matched = []
        for s in keep:
            if len(matched) >= remaining:
                break
            if s["status"] != "downloaded":
                matched.append(s)

        keep_ids = {s["spotify_id"] for s in matched}
        skip_ids = [ps["spotify_id"] for ps in pending_songs if ps["spotify_id"] not in keep_ids]
        log.info(
            "  -> %s: %d/%d YT top songs matched; will download %d (cap %d, %d on disk)",
            name, len(matched), len(top_tracks), len(keep_ids), limit, downloaded,
        )
    else:
        keep_ids = set()
        skip_ids = [ps["spotify_id"] for ps in pending_songs]
        log.warning(
            "  -> %s: no YT Music top list — skipping downloads (set artist URL on Artists tab to fix)",
            name,
        )
        if ytmusic_status not in ("found", "manually_mapped"):
            db_conn.execute(
                "UPDATE artists SET ytmusic_status='not_found', ytmusic_notes='No YT Music top list — add artist URL manually' "
                "WHERE spotify_id=? AND ytmusic_status NOT IN ('manually_mapped', 'found')",
                (sid,),
            )
    if prune_skipped and skip_ids:
        db_conn.execute(
            f"DELETE FROM songs WHERE spotify_id IN ({','.join(['?'] * len(skip_ids))})",
            skip_ids,
        )
        log.info("  -> Deleted %d skipped pending songs for %s", len(skip_ids), name)
    return keep_ids, True


def _youtube_cookies_candidates() -> list[Path]:
    """Paths checked in order (first existing file wins)."""
    explicit = (CFG.get("youtube_cookies_file") or "").strip()
    if explicit:
        return [Path(explicit).expanduser()]
    paths = [
        Path(CFG.get("sync_dir", DEFAULTS["sync_dir"])) / "youtube-cookies.txt",
        BASE / "sync-data" / "youtube-cookies.txt",  # in-repo: git pull on server
    ]
    seen: set[str] = set()
    unique: list[Path] = []
    for p in paths:
        key = str(p.resolve()) if p.exists() else str(p)
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def _resolve_youtube_cookies_file() -> Optional[str]:
    for path in _youtube_cookies_candidates():
        if path.is_file():
            return str(path)
    return None


def _make_ytdlp_downloader(music_dir: Path, fmt: str) -> "custom_dl.YtDlpDownloader":
    return custom_dl.YtDlpDownloader(
        music_dir,
        fmt=fmt,
        cookies_file=_resolve_youtube_cookies_file(),
        cookies_from_browser=CFG.get("youtube_cookies_from_browser") or None,
    )


def _get_top_tracks_ordered(artist_name: str, limit: int, browse_id: Optional[str] = None) -> list[dict]:
    """
    Full top-songs playlist from YT Music (not just the 5-song preview on get_artist).
    Each entry: {title, videoId, norm}.
    """
    import ytm_scanner
    if browse_id is None:
        with db_connect() as db:
            row = db.execute(
                "SELECT ytmusic_browse_id FROM artists WHERE name=? COLLATE NOCASE LIMIT 1",
                (artist_name,),
            ).fetchone()
            if row and row["ytmusic_browse_id"]:
                browse_id = row["ytmusic_browse_id"]
    return ytm_scanner.get_top_songs_ordered(artist_name, limit, cached_browse_id=browse_id)


def _get_top_song_titles_ordered(artist_name: str, limit: int) -> list[str]:
    return [
        t["norm"]
        for t in _get_top_tracks_ordered(artist_name, limit)
        if t.get("norm")
    ]


def _get_top_songs_for_artist(artist_name: str, limit: int) -> set:
    return set(_get_top_song_titles_ordered(artist_name, limit))


def _rank_songs_for_cap(songs: list, top_tracks: list[dict], limit: int) -> tuple[list, list]:
    """Keep library rows matching YT top-songs playlist order (videoId + title), up to limit."""
    if limit <= 0 or not songs:
        return list(songs), []

    if not top_tracks:
        return list(songs), []

    keep: list = []
    used_ids: set[str] = set()

    for entry in top_tracks:
        if len(keep) >= limit:
            break
        for row in songs:
            if row["spotify_id"] in used_ids:
                continue
            if _song_matches_top_entry(row, entry):
                keep.append(row)
                used_ids.add(row["spotify_id"])
                break

    drop = [s for s in songs if s["spotify_id"] not in used_ids]
    return keep, drop


def _delete_song_file(music_dir: Path, file_path: Optional[str]) -> bool:
    if not file_path:
        return False
    fp = Path(file_path)
    if not fp.is_absolute():
        fp = music_dir / fp
    if not fp.is_file():
        return False
    try:
        fp.unlink()
        parent = fp.parent
        for _ in range(4):
            if parent == music_dir or not parent.is_dir():
                break
            try:
                if not any(parent.iterdir()):
                    parent.rmdir()
                    parent = parent.parent
                else:
                    break
            except OSError:
                break
        return True
    except OSError as e:
        log.warning("Could not delete %s: %s", fp, e)
        return False


def _sweep_orphan_artist_files(music_dir: Path, artist_name: str, kept_rels: set[str]) -> int:
    """Remove audio files on disk for this artist that are not in kept_rels."""
    if not custom_dl:
        return 0
    folder = music_dir / custom_dl._clean_filename(artist_name)
    if not folder.is_dir():
        return 0
    removed = 0
    for f in folder.rglob("*"):
        if f.suffix.lower() not in AUDIO_EXTS or not f.is_file():
            continue
        try:
            rel = str(f.relative_to(music_dir)).replace("\\", "/")
        except ValueError:
            continue
        if rel not in kept_rels:
            try:
                f.unlink()
                removed += 1
            except OSError as e:
                log.warning("Could not delete orphan %s: %s", f, e)
    return removed


def prune_artist_to_cap(
    db_conn,
    sid: str,
    name: str,
    artist_max_downloads,
    global_max: int,
    *,
    dry_run: bool = False,
) -> dict:
    """
    Keep only top-viewed tracks up to max_downloads; delete other files and DB rows.
    """
    stats = {"kept": 0, "removed_files": 0, "removed_db": 0, "orphan_files": 0}
    limit = _artist_effective_limit(artist_max_downloads, global_max)
    if limit <= 0:
        return stats

    rows = db_conn.execute(
        """
        SELECT s.spotify_id, s.title, s.file_path, s.status,
               s.youtube_url, al.release_year
        FROM songs s
        JOIN albums al ON s.album_id = al.spotify_id
        WHERE s.artist_id = ?
        """,
        (sid,),
    ).fetchall()
    if not rows:
        return stats

    artist_row = db_conn.execute(
        "SELECT ytmusic_status, ytmusic_browse_id FROM artists WHERE spotify_id=?", (sid,)
    ).fetchone()
    ytmusic_status = (artist_row["ytmusic_status"] if artist_row else None) or "unknown"
    ytm_browse_id = artist_row["ytmusic_browse_id"] if artist_row else None

    downloaded = [r for r in rows if r["status"] == "downloaded"]
    if len(downloaded) <= limit and len(rows) == len(downloaded):
        stats["kept"] = len(downloaded)
        return stats

    top_tracks = _get_top_tracks_ordered(name, limit, browse_id=ytm_browse_id)
    if not top_tracks:
        log.warning("  %s: no YT Music top list — cap not applied (add artist URL on Artists tab)", name)
        stats["kept"] = len(rows)
        return stats

    keep, drop = _rank_songs_for_cap(list(rows), top_tracks, limit)
    drop_ids = {s["spotify_id"] for s in drop}
    if not drop_ids:
        stats["kept"] = len(keep)
        return stats

    music_dir = Path(CFG["music_dir"])
    kept_rels: set[str] = set()

    log.info(
        "  %s: cap %d — %d YT top songs, %d matched in library (of %d total)%s",
        name, limit, len(top_tracks), len(keep), len(rows),
        " [dry-run]" if dry_run else "",
    )

    for s in keep:
        if s["file_path"]:
            rel = str(s["file_path"]).replace("\\", "/")
            if not Path(rel).is_absolute():
                kept_rels.add(rel)

    for s in drop:
        if s["status"] == "downloaded" and s["file_path"]:
            rel = str(s["file_path"]).replace("\\", "/")
            if not dry_run and _delete_song_file(music_dir, rel):
                stats["removed_files"] += 1
            elif dry_run:
                stats["removed_files"] += 1

    if not dry_run:
        db_conn.execute(
            f"DELETE FROM songs WHERE spotify_id IN ({','.join(['?'] * len(drop_ids))})",
            list(drop_ids),
        )
        stats["removed_db"] = len(drop_ids)
        stats["orphan_files"] = _sweep_orphan_artist_files(music_dir, name, kept_rels)

        for album_id_row in db_conn.execute(
            "SELECT spotify_id FROM albums WHERE artist_id=?", (sid,)
        ).fetchall():
            album_id = album_id_row["spotify_id"]
            row = db_conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN status='downloaded' THEN 1 ELSE 0 END) AS done
                FROM songs WHERE album_id=?
                """,
                (album_id,),
            ).fetchone()
            db_conn.execute(
                "UPDATE albums SET track_count=?, downloaded_count=? WHERE spotify_id=?",
                (row["total"], row["done"] or 0, album_id),
            )
    else:
        stats["removed_db"] = len(drop_ids)

    stats["kept"] = len(keep)
    return stats


def cmd_cookies_check(_args):
    """Verify YouTube cookies file is present and accepted by yt-dlp."""
    path = _resolve_youtube_cookies_file()
    browser = (CFG.get("youtube_cookies_from_browser") or "").strip()
    repo_path = BASE / "sync-data" / "youtube-cookies.txt"

    if not path and not browser:
        log.error("No cookies configured.")
        log.info("Easiest via Git: commit this file in your repo (private repo only):")
        log.info("  %s", repo_path)
        log.info("Then on the server: git pull && musicadet cookies-check")
        log.info("Or copy to sync_dir: %s", Path(CFG.get("sync_dir", DEFAULTS["sync_dir"])) / "youtube-cookies.txt")
        log.info("Export from PC with browser extension 'Get cookies.txt LOCALLY'.")
        return

    if path:
        log.info("Using cookies file: %s", path)
    if browser:
        log.info("Using cookies from browser: %s", browser)

    if not custom_dl or not custom_dl.yt_dlp:
        log.error("yt-dlp not installed")
        return

    downloader = _make_ytdlp_downloader(Path(CFG["music_dir"]), CFG.get("format", "opus"))
    test_url = "https://www.youtube.com/watch?v=jNQXAC9IVRw"  # short public video
    opts = {**downloader._base_opts(), "extract_flat": True}
    try:
        with custom_dl.yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(test_url, download=False)
        if info:
            log.info("Cookies OK — YouTube returned metadata for a test video.")
        else:
            log.warning("Test returned no info; cookies may be expired.")
    except Exception as e:
        err = str(e).lower()
        if "bot" in err or "sign in" in err:
            log.error("Cookies rejected (bot check). Re-export cookies from your browser.")
        else:
            log.error("Cookie test failed: %s", e)


def enforce_all_download_caps(
    artist_filter: Optional[str] = None,
    *,
    dry_run: bool = False,
    quiet_if_none: bool = False,
) -> dict:
    """
    Keep only top-viewed tracks per artist cap; delete other files and DB rows.
    Called automatically before/after download and after reconcile.
    """
    global_max = int(CFG.get("max_downloads_per_artist", 0))
    summary = {"artists": 0, "removed_files": 0, "removed_db": 0, "orphan_files": 0}

    with db_connect() as db:
        if artist_filter:
            artists = db.execute(
                "SELECT spotify_id, name, max_downloads FROM artists WHERE active=1 "
                "AND (name LIKE ? OR spotify_id=?)",
                (f"%{artist_filter}%", artist_filter),
            ).fetchall()
        else:
            artists = db.execute(
                "SELECT spotify_id, name, max_downloads FROM artists WHERE active=1"
            ).fetchall()

    capped = [
        a for a in artists
        if _artist_effective_limit(a["max_downloads"], global_max) > 0
    ]
    if not capped:
        if not quiet_if_none:
            log.info("No download caps set (per-artist limit or max_downloads_per_artist).")
        return summary

    log.info(
        "Enforcing top-viewed caps for %d artist(s)%s",
        len(capped), " [dry-run]" if dry_run else "",
    )
    for a in capped:
        with db_connect() as db:
            stats = prune_artist_to_cap(
                db, a["spotify_id"], a["name"], a["max_downloads"], global_max, dry_run=dry_run
            )
        if stats["removed_files"] or stats["removed_db"] or stats["orphan_files"]:
            log.info(
                "  %s: kept %d — removed %d file(s), %d DB row(s), %d orphan file(s)",
                a["name"], stats["kept"], stats["removed_files"],
                stats["removed_db"], stats["orphan_files"],
            )
        summary["artists"] += 1
        summary["removed_files"] += stats["removed_files"]
        summary["removed_db"] += stats["removed_db"]
        summary["orphan_files"] += stats["orphan_files"]

    if summary["removed_files"] or summary["removed_db"] or summary["orphan_files"]:
        log.info(
            "Cap enforcement done — %d file(s), %d DB row(s), %d orphan file(s) removed",
            summary["removed_files"], summary["removed_db"], summary["orphan_files"],
        )
    return summary


def cmd_prune_caps(args):
    """Delete downloaded tracks (and files) over per-artist max_downloads (top viewed)."""
    enforce_all_download_caps(
        getattr(args, "artist", None),
        dry_run=getattr(args, "dry_run", False),
    )


def cmd_download_pending(args):
    """Download pending tracks (top viewed only when artist has a cap)."""
    nested = getattr(args, "_nested", False)
    if not nested:
        log.info("▶ Enforcing caps before download (keeps top viewed, deletes the rest)")
    enforce_all_download_caps(quiet_if_none=True)
    with db_connect() as db:
        # Get artists that actually have pending songs
        artists = db.execute("""
            SELECT DISTINCT a.spotify_id, a.name, a.max_downloads
            FROM artists a 
            JOIN songs s ON s.artist_id = a.spotify_id 
            WHERE a.active=1 AND s.status != 'downloaded'
            ORDER BY a.name COLLATE NOCASE
        """).fetchall()

    workers = min(int(CFG.get("scan_concurrency", 4)), max(len(artists), 1))
    log.info("Artists with pending tracks: %d (×%d workers)", len(artists), workers)
    total = len(artists)
    
    max_dl = int(CFG.get("max_downloads_per_artist", 0))

    def _dl_worker(i_a):
        i, a = i_a
        sid, name, artist_limit = a["spotify_id"], a["name"], a["max_downloads"]
        with db_connect() as db_conn:
            keep_ids, skip_album = _apply_artist_download_cap(
                db_conn, sid, name, artist_limit, max_dl, prune_skipped=True
            )
        if keep_ids is not None:
            log.info("[%d/%d] %s: download cap active (%d track(s) queued)", i, total, name, len(keep_ids))
        return _sync_artist_with_ytdlp(
            sid, name, [], i, total,
            allowed_song_ids=keep_ids,
            skip_album_completeness=skip_album,
        )

    ok = failed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(_dl_worker, enumerate(artists, 1)))
        
    for res in results:
        if res: ok += 1
        else: failed += 1

    log.info("Download pending complete — ✓ %d  ✗ %d", ok, failed)
    if not nested:
        log.info("▶ Enforcing caps after download")
    enforce_all_download_caps(quiet_if_none=True)


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
    downloader = _make_ytdlp_downloader(music_dir, fmt)

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
                INSERT INTO artists (spotify_id, name, source, active, max_downloads) VALUES (?,?,'playlist', 0, 0)
                ON CONFLICT(spotify_id) DO UPDATE SET
                    name = excluded.name
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
            if song_id:
                song_row = db.execute("SELECT album_id FROM songs WHERE spotify_id=?", (song_id,)).fetchone()
                if song_row:
                    album = get_resolved_album_name(db, song_row["album_id"], album, title)
                else:
                    album = custom_dl.detect_singles(album, title)
            else:
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
    global_max_dl = int(CFG.get("max_downloads_per_artist", 0))
    for artist_sid, artist_name in sorted(artist_ids_found, key=lambda x: x[1]):
        log.info("  Artist: %s", artist_name)
        with db_connect() as db:
            row = db.execute(
                "SELECT max_downloads FROM artists WHERE spotify_id=?", (artist_sid,)
            ).fetchone()
            artist_cap = row["max_downloads"] if row else None
        skip_album = _artist_effective_limit(artist_cap, global_max_dl) > 0
        fixed = custom_dl.check_and_complete_artist_albums(
            db_path, music_dir, artist_sid, artist_name, downloader, enabled=not skip_album
        )
        if fixed:
            reconcile_artist_downloads(artist_sid, artist_name)

    for artist_sid, artist_name in sorted(artist_ids_found, key=lambda x: x[1]):
        enforce_all_download_caps(artist_sid, quiet_if_none=True)

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
    downloader = _make_ytdlp_downloader(music_dir, fmt)

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
    remove_track_number_prefixes(Path(CFG["music_dir"]))
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
                    db.execute("INSERT INTO artists (spotify_id, name, source, active) VALUES (?, ?, 'local', 0)", (artist_id, artist_dir.name))
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


# ─────────────────────────────────────────────────────────────────────────────
# Library integrity — disk ↔ DB truth, orphan cleanup, AudioMuse sync
# ─────────────────────────────────────────────────────────────────────────────


def count_files_on_disk(music_dir: Optional[Path] = None) -> int:
    root = music_dir or Path(CFG["music_dir"])
    if not root.exists():
        return 0
    n = 0
    for _r, _d, files in os.walk(root):
        for fname in files:
            if Path(fname).suffix.lower() in AUDIO_EXTS:
                n += 1
    return n


def collect_library_truth() -> dict:
    """Real counts: disk files vs DB rows with existing files."""
    music_dir = Path(CFG["music_dir"])
    with db_connect() as db:
        rows = db.execute("""
            SELECT s.status, s.file_path
            FROM songs s
            JOIN artists a ON a.spotify_id = s.artist_id
            WHERE a.active >= 0
        """).fetchall()
        artists_visible = db.execute(
            "SELECT COUNT(*) FROM artists WHERE active >= 0"
        ).fetchone()[0]
        artists_active = db.execute(
            "SELECT COUNT(*) FROM artists WHERE active = 1"
        ).fetchone()[0]
        albums_total = db.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
        songs_db = db.execute("SELECT COUNT(*) FROM songs").fetchone()[0]

    verified = downloaded = pending = failed = 0
    kept_paths: set[str] = set()
    for row in rows:
        st = row["status"]
        rel = (row["file_path"] or "").replace("\\", "/")
        fp = music_dir / rel if rel and not Path(rel).is_absolute() else (Path(rel) if rel else None)
        exists = bool(fp and fp.is_file())
        if st == "downloaded":
            downloaded += 1
            if exists:
                verified += 1
                kept_paths.add(rel)
        elif st == "pending":
            pending += 1
        elif st == "failed":
            failed += 1

    return {
        "files_on_disk": count_files_on_disk(music_dir),
        "songs_verified": verified,
        "songs_downloaded_db": downloaded,
        "songs_pending": pending,
        "songs_failed": failed,
        "songs_db_total": songs_db,
        "kept_paths": kept_paths,
        "artists_visible": artists_visible,
        "artists_active": artists_active,
        "albums_total": albums_total,
    }


def log_library_truth(prefix: str = "") -> dict:
    truth = collect_library_truth()
    tag = f"{prefix} " if prefix else ""
    log.info(
        "%sLibrary truth — %d files on disk | %d verified in DB | "
        "%d marked downloaded (missing file: %d) | %d pending | %d DB rows total",
        tag,
        truth["files_on_disk"],
        truth["songs_verified"],
        truth["songs_downloaded_db"],
        max(0, truth["songs_downloaded_db"] - truth["songs_verified"]),
        truth["songs_pending"],
        truth["songs_db_total"],
    )
    ghost = truth["files_on_disk"] - truth["songs_verified"]
    if ghost > 0:
        log.info(
            "%s  → %d file(s) on disk not verified in DB (orphans or inactive artists)",
            tag, ghost,
        )
    return truth


def _collect_kept_paths_from_index(
    by_full: dict, by_album_title: dict, db: sqlite3.Connection
) -> set[str]:
    """Paths that reconcile would match — used to avoid deleting valid but unlinked files."""
    kept: set[str] = set()
    music_dir = Path(CFG["music_dir"])
    songs = db.execute("""
        SELECT s.file_path, s.title, s.status, a.name AS artist_name, al.name AS album_name
        FROM songs s
        JOIN artists a ON a.spotify_id = s.artist_id
        JOIN albums al ON al.spotify_id = s.album_id
        WHERE a.active >= 0
    """).fetchall()
    for song in songs:
        rel = (song["file_path"] or "").replace("\\", "/")
        if rel:
            fp = music_dir / rel
            if fp.is_file():
                kept.add(rel)
        title_key = song["title"].strip().lower()
        album_key = (song["album_name"] or "").lower()
        artist_key = song["artist_name"].lower()
        for key in [
            (artist_key, album_key, title_key),
            (artist_key, "", title_key),
        ]:
            hit = by_full.get(key)
            if hit:
                kept.add(str(hit.relative_to(music_dir)).replace("\\", "/"))
        if album_key:
            candidates = by_album_title.get((album_key, title_key)) or []
            for hit in candidates:
                disk_artist = hit.relative_to(music_dir).parts[0]
                if _is_matching_artist(disk_artist, song["artist_name"], song["artist_id"]):
                    kept.add(str(hit.relative_to(music_dir)).replace("\\", "/"))
    return kept


def sweep_global_orphan_files(
    *,
    kept_paths: Optional[set[str]] = None,
    dry_run: bool = False,
) -> int:
    """Delete audio files on disk that are not referenced or matchable in the DB."""
    music_dir = Path(CFG["music_dir"])
    if not music_dir.exists():
        return 0

    if kept_paths is None:
        file_index = _build_file_index(music_dir)
        with db_connect() as db:
            kept_paths = _collect_kept_paths_from_index(*file_index, db)
            for rel in db.execute(
                """
                SELECT s.file_path FROM songs s
                JOIN artists a ON a.spotify_id = s.artist_id
                WHERE a.active >= 0 AND s.status = 'downloaded' AND s.file_path IS NOT NULL
                """
            ).fetchall():
                r = (rel["file_path"] or "").replace("\\", "/")
                if r:
                    kept_paths.add(r)

    removed = 0
    for root, _dirs, files in os.walk(music_dir):
        for fname in files:
            if Path(fname).suffix.lower() not in AUDIO_EXTS:
                continue
            full = Path(root) / fname
            try:
                rel = str(full.relative_to(music_dir)).replace("\\", "/")
            except ValueError:
                continue
            if rel in kept_paths:
                continue
            if dry_run:
                log.info("  [dry-run] Would remove orphan: %s", rel)
                removed += 1
                continue
            try:
                full.unlink()
                removed += 1
                log.info("  Removed orphan file: %s", rel)
                parent = full.parent
                while parent != music_dir and parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
                    parent = parent.parent
            except OSError as e:
                log.warning("Could not remove orphan %s: %s", full, e)
    if removed:
        log.info("Orphan sweep — %d file(s) %s", removed, "would be removed" if dry_run else "removed")
    return removed


def _audiomuse_cfg() -> Optional[dict]:
    am = CFG.get("audiomuse") or {}
    if not am.get("enabled"):
        return None
    return am


def _audiomuse_psql(am: dict, sql: str) -> tuple[int, str, str]:
    container = am.get("postgres_container", "audiomuse-postgres")
    user = am.get("postgres_user", "audiomuse")
    dbname = am.get("postgres_db", "audiomusedb")
    cmd = [
        "docker", "exec", "-i", container,
        "psql", "-U", user, "-d", dbname,
        "-v", "ON_ERROR_STOP=1", "-t", "-A", "-c", sql,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return proc.returncode, proc.stdout or "", proc.stderr or ""
    except (OSError, subprocess.TimeoutExpired) as e:
        return 1, "", str(e)


def sync_audiomuse_scores(
    moved_paths: Optional[list[tuple[str, str]]] = None,
    *,
    verified_paths: Optional[set[str]] = None,
) -> dict:
    """
    Align AudioMuse score rows with files on disk.
    - UPDATE file_path when MusicaDet moved a file
    - DELETE rows whose file_path no longer exists
    - Optionally DELETE rows not in MusicaDet verified set
    """
    am = _audiomuse_cfg()
    stats = {"updated": 0, "removed_missing": 0, "removed_unlisted": 0, "skipped": False}
    if not am:
        stats["skipped"] = True
        return stats

    music_dir = Path(am.get("music_dir") or CFG["music_dir"])
    moved_paths = moved_paths or []

    for old_rel, new_rel in moved_paths:
        old_sql = old_rel.replace("'", "''")
        new_sql = new_rel.replace("'", "''")
        rc, _, err = _audiomuse_psql(
            am,
            f"UPDATE score SET file_path = '{new_sql}' WHERE file_path = '{old_sql}';",
        )
        if rc == 0:
            stats["updated"] += 1
            log.info("AudioMuse path updated: %s → %s", old_rel, new_rel)
        else:
            log.warning("AudioMuse path update failed (%s → %s): %s", old_rel, new_rel, err)

    rc, out, err = _audiomuse_psql(am, "SELECT item_id, file_path FROM score WHERE file_path IS NOT NULL AND file_path != '';")
    if rc != 0:
        log.warning("AudioMuse sync skipped — cannot read score table: %s", err or out)
        return stats

    missing_ids: list[str] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|", 1)
        if len(parts) != 2:
            continue
        item_id, rel = parts[0], parts[1].replace("\\", "/")
        fp = music_dir / rel if not Path(rel).is_absolute() else Path(rel)
        if not fp.is_file():
            missing_ids.append(item_id.replace("'", "''"))

    if missing_ids:
        chunk = 200
        for i in range(0, len(missing_ids), chunk):
            batch = missing_ids[i:i + chunk]
            ids_sql = ",".join(f"'{x}'" for x in batch)
            rc, out, err = _audiomuse_psql(am, f"DELETE FROM score WHERE item_id IN ({ids_sql});")
            if rc == 0:
                stats["removed_missing"] += len(batch)
            else:
                log.warning("AudioMuse delete missing failed: %s", err or out)
        log.info("AudioMuse — removed %d score(s) with missing files", stats["removed_missing"])

    if am.get("prune_unlisted_scores") and verified_paths is not None:
        rc, out, err = _audiomuse_psql(
            am, "SELECT item_id, file_path FROM score WHERE file_path IS NOT NULL AND file_path != '';"
        )
        if rc == 0:
            unlisted: list[str] = []
            for line in out.splitlines():
                line = line.strip()
                if not line or "|" not in line:
                    continue
                item_id, rel = line.split("|", 1)
                rel = rel.replace("\\", "/")
                if rel not in verified_paths:
                    unlisted.append(item_id.replace("'", "''"))
            if unlisted:
                chunk = 200
                for i in range(0, len(unlisted), chunk):
                    batch = unlisted[i:i + chunk]
                    ids_sql = ",".join(f"'{x}'" for x in batch)
                    rc, _, err = _audiomuse_psql(am, f"DELETE FROM score WHERE item_id IN ({ids_sql});")
                    if rc == 0:
                        stats["removed_unlisted"] += len(batch)
                    else:
                        log.warning("AudioMuse prune unlisted failed: %s", err)
                log.info(
                    "AudioMuse — pruned %d score(s) not in MusicaDet verified set",
                    stats["removed_unlisted"],
                )

    rc, out, err = _audiomuse_psql(am, "SELECT COUNT(*) FROM score;")
    if rc == 0 and out.strip():
        try:
            total = int(out.strip().split("\n")[-1])
            log.info("AudioMuse score table now has %d row(s)", total)
        except ValueError:
            pass
    return stats


def run_library_refresh(artist_filter: Optional[str] = None, *, skip_prefix_cleanup: bool = False) -> dict:
    """
    Full disk ↔ DB refresh: reconcile, album cleanup, orphan sweep, AudioMuse sync.
    """
    summary = {
        "artists": 0,
        "downloaded": 0,
        "pending": 0,
        "orphans_removed": 0,
        "moved_paths": [],
        "audiomuse": {},
    }
    if not skip_prefix_cleanup:
        remove_track_number_prefixes(Path(CFG["music_dir"]))

    log.info("▶ File matching — reconcile DB with disk")
    with db_connect() as db:
        if artist_filter:
            artists = db.execute(
                "SELECT * FROM artists WHERE active=1 AND (name LIKE ? OR spotify_id=?)",
                (f"%{artist_filter}%", artist_filter),
            ).fetchall()
        else:
            artists = db.execute("SELECT * FROM artists WHERE active=1").fetchall()

    file_index = _build_file_index(Path(CFG["music_dir"]))
    all_moved: list[tuple[str, str]] = []
    for a in artists:
        log.info("Reconciling: %s", a["name"])
        stats = reconcile_artist_downloads(a["spotify_id"], a["name"], file_index=file_index)
        log.info("  → %d downloaded, %d pending", stats["downloaded"], stats["pending"])
        summary["artists"] += 1
        summary["downloaded"] += stats["downloaded"]
        summary["pending"] += stats["pending"]
        all_moved.extend(stats.get("moved_paths") or [])

    summary["moved_paths"] = all_moved

    log.info("▶ Album cleanup after reconcile")
    with db_connect() as db:
        db.execute("""
            UPDATE albums SET
                track_count = (SELECT COUNT(*) FROM songs WHERE album_id=albums.spotify_id),
                downloaded_count = (SELECT COUNT(*) FROM songs WHERE album_id=albums.spotify_id AND status='downloaded')
        """)
        removed = db.execute("DELETE FROM albums WHERE track_count=0").rowcount
        if removed:
            log.info("  → Removed %d empty album(s)", removed)
        merged = consolidate_small_albums(db)
        if merged:
            log.info("  → Merged %d small album(s) into Singles", merged)
        db.execute("""
            UPDATE albums SET
                track_count = (SELECT COUNT(*) FROM songs WHERE album_id=albums.spotify_id),
                downloaded_count = (SELECT COUNT(*) FROM songs WHERE album_id=albums.spotify_id AND status='downloaded')
        """)

    log.info("▶ Orphan file sweep (disk files with no DB match)")
    file_index = _build_file_index(Path(CFG["music_dir"]))
    with db_connect() as db:
        kept_paths = _collect_kept_paths_from_index(*file_index, db)
    summary["orphans_removed"] = sweep_global_orphan_files(kept_paths=kept_paths)

    log.info("▶ Enforcing caps after reconcile")
    enforce_all_download_caps(artist_filter, quiet_if_none=True)

    truth = collect_library_truth()
    log_library_truth("After refresh —")

    log.info("▶ AudioMuse sync")
    summary["audiomuse"] = sync_audiomuse_scores(
        all_moved, verified_paths=truth["kept_paths"],
    )
    return summary


def cmd_reconcile(args):
    artist_filter = getattr(args, "artist", None)
    run_library_refresh(artist_filter, skip_prefix_cleanup=False)


def cmd_repair_albums(args):
    """Recompute `track_count` and `downloaded_count` for all albums.
    Optionally delete albums that have `track_count == 0`.
    """
    remove_empty = getattr(args, "remove_empty", False)
    dry_run = getattr(args, "dry_run", False)
    with db_connect() as db:
        log.info("Recomputing album track/downloaded counts...")
        db.execute("""
            UPDATE albums SET 
            track_count = (SELECT COUNT(*) FROM songs WHERE album_id=albums.spotify_id),
            downloaded_count = (SELECT COUNT(*) FROM songs WHERE album_id=albums.spotify_id AND status='downloaded')
        """)
        db.commit()

        empties = db.execute("SELECT spotify_id, artist_id, name, track_count, downloaded_count FROM albums WHERE track_count=0 ORDER BY name COLLATE NOCASE").fetchall()
        if not empties:
            log.info("No empty albums found — nothing to do.")
            return

        log.info("Found %d albums with track_count=0", len(empties))
        for r in empties[:200]:
            art = db.execute("SELECT name FROM artists WHERE spotify_id=?", (r["artist_id"],)).fetchone()
            artname = art["name"] if art else r["artist_id"]
            print(f"{r['spotify_id']} — {artname} — {r['name']} (downloaded={r['downloaded_count']})")

        if remove_empty:
            if dry_run:
                log.info("Dry-run: would delete %d empty albums (no changes applied)", len(empties))
                return
            ids = [e["spotify_id"] for e in empties]
            db.execute(f"DELETE FROM albums WHERE spotify_id IN ({','.join(['?']*len(ids))})", ids)
            db.commit()
            log.info("Deleted %d empty albums", len(ids))


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
    log_library_truth("Before sync —")

    log.info("\n▶ Step 1 / 6 — Scanning playlists")
    cmd_scan(argparse.Namespace())

    log.info("\n▶ Step 2 / 6 — Scanning artist albums")
    cmd_scan_artists(argparse.Namespace(new_only=False))

    log.info("\n▶ Step 3 / 6 — File matching (reconcile disk ↔ DB)")
    run_library_refresh(skip_prefix_cleanup=True)

    log.info("\n▶ Step 4 / 6 — Enforcing top-viewed caps (trim library)")
    enforce_all_download_caps(quiet_if_none=True)

    log.info("\n▶ Step 5 / 6 — Downloading top tracks only (respects per-artist limits)")
    cmd_download_pending(argparse.Namespace(_nested=True))

    log.info("\n▶ Step 6 / 6 — Final refresh (orphans + AudioMuse)")
    run_library_refresh(skip_prefix_cleanup=True)

    truth = log_library_truth("Sync complete —")
    with db_connect() as db:
        albums = db.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
        no_cover = db.execute(
            "SELECT COUNT(*) FROM songs WHERE status='downloaded' AND has_cover=0"
        ).fetchone()[0]

    log.info("\n━" * 60)
    log.info(
        "  SYNC COMPLETE — %d albums | %d files on disk | %d verified in DB | %d pending",
        albums,
        truth["files_on_disk"],
        truth["songs_verified"],
        truth["songs_pending"],
    )
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

    subparsers = p.add_subparsers(dest="cmd")
    subparsers.add_parser("sync", help="Full pipeline — default command")
    subparsers.add_parser("scan", help="Scan playlists, discover artists")

    sa = subparsers.add_parser("scan-artists", help="Scan artist albums into DB")
    sa.add_argument("--new-only", action="store_true", help="Only artists not yet scanned")

    as_ = subparsers.add_parser("artists-sync", help="Sync artist discographies (sequential yt-dlp)")
    as_.add_argument("--new-only", action="store_true", help="Only sync artists not yet downloaded")
    as_.add_argument("--artist", help="Sync only this artist (name substring or spotify id)")

    subparsers.add_parser("download-pending", help="Directly download all pending tracks without SpotDL fetches")

    subparsers.add_parser("cookies-check", help="Test YouTube cookies (uploaded file or browser)")

    pr = subparsers.add_parser(
        "prune-caps",
        help="Delete tracks/files over max_downloads (keeps top viewed on YouTube Music)",
    )
    pr.add_argument("--artist", help="Only this artist (name substring or spotify id)")
    pr.add_argument("--dry-run", action="store_true", help="Show what would be deleted")

    ra = subparsers.add_parser(
        "repair-albums",
        help="Recompute album track/downloaded counts and optionally remove empty albums",
    )
    ra.add_argument("--remove-empty", action="store_true", help="Delete albums with track_count == 0")
    ra.add_argument("--dry-run", action="store_true", help="Show what would be removed without deleting")

    # New: playlist-first sync command
    sp = subparsers.add_parser(
        "sync-playlist",
        help="Fetch a Spotify playlist via SpotDL then download all tracks + check artist albums",
    )
    sp.add_argument("url", help="Spotify playlist URL")

    # New: direct download command
    dd = subparsers.add_parser(
        "download",
        help="Directly download a Spotify/YT playlist to local folders (no DB/album checks)",
    )
    dd.add_argument("url", help="Spotify or YouTube URL")

    rec = subparsers.add_parser("reconcile", help="Match filesystem files to DB")
    rec.add_argument("--artist", help="Limit to artist name/id")

    subparsers.add_parser("migrate-structure", help="Move existing flat files to album folders and restrict to primary artist")

    fix = subparsers.add_parser("fix-metadata", help="Re-embed metadata for incomplete songs")
    fix.add_argument("--artist", help="Limit to artist name/id")

    la = subparsers.add_parser("list-albums", help="Show album download progress")
    la.add_argument("artist", nargs="?", help="Filter by artist name")

    add_ = subparsers.add_parser("add", help="Add an artist by URL or name")
    add_.add_argument("artist")

    imp_ = subparsers.add_parser("import", help="Bulk import artists from a text file")
    imp_.add_argument("file")

    subparsers.add_parser("init-db", help="Create/migrate database schema only (no Romanian detection)")
    subparsers.add_parser("list", help="List all artists in the DB")
    
    ytm_issues = subparsers.add_parser("artists-issues", help="List/manage artists with YouTube Music lookup issues")
    ytm_issues.add_argument("--fix", type=str, help="Mark artist with new YT Music name (use: artist_id:new_name)", metavar="ARTIST_ID:NEW_NAME")
    ytm_issues.add_argument("--remove", type=str, help="Remove artist", metavar="ARTIST_ID")

    dis_ = subparsers.add_parser("disable", help="Disable an artist")
    dis_.add_argument("artist")

    en_ = subparsers.add_parser("enable", help="Re-enable a disabled artist")
    en_.add_argument("artist")

    sub = subparsers.add_parser("clean-ytm", help="Wipe all pending YouTube Music data to revert to Spotify scanner")
    subparsers.add_parser("deduplicate", help="Merge duplicate artists and tracks")
    subparsers.add_parser(
        "mark-romanian",
        help="Detect and flag Romanian artists (curated list + MusicBrainz)",
    )

    args = p.parse_args()

    routes = {
        None:                cmd_full_sync,
        "sync":              cmd_full_sync,
        "scan":              cmd_scan,
        "scan-artists":      cmd_scan_artists,
        "artists-sync":      cmd_artists_sync,
        "download-pending":  cmd_download_pending,
        "cookies-check":     cmd_cookies_check,
        "prune-caps":        cmd_prune_caps,
        "sync-playlist":     cmd_sync_playlist,
        "download":          cmd_download_direct,
        "reconcile":         cmd_reconcile,
        "migrate-structure": cmd_migrate_structure,
        "fix-metadata":      cmd_fix_metadata,
        "list-albums":       cmd_list_albums,
        "add":               cmd_add,
        "import":            cmd_import,
        "init-db":           lambda _a: None,
        "list":              cmd_list,
        "artists-issues":    cmd_artists_issues,
        "disable":           cmd_disable,
        "enable":            cmd_enable,
        "clean-ytm":         cmd_clean_ytm,
        "deduplicate":       cmd_deduplicate,
        "mark-romanian":     cmd_mark_romanian,
    }

    fn = routes.get(args.cmd)
    if fn:
        fn(args)
    else:
        p.print_help()


if __name__ == "__main__":
    main()
