#!/usr/bin/env python3
"""
music_sync.py — Automated music library manager for Jellyfin
─────────────────────────────────────────────────────────────
- Scans Spotify playlists daily → discovers artists automatically
- Downloads full artist discographies via spotDL
- SQLite DB tracks artists, playlists, and sync state
- Efficient: only downloads NEW songs on subsequent runs
─────────────────────────────────────────────────────────────
Usage:
  python3 music_sync.py                      # Full sync (default)
  python3 music_sync.py scan                 # Scan playlists only
  python3 music_sync.py artists-sync         # Sync discographies only
  python3 music_sync.py artists-sync --new-only
  python3 music_sync.py add "THE MOTANS"
  python3 music_sync.py add "https://open.spotify.com/artist/..."
  python3 music_sync.py import artists.txt
  python3 music_sync.py list
  python3 music_sync.py disable "Artist Name"
  python3 music_sync.py enable  "Artist Name"
"""

import os
import sys
import json
import sqlite3
import subprocess
import logging
import argparse
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

BASE = Path("/opt/music-sync")
CFG_FILE = BASE / "config.json"

DEFAULTS: dict = {
    "music_dir":        "/mnt/storage_jellyfin/media/music/spotify",
    "sync_dir":         str(BASE / "sync-data"),
    "db_path":          str(BASE / "music.db"),
    "log_dir":          "/var/log/music-sync",
    "format":           "opus",
    "bitrate":          "auto",
    "threads":          4,
    "output_template":  "{album-artist}/{album}/{track-number} - {title}.{output-ext}",
    "playlists": [
        {
            "name": "Today's Top Hits",
            "url":  "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"
        },
        {
            "name": "Top 50 Romania",
            "url":  "https://open.spotify.com/playlist/37i9dQZEVXbNZbJ6TZelCq"
        },
        {
            "name": "Top 50 Global",
            "url":  "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
        },
        {
            "name": "Top melodii Romania",
            "url":  "https://open.spotify.com/playlist/37i9dQZEVXbMeCoUmQDLUW"
        }
    ]
}


def load_cfg() -> dict:
    cfg = DEFAULTS.copy()
    if CFG_FILE.exists():
        try:
            cfg.update(json.loads(CFG_FILE.read_text()))
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
log = logging.getLogger("music-sync")


# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(CFG["db_path"])
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


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
                added_at     TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS playlists (
                spotify_id   TEXT PRIMARY KEY,
                name         TEXT NOT NULL,
                url          TEXT NOT NULL,
                active       INTEGER DEFAULT 1,
                last_synced  TEXT
            );

            -- which artists were found in which playlist
            CREATE TABLE IF NOT EXISTS playlist_artists (
                playlist_id  TEXT NOT NULL REFERENCES playlists(spotify_id) ON DELETE CASCADE,
                artist_id    TEXT NOT NULL REFERENCES artists(spotify_id)   ON DELETE CASCADE,
                PRIMARY KEY  (playlist_id, artist_id)
            );

            CREATE INDEX IF NOT EXISTS idx_artists_active
                ON artists(active, sync_done);
        """)

        # Seed playlists from config
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
    """Extract Spotify ID from a URL like open.spotify.com/artist/ID."""
    marker = f"/{kind}/"
    if marker in url:
        return url.split(marker)[1].split("?")[0].split("/")[0]
    if url.startswith(f"spotify:{kind}:"):
        return url.split(":")[-1]
    return None


def _artist_key(entry: str) -> tuple[str, str]:
    """
    Turn a Spotify URL or plain artist name into (spotify_id, name).
    For URL entries, the spotify_id is the real ID.
    For name entries, we prefix with 'q:' so spotDL uses it as a search query.
    """
    if "open.spotify.com/artist/" in entry:
        sid = _extract_id_from_url(entry, "artist")
        return sid, f"artist:{sid}"
    elif entry.startswith("spotify:artist:"):
        sid = entry.split(":")[-1]
        return sid, f"artist:{sid}"
    else:
        # Name-based: spotDL will search for it
        safe = entry.lower().replace(" ", "_")[:60]
        return f"q:{safe}", entry


# ─────────────────────────────────────────────────────────────────────────────
# spotDL helpers
# ─────────────────────────────────────────────────────────────────────────────

def spotdl_save(url: str) -> list:
    """
    Run `spotdl save URL` to fetch song metadata WITHOUT downloading audio.
    Returns a list of song dicts (empty on failure).
    """
    with tempfile.NamedTemporaryFile(suffix=".spotdl", delete=False) as f:
        tmp = Path(f.name)

    try:
        r = subprocess.run(
            ["spotdl", "save", url, "--save-file", str(tmp)],
            capture_output=True,
            timeout=180,
        )
        if r.returncode != 0:
            log.debug("spotdl save stderr: %s", r.stderr.decode(errors="replace"))

        if not tmp.exists() or tmp.stat().st_size < 5:
            return []

        raw = json.loads(tmp.read_text(encoding="utf-8"))
        return raw if isinstance(raw, list) else raw.get("songs", [])

    except subprocess.TimeoutExpired:
        log.warning("Timeout fetching playlist metadata: %s", url)
        return []
    except json.JSONDecodeError as e:
        log.warning("JSON parse error from spotdl save: %s", e)
        return []
    except Exception as e:
        log.warning("spotdl save failed: %s", e)
        return []
    finally:
        tmp.unlink(missing_ok=True)


def _extract_artist_from_song(song: dict) -> Optional[tuple[str, str]]:
    """
    Extract (spotify_id, name) for the primary artist from a spotDL song dict.
    Returns None if we can't determine the artist.
    """
    # Try various field names spotDL might use across versions
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

    if not sid or not name:
        return None

    # Normalize: strip Spotify URL prefix if present
    if "/" in str(sid):
        sid = str(sid).split("/")[-1].split("?")[0]

    return str(sid), str(name)


def spotdl_sync_artist(spotify_id: str, name: str, target: str) -> bool:
    """
    Download or incrementally sync an artist's discography.
    - First run:  downloads everything + creates .spotdl tracking file
    - Later runs: only downloads new releases (fast)
    Returns True on success.
    """
    sync_file = Path(CFG["sync_dir"]) / f"{spotify_id.replace(':', '_')}.spotdl"
    out_tpl = str(Path(CFG["music_dir"]) / CFG["output_template"])

    base_args = [
        "spotdl", "sync",
        "--output",   out_tpl,
        "--format",   CFG["format"],
        "--bitrate",  str(CFG["bitrate"]),
        "--threads",  str(CFG["threads"]),
        "--log-level", "WARNING",
        "--overwrite", "metadata",
        "--lyrics-provider", "genius", "musixmatch",
    ]

    is_new = not sync_file.exists()
    if is_new:
        cmd = base_args + [target, "--save-file", str(sync_file)]
        log.info("    ↳ First run — downloading full discography")
    else:
        cmd = base_args + [str(sync_file)]
        log.info("    ↳ Checking for new releases (incremental)")

    try:
        r = subprocess.run(cmd, timeout=7200)  # 2 hours max per artist
        if r.returncode != 0 and is_new:
            # Remove corrupt/empty sync file so next run retries cleanly
            if sync_file.exists() and sync_file.stat().st_size < 20:
                sync_file.unlink()
        return r.returncode == 0

    except subprocess.TimeoutExpired:
        log.error("    ✗ Timed out after 2 hours: %s", name)
        return False
    except Exception as e:
        log.error("    ✗ Error: %s", e)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Commands
# ─────────────────────────────────────────────────────────────────────────────

def cmd_add(args):
    """Add a single artist by Spotify URL or name."""
    entry = args.artist.strip()
    sid, name = _artist_key(entry)

    with db_connect() as db:
        cur = db.execute("""
            INSERT INTO artists (spotify_id, name, source) VALUES (?,?,'manual')
            ON CONFLICT(spotify_id) DO UPDATE SET active=1
        """, (sid, name))

    action = "Added" if cur.rowcount else "Already exists (re-enabled)"
    log.info("%s: %s  [%s]", action, name, sid)


def cmd_import(args):
    """Bulk import artists from a text file — one URL or name per line."""
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
    """Print all artists in the DB."""
    with db_connect() as db:
        rows = db.execute("""
            SELECT name, spotify_id, source, active, sync_done, last_synced
            FROM artists
            ORDER BY name COLLATE NOCASE
        """).fetchall()

        pl_count = db.execute("SELECT COUNT(*) FROM playlists WHERE active=1").fetchone()[0]
        total    = len(rows)
        synced   = sum(1 for r in rows if r["sync_done"])
        disabled = sum(1 for r in rows if not r["active"])

    print(f"\n{'Artist':<42} {'Source':<26} {'Sync':>4}  Last synced")
    print("─" * 88)
    for r in rows:
        done = "✓" if r["sync_done"] else "·"
        last = (r["last_synced"] or "never")[:10]
        flag = " [off]" if not r["active"] else ""
        print(f"{r['name']:<42} {r['source']:<26} {done:>4}  {last}{flag}")

    print(f"\n{total} artists total  "
          f"({synced} synced, {total - synced - disabled} pending, {disabled} disabled) "
          f"| {pl_count} playlists active")


def cmd_scan(args):
    """Scan all active playlists and auto-discover artists."""
    with db_connect() as db:
        playlists = db.execute(
            "SELECT * FROM playlists WHERE active=1"
        ).fetchall()

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

                db.execute("""
                    INSERT INTO playlist_artists VALUES (?,?)
                    ON CONFLICT DO NOTHING
                """, (pl["spotify_id"], sid))

            db.execute("""
                UPDATE playlists SET last_synced=datetime('now')
                WHERE spotify_id=?
            """, (pl["spotify_id"],))

        log.info("  → %d new artists discovered", new_artists)
        grand_total += new_artists

    log.info("Playlist scan complete — %d new artists total", grand_total)


def cmd_artists_sync(args):
    """Download / incrementally sync all (or only new) active artists."""
    new_only = getattr(args, "new_only", False)

    with db_connect() as db:
        query = "SELECT * FROM artists WHERE active=1"
        if new_only:
            query += " AND sync_done=0"
        artists = db.execute(query + " ORDER BY name COLLATE NOCASE").fetchall()

    tag = " (new only)" if new_only else ""
    log.info("Artists to sync%s: %d", tag, len(artists))
    ok = failed = 0

    for i, a in enumerate(artists, 1):
        sid  = a["spotify_id"]
        name = a["name"]
        log.info("[%d/%d] %s", i, len(artists), name)

        # Determine spotDL target
        if sid.startswith("q:"):
            target = name            # spotDL will search by name
        else:
            target = f"https://open.spotify.com/artist/{sid}"

        success = spotdl_sync_artist(sid, name, target)

        with db_connect() as db:
            if success:
                db.execute("""
                    UPDATE artists
                    SET sync_done=1, last_synced=datetime('now')
                    WHERE spotify_id=?
                """, (sid,))
                ok += 1
            else:
                failed += 1

    log.info("Artists sync done — ✓ %d  ✗ %d", ok, failed)


def cmd_disable(args):
    """Disable an artist (won't be synced, stays in DB)."""
    with db_connect() as db:
        cur = db.execute("""
            UPDATE artists SET active=0
            WHERE name LIKE ? OR spotify_id=?
        """, (f"%{args.artist}%", args.artist))
    log.info("Disabled %d artist(s) matching '%s'", cur.rowcount, args.artist)


def cmd_enable(args):
    """Re-enable a disabled artist."""
    with db_connect() as db:
        cur = db.execute("""
            UPDATE artists SET active=1
            WHERE name LIKE ? OR spotify_id=?
        """, (f"%{args.artist}%", args.artist))
    log.info("Enabled %d artist(s) matching '%s'", cur.rowcount, args.artist)


def cmd_full_sync(args):
    """Full pipeline: scan playlists → sync all artist discographies."""
    log.info("━" * 60)
    log.info("  FULL SYNC  —  %s", datetime.now().strftime("%Y-%m-%d %H:%M"))
    log.info("━" * 60)

    log.info("\n▶ Step 1 / 2 — Scanning playlists")
    cmd_scan(argparse.Namespace())

    log.info("\n▶ Step 2 / 2 — Syncing artist discographies")
    cmd_artists_sync(argparse.Namespace(new_only=False))

    log.info("\n━" * 60)
    log.info("  SYNC COMPLETE")
    log.info("━" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # Ensure required directories exist
    for d in (CFG["sync_dir"], CFG["music_dir"], CFG["log_dir"]):
        Path(d).mkdir(parents=True, exist_ok=True)

    db_init()

    p = argparse.ArgumentParser(
        prog="music_sync.py",
        description="Automated Spotify→Jellyfin music library manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 music_sync.py                       Full sync (playlists + artists)
  python3 music_sync.py scan                  Discover artists from playlists
  python3 music_sync.py artists-sync          Sync all artist discographies
  python3 music_sync.py artists-sync --new-only
  python3 music_sync.py add "THE MOTANS"
  python3 music_sync.py add https://open.spotify.com/artist/...
  python3 music_sync.py import /opt/music-sync/my_artists.txt
  python3 music_sync.py list
  python3 music_sync.py disable "Artist Name"
        """
    )

    sub = p.add_subparsers(dest="cmd")
    sub.add_parser("sync",          help="Full pipeline — default command")
    sub.add_parser("scan",          help="Scan playlists, discover artists")

    as_ = sub.add_parser("artists-sync", help="Sync artist discographies")
    as_.add_argument("--new-only", action="store_true",
                     help="Only sync artists not yet downloaded")

    add_ = sub.add_parser("add",    help="Add an artist by URL or name")
    add_.add_argument("artist")

    imp_ = sub.add_parser("import", help="Bulk import artists from a text file")
    imp_.add_argument("file")

    sub.add_parser("list",          help="List all artists in the DB")

    dis_ = sub.add_parser("disable", help="Disable an artist (keeps them in DB)")
    dis_.add_argument("artist")

    en_  = sub.add_parser("enable",  help="Re-enable a disabled artist")
    en_.add_argument("artist")

    args = p.parse_args()

    routes = {
        None:            cmd_full_sync,
        "sync":          cmd_full_sync,
        "scan":          cmd_scan,
        "artists-sync":  cmd_artists_sync,
        "add":           cmd_add,
        "import":        cmd_import,
        "list":          cmd_list,
        "disable":       cmd_disable,
        "enable":        cmd_enable,
    }

    fn = routes.get(args.cmd)
    if fn:
        fn(args)
    else:
        p.print_help()


if __name__ == "__main__":
    main()
