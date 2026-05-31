#!/usr/bin/env python3
"""
hud.py — MusicaDet web dashboard for music_sync.py
"""

import asyncio
import json
import os
import sqlite3
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel

BASE = Path(__file__).resolve().parent
CFG_FILE = BASE / "config.json"
PY_SCRIPT = BASE / "music_sync.py"

DEFAULTS = {
    "music_dir": "/mnt/storage_jellyfin/media/music",
    "sync_dir": str(BASE / "sync-data"),
    "db_path": str(BASE / "music.db"),
    "log_dir": "/var/log/musicadet",
    "format": "opus",
    "bitrate": "320k",
    "threads": 4,
    "output_template": "{artist}/{album}/{track-number} - {title}.{output-ext}",
    "download_format": "original",
    "artist_scanner": "ytmusic",
    "playlist_save_timeout": 600,
    "playlist_save_retries": 3,
    "artist_save_timeout": 900,
    "lyrics_providers": ["genius", "musixmatch", "azlyrics"],
    "generate_lrc": False,
    "hud_port": 8800,
    "playlists": [],
}

CONFIG_KEYS = [
    "music_dir", "sync_dir", "db_path", "log_dir", "format", "download_format", "bitrate", "threads",
    "output_template", "artist_scanner", "playlist_save_timeout", "playlist_save_retries",
    "artist_save_timeout", "lyrics_providers", "generate_lrc", "hud_port", "max_downloads_per_artist",
]

AUDIO_EXTS = {".opus", ".mp3", ".m4a", ".flac", ".ogg", ".wav", ".aac", ".webm"}


def load_cfg() -> dict:
    cfg = DEFAULTS.copy()
    if CFG_FILE.exists():
        try:
            cfg.update(json.loads(CFG_FILE.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            pass
    return cfg


def save_cfg(cfg: dict) -> None:
    CFG_FILE.write_text(json.dumps(cfg, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def public_cfg() -> dict:
    return {k: load_cfg().get(k, DEFAULTS.get(k)) for k in CONFIG_KEYS}


def db() -> sqlite3.Connection:
    cfg = load_cfg()
    conn = sqlite3.connect(cfg["db_path"], timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_db() -> None:
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS artists (
              spotify_id TEXT PRIMARY KEY, name TEXT NOT NULL, source TEXT DEFAULT 'manual',
              active INTEGER DEFAULT 1, sync_done INTEGER DEFAULT 0, last_synced TEXT,
              albums_scanned_at TEXT, added_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS playlists (
              spotify_id TEXT PRIMARY KEY, name TEXT NOT NULL, url TEXT NOT NULL,
              active INTEGER DEFAULT 1, last_synced TEXT
            );
            CREATE TABLE IF NOT EXISTS playlist_artists (
              playlist_id TEXT NOT NULL, artist_id TEXT NOT NULL,
              PRIMARY KEY (playlist_id, artist_id)
            );
            CREATE TABLE IF NOT EXISTS albums (
              spotify_id TEXT PRIMARY KEY, artist_id TEXT NOT NULL, name TEXT NOT NULL,
              release_year TEXT, track_count INTEGER DEFAULT 0, downloaded_count INTEGER DEFAULT 0,
              last_scanned TEXT, UNIQUE(artist_id, name)
            );
            CREATE TABLE IF NOT EXISTS songs (
              spotify_id TEXT PRIMARY KEY, album_id TEXT NOT NULL, artist_id TEXT NOT NULL,
              title TEXT NOT NULL, track_number INTEGER, status TEXT DEFAULT 'pending',
              file_path TEXT, has_cover INTEGER DEFAULT 0, has_lyrics INTEGER DEFAULT 0,
              has_core_tags INTEGER DEFAULT 0, metadata_checked_at TEXT, last_error TEXT,
              updated_at TEXT DEFAULT (datetime('now'))
            );
            """
        )
        cols = {r[1] for r in conn.execute("PRAGMA table_info(artists)")}
        if "albums_scanned_at" not in cols:
            conn.execute("ALTER TABLE artists ADD COLUMN albums_scanned_at TEXT")
        if "max_downloads" not in cols:
            conn.execute("ALTER TABLE artists ADD COLUMN max_downloads INTEGER")
        if "is_romanian" not in cols:
            conn.execute("ALTER TABLE artists ADD COLUMN is_romanian INTEGER DEFAULT 0")
        if "romanian_manual" not in cols:
            conn.execute("ALTER TABLE artists ADD COLUMN romanian_manual INTEGER DEFAULT 0")
        conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_albums_artist_id ON albums(artist_id);
            CREATE INDEX IF NOT EXISTS idx_songs_artist_status ON songs(artist_id, status);
            CREATE INDEX IF NOT EXISTS idx_artists_active_name ON artists(active, name);
            """
        )
        for pl in load_cfg().get("playlists", []):
            pid = _extract_id(pl.get("url", ""), "playlist")
            if pid:
                conn.execute(
                    """INSERT INTO playlists (spotify_id, name, url) VALUES (?,?,?)
                       ON CONFLICT(spotify_id) DO UPDATE SET name=excluded.name, url=excluded.url""",
                    (pid, pl["name"], pl["url"]),
                )


def _extract_id(url: str, kind: str) -> Optional[str]:
    marker = f"/{kind}/"
    if marker in url:
        return url.split(marker)[1].split("?")[0].split("/")[0]
    if url.startswith(f"spotify:{kind}:"):
        return url.split(":")[-1]
    return None


class LogBus:
    def __init__(self, maxlen: int = 800):
        self.buffer: deque = deque(maxlen=maxlen)
        self.subs: set = set()
        self.proc: Optional[asyncio.subprocess.Process] = None
        self.running_label: Optional[str] = None

    def emit(self, line: str) -> None:
        line = line.rstrip("\n")
        self.buffer.append(line)
        for q in list(self.subs):
            try:
                q.put_nowait(line)
            except asyncio.QueueFull:
                pass

    async def run(self, args: list, label: str) -> None:
        if self.proc and self.proc.returncode is None:
            self.emit(f"WARN: busy - '{self.running_label}' is already running.")
            return
        self.running_label = label
        self.emit("")
        self.emit(f"=== > {label} - {datetime.now():%H:%M:%S} ===")
        try:
            self.proc = await asyncio.create_subprocess_exec(
                *args, cwd=str(BASE), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
        except Exception as e:
            self.emit(f"x failed to start: {e}")
            self.running_label = None
            return
        assert self.proc.stdout is not None
        async for raw in self.proc.stdout:
            self.emit(raw.decode("utf-8", errors="replace"))
        code = await self.proc.wait()
        mark = "done" if code == 0 else f"exit {code}"
        self.emit(f"=== {mark}: {label} ===")
        self.running_label = None


bus = LogBus()
app = FastAPI(title="MusicaDet HUD")


@app.on_event("startup")
async def _startup() -> None:
    try:
        Path(load_cfg()["db_path"]).parent.mkdir(parents=True, exist_ok=True)
        ensure_db()
    except Exception as exc:
        import logging
        logging.basicConfig(level=logging.ERROR)
        logging.error("HUD startup failed (database init): %s", exc)
        raise


class ArtistIn(BaseModel):
    entry: str


class PlaylistIn(BaseModel):
    name: str
    url: str


class ConfigIn(BaseModel):
    music_dir: Optional[str] = None
    sync_dir: Optional[str] = None
    format: Optional[str] = None
    download_format: Optional[str] = None
    bitrate: Optional[str] = None
    threads: Optional[int] = None
    output_template: Optional[str] = None
    playlist_save_timeout: Optional[int] = None
    playlist_save_retries: Optional[int] = None
    artist_save_timeout: Optional[int] = None
    lyrics_providers: Optional[list[str]] = None
    generate_lrc: Optional[bool] = None
    hud_port: Optional[int] = None
    max_downloads_per_artist: Optional[int] = None


@app.get("/api/config")
def api_get_config():
    return public_cfg()


@app.put("/api/config")
def api_put_config(body: ConfigIn):
    cfg = load_cfg()
    data = body.model_dump(exclude_none=True)
    if "music_dir" in data and not Path(data["music_dir"]).is_absolute():
        return JSONResponse({"error": "music_dir must be absolute"}, status_code=400)
    for k, v in data.items():
        if k in CONFIG_KEYS:
            cfg[k] = v
    if cfg.get("music_dir"):
        Path(cfg["music_dir"]).mkdir(parents=True, exist_ok=True)
    save_cfg(cfg)
    return {"ok": True, "config": public_cfg(), "restart_hud": "hud_port" in data}


@app.get("/api/stats")
def api_stats():
    ensure_db()
    with db() as conn:
        a_total = conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0]
        a_active = conn.execute("SELECT COUNT(*) FROM artists WHERE active=1").fetchone()[0]
        a_synced = conn.execute("SELECT COUNT(*) FROM artists WHERE sync_done=1").fetchone()[0]
        p_total = conn.execute("SELECT COUNT(*) FROM playlists").fetchone()[0]
        p_active = conn.execute("SELECT COUNT(*) FROM playlists WHERE active=1").fetchone()[0]
        albums_total = conn.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
        albums_downloaded = conn.execute("SELECT COUNT(*) FROM albums WHERE downloaded_count >= track_count AND track_count > 0").fetchone()[0]
        songs_downloaded = conn.execute("SELECT COUNT(*) FROM songs WHERE status='downloaded'").fetchone()[0]
        
        # Calculate capped songs pending based on per-artist and global limits
        art_rows = conn.execute("SELECT max_downloads, (SELECT COUNT(*) FROM songs WHERE artist_id=a.spotify_id AND status='downloaded') as dl, (SELECT COUNT(*) FROM songs WHERE artist_id=a.spotify_id AND status!='downloaded') as pd FROM artists a WHERE a.active=1").fetchall()
        global_max = int(load_cfg().get("max_downloads_per_artist", 0))
        songs_pending = 0
        for r in art_rows:
            max_dl = r["max_downloads"] if r["max_downloads"] is not None else global_max
            dl, pd = r["dl"], r["pd"]
            if max_dl > 0:
                allowed = max(0, max_dl - dl)
                songs_pending += min(pd, allowed)
            else:
                songs_pending += pd
        songs_failed = conn.execute("SELECT COUNT(*) FROM songs WHERE status='failed'").fetchone()[0]
        songs_missing_cover = conn.execute(
            "SELECT COUNT(*) FROM songs WHERE status='downloaded' AND has_cover=0"
        ).fetchone()[0]
        songs_missing_lyrics = conn.execute(
            "SELECT COUNT(*) FROM songs WHERE status='downloaded' AND has_lyrics=0"
        ).fetchone()[0]
    return {
        "artists_total": a_total, "artists_active": a_active, "artists_synced": a_synced,
        "artists_pending": a_active - a_synced,
        "playlists_total": p_total, "playlists_active": p_active,
        "albums_total": albums_total, "albums_downloaded": albums_downloaded,
        "songs_downloaded": songs_downloaded, "songs_pending": songs_pending,
        "songs_failed": songs_failed,
        "songs_missing_cover": songs_missing_cover,
        "songs_missing_lyrics": songs_missing_lyrics,
        "tracks": count_tracks(), "running": bus.running_label,
        "music_dir": load_cfg().get("music_dir"),
    }


_track_count_cache = {"n": 0, "ts": 0}


def count_tracks() -> int:
    now = time.time()
    if now - _track_count_cache["ts"] < 60:
        return _track_count_cache["n"]
    music_dir = Path(load_cfg()["music_dir"])
    if not music_dir.exists():
        return 0
    n = 0
    for _root, _dirs, files in os.walk(music_dir):
        for f in files:
            if Path(f).suffix.lower() in AUDIO_EXTS:
                n += 1
    _track_count_cache["n"] = n
    _track_count_cache["ts"] = now
    return n


def _artist_filters(q: str, status: str) -> tuple[list[str], list]:
    where, params = ["a.active >= 0"], []
    if q:
        where.append("a.name LIKE ?")
        params.append(f"%{q}%")
    if status == "active":
        where.append("a.active=1")
    elif status == "disabled":
        where.append("a.active=0")
    elif status == "pending":
        where.append("a.active=1 AND a.sync_done=0")
    elif status == "synced":
        where.append("a.sync_done=1")
    if q2 := {"romanian": "a.is_romanian=1", "international": "a.is_romanian=0"}.get(status):
        where.append(q2)
    return where, params


def _cap_artist_song_counts(rows: list[dict]) -> None:
    global_max = int(load_cfg().get("max_downloads_per_artist", 0))
    for r in rows:
        max_dl = r["max_downloads"] if r["max_downloads"] is not None else global_max
        if max_dl > 0:
            pd = r["songs_total"] - r["songs_dl"]
            allowed_pending = max(0, max_dl - r["songs_dl"])
            capped_pending = min(pd, allowed_pending)
            r["songs_total"] = r["songs_dl"] + capped_pending


@app.get("/api/artists/names")
def api_artist_names():
    with db() as conn:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT spotify_id, name FROM artists WHERE active=1 ORDER BY name COLLATE NOCASE"
            ).fetchall()
        ]


@app.get("/api/artists")
def api_artists(
    q: str = "",
    status: str = "all",
    offset: int = Query(0, ge=0),
    limit: int = Query(80, ge=1, le=500),
):
    where, params = _artist_filters(q, status)
    clause = " WHERE " + " AND ".join(where) if where else ""
    base_from = f"""
        FROM artists a
        LEFT JOIN (
            SELECT artist_id, COUNT(*) AS album_count FROM albums GROUP BY artist_id
        ) ac ON ac.artist_id = a.spotify_id
        LEFT JOIN (
            SELECT artist_id,
                   SUM(CASE WHEN status='downloaded' THEN 1 ELSE 0 END) AS songs_dl,
                   COUNT(*) AS songs_total
            FROM songs GROUP BY artist_id
        ) sc ON sc.artist_id = a.spotify_id
        {clause}
    """
    sql = f"""
        SELECT a.spotify_id, a.name, a.source, a.active, a.sync_done, a.last_synced, a.added_at,
               a.albums_scanned_at, a.max_downloads, a.is_romanian,
               COALESCE(ac.album_count, 0) AS album_count,
               COALESCE(sc.songs_dl, 0) AS songs_dl,
               COALESCE(sc.songs_total, 0) AS songs_total
        {base_from}
        ORDER BY a.name COLLATE NOCASE
        LIMIT ? OFFSET ?
    """
    count_sql = f"SELECT COUNT(*) {base_from}"
    with db() as conn:
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = [dict(r) for r in conn.execute(sql, params + [limit, offset]).fetchall()]
    _cap_artist_song_counts(rows)
    return {"items": rows, "total": total}


@app.get("/api/albums")
def api_albums(
    artist_id: str = "",
    status: str = "all",
    offset: int = Query(0, ge=0),
    limit: int = Query(80, ge=1, le=500),
):
    sql_base = """
        FROM albums al JOIN artists ar ON ar.spotify_id = al.artist_id
    """
    where, params = [], []
    if artist_id:
        where.append("al.artist_id=?")
        params.append(artist_id)
    if status == "complete":
        where.append("al.downloaded_count >= al.track_count AND al.track_count > 0")
    elif status == "pending":
        where.append("al.downloaded_count < al.track_count")
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    sel = f"""
        SELECT al.spotify_id, al.artist_id, al.name, al.release_year,
               al.track_count, al.downloaded_count, al.last_scanned, ar.name AS artist_name
        {sql_base}{clause}
        ORDER BY ar.name, al.name
        LIMIT ? OFFSET ?
    """
    count_sql = f"SELECT COUNT(*) {sql_base}{clause}"
    with db() as conn:
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = [dict(r) for r in conn.execute(sel, params + [limit, offset]).fetchall()]
    return {"items": rows, "total": total}


@app.get("/api/songs")
def api_songs(album_id: str = "", artist_id: str = "", status: str = "all", limit: int = 500):
    sql = """
        SELECT s.spotify_id, s.album_id, s.artist_id, s.title, s.track_number,
               s.status, s.file_path, s.has_cover, s.has_lyrics, s.has_core_tags,
               al.name AS album_name, ar.name AS artist_name
        FROM songs s
        JOIN albums al ON al.spotify_id = s.album_id
        JOIN artists ar ON ar.spotify_id = s.artist_id
    """
    where, params = [], []
    if album_id:
        where.append("s.album_id=?")
        params.append(album_id)
    if artist_id:
        where.append("s.artist_id=?")
        params.append(artist_id)
    if status in ("pending", "downloaded", "failed"):
        where.append("s.status=?")
        params.append(status)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ar.name, al.name, s.track_number LIMIT ?"
    params.append(limit)
    with db() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


@app.post("/api/artists")
async def api_add_artist(body: ArtistIn):
    entry = body.entry.strip()
    if not entry:
        return JSONResponse({"error": "empty"}, status_code=400)
    asyncio.create_task(bus.run([sys.executable, str(PY_SCRIPT), "add", entry], f"add artist: {entry}"))
    return {"ok": True}


@app.post("/api/artists/{spotify_id}/toggle")
def api_toggle_artist(spotify_id: str):
    with db() as conn:
        row = conn.execute("SELECT active FROM artists WHERE spotify_id=?", (spotify_id,)).fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        new = 0 if row["active"] else 1
        conn.execute("UPDATE artists SET active=? WHERE spotify_id=?", (new, spotify_id))
    return {"ok": True, "active": new}


@app.post("/api/artists/{spotify_id}/limit")
async def api_set_artist_limit(spotify_id: str, request: Request):
    data = await request.json()
    limit = data.get("limit")
    with db() as conn:
        if limit is None or limit == "":
            conn.execute("UPDATE artists SET max_downloads = NULL WHERE spotify_id=?", (spotify_id,))
        else:
            conn.execute("UPDATE artists SET max_downloads = ? WHERE spotify_id=?", (int(limit), spotify_id))
    return {"ok": True}


@app.post("/api/artists/{spotify_id}/ro")
async def api_set_romanian(spotify_id: str, request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    with db() as conn:
        row = conn.execute(
            "SELECT is_romanian FROM artists WHERE spotify_id=?", (spotify_id,)
        ).fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        if "is_romanian" in body:
            new = 1 if body["is_romanian"] else 0
        else:
            new = 0 if row["is_romanian"] else 1
        conn.execute(
            "UPDATE artists SET is_romanian=?, romanian_manual=1 WHERE spotify_id=?",
            (new, spotify_id),
        )
    return {"ok": True, "is_romanian": new}


@app.delete("/api/artists/{spotify_id}")
def api_delete_artist(spotify_id: str, delete_files: bool = False):
    import shutil
    import re
    from pathlib import Path
    
    with db() as conn:
        row = conn.execute("SELECT name FROM artists WHERE spotify_id=?", (spotify_id,)).fetchone()
        if row:
            artist_name = row["name"]
            # Soft-delete artist from database so they are ignored in future playlist syncs
            conn.execute("UPDATE artists SET active=-1 WHERE spotify_id=?", (spotify_id,))
            # Delete their albums and songs to clean up the DB
            conn.execute("DELETE FROM albums WHERE artist_id=?", (spotify_id,))
            conn.execute("DELETE FROM songs WHERE artist_id=?", (spotify_id,))
            
            if delete_files:
                # Physically delete the artist folder from storage
                cfg = load_cfg()
                music_dir = Path(cfg["music_dir"])
                
                # Sanitize name helper matching music_sync.py
                def clean_name(name: str) -> str:
                    return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip().strip(".")
                    
                artist_folder = music_dir / clean_name(artist_name)
                if artist_folder.exists() and artist_folder.is_dir():
                    try:
                        shutil.rmtree(str(artist_folder))
                    except Exception:
                        pass
    return {"ok": True}


@app.get("/api/playlists")
def api_playlists():
    with db() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT spotify_id, name, url, active, last_synced FROM playlists ORDER BY name COLLATE NOCASE"
        ).fetchall()]
    return rows


@app.post("/api/playlists")
def api_add_playlist(body: PlaylistIn):
    pid = _extract_id(body.url.strip(), "playlist")
    if not pid:
        return JSONResponse({"error": "invalid playlist url"}, status_code=400)
    name = body.name.strip() or pid
    with db() as conn:
        conn.execute(
            """INSERT INTO playlists (spotify_id, name, url) VALUES (?,?,?)
               ON CONFLICT(spotify_id) DO UPDATE SET name=excluded.name, url=excluded.url, active=1""",
            (pid, name, body.url.strip()),
        )
    cfg = load_cfg()
    pls = [p for p in cfg.get("playlists", []) if _extract_id(p.get("url", ""), "playlist") != pid]
    pls.append({"name": name, "url": body.url.strip()})
    cfg["playlists"] = pls
    save_cfg(cfg)
    return {"ok": True, "id": pid}


@app.post("/api/playlists/{spotify_id}/toggle")
def api_toggle_playlist(spotify_id: str):
    with db() as conn:
        row = conn.execute("SELECT active FROM playlists WHERE spotify_id=?", (spotify_id,)).fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        new = 0 if row["active"] else 1
        conn.execute("UPDATE playlists SET active=? WHERE spotify_id=?", (new, spotify_id))
    return {"ok": True, "active": new}


@app.delete("/api/playlists/{spotify_id}")
def api_delete_playlist(spotify_id: str):
    with db() as conn:
        conn.execute("DELETE FROM playlists WHERE spotify_id=?", (spotify_id,))
    cfg = load_cfg()
    cfg["playlists"] = [p for p in cfg.get("playlists", []) if _extract_id(p.get("url", ""), "playlist") != spotify_id]
    save_cfg(cfg)
    return {"ok": True}


_tracks_index_cache: dict = {"items": [], "ts": 0.0}


def _tracks_index() -> list[dict]:
    now = time.time()
    if now - _tracks_index_cache["ts"] < 60 and _tracks_index_cache["items"]:
        return _tracks_index_cache["items"]
    music_dir = Path(load_cfg()["music_dir"])
    out: list[dict] = []
    if music_dir.exists():
        for root, _dirs, files in os.walk(music_dir):
            for f in files:
                if Path(f).suffix.lower() not in AUDIO_EXTS:
                    continue
                rel = os.path.relpath(os.path.join(root, f), music_dir)
                parts = rel.split(os.sep)
                out.append({
                    "path": rel,
                    "artist": parts[0] if len(parts) > 1 else "",
                    "album": parts[1] if len(parts) > 2 else "",
                    "title": parts[-1],
                })
    out.sort(key=lambda t: t["path"].lower())
    _tracks_index_cache["items"] = out
    _tracks_index_cache["ts"] = now
    return out


@app.get("/api/tracks")
def api_tracks(
    q: str = "",
    offset: int = Query(0, ge=0),
    limit: int = Query(80, ge=1, le=500),
):
    needle = q.lower().strip()
    items = _tracks_index()
    if needle:
        items = [t for t in items if needle in t["path"].lower()]
    total = len(items)
    page = items[offset : offset + limit]
    return {"items": page, "total": total, "index_age": int(time.time() - _tracks_index_cache["ts"])}


@app.get("/api/track/info")
def api_track_info(path: str):
    music_dir = Path(load_cfg()["music_dir"])
    full_path = music_dir / path
    if not full_path.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    
    info = {
        "title": full_path.stem,
        "artist": "", "album": "", "genre": "", "year": "", "has_cover": False,
        "bitrate": 0, "length": 0.0
    }
    
    try:
        import mutagen
        audio = mutagen.File(full_path)
        if audio:
            if hasattr(audio, "info") and audio.info:
                info["bitrate"] = getattr(audio.info, "bitrate", 0)
                info["length"] = getattr(audio.info, "length", 0)
            
            # Calculate bitrate manually if Mutagen failed to provide it
            if not info["bitrate"] and info["length"] > 0:
                import os
                size_bytes = os.path.getsize(full_path)
                info["bitrate"] = int((size_bytes * 8) / info["length"])
                
            if audio.tags:
                tags = audio.tags
                if full_path.suffix.lower() == ".opus":
                    info["title"] = tags.get("title", [info["title"]])[0]
                    info["artist"] = tags.get("artist", [""])[0]
                    info["album"] = tags.get("album", [""])[0]
                    year_str = tags.get("date", [""])[0]
                    info["year"] = year_str[:4] if year_str else ""
                    info["genre"] = tags.get("genre", [""])[0]
                    info["has_cover"] = "metadata_block_picture" in tags
                elif full_path.suffix.lower() == ".mp3":
                    info["title"] = str(tags.get("TIT2", info["title"]))
                    info["artist"] = str(tags.get("TPE1", ""))
                    info["album"] = str(tags.get("TALB", ""))
                    year_str = str(tags.get("TDRC", ""))
                    info["year"] = year_str[:4] if year_str else ""
                    info["genre"] = str(tags.get("TCON", ""))
                    info["has_cover"] = any(k.startswith("APIC") for k in tags)
    except Exception:
        pass

    return info


@app.get("/api/track/cover")
def api_track_cover(path: str):
    music_dir = Path(load_cfg()["music_dir"])
    full_path = music_dir / path
    if not full_path.exists():
        return Response(status_code=404)
    
    try:
        import mutagen
        audio = mutagen.File(full_path)
        if audio and audio.tags:
            if full_path.suffix.lower() == ".opus" and "metadata_block_picture" in audio.tags:
                import base64
                from mutagen.flac import Picture
                b64_data = audio.tags["metadata_block_picture"][0]
                pic = Picture(base64.b64decode(b64_data))
                return Response(content=pic.data, media_type=pic.mime)
            elif full_path.suffix.lower() == ".mp3":
                for k in audio.tags:
                    if k.startswith("APIC"):
                        apic = audio.tags[k]
                        return Response(content=apic.data, media_type=apic.mime)
    except Exception:
        pass
    
    return Response(status_code=404)


@app.get("/api/track/download")
def api_track_download(path: str):
    music_dir = Path(load_cfg()["music_dir"])
    full_path = music_dir / path
    if not full_path.exists():
        return Response(status_code=404)
        
    dl_format = load_cfg().get("download_format", "original")
    ext = full_path.suffix.lower()
    
    if dl_format == "mp3" and ext == ".opus":
        import tempfile, subprocess, os
        from starlette.background import BackgroundTask
        from fastapi.responses import FileResponse
        
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".mp3")
        os.close(tmp_fd)
        
        # Transcode on the fly
        res = subprocess.run(["ffmpeg", "-y", "-i", str(full_path), "-b:a", "320k", tmp_path], 
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Look up info in DB to enforce perfect metadata
        title = full_path.stem
        artist, album, track_num = "", "", None
        rel_path = os.path.relpath(full_path, music_dir)
        with db() as conn:
            song = conn.execute("SELECT * FROM songs WHERE file_path=?", (rel_path,)).fetchone()
            if song:
                title, track_num = song["title"], song["track_number"]
                ar = conn.execute("SELECT name FROM artists WHERE spotify_id=?", (song["artist_id"],)).fetchone()
                if ar: artist = ar["name"]
                al = conn.execute("SELECT name FROM albums WHERE spotify_id=?", (song["album_id"],)).fetchone()
                if al: album = al["name"]
        
        import custom_dl
        if os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
            custom_dl.enforce_primary_artist(Path(tmp_path), artist, title, album, track_num, fetch_cover=True)
        else:
            # If transcode failed completely, just send original
            os.unlink(tmp_path)
            return FileResponse(full_path, filename=full_path.name)
        
        def cleanup():
            try: os.unlink(tmp_path)
            except: pass
            
        return FileResponse(
            path=tmp_path,
            filename=full_path.with_suffix(".mp3").name,
            media_type="audio/mpeg",
            background=BackgroundTask(cleanup)
        )
    
    import mimetypes
    mime = mimetypes.guess_type(str(full_path))[0] or "application/octet-stream"
    filename = full_path.name
    
    data = full_path.read_bytes()
    return Response(
        content=data,
        media_type=mime,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(data)),
        }
    )


ACTIONS = {
    "scan": (["scan"], "Scan playlists"),
    "scan-artists": (["scan-artists"], "Scan artist albums"),
    "scan-artists-new": (["scan-artists", "--new-only"], "Scan new artists"),
    "artists-sync": (["artists-sync"], "Sync all artists"),
    "artists-sync-new": (["artists-sync", "--new-only"], "Sync new artists"),
    "download-pending": (["download-pending"], "Download pending tracks"),
    "reconcile": (["reconcile"], "Reconcile files"),
    "migrate-structure": (["migrate-structure"], "Migrate library structure"),
    "fix-metadata": (["fix-metadata"], "Fix metadata"),
    "deduplicate": (["deduplicate"], "Deduplicate"),
    "mark-romanian": (["mark-romanian"], "Mark Romanian Artists"),
    "full": ([], "Full sync"),
}


@app.post("/api/actions/{action}")
async def api_action(action: str, request: Request):
    if action == "sync-playlist":
        try:
            body = await request.json()
            url = body.get("url", "")
            if not url:
                return JSONResponse({"error": "missing url"}, status_code=400)
            label = "Sync playlist"
            asyncio.create_task(bus.run([sys.executable, str(PY_SCRIPT), "sync-playlist", url], label))
            return {"ok": True, "label": label}
        except Exception:
            return JSONResponse({"error": "invalid payload"}, status_code=400)

    if action == "download":
        try:
            body = await request.json()
            url = body.get("url", "")
            if not url:
                return JSONResponse({"error": "missing url"}, status_code=400)
            label = "Direct download"
            asyncio.create_task(bus.run([sys.executable, str(PY_SCRIPT), "download", url], label))
            return {"ok": True, "label": label}
        except Exception:
            return JSONResponse({"error": "invalid payload"}, status_code=400)

    if action not in ACTIONS:
        return JSONResponse({"error": "unknown action"}, status_code=404)
    sub, label = ACTIONS[action]
    asyncio.create_task(bus.run([sys.executable, str(PY_SCRIPT), *sub], label))
    return {"ok": True, "label": label}


@app.post("/api/stop")
async def api_stop():
    if bus.proc and bus.proc.returncode is None:
        bus.proc.terminate()
        bus.emit("stop requested")
        return {"ok": True}
    return {"ok": False, "error": "nothing running"}


@app.websocket("/ws/logs")
async def ws_logs(ws: WebSocket):
    await ws.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=2000)
    bus.subs.add(q)
    try:
        for line in list(bus.buffer):
            await ws.send_text(line)
        while True:
            line = await q.get()
            await ws.send_text(line)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        bus.subs.discard(q)


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML


HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Musicadet</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22 font-family=%22serif%22 font-style=%22italic%22 font-weight=%22bold%22>M</text></svg>">
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Dancing+Script:wght@700&display=swap" rel="stylesheet"/>
<style>
  :root {
    --bg: #000000;
    --bg-card: rgba(255, 255, 255, 0.02);
    --border-card: rgba(255, 255, 255, 0.08);
    --primary: #ffffff;
    --primary-grad: #ffffff;
    --accent: #a1a1aa;
    --success: #4ade80;
    --warning: #facc15;
    --error: #f87171;
    --txt: #fafafa;
    --muted: #a1a1aa;
  }
  * { box-sizing: border-box; }
  html { color-scheme: dark; }
  body {
    margin: 0;
    font: 14px/1.6 'Inter', system-ui, -apple-system, sans-serif;
    color: var(--txt);
    background: radial-gradient(1000px 600px at 50% -10%, rgba(255, 255, 255, 0.03), transparent 70%), var(--bg);
    min-height: 100vh;
  }
  a {
    color: var(--txt);
    text-decoration: none;
    transition: color 0.2s ease;
  }
  a:hover {
    color: #ffffff;
    text-decoration: underline;
  }
  header {
    position: relative;
    position: sticky;
    top: 0;
    z-index: 20;
    backdrop-filter: blur(20px);
    background: rgba(0, 0, 0, 0.85);
    border-bottom: 1px solid var(--border-card);
    padding: 16px 24px;
    display: flex;
    align-items: center;
    gap: 16px;
    flex-wrap: nowrap;
    overflow-x: auto;
    scrollbar-width: none;
  }
  header::-webkit-scrollbar {
    display: none;
  }
  .header-gradient {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 1px;
    background: var(--border-card);
  }
  .logo {
    font-weight: 700;
    font-size: 32px;
    letter-spacing: -.04em;
    color: #ffffff;
    padding-right: 8px;
    font-family: 'Dancing Script', cursive;
    background: linear-gradient(135deg, #fff 0%, #a1a1aa 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }
  .dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--success);
    box-shadow: 0 0 8px var(--success);
  }
  .dot.busy {
    background: var(--warning);
    box-shadow: 0 0 8px var(--warning);
    animation: pulse 1s infinite alternate;
  }
  @keyframes pulse {
    0% { opacity: 0.3; transform: scale(0.9); }
    100% { opacity: 1; transform: scale(1.1); }
  }
  .status {
    display: flex;
    align-items: center;
    gap: 8px;
    color: var(--muted);
    font-size: 12px;
    background: rgba(255, 255, 255, 0.04);
    padding: 4px 10px;
    border-radius: 99px;
    border: 1px solid var(--border-card);
  }
  nav {
    display: flex;
    gap: 6px;
    margin-left: auto;
    flex-wrap: nowrap;
    white-space: nowrap;
  }
  nav button {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    padding: 8px 14px;
    border-radius: 10px;
    cursor: pointer;
    font: 500 13px 'Inter', sans-serif;
    transition: all 0.2s ease;
  }
  nav button.active {
    color: var(--txt);
    background: rgba(255, 255, 255, 0.06);
    border-color: var(--border-card);
  }
  nav button:hover {
    color: var(--txt);
    background: rgba(255, 255, 255, 0.03);
  }
  main {
    max-width: 1200px;
    margin: 0 auto;
    padding: 24px;
  }
  .grid {
    display: grid;
    gap: 16px;
  }
  .stats {
    grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
  }
  .card {
    background: var(--bg-card);
    border: 1px solid var(--border-card);
    border-radius: 14px;
    padding: 20px;
    transition: transform 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease;
  }
  .card:hover {
    border-color: rgba(255, 255, 255, 0.2);
  }
  .stat {
    border-left: 3px solid var(--border-card);
  }
  .stat:hover {
    border-left-color: var(--primary);
    transform: translateY(-2px);
    box-shadow: 0 4px 20px rgba(255, 255, 255, 0.02);
  }
  .stat .n {
    font-size: 28px;
    font-weight: 700;
    font-variant-numeric: tabular-nums;
    color: var(--txt);
  }
  .stat .l {
    color: var(--muted);
    font-size: 11px;
    margin-top: 4px;
    text-transform: uppercase;
    letter-spacing: .06em;
  }
  .btn {
    border: 1px solid transparent;
    border-radius: 10px;
    padding: 10px 18px;
    font: 600 13px 'Inter', sans-serif;
    cursor: pointer;
    color: #000000;
    background: var(--primary-grad);
    display: inline-flex;
    align-items: center;
    gap: 8px;
    transition: all 0.2s ease;
  }
  .btn:hover {
    transform: scale(1.02);
    background: #e4e4e7;
    box-shadow: 0 4px 12px rgba(255, 255, 255, 0.15);
  }
  .btn.ghost {
    background: transparent;
    color: var(--txt);
    border: 1px solid var(--border-card);
  }
  .btn.ghost:hover {
    border-color: var(--primary);
    background: rgba(255, 255, 255, 0.06);
  }
  .btn.sm {
    padding: 6px 12px;
    font-size: 12px;
    border-radius: 8px;
  }
  .btn-ro {
    border: 1px solid #0057b7;
    background: rgba(0, 87, 183, 0.12);
    color: #9ec5ff;
    border-radius: 8px;
    padding: 6px 10px;
    font: 700 11px 'Inter', sans-serif;
    letter-spacing: 0.06em;
    cursor: pointer;
    transition: all 0.2s ease;
  }
  .btn-ro:hover {
    background: rgba(0, 87, 183, 0.28);
    color: #fff;
    border-color: #3b82f6;
  }
  .filter-chips {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-items: center;
  }
  .filter-chips .chip {
    border: 1px solid var(--border-card);
    background: rgba(255, 255, 255, 0.03);
    color: var(--muted);
    padding: 6px 12px;
    border-radius: 8px;
    font: 500 12px 'Inter', sans-serif;
    cursor: pointer;
    transition: all 0.15s ease;
  }
  .filter-chips .chip:hover {
    color: var(--txt);
    border-color: rgba(255, 255, 255, 0.2);
  }
  .filter-chips .chip.active {
    color: var(--txt);
    background: rgba(255, 255, 255, 0.08);
    border-color: rgba(255, 255, 255, 0.25);
  }
  .filter-chips .chip.chip-ro.active {
    border-color: #0057b7;
    background: rgba(0, 87, 183, 0.2);
    color: #9ec5ff;
  }
  .ro-toggle {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 26px;
    height: 20px;
    margin-right: 8px;
    padding: 0 5px;
    border-radius: 4px;
    border: 1px solid var(--border-card);
    background: transparent;
    color: var(--muted);
    font: 800 9px 'Inter', sans-serif;
    letter-spacing: 0.04em;
    cursor: pointer;
    vertical-align: middle;
    transition: all 0.15s ease;
  }
  .ro-toggle:hover {
    border-color: #0057b7;
    color: #9ec5ff;
  }
  .ro-toggle.on {
    background: #0057b7;
    border-color: #0057b7;
    color: #fff;
  }
  tr.row-ro td:first-child {
    border-left: 2px solid #0057b7;
  }
  .btn.danger {
    background: transparent;
    color: var(--error);
    border: 1px solid rgba(248, 113, 113, 0.4);
  }
  .btn.danger:hover {
    background: rgba(248, 113, 113, 0.15);
    border-color: var(--error);
    box-shadow: 0 4px 12px rgba(248, 113, 113, 0.2);
  }
  .row {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    align-items: center;
  }
  input, select, textarea {
    background: rgba(255, 255, 255, 0.03);
    border: 1px solid var(--border-card);
    color: var(--txt);
    padding: 10px 14px;
    border-radius: 10px;
    font: 13px 'Inter', sans-serif;
    outline: none;
    transition: all 0.2s ease;
  }
  input:focus, select:focus, textarea:focus {
    border-color: var(--primary);
    background: rgba(255, 255, 255, 0.06);
    box-shadow: 0 0 0 3px rgba(255, 255, 255, 0.1);
  }
  select {
    color-scheme: dark;
    background-color: #18181b;
    cursor: pointer;
  }
  select option,
  select optgroup {
    background-color: #18181b;
    color: #fafafa;
  }
  input {
    width: 100%;
    box-sizing: border-box;
    min-width: 160px;
  }
  label {
    font-size: 12px;
    color: var(--muted);
    display: block;
    margin-bottom: 6px;
    font-weight: 500;
  }
  .field {
    margin-bottom: 16px;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }
  th, td {
    text-align: left;
    padding: 12px 14px;
    border-bottom: 1px solid var(--border-card);
  }
  
  /* Toggle Switch */
  .switch {
    position: relative;
    display: inline-block;
    width: 48px;
    height: 26px;
    flex-shrink: 0;
  }
  .switch input { opacity: 0; width: 0; height: 0; }
  .slider {
    position: absolute;
    cursor: pointer;
    top: 0; left: 0; right: 0; bottom: 0;
    background-color: #27272a;
    transition: .4s cubic-bezier(0.4, 0.0, 0.2, 1);
    border-radius: 26px;
    box-shadow: inset 0 2px 4px rgba(0,0,0,0.5);
    border: 1px solid rgba(255,255,255,0.05);
  }
  .slider:before {
    position: absolute;
    content: "";
    height: 20px; width: 20px;
    left: 2px; bottom: 2px;
    background: linear-gradient(180deg, #ffffff 0%, #e4e4e7 100%);
    transition: .4s cubic-bezier(0.4, 0.0, 0.2, 1);
    border-radius: 50%;
    box-shadow: 0 2px 5px rgba(0,0,0,0.4), inset 0 -1px 1px rgba(0,0,0,0.1);
  }
  .switch:hover .slider {
    background-color: #3f3f46;
  }
  input:checked + .slider {
    background-color: var(--success);
    border-color: var(--success);
    box-shadow: inset 0 2px 4px rgba(0,0,0,0.2), 0 0 12px rgba(74, 222, 128, 0.3);
  }
  input:checked + .slider:before { 
    transform: translateX(22px);
    background: linear-gradient(180deg, #ffffff 0%, #f4f4f5 100%);
  }
  input:focus + .slider {
    box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.2);
  }
  
  th {
    color: var(--muted);
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .06em;
    font-weight: 600;
    border-bottom: none;
  }
  tr:hover td {
    background: rgba(255, 255, 255, 0.02);
  }
  .pill {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 99px;
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .02em;
  }
  .pill.on {
    background: rgba(52, 211, 153, 0.1);
    color: var(--success);
    box-shadow: 0 0 10px rgba(52, 211, 153, 0.05);
  }
  .pill.off {
    background: rgba(248, 113, 113, 0.1);
    color: var(--error);
    box-shadow: 0 0 10px rgba(248, 113, 113, 0.05);
  }
  .pill.done {
    background: rgba(255, 255, 255, 0.08);
    color: var(--txt);
    box-shadow: none;
  }
  .pill.pend {
    background: rgba(251, 191, 36, 0.1);
    color: var(--warning);
  }
  .meta-ok { color: var(--success); }
  .meta-no { color: var(--muted); opacity: 0.5; }
  h2 {
    margin: 0 0 16px;
    font-size: 18px;
    font-weight: 700;
    letter-spacing: -.01em;
  }
  .muted { color: var(--muted); }
  .console {
    background: #050508;
    border: 1px solid var(--border-card);
    border-radius: 12px;
    height: 60vh;
    overflow: auto;
    padding: 16px;
    font: 13px/1.6 ui-monospace, Consolas, monospace;
    white-space: pre-wrap;
    word-break: break-word;
  }
  .hide { display: none !important; }
  .skeleton-row td { padding: 10px 14px; }
  .skel {
    display: block;
    height: 14px;
    width: 72%;
    border-radius: 4px;
    background: linear-gradient(
      90deg,
      rgba(255, 255, 255, 0.04) 0%,
      rgba(255, 255, 255, 0.1) 50%,
      rgba(255, 255, 255, 0.04) 100%
    );
    background-size: 200% 100%;
    animation: skel-shimmer 1.1s ease-in-out infinite;
  }
  @keyframes skel-shimmer {
    0% { background-position: 100% 0; }
    100% { background-position: -100% 0; }
  }
  tr.row-flash td {
    animation: row-flash 0.45s ease;
  }
  @keyframes row-flash {
    0% { background: rgba(255, 255, 255, 0.12); }
    100% { background: transparent; }
  }
  .loading-hint {
    font-size: 11px;
    color: var(--muted);
    margin-left: 8px;
  }
  .hint {
    color: var(--muted);
    font-size: 11px;
    margin-top: 8px;
  }
  .actions {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 10px;
  }
  .toast {
    position: fixed;
    bottom: 20px;
    right: 20px;
    background: var(--txt);
    color: #000000;
    padding: 12px 20px;
    border-radius: 10px;
    font-weight: 600;
    font-size: 13px;
    z-index: 50;
    opacity: 0;
    transform: translateY(10px);
    transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
    box-shadow: 0 8px 30px rgba(0, 0, 0, 0.5);
  }
  .toast.show { opacity: 1; transform: none; }
  .progress {
    font-variant-numeric: tabular-nums;
    font-size: 12px;
    color: var(--muted);
  }
  .gradient-sep {
    height: 1px;
    background: var(--border-card);
    margin: 20px 0;
  }
  section:not(.hide) {
    animation: fadeIn 0.25s ease forwards;
  }
  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(6px); }
    to { opacity: 1; transform: translateY(0); }
  }
  
  .modal {
    position: fixed; top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(0,0,0,0.8); z-index: 100;
    display: flex; align-items: center; justify-content: center;
    backdrop-filter: blur(5px);
  }
  .modal-content {
    background: var(--bg-card);
    border: 1px solid var(--border-card);
    border-radius: 16px;
    padding: 24px; width: 90%; max-width: 500px;
    position: relative;
    box-shadow: 0 10px 40px rgba(0,0,0,0.5);
  }
  .modal-close {
    position: absolute; top: 16px; right: 16px;
    background: transparent; border: none; color: var(--muted);
    font-size: 24px; cursor: pointer;
  }
  .modal-close:hover { color: var(--txt); }
  .val { font-weight: 500; font-size: 14px; margin-bottom: 8px; color: var(--txt); }
  @media (max-width: 600px) {
    header {
      padding: 12px 16px;
    }
    .logo {
      font-size: 26px;
    }
    main {
      padding: 12px;
    }
    .grid.stats {
      grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
      gap: 8px;
    }
    .stat {
      padding: 12px;
    }
    .stat .n {
      font-size: 22px;
    }
    .card {
      padding: 16px;
    }
    .row {
      flex-direction: column;
      align-items: stretch;
      gap: 12px;
    }
    .row > * {
      width: 100% !important;
      flex: none !important;
    }
    .modal-body { flex-direction: column; }
    #tmCover { width: 100% !important; height: auto !important; aspect-ratio: 1; }
  }
</style>
</head>
<body>
<header>
  <div class="header-gradient"></div>
  <div class="logo">M</div>
  <div class="status"><span id="dot" class="dot"></span><span id="statusText">idle</span></div>
  <nav>
    <button data-tab="dashboard" class="active">Dashboard</button>
    <button data-tab="library">Library</button>
    <button data-tab="artists">Artists</button>
    <button data-tab="playlists">Playlists</button>
    <button data-tab="tracks">Files</button>
    <button data-tab="settings">Settings</button>
    <button data-tab="console">Console</button>
  </nav>
</header>
<main>
  <section id="dashboard">
    <div class="grid stats" id="statCards"></div>
    <div class="gradient-sep"></div>
    <div class="card">
      <h2>Actions</h2>
      <div class="actions">
        <button class="btn" onclick="action('full')"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"></polygon></svg> Full Sync</button>
        <button class="btn ghost" onclick="action('scan')"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg> Scan Playlists</button>
        <button class="btn ghost" onclick="action('scan-artists')"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><circle cx="12" cy="12" r="3"></circle></svg> Scan Albums</button>
        <button class="btn ghost" onclick="action('artists-sync-new')"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"></path></svg> Sync New</button>
        <button class="btn ghost" onclick="action('artists-sync')"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"></polyline><polyline points="1 20 1 14 7 14"></polyline><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path></svg> Sync All</button>
        <button class="btn ghost" onclick="action('download-pending')"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg> Download</button>
        <button class="btn ghost" onclick="action('reconcile')"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path></svg> Reconcile</button>
        <button class="btn ghost" onclick="action('migrate-structure')"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"></path></svg> Migrate Structure</button>
        <button class="btn ghost" onclick="action('fix-metadata')"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"></path><line x1="7" y1="7" x2="7.01" y2="7"></line></svg> Fix Metadata</button>
        <button class="btn ghost danger-text" onclick="if(confirm('This will deduplicate all artists, tracks, and 1-track albums in the database and filesystem. Proceed?')) action('deduplicate')" style="color: #ff4b4b;"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path></svg> Deduplicate</button>
        <button class="btn danger" onclick="stop()"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="7.86 2 16.14 2 22 7.86 22 16.14 16.14 22 7.86 22 2 16.14 2 7.86 7.86 2"></polygon><line x1="15" y1="9" x2="9" y2="15"></line><line x1="9" y1="9" x2="15" y2="15"></line></svg> Stop</button>
      </div>
      <div class="gradient-sep"></div>
      <div class="row">
        <input id="directDownloadUrl" placeholder="Direct Download (Spotify/YT URL) - no DB checks" style="flex:1"/>
        <button class="btn ghost" onclick="downloadDirect()"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg> Download directly</button>
      </div>
      <div class="hint" style="margin-top:12px">Music folder: <span id="musicDirHint" class="muted">—</span></div>
    </div>
  </section>

  <section id="library" class="hide">
    <div class="card">
      <div class="row" style="margin-bottom:14px">
        <select id="libArtist" onchange="loadLibrary()"><option value="">All artists</option></select>
        <select id="libStatus" onchange="loadLibrary()">
          <option value="all">All albums</option>
          <option value="pending">Incomplete</option>
          <option value="complete">Complete</option>
        </select>
        <button class="btn ghost sm" onclick="loadLibrary()">Refresh</button>
      </div>
      <div style="overflow-x:auto;">
      <table class="table"><thead><tr><th>Artist</th><th>Album</th><th>Progress</th><th>Action</th></tr></thead>
      <tbody id="libRows"></tbody></table>
      </div>
      <div class="row" style="margin-top:12px; justify-content:space-between;">
        <button class="btn ghost sm" onclick="libPage=Math.max(0,libPage-1);renderLibrary()">← Prev</button>
        <span id="libPageInfo" class="muted">Page 1</span>
        <button class="btn ghost sm" onclick="libPage++;renderLibrary()">Next →</button>
      </div>
    </div>
    <div class="card hide" id="songPanel" style="margin-top:16px">
      <h2 id="songPanelTitle">Songs</h2>
      <table><thead><tr><th>#</th><th>Title</th><th>Status</th><th>Cover</th><th>Lyrics</th></tr></thead>
      <tbody id="songRows"></tbody></table>
    </div>
  </section>

  <section id="artists" class="hide">
    <div class="card">
      <h2>Add artist</h2>
      <div class="row">
        <input id="artistEntry" placeholder="Artist name or Spotify artist URL"/>
        <button class="btn" onclick="addArtist()">Add</button>
      </div>
    </div>
    <div class="card" style="margin-top:16px">
      <div class="row" style="margin-bottom:14px">
        <input id="artistSearch" placeholder="Search..." oninput="debouncedLoadArtists()"/>
        <div class="filter-chips" id="artistFilterChips" role="group" aria-label="Filter artists">
          <button type="button" class="chip active" data-value="all">All</button>
          <button type="button" class="chip chip-ro" data-value="romanian">Romanian</button>
          <button type="button" class="chip" data-value="international">International</button>
          <button type="button" class="chip" data-value="active">Active</button>
          <button type="button" class="chip" data-value="pending">Pending</button>
          <button type="button" class="chip" data-value="synced">Synced</button>
          <button type="button" class="chip" data-value="disabled">Disabled</button>
        </div>
        <button class="btn ghost sm" onclick="loadArtists()">Refresh</button>
        <button type="button" class="btn-ro" onclick="action('mark-romanian')" title="Auto-detect Romanian artists (list + MusicBrainz). Manual RO flags are kept.">RO</button>
      </div>
      <div style="overflow-x:auto;">
      <table class="table"><thead><tr><th>Artist</th><th>Albums</th><th>Songs</th><th>Status</th><th>Actions</th></tr></thead>
      <tbody id="artistRows"></tbody></table>
      </div>
      <div class="row" style="margin-top:12px; justify-content:space-between;">
        <button class="btn ghost sm" onclick="artPage=Math.max(0,artPage-1);renderArtists()">← Prev</button>
        <span id="artPageInfo" class="muted">Page 1</span>
        <button class="btn ghost sm" onclick="artPage++;renderArtists()">Next →</button>
      </div>
    </div>
  </section>

  <section id="playlists" class="hide">
    <div class="card">
      <h2>Add playlist</h2>
      <div class="row">
        <input id="plName" placeholder="Name" style="flex:.5"/>
        <input id="plUrl" placeholder="https://open.spotify.com/playlist/..."/>
        <button class="btn" onclick="addPlaylist()">Add</button>
      </div>
    </div>
    <div class="card" style="margin-top:16px">
      <div style="overflow-x:auto;">
      <table class="table"><thead><tr><th>Playlist</th><th>Status</th><th>Last scan</th><th>Actions</th></tr></thead>
      <tbody id="playlistRows"></tbody></table>
      </div>
    </div>
  </section>

  <section id="tracks" class="hide">
    <div class="card">
      <div class="row" style="margin-bottom:14px">
        <input id="trackSearch" placeholder="Search files..." oninput="debouncedLoadTracks()"/>
        <button class="btn ghost sm" onclick="loadTracks()">Refresh</button>
      </div>
      <div style="overflow-x:auto;">
      <table class="table"><thead><tr><th>Artist</th><th>Album</th><th>File</th><th></th></tr></thead>
      <tbody id="trackRows"></tbody></table>
      </div>
      <div class="hint" id="trackHint"></div>
    </div>
  </section>

  <section id="settings" class="hide">
    <div class="card">
      <h2>Settings</h2>
      <div class="field"><label>Music folder</label><input id="cfgMusicDir"/></div>
      <div class="row">
        <div class="field" style="flex:1"><label>Storage format</label>
          <select id="cfgFormat" style="width:100%"><option value="mp3">mp3</option><option value="opus">opus</option><option value="flac">flac</option></select>
        </div>
        <div class="field" style="flex:1"><label>Web download format</label>
          <select id="cfgDlFormat" style="width:100%"><option value="original">Original (as stored)</option><option value="mp3">Transcode to mp3</option></select>
        </div>
        <div class="field" style="flex:1"><label>Bitrate</label><input id="cfgBitrate" placeholder="320k"/></div>
        <div class="field" style="flex:1"><label>Workers (Threads)</label>
          <select id="cfgThreads" style="width:100%"><option value="1">1</option><option value="2">2</option><option value="3">3</option><option value="4">4</option><option value="8">8</option></select>
        </div>
      </div>
      <div class="field">
        <label>Artist Scanner Engine</label>
        <select id="cfgScanner" style="width:100%">
          <option value="spotify">Spotify (Precise but slow)</option>
          <option value="ytmusic">YouTube Music (Fast but includes remixes/EPs)</option>
        </select>
      </div>
      <div class="field"><label>Output template</label><input id="cfgTemplate" placeholder="{artist}/{album}/{track_number} - {title}.{output-ext}"/></div>
      <div class="field"><label>Lyrics providers (comma-separated)</label><input id="cfgLyrics" placeholder="genius,musixmatch,azlyrics"/></div>
      <div class="row">
        <div class="field" style="flex:1"><label>Playlist timeout (s)</label><input id="cfgPlTimeout" type="number"/></div>
        <div class="field" style="flex:1"><label>Playlist retries</label><input id="cfgPlRetries" type="number"/></div>
        <div class="field" style="flex:1"><label>Max DLs per Artist (0=∞)</label><input id="cfgMaxDl" type="number" placeholder="200"/></div>
      </div>
      <label style="display:flex; align-items:center; gap:12px; margin-bottom:24px; cursor:pointer;">
        <div class="switch">
          <input type="checkbox" id="cfgLrc">
          <span class="slider"></span>
        </div>
        <span style="font-size:14px; font-weight:500; color:var(--txt)">Generate .lrc sidecar files (Synchronized Lyrics)</span>
      </label>
      <button class="btn" onclick="saveSettings()" style="padding:12px 24px; font-size:14px;">Save settings</button>
      <div class="hint" style="margin-top:12px">Changing HUD port requires restarting the service.</div>
    </div>
  </section>

  <section id="console" class="hide">
    <div class="card">
      <div class="row" style="margin-bottom:14px">
        <h2 style="margin:0">Live Console</h2>
        <span class="status-live" style="display:inline-flex;align-items:center;gap:6px;font-size:12px;color:var(--success)"><span style="width:6px;height:6px;border-radius:50%;background:var(--success);box-shadow:0 0 6px var(--success)"></span><span id="wsState">connecting...</span></span>
        <button class="btn ghost sm" style="margin-left:auto" onclick="clearConsole()">Clear</button>
      </div>
      <div class="console" id="consoleOut"></div>
    </div>
  </section>
</main>

<div id="trackModal" class="modal hide" onclick="if(event.target===this) this.classList.add('hide')">
  <div class="modal-content">
    <button class="modal-close" onclick="document.getElementById('trackModal').classList.add('hide')">×</button>
    <div class="modal-body" style="display:flex; gap:24px;">
      <img id="tmCover" src="" style="width:200px; height:200px; object-fit:cover; border-radius:12px; background:#111; border: 1px solid var(--border-card);" />
      <div style="flex:1;">
        <h2 id="tmTitle" style="margin-top:0; font-size:22px; font-weight:800; line-height:1.3; margin-bottom:16px; font-family: 'Inter', sans-serif; background: linear-gradient(135deg, #fff 0%, #a1a1aa 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; text-overflow:ellipsis;"></h2>
        <div class="field" style="margin-bottom:6px"><label style="margin-bottom:2px">Artist</label><div id="tmArtist" class="val" style="font-family:'Inter', sans-serif"></div></div>
        <div class="field" style="margin-bottom:6px"><label style="margin-bottom:2px">Album</label><div id="tmAlbum" class="val" style="font-family:'Inter', sans-serif"></div></div>
        <div class="field" style="margin-bottom:6px"><label style="margin-bottom:2px">Genre</label><div id="tmGenre" class="val" style="font-family:'Inter', sans-serif"></div></div>
        <div class="field" style="margin-bottom:6px"><label style="margin-bottom:2px">Year</label><div id="tmYear" class="val" style="font-family:'Inter', sans-serif"></div></div>
        <div style="display:flex; gap:16px;">
          <div class="field" style="margin-bottom:6px; flex:1;"><label style="margin-bottom:2px">Length</label><div id="tmLength" class="val" style="font-family:'Inter', sans-serif"></div></div>
          <div class="field" style="margin-bottom:6px; flex:1;"><label style="margin-bottom:2px">Bitrate</label><div id="tmBitrate" class="val" style="font-family:'Inter', sans-serif"></div></div>
        </div>
      </div>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
const $=s=>document.querySelector(s);
const CHUNK=80;
let autoscroll=true;
let artistsLoadGen=0, libLoadGen=0, tracksLoadGen=0;
let artistTotal=0, libTotal=0, trackTotal=0;
let curPl=[];
function toast(m){const t=$('#toast');t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2200);}
function debounce(fn,ms){let t;return(...a)=>{clearTimeout(t);t=setTimeout(()=>fn(...a),ms);};}
function skeletonRows(cols,n=10){return Array.from({length:n},()=>'<tr class="skeleton-row">'+Array.from({length:cols},()=>'<td><span class="skel"></span></td>').join('')+'</tr>').join('');}
async function api(path,opts={},signal){
  const r=await fetch(path,{...opts,signal});
  if(!r.ok)throw new Error('HTTP '+r.status);
  return r.json();
}
function esc(s){return (s==null?'':s).toString().replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}
function artistById(id){return curArt.find(a=>a.spotify_id===id);}
function flashArtistRow(id){
  const tr=document.querySelector(`tr[data-aid="${id}"]`);
  if(tr){tr.classList.remove('row-flash');void tr.offsetWidth;tr.classList.add('row-flash');}
}
async function fetchAllChunks(baseUrl,isStale,onChunk,seed=[],startAt=0){
  let items=seed.slice(),offset=startAt,total=null;
  while(true){
    if(isStale())return items;
    const sep=baseUrl.includes('?')?'&':'?';
    const page=await api(`${baseUrl}${sep}offset=${offset}&limit=${CHUNK}`);
    if(isStale())return items;
    if(!page.items)break;
    total=page.total??page.items.length;
    items=items.concat(page.items);
    offset=items.length;
    onChunk(items,total);
    if(items.length>=total||page.items.length<CHUNK)break;
    await new Promise(r=>setTimeout(r,0));
  }
  return items;
}
document.querySelectorAll('nav button').forEach(b=>b.onclick=()=>{
  document.querySelectorAll('nav button').forEach(x=>x.classList.remove('active'));
  b.classList.add('active');
  ['dashboard','library','artists','playlists','tracks','settings','console'].forEach(id=>$('#'+id).classList.add('hide'));
  $('#'+b.dataset.tab).classList.remove('hide');
  if(b.dataset.tab==='artists')loadArtists();
  if(b.dataset.tab==='playlists')loadPlaylists();
  if(b.dataset.tab==='tracks')loadTracks();
  if(b.dataset.tab==='library'){loadLibArtists();loadLibrary();}
  if(b.dataset.tab==='settings')loadSettings();
});
async function loadStats(){
  const s=await api('/api/stats');
  const total_songs = s.songs_downloaded + s.songs_pending;
  const cards = [
    { title: 'Artists', main: s.artists_total, sub: `${s.artists_synced} / ${s.artists_total} synced` },
    { title: 'Albums', main: s.albums_total, sub: `${s.albums_downloaded} / ${s.albums_total} fully downloaded` },
    { title: 'Songs', main: total_songs, sub: `${s.songs_downloaded} / ${total_songs} downloaded` }
  ];
  $('#statCards').innerHTML=cards.map(c=>`
    <div class="card stat" style="padding: 20px 24px;">
      <div class="n" style="display:flex; align-items:baseline; gap:12px; margin-bottom: 8px;">
        <span style="font-size: 1.1em;">${c.main}</span>
        <span style="font-size:0.45em; color:var(--muted); font-weight:normal; letter-spacing: 0.5px;">${c.sub}</span>
      </div>
      <div class="l" style="font-weight: 600; letter-spacing: 1px; text-transform: uppercase; font-size: 0.75em; color: var(--txt); opacity: 0.9;">${c.title}</div>
    </div>
  `).join('');
  const busy=!!s.running;
  $('#dot').className='dot'+(busy?' busy':'');
  $('#statusText').textContent=busy?('running: '+s.running):'idle';
  $('#musicDirHint').textContent=s.music_dir||'—';
}
async function loadSettings(){
  const c=await api('/api/config');
  $('#cfgMusicDir').value=c.music_dir||'';
  $('#cfgFormat').value=c.format||'mp3';
  $('#cfgDlFormat').value=c.download_format||'original';
  $('#cfgScanner').value=c.artist_scanner||'spotify';
  $('#cfgBitrate').value=c.bitrate||'320k';
  $('#cfgTemplate').value=c.output_template||'{artist}/{album}/{track-number} - {title}.{output-ext}';
  $('#cfgLyrics').value=(c.lyrics_providers||[]).join(',');
  $('#cfgPlTimeout').value=c.playlist_save_timeout||600;
  $('#cfgPlRetries').value=c.playlist_save_retries||3;
  $('#cfgMaxDl').value=c.max_downloads_per_artist||0;
  $('#cfgLrc').checked=!!c.generate_lrc;
  $('#cfgThreads').value=c.threads||4;
}
async function saveSettings(){
  const body={
    music_dir:$('#cfgMusicDir').value.trim(),
    format:$('#cfgFormat').value,
    download_format:$('#cfgDlFormat').value,
    artist_scanner:$('#cfgScanner').value,
    bitrate:$('#cfgBitrate').value.trim(),
    threads:parseInt($('#cfgThreads').value)||4,
    output_template:$('#cfgTemplate').value.trim(),
    lyrics_providers:$('#cfgLyrics').value.split(',').map(s=>s.trim()).filter(Boolean),
    playlist_save_timeout:parseInt($('#cfgPlTimeout').value)||600,
    playlist_save_retries:parseInt($('#cfgPlRetries').value)||3,
    max_downloads_per_artist:parseInt($('#cfgMaxDl').value)||0,
    generate_lrc:$('#cfgLrc').checked
  };
  const r=await api('/api/config',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  if(r.error){toast(r.error);return;}
  toast('Settings saved');
  loadStats();
}
async function loadLibArtists(){
  const rows=await api('/api/artists/names');
  const sel=$('#libArtist');
  const cur=sel.value;
  sel.innerHTML='<option value="">All artists</option>'+rows.map(r=>`<option value="${r.spotify_id}">${esc(r.name)}</option>`).join('');
  if(cur)sel.value=cur;
}
let curLib = [], libPage = 0;
function libBaseUrl(){
  const aid=$('#libArtist').value,st=$('#libStatus').value;
  let url=`/api/albums?status=${st}`;
  if(aid)url+=`&artist_id=${encodeURIComponent(aid)}`;
  return url;
}
async function loadLibrary(){
  const gen=++libLoadGen;
  libPage=0;
  curLib=[];
  libTotal=0;
  $('#libRows').innerHTML=skeletonRows(4);
  $('#libPageInfo').innerHTML='Loading<span class="loading-hint">…</span>';
  try{
    const first=await api(`${libBaseUrl()}&offset=0&limit=${CHUNK}`);
    if(gen!==libLoadGen)return;
    curLib=first.items||[];
    libTotal=first.total??curLib.length;
    renderLibrary();
    fetchAllChunks(libBaseUrl(),()=>gen!==libLoadGen,(items,total)=>{
      curLib=items;libTotal=total;renderLibrary();
    },curLib,curLib.length);
  }catch(e){
    if(gen===libLoadGen)$('#libRows').innerHTML='<tr><td colspan=4 class="muted">Load failed.</td></tr>';
  }
}
function renderLibrary(){
  const start=libPage*100, end=start+100;
  const pageRows=curLib.slice(start,end);
  const loaded=curLib.length;
  const tail=loaded<libTotal?` · loading ${loaded}/${libTotal}`:'';
  $('#libPageInfo').textContent=`Page ${libPage+1} of ${Math.ceil(Math.max(loaded,1)/100)||1} (${libTotal||loaded} total)${tail}`;
  $('#libRows').innerHTML=pageRows.map(r=>{
    const pct=r.track_count?Math.round(100*r.downloaded_count/r.track_count):0;
    const bar=`<div style="height:4px;background:rgba(255,255,255,.1);border-radius:99px;overflow:hidden;width:60px;display:inline-block;vertical-align:middle;margin-left:6px"><div style="height:100%;width:${pct}%;background:#ffffff;border-radius:99px"></div></div>`;
    const pill=r.downloaded_count>=r.track_count&&r.track_count>0?'<span class="pill done">done</span>':'<span class="pill pend">'+pct+'%</span>';
    return `<tr><td>${esc(r.artist_name)}</td><td>${esc(r.name)}</td><td class="progress">${r.downloaded_count}/${r.track_count} ${bar} ${pill}</td>
      <td><button class="btn ghost sm" onclick="showSongs('${r.spotify_id}','${esc(r.name)}')">Songs</button></td></tr>`;
  }).join('')||'<tr><td colspan=4 class="muted">No albums yet — run Scan Albums.</td></tr>';
}
async function showSongs(albumId,albumName){
  $('#songPanelTitle').textContent='Songs — '+albumName;
  $('#songPanel').classList.remove('hide');
  $('#songRows').innerHTML=skeletonRows(5,6);
  const res=await api(`/api/songs?album_id=${encodeURIComponent(albumId)}`);
  const rows=Array.isArray(res)?res:(res.items||[]);
  const meta=v=>v?'<span class="meta-ok">✓</span>':'<span class="meta-no">—</span>';
  $('#songRows').innerHTML=rows.map(r=>{
    const st=r.status==='downloaded'?'<span class="pill done">ok</span>':'<span class="pill pend">'+r.status+'</span>';
    return `<tr><td class="muted">${r.track_number||''}</td><td>${esc(r.title)}</td><td>${st}</td><td>${meta(r.has_cover)}</td><td>${meta(r.has_lyrics)}</td></tr>`;
  }).join('')||'<tr><td colspan=5 class="muted">No songs.</td></tr>';
}
let curArt = [], artPage = 0, artistFilter = 'all';
document.getElementById('artistFilterChips')?.addEventListener('click', e => {
  const chip = e.target.closest('.chip');
  if (!chip) return;
  artistFilter = chip.dataset.value || 'all';
  document.querySelectorAll('#artistFilterChips .chip').forEach(c =>
    c.classList.toggle('active', c === chip));
  loadArtists();
});
const debouncedLoadArtists=debounce(()=>loadArtists(),280);
function artistsBaseUrl(){
  const q=encodeURIComponent($('#artistSearch').value||'');
  return `/api/artists?q=${q}&status=${artistFilter}`;
}
async function loadArtists(){
  const gen=++artistsLoadGen;
  artPage=0;
  curArt=[];
  artistTotal=0;
  $('#artistRows').innerHTML=skeletonRows(5);
  $('#artPageInfo').innerHTML='Loading<span class="loading-hint">…</span>';
  try{
    const first=await api(`${artistsBaseUrl()}&offset=0&limit=${CHUNK}`);
    if(gen!==artistsLoadGen)return;
    curArt=first.items||[];
    artistTotal=first.total??curArt.length;
    renderArtists();
    fetchAllChunks(artistsBaseUrl(),()=>gen!==artistsLoadGen,(items,total)=>{
      curArt=items;artistTotal=total;renderArtists();
    },curArt,curArt.length);
  }catch(e){
    if(gen===artistsLoadGen)$('#artistRows').innerHTML='<tr><td colspan=5 class="muted">Load failed.</td></tr>';
  }
}
function renderArtists(){
  const start=artPage*100, end=start+100;
  const pageRows=curArt.slice(start,end);
  const loaded=curArt.length;
  const tail=loaded<artistTotal?` · loading ${loaded}/${artistTotal}`:'';
  $('#artPageInfo').textContent=`Page ${artPage+1} of ${Math.ceil(Math.max(loaded,1)/100)||1} (${artistTotal||loaded} total)${tail}`;
  $('#artistRows').innerHTML=pageRows.map(r=>{
    const sync=r.sync_done?'<span class="pill done">synced</span>':'<span class="pill pend">pending</span>';
    const act=r.active?'<span class="pill on">on</span>':'<span class="pill off">off</span>';
    const prog=(r.songs_dl||0)+'/'+(r.songs_total||0);
    const roCls = r.is_romanian ? 'on' : '';
    const roTitle = r.is_romanian ? 'Clear Romanian flag' : 'Mark as Romanian';
    const rowCls = r.is_romanian ? 'row-ro' : '';
    return `<tr class="${rowCls}" data-aid="${r.spotify_id}"><td><button type="button" class="ro-toggle ${roCls}" title="${roTitle}" onclick="setRomanian('${r.spotify_id}', ${r.is_romanian?0:1})">RO</button>${esc(r.name)}</td><td class="muted">${r.album_count||0}</td><td class="muted">${prog}</td><td>${act} ${sync}</td>
      <td>
        <div style="display:flex; gap:6px; align-items:center;">
          <select class="sm" style="width:85px; padding: 2px 4px; border: 1px solid var(--border-card); background: #111; color: var(--txt); border-radius: 4px;" onchange="setArtistLimit('${r.spotify_id}', this.value)" title="Max Downloads">
            <option value="" ${r.max_downloads==null?'selected':''}>Global</option>
            <option value="50" ${r.max_downloads===50?'selected':''}>50</option>
            <option value="100" ${r.max_downloads===100?'selected':''}>100</option>
            <option value="150" ${r.max_downloads===150?'selected':''}>150</option>
            <option value="0" ${r.max_downloads===0?'selected':''}>Unlimit</option>
          </select>
          <button class="btn ghost sm" onclick="toggleArtist('${r.spotify_id}')">${r.active?'Off':'On'}</button>
          <button class="btn danger sm" onclick="delArtist('${r.spotify_id}')">×</button>
        </div>
      </td></tr>`;
  }).join('')||'<tr><td colspan=5 class="muted">No artists yet.</td></tr>';
}
async function addArtist(){
  const v=$('#artistEntry').value.trim();if(!v)return;
  await api('/api/artists',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({entry:v})});
  $('#artistEntry').value='';toast('Adding — see console');showConsole();
}
async function toggleArtist(id){
  const a=artistById(id);if(!a)return;
  const prev=a.active;
  a.active=prev?0:1;
  renderArtists();flashArtistRow(id);
  try{
    const r=await api(`/api/artists/${id}/toggle`,{method:'POST'});
    a.active=r.active?1:0;
    renderArtists();
  }catch(e){a.active=prev;renderArtists();toast('Update failed');}
}
async function setRomanian(id, val){
  const a=artistById(id);if(!a)return;
  const prev=!!a.is_romanian;
  a.is_romanian=val?1:0;
  renderArtists();flashArtistRow(id);
  try{
    const r=await api(`/api/artists/${id}/ro`,{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({is_romanian:!!val}),
    });
    a.is_romanian=r.is_romanian?1:0;
    renderArtists();
  }catch(e){a.is_romanian=prev?1:0;renderArtists();toast('Update failed');}
}
async function setArtistLimit(id, limit){
  const a=artistById(id);if(!a)return;
  const prev=a.max_downloads;
  a.max_downloads=limit===''?null:parseInt(limit,10);
  renderArtists();flashArtistRow(id);
  try{
    await api(`/api/artists/${id}/limit`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({limit:limit})});
  }catch(e){a.max_downloads=prev;renderArtists();toast('Update failed');}
}
async function delArtist(id){
  if(!confirm('Remove artist from database?'))return;
  const delFiles=confirm('Do you also want to physically delete all downloaded files/folders for this artist from storage?');
  await api(`/api/artists/${id}?delete_files=${delFiles}`,{method:'DELETE'});
  loadArtists();
}
function renderPlaylists(){
  $('#playlistRows').innerHTML=curPl.map(r=>{
    const act=r.active?'<span class="pill on">on</span>':'<span class="pill off">off</span>';
    return `<tr><td><a href="${esc(r.url)}" target="_blank">${esc(r.name)}</a></td><td>${act}</td><td class="muted">${(r.last_synced||'-').slice(0,10)}</td>
      <td>
        <div style="display:flex; gap:6px; align-items:center;">
          <button class="btn ghost sm" onclick="syncPl('${r.url}')">Sync</button>
          <button class="btn ghost sm" onclick="togglePl('${r.spotify_id}')">${r.active?'Off':'On'}</button>
          <button class="btn danger sm" onclick="delPl('${r.spotify_id}')">×</button>
        </div>
      </td></tr>`;
  }).join('')||'<tr><td colspan=4 class="muted">No playlists.</td></tr>';
}
async function loadPlaylists(){
  curPl=await api('/api/playlists');
  renderPlaylists();
}
async function addPlaylist(){
  const name=$('#plName').value.trim(),url=$('#plUrl').value.trim();if(!url)return;
  const r=await api('/api/playlists',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,url})});
  if(r.error){toast(r.error);return;}
  $('#plName').value='';$('#plUrl').value='';loadPlaylists();toast('Added');
}
async function syncPl(url){
  const r=await api('/api/actions/sync-playlist',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
  if(r.error){toast(r.error);return;}
  toast('Started: '+r.label);showConsole();
}
async function togglePl(id){
  const p=curPl.find(x=>x.spotify_id===id);if(!p)return;
  const prev=p.active;
  p.active=prev?0:1;
  renderPlaylists();
  try{
    const r=await api(`/api/playlists/${id}/toggle`,{method:'POST'});
    p.active=r.active?1:0;
    renderPlaylists();
  }catch(e){p.active=prev;renderPlaylists();toast('Update failed');}
}
async function delPl(id){if(!confirm('Remove?'))return;await api(`/api/playlists/${id}`,{method:'DELETE'});loadPlaylists();}
let curTracks=[];
function tracksBaseUrl(){
  const q=encodeURIComponent($('#trackSearch').value||'');
  return `/api/tracks?q=${q}`;
}
async function loadTracks(){
  const gen=++tracksLoadGen;
  curTracks=[];
  trackTotal=0;
  $('#trackRows').innerHTML=skeletonRows(4);
  $('#trackHint').textContent='Loading…';
  try{
    const first=await api(`${tracksBaseUrl()}&offset=0&limit=${CHUNK}`);
    if(gen!==tracksLoadGen)return;
    curTracks=first.items||[];
    trackTotal=first.total??curTracks.length;
    renderTracks();
    fetchAllChunks(tracksBaseUrl(),()=>gen!==tracksLoadGen,(items,total)=>{
      curTracks=items;trackTotal=total;renderTracks();
    },curTracks,curTracks.length);
  }catch(e){
    if(gen===tracksLoadGen)$('#trackRows').innerHTML='<tr><td colspan=4 class="muted">Load failed.</td></tr>';
  }
}
const debouncedLoadTracks=debounce(()=>loadTracks(),320);
function renderTracks(){
  const tail=curTracks.length<trackTotal?` (${curTracks.length}/${trackTotal} loaded)`:'';
  $('#trackRows').innerHTML=curTracks.map(r=>`<tr><td>${esc(r.artist)}</td><td class="muted">${esc(r.album)}</td><td>${esc(r.title)}</td>
    <td style="text-align:right; white-space:nowrap;">
      <div style="display:inline-flex; gap:6px; justify-content:flex-end; align-items:center;">
        <a class="btn ghost sm" href="/api/track/download?path=${encodeURIComponent(r.path)}" download title="Download"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg></a>
        <button class="btn ghost sm" onclick="showTrackInfo('${esc(r.path)}')">Info</button>
      </div>
    </td></tr>`).join('')
    ||'<tr><td colspan=4 class="muted">No files found.</td></tr>';
  $('#trackHint').textContent=(trackTotal||curTracks.length)+' files'+(tail?' — still indexing…':'');
}
async function showTrackInfo(path) {
  $('#tmCover').src=''; $('#tmTitle').textContent='Loading...';
  $('#tmArtist').textContent='-'; $('#tmAlbum').textContent='-';
  $('#tmGenre').textContent='-'; $('#tmYear').textContent='-';
  $('#tmLength').textContent='-'; $('#tmBitrate').textContent='-';
  $('#trackModal').classList.remove('hide');
  const info=await api('/api/track/info?path='+encodeURIComponent(path));
  if(info.error){ $('#tmTitle').textContent='Error'; return; }
  $('#tmTitle').textContent=info.title||'Unknown';
  $('#tmArtist').textContent=info.artist||'-';
  $('#tmAlbum').textContent=info.album||'-';
  $('#tmGenre').textContent=info.genre||'-';
  $('#tmYear').textContent=info.year||'-';
  
  if(info.length) {
    const s = Math.round(info.length);
    $('#tmLength').textContent=Math.floor(s/60)+':'+(s%60).toString().padStart(2,'0');
  }
  if(info.bitrate) {
    $('#tmBitrate').textContent=Math.round(info.bitrate/1000)+' kbps';
  }

  if(info.has_cover) {
    $('#tmCover').src='/api/track/cover?path='+encodeURIComponent(path)+'&t='+Date.now();
  } else {
    $('#tmCover').src='';
  }
}
async function action(a){const r=await api('/api/actions/'+a,{method:'POST'});toast('Started: '+(r.label||a));showConsole();}
async function downloadDirect(){
  const url=$('#directDownloadUrl').value.trim();if(!url)return;
  const r=await api('/api/actions/download',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
  if(r.error){toast(r.error);return;}
  $('#directDownloadUrl').value='';
  toast('Started: '+r.label);showConsole();
}
async function stop(){await api('/api/stop',{method:'POST'});toast('Stop requested');}
function showConsole(){document.querySelector('nav button[data-tab="console"]').click();}
function clearConsole(){$('#consoleOut').innerHTML='';}
function connectWS(){
  const proto=location.protocol==='https:'?'wss':'ws';
  const ws=new WebSocket(`${proto}://${location.host}/ws/logs`);
  ws.onopen=()=>$('#wsState').textContent='live';
  ws.onclose=()=>{$('#wsState').textContent='reconnecting...';setTimeout(connectWS,1500);};
  ws.onmessage=e=>{
    const out=$('#consoleOut');
    const d=document.createElement('div');
    const text=e.data;
    d.textContent=text;
    if(/ERROR|✗|failed|error/i.test(text)) d.style.color='#f87171';
    else if(/WARN|WARNING/i.test(text)) d.style.color='#fbbf24';
    else if(/===/.test(text)) d.style.color='#ffffff';
    else if(/✓|done|complete|ok/i.test(text)) d.style.color='#34d399';
    else if(/\[\d+\/\d+\]/.test(text)) d.style.color='#ffffff';
    out.appendChild(d);
    while(out.childNodes.length>1200)out.removeChild(out.firstChild);
    if(autoscroll)out.scrollTop=out.scrollHeight;
  };
}
$('#consoleOut').addEventListener('scroll',e=>{
  const el=e.target;autoscroll=(el.scrollHeight-el.scrollTop-el.clientHeight)<40;
});
loadStats();connectWS();setInterval(loadStats,15000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(load_cfg().get("hud_port", 8800)))
