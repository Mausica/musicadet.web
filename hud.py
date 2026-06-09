#!/usr/bin/env python3
"""
hud.py — MusicaDet web dashboard for music_sync.py
"""

import asyncio
import json
import os
import sqlite3
import sys
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

_db_lock = threading.RLock()
_schema_ready = False

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
    "output_template": "{artist}/{album}/{title}.{output-ext}",
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
    "youtube_cookies_file", "youtube_cookies_from_browser",
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
    conn = sqlite3.connect(cfg["db_path"], timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _commit_retry(conn: sqlite3.Connection, attempts: int = 8) -> None:
    for attempt in range(attempts):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt >= attempts - 1:
                raise
            time.sleep(0.15 * (attempt + 1))


@contextmanager
def db_tx():
    """Writable DB transaction — serialized to avoid database is locked."""
    with _db_lock:
        conn = db()
        try:
            yield conn
            _commit_retry(conn)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _ensure_artist_schema(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(artists)")}
    if "max_downloads" not in cols:
        conn.execute("ALTER TABLE artists ADD COLUMN max_downloads INTEGER")
    if "is_romanian" not in cols:
        conn.execute("ALTER TABLE artists ADD COLUMN is_romanian INTEGER DEFAULT 0")
    if "romanian_manual" not in cols:
        conn.execute("ALTER TABLE artists ADD COLUMN romanian_manual INTEGER DEFAULT 0")


def ensure_db(force: bool = False) -> None:
    global _schema_ready
    if _schema_ready and not force:
        return
    last_exc: Optional[Exception] = None
    for attempt in range(10):
        try:
            with db_tx() as conn:
                _run_ensure_db_schema(conn)
            _schema_ready = True
            return
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if "locked" not in str(exc).lower():
                raise
            time.sleep(0.3 * (attempt + 1))
    if last_exc:
        raise last_exc


def _run_ensure_db_schema(conn: sqlite3.Connection) -> None:
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
    if "ytmusic_name" not in cols:
        conn.execute("ALTER TABLE artists ADD COLUMN ytmusic_name TEXT")
    if "ytmusic_browse_id" not in cols:
        conn.execute("ALTER TABLE artists ADD COLUMN ytmusic_browse_id TEXT")
    if "ytmusic_status" not in cols:
        conn.execute("ALTER TABLE artists ADD COLUMN ytmusic_status TEXT DEFAULT 'unknown'")
    if "ytmusic_notes" not in cols:
        conn.execute("ALTER TABLE artists ADD COLUMN ytmusic_notes TEXT")
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
    import logging
    logging.basicConfig(level=logging.INFO)
    Path(load_cfg()["db_path"]).parent.mkdir(parents=True, exist_ok=True)
    try:
        ensure_db()
    except Exception as exc:
        logging.error("HUD schema init deferred (will retry on write): %s", exc)


class ArtistIn(BaseModel):
    entry: str


class BulkArtistsIn(BaseModel):
    ids: list[str] = []
    action: str
    q: str = ""
    status: str = "all"
    limit: Optional[int] = None


class ArtistLimitBody(BaseModel):
    spotify_id: str
    limit: Optional[Union[str, int]] = None


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
    with db() as conn:
        a_total = conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0]
        a_active = conn.execute("SELECT COUNT(*) FROM artists WHERE active=1").fetchone()[0]
        a_synced = conn.execute("SELECT COUNT(*) FROM artists WHERE sync_done=1").fetchone()[0]
        p_total = conn.execute("SELECT COUNT(*) FROM playlists").fetchone()[0]
        p_active = conn.execute("SELECT COUNT(*) FROM playlists WHERE active=1").fetchone()[0]
        albums_total = conn.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
        albums_downloaded = conn.execute("SELECT COUNT(*) FROM albums WHERE downloaded_count >= track_count AND track_count > 0").fetchone()[0]
        cap = _aggregate_capped_song_stats(conn, active_only=True)
        songs_downloaded = cap["songs_downloaded"]
        songs_pending = cap["songs_pending"]
        songs_target = cap["songs_target"]
        songs_catalog = cap["songs_catalog"]
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
        "songs_downloaded": songs_downloaded,
        "songs_pending": songs_pending,
        "songs_target": songs_target,
        "songs_catalog": songs_catalog,
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


def _decode_artist_id(spotify_id: str) -> str:
    from urllib.parse import unquote
    return unquote(spotify_id)


def _apply_artist_limit(spotify_id: str, limit_raw) -> dict:
    _db_ready()
    sid = _decode_artist_id((spotify_id or "").strip())
    if not sid:
        return {"error": "missing artist id"}
    with db_tx() as conn:
        _ensure_artist_schema(conn)
        if not conn.execute("SELECT 1 FROM artists WHERE spotify_id=?", (sid,)).fetchone():
            return {"error": "not found"}
        if limit_raw is None or str(limit_raw).strip() == "":
            conn.execute("UPDATE artists SET max_downloads=NULL WHERE spotify_id=?", (sid,))
        else:
            try:
                val = int(limit_raw)
            except (TypeError, ValueError):
                return {"error": "invalid limit"}
            conn.execute("UPDATE artists SET max_downloads=? WHERE spotify_id=?", (val, sid))
        saved = conn.execute(
            "SELECT max_downloads FROM artists WHERE spotify_id=?", (sid,)
        ).fetchone()
    return {"ok": True, "max_downloads": saved["max_downloads"] if saved else None}


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
    elif status == "ytok":
        where.append("a.ytmusic_status='found'")
    elif status == "ytmissing":
        where.append("a.ytmusic_status='not_found'")
    elif status == "ytunknown":
        where.append("(a.ytmusic_status IS NULL OR a.ytmusic_status='unknown')")
    if q2 := {"romanian": "a.is_romanian=1", "international": "a.is_romanian=0"}.get(status):
        where.append(q2)
    return where, params


def _effective_artist_limit(artist_max_downloads, global_max: int) -> int:
    if artist_max_downloads is not None:
        return int(artist_max_downloads or 0)
    return int(global_max or 0)


def _aggregate_capped_song_stats(conn, *, active_only: bool = True) -> dict:
    """Totals for dashboard: target = sum of per-artist caps (or full catalog if unlimited)."""
    global_max = int(load_cfg().get("max_downloads_per_artist", 0))
    clause = "WHERE a.active=1" if active_only else ""
    rows = conn.execute(
        f"""
        SELECT a.max_downloads,
               COALESCE(SUM(CASE WHEN s.status='downloaded' THEN 1 ELSE 0 END), 0) AS dl,
               COALESCE(COUNT(s.spotify_id), 0) AS catalog
        FROM artists a
        LEFT JOIN songs s ON s.artist_id = a.spotify_id
        {clause}
        GROUP BY a.spotify_id, a.max_downloads
        """
    ).fetchall()

    downloaded = pending = target = catalog = 0
    for r in rows:
        dl = int(r["dl"])
        cat = int(r["catalog"])
        cap = _effective_artist_limit(r["max_downloads"], global_max)
        catalog += cat
        if cap > 0:
            target += cap
            downloaded += min(dl, cap)
            pending += min(max(0, cap - dl), max(0, cat - dl))
        else:
            target += cat
            downloaded += dl
            pending += max(0, cat - dl)

    return {
        "songs_downloaded": downloaded,
        "songs_target": target,
        "songs_pending": pending,
        "songs_catalog": catalog,
    }


def _cap_artist_song_counts(rows: list[dict]) -> None:
    """Per-artist SONGS column: downloaded / cap (not raw catalog size)."""
    global_max = int(load_cfg().get("max_downloads_per_artist", 0))
    for r in rows:
        max_dl = _effective_artist_limit(r["max_downloads"], global_max)
        if max_dl > 0:
            dl = min(int(r.get("songs_dl") or 0), max_dl)
            r["songs_dl"] = dl
            r["songs_total"] = max_dl


@app.get("/api/artists/names")
def api_artist_names():
    with db() as conn:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT spotify_id, name FROM artists WHERE active=1 ORDER BY name COLLATE NOCASE"
            ).fetchall()
        ]


_ARTIST_SORT = {
    "name": "a.name COLLATE NOCASE",
    "songs_desc": "songs_total DESC, a.name COLLATE NOCASE",
    "songs_asc": "songs_total ASC, a.name COLLATE NOCASE",
}


@app.get("/api/artists")
def api_artists(
    q: str = "",
    status: str = "all",
    sort: str = "name",
    offset: int = Query(0, ge=0),
    limit: int = Query(80, ge=1, le=500),
):
    order_by = _ARTIST_SORT.get(sort, _ARTIST_SORT["name"])
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
               a.albums_scanned_at, a.max_downloads, a.is_romanian, a.ytmusic_status, a.ytmusic_name, a.ytmusic_notes,
               COALESCE(ac.album_count, 0) AS album_count,
               COALESCE(sc.songs_dl, 0) AS songs_dl,
               COALESCE(sc.songs_total, 0) AS songs_total
        {base_from}
        ORDER BY {order_by}
        LIMIT ? OFFSET ?
    """
    count_sql = f"SELECT COUNT(*) {base_from}"
    with db() as conn:
        total = conn.execute(count_sql, params).fetchone()[0]
        rows = [dict(r) for r in conn.execute(sql, params + [limit, offset]).fetchall()]
    _cap_artist_song_counts(rows)
    return {"items": rows, "total": total, "sort": sort}


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


@app.get("/api/db/summary")
def api_db_summary():
    with db() as conn:
        def one(sql: str, params: tuple = ()) -> int:
            return conn.execute(sql, params).fetchone()[0]

        incomplete = one(
            "SELECT COUNT(*) FROM albums WHERE track_count > 0 AND downloaded_count < track_count"
        )
        cap = _aggregate_capped_song_stats(conn, active_only=True)
        return {
            "artists_total": one("SELECT COUNT(*) FROM artists WHERE active >= 0"),
            "artists_active": one("SELECT COUNT(*) FROM artists WHERE active = 1"),
            "artists_disabled": one("SELECT COUNT(*) FROM artists WHERE active = 0"),
            "artists_pending_sync": one(
                "SELECT COUNT(*) FROM artists WHERE active = 1 AND sync_done = 0"
            ),
            "artists_romanian": one("SELECT COUNT(*) FROM artists WHERE is_romanian = 1"),
            "albums_total": one("SELECT COUNT(*) FROM albums"),
            "albums_incomplete": incomplete,
            "songs_total": cap["songs_target"],
            "songs_downloaded": cap["songs_downloaded"],
            "songs_pending": cap["songs_pending"],
            "songs_catalog": cap["songs_catalog"],
            "songs_failed": one("SELECT COUNT(*) FROM songs WHERE status = 'failed'"),
            "playlists_active": one("SELECT COUNT(*) FROM playlists WHERE active = 1"),
        }


@app.post("/api/artists/bulk")
def api_bulk_artists(body: BulkArtistsIn):
    _db_ready()
    allowed = {"enable", "disable", "ro_on", "ro_off", "limit"}
    if body.action not in allowed:
        return JSONResponse({"error": "invalid action"}, status_code=400)
    with db_tx() as conn:
        _ensure_artist_schema(conn)
        if body.ids:
            target_ids = list(dict.fromkeys(body.ids))
        else:
            where, params = _artist_filters(body.q, body.status)
            clause = " WHERE " + " AND ".join(where) if where else ""
            target_ids = [
                r[0]
                for r in conn.execute(
                    f"SELECT spotify_id FROM artists a{clause}", params
                ).fetchall()
            ]
        if not target_ids:
            return {"ok": True, "updated": 0}
        placeholders = ",".join("?" * len(target_ids))
        if body.action == "enable":
            conn.execute(
                f"UPDATE artists SET active=1 WHERE spotify_id IN ({placeholders})",
                target_ids,
            )
        elif body.action == "disable":
            conn.execute(
                f"UPDATE artists SET active=0 WHERE spotify_id IN ({placeholders})",
                target_ids,
            )
        elif body.action == "ro_on":
            conn.execute(
                f"UPDATE artists SET is_romanian=1, romanian_manual=1 WHERE spotify_id IN ({placeholders})",
                target_ids,
            )
        elif body.action == "ro_off":
            conn.execute(
                f"UPDATE artists SET is_romanian=0, romanian_manual=1 WHERE spotify_id IN ({placeholders})",
                target_ids,
            )
        elif body.action == "limit":
            if body.limit is None:
                conn.execute(
                    f"UPDATE artists SET max_downloads=NULL WHERE spotify_id IN ({placeholders})",
                    target_ids,
                )
            else:
                conn.execute(
                    f"UPDATE artists SET max_downloads=? WHERE spotify_id IN ({placeholders})",
                    [int(body.limit)] + target_ids,
                )
    return {"ok": True, "updated": len(target_ids)}


@app.post("/api/artists")
async def api_add_artist(body: ArtistIn):
    entry = body.entry.strip()
    if not entry:
        return JSONResponse({"error": "empty"}, status_code=400)
    asyncio.create_task(bus.run([sys.executable, str(PY_SCRIPT), "add", entry], f"add artist: {entry}"))
    return {"ok": True}


def _db_ready() -> None:
    if not _schema_ready:
        ensure_db()


@app.post("/api/artists/{spotify_id:path}/toggle")
def api_toggle_artist(spotify_id: str):
    _db_ready()
    spotify_id = _decode_artist_id(spotify_id)
    with db_tx() as conn:
        row = conn.execute("SELECT active FROM artists WHERE spotify_id=?", (spotify_id,)).fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        new = 0 if row["active"] else 1
        conn.execute("UPDATE artists SET active=? WHERE spotify_id=?", (new, spotify_id))
    return {"ok": True, "active": new}


@app.post("/api/artists/{spotify_id:path}/download")
async def api_download_artist(spotify_id: str):
    _db_ready()
    spotify_id = _decode_artist_id(spotify_id)
    with db() as conn:
        row = conn.execute("SELECT name FROM artists WHERE spotify_id=?", (spotify_id,)).fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        artist_name = row["name"]
    label = f"Descărcare artist: {artist_name}"
    asyncio.create_task(bus.run([sys.executable, str(PY_SCRIPT), "artists-sync", "--artist", spotify_id], label))
    return {"ok": True, "label": label}


@app.post("/api/artists/limit")
def api_set_artist_limit_body(body: ArtistLimitBody):
    result = _apply_artist_limit(body.spotify_id, body.limit)
    if result.get("error") == "not found":
        return JSONResponse(result, status_code=404)
    if result.get("error"):
        return JSONResponse(result, status_code=400)
    return result


@app.post("/api/artists/{spotify_id:path}/limit")
async def api_set_artist_limit(spotify_id: str, request: Request):
    try:
        data = await request.json()
    except Exception:
        data = {}
    limit = data.get("limit")
    result = _apply_artist_limit(spotify_id, limit)
    if result.get("error") == "not found":
        return JSONResponse(result, status_code=404)
    if result.get("error"):
        return JSONResponse(result, status_code=400)
    return result


@app.post("/api/artists/{spotify_id:path}/ro")
async def api_set_romanian(spotify_id: str, request: Request):
    _db_ready()
    spotify_id = _decode_artist_id(spotify_id)
    try:
        body = await request.json()
    except Exception:
        body = {}
    with db_tx() as conn:
        _ensure_artist_schema(conn)
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


@app.post("/api/artists/{spotify_id:path}/ytmusic")
async def api_mark_artist_ytmusic(spotify_id: str, request: Request):
    """Mark artist as found, not found, or manually mapped on YouTube Music."""
    _db_ready()
    spotify_id = _decode_artist_id(spotify_id)
    try:
        body = await request.json()
    except Exception:
        body = {}
    
    status = body.get("status")  # 'found', 'not_found', 'manually_mapped'
    ytmusic_name = body.get("ytmusic_name")  # New name if manually mapped
    notes = body.get("notes", "")
    
    if status not in ("found", "not_found", "manually_mapped"):
        return JSONResponse({"error": "invalid status"}, status_code=400)
    
    with db_tx() as conn:
        _ensure_artist_schema(conn)
        row = conn.execute("SELECT name FROM artists WHERE spotify_id=?", (spotify_id,)).fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        
        updates = {"ytmusic_status": status}
        if ytmusic_name:
            updates["ytmusic_name"] = ytmusic_name
        if notes:
            updates["ytmusic_notes"] = notes
        
        update_sql = "UPDATE artists SET " + ", ".join(f"{k}=?" for k in updates.keys()) + " WHERE spotify_id=?"
        conn.execute(update_sql, list(updates.values()) + [spotify_id])
    
    return {"ok": True, "status": status, "ytmusic_name": ytmusic_name or row["name"]}


@app.post("/api/artists/{spotify_id:path}/edit")
async def api_edit_artist(spotify_id: str, request: Request):
    """Edit artist name/metadata (for manual corrections and marking as manually added)."""
    _db_ready()
    spotify_id = _decode_artist_id(spotify_id)
    try:
        body = await request.json()
    except Exception:
        body = {}
    
    new_name = body.get("name")
    is_manually_mapped = body.get("manually_mapped", False)
    
    if not new_name or not new_name.strip():
        return JSONResponse({"error": "name required"}, status_code=400)
    
    new_name = new_name.strip()
    
    with db_tx() as conn:
        _ensure_artist_schema(conn)
        row = conn.execute("SELECT name, ytmusic_name FROM artists WHERE spotify_id=?", (spotify_id,)).fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        
        updates = {}
        
        # If the new name differs from current, update the name
        if new_name != row["name"]:
            updates["name"] = new_name
        
        # Mark as manually mapped on YT Music if requested
        if is_manually_mapped:
            updates["ytmusic_status"] = "manually_mapped"
            updates["ytmusic_name"] = new_name
            updates["ytmusic_notes"] = "Manually corrected by user"
        
        if updates:
            update_sql = "UPDATE artists SET " + ", ".join(f"{k}=?" for k in updates.keys()) + " WHERE spotify_id=?"
            conn.execute(update_sql, list(updates.values()) + [spotify_id])
    
    return {
        "ok": True, 
        "name": new_name,
        "ytmusic_status": "manually_mapped" if is_manually_mapped else None,
        "message": f"Artist updated {'and marked as manually mapped' if is_manually_mapped else ''}"
    }


def _purge_artist_data(spotify_id: str, artist_name: str, delete_files: bool) -> None:
    """Remove albums/songs (and optionally files) after artist is hidden from the HUD."""
    import re
    import shutil
    from pathlib import Path

    with db() as conn:
        conn.execute("DELETE FROM songs WHERE artist_id=?", (spotify_id,))
        conn.execute("DELETE FROM albums WHERE artist_id=?", (spotify_id,))
    if delete_files:
        music_dir = Path(load_cfg()["music_dir"])

        def clean_name(name: str) -> str:
            return re.sub(r'[\\/*?:"<>|]', "", str(name)).strip().strip(".")

        artist_folder = music_dir / clean_name(artist_name)
        if artist_folder.exists() and artist_folder.is_dir():
            try:
                shutil.rmtree(str(artist_folder))
                _tracks_index_cache["ts"] = 0.0
            except Exception:
                pass


@app.delete("/api/artists/{spotify_id:path}")
async def api_delete_artist(spotify_id: str, delete_files: bool = False):
    _db_ready()
    spotify_id = _decode_artist_id(spotify_id)
    with db_tx() as conn:
        row = conn.execute("SELECT name FROM artists WHERE spotify_id=?", (spotify_id,)).fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, status_code=404)
        artist_name = row["name"]
        conn.execute("UPDATE artists SET active=-1 WHERE spotify_id=?", (spotify_id,))
    asyncio.create_task(
        asyncio.to_thread(_purge_artist_data, spotify_id, artist_name, delete_files)
    )
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
    refresh: bool = False,
):
    if refresh:
        _tracks_index_cache["ts"] = 0.0
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
    "full": ([], "Update library"),
    "download-pending": (["download-pending"], "Download top tracks"),
    "reconcile": (["reconcile"], "Refresh files"),
    "fix-metadata": (["fix-metadata"], "Fix tags & covers"),
    "mark-romanian": (["mark-romanian"], "Mark Romanian artists"),
    "deduplicate": (["deduplicate"], "Deduplicate library"),
    "migrate-structure": (["migrate-structure"], "Migrate folders"),
    # Legacy / advanced (not shown on dashboard)
    "scan": (["scan"], "Scan playlists"),
    "scan-artists": (["scan-artists"], "Scan artist albums"),
    "scan-artists-new": (["scan-artists", "--new-only"], "Scan new artists"),
    "artists-sync": (["artists-sync"], "Sync all artists"),
    "artists-sync-new": (["artists-sync", "--new-only"], "Sync new artists"),
    "prune-caps": (["prune-caps"], "Enforce caps only"),
    "cookies-check": (["cookies-check"], "Test YouTube cookies"),
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
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"/>
<meta name="theme-color" content="#000000"/>
<meta name="apple-mobile-web-app-capable" content="yes"/>
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"/>
<title>Musicadet</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22 font-family=%22serif%22 font-style=%22italic%22 font-weight=%22bold%22>M</text></svg>">
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<style>
  :root {
    --bg: #000000;
    --surface: #0a0a0a;
    --surface-hover: #111111;
    --bg-card: #0a0a0a;
    --border: #262626;
    --border-card: #262626;
    --primary: #ededed;
    --primary-fg: #0a0a0a;
    --success: #22c55e;
    --warning: #eab308;
    --error: #ef4444;
    --txt: #ededed;
    --muted: #888888;
    --radius: 6px;
    --radius-lg: 8px;
  }
  * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
  html { color-scheme: dark; }
  body {
    margin: 0;
    font: 14px/1.5 'Inter', system-ui, -apple-system, sans-serif;
    color: var(--txt);
    background: var(--bg);
    min-height: 100vh;
    min-height: 100dvh;
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
    position: sticky;
    top: 0;
    z-index: 20;
    backdrop-filter: blur(12px);
    background: rgba(0, 0, 0, 0.85);
    border-bottom: 1px solid var(--border);
    padding: 12px 16px;
    padding-top: max(12px, env(safe-area-inset-top));
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: nowrap;
    overflow-x: auto;
    scrollbar-width: none;
    -webkit-overflow-scrolling: touch;
  }
  header::-webkit-scrollbar { display: none; }
  .header-gradient {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 1px;
    background: var(--border-card);
  }
  .logo {
    font-weight: 600;
    font-size: 18px;
    letter-spacing: -0.03em;
    color: var(--txt);
    flex-shrink: 0;
    font-family: 'Inter', system-ui, sans-serif;
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
    box-shadow: none;
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
    background: var(--surface);
    padding: 4px 10px;
    border-radius: 999px;
    border: 1px solid var(--border);
    flex-shrink: 0;
  }
  header nav {
    display: flex;
    gap: 4px;
    margin-left: auto;
    flex: 0 0 auto;
    white-space: nowrap;
  }
  header nav button {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    padding: 6px 12px;
    border-radius: var(--radius);
    cursor: pointer;
    font: 500 13px 'Inter', sans-serif;
    transition: background 0.15s ease, color 0.15s ease, border-color 0.15s ease;
    flex: 0 0 auto;
    touch-action: manipulation;
  }
  header nav button.active {
    color: var(--txt);
    background: var(--surface);
    border-color: var(--border);
  }
  header nav button:hover {
    color: var(--txt);
    background: var(--surface-hover);
  }
  main {
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px 16px;
    padding-bottom: max(20px, env(safe-area-inset-bottom));
  }
  .table-scroll {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    margin: 0 -4px;
    padding: 0 4px;
  }
  .pager {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-top: 14px;
    flex-wrap: wrap;
  }
  .pager .btn { justify-content: center; }
  .toolbar-stack {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    align-items: center;
    margin-bottom: 14px;
  }
  .toolbar-stack > input { flex: 1 1 180px; min-width: 0; }
  .toolbar-stack > .filter-chips { flex: 1 1 100%; }
  .grid {
    display: grid;
    gap: 16px;
  }
  .stats {
    grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
  }
  .card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: var(--radius-lg);
    padding: 20px;
  }
  .stat {
    border-left: 2px solid var(--border);
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
  .stat-label {
    font-size: 11px;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--muted);
    margin-bottom: 8px;
  }
  .stat-main {
    font-size: 32px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    line-height: 1.1;
    color: var(--txt);
  }
  .stat-sub {
    font-size: 13px;
    color: var(--muted);
    margin-top: 8px;
    line-height: 1.4;
    word-break: break-word;
  }
  .artist-cards {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .artist-card {
    border: 1px solid var(--border);
    border-radius: var(--radius-lg);
    background: var(--surface);
    padding: 12px 14px;
  }
  .artist-card.row-picked {
    border-color: #525252;
    background: rgba(255, 255, 255, 0.04);
  }
  .artist-card.row-ro {
    border-left: 2px solid #525252;
  }
  .artist-card-head {
    display: flex;
    align-items: flex-start;
    gap: 8px;
    margin-bottom: 8px;
    font-weight: 600;
    font-size: 15px;
    line-height: 1.3;
  }
  .artist-card-meta {
    font-size: 12px;
    color: var(--muted);
    margin-bottom: 10px;
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-items: center;
  }
  .artist-card-actions {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }
  .artist-card-actions select {
    width: 100%;
    min-width: 0;
  }
  .artist-card-actions .btn-row {
    display: flex;
    gap: 8px;
  }
  .artist-card-actions .btn-row .btn {
    flex: 1;
    justify-content: center;
  }
  .lib-cards {
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .lib-card {
    border: 1px solid var(--border);
    border-radius: var(--radius-lg);
    background: var(--surface);
    padding: 12px 14px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .lib-card-title {
    font-weight: 600;
    font-size: 14px;
    line-height: 1.3;
  }
  .lib-card-artist {
    font-size: 12px;
    color: var(--muted);
  }
  .lib-card-foot {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    flex-wrap: wrap;
  }
  .lib-card-progress {
    font-size: 12px;
    color: var(--muted);
    font-variant-numeric: tabular-nums;
  }
  .song-modal-content {
    max-width: 520px;
    max-height: min(85vh, 640px);
    display: flex;
    flex-direction: column;
    padding: 0;
    overflow: hidden;
  }
  .song-modal-content h2 {
    margin: 0;
    padding: 16px 48px 12px 16px;
    font-size: 16px;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }
  .song-modal-list {
    overflow-y: auto;
    -webkit-overflow-scrolling: touch;
    padding: 8px 0;
    flex: 1;
    min-height: 0;
  }
  .song-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 16px;
    border-bottom: 1px solid var(--border);
    font-size: 13px;
  }
  .song-item:last-child { border-bottom: none; }
  .song-num {
    width: 24px;
    flex-shrink: 0;
    color: var(--muted);
    font-size: 12px;
    text-align: right;
  }
  .song-title {
    flex: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .song-meta {
    flex-shrink: 0;
    display: flex;
    gap: 6px;
    align-items: center;
    font-size: 11px;
    color: var(--muted);
  }
  .btn {
    border: 1px solid var(--primary);
    border-radius: var(--radius);
    padding: 8px 14px;
    font: 500 13px 'Inter', sans-serif;
    cursor: pointer;
    color: var(--primary-fg);
    background: var(--primary);
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
    transition: background 0.15s ease, border-color 0.15s ease;
    touch-action: manipulation;
  }
  .btn:hover {
    background: #d4d4d4;
    border-color: #d4d4d4;
  }
  .btn.ghost,
  .sel-btn,
  .btn-ro {
    background: var(--surface);
    color: var(--txt);
    border: 1px solid var(--border);
  }
  .btn.ghost:hover,
  .sel-btn:hover,
  .btn-ro:hover {
    background: var(--surface-hover);
    border-color: #404040;
  }
  .btn.sm {
    padding: 6px 12px;
    font-size: 12px;
    min-height: 32px;
  }
  .btn-ro {
    padding: 6px 12px;
    font: 500 12px 'Inter', sans-serif;
    letter-spacing: 0.04em;
  }
  .filter-chips {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    align-items: center;
  }
  .filter-chips .chip {
    border: 1px solid var(--border);
    background: var(--surface);
    color: var(--muted);
    padding: 6px 12px;
    border-radius: var(--radius);
    font: 500 12px 'Inter', sans-serif;
    cursor: pointer;
    transition: background 0.15s ease, color 0.15s ease, border-color 0.15s ease;
  }
  .filter-chips .chip:hover {
    color: var(--txt);
    background: var(--surface-hover);
  }
  .filter-chips .chip.active,
  .filter-chips .chip.chip-ro.active {
    color: var(--txt);
    background: var(--surface-hover);
    border-color: #525252;
  }
  .ro-toggle {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 28px;
    height: 24px;
    margin-right: 8px;
    padding: 0 6px;
    border-radius: var(--radius);
    border: 1px solid var(--border);
    background: var(--surface);
    color: var(--muted);
    font: 600 10px 'Inter', sans-serif;
    letter-spacing: 0.04em;
    cursor: pointer;
    vertical-align: middle;
    transition: background 0.15s ease, border-color 0.15s ease, color 0.15s ease;
  }
  .ro-toggle:hover {
    background: var(--surface-hover);
    color: var(--txt);
  }
  .ro-toggle.on {
    background: var(--surface-hover);
    border-color: #525252;
    color: var(--txt);
  }
  tr.row-ro td:first-child {
    border-left: 2px solid #525252;
  }
  .btn.danger {
    background: var(--surface);
    color: var(--error);
    border: 1px solid var(--border);
  }
  .btn.danger:hover {
    background: var(--surface-hover);
    border-color: #404040;
  }
  .row {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    align-items: center;
  }
  input, select, textarea, .toolbar-select {
    background: var(--surface);
    border: 1px solid var(--border);
    color: var(--txt);
    padding: 8px 12px;
    border-radius: var(--radius);
    font: 400 13px 'Inter', sans-serif;
    outline: none;
    transition: border-color 0.15s ease, background 0.15s ease;
  }
  input:focus, select:focus, textarea:focus, .toolbar-select:focus {
    border-color: #525252;
    background: var(--surface-hover);
  }
  select, .toolbar-select {
    color-scheme: dark;
    cursor: pointer;
    appearance: none;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%23888' stroke-width='2'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E");
    background-repeat: no-repeat;
    background-position: right 10px center;
    padding-right: 28px;
  }
  select option,
  select optgroup {
    background-color: #0a0a0a;
    color: var(--txt);
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
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 10px;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: .04em;
    border: 1px solid var(--border);
    background: var(--surface);
  }
  .pill.on { color: var(--success); border-color: #166534; }
  .pill.off { color: var(--muted); }
  .pill.done { color: var(--txt); }
  .pill.pend { color: var(--muted); border-color: #6b7280; }
  .pill.warn { color: #ef4444; border-color: #b91c1c; }
  .pill.unknown { color: var(--muted); border-color: #6b7280; }
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
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: var(--radius-lg);
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
  tr.row-flash td,
  .artist-card.row-flash {
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
  .db-summary {
    font-size: 11px;
    color: var(--muted);
    margin-bottom: 12px;
    letter-spacing: 0.02em;
  }
  .db-summary b { color: var(--txt); font-weight: 600; }
  .sel-bar {
    display: flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
    margin-bottom: 10px;
    padding: 8px 10px;
    border-radius: var(--radius);
    background: var(--surface);
    border: 1px solid var(--border);
  }
  .sel-bar.hide { display: none; }
  .sel-count {
    font-size: 11px;
    font-weight: 700;
    color: var(--muted);
    margin-right: 4px;
    min-width: 14px;
  }
  .sel-btn {
    padding: 6px 12px;
    font: 500 12px 'Inter', sans-serif;
    cursor: pointer;
    line-height: 1.2;
    touch-action: manipulation;
  }
  .sel-btn.ro { color: var(--txt); }
  .sel-btn.ghost {
    color: var(--muted);
    background: transparent;
    border-color: transparent;
  }
  .sel-btn.ghost:hover { background: var(--surface-hover); border-color: var(--border); }
  .sel-limit {
    min-width: 52px;
    width: auto;
    font-size: 12px;
    padding: 6px 28px 6px 10px;
  }
  .btn-select-mode.active {
    color: var(--txt);
    background: var(--surface-hover);
    border-color: #525252;
  }
  .chk-col { width: 26px; text-align: center; padding-left: 8px !important; }
  input.row-chk {
    width: 14px;
    height: 14px;
    min-width: 0;
    padding: 0;
    margin: 0;
    accent-color: #ededed;
    cursor: pointer;
  }
  tr.row-picked td { background: rgba(255, 255, 255, 0.03); }
  .toolbar-select {
    min-width: 120px;
    width: auto;
    font-size: 12px;
    line-height: 1.25;
    box-sizing: border-box;
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
  .actions-primary {
    grid-template-columns: 1fr;
    gap: 12px;
  }
  @media (min-width: 720px) {
    .actions-primary { grid-template-columns: repeat(3, 1fr); }
  }
  .action-tile {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    text-align: left;
    gap: 6px;
    min-height: 72px;
    height: auto;
    padding: 14px 16px;
    line-height: 1.35;
  }
  .action-tile svg { flex-shrink: 0; margin-bottom: 2px; }
  .action-title { font-weight: 600; font-size: 14px; }
  .action-desc {
    font-size: 11px;
    font-weight: 400;
    color: var(--muted);
    line-height: 1.4;
  }
  .btn.action-tile:not(.ghost) .action-desc { color: rgba(255,255,255,0.75); }
  .tools-panel {
    margin-top: 16px;
    border: 1px solid var(--border-card);
    border-radius: var(--radius);
    padding: 12px 14px;
    background: rgba(0,0,0,0.15);
  }
  .tools-panel summary {
    cursor: pointer;
    font-size: 13px;
    font-weight: 500;
    color: var(--muted);
    user-select: none;
  }
  .tools-panel .actions { margin-top: 12px; }
  .dash-intro { margin: 0 0 16px; max-width: 52rem; }
  .toast {
    position: fixed;
    bottom: 20px;
    right: 20px;
    background: var(--txt);
    color: var(--primary-fg);
    padding: 10px 16px;
    border-radius: var(--radius);
    font-weight: 500;
    font-size: 13px;
    z-index: 50;
    opacity: 0;
    transform: translateY(8px);
    transition: opacity 0.2s ease, transform 0.2s ease;
    border: 1px solid var(--border);
    box-shadow: 0 8px 30px rgba(0, 0, 0, 0.4);
  }
  .toast.show { opacity: 1; transform: none; }
  .toast.err {
    background: #450a0a;
    color: #fecaca;
    border: 1px solid var(--error);
    max-width: min(420px, calc(100vw - 40px));
    white-space: pre-wrap;
    word-break: break-word;
  }
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
  @media (max-width: 640px) {
    .logo {
      font-size: 0;
      width: 28px;
    }
    .logo::before {
      content: 'M';
      font-size: 16px;
      font-weight: 600;
    }
    header {
      padding: 10px 12px;
      padding-top: max(10px, env(safe-area-inset-top));
    }
    header nav button {
      padding: 8px 12px;
      min-height: 36px;
      font-size: 12px;
    }
    main {
      padding: 12px;
      padding-bottom: max(16px, env(safe-area-inset-bottom));
    }
    .card { padding: 16px; }
    .actions {
      grid-template-columns: 1fr;
    }
    .actions .btn,
    .btn.sm,
    .sel-btn,
    .filter-chips .chip {
      min-height: 40px;
    }
    th, td {
      padding: 12px 10px;
      font-size: 13px;
    }
    .toolbar-stack > input,
    .toolbar-stack .toolbar-select {
      flex: 1 1 100%;
      min-height: 40px;
    }
    .toast {
      left: 12px;
      right: 12px;
      bottom: max(16px, env(safe-area-inset-bottom));
      text-align: center;
      max-width: none;
    }
    .modal-content {
      width: 94%;
      max-height: 90vh;
      overflow-y: auto;
    }
    .modal-body { flex-direction: column; }
    #tmCover { width: 100% !important; height: auto !important; aspect-ratio: 1; }
    .row {
      flex-direction: column;
      align-items: stretch;
    }
    .row > * {
      width: 100% !important;
      flex: none !important;
    }
    .pager .btn { min-height: 40px; min-width: 72px; }
    .grid.stats {
      grid-template-columns: 1fr;
      gap: 10px;
    }
    .stat-main { font-size: 26px; }
    .stat-sub { font-size: 12px; }
    .card.stat { padding: 14px 16px !important; }
    .song-modal-content {
      width: 100%;
      max-width: none;
      max-height: 92vh;
      margin: 0;
      border-radius: var(--radius-lg) var(--radius-lg) 0 0;
      align-self: flex-end;
    }
    #songModal.modal {
      align-items: flex-end;
      padding: 0;
    }
  }

  /* Visual Pipeline Flow Styles */
  .pipeline-container {
    background: linear-gradient(135deg, rgba(13, 13, 18, 0.45) 0%, rgba(20, 20, 29, 0.45) 100%);
    backdrop-filter: blur(12px);
    border: 1px solid rgba(255, 255, 255, 0.05);
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 20px;
    box-shadow: 0 4px 30px rgba(0, 0, 0, 0.2);
  }
  .pipeline-flow {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    overflow-x: auto;
    scrollbar-width: none;
    -webkit-overflow-scrolling: touch;
    padding: 4px 2px;
  }
  .pipeline-flow::-webkit-scrollbar { display: none; }
  .pipeline-step {
    flex: 1;
    min-width: 130px;
    background: rgba(255, 255, 255, 0.02);
    border: 1px solid rgba(255, 255, 255, 0.04);
    border-radius: 10px;
    padding: 14px 12px;
    text-align: center;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    position: relative;
  }
  .pipeline-step.active-path {
    background: rgba(99, 102, 241, 0.04);
    border-color: rgba(99, 102, 241, 0.35);
    box-shadow: 0 0 15px rgba(99, 102, 241, 0.1);
  }
  .pipeline-step.running {
    background: rgba(16, 185, 129, 0.06);
    border-color: var(--success);
    box-shadow: 0 0 20px rgba(16, 185, 129, 0.2);
    animation: stepPulse 1.8s infinite alternate;
  }
  @keyframes stepPulse {
    0% { transform: translateY(0) scale(0.99); opacity: 0.95; }
    100% { transform: translateY(-2px) scale(1.01); opacity: 1; }
  }
  .pipeline-arrow {
    color: rgba(255, 255, 255, 0.15);
    font-size: 20px;
    user-select: none;
    flex-shrink: 0;
    font-weight: 300;
  }
  .step-icon {
    width: 26px;
    height: 26px;
    border-radius: 50%;
    background: rgba(255, 255, 255, 0.08);
    color: var(--muted);
    display: flex;
    align-items: center;
    justify-content: center;
    margin: 0 auto 10px;
    font-size: 11px;
    font-weight: 700;
    transition: all 0.3s ease;
  }
  .pipeline-step.active-path .step-icon {
    background: rgba(99, 102, 241, 0.2);
    color: #818cf8;
    box-shadow: 0 0 8px rgba(99, 102, 241, 0.25);
  }
  .pipeline-step.running .step-icon {
    background: var(--success);
    color: #000;
    box-shadow: 0 0 10px rgba(16, 185, 129, 0.4);
  }
  .step-label {
    font-weight: 600;
    font-size: 13px;
    color: var(--txt);
    margin-bottom: 4px;
    letter-spacing: -0.01em;
  }
  .step-desc {
    font-size: 10px;
    color: var(--muted);
    line-height: 1.4;
  }
  
  /* Modern styling overrides */
  @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');
  :root {
    --bg: #06060a;
    --surface: #0e0e15;
    --surface-hover: #161622;
    --bg-card: #0e0e15;
    --border: rgba(255, 255, 255, 0.06);
    --border-card: rgba(255, 255, 255, 0.06);
    --primary: #6366f1;
    --primary-fg: #ffffff;
    --txt: #f3f4f6;
    --muted: #9ca3af;
  }
  body {
    font-family: 'Outfit', 'Inter', system-ui, -apple-system, sans-serif;
  }
  body::before {
    content: "";
    position: fixed;
    top: -20%;
    left: 10%;
    width: 60%;
    height: 60%;
    background: radial-gradient(circle, rgba(99, 102, 241, 0.05) 0%, rgba(0, 0, 0, 0) 70%);
    pointer-events: none;
    z-index: -1;
  }
  .card {
    background: linear-gradient(135deg, rgba(14, 14, 21, 0.7) 0%, rgba(22, 22, 34, 0.7) 100%);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255, 255, 255, 0.06);
    border-radius: 12px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
  }
  .btn {
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(99, 102, 241, 0.25);
    font-family: 'Outfit', sans-serif;
  }
  .btn:hover {
    background: #4f46e5;
    border-color: #4f46e5;
    box-shadow: 0 4px 12px rgba(99, 102, 241, 0.4);
  }
  .btn.ghost {
    box-shadow: none;
  }
  .btn.ghost:hover {
    background: var(--surface-hover);
    border-color: rgba(99, 102, 241, 0.4);
    box-shadow: 0 2px 8px rgba(99, 102, 241, 0.1);
  }
  .btn-select-mode.active, header nav button.active {
    border-color: rgba(99, 102, 241, 0.6) !important;
    background: rgba(99, 102, 241, 0.1) !important;
    color: #a5b4fc !important;
  }
  header {
    border-bottom: 1px solid rgba(255, 255, 255, 0.06);
  }
  @media (max-width: 720px) {
    .pipeline-flow {
      flex-direction: column;
      align-items: stretch;
      gap: 12px;
    }
    .pipeline-arrow {
      transform: rotate(90deg);
      margin: 2px auto;
    }
    .pipeline-step {
      min-height: auto;
    }
  }
</style>
</head>
<body>
<header>
  <div class="header-gradient"></div>
  <div class="logo">MusicaDet</div>
  <div class="status"><span id="dot" class="dot"></span><span id="statusText">idle</span></div>
  <nav aria-label="Sections">
    <button type="button" data-tab="dashboard" class="active">Dashboard</button>
    <button type="button" data-tab="library">Library</button>
    <button type="button" data-tab="artists">Artists</button>
    <button type="button" data-tab="playlists">Playlists</button>
    <button type="button" data-tab="tracks">Files</button>
    <button type="button" data-tab="settings">Settings</button>
    <button type="button" data-tab="console">Console</button>
  </nav>
</header>
<main>
  <section id="dashboard">
    <div class="grid stats" id="statCards"></div>
    <div class="gradient-sep"></div>
    
    <!-- Visual Pipeline Flow -->
    <div class="pipeline-container">
      <h3 style="margin-top:0; font-size:12px; font-weight:600; text-transform:uppercase; letter-spacing:0.06em; color:var(--muted); margin-bottom:16px;">Pipeline Execuție</h3>
      <div class="pipeline-flow">
        <div class="pipeline-step" id="step-scan">
          <div class="step-icon">1</div>
          <div class="step-label">Scanare Playlist</div>
          <div class="step-desc">Scanare playlist-uri Spotify</div>
        </div>
        <div class="pipeline-arrow">→</div>
        <div class="pipeline-step" id="step-catalog">
          <div class="step-icon">2</div>
          <div class="step-label">Scanare Catalog</div>
          <div class="step-desc">Descoperire albume și piese</div>
        </div>
        <div class="pipeline-arrow">→</div>
        <div class="pipeline-step" id="step-match">
          <div class="step-icon">3</div>
          <div class="step-label">Potrivire Fișiere</div>
          <div class="step-desc">Reconciliere fișiere disc ↔ DB</div>
        </div>
        <div class="pipeline-arrow">→</div>
        <div class="pipeline-step" id="step-trim">
          <div class="step-icon">4</div>
          <div class="step-label">Limitare &amp; Curățare</div>
          <div class="step-desc">Eliminare piese peste limită</div>
        </div>
        <div class="pipeline-arrow">→</div>
        <div class="pipeline-step" id="step-download">
          <div class="step-icon">5</div>
          <div class="step-label">Descărcare</div>
          <div class="step-desc">Descărcare piese lipsă</div>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Library</h2>
      <p class="hint dash-intro">Set each artist&apos;s <strong>Limit</strong> on the Artists tab (e.g. 10). The app keeps only their <strong>top viewed</strong> tracks on YouTube Music and deletes the rest automatically when you use the buttons below.</p>
      <div class="actions actions-primary">
        <button type="button" class="btn action-tile" onclick="action('full')"
                onmouseover="highlightPipeline(['scan', 'catalog', 'match', 'trim', 'download'])"
                onmouseout="clearPipelineHighlight()">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"></polygon></svg>
          <span class="action-title">Update everything</span>
          <span class="action-desc">Scan playlists &amp; catalogs, trim to caps, download missing top tracks</span>
        </button>
        <button type="button" class="btn ghost action-tile" onclick="action('download-pending')"
                onmouseover="highlightPipeline(['trim', 'download'])"
                onmouseout="clearPipelineHighlight()">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>
          <span class="action-title">Download top tracks</span>
          <span class="action-desc">Remove extras over limit, then download only top viewed (per artist cap)</span>
        </button>
        <button type="button" class="btn ghost action-tile" onclick="action('reconcile')"
                onmouseover="highlightPipeline(['match', 'trim'])"
                onmouseout="clearPipelineHighlight()">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path></svg>
          <span class="action-title">Refresh files</span>
          <span class="action-desc">Match disk ↔ database, then delete tracks over limit (not top viewed)</span>
        </button>
      </div>
      <details class="tools-panel">
        <summary>More tools</summary>
        <div class="actions">
          <button type="button" class="btn ghost" onclick="action('fix-metadata')"
                  onmouseover="highlightPipeline(['match'])"
                  onmouseout="clearPipelineHighlight()">Fix tags &amp; covers</button>
          <button type="button" class="btn ghost" onclick="action('mark-romanian')">Mark Romanian artists</button>
          <button type="button" class="btn ghost" onclick="action('migrate-structure')"
                  onmouseover="highlightPipeline(['match'])"
                  onmouseout="clearPipelineHighlight()">Migrate folder layout</button>
          <button type="button" class="btn ghost danger" onclick="if(confirm('Deduplicate all artists and tracks?'))action('deduplicate')">Deduplicate library</button>
        </div>
      </details>
      <div class="gradient-sep"></div>
      <div class="row" style="align-items:center;gap:10px;flex-wrap:wrap">
        <button type="button" class="btn danger sm" onclick="stop()">Stop running job</button>
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
      <div class="toolbar-stack">
        <select id="libArtist" class="toolbar-select" onchange="loadLibrary()"><option value="">All artists</option></select>
        <select id="libStatus" class="toolbar-select" onchange="loadLibrary()">
          <option value="all">All albums</option>
          <option value="pending">Incomplete</option>
          <option value="complete">Complete</option>
        </select>
        <button type="button" class="btn ghost sm" onclick="loadLibrary()">Refresh</button>
      </div>
      <div id="libCards" class="lib-cards hide"></div>
      <div class="table-scroll lib-table-wrap">
      <table class="table lib-table"><thead><tr><th>Artist</th><th>Album</th><th>Progress</th><th>Action</th></tr></thead>
      <tbody id="libRows"></tbody></table>
      </div>
      <div class="pager">
        <button type="button" class="btn ghost sm" onclick="libPrev()">← Prev</button>
        <span id="libPageInfo" class="muted">Page 1</span>
        <button type="button" class="btn ghost sm" onclick="libNext()">Next →</button>
      </div>
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
      <div id="dbSummary" class="db-summary muted">Loading database stats…</div>
      <div class="toolbar-stack">
        <input id="artistSearch" placeholder="Search artists…" oninput="debouncedLoadArtists()"/>
        <div class="filter-chips" id="artistFilterChips" role="group" aria-label="Filter artists">
          <button type="button" class="chip active" data-value="all">All</button>
          <button type="button" class="chip chip-ro" data-value="romanian">Romanian</button>
          <button type="button" class="chip" data-value="international">International</button>
          <button type="button" class="chip" data-value="ytok">YT OK</button>
          <button type="button" class="chip" data-value="ytunknown">YT unknown</button>
          <button type="button" class="chip" data-value="ytmissing">YT missing</button>
          <button type="button" class="chip" data-value="active">Active</button>
          <button type="button" class="chip" data-value="pending">Pending</button>
          <button type="button" class="chip" data-value="synced">Synced</button>
          <button type="button" class="chip" data-value="disabled">Disabled</button>
        </div>
        <select id="artistSort" class="toolbar-select" onchange="loadArtists()" title="Sort">
          <option value="name">A–Z</option>
          <option value="songs_desc">Songs ↓</option>
          <option value="songs_asc">Songs ↑</option>
        </select>
        <button type="button" id="btnSelectMode" class="btn ghost sm btn-select-mode" onclick="toggleSelectMode()" title="Multi-select">Select</button>
        <button type="button" class="btn ghost sm" onclick="loadArtists()">Refresh</button>
        <button type="button" class="btn-ro" onclick="action('mark-romanian')" title="Detect Romanian">RO</button>
      </div>
      <div id="selectionBar" class="sel-bar hide">
        <span id="selCount" class="sel-count">0</span>
        <button type="button" class="sel-btn" onclick="bulkSel('enable')">On</button>
        <button type="button" class="sel-btn" onclick="bulkSel('disable')">Off</button>
        <button type="button" class="sel-btn ro" onclick="bulkSel('ro_on')">RO+</button>
        <button type="button" class="sel-btn" onclick="bulkSel('ro_off')">RO−</button>
        <select id="bulkLimit" class="toolbar-select sel-limit" title="Limit">
          <option value="">—</option>
          <option value="10">10</option>
          <option value="50">50</option>
          <option value="100">100</option>
          <option value="150">150</option>
          <option value="0">∞</option>
        </select>
        <button type="button" class="sel-btn" onclick="bulkSel('limit')">Lim</button>
        <button type="button" class="sel-btn ghost" onclick="pickPage()">Page</button>
        <button type="button" class="sel-btn ghost" onclick="clearSel()" title="Clear">✕</button>
      </div>
      <div id="artistCards" class="artist-cards hide"></div>
      <div class="table-scroll artist-table-wrap">
      <table class="table artists-table"><thead><tr>
        <th class="chk-col sel-col hide"><input type="checkbox" class="row-chk" id="artPickPage" title="Page" onchange="pickPage(this.checked)"/></th>
        <th>Artist</th><th>Albums</th><th title="Downloaded / limit (top viewed cap)">Songs</th><th>Status</th><th>Actions</th></tr></thead>
      <tbody id="artistRows"></tbody></table>
      </div>
      <div class="pager">
        <button type="button" class="btn ghost sm" onclick="artPrev()">← Prev</button>
        <span id="artPageInfo" class="muted">Page 1</span>
        <button type="button" class="btn ghost sm" onclick="artNext()">Next →</button>
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
      <div class="table-scroll">
      <table class="table"><thead><tr><th>Playlist</th><th>Status</th><th>Last scan</th><th>Actions</th></tr></thead>
      <tbody id="playlistRows"></tbody></table>
      </div>
    </div>
  </section>

  <section id="tracks" class="hide">
    <div class="card">
      <div class="toolbar-stack">
        <input id="trackSearch" placeholder="Search files…" oninput="debouncedLoadTracks()"/>
        <button type="button" class="btn ghost sm" onclick="loadTracks(true)">Refresh</button>
        <button type="button" class="btn ghost sm" onclick="loadTracks(true,true)" title="Rescan music folder">Rescan</button>
      </div>
      <div class="table-scroll">
      <table class="table"><thead><tr><th>Artist</th><th>Album</th><th>File</th><th></th></tr></thead>
      <tbody id="trackRows"></tbody></table>
      </div>
      <div class="pager">
        <button type="button" class="btn ghost sm" onclick="trackPrev()">← Prev</button>
        <span id="trackPageInfo" class="muted">Page 1</span>
        <button type="button" class="btn ghost sm" onclick="trackNext()">Next →</button>
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
      <div class="field"><label>Output template</label><input id="cfgTemplate" placeholder="{artist}/{album}/{title}.{output-ext}"/></div>
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

<div id="songModal" class="modal hide" onclick="if(event.target===this) closeSongModal()">
  <div class="modal-content song-modal-content" onclick="event.stopPropagation()">
    <button type="button" class="modal-close" onclick="closeSongModal()">×</button>
    <h2 id="songModalTitle">Songs</h2>
    <div class="song-modal-list" id="songModalList"></div>
  </div>
</div>

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
const TRACK_PAGE=100;
const LIB_PAGE=80;
const ART_PAGE=100;
let autoscroll=true;
let artistsLoadGen=0, libLoadGen=0, tracksLoadGen=0;
let artistTotal=0, libTotal=0, trackTotal=0;
let trackPage=0;
let curPl=[];
let selectMode=false;
let selectedArtists=new Set();
function artistUrl(id,action){return `/api/artists/${encodeURIComponent(id)}/${action}`;}
let _toastT;
function toast(m,isErr){
  const t=$('#toast');
  t.textContent=m;
  t.classList.toggle('err',!!isErr);
  t.classList.add('show');
  clearTimeout(_toastT);
  _toastT=setTimeout(()=>t.classList.remove('show','err'),isErr?7000:2200);
  if(isErr)console.error('[MusicaDet]',m);
}
function toastErr(m){toast(m,true);}
function debounce(fn,ms){let t;return(...a)=>{clearTimeout(t);t=setTimeout(()=>fn(...a),ms);};}
const mobileMq=window.matchMedia('(max-width:640px)');
function isMobile(){return mobileMq.matches;}
function syncMobileLists(){
  const m=isMobile();
  $('#artistCards')?.classList.toggle('hide',!m);
  document.querySelector('.artist-table-wrap')?.classList.toggle('hide',m);
  $('#libCards')?.classList.toggle('hide',!m);
  document.querySelector('.lib-table-wrap')?.classList.toggle('hide',m);
}
mobileMq.addEventListener('change',()=>{syncMobileLists();renderArtists();renderLibrary();});
syncMobileLists();
function closeSongModal(){$('#songModal')?.classList.add('hide');}
function skeletonRows(cols,n=10){return Array.from({length:n},()=>'<tr class="skeleton-row">'+Array.from({length:cols},()=>'<td><span class="skel"></span></td>').join('')+'</tr>').join('');}
async function api(path,opts={},signal){
  const r=await fetch(path,{...opts,signal});
  let text='';
  try{text=await r.text();}catch(_){}
  let data={};
  if(text){
    try{data=JSON.parse(text);}catch(_){
      if(!r.ok)throw new Error(text.trim().slice(0,240)||('HTTP '+r.status));
    }
  }
  if(!r.ok){
    let msg=data.error||data.message;
    if(!msg&&data.detail){
      msg=Array.isArray(data.detail)
        ?data.detail.map(d=>d.msg||(typeof d==='string'?d:JSON.stringify(d))).join('; ')
        :data.detail;
    }
    if(!msg&&text)msg=text.trim().slice(0,240);
    throw new Error(msg||('HTTP '+r.status));
  }
  return data;
}
function esc(s){return (s==null?'':s).toString().replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}
function artistById(id){return curArt.find(a=>a.spotify_id===id);}
function artistYtMusicStatusHtml(r){
  const status = (r.ytmusic_status||'unknown').toLowerCase();
  const name = r.ytmusic_name || r.name || '';
  const notes = r.ytmusic_notes ? ` — ${r.ytmusic_notes}` : '';
  if(status==='found'){
    return `<span class="pill done" title="YT Music matched: ${esc(name)}${esc(notes)}">YT OK</span>`;
  }
  if(status==='manually_mapped'){
    return `<span class="pill on" title="Manually mapped to ${esc(name)}${esc(notes)}">YT fixed</span>`;
  }
  if(status==='not_found'){
    return `<span class="pill warn" title="YT Music not found${esc(notes)}">YT missing</span>`;
  }
  return `<span class="pill unknown" title="YT Music unknown${esc(notes)}">YT unknown</span>`;
}
function artistNeedsYtFix(r){
  const status = (r.ytmusic_status||'unknown').toLowerCase();
  return status==='not_found' || status==='unknown';
}
function flashArtistRow(id){
  const sel=`[data-aid="${CSS.escape(id)}"]`;
  document.querySelectorAll(`tr${sel}, .artist-card${sel}`).forEach(el=>{
    el.classList.remove('row-flash');void el.offsetWidth;el.classList.add('row-flash');
  });
}
document.querySelectorAll('header nav button').forEach(b=>b.onclick=()=>{
  document.querySelectorAll('header nav button').forEach(x=>x.classList.remove('active'));
  b.classList.add('active');
  b.scrollIntoView({inline:'center',block:'nearest',behavior:'smooth'});
  ['dashboard','library','artists','playlists','tracks','settings','console'].forEach(id=>$('#'+id).classList.add('hide'));
  $('#'+b.dataset.tab).classList.remove('hide');
  if(b.dataset.tab==='artists'){loadDbSummary();loadArtists();}
  if(b.dataset.tab==='playlists')loadPlaylists();
  if(b.dataset.tab==='tracks')loadTracks();
  if(b.dataset.tab==='library'){loadLibArtists();loadLibrary();}
  if(b.dataset.tab==='settings')loadSettings();
});
async function loadStats(){
  let s;
  try{s=await api('/api/stats');}catch(e){toastErr('Dashboard: '+(e.message||e));return;}
  const target=s.songs_target??(s.songs_downloaded+s.songs_pending);
  const songsSub=`${s.songs_downloaded} / ${target} downloaded`+
    (s.songs_catalog&&s.songs_catalog>target?` · ${s.songs_pending} to fetch`:'');
  const cards = [
    { title: 'Artists', main: s.artists_total, sub: `${s.artists_synced} / ${s.artists_total} synced` },
    { title: 'Albums', main: s.albums_total, sub: `${s.albums_downloaded} / ${s.albums_total} fully downloaded` },
    { title: 'Songs', main: target, sub: songsSub }
  ];
  $('#statCards').innerHTML=cards.map(c=>`
    <div class="card stat">
      <div class="stat-label">${c.title}</div>
      <div class="stat-main">${c.main}</div>
      <div class="stat-sub">${c.sub}</div>
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
  $('#cfgTemplate').value=c.output_template||'{artist}/{album}/{title}.{output-ext}';
  $('#cfgLyrics').value=(c.lyrics_providers||[]).join(',');
  $('#cfgPlTimeout').value=c.playlist_save_timeout||600;
  $('#cfgPlRetries').value=c.playlist_save_retries||3;
  $('#cfgMaxDl').value=c.max_downloads_per_artist||0;
  $('#cfgLrc').checked=!!c.generate_lrc;
  $('#cfgThreads').value=c.threads||4;
}
async function saveSettings(){
  try{
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
  if(r.error){toastErr(r.error);return;}
  toast('Settings saved');
  loadStats();
  }catch(e){toastErr(e.message||'Save settings failed');}
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
async function loadLibrary(reset=true){
  if(reset)libPage=0;
  const gen=++libLoadGen;
  if(isMobile())$('#libCards').innerHTML='<div class="muted" style="padding:12px">Loading…</div>';
  else $('#libRows').innerHTML=skeletonRows(4);
  $('#libPageInfo').textContent='…';
  try{
    const off=libPage*LIB_PAGE;
    const page=await api(`${libBaseUrl()}&offset=${off}&limit=${LIB_PAGE}`);
    if(gen!==libLoadGen)return;
    curLib=page.items||[];
    libTotal=page.total??0;
    renderLibrary();
  }catch(e){
    if(gen===libLoadGen){
      $('#libRows').innerHTML='<tr><td colspan=4 class="muted">Load failed.</td></tr>';
      $('#libCards').innerHTML='<div class="muted" style="padding:12px">Load failed.</div>';
      toastErr('Library: '+(e.message||e));
    }
  }
}
function libPrev(){if(libPage>0){libPage--;loadLibrary(false);}}
function libNext(){if((libPage+1)*LIB_PAGE<libTotal){libPage++;loadLibrary(false);}}
function renderLibrary(){
  const pages=Math.max(1,Math.ceil(libTotal/LIB_PAGE));
  $('#libPageInfo').textContent=`${libPage+1} / ${pages} · ${libTotal} albums`;
  const empty='<tr><td colspan=4 class="muted">No albums yet — run Scan Albums.</td></tr>';
  const emptyCards='<div class="muted" style="padding:12px">No albums yet — run Scan Albums.</div>';
  const rows=curLib.map(r=>{
    const pct=r.track_count?Math.round(100*r.downloaded_count/r.track_count):0;
    const bar=`<div style="height:4px;background:rgba(255,255,255,.08);border-radius:99px;overflow:hidden;flex:1;max-width:120px"><div style="height:100%;width:${pct}%;background:#ededed;border-radius:99px"></div></div>`;
    const pill=r.downloaded_count>=r.track_count&&r.track_count>0?'<span class="pill done">done</span>':'<span class="pill pend">'+pct+'%</span>';
    const prog=`${r.downloaded_count}/${r.track_count}`;
  return {r,pct,bar,pill,prog};
  });
  $('#libRows').innerHTML=rows.length?rows.map(({r,bar,pill,prog})=>`<tr><td>${esc(r.artist_name)}</td><td>${esc(r.name)}</td><td class="progress">${prog} ${bar} ${pill}</td>
      <td><button type="button" class="btn ghost sm" data-album="${esc(r.spotify_id)}" data-album-name="${esc(r.name)}">Songs</button></td></tr>`).join(''):empty;
  $('#libCards').innerHTML=rows.length?rows.map(({r,pill,prog})=>`<div class="lib-card">
      <div class="lib-card-artist">${esc(r.artist_name)}</div>
      <div class="lib-card-title">${esc(r.name)}</div>
      <div class="lib-card-foot">
        <span class="lib-card-progress">${prog} ${pill}</span>
        <button type="button" class="btn ghost sm" data-album="${esc(r.spotify_id)}" data-album-name="${esc(r.name)}">Songs</button>
      </div>
    </div>`).join(''):emptyCards;
  syncMobileLists();
}
async function showSongs(albumId,albumName){
  $('#songModalTitle').textContent=albumName;
  $('#songModalList').innerHTML='<div class="muted" style="padding:16px">Loading…</div>';
  $('#songModal').classList.remove('hide');
  let res;
  try{res=await api(`/api/songs?album_id=${encodeURIComponent(albumId)}`);}
  catch(e){$('#songModalList').innerHTML='<div class="muted" style="padding:16px">Load failed.</div>';toastErr(e.message||'Songs failed');return;}
  const rows=Array.isArray(res)?res:(res.items||[]);
  const meta=v=>v?'<span title="Cover">Cv</span>':'';
  const metaL=v=>v?'<span title="Lyrics">Lr</span>':'';
  $('#songModalList').innerHTML=rows.length?rows.map(r=>{
    const st=r.status==='downloaded'?'<span class="pill done">ok</span>':'<span class="pill pend">'+r.status+'</span>';
    return `<div class="song-item"><span class="song-num">${r.track_number||''}</span><span class="song-title">${esc(r.title)}</span><span class="song-meta">${st}${meta(r.has_cover)}${metaL(r.has_lyrics)}</span></div>`;
  }).join(''):'<div class="muted" style="padding:16px">No songs.</div>';
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
  const sort=encodeURIComponent($('#artistSort')?.value||'name');
  return `/api/artists?q=${q}&status=${artistFilter}&sort=${sort}`;
}
async function loadDbSummary(){
  try{
    const s=await api('/api/db/summary');
    const songs=`${s.songs_downloaded}/${s.songs_total} songs`+
      (s.songs_catalog&&s.songs_catalog>s.songs_total?` (${s.songs_catalog} in catalog)`:'');
    $('#dbSummary').innerHTML=`<b>${s.artists_active}</b> on · <b>${s.artists_pending_sync}</b> pending sync · ${songs} · <b>${s.artists_romanian}</b> ro · <b>${s.albums_incomplete}</b> albums incomplete`;
  }catch(e){$('#dbSummary').textContent='';toastErr('Summary: '+(e.message||e));}
}
function syncSelBar(){
  const n=selectedArtists.size;
  $('#selCount').textContent=n||'';
  $('#selectionBar').classList.toggle('hide',!selectMode&&!n);
  document.querySelectorAll('.sel-col').forEach(el=>el.classList.toggle('hide',!selectMode));
}
function toggleSelectMode(){
  selectMode=!selectMode;
  $('#btnSelectMode').classList.toggle('active',selectMode);
  if(!selectMode){selectedArtists.clear();const p=$('#artPickPage');if(p)p.checked=false;}
  syncSelBar();renderArtists();
}
function togglePick(id,on){
  if(on)selectedArtists.add(id);else selectedArtists.delete(id);
  syncSelBar();renderArtists();
}
function pickPage(forceOn){
  const allPicked=curArt.length>0&&curArt.every(a=>selectedArtists.has(a.spotify_id));
  const on=typeof forceOn==='boolean'?forceOn:!allPicked;
  curArt.forEach(a=>{
    if(on)selectedArtists.add(a.spotify_id);else selectedArtists.delete(a.spotify_id);
  });
  const p=$('#artPickPage');if(p)p.checked=on;
  syncSelBar();
}
function clearSel(){
  selectedArtists.clear();
  const p=$('#artPickPage');if(p)p.checked=false;
  syncSelBar();renderArtists();
}
async function bulkSel(action){
  if(!selectedArtists.size){toast('Pick artists');return;}
  const body={action,ids:[...selectedArtists]};
  if(action==='limit'){
    const v=$('#bulkLimit').value;
    if(v===''){toast('Limit?');return;}
    body.limit=parseInt(v,10);
  }
  try{
    const r=await api('/api/artists/bulk',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    toast(String(r.updated));
    clearSel();loadDbSummary();loadArtists();
  }catch(e){toastErr(e.message||'Bulk update failed');}
}
async function loadArtists(reset=true){
  if(reset){artPage=0;selectedArtists.clear();}
  const gen=++artistsLoadGen;
  if(isMobile())$('#artistCards').innerHTML='<div class="muted" style="padding:12px">Loading…</div>';
  else $('#artistRows').innerHTML=skeletonRows(5);
  $('#artPageInfo').textContent='…';
  try{
    const off=artPage*ART_PAGE;
    const page=await api(`${artistsBaseUrl()}&offset=${off}&limit=${ART_PAGE}`);
    if(gen!==artistsLoadGen)return;
    curArt=page.items||[];
    artistTotal=page.total??0;
    renderArtists();
  }catch(e){
    if(gen===artistsLoadGen){
      $('#artistRows').innerHTML=`<tr><td colspan="${selectMode?6:5}" class="muted">Load failed.</td></tr>`;
      $('#artistCards').innerHTML='<div class="muted" style="padding:12px">Load failed.</div>';
      toastErr('Artists: '+(e.message||e));
    }
  }
}
function artPrev(){if(artPage>0){artPage--;loadArtists(false);}}
function artNext(){if((artPage+1)*ART_PAGE<artistTotal){artPage++;loadArtists(false);}}
function artistLimitSelected(r,val){
  const m=r.max_downloads;
  if(val==='')return m==null||m===undefined;
  return Number(m)===Number(val);
}
function artistLimitSelectHtml(r){
  return `<select class="toolbar-select" data-limit data-aid="${esc(r.spotify_id)}" title="Max Downloads">
    <option value="" ${artistLimitSelected(r,'')?'selected':''}>Global</option>
    <option value="10" ${artistLimitSelected(r,10)?'selected':''}>10</option>
    <option value="50" ${artistLimitSelected(r,50)?'selected':''}>50</option>
    <option value="100" ${artistLimitSelected(r,100)?'selected':''}>100</option>
    <option value="150" ${artistLimitSelected(r,150)?'selected':''}>150</option>
    <option value="0" ${artistLimitSelected(r,0)?'selected':''}>Unlimit</option>
  </select>`;
}
function artistRowHtml(r){
  const sync=r.sync_done?'<span class="pill done">synced</span>':'<span class="pill pend">pending</span>';
  const act=r.active?'<span class="pill on">on</span>':'<span class="pill off">off</span>';
  const prog=(r.songs_dl||0)+'/'+(r.songs_total||0);
  const roCls=r.is_romanian?'on':'';
  const roTitle=r.is_romanian?'Clear Romanian flag':'Mark as Romanian';
  const rowCls=r.is_romanian?'row-ro':'';
  const picked=selectedArtists.has(r.spotify_id);
  const chk=selectMode?`<td class="chk-col sel-col"><input type="checkbox" class="row-chk" data-aid="${esc(r.spotify_id)}" ${picked?'checked':''}/></td>`:'';
  const fixButton = artistNeedsYtFix(r) ? `<button type="button" class="btn ghost sm" data-fix title="Fix YouTube Music mapping">YT</button>` : '';
  const dlBtn = `<button type="button" class="btn ghost sm" data-download title="Descărcare piese manual">DL</button>`;
  return `<tr class="${rowCls}${picked?' row-picked':''}" data-aid="${esc(r.spotify_id)}">${chk}<td><button type="button" class="ro-toggle ${roCls}" data-ro data-val="${r.is_romanian?0:1}" title="${roTitle}">RO</button>${esc(r.name)}</td><td class="muted">${r.album_count||0}</td><td class="muted">${prog}</td><td>${act} ${sync} ${artistYtMusicStatusHtml(r)}</td>
    <td class="td-actions"><div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">${artistLimitSelectHtml(r)}
      ${fixButton}
      ${dlBtn}
      <button type="button" class="btn ghost sm" data-toggle>${r.active?'Off':'On'}</button>
      <button type="button" class="btn danger sm" data-del title="Remove from database. Shift+click: delete files.">×</button></div></td></tr>`;
}
function artistCardHtml(r){
  const sync=r.sync_done?'<span class="pill done">synced</span>':'<span class="pill pend">pending</span>';
  const act=r.active?'<span class="pill on">on</span>':'<span class="pill off">off</span>';
  const prog=(r.songs_dl||0)+'/'+(r.songs_total||0);
  const roCls=r.is_romanian?'on':'';
  const roTitle=r.is_romanian?'Clear Romanian flag':'Mark as Romanian';
  const rowCls=r.is_romanian?'row-ro':'';
  const picked=selectedArtists.has(r.spotify_id);
  const chk=selectMode?`<input type="checkbox" class="row-chk" data-aid="${esc(r.spotify_id)}" ${picked?'checked':''}/>`:'';
  const fixButton = artistNeedsYtFix(r) ? `<button type="button" class="btn ghost sm" data-fix title="Fix YouTube Music mapping">YT</button>` : '';
  const dlBtn = `<button type="button" class="btn ghost sm" data-download title="Descărcare piese manual">DL</button>`;
  return `<div class="artist-card ${rowCls}${picked?' row-picked':''}" data-aid="${esc(r.spotify_id)}">
    <div class="artist-card-head">${chk}<button type="button" class="ro-toggle ${roCls}" data-ro data-val="${r.is_romanian?0:1}" title="${roTitle}">RO</button><span>${esc(r.name)}</span></div>
    <div class="artist-card-meta"><span>${r.album_count||0} albums</span><span>${prog} songs</span>${act}${sync} ${artistYtMusicStatusHtml(r)}</div>
    <div class="artist-card-actions">${artistLimitSelectHtml(r)}
      <div class="btn-row">${fixButton}${dlBtn}<button type="button" class="btn ghost sm" data-toggle>${r.active?'Off':'On'}</button>
      <button type="button" class="btn danger sm" data-del title="Remove. Shift+click: delete files.">Remove</button></div></div></div>`;
}
function renderArtists(){
  const pages=Math.max(1,Math.ceil(artistTotal/ART_PAGE));
  $('#artPageInfo').textContent=`${artPage+1} / ${pages} · ${artistTotal}`;
  const empty=`<tr><td colspan="${selectMode?6:5}" class="muted">No artists yet.</td></tr>`;
  const emptyCards='<div class="muted" style="padding:12px">No artists yet.</div>';
  if(!curArt.length){
    $('#artistRows').innerHTML=empty;
    $('#artistCards').innerHTML=emptyCards;
  }else{
    $('#artistRows').innerHTML=curArt.map(artistRowHtml).join('');
    $('#artistCards').innerHTML=curArt.map(artistCardHtml).join('');
  }
  syncMobileLists();
  syncSelBar();
}
function onArtistListClick(e){
  const card=e.target.closest('.artist-card');
  const tr=e.target.closest('tr[data-aid]');
  const root=card||tr;
  if(!root)return;
  const id=root.dataset.aid;
  if(e.target.closest('button[data-ro]')){setRomanian(id,parseInt(e.target.closest('button[data-ro]').dataset.val,10));return;}
  if(e.target.closest('button[data-fix]')){fixArtistYtMusic(id);return;}
  if(e.target.closest('button[data-download]')){downloadArtist(id);return;}
  if(e.target.closest('button[data-toggle]')){toggleArtist(id);return;}
  if(e.target.closest('button[data-del]')){delArtist(id,e);return;}
}
function onArtistListChange(e){
  if(e.target.matches('select[data-limit]'))setArtistLimit(e.target.dataset.aid,e.target.value);
  if(e.target.matches('input.row-chk'))togglePick(e.target.dataset.aid,e.target.checked);
}
async function addArtist(){
  const v=$('#artistEntry').value.trim();if(!v)return;
  try{
    await api('/api/artists',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({entry:v})});
    $('#artistEntry').value='';toast('Adding — see console');showConsole();
  }catch(e){toastErr(e.message||'Add artist failed');}
}
async function toggleArtist(id){
  const a=artistById(id);if(!a)return;
  const prev=a.active;
  a.active=prev?0:1;
  renderArtists();flashArtistRow(id);
  try{
    const r=await api(artistUrl(id,'toggle'),{method:'POST'});
    a.active=r.active?1:0;
    renderArtists();
  }catch(e){a.active=prev;renderArtists();toastErr(e.message||'Toggle failed');}
}
async function setRomanian(id, val){
  const a=artistById(id);if(!a)return;
  const prev=!!a.is_romanian;
  a.is_romanian=val?1:0;
  renderArtists();flashArtistRow(id);
  try{
    const r=await api(artistUrl(id,'ro'),{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({is_romanian:!!val}),
    });
    a.is_romanian=r.is_romanian?1:0;
    renderArtists();
  }catch(e){a.is_romanian=prev?1:0;renderArtists();toastErr(e.message||'Romanian flag failed');}
}
function patchArtistLimit(id, maxDl){
  const a=artistById(id);
  if(a)a.max_downloads=maxDl;
}
async function setArtistLimit(id, limit){
  const a=artistById(id);if(!a)return;
  const prev=a.max_downloads;
  const next=limit===''?null:parseInt(limit,10);
  patchArtistLimit(id,next);
  renderArtists();flashArtistRow(id);
  try{
    const r=await api('/api/artists/limit',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({spotify_id:id,limit:limit===''?null:limit}),
    });
    if(r.error)throw new Error(r.error);
    patchArtistLimit(id,r.max_downloads);
    renderArtists();
  }catch(e){patchArtistLimit(id,prev);renderArtists();toastErr(e.message||'Limit save failed');}
}
async function downloadArtist(id){
  const a=artistById(id);if(!a)return;
  toast('Descărcare manuală pornită pentru: '+a.name);
  try{
    const r=await api(artistUrl(id,'download'),{method:'POST'});
    toast('Pornit: '+(r.label||a.name));showConsole();
  }catch(e){toastErr(e.message||'Descărcare eșuată');}
}
async function fixArtistYtMusic(id){
  const a=artistById(id);if(!a)return;
  const currentStatus=a.ytmusic_status||'unknown';
  const currentName=a.ytmusic_name||a.name||'';
  const name=prompt(`Enter YouTube Music artist name for "${a.name}" (leave blank to mark not found):`, currentName);
  if(name===null) return;
  const trimmed=name.trim();
  let status='manually_mapped';
  if(!trimmed){
    if(!confirm(`Mark "${a.name}" as not found on YouTube Music?`)) return;
    status='not_found';
  }
  const body={status};
  if(trimmed) body.ytmusic_name = trimmed;
  try{
    const r=await api(artistUrl(id,'ytmusic'),{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify(body),
    });
    a.ytmusic_status = r.status;
    if(r.ytmusic_name) a.ytmusic_name = r.ytmusic_name;
    renderArtists(); flashArtistRow(id);
    toast(`YT mapping updated: ${r.status}`);
  }catch(e){toastErr(e.message||'YT mapping update failed');}
}
async function delArtist(id,ev){
  const a=artistById(id);
  if(!a)return;
  const delFiles=!!(ev&&ev.shiftKey);
  const msg=delFiles
    ?`Remove “${a.name}” and delete their music folder on disk?`
    :`Remove “${a.name}” from the database? (albums/songs cleaned up in background)`;
  if(!confirm(msg))return;
  const idx=curArt.findIndex(x=>x.spotify_id===id);
  const backup=idx>=0?curArt[idx]:null;
  if(idx>=0){
    curArt.splice(idx,1);
    artistTotal=Math.max(0,artistTotal-1);
    renderArtists();
  }
  try{
    await api(`/api/artists/${encodeURIComponent(id)}?delete_files=${delFiles}`,{method:'DELETE'});
    toast(delFiles?'Removed (+ files deleting)':'Removed');
    loadDbSummary();
  }catch(e){
    if(backup&&idx>=0){
      curArt.splice(idx,0,backup);
      artistTotal++;
      renderArtists();
    }
    toastErr(e.message||'Remove failed');
  }
}
function renderPlaylists(){
  $('#playlistRows').innerHTML=curPl.map(r=>{
    const act=r.active?'<span class="pill on">on</span>':'<span class="pill off">off</span>';
    return `<tr><td data-label="Playlist"><a href="${esc(r.url)}" target="_blank">${esc(r.name)}</a></td><td data-label="Status">${act}</td><td data-label="Last scan" class="muted">${(r.last_synced||'-').slice(0,10)}</td>
      <td class="td-actions" data-label="Actions">
        <div style="display:flex; gap:6px; align-items:center;">
          <button type="button" class="btn ghost sm" data-pl-sync data-url="${esc(r.url)}">Sync</button>
          <button type="button" class="btn ghost sm" data-pl-toggle data-pl-id="${esc(r.spotify_id)}">${r.active?'Off':'On'}</button>
          <button type="button" class="btn danger sm" data-pl-del data-pl-id="${esc(r.spotify_id)}">×</button>
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
  if(r.error){toastErr(r.error);return;}
  $('#plName').value='';$('#plUrl').value='';loadPlaylists();toast('Added');
}
async function syncPl(url){
  const r=await api('/api/actions/sync-playlist',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
  if(r.error){toastErr(r.error);return;}
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
  }catch(e){p.active=prev;renderPlaylists();toastErr(e.message||'Playlist toggle failed');}
}
async function delPl(id){
  if(!confirm('Remove?'))return;
  try{await api(`/api/playlists/${encodeURIComponent(id)}`,{method:'DELETE'});loadPlaylists();}
  catch(e){toastErr(e.message||'Remove playlist failed');}
}
let curTracks=[];
function tracksBaseUrl(refresh=false){
  const q=encodeURIComponent($('#trackSearch').value||'');
  const r=refresh?'&refresh=1':'';
  return `/api/tracks?q=${q}${r}`;
}
async function loadTracks(reset=true,refreshIndex=false){
  if(reset)trackPage=0;
  const gen=++tracksLoadGen;
  curTracks=[];
  $('#trackRows').innerHTML=skeletonRows(4);
  $('#trackPageInfo').innerHTML='Loading<span class="loading-hint">…</span>';
  $('#trackHint').textContent='';
  try{
    const off=trackPage*TRACK_PAGE;
    const page=await api(`${tracksBaseUrl(refreshIndex)}&offset=${off}&limit=${TRACK_PAGE}`);
    if(gen!==tracksLoadGen)return;
    curTracks=page.items||[];
    trackTotal=page.total??0;
    renderTracks();
  }catch(e){
    if(gen===tracksLoadGen){
      $('#trackRows').innerHTML='<tr><td colspan=4 class="muted">Load failed.</td></tr>';
      toastErr('Files: '+(e.message||e));
    }
  }
}
const debouncedLoadTracks=debounce(()=>loadTracks(true),320);
function trackPrev(){if(trackPage>0){trackPage--;loadTracks(false);}}
function trackNext(){
  if((trackPage+1)*TRACK_PAGE<trackTotal){trackPage++;loadTracks(false);}
}
function renderTracks(){
  const pages=Math.max(1,Math.ceil(trackTotal/TRACK_PAGE));
  $('#trackPageInfo').textContent=`Page ${trackPage+1} of ${pages} (${trackTotal} files)`;
  $('#trackRows').innerHTML=curTracks.map(r=>`<tr><td data-label="Artist">${esc(r.artist)}</td><td data-label="Album" class="muted">${esc(r.album)}</td><td data-label="File">${esc(r.title)}</td>
    <td class="td-actions" data-label="">
      <div style="display:inline-flex; gap:6px; justify-content:flex-end; align-items:center;">
        <a class="btn ghost sm" href="/api/track/download?path=${encodeURIComponent(r.path)}" download title="Download"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg></a>
        <button type="button" class="btn ghost sm" data-track-info data-path="${esc(r.path)}">Info</button>
      </div>
    </td></tr>`).join('')
    ||'<tr><td colspan=4 class="muted">No files found.</td></tr>';
  $('#trackHint').textContent=trackTotal?`${trackTotal} files on disk (100 per page)`:'No audio files in music folder';
}
async function showTrackInfo(path) {
  $('#tmCover').src=''; $('#tmTitle').textContent='Loading...';
  $('#tmArtist').textContent='-'; $('#tmAlbum').textContent='-';
  $('#tmGenre').textContent='-'; $('#tmYear').textContent='-';
  $('#tmLength').textContent='-'; $('#tmBitrate').textContent='-';
  $('#trackModal').classList.remove('hide');
  try{
  const info=await api('/api/track/info?path='+encodeURIComponent(path));
  if(info.error){ $('#tmTitle').textContent=info.error; toastErr(info.error); return; }
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
  }catch(e){$('#tmTitle').textContent='Error';toastErr(e.message||'Track info failed');}
}
async function action(a){
  try{
    const r=await api('/api/actions/'+a,{method:'POST'});
    toast('Started: '+(r.label||a));showConsole();
  }catch(e){toastErr(e.message||'Action failed');}
}
async function downloadDirect(){
  const url=$('#directDownloadUrl').value.trim();if(!url)return;
  try{
    const r=await api('/api/actions/download',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});
    if(r.error){toastErr(r.error);return;}
    $('#directDownloadUrl').value='';
    toast('Started: '+r.label);showConsole();
  }catch(e){toastErr(e.message||'Download failed');}
}
async function stop(){
  try{await api('/api/stop',{method:'POST'});toast('Stop requested');}
  catch(e){toastErr(e.message||'Stop failed');}
}
function showConsole(){document.querySelector('header nav button[data-tab="console"]')?.click();}
function clearConsole(){$('#consoleOut').innerHTML='';}
function highlightPipeline(steps) {
  document.querySelectorAll('.pipeline-step').forEach(el => {
    if (!el.classList.contains('running')) {
      el.classList.remove('active-path');
    }
  });
  steps.forEach(s => document.getElementById('step-' + s)?.classList.add('active-path'));
}
function clearPipelineHighlight() {
  document.querySelectorAll('.pipeline-step').forEach(el => {
    if (!el.classList.contains('running')) {
      el.classList.remove('active-path');
    }
  });
}
function setStepRunning(stepId) {
  document.querySelectorAll('.pipeline-step').forEach(el => {
    el.classList.remove('running');
    if (!stepId) el.classList.remove('active-path');
  });
  if (stepId) {
    const stepEl = document.getElementById('step-' + stepId);
    if (stepEl) {
      stepEl.classList.add('running');
      stepEl.classList.add('active-path');
    }
  }
}
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
    
    // Set step running in pipeline
    if (text.includes("Step 1") || text.includes("Scanning playlists")) {
      setStepRunning('scan');
    } else if (text.includes("Step 2") || text.includes("Scanning artist albums") || text.includes("Scanning catalog")) {
      setStepRunning('catalog');
    } else if (text.includes("Reconciling") || text.includes("potrivire") || text.includes("Reconcile") || text.includes("reconcile") || text.includes("potrivesc")) {
      setStepRunning('match');
    } else if (text.includes("Step 3") || text.includes("Enforcing caps") || text.includes("caps") || text.includes("trim library") || text.includes("trimming")) {
      setStepRunning('trim');
    } else if (text.includes("Step 4") || text.includes("Downloading top tracks") || text.includes("Downloading:") || text.includes("tracks to download") || text.includes("Downloading")) {
      setStepRunning('download');
    } else if (text.includes("complete") || text.includes("done —") || text.includes("exit 0") || text.includes("exit")) {
      setStepRunning(null);
    }
    
    if(/ERROR|✗|failed|error/i.test(text)) d.style.color='#f87171';
    else if(/WARN|WARNING/i.test(text)) d.style.color='#fbbf24';
    else if(/===/.test(text)) d.style.color='#ffffff';
    else if(/✓|done|complete|ok/i.test(text)) d.style.color='#34d399';
    else if(/\[\d+\/\d+\]/.test(text)) d.style.color='#ffffff';
    out.appendChild(d);
    while(out.childNodes.length>1200)out.removeChild(out.firstChild);
    if(autoscroll)out.scrollTop=out.scrollHeight;
    if(/complete|done —|prune complete|download pending complete/i.test(text)){
      loadStats();
      if(!$('#artists').classList.contains('hide')){loadDbSummary();loadArtists();}
    }
  };
}
$('#consoleOut').addEventListener('scroll',e=>{
  const el=e.target;autoscroll=(el.scrollHeight-el.scrollTop-el.clientHeight)<40;
});
document.getElementById('artistRows')?.addEventListener('click',onArtistListClick);
document.getElementById('artistCards')?.addEventListener('click',onArtistListClick);
document.getElementById('artistRows')?.addEventListener('change',onArtistListChange);
document.getElementById('artistCards')?.addEventListener('change',onArtistListChange);
function onLibSongsClick(e){
  const b=e.target.closest('button[data-album]');
  if(!b)return;
  showSongs(b.dataset.album,b.dataset.albumName||'').catch(err=>toastErr(err.message||'Songs failed'));
}
document.getElementById('libRows')?.addEventListener('click',onLibSongsClick);
document.getElementById('libCards')?.addEventListener('click',onLibSongsClick);
document.getElementById('playlistRows')?.addEventListener('click',e=>{
  const sync=e.target.closest('[data-pl-sync]');
  if(sync){syncPl(sync.dataset.url).catch(err=>toastErr(err.message||'Sync failed'));return;}
  const tgl=e.target.closest('[data-pl-toggle]');
  if(tgl){togglePl(tgl.dataset.plId);return;}
  const del=e.target.closest('[data-pl-del]');
  if(del){delPl(del.dataset.plId);return;}
});
document.getElementById('trackRows')?.addEventListener('click',e=>{
  const b=e.target.closest('[data-track-info]');
  if(b)showTrackInfo(b.dataset.path);
});
loadStats();connectWS();setInterval(loadStats,15000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(load_cfg().get("hud_port", 8800)))
