#!/usr/bin/env python3
"""
hud.py — MusicaDet web dashboard for music_sync.py
"""

import asyncio
import json
import os
import sqlite3
import sys
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

BASE = Path(__file__).resolve().parent
CFG_FILE = BASE / "config.json"
PY_SCRIPT = BASE / "music_sync.py"

DEFAULTS = {
    "music_dir": "/mnt/storage_jellyfin/media/music",
    "sync_dir": str(BASE / "sync-data"),
    "db_path": str(BASE / "music.db"),
    "log_dir": "/var/log/musicadet",
    "format": "mp3",
    "bitrate": "320k",
    "threads": 4,
    "output_template": "{artist}/{album}/{track-number} - {title}.{output-ext}",
    "playlist_save_timeout": 600,
    "playlist_save_retries": 3,
    "artist_save_timeout": 900,
    "lyrics_providers": ["genius", "musixmatch", "azlyrics"],
    "generate_lrc": False,
    "hud_port": 8800,
    "playlists": [],
}

CONFIG_KEYS = [
    "music_dir", "sync_dir", "db_path", "log_dir", "format", "bitrate", "threads",
    "output_template", "playlist_save_timeout", "playlist_save_retries",
    "artist_save_timeout", "lyrics_providers", "generate_lrc", "hud_port",
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
    bitrate: Optional[str] = None
    threads: Optional[int] = None
    output_template: Optional[str] = None
    playlist_save_timeout: Optional[int] = None
    playlist_save_retries: Optional[int] = None
    artist_save_timeout: Optional[int] = None
    lyrics_providers: Optional[list[str]] = None
    generate_lrc: Optional[bool] = None
    hud_port: Optional[int] = None


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
        songs_downloaded = conn.execute("SELECT COUNT(*) FROM songs WHERE status='downloaded'").fetchone()[0]
        songs_pending = conn.execute("SELECT COUNT(*) FROM songs WHERE status='pending'").fetchone()[0]
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
        "albums_total": albums_total,
        "songs_downloaded": songs_downloaded, "songs_pending": songs_pending,
        "songs_failed": songs_failed,
        "songs_missing_cover": songs_missing_cover,
        "songs_missing_lyrics": songs_missing_lyrics,
        "tracks": count_tracks(), "running": bus.running_label,
        "music_dir": load_cfg().get("music_dir"),
    }


def count_tracks() -> int:
    music_dir = Path(load_cfg()["music_dir"])
    if not music_dir.exists():
        return 0
    n = 0
    for _root, _dirs, files in os.walk(music_dir):
        for f in files:
            if Path(f).suffix.lower() in AUDIO_EXTS:
                n += 1
    return n


@app.get("/api/artists")
def api_artists(q: str = "", status: str = "all"):
    ensure_db()
    sql = """
        SELECT a.spotify_id, a.name, a.source, a.active, a.sync_done, a.last_synced, a.added_at,
               a.albums_scanned_at,
               (SELECT COUNT(*) FROM albums WHERE artist_id=a.spotify_id) AS album_count,
               (SELECT COUNT(*) FROM songs WHERE artist_id=a.spotify_id AND status='downloaded') AS songs_dl,
               (SELECT COUNT(*) FROM songs WHERE artist_id=a.spotify_id) AS songs_total
        FROM artists a
    """
    where, params = [], []
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
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY a.name COLLATE NOCASE LIMIT 2000"
    with db() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    return rows


@app.get("/api/albums")
def api_albums(artist_id: str = "", status: str = "all", limit: int = 500):
    ensure_db()
    sql = """
        SELECT al.spotify_id, al.artist_id, al.name, al.release_year,
               al.track_count, al.downloaded_count, al.last_scanned, ar.name AS artist_name
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
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY ar.name, al.name LIMIT ?"
    params.append(limit)
    with db() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


@app.get("/api/songs")
def api_songs(album_id: str = "", artist_id: str = "", status: str = "all", limit: int = 500):
    ensure_db()
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


@app.delete("/api/artists/{spotify_id}")
def api_delete_artist(spotify_id: str):
    with db() as conn:
        conn.execute("DELETE FROM artists WHERE spotify_id=?", (spotify_id,))
    return {"ok": True}


@app.get("/api/playlists")
def api_playlists():
    ensure_db()
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


@app.get("/api/tracks")
def api_tracks(q: str = "", limit: int = 300):
    music_dir = Path(load_cfg()["music_dir"])
    out = []
    if music_dir.exists():
        for root, _dirs, files in os.walk(music_dir):
            for f in files:
                if Path(f).suffix.lower() not in AUDIO_EXTS:
                    continue
                rel = os.path.relpath(os.path.join(root, f), music_dir)
                if q and q.lower() not in rel.lower():
                    continue
                parts = rel.split(os.sep)
                out.append({
                    "path": rel,
                    "artist": parts[0] if len(parts) > 1 else "",
                    "album": parts[1] if len(parts) > 2 else "",
                    "title": parts[-1],
                })
                if len(out) >= limit:
                    break
            if len(out) >= limit:
                break
    out.sort(key=lambda t: t["path"].lower())
    return out


ACTIONS = {
    "scan": (["scan"], "Scan playlists"),
    "scan-artists": (["scan-artists"], "Scan artist albums"),
    "scan-artists-new": (["scan-artists", "--new-only"], "Scan new artists"),
    "artists-sync": (["artists-sync"], "Sync all artists"),
    "artists-sync-new": (["artists-sync", "--new-only"], "Sync new artists"),
    "reconcile": (["reconcile"], "Reconcile files"),
    "fix-metadata": (["fix-metadata"], "Fix metadata"),
    "full": ([], "Full sync"),
}


@app.post("/api/actions/{action}")
async def api_action(action: str):
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
<title>MusicaDet</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<style>
  :root{
    --bg:#000;--bg2:#09090b;--card:rgba(255,255,255,.03);--line:rgba(255,255,255,.08);
    --txt:#fafafa;--muted:#a1a1aa;--accent:#fff;--ok:#4ade80;--warn:#facc15;--bad:#f87171;
  }
  *{box-sizing:border-box}
  body{margin:0;font:14px/1.5 Inter,system-ui,sans-serif;color:var(--txt);
    background:radial-gradient(900px 500px at 50% -20%,rgba(255,255,255,.04),transparent 60%),var(--bg);min-height:100vh}
  header{position:sticky;top:0;z-index:20;backdrop-filter:blur(12px);background:rgba(0,0,0,.75);
    border-bottom:1px solid var(--line);padding:12px 20px;display:flex;align-items:center;gap:14px;flex-wrap:wrap}
  .logo{font-weight:600;font-size:17px;color:var(--txt);letter-spacing:-.02em}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--ok)}
  .dot.busy{background:var(--warn);animation:pulse 1s infinite}
  @keyframes pulse{50%{opacity:.4}}
  .status{display:flex;align-items:center;gap:8px;color:var(--muted);font-size:12px}
  nav{display:flex;gap:4px;margin-left:auto;flex-wrap:wrap}
  nav button{background:transparent;border:1px solid transparent;color:var(--muted);padding:7px 12px;border-radius:8px;cursor:pointer;font:500 13px Inter,sans-serif}
  nav button.active{color:var(--txt);background:rgba(255,255,255,.06);border-color:var(--line)}
  nav button:hover{color:var(--txt)}
  main{max-width:1200px;margin:0 auto;padding:20px}
  .grid{display:grid;gap:12px}
  .stats{grid-template-columns:repeat(auto-fit,minmax(130px,1fr))}
  .card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px}
  .stat .n{font-size:26px;font-weight:600;font-variant-numeric:tabular-nums;color:var(--txt)}
  .stat .l{color:var(--muted);font-size:11px;margin-top:2px;text-transform:uppercase;letter-spacing:.04em}
  .btn{border:1px solid var(--line);border-radius:8px;padding:8px 14px;font:600 13px Inter,sans-serif;cursor:pointer;color:#000;background:var(--accent)}
  .btn:hover{background:#e4e4e7}
  .btn.ghost{background:transparent;color:var(--txt);box-shadow:none}
  .btn.ghost:hover{background:rgba(255,255,255,.06)}
  .btn.sm{padding:5px 10px;font-size:12px;border-radius:6px}
  .btn.danger{background:transparent;color:var(--bad);border-color:rgba(248,113,113,.3)}
  .btn.danger:hover{background:rgba(248,113,113,.1)}
  .row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
  input,select,textarea{background:rgba(255,255,255,.04);border:1px solid var(--line);color:var(--txt);padding:8px 10px;border-radius:8px;font:13px Inter,sans-serif;outline:none}
  input:focus,select:focus,textarea:focus{border-color:rgba(255,255,255,.2)}
  input{flex:1;min-width:140px}
  label{font-size:12px;color:var(--muted);display:block;margin-bottom:4px}
  .field{margin-bottom:12px}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{text-align:left;padding:8px 10px;border-bottom:1px solid var(--line)}
  th{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.04em;font-weight:500}
  tr:hover td{background:rgba(255,255,255,.02)}
  .pill{display:inline-block;padding:2px 8px;border-radius:99px;font-size:10px;font-weight:600}
  .pill.on{background:rgba(74,222,128,.12);color:var(--ok)}
  .pill.off{background:rgba(248,113,113,.12);color:var(--bad)}
  .pill.done{background:rgba(255,255,255,.08);color:var(--txt)}
  .pill.pend{background:rgba(250,204,21,.12);color:var(--warn)}
  .meta-ok{color:var(--ok)}.meta-no{color:var(--muted)}
  h2{margin:0 0 12px;font-size:16px;font-weight:600}
  .muted{color:var(--muted)}
  .console{background:#0a0a0a;border:1px solid var(--line);border-radius:10px;height:62vh;overflow:auto;padding:12px 14px;
    font:12px/1.5 ui-monospace,Consolas,monospace;white-space:pre-wrap;word-break:break-word}
  .hide{display:none}
  .hint{color:var(--muted);font-size:11px;margin-top:6px}
  .actions{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px}
  .toast{position:fixed;bottom:16px;right:16px;background:var(--txt);color:#000;padding:10px 16px;border-radius:8px;font-weight:600;font-size:13px;z-index:50;opacity:0;transform:translateY(8px);transition:.2s}
  .toast.show{opacity:1;transform:none}
  .progress{font-variant-numeric:tabular-nums;font-size:12px;color:var(--muted)}
</style>
</head>
<body>
<header>
  <div class="logo">MusicaDet</div>
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
    <div class="card" style="margin-top:14px">
      <h2>Actions</h2>
      <div class="actions">
        <button class="btn" onclick="action('full')">Full Sync</button>
        <button class="btn ghost" onclick="action('scan')">Scan Playlists</button>
        <button class="btn ghost" onclick="action('scan-artists')">Scan Albums</button>
        <button class="btn ghost" onclick="action('artists-sync-new')">Sync New</button>
        <button class="btn ghost" onclick="action('artists-sync')">Sync All</button>
        <button class="btn ghost" onclick="action('reconcile')">Reconcile</button>
        <button class="btn ghost" onclick="action('fix-metadata')">Fix Metadata</button>
        <button class="btn danger" onclick="stop()">Stop</button>
      </div>
      <div class="hint">Music folder: <span id="musicDirHint" class="muted">—</span></div>
    </div>
  </section>

  <section id="library" class="hide">
    <div class="card">
      <div class="row" style="margin-bottom:10px">
        <select id="libArtist" onchange="loadLibrary()"><option value="">All artists</option></select>
        <select id="libStatus" onchange="loadLibrary()">
          <option value="all">All albums</option>
          <option value="pending">Incomplete</option>
          <option value="complete">Complete</option>
        </select>
        <button class="btn ghost sm" onclick="loadLibrary()">Refresh</button>
      </div>
      <table><thead><tr><th>Artist</th><th>Album</th><th>Progress</th><th></th></tr></thead>
      <tbody id="libRows"></tbody></table>
    </div>
    <div class="card hide" id="songPanel" style="margin-top:12px">
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
    <div class="card" style="margin-top:12px">
      <div class="row" style="margin-bottom:10px">
        <input id="artistSearch" placeholder="Search..." oninput="loadArtists()"/>
        <select id="artistFilter" onchange="loadArtists()">
          <option value="all">All</option><option value="active">Active</option>
          <option value="pending">Pending</option><option value="synced">Synced</option>
          <option value="disabled">Disabled</option>
        </select>
        <button class="btn ghost sm" onclick="loadArtists()">Refresh</button>
      </div>
      <table><thead><tr><th>Artist</th><th>Albums</th><th>Songs</th><th>Status</th><th></th></tr></thead>
      <tbody id="artistRows"></tbody></table>
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
    <div class="card" style="margin-top:12px">
      <table><thead><tr><th>Playlist</th><th>Status</th><th>Last scan</th><th></th></tr></thead>
      <tbody id="playlistRows"></tbody></table>
    </div>
  </section>

  <section id="tracks" class="hide">
    <div class="card">
      <div class="row" style="margin-bottom:10px">
        <input id="trackSearch" placeholder="Search files..." oninput="loadTracks()"/>
        <button class="btn ghost sm" onclick="loadTracks()">Refresh</button>
      </div>
      <table><thead><tr><th>Artist</th><th>Album</th><th>File</th></tr></thead>
      <tbody id="trackRows"></tbody></table>
      <div class="hint" id="trackHint"></div>
    </div>
  </section>

  <section id="settings" class="hide">
    <div class="card">
      <h2>Settings</h2>
      <div class="field"><label>Music folder</label><input id="cfgMusicDir"/></div>
      <div class="row">
        <div class="field" style="flex:1"><label>Format</label>
          <select id="cfgFormat"><option value="mp3">mp3</option><option value="opus">opus</option><option value="flac">flac</option></select>
        </div>
        <div class="field" style="flex:1"><label>Bitrate</label><input id="cfgBitrate" placeholder="320k"/></div>
      </div>
      <div class="field"><label>Output template</label><input id="cfgTemplate"/></div>
      <div class="field"><label>Lyrics providers (comma-separated)</label><input id="cfgLyrics" placeholder="genius,musixmatch,azlyrics"/></div>
      <div class="row">
        <div class="field" style="flex:1"><label>Playlist timeout (s)</label><input id="cfgPlTimeout" type="number"/></div>
        <div class="field" style="flex:1"><label>Playlist retries</label><input id="cfgPlRetries" type="number"/></div>
      </div>
      <label style="display:flex;align-items:center;gap:8px;margin-bottom:12px">
        <input type="checkbox" id="cfgLrc" style="flex:0"/> Generate .lrc sidecar files
      </label>
      <button class="btn" onclick="saveSettings()">Save settings</button>
      <div class="hint">Changing HUD port requires restarting the service.</div>
    </div>
  </section>

  <section id="console" class="hide">
    <div class="card">
      <div class="row" style="margin-bottom:8px">
        <h2 style="margin:0">Live Console</h2>
        <span class="muted" id="wsState">connecting...</span>
        <button class="btn ghost sm" style="margin-left:auto" onclick="clearConsole()">Clear</button>
      </div>
      <div class="console" id="consoleOut"></div>
    </div>
  </section>
</main>
<div class="toast" id="toast"></div>

<script>
const $=s=>document.querySelector(s);
let autoscroll=true;
function toast(m){const t=$('#toast');t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2200);}
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
async function api(path,opts){const r=await fetch(path,opts);return r.json();}
async function loadStats(){
  const s=await api('/api/stats');
  const cards=[['Artists',s.artists_total],['Synced',s.artists_synced],['Pending',s.artists_pending],
    ['Albums',s.albums_total],['Songs OK',s.songs_downloaded],['Songs wait',s.songs_pending],
    ['No cover',s.songs_missing_cover],['No lyrics',s.songs_missing_lyrics],['Files',s.tracks]];
  $('#statCards').innerHTML=cards.map(c=>`<div class="card stat"><div class="n">${c[1]}</div><div class="l">${c[0]}</div></div>`).join('');
  const busy=!!s.running;
  $('#dot').className='dot'+(busy?' busy':'');
  $('#statusText').textContent=busy?('running: '+s.running):'idle';
  $('#musicDirHint').textContent=s.music_dir||'—';
}
async function loadSettings(){
  const c=await api('/api/config');
  $('#cfgMusicDir').value=c.music_dir||'';
  $('#cfgFormat').value=c.format||'mp3';
  $('#cfgBitrate').value=c.bitrate||'320k';
  $('#cfgTemplate').value=c.output_template||'';
  $('#cfgLyrics').value=(c.lyrics_providers||[]).join(',');
  $('#cfgPlTimeout').value=c.playlist_save_timeout||600;
  $('#cfgPlRetries').value=c.playlist_save_retries||3;
  $('#cfgLrc').checked=!!c.generate_lrc;
}
async function saveSettings(){
  const body={
    music_dir:$('#cfgMusicDir').value.trim(),
    format:$('#cfgFormat').value,
    bitrate:$('#cfgBitrate').value.trim(),
    output_template:$('#cfgTemplate').value.trim(),
    lyrics_providers:$('#cfgLyrics').value.split(',').map(s=>s.trim()).filter(Boolean),
    playlist_save_timeout:parseInt($('#cfgPlTimeout').value)||600,
    playlist_save_retries:parseInt($('#cfgPlRetries').value)||3,
    generate_lrc:$('#cfgLrc').checked
  };
  const r=await api('/api/config',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  if(r.error){toast(r.error);return;}
  toast('Settings saved');
  loadStats();
}
async function loadLibArtists(){
  const rows=await api('/api/artists?status=active');
  const sel=$('#libArtist');
  const cur=sel.value;
  sel.innerHTML='<option value="">All artists</option>'+rows.map(r=>`<option value="${r.spotify_id}">${esc(r.name)}</option>`).join('');
  if(cur)sel.value=cur;
}
async function loadLibrary(){
  const aid=$('#libArtist').value,st=$('#libStatus').value;
  let url=`/api/albums?status=${st}&limit=300`;
  if(aid)url+=`&artist_id=${encodeURIComponent(aid)}`;
  const rows=await api(url);
  $('#libRows').innerHTML=rows.map(r=>{
    const pct=r.track_count?Math.round(100*r.downloaded_count/r.track_count):0;
    const pill=r.downloaded_count>=r.track_count&&r.track_count>0?'<span class="pill done">done</span>':'<span class="pill pend">'+pct+'%</span>';
    return `<tr><td>${esc(r.artist_name)}</td><td>${esc(r.name)}</td><td class="progress">${r.downloaded_count}/${r.track_count} ${pill}</td>
      <td><button class="btn ghost sm" onclick="showSongs('${r.spotify_id}','${esc(r.name)}')">Songs</button></td></tr>`;
  }).join('')||'<tr><td colspan=4 class="muted">No albums yet — run Scan Albums.</td></tr>';
}
async function showSongs(albumId,albumName){
  const rows=await api(`/api/songs?album_id=${encodeURIComponent(albumId)}`);
  $('#songPanelTitle').textContent='Songs — '+albumName;
  $('#songPanel').classList.remove('hide');
  const meta=v=>v?'<span class="meta-ok">✓</span>':'<span class="meta-no">—</span>';
  $('#songRows').innerHTML=rows.map(r=>{
    const st=r.status==='downloaded'?'<span class="pill done">ok</span>':'<span class="pill pend">'+r.status+'</span>';
    return `<tr><td class="muted">${r.track_number||''}</td><td>${esc(r.title)}</td><td>${st}</td><td>${meta(r.has_cover)}</td><td>${meta(r.has_lyrics)}</td></tr>`;
  }).join('')||'<tr><td colspan=5 class="muted">No songs.</td></tr>';
}
async function loadArtists(){
  const q=encodeURIComponent($('#artistSearch').value||'');
  const st=$('#artistFilter').value;
  const rows=await api(`/api/artists?q=${q}&status=${st}`);
  $('#artistRows').innerHTML=rows.map(r=>{
    const sync=r.sync_done?'<span class="pill done">synced</span>':'<span class="pill pend">pending</span>';
    const act=r.active?'<span class="pill on">on</span>':'<span class="pill off">off</span>';
    const prog=(r.songs_dl||0)+'/'+(r.songs_total||0);
    return `<tr><td>${esc(r.name)}</td><td class="muted">${r.album_count||0}</td><td class="muted">${prog}</td><td>${act} ${sync}</td>
      <td class="row" style="border:none">
        <button class="btn ghost sm" onclick="toggleArtist('${r.spotify_id}')">${r.active?'Off':'On'}</button>
        <button class="btn danger sm" onclick="delArtist('${r.spotify_id}')">×</button>
      </td></tr>`;
  }).join('')||'<tr><td colspan=5 class="muted">No artists yet.</td></tr>';
}
async function addArtist(){
  const v=$('#artistEntry').value.trim();if(!v)return;
  await api('/api/artists',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({entry:v})});
  $('#artistEntry').value='';toast('Adding — see console');showConsole();
}
async function toggleArtist(id){await api(`/api/artists/${id}/toggle`,{method:'POST'});loadArtists();}
async function delArtist(id){if(!confirm('Remove artist?'))return;await api(`/api/artists/${id}`,{method:'DELETE'});loadArtists();}
async function loadPlaylists(){
  const rows=await api('/api/playlists');
  $('#playlistRows').innerHTML=rows.map(r=>{
    const act=r.active?'<span class="pill on">on</span>':'<span class="pill off">off</span>';
    return `<tr><td><a href="${esc(r.url)}" target="_blank">${esc(r.name)}</a></td><td>${act}</td><td class="muted">${(r.last_synced||'-').slice(0,10)}</td>
      <td class="row" style="border:none">
        <button class="btn ghost sm" onclick="togglePl('${r.spotify_id}')">${r.active?'Off':'On'}</button>
        <button class="btn danger sm" onclick="delPl('${r.spotify_id}')">×</button>
      </td></tr>`;
  }).join('')||'<tr><td colspan=4 class="muted">No playlists.</td></tr>';
}
async function addPlaylist(){
  const name=$('#plName').value.trim(),url=$('#plUrl').value.trim();if(!url)return;
  const r=await api('/api/playlists',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,url})});
  if(r.error){toast(r.error);return;}
  $('#plName').value='';$('#plUrl').value='';loadPlaylists();toast('Added');
}
async function togglePl(id){await api(`/api/playlists/${id}/toggle`,{method:'POST'});loadPlaylists();}
async function delPl(id){if(!confirm('Remove?'))return;await api(`/api/playlists/${id}`,{method:'DELETE'});loadPlaylists();}
async function loadTracks(){
  const q=encodeURIComponent($('#trackSearch').value||'');
  const rows=await api(`/api/tracks?q=${q}&limit=300`);
  $('#trackRows').innerHTML=rows.map(r=>`<tr><td>${esc(r.artist)}</td><td class="muted">${esc(r.album)}</td><td>${esc(r.title)}</td></tr>`).join('')
    ||'<tr><td colspan=3 class="muted">No files found.</td></tr>';
  $('#trackHint').textContent=rows.length+' files shown';
}
async function action(a){const r=await api('/api/actions/'+a,{method:'POST'});toast('Started: '+(r.label||a));showConsole();}
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
    const d=document.createElement('div');d.textContent=e.data;out.appendChild(d);
    while(out.childNodes.length>1200)out.removeChild(out.firstChild);
    if(autoscroll)out.scrollTop=out.scrollHeight;
  };
}
$('#consoleOut').addEventListener('scroll',e=>{
  const el=e.target;autoscroll=(el.scrollHeight-el.scrollTop-el.clientHeight)<40;
});
function esc(s){return (s==null?'':s).toString().replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));}
loadStats();connectWS();setInterval(loadStats,4000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(load_cfg().get("hud_port", 8800)))
