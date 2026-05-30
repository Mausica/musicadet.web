#!/usr/bin/env python3
"""
hud.py — Audio Alchemist HUD
A self-hosted web dashboard for music_sync.py.

Features:
  - Dark gradient UI (single file, no build step)
  - Live console streaming spotDL / music_sync output over WebSocket
  - Edit the database: artists (add / enable / disable / delete) and
    playlists (add / toggle / remove) that get fetched
  - One-click actions: scan playlists, sync artists, full sync
  - Live stats + downloaded-track browser

Run:  python3 -m uvicorn hud:app --host 0.0.0.0 --port 8800
Reads the same config.json + music.db as music_sync.py.
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
    "music_dir": "/mnt/storage_jellyfin/media/music/spotify",
    "sync_dir": str(BASE / "sync-data"),
    "db_path": str(BASE / "music.db"),
    "log_dir": "/var/log/music-sync",
    "hud_port": 8800,
    "playlists": [],
}

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
              spotify_id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              source TEXT DEFAULT 'manual',
              active INTEGER DEFAULT 1,
              sync_done INTEGER DEFAULT 0,
              last_synced TEXT,
              added_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS playlists (
              spotify_id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              url TEXT NOT NULL,
              active INTEGER DEFAULT 1,
              last_synced TEXT
            );
            CREATE TABLE IF NOT EXISTS playlist_artists (
              playlist_id TEXT NOT NULL,
              artist_id TEXT NOT NULL,
              PRIMARY KEY (playlist_id, artist_id)
            );
            """
        )
        for pl in load_cfg().get("playlists", []):
            pid = _extract_id(pl.get("url", ""), "playlist")
            if pid:
                conn.execute(
                    """INSERT INTO playlists (spotify_id, name, url) VALUES (?,?,?)
                       ON CONFLICT(spotify_id) DO UPDATE
                       SET name=excluded.name, url=excluded.url""",
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
                *args,
                cwd=str(BASE),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
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
app = FastAPI(title="Audio Alchemist HUD")


@app.on_event("startup")
async def _startup() -> None:
    ensure_db()


class ArtistIn(BaseModel):
    entry: str


class PlaylistIn(BaseModel):
    name: str
    url: str


@app.get("/api/stats")
def api_stats():
    ensure_db()
    with db() as conn:
        a_total = conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0]
        a_active = conn.execute("SELECT COUNT(*) FROM artists WHERE active=1").fetchone()[0]
        a_synced = conn.execute("SELECT COUNT(*) FROM artists WHERE sync_done=1").fetchone()[0]
        p_total = conn.execute("SELECT COUNT(*) FROM playlists").fetchone()[0]
        p_active = conn.execute("SELECT COUNT(*) FROM playlists WHERE active=1").fetchone()[0]
    return {
        "artists_total": a_total,
        "artists_active": a_active,
        "artists_synced": a_synced,
        "artists_pending": a_active - a_synced,
        "playlists_total": p_total,
        "playlists_active": p_active,
        "tracks": count_tracks(),
        "running": bus.running_label,
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
    sql = "SELECT spotify_id, name, source, active, sync_done, last_synced, added_at FROM artists"
    where, params = [], []
    if q:
        where.append("name LIKE ?")
        params.append(f"%{q}%")
    if status == "active":
        where.append("active=1")
    elif status == "disabled":
        where.append("active=0")
    elif status == "pending":
        where.append("active=1 AND sync_done=0")
    elif status == "synced":
        where.append("sync_done=1")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY name COLLATE NOCASE LIMIT 2000"
    with db() as conn:
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    return rows


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
    "artists-sync": (["artists-sync"], "Sync all artists"),
    "artists-sync-new": (["artists-sync", "--new-only"], "Sync new artists"),
    "full": ([], "Full sync (scan + download)"),
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
<title>Audio Alchemist HUD</title>
<style>
  :root{
    --bg:#0a0710; --bg2:#120a1f; --card:rgba(255,255,255,.04); --line:rgba(255,255,255,.08);
    --txt:#ece9f5; --muted:#9b93b5; --p1:#a855f7; --p2:#6366f1; --p3:#ec4899; --ok:#34d399; --warn:#fbbf24; --bad:#f87171;
  }
  *{box-sizing:border-box}
  body{margin:0;font:15px/1.5 'Segoe UI',system-ui,sans-serif;color:var(--txt);
    background:radial-gradient(1200px 700px at 10% -10%,#2a1248 0%,transparent 55%),
               radial-gradient(1000px 600px at 100% 0%,#16244d 0%,transparent 50%),
               linear-gradient(160deg,var(--bg),var(--bg2));min-height:100vh}
  a{color:inherit}
  header{position:sticky;top:0;z-index:20;backdrop-filter:blur(14px);
    background:linear-gradient(90deg,rgba(168,85,247,.12),rgba(99,102,241,.10),rgba(236,72,153,.10));
    border-bottom:1px solid var(--line);padding:14px 22px;display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  .logo{font-weight:800;font-size:20px;letter-spacing:.3px;
    background:linear-gradient(90deg,var(--p1),var(--p3),var(--p2));-webkit-background-clip:text;background-clip:text;color:transparent}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--ok);box-shadow:0 0 10px var(--ok)}
  .dot.busy{background:var(--warn);box-shadow:0 0 10px var(--warn);animation:pulse 1s infinite}
  @keyframes pulse{50%{opacity:.35}}
  .status{display:flex;align-items:center;gap:8px;color:var(--muted);font-size:13px}
  nav{display:flex;gap:6px;margin-left:auto;flex-wrap:wrap}
  nav button{background:transparent;border:1px solid transparent;color:var(--muted);padding:8px 14px;border-radius:10px;cursor:pointer;font-size:14px;font-weight:600}
  nav button.active{color:#fff;background:linear-gradient(90deg,rgba(168,85,247,.25),rgba(99,102,241,.25));border-color:var(--line)}
  nav button:hover{color:#fff}
  main{max-width:1180px;margin:0 auto;padding:24px}
  .grid{display:grid;gap:16px}
  .stats{grid-template-columns:repeat(auto-fit,minmax(150px,1fr))}
  .card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:18px;backdrop-filter:blur(8px)}
  .stat .n{font-size:32px;font-weight:800;background:linear-gradient(90deg,var(--p1),var(--p2));-webkit-background-clip:text;background-clip:text;color:transparent}
  .stat .l{color:var(--muted);font-size:13px;margin-top:4px;text-transform:uppercase;letter-spacing:.5px}
  .btn{border:none;border-radius:11px;padding:10px 16px;font-weight:700;font-size:14px;cursor:pointer;color:#fff;
    background:linear-gradient(90deg,var(--p1),var(--p2));box-shadow:0 6px 20px -8px var(--p1)}
  .btn:hover{filter:brightness(1.1)}
  .btn.ghost{background:rgba(255,255,255,.06);box-shadow:none;border:1px solid var(--line)}
  .btn.sm{padding:6px 10px;font-size:12px;border-radius:8px}
  .btn.danger{background:linear-gradient(90deg,#ef4444,#b91c1c);box-shadow:none}
  .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
  input,select{background:rgba(0,0,0,.3);border:1px solid var(--line);color:var(--txt);padding:10px 12px;border-radius:10px;font-size:14px;outline:none}
  input:focus,select:focus{border-color:var(--p1)}
  input{flex:1;min-width:160px}
  table{width:100%;border-collapse:collapse;font-size:14px}
  th,td{text-align:left;padding:10px 12px;border-bottom:1px solid var(--line)}
  th{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.5px}
  tr:hover td{background:rgba(255,255,255,.02)}
  .pill{display:inline-block;padding:2px 9px;border-radius:99px;font-size:11px;font-weight:700}
  .pill.on{background:rgba(52,211,153,.15);color:var(--ok)}
  .pill.off{background:rgba(248,113,113,.15);color:var(--bad)}
  .pill.done{background:rgba(99,102,241,.18);color:#a5b4fc}
  .pill.pend{background:rgba(251,191,36,.15);color:var(--warn)}
  h2{margin:0 0 14px;font-size:18px}
  .muted{color:var(--muted)}
  .console{background:#06040c;border:1px solid var(--line);border-radius:14px;height:62vh;overflow:auto;padding:14px 16px;
    font:13px/1.55 'Cascadia Code',Consolas,monospace;white-space:pre-wrap;word-break:break-word}
  .console .ln{opacity:.92}
  .hide{display:none}
  .hint{color:var(--muted);font-size:12px;margin-top:6px}
  .actions{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}
  .toast{position:fixed;bottom:20px;right:20px;background:linear-gradient(90deg,var(--p2),var(--p1));padding:12px 18px;border-radius:12px;font-weight:600;box-shadow:0 10px 30px -10px #000;z-index:50;opacity:0;transform:translateY(10px);transition:.25s}
  .toast.show{opacity:1;transform:none}
</style>
</head>
<body>
<header>
  <div class="logo">&#9672; Audio Alchemist</div>
  <div class="status"><span id="dot" class="dot"></span><span id="statusText">idle</span></div>
  <nav>
    <button data-tab="dashboard" class="active">Dashboard</button>
    <button data-tab="artists">Artists</button>
    <button data-tab="playlists">Playlists</button>
    <button data-tab="tracks">Tracks</button>
    <button data-tab="console">Live Console</button>
  </nav>
</header>
<main>
  <section id="dashboard">
    <div class="grid stats" id="statCards"></div>
    <div class="card" style="margin-top:18px">
      <h2>Actions</h2>
      <div class="actions">
        <button class="btn" onclick="action('full')">&#9654; Full Sync</button>
        <button class="btn ghost" onclick="action('scan')">&#128269; Scan Playlists</button>
        <button class="btn ghost" onclick="action('artists-sync-new')">&#11015; Sync New Artists</button>
        <button class="btn ghost" onclick="action('artists-sync')">&#8635; Re-sync All</button>
        <button class="btn danger" onclick="stop()">&#9632; Stop</button>
      </div>
      <div class="hint">Output streams live in the <b>Live Console</b> tab.</div>
    </div>
  </section>

  <section id="artists" class="hide">
    <div class="card">
      <h2>Add artist</h2>
      <div class="row">
        <input id="artistEntry" placeholder="Artist name (e.g. THE MOTANS) or Spotify artist URL"/>
        <button class="btn" onclick="addArtist()">+ Add</button>
      </div>
      <div class="hint">Names are searched on Spotify; URLs are exact. Adding shows progress in the console.</div>
    </div>
    <div class="card" style="margin-top:16px">
      <div class="row" style="margin-bottom:12px">
        <input id="artistSearch" placeholder="Search artists..." oninput="loadArtists()"/>
        <select id="artistFilter" onchange="loadArtists()">
          <option value="all">All</option>
          <option value="active">Active</option>
          <option value="pending">Pending</option>
          <option value="synced">Synced</option>
          <option value="disabled">Disabled</option>
        </select>
        <button class="btn ghost sm" onclick="loadArtists()">Refresh</button>
      </div>
      <table><thead><tr><th>Artist</th><th>Source</th><th>Status</th><th>Last synced</th><th></th></tr></thead>
      <tbody id="artistRows"></tbody></table>
    </div>
  </section>

  <section id="playlists" class="hide">
    <div class="card">
      <h2>Add playlist to fetch</h2>
      <div class="row">
        <input id="plName" placeholder="Name (e.g. Top 50 Romania)" style="flex:.6"/>
        <input id="plUrl" placeholder="https://open.spotify.com/playlist/..."/>
        <button class="btn" onclick="addPlaylist()">+ Add</button>
      </div>
      <div class="hint">Saved to config.json + database so it persists across syncs.</div>
    </div>
    <div class="card" style="margin-top:16px">
      <table><thead><tr><th>Playlist</th><th>Status</th><th>Last scan</th><th></th></tr></thead>
      <tbody id="playlistRows"></tbody></table>
    </div>
  </section>

  <section id="tracks" class="hide">
    <div class="card">
      <div class="row" style="margin-bottom:12px">
        <input id="trackSearch" placeholder="Search downloaded tracks..." oninput="loadTracks()"/>
        <button class="btn ghost sm" onclick="loadTracks()">Refresh</button>
      </div>
      <table><thead><tr><th>Artist</th><th>Album</th><th>Title</th></tr></thead>
      <tbody id="trackRows"></tbody></table>
      <div class="hint" id="trackHint"></div>
    </div>
  </section>

  <section id="console" class="hide">
    <div class="card">
      <div class="row" style="margin-bottom:10px">
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
  ['dashboard','artists','playlists','tracks','console'].forEach(id=>$('#'+id).classList.add('hide'));
  $('#'+b.dataset.tab).classList.remove('hide');
  if(b.dataset.tab==='artists')loadArtists();
  if(b.dataset.tab==='playlists')loadPlaylists();
  if(b.dataset.tab==='tracks')loadTracks();
});
async function api(path,opts){const r=await fetch(path,opts);return r.json();}
async function loadStats(){
  const s=await api('/api/stats');
  const cards=[['Artists',s.artists_total],['Active',s.artists_active],['Synced',s.artists_synced],
    ['Pending',s.artists_pending],['Playlists',s.playlists_active+'/'+s.playlists_total],['Tracks',s.tracks]];
  $('#statCards').innerHTML=cards.map(c=>`<div class="card stat"><div class="n">${c[1]}</div><div class="l">${c[0]}</div></div>`).join('');
  const busy=!!s.running;
  $('#dot').className='dot'+(busy?' busy':'');
  $('#statusText').textContent=busy?('running: '+s.running):'idle';
}
async function loadArtists(){
  const q=encodeURIComponent($('#artistSearch').value||'');
  const st=$('#artistFilter').value;
  const rows=await api(`/api/artists?q=${q}&status=${st}`);
  $('#artistRows').innerHTML=rows.map(r=>{
    const sync=r.sync_done?'<span class="pill done">synced</span>':'<span class="pill pend">pending</span>';
    const act=r.active?'<span class="pill on">on</span>':'<span class="pill off">off</span>';
    const last=(r.last_synced||'-').slice(0,10);
    return `<tr><td>${esc(r.name)}</td><td class="muted">${esc(r.source)}</td><td>${act} ${sync}</td><td class="muted">${last}</td>
      <td class="row" style="border:none">
        <button class="btn ghost sm" onclick="toggleArtist('${r.spotify_id}')">${r.active?'Disable':'Enable'}</button>
        <button class="btn danger sm" onclick="delArtist('${r.spotify_id}')">x</button>
      </td></tr>`;
  }).join('')||'<tr><td colspan=5 class="muted">No artists yet.</td></tr>';
}
async function addArtist(){
  const v=$('#artistEntry').value.trim();if(!v)return;
  await api('/api/artists',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({entry:v})});
  $('#artistEntry').value='';toast('Adding '+v+' - see console');showConsole();
}
async function toggleArtist(id){await api(`/api/artists/${id}/toggle`,{method:'POST'});loadArtists();}
async function delArtist(id){if(!confirm('Remove from database?'))return;await api(`/api/artists/${id}`,{method:'DELETE'});loadArtists();}
async function loadPlaylists(){
  const rows=await api('/api/playlists');
  $('#playlistRows').innerHTML=rows.map(r=>{
    const act=r.active?'<span class="pill on">on</span>':'<span class="pill off">off</span>';
    const last=(r.last_synced||'-').slice(0,10);
    return `<tr><td><a href="${esc(r.url)}" target="_blank">${esc(r.name)}</a></td><td>${act}</td><td class="muted">${last}</td>
      <td class="row" style="border:none">
        <button class="btn ghost sm" onclick="togglePl('${r.spotify_id}')">${r.active?'Disable':'Enable'}</button>
        <button class="btn danger sm" onclick="delPl('${r.spotify_id}')">x</button>
      </td></tr>`;
  }).join('')||'<tr><td colspan=4 class="muted">No playlists yet.</td></tr>';
}
async function addPlaylist(){
  const name=$('#plName').value.trim(),url=$('#plUrl').value.trim();if(!url)return;
  const r=await api('/api/playlists',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,url})});
  if(r.error){toast(r.error);return;}
  $('#plName').value='';$('#plUrl').value='';loadPlaylists();toast('Playlist added');
}
async function togglePl(id){await api(`/api/playlists/${id}/toggle`,{method:'POST'});loadPlaylists();}
async function delPl(id){if(!confirm('Remove playlist?'))return;await api(`/api/playlists/${id}`,{method:'DELETE'});loadPlaylists();}
async function loadTracks(){
  const q=encodeURIComponent($('#trackSearch').value||'');
  const rows=await api(`/api/tracks?q=${q}&limit=300`);
  $('#trackRows').innerHTML=rows.map(r=>`<tr><td>${esc(r.artist)}</td><td class="muted">${esc(r.album)}</td><td>${esc(r.title)}</td></tr>`).join('')
    ||'<tr><td colspan=3 class="muted">No downloaded tracks found yet.</td></tr>';
  $('#trackHint').textContent=rows.length>=300?'Showing first 300 - refine your search.':(rows.length+' tracks');
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
    const d=document.createElement('div');d.className='ln';d.textContent=e.data;out.appendChild(d);
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
