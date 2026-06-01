"""
YouTube Music catalog scanner — uses ytmusicapi (no auth needed).
Returns a list of song-dicts compatible with _upsert_artist_catalog().

Smart filtering:
  - Verifies artist name matches to avoid wrong-artist results
  - Skips compilations / Various Artists
  - Groups singles into one virtual 'Singles' album
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional

log = logging.getLogger("musicadet.ytm")

try:
    from ytmusicapi import YTMusic
except ImportError:
    YTMusic = None


def _normalize_for_compare(name: str) -> str:
    """Lowercase, strip spaces/punctuation for fuzzy artist comparison."""
    return re.sub(r'[^a-z0-9]', '', name.lower())


def _artist_matches(expected: str, actual: str) -> bool:
    """Check if YTM artist result is the same as what we searched for."""
    e = _normalize_for_compare(expected)
    a = _normalize_for_compare(actual)
    if not e or not a:
        return False
    # Exact match or one contains the other
    return e == a or e in a or a in e


def scan_artist(artist_name: str, ytmusic_instance=None) -> List[Dict]:
    """Fetch full catalog for *artist_name* from YouTube Music."""
    if YTMusic is None:
        log.error("ytmusicapi not installed — pip install ytmusicapi")
        return []

    ytm = ytmusic_instance or YTMusic()
    log.info("Searching YTMusic for artist: %s", artist_name)

    try:
        results = ytm.search(artist_name, filter="artists", limit=3)
    except Exception as e:
        log.warning("YTMusic search failed for %s: %s", artist_name, e)
        return []

    if not results:
        log.warning("No YTMusic results for %s", artist_name)
        return []

    # Find best matching artist from results
    browse_id = None
    matched_name = None
    for r in results:
        candidate_name = r.get("artist", r.get("name", ""))
        if _artist_matches(artist_name, candidate_name):
            browse_id = r.get("browseId")
            matched_name = candidate_name
            break

    if not browse_id:
        log.warning("No matching YTMusic artist for '%s' (got: %s)",
                    artist_name, [r.get("artist", r.get("name")) for r in results[:3]])
        return []

    log.info("Fetching catalog for %s (ID: %s)", matched_name, browse_id)

    try:
        artist_data = ytm.get_artist(browse_id)
    except Exception as e:
        log.warning("Failed to get artist data for %s: %s", artist_name, e)
        return []

    songs: List[Dict] = []
    singles_tracks: List[Dict] = []  # Collect singles separately

    for category_key in ("albums", "singles"):
        cat = artist_data.get(category_key)
        if not cat:
            continue

        releases = []
        cat_browse_id = cat.get("browseId")
        params = cat.get("params")
        if cat_browse_id:
            try:
                full_list = ytm.get_artist_albums(cat_browse_id, params)
                releases = full_list if full_list else []
            except Exception as e:
                log.warning("Failed to get %s for %s: %s", category_key, artist_name, e)
                releases = cat.get("results", [])
        else:
            releases = cat.get("results", [])

        for rel in releases:
            rel_id = rel.get("browseId")
            rel_title = rel.get("title", "Unknown Album")
            rel_year = rel.get("year")

            if not rel_id:
                continue

            # Skip compilations / Various Artists
            rel_artists = rel.get("artists", [])
            if rel_artists:
                primary = rel_artists[0].get("name", "") if isinstance(rel_artists[0], dict) else str(rel_artists[0])
                if primary.lower() in ("various artists", "various"):
                    log.debug("  Skipping compilation: %s", rel_title)
                    continue
                # Verify this release belongs to our artist
                if not _artist_matches(artist_name, primary):
                    log.debug("  Skipping mismatched artist release: %s by %s", rel_title, primary)
                    continue

            try:
                album_data = ytm.get_album(rel_id)
            except Exception as e:
                log.warning("  Failed to get album %s: %s", rel_title, e)
                continue

            tracks = album_data.get("tracks", [])
            if not tracks:
                continue

            for idx, track in enumerate(tracks, 1):
                vid = track.get("videoId")
                if not vid:
                    continue
                track_title = track.get("title", "Unknown")

                song_dict = {
                    "id": vid,
                    "name": track_title,
                    "artist": artist_name,  # Always use OUR name, not YTM's
                    "album_id": rel_id,
                    "album_name": rel_title,
                    "track_number": idx,
                    "year": rel_year or (album_data.get("year")),
                    "url": f"https://music.youtube.com/watch?v={vid}",
                    "download_url": f"https://music.youtube.com/watch?v={vid}",
                }

                # Singles go into a merged "Singles" bucket
                if len(tracks) < 5 or category_key == "singles":
                    singles_tracks.append(song_dict)
                else:
                    songs.append(song_dict)

    # Merge all singles into one virtual "Singles" album
    if singles_tracks:
        for idx, s in enumerate(singles_tracks, 1):
            s["album_id"] = f"singles:{browse_id}"
            s["album_name"] = "Singles"
            s["track_number"] = idx
            songs.append(s)

    log.info("Found %d tracks across %d releases for %s",
             len(songs),
             len({s["album_id"] for s in songs}),
             artist_name)
    return songs


def _strip_topic_suffix(name: str) -> str:
    n = (name or "").strip()
    for suffix in (" - Topic", " Topic", " - topic"):
        if n.endswith(suffix):
            n = n[: -len(suffix)].strip()
    return n


def _normalize_track_title(title: str) -> str:
    s = (title or "").lower()
    s = re.sub(r'\([^)]*\)', '', s)
    s = re.sub(r'\[[^\]]*\]', '', s)
    s = re.split(r'\b(feat|featuring|ft|with)\b', s)[0]
    return re.sub(r'[^a-z0-9]', '', s).strip()


def get_top_songs_ordered(artist_name: str, limit: int) -> List[Dict]:
    """
    Artist's top songs by views (full playlist, not the 5-song preview).

    Returns [{"title", "videoId", "norm"}, ...] in popularity order.
    """
    if YTMusic is None or limit <= 0:
        return []

    search_name = _strip_topic_suffix(artist_name)
    ytm = YTMusic()

    try:
        results = ytm.search(search_name, filter="artists", limit=5)
    except Exception as e:
        log.warning("YTMusic artist search failed for %s: %s", artist_name, e)
        results = []

    browse_id = None
    if results:
        for r in results:
            candidate = r.get("artist", r.get("name", ""))
            if _artist_matches(search_name, candidate) or _artist_matches(artist_name, candidate):
                browse_id = r.get("browseId")
                break
        if not browse_id:
            browse_id = results[0].get("browseId")

    artist_data = None
    if browse_id:
        try:
            artist_data = ytm.get_artist(browse_id)
        except Exception as e:
            log.warning("get_artist failed for %s (ID: %s): %s", artist_name, browse_id, e)

    raw_tracks: List[Dict] = []
    if artist_data:
        songs_section = artist_data.get("songs") or {}
        playlist_id = songs_section.get("browseId")

        if playlist_id:
            try:
                playlist = ytm.get_playlist(playlist_id, limit=limit)
                raw_tracks = playlist.get("tracks") or []
            except Exception as e:
                log.warning("get_playlist top songs failed for %s: %s", artist_name, e)

        if not raw_tracks:
            raw_tracks = songs_section.get("results") or []

    # Fallback to direct song search if we couldn't get any top songs from the artist page
    if not raw_tracks:
        log.info("No artist page tracks found for %s. Trying direct song search fallback...", artist_name)
        try:
            search_songs = ytm.search(search_name, filter="songs", limit=max(limit * 2, 20))
            # Filter search_songs to make sure we only include tracks matching the artist name
            for s in search_songs:
                artists = s.get("artists", [])
                matched_art = False
                for a in artists:
                    a_name = a.get("name", "") if isinstance(a, dict) else str(a)
                    if _artist_matches(search_name, a_name) or _artist_matches(artist_name, a_name):
                        matched_art = True
                        break
                if matched_art:
                    raw_tracks.append(s)
        except Exception as e:
            log.warning("YTMusic direct song search fallback failed for %s: %s", artist_name, e)

    ordered: List[Dict] = []
    seen_norm: set[str] = set()
    seen_vid: set[str] = set()

    for track in raw_tracks:
        if len(ordered) >= limit:
            break
        title = track.get("title") or ""
        vid = track.get("videoId") or track.get("id")
        norm = _normalize_track_title(title)
        if not norm and not vid:
            continue
        if vid and vid in seen_vid:
            continue
        if norm and norm in seen_norm:
            continue
        if vid:
            seen_vid.add(vid)
        if norm:
            seen_norm.add(norm)
        ordered.append({"title": title, "videoId": vid, "norm": norm})

    if ordered:
        log.debug(
            "Top songs for %s: %d from YT Music (requested %d)",
            artist_name, len(ordered), limit,
        )
    return ordered

