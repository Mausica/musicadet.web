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
