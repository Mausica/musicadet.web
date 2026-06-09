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


def _try_alternative_searches(artist_name: str, ytm) -> Optional[tuple[str, str]]:
    """
    Try fuzzy/alternative search queries to find the artist.
    Returns (browse_id, matched_name) or None if all fail.
    """
    alternatives = [
        artist_name,  # Original
        artist_name.replace(" - Topic", "").strip(),  # Remove Topic suffix
        artist_name.replace(" Topic", "").strip(),
        artist_name.replace(" -", "").strip(),  # Remove dashes
        re.sub(r'\s+', ' ', artist_name).strip(),  # Normalize spaces
    ]
    
    # Remove duplicates while preserving order
    seen = set()
    alternatives = [x for x in alternatives if not (x in seen or seen.add(x))]
    
    for alt_name in alternatives:
        if not alt_name:
            continue
        try:
            results = ytm.search(alt_name, filter="artists", limit=5)
            for r in results:
                candidate_name = r.get("artist", r.get("name", ""))
                if _artist_matches(artist_name, candidate_name):
                    browse_id = r.get("browseId")
                    if browse_id:
                        return browse_id, candidate_name
        except Exception:
            continue
    
    return None


def _search_artists_safe(artist_name: str, ytm) -> list[dict]:
    """Try artist search with fallback filters and return any results."""
    for args in [
        {"filter": "artists", "limit": 5},
        {"limit": 5},
        {"filter": "songs", "limit": 25},
    ]:
        try:
            results = ytm.search(artist_name, **args) if "filter" in args else ytm.search(artist_name, limit=args["limit"])
            if results:
                return results
        except Exception as e:
            log.info("YTMusic search fallback failed for %s (%s): %s", artist_name, args, e)
            continue
    return []


def scan_artist(artist_name: str, ytmusic_instance=None) -> List[Dict]:
    """Fetch full catalog for *artist_name* from YouTube Music."""
    songs, _, _ = scan_artist_with_metadata(artist_name, ytmusic_instance)
    return songs


def scan_artist_with_metadata(
    artist_name: str,
    ytmusic_instance=None,
    cached_browse_id: Optional[str] = None,
) -> tuple[List[Dict], Optional[str], Optional[str]]:
    """
    Fetch full catalog for *artist_name* from YouTube Music.
    
    Returns: (songs, matched_artist_name, browse_id)
    - songs: list of track dicts
    - matched_artist_name: the actual artist name from YT Music (e.g., "TwistaTv" if we searched "TWISTA")
    - browse_id: the YouTube Music browse ID for this artist (for future cached lookups)
    """
    if YTMusic is None:
        log.error("ytmusicapi not installed — pip install ytmusicapi")
        return [], None, None

    ytm = ytmusic_instance or YTMusic()
    
    # If we have a cached browse_id, skip the search step
    if cached_browse_id:
        log.info("Using cached YT Music artist ID for: %s", artist_name)
        browse_id = cached_browse_id
        matched_name = artist_name
    else:
        log.info("Searching YTMusic for artist: %s", artist_name)

        results = _search_artists_safe(artist_name, ytm)
        if not results:
            log.warning("No YTMusic results for %s", artist_name)
            return [], None, None

        # Find best matching artist from results
        browse_id = None
        matched_name = None
        for r in results:
            candidate_name = r.get("artist", r.get("name", ""))
            if _artist_matches(artist_name, candidate_name):
                browse_id = r.get("browseId")
                matched_name = candidate_name
                break

        # If no match in primary results, try alternative searches
        if not browse_id:
            log.info("No exact match for '%s', trying alternative searches...", artist_name)
            alt_result = _try_alternative_searches(artist_name, ytm)
            if alt_result:
                browse_id, matched_name = alt_result
                log.info("Found via alternative search: %s → %s", artist_name, matched_name)
            else:
                # Try searching by stripped Topic suffix if available
                stripped_name = _strip_topic_suffix(artist_name)
                if stripped_name != artist_name:
                    log.info("Trying stripped artist name: %s", stripped_name)
                    alt_result = _try_alternative_searches(stripped_name, ytm)
                    if alt_result:
                        browse_id, matched_name = alt_result
                        log.info("Found via stripped alternative search: %s → %s", artist_name, matched_name)

        if not browse_id:
            log.warning("No matching YTMusic artist for '%s' (got: %s)",
                        artist_name, [r.get("artist", r.get("name")) for r in results[:3]])
            return [], None, None

    log.info("Fetching catalog for %s (ID: %s)", matched_name or artist_name, browse_id)

    try:
        artist_data = ytm.get_artist(browse_id)
    except Exception as e:
        log.warning("Failed to get artist data for %s: %s — attempting direct song-search fallback", artist_name, e)
        # Fallback: try a broad direct song search and synthesize a catalog from matching tracks
        try:
            search_name = _strip_topic_suffix(artist_name)
            search_songs = ytm.search(search_name, filter="songs", limit=200)
        except Exception as e2:
            log.warning("Direct song search fallback also failed for %s: %s", artist_name, e2)
            return [], None, None

        if not search_songs:
            log.warning("Direct song search returned no results for %s", artist_name)
            return [], None, None

        # Build a minimal songs list from search results, matching by artist name
        songs = []
        albums_map: dict = {}
        for s in search_songs:
            # Extract artist names to verify match
            names_to_check = []
            artists_val = s.get("artists")
            if isinstance(artists_val, list):
                for a in artists_val:
                    if isinstance(a, dict):
                        names_to_check.append(a.get("name", ""))
                    elif isinstance(a, str):
                        names_to_check.append(a)
            elif isinstance(artists_val, str):
                names_to_check.append(artists_val)
            elif isinstance(artists_val, dict):
                names_to_check.append(artists_val.get("name", ""))

            artist_val = s.get("artist")
            if isinstance(artist_val, list):
                for a in artist_val:
                    if isinstance(a, dict):
                        names_to_check.append(a.get("name", ""))
                    elif isinstance(a, str):
                        names_to_check.append(a)
            elif isinstance(artist_val, str):
                names_to_check.append(artist_val)
            elif isinstance(artist_val, dict):
                names_to_check.append(artist_val.get("name", ""))

            matched = False
            for n in names_to_check:
                if n and _artist_matches(search_name, n) or _artist_matches(artist_name, n):
                    matched = True
                    break
            if not matched:
                continue

            vid = s.get("videoId") or s.get("id") or s.get("resultId")
            title = s.get("title") or s.get("name") or "Unknown"
            album_info = s.get("album") or {}
            album_name = album_info.get("name") if isinstance(album_info, dict) else (album_info or "Singles")
            if not album_name:
                album_name = "Singles"

            # Create a synthetic album id per artist+album
            key = f"search:{search_name}:{album_name}"
            album_id = albums_map.get(key)
            if not album_id:
                album_id = f"search:{abs(hash(key))}"
                albums_map[key] = album_id

            song_dict = {
                "id": vid or title,
                "name": title,
                "artist": artist_name,
                "album_id": album_id,
                "album_name": album_name,
                "track_number": 0,
                "year": None,
                "url": f"https://music.youtube.com/watch?v={vid}" if vid else None,
                "download_url": f"https://music.youtube.com/watch?v={vid}" if vid else None,
            }
            songs.append(song_dict)

        if songs:
            log.info("Direct-search fallback found %d tracks for %s", len(songs), artist_name)
            # matched_name remains None (we didn't resolve a browse_id)
            return songs, None, None
        return [], None, None

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
    return songs, matched_name, browse_id


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

    results = _search_artists_safe(search_name, ytm)
    if not results:
        log.warning("YTMusic artist search failed for %s", artist_name)
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
                names_to_check = []
                
                # Check "artists" (list of dicts, list of strings, or string)
                artists_val = s.get("artists")
                if isinstance(artists_val, list):
                    for a in artists_val:
                        if isinstance(a, dict):
                            names_to_check.append(a.get("name", ""))
                        elif isinstance(a, str):
                            names_to_check.append(a)
                elif isinstance(artists_val, str):
                    names_to_check.append(artists_val)
                elif isinstance(artists_val, dict):
                    names_to_check.append(artists_val.get("name", ""))

                # Check "artist" (string, dict, list of dicts, or list of strings)
                artist_val = s.get("artist")
                if isinstance(artist_val, list):
                    for a in artist_val:
                        if isinstance(a, dict):
                            names_to_check.append(a.get("name", ""))
                        elif isinstance(a, str):
                            names_to_check.append(a)
                elif isinstance(artist_val, str):
                    names_to_check.append(artist_val)
                elif isinstance(artist_val, dict):
                    names_to_check.append(artist_val.get("name", ""))

                matched_art = False
                for a_name in names_to_check:
                    if not a_name:
                        continue
                    if _artist_matches(search_name, a_name) or _artist_matches(artist_name, a_name):
                        matched_art = True
                        break
                if matched_art:
                    raw_tracks.append(s)

            # Absolute fallback: if filtering yielded 0 tracks, accept the top search results directly!
            if not raw_tracks and search_songs:
                log.info("Direct song search filtering yielded 0 tracks for %s. Falling back to accepting top search results directly.", artist_name)
                raw_tracks = search_songs[:limit]
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

