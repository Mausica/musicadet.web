import logging
from typing import Optional, List, Dict

try:
    from ytmusicapi import YTMusic
except ImportError:
    YTMusic = None

log = logging.getLogger("musicadet.ytm")

def scan_artist(artist_name: str, ytmusic_instance=None) -> List[Dict]:
    """
    Scrape YouTube Music for an artist's full catalog (albums and singles).
    Returns a list of standardized track dictionaries.
    """
    if YTMusic is None:
        log.error("ytmusicapi is not installed. Please run: pip install ytmusicapi")
        return []
        
    ytm = ytmusic_instance or YTMusic()
    log.info("Searching YTMusic for artist: %s", artist_name)
    
    results = ytm.search(artist_name, filter="artists", limit=1)
    if not results:
        log.warning("No artist found for: %s", artist_name)
        return []
        
    browse_id = results[0].get("browseId")
    if not browse_id:
        return []
        
    log.info("Fetching catalog for %s (ID: %s)", artist_name, browse_id)
    try:
        artist_data = ytm.get_artist(browse_id)
    except Exception as e:
        log.error("Failed to fetch artist data for %s: %s", artist_name, e)
        return []
        
    songs = []
    
    def process_category(category_key):
        if category_key not in artist_data or not artist_data[category_key].get("browseId"):
            if category_key in artist_data and "results" in artist_data[category_key]:
                return artist_data[category_key]["results"]
            return []
            
        cat_browse_id = artist_data[category_key]["browseId"]
        params = artist_data[category_key].get("params")
        try:
            return ytm.get_artist_albums(cat_browse_id, params)
        except Exception:
            return artist_data[category_key].get("results", [])

    albums = process_category("albums")
    singles = process_category("singles")
    
    for release in (albums + singles):
        rel_id = release.get("browseId")
        if not rel_id:
            continue
            
        rel_title = release.get("title")
        rel_year = release.get("year", "")
        
        try:
            rel_data = ytm.get_album(rel_id)
        except Exception as e:
            log.warning("Failed to fetch album %s: %s", rel_title, e)
            continue
            
        tracks = rel_data.get("tracks", [])
        for idx, t in enumerate(tracks, 1):
            vid = t.get("videoId")
            if not vid:
                continue
                
            song = {
                "id": vid,
                "name": t.get("title"),
                "artist": artist_name,
                "album_id": rel_id,
                "album_name": rel_title,
                "track_number": idx,
                "year": rel_year,
                "url": f"https://music.youtube.com/watch?v={vid}",
                "download_url": f"https://music.youtube.com/watch?v={vid}"
            }
            songs.append(song)
            
    log.info("Found %d tracks across %d releases for %s", len(songs), len(albums)+len(singles), artist_name)
    return songs
