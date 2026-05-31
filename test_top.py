from ytmusicapi import YTMusic
yt = YTMusic()
results = yt.search("Bad Bunny", filter="artists")
if results:
    artist_data = yt.get_artist(results[0]['browseId'])
    if "songs" in artist_data and "results" in artist_data["songs"]:
        songs = artist_data["songs"]["results"]
        print(f"Found {len(songs)} top songs for Bad Bunny")
        for s in songs[:5]:
            print(s.get("title"), s.get("playCount", s.get("views", "N/A")))
