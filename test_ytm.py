from ytmusicapi import YTMusic
yt = YTMusic()
results = yt.search("The Neighbourhood", filter="artists")
artist_data = yt.get_artist(results[0]['browseId'])
print("Top songs play counts:")
if "songs" in artist_data and "results" in artist_data["songs"]:
    for s in artist_data["songs"]["results"][:3]:
        print(s.get("title"), s.get("playCount"))

albums = yt.get_artist_albums(artist_data["albums"]["browseId"], artist_data["albums"]["params"])
album = yt.get_album(albums[0]['browseId'])
print("\nAlbum tracks play counts:")
for t in album['tracks'][:3]:
    print(t.get('title'), t.get('playCount', t.get('views')))
